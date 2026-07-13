use alloy::primitives::{Address, U256, U512};
use rustc_hash::FxHashMap;

use crate::core::math::dodo::estimate_dodo_hop_capacity;
use crate::core::math::fixed_point::ONE;
use crate::core::types::{Edge, FoundCycle, PoolState, ProtocolType, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::{cl_amount_cap, simulate_route_minimal};
use crate::pipeline::route_sim_cache::RouteSimCache;
use crate::pipeline::sim_sanity::{
    SimSanityInput, check_sim_sanity, check_sim_sanity_fast, check_sim_sanity_for_dispatch,
    max_flash_borrow_wei, min_economic_amount_in,
};
use crate::pipeline::types::OptimizationResult;
use crate::services::execution::gas_oracle::{GasOracle, RouteGasLookup};
use crate::services::execution::profit::{ProfitEvalContext, net_profit_after_gas_from_sim};

/// Resolve simulated hop gas the same way probe ranking does before Brent scoring.
#[derive(Debug, Clone, Copy)]
pub struct RouteGasCosting<'a> {
    pub lookup: &'a RouteGasLookup,
    pub oracle: &'a GasOracle,
    pub fingerprint: u64,
}
use crate::services::oracle::{resolve_token_decimals_for_index, resolve_token_to_matic_rate};
use crate::util::ten_pow_u256_cached;

const BRENT_CACHE_SLOTS: usize = 24;
const GOLDEN_RATIO: u128 = 382;
const CONVERGENCE_DIVISOR: u128 = 1000;
const DEFAULT_BRENT_ITERATIONS: u32 = 16;
const DEFAULT_MAX_FLASH_LOAN_USD: u64 = 50_000;

fn lookup_sim_cache(
    cache: &[(U256, crate::pipeline::types::MinimalSimResult)],
    amount: U256,
) -> Option<&crate::pipeline::types::MinimalSimResult> {
    cache.iter().find(|(a, _)| *a == amount).map(|(_, sim)| sim)
}

/// Tickless CL pools cap trade size at the decimal-aware probe; Brent cannot search above that.
fn optimize_at_amount_cap(
    arena: &StateArena,
    edges: &[Edge],
    amount: U256,
    search_low: U256,
    start_decimals: u8,
    start_rate: U256,
) -> Option<OptimizationResult> {
    let sim = simulate_route_minimal(arena, edges, amount)?;
    if sim.profit.is_zero()
        || check_sim_sanity(SimSanityInput {
            amount_in: amount,
            gross_profit: sim.profit,
            search_low,
            token_decimals: start_decimals,
            token_to_matic_rate: start_rate,
        })
        .is_err()
    {
        return None;
    }
    Some(OptimizationResult {
        optimal_input: amount,
        expected_gross: sim.amount_out,
        net_profit: sim.profit,
        total_gas: sim.total_gas,
        search_low,
    })
}

/// Bounded golden-section maximization over a U256 search range.
pub fn solve_brent_optimal<F>(low: U256, high: U256, mut evaluate: F, max_iterations: u32) -> U256
where
    F: FnMut(U256) -> U256,
{
    if low >= high {
        return low;
    }

    let max_iter = if max_iterations == 0 {
        DEFAULT_BRENT_ITERATIONS
    } else {
        max_iterations
    };

    let mut cache_amounts = [U256::ZERO; BRENT_CACHE_SLOTS];
    let mut cache_profits = [U256::ZERO; BRENT_CACHE_SLOTS];
    let mut cache_size = 0usize;

    let mut cached_evaluate = |amount: U256| -> U256 {
        // ponytail: manual unrolled search over small fixed-size cache (16 slots).
        // The optimizer struggles to auto-vectorize the dynamic bounds path when
        // cache_size < BRENT_CACHE_SLOTS, so we search the full allocated region
        // and short-circuit on ZERO sentinels.
        for i in 0..BRENT_CACHE_SLOTS {
            if cache_amounts[i] == amount {
                return cache_profits[i];
            }
            if cache_amounts[i].is_zero() {
                break;
            }
        }
        let profit = evaluate(amount);
        let slot = cache_size % BRENT_CACHE_SLOTS;
        cache_amounts[slot] = amount;
        cache_profits[slot] = profit;
        cache_size += 1;
        profit
    };

    let mut a = low;
    let mut b = high;
    let mut left = a + (b - a) * U256::from(GOLDEN_RATIO) / U256::from(1_000u16);
    let mut right = b - (b - a) * U256::from(GOLDEN_RATIO) / U256::from(1_000u16);
    let mut left_value = cached_evaluate(left);
    let mut right_value = cached_evaluate(right);

    for _ in 0..max_iter {
        let width = b.saturating_sub(a);
        let tol = (width / U256::from(CONVERGENCE_DIVISOR)).max(U256::from(1u8));
        if width <= tol {
            break;
        }

        if left_value < right_value {
            a = left;
            left = right;
            left_value = right_value;
            right = b - (b - a) * U256::from(GOLDEN_RATIO) / U256::from(1_000u16);
            right_value = cached_evaluate(right);
        } else {
            b = right;
            right = left;
            right_value = left_value;
            left = a + (b - a) * U256::from(GOLDEN_RATIO) / U256::from(1_000u16);
            left_value = cached_evaluate(left);
        }
    }

    let candidates = [
        (low, cached_evaluate(low)),
        (left, left_value),
        (right, right_value),
        (high, cached_evaluate(high)),
    ];
    let mut best = candidates[0];
    for candidate in candidates.into_iter().skip(1) {
        if candidate.1 > best.1 {
            best = candidate;
        }
    }
    best.0
}

fn cap_or_default(cap: U256, default: U256) -> U256 {
    if cap.is_zero() { default } else { cap }
}

fn hop_capacity(arena: &StateArena, edge: &Edge) -> Option<U256> {
    let state = arena.pool_state(edge.pool_index)?;
    let default_cap = ONE;

    match (state, edge.protocol) {
        (PoolState::V2(s), ProtocolType::UniswapV2) => {
            let cap = if edge.zero_for_one {
                s.reserve0
            } else {
                s.reserve1
            };
            Some(cap_or_default(cap, default_cap))
        }
        (PoolState::V3(s), ProtocolType::UniswapV3)
        | (PoolState::V4(s), ProtocolType::UniswapV4) => {
            if s.sqrt_price_x96.is_zero() || s.liquidity == 0 {
                return None;
            }
            let liq_u = U256::from(s.liquidity);
            let cap: U256 = if edge.zero_for_one {
                (liq_u << 96) / s.sqrt_price_x96
            } else {
                (liq_u * s.sqrt_price_x96) >> 96
            };
            Some(cap_or_default(cap, default_cap))
        }
        (PoolState::Curve(s), ProtocolType::CurveStable | ProtocolType::CurveCrypto) => {
            let idx = edge.token_in_idx as usize;
            let cap = s.balances.get(idx).copied().unwrap_or(U256::ZERO);
            Some(cap_or_default(cap, default_cap))
        }
        (PoolState::Balancer(s), ProtocolType::BalancerV2) => {
            let idx = edge.token_in_idx as usize;
            let cap = s.balances.get(idx).copied().unwrap_or(U256::ZERO);
            Some(cap_or_default(cap, default_cap))
        }
        (PoolState::Dodo(s), ProtocolType::Dodo) => {
            let cap = estimate_dodo_hop_capacity(s, edge.zero_for_one);
            Some(cap_or_default(cap, default_cap))
        }
        (PoolState::Woofi(s), ProtocolType::Woofi) => {
            let cap = if edge.token_in_idx as usize >= s.base_states.len() {
                s.quote_reserve
            } else {
                s.base_states
                    .get(edge.token_in_idx as usize)
                    .map_or(U256::ZERO, |b| b.reserve)
            };
            Some(cap_or_default(cap, default_cap))
        }
        _ => Some(default_cap),
    }
}

/// Liquidity-aware Brent search bounds in start-token units.
#[allow(clippy::too_many_arguments)]
fn get_dynamic_search_bounds(
    cycle: &FoundCycle,
    arena: &StateArena,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
    token_decimals: &FxHashMap<Address, u8>,
    start_rate: U256,
    start_decimals: u8,
    max_flash_loan_usd: u64,
    matic_usd: f64,
    liquidity_cap: Option<U256>,
) -> (U256, U256) {
    let mut min_capacity = U256::MAX;
    let mut can_normalize_all = true;
    let mut saw_capacity = false;

    for edge in &cycle.edges {
        let Some(mut capacity) = hop_capacity(arena, edge) else {
            crate::trace!("get_dynamic_search_bounds: zero capacity for edge");
            return (U256::ZERO, U256::ZERO);
        };
        saw_capacity = true;

        let token_in_rate = resolve_token_to_matic_rate(edge.token_in, token_to_matic_rates);
        if token_in_rate.is_zero() || start_rate.is_zero() {
            can_normalize_all = false;
        } else {
            let token_in_decimals =
                resolve_token_decimals_for_index(edge.token_in, arena, token_decimals);
            let start_scale = ten_pow_u256_cached(start_decimals);
            let token_in_scale = ten_pow_u256_cached(token_in_decimals);
            // U512 widening prevents overflow when capacity * token_in_rate * start_scale
            // exceeds U256::MAX — common for 18-decimal tokens with large reserves.
            let num = U512::from(capacity) * U512::from(token_in_rate) * U512::from(start_scale);
            let den = U512::from(start_rate) * U512::from(token_in_scale);
            capacity = crate::util::u512_to_u256(num / den);
        }

        if capacity < min_capacity {
            min_capacity = capacity;
        }
    }

    if !can_normalize_all || !saw_capacity || min_capacity.is_zero() || min_capacity == U256::MAX {
        min_capacity = ONE * U256::from(100u8);
    }

    let mut low = min_capacity / U256::from(5000u16);
    let mut high = min_capacity / U256::from(10u8);

    let max_search_low = min_capacity / U256::from(50u8);
    let max_search_high = min_capacity / U256::from(5u8);
    if low > max_search_low {
        low = max_search_low;
    }
    if high > max_search_high {
        high = max_search_high;
    }

    if !start_rate.is_zero() {
        let min_economic = min_economic_amount_in(start_decimals, start_rate);
        if min_economic <= max_search_low && low < min_economic {
            low = min_economic;
        }
        if let Some(max_wei) =
            max_flash_borrow_wei(max_flash_loan_usd, start_decimals, start_rate, matic_usd)
            && high > max_wei
        {
            high = max_wei;
        }
    }

    let floor_low = high / U256::from(100u8);
    let effective_floor = if floor_low > U256::from(1u8) {
        floor_low
    } else {
        U256::from(1u8)
    };
    let final_low = if low > effective_floor {
        low
    } else {
        effective_floor
    };
    let final_high = if high > final_low {
        high
    } else {
        final_low + U256::from(1u8)
    };

    let (mut out_low, mut out_high) = (final_low, final_high);
    // ponytail: single liquidity-cap clamping step (was triple-redundant).
    if let Some(cap) = liquidity_cap.filter(|c| !c.is_zero()) {
        out_high = out_high.min(cap);
        if out_low >= out_high {
            out_low = out_high
                .saturating_sub(U256::from(1u8))
                .max(U256::from(1u8));
        }
    }

    let economic_floor = min_economic_amount_in(start_decimals, start_rate);
    if out_low < economic_floor && economic_floor <= out_high {
        out_low = economic_floor;
    }
    if out_high <= out_low {
        out_high = out_low.saturating_add(U256::from(1u8));
    }

    (out_low, out_high)
}

#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn optimize_cycle(
    arena: &StateArena,
    cycle: &FoundCycle,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
    token_decimals: &FxHashMap<Address, u8>,
    max_flash_loan_usd: Option<u64>,
    matic_usd: f64,
    max_iterations: Option<u32>,
    liquidity_cap: Option<U256>,
    profit_ctx: &ProfitEvalContext,
    seed_sims: Option<&[(U256, crate::pipeline::types::MinimalSimResult)]>,
    route_gas: Option<RouteGasCosting<'_>>,
    route_sim_cache: Option<(&RouteSimCache, u64, u64)>,
) -> Option<OptimizationResult> {
    let start_rate = resolve_token_to_matic_rate(cycle.start_token, token_to_matic_rates);
    let start_decimals = resolve_token_decimals_for_index(cycle.start_token, arena, token_decimals);
    let economic_floor = min_economic_amount_in(start_decimals, start_rate);

    let edges = &cycle.edges;
    let (mut low, mut high) = get_dynamic_search_bounds(
        cycle,
        arena,
        token_to_matic_rates,
        token_decimals,
        start_rate,
        start_decimals,
        max_flash_loan_usd.unwrap_or(DEFAULT_MAX_FLASH_LOAN_USD),
        matic_usd,
        liquidity_cap,
    );
    if high < economic_floor {
        high = economic_floor.saturating_mul(U256::from(100u8));
        if !start_rate.is_zero()
            && let Some(max_wei) = max_flash_borrow_wei(
                max_flash_loan_usd.unwrap_or(DEFAULT_MAX_FLASH_LOAN_USD),
                start_decimals,
                start_rate,
                matic_usd,
            )
            && high > max_wei
        {
            high = max_wei;
        }
    }
    if low < economic_floor {
        low = economic_floor;
    }
    if high < economic_floor || high <= low {
        crate::trace!(
            "optimize_cycle: bounds empty low={low} high={high} economic_floor={economic_floor} edge_count={}",
            edges.len()
        );
        return None;
    }

    if let Some(cap) = cl_amount_cap(arena, edges) {
        if cap.is_zero() {
            crate::trace!("optimize_cycle: CL cap is zero");
            return None;
        }
        if economic_floor > cap {
            return optimize_at_amount_cap(arena, edges, cap, cap, start_decimals, start_rate);
        }
        if high > cap {
            high = cap;
        }
        if high <= low {
            crate::trace!(
                "optimize_cycle: bounds empty after CL cap low={low} high={high} cap={cap}"
            );
            return None;
        }
    }

    let mut sim_cache: Vec<(U256, crate::pipeline::types::MinimalSimResult)> =
        Vec::with_capacity(BRENT_CACHE_SLOTS);
    if let Some(seeds) = seed_sims {
        for (amount, sim) in seeds {
            if sim_cache.len() >= BRENT_CACHE_SLOTS {
                break;
            }
            sim_cache.push((*amount, *sim));
        }
    }
    let mut brent_local_hits = 0u32;
    let mut brent_route_hits = 0u32;
    let mut brent_sim_calls = 0u32;
    let mut brent_rejected = 0u32;
    let evaluate = |amount: U256| -> U256 {
        if amount < economic_floor {
            return U256::ZERO;
        }
        if let Some(sim) = lookup_sim_cache(&sim_cache, amount) {
            brent_local_hits += 1;
            return net_profit_after_gas_from_sim(sim, amount, profit_ctx);
        }
        if let Some((cache, generation, route_fp)) = route_sim_cache
            && let Some(cached) = cache.get(generation, route_fp, amount)
        {
            brent_route_hits += 1;
            if sim_cache.len() < BRENT_CACHE_SLOTS {
                sim_cache.push((amount, cached));
            }
            return net_profit_after_gas_from_sim(&cached, amount, profit_ctx);
        }
        brent_sim_calls += 1;
        match simulate_route_minimal(arena, edges, amount) {
            Some(mut sim) => {
                if let Some(costing) = route_gas {
                    sim.total_gas = costing.lookup.route_gas_or_heuristic(
                        costing.oracle,
                        costing.fingerprint,
                        sim.total_gas,
                    );
                }
                if sim.profit.is_zero()
                    || check_sim_sanity_fast(SimSanityInput {
                        amount_in: amount,
                        gross_profit: sim.profit,
                        search_low: low,
                        token_decimals: start_decimals,
                        token_to_matic_rate: start_rate,
                    })
                    .is_err()
                {
                    brent_rejected += 1;
                    return U256::ZERO;
                }
                let score = net_profit_after_gas_from_sim(&sim, amount, profit_ctx);
                if let Some((cache, generation, route_fp)) = route_sim_cache {
                    cache.insert(generation, route_fp, amount, sim);
                }
                if sim_cache.len() < BRENT_CACHE_SLOTS {
                    sim_cache.push((amount, sim));
                }
                score
            }
            None => {
                brent_rejected += 1;
                U256::ZERO
            }
        }
    };

    let iterations = max_iterations.unwrap_or(DEFAULT_BRENT_ITERATIONS);
    let optimal = solve_brent_optimal(low, high, evaluate, iterations);
    if brent_sim_calls > 4 {
        crate::debug!(
            "brent sim: hops={} iters={iterations} walk_sim={brent_sim_calls} local_hit={brent_local_hits} route_hit={brent_route_hits} rejected={brent_rejected}",
            edges.len()
        );
    }
    if optimal < economic_floor {
        crate::trace!(
            "optimize_cycle: optimal={optimal} < economic_floor={economic_floor} low={low} high={high}"
        );
        return None;
    }
    let mut sim = lookup_sim_cache(&sim_cache, optimal)
        .copied()
        .or_else(|| simulate_route_minimal(arena, edges, optimal))?;
    if let Some(costing) = route_gas {
        sim.total_gas = costing.lookup.route_gas_or_heuristic(
            costing.oracle,
            costing.fingerprint,
            sim.total_gas,
        );
    }
    if sim.profit.is_zero() {
        crate::trace!("optimize_cycle: optimal sim zero profit optimal={optimal} low={low}");
        return None;
    }
    let sanity_input = SimSanityInput {
        amount_in: optimal,
        gross_profit: sim.profit,
        search_low: low,
        token_decimals: start_decimals,
        token_to_matic_rate: start_rate,
    };
    if check_sim_sanity_for_dispatch(sanity_input).is_err() {
        crate::trace!(
            "optimize_cycle: sanity optimal={optimal} profit={} low={low}",
            sim.profit
        );
        return None;
    }
    let search_low = if check_sim_sanity(sanity_input).is_ok() {
        low
    } else {
        U256::ZERO
    };
    Some(OptimizationResult {
        optimal_input: optimal,
        expected_gross: sim.amount_out,
        net_profit: sim.profit,
        total_gas: sim.total_gas,
        search_low,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_brent_converges() {
        let r = solve_brent_optimal(
            U256::from(0u8),
            U256::from(100u8),
            |x| x * (U256::from(100u8) - x),
            16,
        );
        assert!(r > U256::ZERO);
    }

    fn peaked_at(x: U256, peak: u64) -> U256 {
        let peak = U256::from(peak);
        let distance = if x > peak { x - peak } else { peak - x };
        U256::from(10_000u64).saturating_sub(distance * distance)
    }

    #[test]
    fn optimizer_explores_left_of_midpoint() {
        let optimal = solve_brent_optimal(U256::ZERO, U256::from(100u8), |x| peaked_at(x, 20), 16);
        assert!((U256::from(18u8)..=U256::from(22u8)).contains(&optimal));
    }

    #[test]
    fn optimizer_explores_right_of_midpoint() {
        let optimal = solve_brent_optimal(U256::ZERO, U256::from(100u8), |x| peaked_at(x, 80), 16);
        assert!((U256::from(78u8)..=U256::from(82u8)).contains(&optimal));
    }
}
