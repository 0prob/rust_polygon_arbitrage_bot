use alloy::primitives::{Address, U256};
use rustc_hash::FxHashMap;

use crate::core::constants::MIN_ECONOMIC_VALUE_MATIC_WEI;
use crate::core::math::dodo::estimate_dodo_hop_capacity;
use crate::core::math::fixed_point::ONE;
use crate::core::types::{Edge, FoundCycle, PoolState, ProtocolType, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::{cl_amount_cap, simulate_route_minimal};
use crate::pipeline::sim_sanity::{SimSanityInput, check_sim_sanity, min_economic_amount_in};
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

const BRENT_CACHE_SLOTS: usize = 16;
const GOLDEN_RATIO: u128 = 382; // (3 - sqrt(5))/2 * 1000
const CONVERGENCE_DIVISOR: u128 = 1000;
const DEFAULT_BRENT_ITERATIONS: u32 = 8;
const DEFAULT_MAX_FLASH_LOAN_USD: u64 = 50_000;

fn lookup_sim_cache(
    cache: &[(U256, crate::pipeline::types::MinimalSimResult)],
    amount: U256,
) -> Option<&crate::pipeline::types::MinimalSimResult> {
    cache.iter().find(|(a, _)| *a == amount).map(|(_, sim)| sim)
}

/// Tickless CL pools cap trade size at `SPOT_PROBE`; Brent cannot search above that.
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
        for i in 0..cache_size.min(BRENT_CACHE_SLOTS) {
            if cache_amounts[i] == amount {
                return cache_profits[i];
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

        let token_in_rate = resolve_token_to_matic_rate(edge.token_in, arena, token_to_matic_rates);
        if token_in_rate.is_zero() || start_rate.is_zero() {
            can_normalize_all = false;
        } else {
            let token_in_decimals =
                resolve_token_decimals_for_index(edge.token_in, arena, token_decimals);
            let start_scale = ten_pow_u256_cached(start_decimals);
            let token_in_scale = ten_pow_u256_cached(token_in_decimals);
            capacity = (capacity * token_in_rate * start_scale) / (start_rate * token_in_scale);
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
        let start_scale = ten_pow_u256_cached(start_decimals);
        let min_economic = (U256::from(MIN_ECONOMIC_VALUE_MATIC_WEI) * start_scale) / start_rate;
        if min_economic <= max_search_low && low < min_economic {
            low = min_economic;
        }
        let max_wei = (U256::from(max_flash_loan_usd) * ONE * start_scale) / start_rate;
        if high > max_wei {
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
    max_iterations: Option<u32>,
    liquidity_cap: Option<U256>,
    profit_ctx: &ProfitEvalContext,
    seed_sims: Option<&[(U256, crate::pipeline::types::MinimalSimResult)]>,
    route_gas: Option<RouteGasCosting<'_>>,
) -> Option<OptimizationResult> {
    let start_rate = resolve_token_to_matic_rate(cycle.start_token, arena, token_to_matic_rates);
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
        liquidity_cap,
    );
    if high < economic_floor {
        high = economic_floor.saturating_mul(U256::from(100u8));
        if !start_rate.is_zero() {
            let scale = ten_pow_u256_cached(start_decimals);
            let max_wei = (U256::from(max_flash_loan_usd.unwrap_or(DEFAULT_MAX_FLASH_LOAN_USD))
                * ONE
                * scale)
                / start_rate;
            if high > max_wei {
                high = max_wei;
            }
        }
    }
    if low < economic_floor {
        low = economic_floor;
    }
    if let Some(seeds) = seed_sims
        && let Some((seed_amount, seed_sim)) = seeds.first()
        && !seed_sim.profit.is_zero()
        && *seed_amount >= economic_floor
        && *seed_amount <= high
        && *seed_amount > low
    {
        low = *seed_amount;
    }
    if high < economic_floor || high <= low {
        crate::trace!(
            "optimize_cycle: bounds empty low={low} high={high} economic_floor={economic_floor} edge_count={}",
            edges.len()
        );
        return None;
    }
    if low < economic_floor {
        low = economic_floor;
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
            sim_cache.push((*amount, sim.clone()));
        }
    }
    let evaluate = |amount: U256| -> U256 {
        if amount < economic_floor {
            return U256::ZERO;
        }
        if let Some(sim) = lookup_sim_cache(&sim_cache, amount) {
            return net_profit_after_gas_from_sim(sim, amount, profit_ctx);
        }
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
                    || check_sim_sanity(SimSanityInput {
                        amount_in: amount,
                        gross_profit: sim.profit,
                        search_low: low,
                        token_decimals: start_decimals,
                        token_to_matic_rate: start_rate,
                    })
                    .is_err()
                {
                    return U256::ZERO;
                }
                let score = net_profit_after_gas_from_sim(&sim, amount, profit_ctx);
                if sim_cache.len() < BRENT_CACHE_SLOTS {
                    sim_cache.push((amount, sim));
                }
                score
            }
            None => U256::ZERO,
        }
    };

    let iterations = max_iterations.unwrap_or(DEFAULT_BRENT_ITERATIONS);
    let optimal = solve_brent_optimal(low, high, evaluate, iterations);
    if optimal < economic_floor {
        crate::trace!(
            "optimize_cycle: optimal={optimal} < economic_floor={economic_floor} low={low} high={high}"
        );
        return None;
    }
    let mut sim = lookup_sim_cache(&sim_cache, optimal)
        .cloned()
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
    if let Err(reason) = check_sim_sanity(SimSanityInput {
        amount_in: optimal,
        gross_profit: sim.profit,
        search_low: low,
        token_decimals: start_decimals,
        token_to_matic_rate: start_rate,
    }) {
        crate::trace!(
            "optimize_cycle: sanity({reason:?}) optimal={optimal} profit={} low={low}",
            sim.profit
        );
        return None;
    }
    Some(OptimizationResult {
        optimal_input: optimal,
        expected_gross: sim.amount_out,
        net_profit: sim.profit,
        total_gas: sim.total_gas,
        search_low: low,
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
