use std::collections::HashMap;

use ruint::aliases::U256;
use rustc_hash::FxHashMap;


use alloy::primitives::Address;

use crate::core::constants::MIN_ECONOMIC_VALUE_MATIC_WEI;
use crate::core::math::dodo::estimate_dodo_hop_capacity;
use crate::core::types::{Edge, FoundCycle, PoolState, ProtocolType, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::simulate_route_minimal;
use crate::pipeline::sim_sanity::{SimSanityInput, check_sim_sanity, min_economic_amount_in};
use crate::pipeline::types::OptimizationResult;
use crate::services::execution::profit::{ProfitEvalContext, net_profit_after_gas_from_sim};
use crate::services::oracle::resolve_token_to_matic_rate;
use crate::util::ten_pow_u256;

const BRENT_CACHE_SLOTS: usize = 16;
const GOLDEN_RATIO: u128 = 382; // (3 - sqrt(5))/2 * 1000
const CONVERGENCE_DIVISOR: u128 = 1000;
const DEFAULT_BRENT_ITERATIONS: u32 = 8;
const DEFAULT_MAX_FLASH_LOAN_USD: u64 = 50_000;
const TEN_POW_18: U256 = U256::from_limbs([0xDE0B6B3A7640000, 0, 0, 0]);

#[inline]
fn ten_pow_u256_cached(decimals: u8) -> U256 {
    if decimals == 18 {
        TEN_POW_18
    } else {
        ten_pow_u256(decimals)
    }
}

fn lookup_sim_cache(
    cache: &[(U256, crate::pipeline::types::MinimalSimResult)],
    amount: U256,
) -> Option<crate::pipeline::types::MinimalSimResult> {
    cache
        .iter()
        .find(|(a, _)| *a == amount)
        .map(|(_, sim)| sim.clone())
}

/// Brent's method for maximizing profit over a U256 search range.
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
        for i in 0..cache_size {
            if cache_amounts[i] == amount {
                return cache_profits[i];
            }
        }
        let profit = evaluate(amount);
        if cache_size < BRENT_CACHE_SLOTS {
            cache_amounts[cache_size] = amount;
            cache_profits[cache_size] = profit;
            cache_size += 1;
        }
        profit
    };

    let mut a = low;
    let mut b = high;
    let mut x = a + (b - a) / U256::from(2u8);
    let mut w = x;
    let mut v = x;
    let mut fx = cached_evaluate(x);
    let mut fw = fx;
    let mut fv = fx;
    let mut d = U256::ZERO;
    let mut e = U256::ZERO;

    for _ in 0..max_iter {
        let xm = a + (b - a) / U256::from(2u8);
        let tol = {
            let bracket = b.saturating_sub(a);
            let t = bracket / U256::from(CONVERGENCE_DIVISOR);
            if t > U256::from(1u8) {
                t
            } else {
                U256::from(1u8)
            }
        };

        if b - a <= tol {
            break;
        }

        if e > tol {
            let parabolic_step = parabolic_step_u256(x, w, v, fx, fw, fv, a, b, e);
            if let Some(step) = parabolic_step {
                e = d;
                d = step;
            } else {
                e = if x >= xm {
                    x.saturating_sub(a)
                } else {
                    b.saturating_sub(x)
                };
                d = e * U256::from(GOLDEN_RATIO) / U256::from(1000u16);
            }
        } else {
            e = if x >= xm {
                x.saturating_sub(a)
            } else {
                b.saturating_sub(x)
            };
            d = e * U256::from(GOLDEN_RATIO) / U256::from(1000u16);
        }

        let mut u = x.saturating_add(d);
        if u.saturating_sub(a) < tol {
            u = a.saturating_add(tol);
        } else if b.saturating_sub(u) < tol {
            u = b.saturating_sub(tol);
        }
        if u < low {
            u = low;
        }
        if u > high {
            u = high;
        }

        let fu = cached_evaluate(u);

        if fu >= fx {
            if u >= x {
                a = x;
            } else {
                b = x;
            }
            v = w;
            fv = fw;
            w = x;
            fw = fx;
            x = u;
            fx = fu;
        } else {
            if u < x {
                a = u;
            } else {
                b = u;
            }
            if fu >= fw || w == x {
                v = w;
                fv = fw;
                w = u;
                fw = fu;
            } else if fu >= fv || v == x || v == w {
                v = u;
                fv = fu;
            }
        }
    }

    let flow = cached_evaluate(low);
    let fhigh = cached_evaluate(high);
    if flow > fx && flow >= fhigh {
        return low;
    }
    if fhigh > fx && fhigh > flow {
        return high;
    }
    x
}

fn u256_to_i128(v: U256) -> Option<i128> {
    if v > U256::from(i128::MAX as u128) {
        None
    } else {
        Some(v.to::<u128>() as i128)
    }
}

fn parabolic_step_u256(
    x: U256,
    w: U256,
    v: U256,
    fx: U256,
    fw: U256,
    fv: U256,
    a: U256,
    b: U256,
    e: U256,
) -> Option<U256> {
    let x_i = u256_to_i128(x)?;
    let w_i = u256_to_i128(w)?;
    let v_i = u256_to_i128(v)?;
    let fx_i = u256_to_i128(fx)?;
    let fw_i = u256_to_i128(fw)?;
    let fv_i = u256_to_i128(fv)?;
    let a_i = u256_to_i128(a)?;
    let b_i = u256_to_i128(b)?;
    let e_i = u256_to_i128(e)?;

    let tmp1 = (x_i - w_i).saturating_mul(fx_i.saturating_sub(fv_i));
    let tmp2 = (x_i - v_i).saturating_mul(fx_i.saturating_sub(fw_i));
    let mut p = (x_i - v_i)
        .saturating_mul(tmp2)
        .saturating_sub((x_i - w_i).saturating_mul(tmp1));
    let mut q = 2i128.saturating_mul(tmp2.saturating_sub(tmp1));

    if q > 0 {
        p = -p;
    } else {
        q = -q;
    }

    if q <= 0 {
        return None;
    }

    let parabolic_ok = p > q.saturating_mul(a_i.saturating_sub(x_i))
        && p < q.saturating_mul(b_i.saturating_sub(x_i))
        && p.unsigned_abs() < (q.saturating_mul(e_i) / 2).unsigned_abs();

    if parabolic_ok {
        let step = p / q;
        // Negative parabolic steps must fall back to golden-section (unsigned d cannot represent them).
        if step <= 0 {
            return None;
        }
        Some(U256::try_from(step as u128).unwrap_or(U256::ZERO))
    } else {
        None
    }
}

fn hop_capacity(arena: &StateArena, edge: &Edge) -> Option<U256> {
    let state = arena.pool_state(edge.pool_index)?;
    let default_cap = TEN_POW_18;

    match (state, edge.protocol) {
        (PoolState::V2(s), ProtocolType::UniswapV2) => {
            let cap = if edge.zero_for_one {
                s.reserve0
            } else {
                s.reserve1
            };
            Some(if cap.is_zero() { default_cap } else { cap })
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
            Some(if cap.is_zero() { default_cap } else { cap })
        }
        (PoolState::Curve(s), ProtocolType::CurveStable)
        | (PoolState::Curve(s), ProtocolType::CurveCrypto) => {
            let idx = edge.token_in_idx as usize;
            let cap = s.balances.get(idx).copied().unwrap_or(U256::ZERO);
            Some(if cap.is_zero() { default_cap } else { cap })
        }
        (PoolState::Balancer(s), ProtocolType::BalancerV2) => {
            let idx = edge.token_in_idx as usize;
            let cap = s.balances.get(idx).copied().unwrap_or(U256::ZERO);
            Some(if cap.is_zero() { default_cap } else { cap })
        }
        (PoolState::Dodo(s), ProtocolType::Dodo) => {
            let cap = estimate_dodo_hop_capacity(s, edge.zero_for_one);
            Some(if cap.is_zero() { default_cap } else { cap })
        }
        (PoolState::Woofi(s), ProtocolType::Woofi) => {
            let n = s.base_states.len();
            if edge.token_in_idx as usize >= n {
                let cap = s.quote_reserve;
                Some(if cap.is_zero() { default_cap } else { cap })
            } else {
                let cap = s
                    .base_states
                    .get(edge.token_in_idx as usize)
                    .map(|b| b.reserve)
                    .unwrap_or(U256::ZERO);
                Some(if cap.is_zero() { default_cap } else { cap })
            }
        }
        _ => Some(default_cap),
    }
}

/// Liquidity-aware Brent search bounds in start-token units.
pub fn get_dynamic_search_bounds(
    cycle: &FoundCycle,
    arena: &StateArena,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
    token_decimals: &HashMap<Address, u8>,
    max_flash_loan_usd: u64,
    liquidity_cap: Option<U256>,
) -> (U256, U256) {
    let start_rate =
        resolve_token_to_matic_rate(cycle.start_token, arena, token_to_matic_rates);
    let start_decimals = arena
        .token_address(cycle.start_token)
        .map(|a| token_decimals.get(&a).copied().unwrap_or(18))
        .unwrap_or(18);

    let mut min_capacity = U256::MAX;
    let mut can_normalize_all = true;
    let mut saw_capacity = false;

    for edge in &cycle.edges {
        let Some(mut capacity) = hop_capacity(arena, edge) else {
            continue;
        };
        saw_capacity = true;

        let token_in_rate =
            resolve_token_to_matic_rate(edge.token_in, arena, token_to_matic_rates);
        if token_in_rate.is_zero() {
            can_normalize_all = false;
        } else {
            let token_in_decimals = arena
                .token_address(edge.token_in)
                .map(|a| token_decimals.get(&a).copied().unwrap_or(18))
                .unwrap_or(18);
            let start_scale = ten_pow_u256_cached(start_decimals);
            let token_in_scale = ten_pow_u256_cached(token_in_decimals);
            capacity =
                (capacity * token_in_rate * start_scale) / (start_rate * token_in_scale);
        }

        if capacity < min_capacity {
            min_capacity = capacity;
        }
    }

    if !can_normalize_all || !saw_capacity || min_capacity.is_zero() || min_capacity == U256::MAX {
        min_capacity = TEN_POW_18 * U256::from(100u8);
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
    }

    if !start_rate.is_zero() {
        let scale = ten_pow_u256_cached(start_decimals);
        let max_wei = (U256::from(max_flash_loan_usd) * TEN_POW_18 * scale) / start_rate;
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
    if let Some(cap) = liquidity_cap.filter(|c| !c.is_zero())
        && out_high > cap
    {
        out_high = cap;
        if out_low > out_high {
            let floor = out_high / U256::from(100u8);
            out_low = if floor > U256::from(1u8) {
                floor
            } else {
                U256::from(1u8)
            };
            if out_low > out_high {
                out_low = out_high
                    .saturating_sub(U256::from(1u8))
                    .max(U256::from(1u8));
            }
        }
    }
    if out_high <= out_low {
        if let Some(cap) = liquidity_cap.filter(|c| !c.is_zero()) {
            // Keep the search range inside the flash-liquidity cap.
            if out_low >= cap {
                out_low = cap.saturating_sub(U256::from(1u8)).max(U256::from(1u8));
                out_high = cap;
            } else {
                out_high = (out_low + U256::from(1u8)).min(cap);
            }
        } else {
            out_high = out_low + U256::from(1u8);
        }
    }

    if let Some(cap) = liquidity_cap.filter(|c| !c.is_zero()) {
        out_high = out_high.min(cap);
        if out_low > out_high {
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

pub fn optimize_cycle(
    arena: &StateArena,
    cycle: &FoundCycle,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
    token_decimals: &HashMap<Address, u8>,
    max_flash_loan_usd: Option<u64>,
    max_iterations: Option<u32>,
    liquidity_cap: Option<U256>,
    profit_ctx: &ProfitEvalContext,
    seed_sims: Option<&[(U256, crate::pipeline::types::MinimalSimResult)]>,
) -> Option<OptimizationResult> {
    let start_rate =
        resolve_token_to_matic_rate(cycle.start_token, arena, token_to_matic_rates);
    let start_decimals = arena
        .token_address(cycle.start_token)
        .map(|a| token_decimals.get(&a).copied().unwrap_or(18))
        .unwrap_or(18);
    let economic_floor = min_economic_amount_in(start_decimals, start_rate);

    let edges = &cycle.edges;
    let (mut low, mut high) = get_dynamic_search_bounds(
        cycle,
        arena,
        token_to_matic_rates,
        token_decimals,
        max_flash_loan_usd.unwrap_or(DEFAULT_MAX_FLASH_LOAN_USD),
        liquidity_cap,
    );
    if high < economic_floor {
        high = economic_floor.saturating_mul(U256::from(100u8));
        if !start_rate.is_zero() {
            let scale = ten_pow_u256_cached(start_decimals);
            let max_wei = (U256::from(
                max_flash_loan_usd.unwrap_or(DEFAULT_MAX_FLASH_LOAN_USD),
            ) * TEN_POW_18
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
        return None;
    }
    if low < economic_floor {
        low = economic_floor;
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
            return score_sim(amount, &sim, profit_ctx);
        }
        match simulate_route_minimal(arena, edges, amount) {
            Some(sim) => {
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
                let score = score_sim(amount, &sim, profit_ctx);
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
        return None;
    }
    let sim = lookup_sim_cache(&sim_cache, optimal)
        .or_else(|| simulate_route_minimal(arena, edges, optimal))?;
    if sim.profit.is_zero()
        || check_sim_sanity(SimSanityInput {
            amount_in: optimal,
            gross_profit: sim.profit,
            search_low: low,
            token_decimals: start_decimals,
            token_to_matic_rate: start_rate,
        })
        .is_err()
    {
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

fn score_sim(
    amount_in: U256,
    sim: &crate::pipeline::types::MinimalSimResult,
    profit_ctx: &ProfitEvalContext,
) -> U256 {
    net_profit_after_gas_from_sim(sim, amount_in, profit_ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{FlashLoanSource, PoolState, ProtocolType, V2PoolState};
    use crate::pipeline::graph::{build_graph, pool_meta_from_pair};
    use crate::pipeline::{cycle_finder, local_sim};
    use crate::services::execution::profit::ProfitEvalContext;
    use alloy::primitives::Address;

    #[test]
    fn brent_finds_peak_on_quadratic() {
        let peak = U256::from(500u64);
        let optimal = solve_brent_optimal(
            U256::from(1u64),
            U256::from(1000u64),
            |x| {
                if x > peak { peak - (x - peak) } else { x }
            },
            12,
        );
        assert!(optimal >= U256::from(400u64) && optimal <= U256::from(600u64));
    }

    #[test]
    fn brent_converges_on_narrow_bracket() {
        let peak = U256::from(1_000_000u64);
        let optimal = solve_brent_optimal(
            U256::from(999_000u64),
            U256::from(1_001_000u64),
            |x| {
                if x > peak {
                    peak.saturating_sub(x - peak)
                } else {
                    x
                }
            },
            12,
        );
        assert!(
            optimal >= U256::from(999_500u64) && optimal <= U256::from(1_000_500u64),
            "optimal {optimal}"
        );
    }

    #[test]
    fn optimize_v2_triangle() {
        let mut arena = StateArena::new();
        let t0 = arena.register_token(Address::repeat_byte(1));
        let t1 = arena.register_token(Address::repeat_byte(2));
        let t2 = arena.register_token(Address::repeat_byte(3));
        let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
        let v2 = |r0: U256, r1: U256| {
            PoolState::V2(V2PoolState {
                reserve0: r0,
                reserve1: r1,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            })
        };
        let p01 = arena.register_pool(
            Address::repeat_byte(0x10),
            v2(reserve, reserve * U256::from(2u8)),
        );
        let p12 = arena.register_pool(
            Address::repeat_byte(0x11),
            v2(reserve, reserve * U256::from(2u8)),
        );
        let p20 = arena.register_pool(
            Address::repeat_byte(0x12),
            v2(reserve * U256::from(2u8), reserve),
        );
        let pools = vec![
            pool_meta_from_pair(p01, ProtocolType::UniswapV2, t0, t1, Some(30)),
            pool_meta_from_pair(p12, ProtocolType::UniswapV2, t1, t2, Some(30)),
            pool_meta_from_pair(p20, ProtocolType::UniswapV2, t2, t0, Some(30)),
        ];
        let graph = build_graph(&arena, &pools);
        let cycles = cycle_finder::find_cycles(&arena, &graph, 4, 50);
        let cycle = cycles
            .into_iter()
            .find(|c| c.hop_count == 3)
            .expect("3-hop");
        let mut rates = FxHashMap::default();
        rates.insert(t0, U256::from(10u128).pow(U256::from(18)));
        let profit_ctx = ProfitEvalContext::for_cycle(
            t0,
            &arena,
            &rates,
            &HashMap::new(),
            U256::from(30_000_000_000u64),
            50,
            FlashLoanSource::Balancer,
        );
        let opt = optimize_cycle(
            &arena,
            &cycle,
            &rates,
            &HashMap::new(),
            None,
            None,
            None,
            &profit_ctx,
            None,
        )
        .expect("opt");
        assert!(opt.optimal_input > U256::ZERO);
        let sim =
            local_sim::simulate_route_minimal(&arena, &cycle.edges, opt.optimal_input).unwrap();
        assert_eq!(sim.profit, opt.net_profit);
    }

    #[test]
    fn liquidity_cap_bounds_respect_cap_when_low_equals_cap() {
        let cap = U256::from(5u64);
        let cycle = FoundCycle {
            start_token: TokenIndex(0),
            edges: vec![].into(),
            hop_count: 0,
            log_weight: 0.0,
            cumulative_fee_bps: 0,
            score: 0.0,
        };
        let (low, high) = get_dynamic_search_bounds(
            &cycle,
            &StateArena::new(),
            &FxHashMap::default(),
            &HashMap::new(),
            DEFAULT_MAX_FLASH_LOAN_USD,
            Some(cap),
        );
        assert!(high <= cap, "high {high} exceeds cap {cap}");
        assert!(low <= high);
    }
}
