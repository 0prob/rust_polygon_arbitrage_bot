use alloy::primitives::{Address, U256, U512};
use rustc_hash::FxHashMap;

use crate::core::math::dodo::estimate_dodo_hop_capacity;
use crate::core::math::fixed_point::ONE;
use crate::core::types::{Edge, FoundCycle, PoolState, ProtocolType, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::brent_diag::{
    BrentOptimizeReject, record_brent_attempt, record_brent_bal_zero_other,
    record_brent_cache_local, record_brent_cache_route, BrentEvalReject, BrentSimNoneKind,
    record_brent_cl_depth_clamp, record_brent_eval_reject, record_brent_eval_sim, record_brent_ok,
    record_brent_reject, record_brent_seed_high_clamp, record_brent_shallow_hop,
    record_brent_sim_none_kind, record_brent_unsupported_protocol, record_brent_warm_seed,
    record_brent_zero_out_protocol, should_sample_brent_sim_none,
};
use crate::pipeline::local_sim::{
    MinimalSimFailure, minimal_sim_failure, precompute_route_shallow_caps,
    simulate_route_minimal_with_caps, tickless_cl_start_input_cap,
};
use crate::pipeline::route_sim_cache::RouteSimCache;
use crate::pipeline::sim_sanity::{
    FlashBorrowCapParams, SimSanityInput, check_sim_sanity, check_sim_sanity_fast,
    check_sim_sanity_for_dispatch, min_economic_amount_in,
};
use crate::pipeline::ternary_diag::{
    TernaryBoundsReject, record_ternary_bounds_call, record_ternary_bounds_ok,
    record_ternary_bounds_reject, record_ternary_economic_high_raise,
    record_ternary_flash_cap_clamp, record_ternary_golden_zero_exit,
    record_ternary_bal_high_clamp, record_ternary_cl_depth_clamp,
    record_ternary_liquidity_cap_clamp, record_ternary_rate_fallback,
    record_ternary_seed_high_clamp,
};
use crate::pipeline::types::OptimizationResult;
use crate::services::execution::gas_oracle::{GasOracle, RouteGasLookup};
use crate::services::execution::profit::{ProfitEvalContext, brent_score_matic_from_sim};

/// Resolve simulated hop gas the same way probe ranking does before Brent scoring.
#[derive(Debug, Clone, Copy)]
pub struct RouteGasCosting<'a> {
    pub lookup: &'a RouteGasLookup,
    pub oracle: &'a GasOracle,
    pub fingerprint: u64,
}
use crate::services::oracle::{resolve_token_decimals_for_index, resolve_token_to_matic_rate};
use crate::util::ten_pow_u256_cached;

pub const BRENT_SEED_CACHE_SLOTS: usize = 24;
const BRENT_CACHE_SLOTS: usize = BRENT_SEED_CACHE_SLOTS;
const GOLDEN_RATIO: u128 = 382;
const CONVERGENCE_DIVISOR: u128 = 1000;
const DEFAULT_BRENT_ITERATIONS: u32 = 16;
const DEFAULT_MAX_FLASH_LOAN_USD: u64 = 50_000;

#[derive(Debug, Clone, Copy)]
struct TernarySearchBounds {
    low: U256,
    high: U256,
}

/// Tickless CL pools cap trade size at the decimal-aware probe; Brent cannot search above that.
fn optimize_at_amount_cap(
    arena: &StateArena,
    edges: &[Edge],
    amount: U256,
    search_low: U256,
    start_decimals: u8,
    start_rate: U256,
    shallow_caps: Option<&[U256; crate::core::constants::HOP_CAP_USIZE]>,
) -> Option<OptimizationResult> {
    let sim = simulate_route_minimal_with_caps(arena, edges, amount, shallow_caps)?;
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
    record_brent_ok();
    Some(OptimizationResult {
        optimal_input: amount,
        expected_gross: sim.amount_out,
        net_profit: sim.profit,
        total_gas: sim.total_gas,
        search_low,
    })
}

/// Bounded golden-section maximization over a U256 search range.
pub fn solve_brent_optimal<F>(low: U256, high: U256, evaluate: F, max_iterations: u32) -> U256
where
    F: FnMut(U256) -> U256,
{
    solve_brent_optimal_warm(low, high, evaluate, max_iterations, &[])
}

/// Golden-section search with pre-scored probe seeds inside `[low, high]` (skips re-sim).
pub fn solve_brent_optimal_warm<F>(
    low: U256,
    high: U256,
    mut evaluate: F,
    max_iterations: u32,
    warm: &[(U256, U256)],
) -> U256
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
    for &(amount, profit) in warm {
        if amount < low || amount > high {
            continue;
        }
        let slot = cache_size % BRENT_CACHE_SLOTS;
        cache_amounts[slot] = amount;
        cache_profits[slot] = profit;
        cache_size += 1;
        record_brent_warm_seed();
    }

    let mut cached_evaluate = |amount: U256| -> U256 {
        let scan = cache_size.min(BRENT_CACHE_SLOTS);
        for i in 0..scan {
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
    let (mut left, mut right) = if let Some((hint, _)) = warm
        .iter()
        .filter(|(amt, _)| *amt >= low && *amt <= high)
        .max_by_key(|(_, profit)| *profit)
        .filter(|(_, profit)| !profit.is_zero())
    {
        let width = b.saturating_sub(a);
        let quarter = width * U256::from(GOLDEN_RATIO) / U256::from(1_000u16);
        let l = hint.saturating_sub(quarter).max(a);
        let r = hint.saturating_add(quarter).min(b);
        if r > l {
            (l, r)
        } else {
            (
                a + width * U256::from(GOLDEN_RATIO) / U256::from(1_000u16),
                b - width * U256::from(GOLDEN_RATIO) / U256::from(1_000u16),
            )
        }
    } else {
        (
            a + (b - a) * U256::from(GOLDEN_RATIO) / U256::from(1_000u16),
            b - (b - a) * U256::from(GOLDEN_RATIO) / U256::from(1_000u16),
        )
    };
    let mut left_value = cached_evaluate(left);
    let mut right_value = cached_evaluate(right);

    let mut zero_streak = 0u32;
    for _ in 0..max_iter {
        let width = b.saturating_sub(a);
        let tol = (width / U256::from(CONVERGENCE_DIVISOR)).max(U256::from(1u8));
        if width <= tol {
            break;
        }
        if left_value.is_zero() && right_value.is_zero() {
            zero_streak += 1;
            if zero_streak >= 2 {
                // Re-center on best warm seed once before giving up — otherwise a
                // mid-window dead band exits without ever comparing ladder seeds.
                if let Some((hint, _)) = warm
                    .iter()
                    .filter(|(amt, score)| {
                        *amt >= a && *amt <= b && !score.is_zero()
                    })
                    .max_by_key(|(_, score)| *score)
                {
                    let width = b.saturating_sub(a);
                    let quarter = width * U256::from(GOLDEN_RATIO) / U256::from(1_000u16);
                    left = hint.saturating_sub(quarter).max(a);
                    right = hint.saturating_add(quarter).min(b);
                    if right > left {
                        left_value = cached_evaluate(left);
                        right_value = cached_evaluate(right);
                        zero_streak = 0;
                        continue;
                    }
                }
                record_ternary_golden_zero_exit();
                break;
            }
        } else {
            zero_streak = 0;
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

    // Always compete warm seeds against the golden-section endpoints. Early zero-exit
    // used to discard mid-window ladder scores that never landed on left/right.
    let mut best = (low, cached_evaluate(low));
    for candidate in [
        (left, left_value),
        (right, right_value),
        (high, cached_evaluate(high)),
    ] {
        if candidate.1 > best.1 {
            best = candidate;
        }
    }
    for &(amount, score) in warm {
        if amount < low || amount > high || score.is_zero() {
            continue;
        }
        if score > best.1 {
            best = (amount, score);
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
            let bal = s.balances.get(idx).copied().unwrap_or(U256::ZERO);
            // Vault `MAX_IN_RATIO` = 30%. Live `bal_zo` samples were 100% max_in
            // even at a 20% soft cap — FX-normalized mid-hop start amounts still
            // overshoot when prior hops diverge from oracle rates. Use 10%.
            let cap = bal / U256::from(10u8);
            Some(cap_or_default(cap, default_cap))
        }
        (PoolState::Dodo(s), ProtocolType::Dodo) => {
            // Meta is [base, quote]; capacity of the sold leg uses token_in_idx.
            let cap = estimate_dodo_hop_capacity(s, edge.token_in_idx == 0);
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

/// Liquidity-aware golden-section bounds in start-token units.
#[allow(clippy::too_many_arguments)]
fn compute_ternary_search_bounds(
    cycle: &FoundCycle,
    arena: &StateArena,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
    token_decimals: &FxHashMap<Address, u8>,
    start_rate: U256,
    start_decimals: u8,
    max_flash_loan_usd: u64,
    matic_usd: f64,
    matic_usd_chainlink: Option<alloy::primitives::I256>,
    liquidity_cap: Option<U256>,
) -> Option<TernarySearchBounds> {
    record_ternary_bounds_call();
    let flash_cap_params = FlashBorrowCapParams {
        max_flash_loan_usd,
        token_decimals: start_decimals,
        token_to_matic_rate: start_rate,
        matic_usd,
        matic_usd_chainlink,
    };
    if flash_cap_params.cap_enforced_but_unresolved() {
        record_ternary_bounds_reject(TernaryBoundsReject::FlashCapUnavailable);
        crate::trace!("ternary: bounds flash_cap_unavailable");
        return None;
    }
    let mut min_capacity = U256::MAX;
    let mut can_normalize_all = true;
    let mut saw_start_unit_capacity = false;
    let start_scale = ten_pow_u256_cached(start_decimals);

    for edge in &cycle.edges {
        let Some(mut capacity) = hop_capacity(arena, edge) else {
            record_ternary_bounds_reject(TernaryBoundsReject::HopCapacity);
            crate::trace!("ternary: bounds hop_capacity_fail");
            return None;
        };

        // Only fold hop caps that are already in start-token units. Mixing raw
        // hop-token balances into min_capacity then wiping to 100*ONE on any
        // missing FX rate discarded Balancer/V2 soft caps (live rate_fallback
        // ~6% of bounds) and let Brent high land in ZeroOutput / BAL#304.
        if edge.token_in == cycle.start_token {
            // Capacity already denominated in the search token.
        } else {
            let token_in_rate = resolve_token_to_matic_rate(edge.token_in, token_to_matic_rates);
            if token_in_rate.is_zero() || start_rate.is_zero() {
                // Path-aware `clamp_high_to_balancer_max_in` still constrains Balancer
                // after bounds; skipping here only loses the soft-cap contribution.
                can_normalize_all = false;
                continue;
            }
            let token_in_decimals =
                resolve_token_decimals_for_index(edge.token_in, arena, token_decimals);
            let token_in_scale = ten_pow_u256_cached(token_in_decimals);
            // U512 widening prevents overflow when capacity * token_in_rate * start_scale
            // exceeds U256::MAX — common for 18-decimal tokens with large reserves.
            let num = U512::from(capacity) * U512::from(token_in_rate) * U512::from(start_scale);
            let den = U512::from(start_rate) * U512::from(token_in_scale);
            capacity = crate::util::u512_to_u256(num / den);
            // Extra haircut on FX-converted Balancer caps: oracle rates disagree
            // with path execution, so start-token sizing overshoots hop MAX_IN.
            if edge.protocol == ProtocolType::BalancerV2 {
                capacity /= U256::from(2u8);
            }
        }

        saw_start_unit_capacity = true;
        if capacity < min_capacity {
            min_capacity = capacity;
        }
    }

    if !saw_start_unit_capacity || min_capacity.is_zero() || min_capacity == U256::MAX {
        record_ternary_rate_fallback();
        min_capacity = ONE * U256::from(100u8);
    } else if !can_normalize_all {
        // Partial FX: keep start-unit bottleneck (incl. Balancer soft cap on
        // start hop) instead of replacing with the unbounded 100*ONE default.
        record_ternary_rate_fallback();
    }

    // Search window: ~0.02%–10% of bottleneck hop capacity (start-token units).
    // Former max_search_low/high clamps were dead (low always < max_low, high always < max_high).
    let mut low = min_capacity / U256::from(5000u16);
    let mut high = min_capacity / U256::from(10u8);

    // Resolve flash USD ceiling once (was recomputed on every clamp site).
    let flash_cap_wei = if start_rate.is_zero() {
        None
    } else {
        flash_cap_params.cap_wei()
    };

    if !start_rate.is_zero() {
        let min_economic = min_economic_amount_in(start_decimals, start_rate);
        if low < min_economic {
            low = min_economic;
        }
        if let Some(max_wei) = flash_cap_wei
            && high > max_wei
        {
            record_ternary_flash_cap_clamp();
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
        record_ternary_liquidity_cap_clamp();
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
    if out_high < economic_floor {
        record_ternary_economic_high_raise();
        out_high = economic_floor.saturating_mul(U256::from(100u8));
        if let Some(max_wei) = flash_cap_wei
            && out_high > max_wei
        {
            record_ternary_flash_cap_clamp();
            out_high = max_wei;
        }
    }
    if out_low < economic_floor {
        out_low = economic_floor;
    }
    if out_high <= out_low {
        out_high = out_low.saturating_add(U256::from(1u8));
    }
    if out_high <= out_low || out_high < economic_floor {
        record_ternary_bounds_reject(TernaryBoundsReject::InvalidRange);
        return None;
    }
    record_ternary_bounds_ok();
    Some(TernarySearchBounds {
        low: out_low,
        high: out_high,
    })
}

/// Cap Brent `high` to `8 ×` largest profitable probe seed when that is tighter
/// than the capacity-derived window. Returns `None` when no clamp applies.
#[must_use]
fn clamp_brent_high_to_probe_seeds(
    high: U256,
    low: U256,
    economic_floor: U256,
    seeds: &[(U256, crate::pipeline::types::MinimalSimResult)],
) -> Option<U256> {
    let mut max_feasible_seed = U256::ZERO;
    for (amount, sim) in seeds {
        if !sim.profit.is_zero() && *amount > max_feasible_seed {
            max_feasible_seed = *amount;
        }
    }
    if max_feasible_seed.is_zero() {
        return None;
    }
    // 8× headroom: 4× cut ok-rate in live capture (774→530) by starving Brent
    // of room above dust probes; 8× still far below capacity-derived highs.
    let mut seed_high = max_feasible_seed.saturating_mul(U256::from(8u8));
    if seed_high < economic_floor || seed_high >= high {
        return None;
    }
    if seed_high <= low {
        seed_high = low.saturating_add(U256::from(1u8));
    }
    Some(seed_high)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClDepthHighClamp {
    /// `high` already walks — no clamp.
    Noop,
    /// Tighten Brent `high` to max simulatable start.
    Clamped(U256),
    /// Even `economic_floor`/`low` is CL-shallow (live residual hop 2+) — abandon.
    WindowInfeasible,
}

/// When Brent `high` itself is CL-shallow (seed/capacity clamps still overshoot tick
/// depth), binary-search the largest start amount that still simulates. If the
/// floor is also shallow, mark the window infeasible so Brent does not thrash
/// hop-2+ SimNone (cldepth2: shallow_hop 2p=24 after clamp-only).
#[must_use]
fn clamp_brent_high_to_cl_feasible(
    arena: &StateArena,
    edges: &[Edge],
    low: U256,
    high: U256,
    economic_floor: U256,
    shallow_caps: Option<&[U256; crate::core::constants::HOP_CAP_USIZE]>,
) -> ClDepthHighClamp {
    if !edges.iter().any(|edge| {
        matches!(
            edge.protocol,
            ProtocolType::UniswapV3 | ProtocolType::UniswapV4
        )
    }) {
        return ClDepthHighClamp::Noop;
    }
    if simulate_route_minimal_with_caps(arena, edges, high, shallow_caps).is_some() {
        return ClDepthHighClamp::Noop;
    }
    let floor = if economic_floor > low {
        economic_floor
    } else {
        low
    };
    if floor >= high {
        return ClDepthHighClamp::WindowInfeasible;
    }
    if simulate_route_minimal_with_caps(arena, edges, floor, shallow_caps).is_none() {
        return ClDepthHighClamp::WindowInfeasible;
    }
    let mut lo = floor;
    let mut hi = high;
    for _ in 0..12 {
        if hi <= lo.saturating_add(U256::from(1u8)) {
            break;
        }
        let mid = lo + (hi - lo) / U256::from(2u8);
        if simulate_route_minimal_with_caps(arena, edges, mid, shallow_caps).is_some() {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    if lo < high && lo >= floor {
        ClDepthHighClamp::Clamped(lo)
    } else {
        ClDepthHighClamp::WindowInfeasible
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BalancerHighClamp {
    /// No start amount in `[floor, high]` stays under vault `MAX_IN_RATIO`.
    WindowInfeasible,
    /// Tighten `high` (and optionally raise-shrink `low`) to a feasible band.
    Clamped { low: U256, high: U256 },
}

/// Path-aware Balancer `MAX_IN_RATIO` bound. Soft FX caps alone still overshoot
/// when prior-hop execution diverges from oracle rates (`bal_zo` 100% max_in).
/// When capacity-derived `low` already trips MAX_IN, search downward toward
/// `economic_floor` (live: ~54% of Brent attempts were `bounds_fail` from
/// infeasible Balancer windows).
#[must_use]
fn balancer_max_in_high_clamp(
    arena: &StateArena,
    edges: &[Edge],
    low: U256,
    high: U256,
    economic_floor: U256,
) -> Option<BalancerHighClamp> {
    if high <= low || !edges.iter().any(|e| e.protocol == ProtocolType::BalancerV2) {
        return None;
    }
    let hits_max_in = |amount: U256| {
        matches!(
            minimal_sim_failure(arena, edges, amount),
            Some(MinimalSimFailure::BalancerMaxInRatio { .. })
        )
    };
    let search_low = low;
    if hits_max_in(low) {
        let floor = economic_floor.min(low);
        if floor.is_zero() || hits_max_in(floor) {
            return Some(BalancerHighClamp::WindowInfeasible);
        }
        // Largest feasible amount in [floor, low).
        let mut lo = floor;
        let mut hi = low;
        for _ in 0..48 {
            if hi.saturating_sub(lo) <= U256::from(1u8) {
                break;
            }
            let mid = lo + (hi - lo) / U256::from(2u8);
            if hits_max_in(mid) {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        let mut clamped = lo.saturating_mul(U256::from(95u8)) / U256::from(100u8);
        if clamped < floor {
            clamped = floor;
        }
        if clamped <= floor {
            return Some(BalancerHighClamp::Clamped {
                low: floor,
                high: floor.saturating_add(U256::from(1u8)).min(lo.max(floor)),
            });
        }
        return Some(BalancerHighClamp::Clamped {
            low: floor,
            high: clamped,
        });
    }
    if !hits_max_in(high) {
        return None;
    }
    let mut lo = search_low;
    let mut hi = high;
    for _ in 0..48 {
        if hi.saturating_sub(lo) <= U256::from(1u8) {
            break;
        }
        let mid = lo + (hi - lo) / U256::from(2u8);
        if hits_max_in(mid) {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    // Haircut 5% below the last feasible probe so golden-section right endpoint
    // cannot land on the first infeasible integer after binary-search quantization.
    let mut clamped = lo.saturating_mul(U256::from(95u8)) / U256::from(100u8);
    if clamped <= search_low {
        clamped = search_low.saturating_add(U256::from(1u8));
    }
    if clamped < high {
        Some(BalancerHighClamp::Clamped {
            low: search_low,
            high: clamped,
        })
    } else {
        None
    }
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
    matic_usd_chainlink: Option<alloy::primitives::I256>,
    max_iterations: Option<u32>,
    liquidity_cap: Option<U256>,
    profit_ctx: &ProfitEvalContext,
    seed_sims: Option<&[(U256, crate::pipeline::types::MinimalSimResult)]>,
    route_gas: Option<RouteGasCosting<'_>>,
    route_sim_cache: Option<(&RouteSimCache, u64, u64)>,
) -> Option<OptimizationResult> {
    record_brent_attempt();
    let start_rate = resolve_token_to_matic_rate(cycle.start_token, token_to_matic_rates);
    let start_decimals = resolve_token_decimals_for_index(cycle.start_token, arena, token_decimals);
    let economic_floor = min_economic_amount_in(start_decimals, start_rate);

    let edges = &cycle.edges;
    let TernarySearchBounds {
        mut low,
        mut high,
    } = compute_ternary_search_bounds(
        cycle,
        arena,
        token_to_matic_rates,
        token_decimals,
        start_rate,
        start_decimals,
        max_flash_loan_usd.unwrap_or(DEFAULT_MAX_FLASH_LOAN_USD),
        matic_usd,
        matic_usd_chainlink,
        liquidity_cap,
    )
    .or_else(|| {
        record_brent_reject(BrentOptimizeReject::BoundsEmpty);
        crate::trace!(
            "optimize_cycle: ternary bounds empty economic_floor={economic_floor} edge_count={}",
            edges.len()
        );
        None
    })?;

    let brent_shallow_caps = precompute_route_shallow_caps(arena, edges);
    if let Some(tickless_high) =
        tickless_cl_start_input_cap(arena, cycle.start_token, edges)
    {
        if tickless_high.is_zero() {
            record_brent_reject(BrentOptimizeReject::ClCapZero);
            crate::trace!("optimize_cycle: tickless CL cap is zero");
            return None;
        }
        if economic_floor > tickless_high {
            return optimize_at_amount_cap(
                arena,
                edges,
                tickless_high,
                tickless_high,
                start_decimals,
                start_rate,
                brent_shallow_caps.as_ref(),
            );
        }
        if high > tickless_high {
            high = tickless_high;
        }
        if high <= low {
            record_brent_reject(BrentOptimizeReject::ClCapBoundsEmpty);
            crate::trace!(
                "optimize_cycle: bounds empty after tickless CL cap low={low} high={high} cap={tickless_high}"
            );
            return None;
        }
    }

    // Capacity-based high is often far above any amount that still simulates
    // (ZeroOutput dominated Brent SimNone). Cap search to a small multiple of the
    // largest profitable probe seed so Brent stays in the feasible band; the
    // first_infeasible wall still catches residual overshoots.
    if let Some(seeds) = seed_sims
        && let Some(clamped) = clamp_brent_high_to_probe_seeds(high, low, economic_floor, seeds)
    {
        record_ternary_seed_high_clamp();
        record_brent_seed_high_clamp();
        high = clamped;
    }

    // Path-aware Balancer MAX_IN_RATIO clamp (after seed clamp). Soft FX caps still
    // overshoot when prior hops amplify vs oracle — binary-search real feasible high.
    // If capacity `low` already trips MAX_IN, shrink the window toward economic_floor.
    match balancer_max_in_high_clamp(arena, edges, low, high, economic_floor) {
        Some(BalancerHighClamp::WindowInfeasible) => {
            record_brent_reject(BrentOptimizeReject::BalancerBoundsEmpty);
            crate::trace!(
                "optimize_cycle: balancer MAX_IN_RATIO infeasible at low={low} high={high} floor={economic_floor}"
            );
            return None;
        }
        Some(BalancerHighClamp::Clamped {
            low: new_low,
            high: new_high,
        }) => {
            record_ternary_bal_high_clamp();
            low = new_low;
            high = new_high;
            if high <= low {
                record_brent_reject(BrentOptimizeReject::BalancerBoundsEmpty);
                return None;
            }
        }
        None => {}
    }

    // Seed/capacity highs still overshoot ticked CL depth → Brent SimNone=shallow.
    // Tighten high to the max amount that still walks; if floor is also shallow
    // (typical hop-2+ depth), abandon before Brent thrash.
    match clamp_brent_high_to_cl_feasible(
        arena,
        edges,
        low,
        high,
        economic_floor,
        brent_shallow_caps.as_ref(),
    ) {
        ClDepthHighClamp::Noop => {}
        ClDepthHighClamp::WindowInfeasible => {
            record_brent_reject(BrentOptimizeReject::ClCapBoundsEmpty);
            crate::trace!(
                "optimize_cycle: CL depth infeasible at floor low={low} high={high} floor={economic_floor}"
            );
            return None;
        }
        ClDepthHighClamp::Clamped(clamped) => {
            record_ternary_cl_depth_clamp();
            record_brent_cl_depth_clamp();
            high = clamped;
            if high <= low {
                record_brent_reject(BrentOptimizeReject::ClCapBoundsEmpty);
                return None;
            }
        }
    }

    let mut sim_cache: FxHashMap<U256, crate::pipeline::types::MinimalSimResult> =
        FxHashMap::default();
    if let Some(seeds) = seed_sims {
        for (amount, sim) in seeds {
            if sim_cache.len() >= BRENT_CACHE_SLOTS {
                break;
            }
            let mut seeded = *sim;
            if let Some(costing) = route_gas {
                seeded.total_gas = costing.lookup.route_gas_or_heuristic(
                    costing.oracle,
                    costing.fingerprint,
                    seeded.total_gas,
                );
            }
            sim_cache.insert(*amount, seeded);
        }
    }
    let mut warm_scores: Vec<(U256, U256)> = Vec::with_capacity(sim_cache.len());
    for (amount, sim) in &sim_cache {
        if *amount >= low && *amount <= high {
            warm_scores.push((
                *amount,
                brent_score_matic_from_sim(sim, *amount, profit_ctx),
            ));
        }
    }
    // First amount that failed to simulate. AMM depth failures are monotonic in
    // size — once A is infeasible, every B >= A is too. Without this wall Brent
    // re-sims the dead upper band thousands of times per tick (SimNone ~80%).
    let mut first_infeasible = high.saturating_add(U256::from(1u8));
    let evaluate = |amount: U256| -> U256 {
        if amount < economic_floor || amount >= first_infeasible {
            return U256::ZERO;
        }
        if let Some(sim) = sim_cache.get(&amount) {
            record_brent_cache_local();
            return brent_score_matic_from_sim(sim, amount, profit_ctx);
        }
        if let Some((cache, route_state_revision, route_fp)) = route_sim_cache
            && let Some(cached) = cache.get(route_state_revision, route_fp, amount)
        {
            record_brent_cache_route();
            if sim_cache.len() < BRENT_CACHE_SLOTS {
                sim_cache.insert(amount, cached);
            }
            return brent_score_matic_from_sim(&cached, amount, profit_ctx);
        }
        record_brent_eval_sim();
        match simulate_route_minimal_with_caps(arena, edges, amount, brent_shallow_caps.as_ref()) {
            Some(mut sim) => {
                if let Some(costing) = route_gas {
                    sim.total_gas = costing.lookup.route_gas_or_heuristic(
                        costing.oracle,
                        costing.fingerprint,
                        sim.total_gas,
                    );
                }
                if sim.profit.is_zero() {
                    record_brent_eval_reject(BrentEvalReject::ZeroProfit);
                    return U256::ZERO;
                }
                if check_sim_sanity_fast(SimSanityInput {
                    amount_in: amount,
                    gross_profit: sim.profit,
                    search_low: low,
                    token_decimals: start_decimals,
                    token_to_matic_rate: start_rate,
                })
                .is_err()
                {
                    record_brent_eval_reject(BrentEvalReject::Sanity);
                    return U256::ZERO;
                }
                let score = brent_score_matic_from_sim(&sim, amount, profit_ctx);
                if let Some((cache, route_state_revision, route_fp)) = route_sim_cache {
                    cache.insert(route_state_revision, route_fp, amount, sim);
                }
                if sim_cache.len() < BRENT_CACHE_SLOTS {
                    sim_cache.insert(amount, sim);
                }
                score
            }
            None => {
                first_infeasible = amount;
                record_brent_eval_reject(BrentEvalReject::SimNone);
                if should_sample_brent_sim_none() {
                    let failure = minimal_sim_failure(arena, edges, amount);
                    let kind = match failure {
                        Some(MinimalSimFailure::V2ReserveExhausted { .. }) => {
                            BrentSimNoneKind::V2Reserve
                        }
                        Some(MinimalSimFailure::ShallowCl { hop })
                        | Some(MinimalSimFailure::ClCapExceeded { hop }) => {
                            record_brent_shallow_hop(hop);
                            BrentSimNoneKind::ShallowCl
                        }
                        Some(MinimalSimFailure::ClTickless { .. }) => BrentSimNoneKind::ClTickless,
                        Some(MinimalSimFailure::BalancerMaxInRatio { hop }) => {
                            if let Some(edge) = edges.get(hop) {
                                record_brent_zero_out_protocol(edge.protocol);
                            }
                            BrentSimNoneKind::BalancerMaxIn
                        }
                        Some(MinimalSimFailure::ZeroOutput { hop, protocol }) => {
                            record_brent_zero_out_protocol(protocol);
                            if protocol == ProtocolType::BalancerV2 {
                                record_brent_bal_zero_other();
                            }
                            let _ = hop;
                            BrentSimNoneKind::ZeroOutput
                        }
                        Some(MinimalSimFailure::UnsupportedState { expected, .. }) => {
                            record_brent_unsupported_protocol(expected);
                            BrentSimNoneKind::Unsupported
                        }
                        Some(MinimalSimFailure::TokenMismatch { .. }) => {
                            BrentSimNoneKind::TokenMismatch
                        }
                        _ => BrentSimNoneKind::Other,
                    };
                    record_brent_sim_none_kind(kind);
                }
                U256::ZERO
            }
        }
    };

    let iterations = max_iterations.unwrap_or(DEFAULT_BRENT_ITERATIONS);
    let optimal = solve_brent_optimal_warm(low, high, evaluate, iterations, &warm_scores);
    if optimal < economic_floor {
        record_brent_reject(BrentOptimizeReject::BelowEconomicFloor);
        crate::trace!(
            "optimize_cycle: optimal={optimal} < economic_floor={economic_floor} low={low} high={high}"
        );
        return None;
    }
    let mut sim = sim_cache
        .get(&optimal)
        .copied()
        .or_else(|| {
            route_sim_cache.and_then(|(cache, route_state_revision, route_fp)| {
                cache.get(route_state_revision, route_fp, optimal)
            })
        })
        .or_else(|| {
            simulate_route_minimal_with_caps(arena, edges, optimal, brent_shallow_caps.as_ref())
        })?;
    if let Some(costing) = route_gas {
        sim.total_gas = costing.lookup.route_gas_or_heuristic(
            costing.oracle,
            costing.fingerprint,
            sim.total_gas,
        );
    }
    if sim.profit.is_zero() {
        record_brent_reject(BrentOptimizeReject::ZeroProfit);
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
        record_brent_reject(BrentOptimizeReject::SanityDispatch);
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
    record_brent_ok();
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

    #[test]
    fn warm_seed_survives_mid_window_zero_band() {
        // Evaluate returns 0 everywhere except a mid-window ladder seed.
        // Old solver golden-zero-exited and only compared endpoints → missed the seed.
        let seed = U256::from(40u8);
        let warm = [(seed, U256::from(9_000u64))];
        let optimal = solve_brent_optimal_warm(
            U256::from(1u8),
            U256::from(100u8),
            |x| {
                if x == seed {
                    U256::from(9_000u64)
                } else {
                    U256::ZERO
                }
            },
            16,
            &warm,
        );
        assert_eq!(optimal, seed);
    }

    #[test]
    fn balancer_max_in_high_clamp_noop_without_balancer() {
        // Empty edge list → no Balancer hops → no clamp.
        assert!(balancer_max_in_high_clamp(
            &StateArena::default(),
            &[],
            U256::from(1u8),
            U256::from(100u8),
            U256::from(1u8),
        )
        .is_none());
    }

    #[test]
    fn clamp_brent_high_to_cl_feasible_noop_without_cl() {
        assert_eq!(
            clamp_brent_high_to_cl_feasible(
                &StateArena::default(),
                &[],
                U256::from(1u8),
                U256::from(100u8),
                U256::from(1u8),
                None,
            ),
            ClDepthHighClamp::Noop
        );
    }

    #[test]
    fn clamp_brent_high_uses_eight_x_profitable_probe_seed() {
        use crate::pipeline::types::MinimalSimResult;
        let seed = U256::from(1_000u64);
        let seeds = [(
            seed,
            MinimalSimResult {
                profit: U256::from(1u8),
                amount_out: seed + U256::from(1u8),
                total_gas: 100_000,
            },
        )];
        let high = U256::from(1_000_000u64);
        let low = U256::from(1u8);
        let floor = U256::from(10u8);
        let clamped = clamp_brent_high_to_probe_seeds(high, low, floor, &seeds).unwrap();
        assert_eq!(clamped, seed * U256::from(8u8));
        // Zero-profit seeds do not clamp.
        let dust = [(
            seed,
            MinimalSimResult {
                profit: U256::ZERO,
                amount_out: seed,
                total_gas: 100_000,
            },
        )];
        assert!(clamp_brent_high_to_probe_seeds(high, low, floor, &dust).is_none());
        // Already-tight high is left alone.
        assert!(clamp_brent_high_to_probe_seeds(U256::from(8_000u64), low, floor, &seeds).is_none());
    }

    #[test]
    fn warm_seed_reduces_evaluate_calls() {
        let peak = U256::from(50u8);
        let mut cold = 0u32;
        let _ = solve_brent_optimal(
            U256::ZERO,
            U256::from(100u8),
            |x| {
                cold += 1;
                peaked_at(x, 50)
            },
            8,
        );
        let mut hot = 0u32;
        let _ = solve_brent_optimal_warm(
            U256::ZERO,
            U256::from(100u8),
            |x| {
                hot += 1;
                peaked_at(x, 50)
            },
            8,
            &[(peak, peaked_at(peak, 50))],
        );
        assert!(hot <= cold, "warm={hot} cold={cold}");
    }
}
