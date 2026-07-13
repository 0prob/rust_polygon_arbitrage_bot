use crate::core::constants::BPS_SCALE;
use crate::core::math::fixed_point::{ONE, ONE_U512};
use crate::core::math::uniswap_v2::simulate_v2_swap;
use crate::core::math::uniswap_v3::simulate_v3_swap;
use crate::core::types::{
    ConcentratedLiquidityPoolState, Edge, FlashLoanSource, FoundCycle, PoolState, ProtocolType,
    TokenIndex,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_finder::clamp_fee_bps;
use crate::pipeline::local_sim::simulate_hop_amount_out_with_cap;
use crate::pipeline::sim_sanity::min_economic_amount_in;
use crate::pipeline::types::RoutingGraph;
use crate::services::execution::profit::flash_loan_fee_bps;
use crate::util::{ten_pow_u256_cached, u256_to_f64, u512_to_u256_checked};
use alloy::primitives::{Address, U256, U512};
use rustc_hash::FxHashMap;
use std::sync::Arc;

const ONE_F64: f64 = 1_000_000_000_000_000_000.0;

/// Decimal-aware marginal probe: ~10^(decimals−6) wei, clamped to [0.001, 0.01] token units.
#[must_use]
pub fn spot_probe_for_decimals(decimals: u8) -> U256 {
    let scale = ten_pow_u256_cached(decimals);
    let exp_probe = if decimals >= 6 {
        ten_pow_u256_cached(decimals - 6)
    } else {
        U256::from(1u64)
    };
    let dust_floor = scale / U256::from(1000u64);
    let cap = scale / U256::from(100u64);
    exp_probe.max(dust_floor).max(U256::from(1000u64)).min(cap)
}

#[must_use]
pub fn spot_probe_for_token(arena: &StateArena, token: TokenIndex) -> U256 {
    spot_probe_for_decimals(arena.token_decimals(token))
}

/// Multiply two fixed-point ratios (`ONE` = 1e18). Returns `None` on U256 overflow.
#[inline]
#[must_use]
pub fn mul_ratio(a: U256, b: U256) -> Option<U256> {
    if a.is_zero() || b.is_zero() {
        return Some(U256::ZERO);
    }
    let product = U512::from(a).checked_mul(U512::from(b))? / ONE_U512;
    u512_to_u256_checked(product)
}

/// Chained ratio product; saturates to `U256::MAX` on overflow (profitable sentinel).
#[inline]
#[must_use]
pub fn mul_ratio_saturating(a: U256, b: U256) -> U256 {
    mul_ratio(a, b).unwrap_or(U256::MAX)
}

/// True when `current * edge_ratio / ONE` strictly improves `best` (overflow ⇒ true).
#[inline]
#[must_use]
pub fn ratio_product_improves(current: U256, edge_ratio: U256, best: U256) -> bool {
    match mul_ratio(current, edge_ratio) {
        Some(r) => r > best,
        None => true,
    }
}

#[inline]
fn simulated_edge_ratio(out: U256, probe: U256) -> Option<U256> {
    if out.is_zero() || probe.is_zero() {
        return None;
    }
    out.checked_mul(ONE)?.checked_div(probe)
}

/// V2 edge ratio via constant-product simulation at `probe` (fee-inclusive).
#[inline]
fn v2_edge_ratio_u256(
    state: &crate::core::types::V2PoolState,
    edge: &Edge,
    probe: U256,
) -> Option<U256> {
    let out = simulate_v2_swap(state, probe, edge.zero_for_one, Some(edge.fee_bps));
    simulated_edge_ratio(out, probe)
}

#[inline]
fn cl_has_ticks(state: &ConcentratedLiquidityPoolState) -> bool {
    !state.ticks.is_empty()
}

/// CL edge ratio via probe simulation (tickless pools use shallow sim, not spot).
#[inline]
fn cl_edge_ratio_u256(
    state: &ConcentratedLiquidityPoolState,
    edge: &Edge,
    probe: U256,
) -> Option<U256> {
    if probe.is_zero() {
        return cl_spot_u256(state, edge);
    }
    let r = simulate_v3_swap(state, probe, edge.zero_for_one, Some(edge.fee_bps));
    simulated_edge_ratio(r.amount_out, probe)
}

/// Compute CL V3/V4 spot price ratio as U256 fixed-point (tickless fallback).
#[inline]
fn cl_spot_u256(state: &ConcentratedLiquidityPoolState, edge: &Edge) -> Option<U256> {
    let sqrt = state.sqrt_price_x96;
    if sqrt.is_zero() {
        return None;
    }
    let spot_u256 = if edge.zero_for_one {
        let sqrt_hi: U256 = sqrt >> 96;
        let sqrt_lo: U256 = sqrt & ((U256::from(1u128) << 96) - U256::from(1));
        let hi_term = sqrt_hi.checked_mul(sqrt_hi)?.checked_mul(ONE)?;
        let cross = U512::from(sqrt_hi) * U512::from(sqrt_lo) * ONE_U512;
        let cross_term = crate::util::u512_to_u256(cross >> 95);
        let lo_sq = U512::from(sqrt_lo) * U512::from(sqrt_lo);
        let lo_numer = lo_sq * ONE_U512;
        let lo_term = crate::util::u512_to_u256(lo_numer >> 192);
        hi_term.checked_add(cross_term)?.checked_add(lo_term)?
    } else {
        let two_pow_192 = U256::from(1u128) << 192;
        let numerator = U512::from(two_pow_192) * ONE_U512;
        let sqrt_sq = U512::from(sqrt) * U512::from(sqrt);
        let raw = numerator / sqrt_sq;
        if raw.is_zero() {
            return None;
        }
        crate::util::u512_to_u256(raw)
    };
    let fee_numer = U256::from(10000u64 - u64::from(clamp_fee_bps(edge.fee_bps)));
    spot_u256
        .checked_mul(fee_numer)
        .map(|v| v / U256::from(10000u64))
}

#[inline]
fn spot_ratio_to_f64(ratio: Option<U256>) -> f64 {
    match ratio {
        Some(r) if !r.is_zero() => u256_to_f64(r) / ONE_F64,
        _ => 0.0,
    }
}

const HOP_PENALTIES: [f64; 9] = [0.0, 0.0, 0.0, 0.01, 0.03, 0.08, 0.15, 0.30, 0.50];

/// Minimum `cycle_ratio` for `hops` to clear per-hop fee + gas drag at graph-search margin.
/// Two-hop cycles stay permissive (probe-rank applies gas); longer routes prune earlier.
#[must_use]
pub fn min_profitable_cycle_ratio(hops: u32) -> U256 {
    if hops <= 2 {
        return ONE.saturating_add(U256::from(1u8));
    }
    // Base 20 bps + 38 bps per hop above 2 (executor + flash drag grows with depth).
    let extra = hops.saturating_sub(2);
    let drag_bps = 20u64 + u64::from(extra) * 38;
    ONE.saturating_add(ONE * U256::from(drag_bps) / U256::from(10_000u64))
}

#[must_use]
pub fn hop_penalty(hops: u32) -> f64 {
    HOP_PENALTIES
        .get(hops as usize)
        .copied()
        .unwrap_or(hops as f64 * 0.15)
}

#[must_use]
pub fn compute_edge_log_weight(fee_bps: u32) -> f64 {
    (fee_bps as f64 / 10_000.0).ln_1p()
}

#[derive(Debug, Clone, Default)]
pub struct SpotTable {
    values: rustc_hash::FxHashMap<u64, f64>,
}

impl SpotTable {
    #[inline]
    fn key(edge: &Edge) -> u64 {
        crate::pipeline::cycle_filter::edge_hop_key(edge)
    }

    #[must_use]
    pub fn new(pool_count: usize) -> Self {
        Self::with_capacity(pool_count.saturating_mul(4))
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: rustc_hash::FxHashMap::with_capacity_and_hasher(
                capacity,
                rustc_hash::FxBuildHasher,
            ),
        }
    }

    /// Pre-fill spot prices from graph edge ratios (avoids re-simulating at rescore time).
    pub fn populate_from_graph(&mut self, graph: &RoutingGraph) {
        use crate::pipeline::types::GraphHopPhase;
        for adj in &graph.adjacency {
            for ge in adj {
                if ge.phase != GraphHopPhase::Direct || ge.ratio.is_zero() {
                    continue;
                }
                let spot = u256_to_f64(ge.ratio) / ONE_F64;
                self.set(&ge.edge, spot);
            }
        }
    }

    #[must_use]
    pub fn get(&self, edge: &Edge) -> f64 {
        self.values.get(&Self::key(edge)).copied().unwrap_or(0.0)
    }

    #[must_use]
    pub fn get_opt(&self, edge: &Edge) -> Option<f64> {
        self.values.get(&Self::key(edge)).copied()
    }

    pub fn set(&mut self, edge: &Edge, spot: f64) {
        self.values.insert(Self::key(edge), spot);
    }
}

#[inline]
fn v2_marginal_spot(state: &crate::core::types::V2PoolState, edge: &Edge, probe: U256) -> f64 {
    spot_ratio_to_f64(v2_edge_ratio_u256(state, edge, probe))
}

#[inline]
fn cl_marginal_spot(state: &ConcentratedLiquidityPoolState, edge: &Edge) -> f64 {
    match cl_spot_u256(state, edge) {
        Some(ratio) if !ratio.is_zero() => u256_to_f64(ratio) / ONE_F64,
        _ => 0.0,
    }
}

#[must_use]
pub fn edge_log_weight_from_spot(spot_price: f64, fee_bps: u32) -> f64 {
    if spot_price <= 0.0 || !spot_price.is_finite() {
        return compute_edge_log_weight(fee_bps);
    }
    -spot_price.ln()
}

#[must_use]
pub fn compute_spot_price(arena: &StateArena, edge: &Edge) -> f64 {
    let ratio = compute_edge_ratio(arena, edge);
    if ratio.is_zero() {
        0.0
    } else {
        u256_to_f64(ratio) / ONE_F64
    }
}

#[must_use]
pub fn spot_price_from_state(state: &PoolState, edge: &Edge, token_in_decimals: u8) -> f64 {
    let probe = spot_probe_for_decimals(token_in_decimals);
    let shallow_cap = probe;
    match (state, edge.protocol) {
        (PoolState::V2(s), ProtocolType::UniswapV2) => v2_marginal_spot(s, edge, probe),
        (PoolState::V3(s), ProtocolType::UniswapV3)
        | (PoolState::V4(s), ProtocolType::UniswapV4) => {
            if cl_has_ticks(s) {
                spot_ratio_to_f64(cl_edge_ratio_u256(s, edge, probe))
            } else {
                cl_marginal_spot(s, edge)
            }
        }
        _ => {
            let out = match simulate_hop_amount_out_with_cap(state, edge, probe, shallow_cap) {
                Some(v) if !v.is_zero() => v,
                _ => return 0.0,
            };
            let p = u256_to_f64(probe);
            if p <= 0.0 { 0.0 } else { u256_to_f64(out) / p }
        }
    }
}

#[must_use]
pub fn compute_edge_ratio(arena: &StateArena, edge: &Edge) -> U256 {
    let Some(state) = arena.pool_state(edge.pool_index) else {
        return U256::ZERO;
    };
    if !state.is_tradable() {
        return U256::ZERO;
    }
    let probe = spot_probe_for_token(arena, edge.token_in);
    let shallow_cap = probe;
    match (state, edge.protocol) {
        (PoolState::V2(s), ProtocolType::UniswapV2) => {
            v2_edge_ratio_u256(s, edge, probe).unwrap_or(U256::ZERO)
        }
        (PoolState::V3(s), ProtocolType::UniswapV3)
        | (PoolState::V4(s), ProtocolType::UniswapV4) => {
            cl_edge_ratio_u256(s, edge, probe).unwrap_or(U256::ZERO)
        }
        _ => {
            let out = simulate_hop_amount_out_with_cap(state, edge, probe, shallow_cap)
                .unwrap_or(U256::ZERO);
            simulated_edge_ratio(out, probe).unwrap_or(U256::ZERO)
        }
    }
}

#[must_use]
pub fn gas_log_penalty_for_cycle(
    edges: &[Edge],
    gas_price_wei: U256,
    token_to_matic_rates: Option<&FxHashMap<TokenIndex, U256>>,
    start_token: TokenIndex,
    start_token_decimals: u8,
    flash_source: Option<FlashLoanSource>,
) -> f64 {
    let gas_units = crate::pipeline::local_sim::estimate_route_gas(edges);
    if gas_units == 0 || gas_price_wei.is_zero() {
        return 0.0;
    }
    let gas_cost_wei = U256::from(gas_units) * gas_price_wei;
    let Some(rate) = token_to_matic_rates
        .and_then(|m| m.get(&start_token).copied())
        .filter(|r| *r >= crate::core::constants::MIN_TOKEN_TO_MATIC_RATE)
    else {
        return f64::INFINITY;
    };
    let mut drag_wei = gas_cost_wei;
    if let Some(source) = flash_source {
        let probe = min_economic_amount_in(start_token_decimals, rate);
        let flash_fee = (probe * U256::from(flash_loan_fee_bps(source))) / BPS_SCALE;
        let scale = crate::util::ten_pow_u256(start_token_decimals);
        drag_wei = drag_wei.saturating_add((flash_fee * rate) / scale);
    }
    let drag_f64 = u256_to_f64(drag_wei);
    let rate_f64 = u256_to_f64(rate);
    if drag_f64 <= 0.0 || rate_f64 <= 0.0 {
        return 0.0;
    }
    (drag_f64 / rate_f64).ln_1p()
}

pub fn rescore_cycles_with_table(
    arena: &StateArena,
    table: &mut SpotTable,
    cycles: &mut [FoundCycle],
) {
    rescore_cycles_with_table_and_gas(arena, table, cycles, None, None, None, None);
}

#[allow(clippy::too_many_arguments)]
fn rescore_one_cycle(
    arena: &StateArena,
    table: &mut SpotTable,
    cycle: &mut FoundCycle,
    gas_price_wei: Option<U256>,
    token_to_matic_rates: Option<&FxHashMap<TokenIndex, U256>>,
    token_decimals: Option<&FxHashMap<Address, u8>>,
    flash_source: Option<FlashLoanSource>,
) {
    let edge_hops = u32::try_from(cycle.edges.len()).unwrap_or(cycle.hop_count);
    if edge_hops != cycle.hop_count {
        cycle.hop_count = edge_hops;
    }
    let start_decimals = token_decimals.map_or(18, |m| {
        crate::services::oracle::resolve_token_decimals_for_index(cycle.start_token, arena, m)
    });
    let mut log_weight = 0.0;
    let mut cum_fee = 0u32;
    let mut missing_spot = 0u32;
    for edge in &cycle.edges {
        cum_fee = cum_fee.saturating_add(clamp_fee_bps(edge.fee_bps));
        let Some(state) = arena.pool_state(edge.pool_index) else {
            log_weight = crate::pipeline::cycle_finder::DEAD_EDGE_LOG_WEIGHT;
            break;
        };
        if !state.hop_pair_routable(edge.token_in_idx as usize, edge.token_out_idx as usize) {
            log_weight = crate::pipeline::cycle_finder::DEAD_EDGE_LOG_WEIGHT;
            break;
        }
        let tin_decimals = match token_decimals {
            Some(m) => {
                crate::services::oracle::resolve_token_decimals_for_index(edge.token_in, arena, m)
            }
            None => arena.token_decimals(edge.token_in),
        };
        let spot = match table.get_opt(edge) {
            Some(v) => v,
            None => {
                let val = spot_price_from_state(state, edge, tin_decimals);
                table.set(edge, val);
                val
            }
        };
        if spot <= 0.0 {
            missing_spot += 1;
            log_weight += compute_edge_log_weight(edge.fee_bps);
        } else {
            log_weight += edge_log_weight_from_spot(spot, edge.fee_bps);
        }
    }
    let missing_penalty = if missing_spot > 0 {
        (missing_spot.min(5) * 2) as f64
    } else {
        0.0
    };
    let gas_penalty = gas_price_wei.filter(|p| !p.is_zero()).map_or(0.0, |price| {
        gas_log_penalty_for_cycle(
            &cycle.edges,
            price,
            token_to_matic_rates,
            cycle.start_token,
            start_decimals,
            flash_source,
        )
    });
    log_weight += hop_penalty(cycle.hop_count) + missing_penalty + gas_penalty;
    cycle.log_weight = log_weight;
    cycle.score = log_weight;
    cycle.cumulative_fee_bps = cum_fee;
}

pub fn rescore_cycles_with_table_and_gas(
    arena: &StateArena,
    table: &mut SpotTable,
    cycles: &mut [FoundCycle],
    gas_price_wei: Option<U256>,
    token_to_matic_rates: Option<&FxHashMap<TokenIndex, U256>>,
    token_decimals: Option<&FxHashMap<Address, u8>>,
    flash_source: Option<FlashLoanSource>,
) {
    for cycle in cycles.iter_mut() {
        rescore_one_cycle(
            arena,
            table,
            cycle,
            gas_price_wei,
            token_to_matic_rates,
            token_decimals,
            flash_source,
        );
    }
}

pub fn rescore_arc_cycles_with_table_and_gas(
    arena: &StateArena,
    table: &mut SpotTable,
    cycles: &mut [Arc<FoundCycle>],
    gas_price_wei: Option<U256>,
    token_to_matic_rates: Option<&FxHashMap<TokenIndex, U256>>,
    token_decimals: Option<&FxHashMap<Address, u8>>,
    flash_source: Option<FlashLoanSource>,
) {
    for cycle in cycles {
        rescore_one_cycle(
            arena,
            table,
            Arc::make_mut(cycle),
            gas_price_wei,
            token_to_matic_rates,
            token_decimals,
            flash_source,
        );
    }
}

#[must_use]
pub fn finalize_enumerated_cycles(cycles: Vec<FoundCycle>, max_cycles: usize) -> Vec<FoundCycle> {
    crate::pipeline::cycle_finder::apply_protocol_diverse_selection(cycles, max_cycles)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::PoolIndex;

    fn edge_with_indices(pool: u32, tin: u8, tout: u8, zero_for_one: bool) -> Edge {
        Edge {
            pool_index: PoolIndex(pool),
            token_in: TokenIndex(tin as u32),
            token_out: TokenIndex(tout as u32),
            token_in_idx: tin,
            token_out_idx: tout,
            protocol: ProtocolType::BalancerV2,
            fee_bps: 10,
            zero_for_one,
        }
    }

    #[test]
    fn spot_probe_scales_with_decimals() {
        let six = spot_probe_for_decimals(6);
        let eighteen = spot_probe_for_decimals(18);
        assert_eq!(six, U256::from(1_000u64));
        assert_eq!(eighteen, U256::from(10u128.pow(15)));
        assert!(six < eighteen);
    }

    #[test]
    fn min_profitable_cycle_ratio_scales_with_hop_depth() {
        let two = min_profitable_cycle_ratio(2);
        let four = min_profitable_cycle_ratio(4);
        let six = min_profitable_cycle_ratio(6);
        assert!(two < four);
        assert!(four < six);
        // 4-hop: 20 + 2*38 = 96 bps over ONE
        assert_eq!(four, ONE + ONE * U256::from(96u64) / U256::from(10_000u64));
    }

    #[test]
    fn mul_ratio_detects_overflow() {
        let huge = U256::MAX / U256::from(2u64);
        assert!(mul_ratio(huge, U256::MAX).is_none());
        assert_eq!(mul_ratio_saturating(huge, U256::MAX), U256::MAX);
    }

    #[test]
    fn spot_table_keys_distinguish_multi_token_directions() {
        let mut table = SpotTable::new(1);
        let ab = edge_with_indices(0, 0, 1, true);
        let ac = edge_with_indices(0, 0, 2, true);
        table.set(&ab, 1.5);
        table.set(&ac, 2.5);
        assert_eq!(table.get(&ab), 1.5);
        assert_eq!(table.get(&ac), 2.5);
    }

    #[test]
    fn spot_table_pair_directions_remain_distinct() {
        let mut table = SpotTable::new(1);
        let forward = edge_with_indices(0, 0, 1, true);
        let reverse = edge_with_indices(0, 1, 0, false);
        table.set(&forward, 3.0);
        table.set(&reverse, 4.0);
        assert_eq!(table.get(&forward), 3.0);
        assert_eq!(table.get(&reverse), 4.0);
    }
}
