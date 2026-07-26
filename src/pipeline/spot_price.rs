use crate::core::math::fixed_point::{ONE, ONE_U512, edge_log_weight_from_ratio};
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
use crate::util::{ten_pow_u256_cached, u256_to_f64, u512_to_u256_checked};
use alloy::primitives::{Address, U256, U512};
use rayon::prelude::*;
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

/// Micro / spot / economic / 0.1 / 1.0 token ladder for ranking / Brent warm seeds.
/// Visits in **ascending** size order so thin V2 pools are probed at dust before
/// 0.1–1.0 token (avoids false `V2ReserveExhausted` attribution and wasted sims).
/// Micro (`10^(decimals-6)` when below spot) recovers pools that exhaust at spot=0.001.
pub fn for_each_rank_probe_amount(decimals: u8, rate: U256, mut visit: impl FnMut(U256)) {
    let economic = min_economic_amount_in(decimals, rate);
    let spot = spot_probe_for_decimals(decimals);
    let scale = ten_pow_u256_cached(decimals);
    let micro = if decimals >= 6 {
        ten_pow_u256_cached(decimals - 6)
    } else {
        U256::from(1u64)
    };
    let tenth = scale / U256::from(10u8);
    let one = scale;
    let mut amounts = [U256::ZERO; 5];
    let mut n = 0usize;
    for candidate in [micro, spot, economic, tenth, one] {
        if candidate.is_zero() {
            continue;
        }
        if (0..n).any(|i| amounts[i] == candidate) {
            continue;
        }
        amounts[n] = candidate;
        n += 1;
    }
    amounts[..n].sort_unstable();
    for amount in amounts.iter().take(n) {
        visit(*amount);
    }
}

#[inline]
fn cl_edge_ratio_from_state(
    state: &ConcentratedLiquidityPoolState,
    edge: &Edge,
    probe: U256,
) -> U256 {
    if cl_has_ticks(state) {
        cl_edge_ratio_u256(state, edge, probe).unwrap_or(U256::ZERO)
    } else {
        cl_spot_u256(state, edge).unwrap_or(U256::ZERO)
    }
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
    let num = U512::from(out).checked_mul(U512::from(ONE))?;
    u512_to_u256_checked(num / U512::from(probe))
}

/// Chained fixed-point edge ratio product for a closed route (`ONE` = no net change).
#[must_use]
pub fn cycle_product_ratio(arena: &StateArena, edges: &[Edge]) -> U256 {
    let mut product = ONE;
    for edge in edges {
        let ratio = compute_edge_ratio(arena, edge);
        if ratio.is_zero() {
            return U256::ZERO;
        }
        product = mul_ratio_saturating(product, ratio);
    }
    product
}

/// V2 edge ratio via constant-product simulation at `probe` (fee-inclusive).
#[inline]
fn v2_marginal_probe(probe: U256, reserve_in: U256) -> Option<U256> {
    if reserve_in <= U256::ONE {
        return None;
    }
    let cap = reserve_in - U256::ONE;
    let amount = probe.min(cap);
    (!amount.is_zero()).then_some(amount)
}

fn v2_edge_ratio_u256(
    state: &crate::core::types::V2PoolState,
    edge: &Edge,
    probe: U256,
) -> Option<U256> {
    let reserve_in = if edge.zero_for_one {
        state.reserve0
    } else {
        state.reserve1
    };
    let amount = v2_marginal_probe(probe, reserve_in)?;
    let out = simulate_v2_swap(state, amount, edge.zero_for_one, Some(edge.fee_bps));
    simulated_edge_ratio(out, amount)
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
    let allow_zero = edge.protocol == ProtocolType::UniswapV4;
    let r = simulate_v3_swap(
        state,
        probe,
        edge.zero_for_one,
        Some(edge.fee_bps),
        allow_zero,
    );
    if r.shallow {
        return None;
    }
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

/// Fee-only fallback weight when spot ratio is missing/non-finite.
/// Assumes neutral spot=1 after fee → `-ln(1 - fee)` (not `ln(1 + fee)`).
#[must_use]
pub fn compute_edge_log_weight(fee_bps: u32) -> f64 {
    if fee_bps == 0 {
        return 0.0;
    }
    if fee_bps >= 10_000 {
        return f64::INFINITY;
    }
    let keep = 1.0 - (fee_bps as f64 / 10_000.0);
    -keep.ln()
}

#[derive(Debug, Clone, Default)]
pub struct SpotTable {
    ratios: rustc_hash::FxHashMap<u64, U256>,
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
            ratios: rustc_hash::FxHashMap::with_capacity_and_hasher(
                capacity,
                rustc_hash::FxBuildHasher,
            ),
        }
    }

    /// Pre-fill marginal ratios from graph build (avoids re-simulating at rescore time).
    pub fn populate_from_graph(&mut self, graph: &RoutingGraph) {
        use crate::pipeline::types::GraphHopPhase;
        for adj in &graph.adjacency {
            for ge in adj {
                if ge.phase != GraphHopPhase::Direct || ge.ratio.is_zero() {
                    continue;
                }
                self.set_ratio(&ge.edge, ge.ratio);
            }
        }
    }

    #[must_use]
    pub fn get_ratio(&self, edge: &Edge) -> Option<U256> {
        self.ratios.get(&Self::key(edge)).copied()
    }

    pub fn set_ratio(&mut self, edge: &Edge, ratio: U256) {
        if !ratio.is_zero() {
            self.ratios.insert(Self::key(edge), ratio);
        }
    }

    /// Marginal spot (output per input token) derived from the cached fixed-point ratio.
    #[must_use]
    pub fn get(&self, edge: &Edge) -> f64 {
        self.get_ratio(edge)
            .filter(|r| !r.is_zero())
            .map(|r| u256_to_f64(r) / ONE_F64)
            .unwrap_or(0.0)
    }

    #[must_use]
    pub fn get_opt(&self, edge: &Edge) -> Option<f64> {
        let spot = self.get(edge);
        (spot > 0.0).then_some(spot)
    }

    pub fn get_or_compute_ratio(&mut self, arena: &StateArena, edge: &Edge) -> U256 {
        if let Some(r) = self.get_ratio(edge) {
            return r;
        }
        let r = compute_edge_ratio(arena, edge);
        self.set_ratio(edge, r);
        r
    }

    /// Read-only ratio: table hit, else live compute (no insert — safe for parallel rescore).
    #[inline]
    #[must_use]
    pub fn ratio_or_compute(&self, arena: &StateArena, edge: &Edge) -> U256 {
        self.get_ratio(edge)
            .filter(|r| !r.is_zero())
            .unwrap_or_else(|| compute_edge_ratio(arena, edge))
    }
}

#[must_use]
pub fn edge_log_weight_from_spot(spot_price: f64, fee_bps: u32) -> f64 {
    if spot_price <= 0.0 || !spot_price.is_finite() {
        return compute_edge_log_weight(fee_bps);
    }
    -spot_price.ln()
}

/// Marginal output/input ratio at `probe` (fee-inclusive where applicable).
#[must_use]
pub fn edge_ratio_from_state(state: &PoolState, edge: &Edge, probe: U256) -> U256 {
    if probe.is_zero() || !state.is_tradable() {
        return U256::ZERO;
    }
    let shallow_cap = probe;
    match (state, edge.protocol) {
        (PoolState::V2(s), ProtocolType::UniswapV2) => {
            v2_edge_ratio_u256(s, edge, probe).unwrap_or(U256::ZERO)
        }
        (PoolState::V3(s), ProtocolType::UniswapV3)
        | (PoolState::V4(s), ProtocolType::UniswapV4) => cl_edge_ratio_from_state(s, edge, probe),
        _ => {
            let out = simulate_hop_amount_out_with_cap(state, edge, probe, shallow_cap)
                .unwrap_or(U256::ZERO);
            simulated_edge_ratio(out, probe).unwrap_or(U256::ZERO)
        }
    }
}

#[must_use]
pub fn compute_spot_price(arena: &StateArena, edge: &Edge) -> f64 {
    spot_ratio_to_f64(Some(compute_edge_ratio(arena, edge)))
}

#[must_use]
pub fn spot_price_from_state(state: &PoolState, edge: &Edge, token_in_decimals: u8) -> f64 {
    let probe = spot_probe_for_decimals(token_in_decimals);
    spot_ratio_to_f64(Some(edge_ratio_from_state(state, edge, probe)))
}

#[must_use]
pub fn compute_edge_ratio(arena: &StateArena, edge: &Edge) -> U256 {
    let Some(state) = arena.pool_state(edge.pool_index) else {
        return U256::ZERO;
    };
    let probe = spot_probe_for_token(arena, edge.token_in);
    edge_ratio_from_state(state, edge, probe)
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
        let flash_fee = crate::services::execution::profit::flash_loan_fee_amount(source, probe)
            .unwrap_or(U256::ZERO);
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

pub fn rescore_cycles_with_table(arena: &StateArena, table: &SpotTable, cycles: &mut [FoundCycle]) {
    rescore_cycles_with_table_and_gas(arena, table, cycles, None, None, None, None);
}

#[allow(clippy::too_many_arguments)]
fn rescore_one_cycle(
    arena: &StateArena,
    table: &SpotTable,
    cycle: &mut FoundCycle,
    gas_price_wei: Option<U256>,
    token_to_matic_rates: Option<&FxHashMap<TokenIndex, U256>>,
    token_decimals: Option<&FxHashMap<Address, u8>>,
    flash_source: Option<FlashLoanSource>,
) {
    let edge_hops = cycle.edge_hops();
    cycle.hop_count = edge_hops;
    let start_decimals = token_decimals.map_or(18, |m| {
        crate::services::oracle::resolve_token_decimals_for_index(cycle.start_token, arena, m)
    });
    let mut log_weight = 0.0;
    let mut cum_fee = 0u32;
    let mut product_ratio = ONE;
    let mut dead = false;
    for edge in &cycle.edges {
        cum_fee = cum_fee.saturating_add(clamp_fee_bps(edge.fee_bps));
        let Some(state) = arena.pool_state(edge.pool_index) else {
            dead = true;
            break;
        };
        if !state.hop_pair_routable(edge.token_in_idx as usize, edge.token_out_idx as usize) {
            dead = true;
            break;
        }
        // Read-only table lookup — parallel-safe after `populate_from_graph`.
        let ratio = table.ratio_or_compute(arena, edge);
        if ratio.is_zero() {
            dead = true;
            break;
        }
        product_ratio = mul_ratio_saturating(product_ratio, ratio);
        let mut hop_log = edge_log_weight_from_ratio(ratio);
        if !hop_log.is_finite() {
            hop_log = compute_edge_log_weight(edge.fee_bps);
        }
        log_weight += hop_log;
    }
    if dead {
        log_weight = crate::pipeline::cycle_finder::DEAD_EDGE_LOG_WEIGHT;
        product_ratio = U256::ZERO;
    }
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
    log_weight += hop_penalty(edge_hops) + gas_penalty;
    cycle.log_weight = log_weight;
    cycle.score = log_weight;
    cycle.cumulative_fee_bps = cum_fee;
    cycle.cycle_ratio = product_ratio;
}

pub fn rescore_cycles_with_table_and_gas(
    arena: &StateArena,
    table: &SpotTable,
    cycles: &mut [FoundCycle],
    gas_price_wei: Option<U256>,
    token_to_matic_rates: Option<&FxHashMap<TokenIndex, U256>>,
    token_decimals: Option<&FxHashMap<Address, u8>>,
    flash_source: Option<FlashLoanSource>,
) {
    // Table is read-only (callers pre-fill via `populate_from_graph`) — parallel-safe.
    if crate::util::should_use_rayon(cycles.len()) {
        cycles.par_iter_mut().for_each(|cycle| {
            rescore_one_cycle(
                arena,
                table,
                cycle,
                gas_price_wei,
                token_to_matic_rates,
                token_decimals,
                flash_source,
            );
        });
    } else {
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
}

pub fn rescore_arc_cycles_with_table_and_gas(
    arena: &StateArena,
    table: &SpotTable,
    cycles: &mut [Arc<FoundCycle>],
    gas_price_wei: Option<U256>,
    token_to_matic_rates: Option<&FxHashMap<TokenIndex, U256>>,
    token_decimals: Option<&FxHashMap<Address, u8>>,
    flash_source: Option<FlashLoanSource>,
) {
    if crate::util::should_use_rayon(cycles.len()) {
        cycles.par_iter_mut().for_each(|cycle| {
            rescore_one_cycle(
                arena,
                table,
                Arc::make_mut(cycle),
                gas_price_wei,
                token_to_matic_rates,
                token_decimals,
                flash_source,
            );
        });
    } else {
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
}

#[must_use]
pub fn finalize_enumerated_cycles(cycles: Vec<FoundCycle>, max_cycles: usize) -> Vec<FoundCycle> {
    crate::pipeline::cycle_finder::apply_protocol_diverse_selection(cycles, max_cycles)
}

#[cfg(test)]
#[allow(clippy::panic, clippy::manual_let_else)]
mod tests {
    use super::*;
    use crate::core::types::PoolIndex;

    #[test]
    fn fee_only_log_weight_is_minus_ln_one_minus_fee() {
        let w = compute_edge_log_weight(30);
        let expected = -(1.0 - 0.003_f64).ln();
        assert!((w - expected).abs() < 1e-12, "w={w} expected={expected}");
        assert!(compute_edge_log_weight(0).abs() < 1e-15);
        assert!(compute_edge_log_weight(10_000).is_infinite());
    }

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
    fn tickless_cl_edge_ratio_uses_sqrt_spot_not_shallow_probe_sim() {
        use crate::core::types::V3PoolState;
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from([]),
            })),
        );
        let edge = Edge {
            pool_index: pool,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: true,
        };
        let ratio = compute_edge_ratio(&arena, &edge);
        assert!(ratio > U256::ZERO);
        assert_eq!(
            ratio,
            cl_edge_ratio_from_state(
                match arena.pool_state(pool).expect("pool") {
                    PoolState::V3(s) => s,
                    _ => panic!("expected v3"),
                },
                &edge,
                spot_probe_for_token(&arena, t0),
            )
        );
    }

    #[test]
    fn spot_table_populate_from_graph_round_trips_ratio() {
        use crate::pipeline::types::{GraphEdge, GraphHopPhase, RoutingGraph};

        let edge = edge_with_indices(0, 0, 1, true);
        let ratio = ONE + ONE / U256::from(50u64);
        let mut graph = RoutingGraph::default();
        graph.push_edge_at(
            0,
            GraphEdge {
                edge,
                phase: GraphHopPhase::Direct,
                target_node: 1,
                log_weight: -0.01,
                ratio,
            },
        );
        let mut table = SpotTable::new(1);
        table.populate_from_graph(&graph);
        assert_eq!(table.get_ratio(&edge), Some(ratio));
    }

    #[test]
    fn v2_edge_ratio_none_when_reserve_too_shallow_for_marginal_probe() {
        use crate::core::types::V2PoolState;

        let state = PoolState::V2(V2PoolState {
            reserve0: U256::ONE,
            reserve1: U256::from(2_000u64),
            fee: U256::from(9970u64),
            fee_denominator: U256::from(10_000u64),
            block_timestamp_last: 0,
        });
        let edge = Edge {
            pool_index: PoolIndex(0),
            token_in: TokenIndex(0),
            token_out: TokenIndex(1),
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let v2 = match &state {
            PoolState::V2(s) => s,
            _ => panic!("v2"),
        };
        assert!(super::v2_edge_ratio_u256(v2, &edge, U256::from(1_000u64)).is_none());
    }

    #[test]
    fn v2_edge_ratio_caps_probe_below_reserve_in() {
        use crate::core::types::V2PoolState;

        let state = PoolState::V2(V2PoolState {
            reserve0: U256::from(1_000u64),
            reserve1: U256::from(2_000u64),
            fee: U256::from(9970u64),
            fee_denominator: U256::from(10_000u64),
            block_timestamp_last: 0,
        });
        let edge = Edge {
            pool_index: PoolIndex(0),
            token_in: TokenIndex(0),
            token_out: TokenIndex(1),
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let v2 = match &state {
            PoolState::V2(s) => s,
            _ => panic!("v2"),
        };
        let ratio =
            super::v2_edge_ratio_u256(v2, &edge, U256::from(10u128.pow(18))).expect("ratio");
        assert!(!ratio.is_zero());
    }

    #[test]
    fn edge_ratio_from_state_matches_compute_edge_ratio() {
        use crate::core::types::V2PoolState;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: U256::from(5_000_000u64) * U256::from(1_000_000u64),
                reserve1: U256::from(5_000_000u64) * U256::from(1_000_000_000_000_000_000u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        );
        let edge = Edge {
            pool_index: pool,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let state = arena.pool_state(pool).expect("pool");
        let probe = spot_probe_for_token(&arena, t0);
        let from_state = edge_ratio_from_state(state, &edge, probe);
        assert_eq!(from_state, compute_edge_ratio(&arena, &edge));
        assert!(!from_state.is_zero());
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
    fn simulated_edge_ratio_scales_large_outputs_without_u256_mul_overflow() {
        let out = U256::from(1u128 << 100);
        let probe = U256::from(10u128.pow(15));
        let ratio = super::simulated_edge_ratio(out, probe).expect("ratio");
        assert!(ratio > ONE);
    }

    #[test]
    fn cycle_product_ratio_is_one_for_identity_edge() {
        use crate::core::types::V2PoolState;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: U256::from(1_000_000u64) * U256::from(10u128.pow(18)),
                reserve1: U256::from(1_000_000u64) * U256::from(10u128.pow(18)),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        );
        let edge = Edge {
            pool_index: pool,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let single = cycle_product_ratio(&arena, &[edge]);
        let r = compute_edge_ratio(&arena, &edge);
        assert_eq!(single, r);
        assert!(!single.is_zero());
    }

    #[test]
    fn rescore_refreshes_cycle_ratio_from_live_edge_ratios() {
        use crate::core::types::{CycleEdges, V2PoolState};

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([4u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: U256::from(2_000_000u64) * U256::from(10u128.pow(18)),
                reserve1: U256::from(1_000_000u64) * U256::from(10u128.pow(18)),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        );
        let edge = Edge {
            pool_index: pool,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let mut cycle = FoundCycle {
            start_token: t0,
            edges: CycleEdges::from_slice(&[edge]),
            hop_count: 1,
            log_weight: 0.0,
            cumulative_fee_bps: 0,
            score: 0.0,
            cycle_ratio: U256::ZERO,
        };
        let table = SpotTable::new(1);
        rescore_one_cycle(&arena, &table, &mut cycle, None, None, None, None);
        let expected = cycle_product_ratio(&arena, &cycle.edges);
        assert_eq!(cycle.cycle_ratio, expected);
        assert!(!cycle.cycle_ratio.is_zero());
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
        table.set_ratio(&ab, ONE + ONE / U256::from(2u64));
        table.set_ratio(&ac, ONE + ONE / U256::from(4u64));
        assert!((table.get(&ab) - 1.5).abs() < 1e-9);
        assert!((table.get(&ac) - 1.25).abs() < 1e-9);
    }

    #[test]
    fn spot_table_pair_directions_remain_distinct() {
        let mut table = SpotTable::new(1);
        let forward = edge_with_indices(0, 0, 1, true);
        let reverse = edge_with_indices(0, 1, 0, false);
        table.set_ratio(&forward, ONE * U256::from(3u64));
        table.set_ratio(&reverse, ONE * U256::from(4u64));
        assert!((table.get(&forward) - 3.0).abs() < 1e-9);
        assert!((table.get(&reverse) - 4.0).abs() < 1e-9);
    }

    #[test]
    fn rank_probe_ladder_visits_ascending() {
        // WMATIC-like: micro 1e12 < spot 1e15 < economic … < one 1e18
        let rate = ONE; // 1:1 MATIC
        let mut seen = Vec::new();
        for_each_rank_probe_amount(18, rate, |a| seen.push(a));
        assert!(!seen.is_empty());
        for w in seen.windows(2) {
            assert!(w[0] <= w[1], "ladder must be ascending: {seen:?}");
        }
        let micro = U256::from(10u128.pow(12));
        let spot = spot_probe_for_decimals(18);
        assert!(micro < spot);
        assert_eq!(seen[0], micro, "micro-dust before spot (thin V2 friendly)");
    }
}
