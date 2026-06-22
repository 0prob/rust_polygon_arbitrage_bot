use crate::core::types::{
    ConcentratedLiquidityPoolState, Edge, FoundCycle, PoolState, ProtocolType, TokenIndex,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_finder::clamp_fee_bps;
use crate::pipeline::local_sim::simulate_hop_amount_out;
use crate::pipeline::types::{GraphEdge, RoutingGraph};
use crate::util::u256_to_f64;
use ruint::aliases::U256;
use rustc_hash::FxHashMap;

pub const SPOT_PROBE: U256 = U256::from_limbs([1_000_000_000_000, 0, 0, 0]); // 1e12 wei
const Q96_F64: f64 = 79228162514264337593543950336.0; // 2^96

const HOP_PENALTIES: [f64; 9] = [0.0, 0.0, 0.0, 0.01, 0.03, 0.08, 0.15, 0.30, 0.50];

/// Discourage long routes in log-weight scoring (gas + execution risk).
pub fn hop_penalty(hops: u32) -> f64 {
    HOP_PENALTIES
        .get(hops as usize)
        .copied()
        .unwrap_or(hops as f64 * 0.15)
}

fn fee_log_weight(fee_bps: u32) -> f64 {
    (fee_bps as f64 / 10_000.0).ln_1p()
}

/// Spot-free edge weight: fee penalty only (fallback when reserves are unknown).
pub fn compute_edge_log_weight(fee_bps: u32) -> f64 {
    fee_log_weight(fee_bps)
}

/// Dense spot cache keyed by `(pool_index, zero_for_one)`.
#[derive(Debug, Clone)]
pub struct SpotTable {
    values: Vec<f64>,
}

impl SpotTable {
    #[inline]
    fn slot(pool: crate::core::types::PoolIndex, zero_for_one: bool) -> usize {
        pool.0 as usize * 2 + usize::from(zero_for_one)
    }

    pub fn new(pool_count: usize) -> Self {
        Self {
            values: vec![0.0; pool_count.max(1) * 2],
        }
    }

    pub fn get(&self, edge: &Edge) -> f64 {
        self.values
            .get(Self::slot(edge.pool_index, edge.zero_for_one))
            .copied()
            .unwrap_or(0.0)
    }

    pub fn set(&mut self, edge: &Edge, spot: f64) {
        let slot = Self::slot(edge.pool_index, edge.zero_for_one);
        if let Some(v) = self.values.get_mut(slot) {
            *v = spot;
        }
    }

    pub fn ensure_edge(&mut self, arena: &StateArena, edge: &Edge) -> f64 {
        let slot = Self::slot(edge.pool_index, edge.zero_for_one);
        if self.values.get(slot).copied().unwrap_or(0.0) > 0.0 {
            return self.values[slot];
        }
        let spot = compute_spot_price(arena, edge);
        if let Some(v) = self.values.get_mut(slot) {
            *v = spot;
        }
        spot
    }

    pub fn build_for_graph(arena: &StateArena, graph: &RoutingGraph) -> Self {
        let mut table = Self::new(arena.pool_count());
        for adj in &graph.adjacency {
            for ge in adj {
                table.ensure_edge(arena, &ge.edge);
            }
        }
        table
    }
}

/// Marginal V2 spot from reserves (no swap simulation).
#[inline]
fn v2_marginal_spot(state: &crate::core::types::V2PoolState, edge: &Edge) -> f64 {
    let (reserve_in, reserve_out) = if edge.zero_for_one {
        (state.reserve0, state.reserve1)
    } else {
        (state.reserve1, state.reserve0)
    };
    if reserve_in.is_zero() || reserve_out.is_zero() {
        return 0.0;
    }
    let rin = u256_to_f64(reserve_in);
    if rin <= 0.0 {
        return 0.0;
    }
    let fee_factor = 1.0 - edge.fee_bps as f64 / 10_000.0;
    u256_to_f64(reserve_out) / rin * fee_factor
}

/// Marginal V3/V4 spot from `sqrt_price_x96` (no tick walk).
#[inline]
fn cl_marginal_spot(state: &ConcentratedLiquidityPoolState, edge: &Edge) -> f64 {
    if state.sqrt_price_x96.is_zero() {
        return 0.0;
    }
    let sqrt = u256_to_f64(state.sqrt_price_x96);
    let price1_per_0 = (sqrt / Q96_F64).powi(2);
    if !price1_per_0.is_finite() || price1_per_0 <= 0.0 {
        return 0.0;
    }
    let raw = if edge.zero_for_one {
        price1_per_0
    } else {
        1.0 / price1_per_0
    };
    let fee_factor = 1.0 - edge.fee_bps as f64 / 10_000.0;
    raw * fee_factor
}

fn spot_ratio(amount_out: U256, amount_in: U256) -> f64 {
    let probe = u256_to_f64(amount_in);
    if probe <= 0.0 {
        return 0.0;
    }
    u256_to_f64(amount_out) / probe
}

pub fn edge_log_weight_from_spot(spot_price: f64, fee_bps: u32) -> f64 {
    if spot_price <= 0.0 || !spot_price.is_finite() {
        return compute_edge_log_weight(fee_bps);
    }
    -spot_price.ln()
}

pub fn compute_spot_price(arena: &StateArena, edge: &Edge) -> f64 {
    let state = match arena.pool_state(edge.pool_index) {
        Some(s) if s.is_tradable() => s,
        _ => return 0.0,
    };
    match (state, edge.protocol) {
        (PoolState::V2(s), ProtocolType::UniswapV2) => v2_marginal_spot(s, edge),
        (PoolState::V3(s), ProtocolType::UniswapV3) => cl_marginal_spot(s, edge),
        (PoolState::V4(s), ProtocolType::UniswapV4) => cl_marginal_spot(s, edge),
        _ => {
            let out = match simulate_hop_amount_out(state, edge, SPOT_PROBE) {
                Some(v) if !v.is_zero() => v,
                _ => return 0.0,
            };
            spot_ratio(out, SPOT_PROBE)
        }
    }
}

pub fn compute_edge_log_weight_with_state(
    arena: &StateArena,
    edge: &Edge,
    precomputed_spot: Option<f64>,
) -> f64 {
    let state = arena.pool_state(edge.pool_index);
    if state.map(|s| !s.is_tradable()).unwrap_or(true) {
        return 15.0;
    }
    let spot = precomputed_spot.unwrap_or_else(|| compute_spot_price(arena, edge));
    edge_log_weight_from_spot(spot, edge.fee_bps)
}

pub fn compute_edge_log_weight_with_table(table: &SpotTable, edge: &Edge) -> f64 {
    let spot = table.get(edge);
    if spot <= 0.0 {
        return compute_edge_log_weight(edge.fee_bps);
    }
    edge_log_weight_from_spot(spot, edge.fee_bps)
}

pub fn rescore_cycles_by_spot_price(arena: &StateArena, cycles: &mut [FoundCycle]) {
    let mut table = SpotTable::new(arena.pool_count());
    rescore_cycles_with_table(arena, &mut table, cycles);
}

/// Convert expected route gas into a log-weight penalty for cycle ranking.
pub fn gas_log_penalty_for_cycle(
    edges: &[Edge],
    gas_price_wei: U256,
    token_to_matic_rates: Option<&FxHashMap<TokenIndex, U256>>,
    _arena: &StateArena,
    start_token: TokenIndex,
) -> f64 {
    let gas_units = crate::pipeline::local_sim::estimate_route_gas(edges);
    if gas_units == 0 || gas_price_wei.is_zero() {
        return 0.0;
    }
    let gas_cost_wei = U256::from(gas_units) * gas_price_wei;
    let rate = token_to_matic_rates
        .and_then(|m| m.get(&start_token).copied())
        .filter(|r| !r.is_zero())
        .unwrap_or_else(crate::services::oracle::price_oracle::bootstrap_matic_rate_per_unit);
    let gas_f64 = u256_to_f64(gas_cost_wei);
    let rate_f64 = u256_to_f64(rate);
    if gas_f64 <= 0.0 || rate_f64 <= 0.0 {
        return 0.0;
    }
    // Express gas as a fractional drag on a unit trade: ln(1 + gas/token_value).
    (gas_f64 / rate_f64).ln_1p()
}

pub fn rescore_cycles_with_table(
    arena: &StateArena,
    table: &mut SpotTable,
    cycles: &mut [FoundCycle],
) {
    rescore_cycles_with_table_and_gas(arena, table, cycles, None, None);
}

/// Rescore cycles with spot prices and optional gas penalty (lower score = better).
pub fn rescore_cycles_with_table_and_gas(
    arena: &StateArena,
    table: &mut SpotTable,
    cycles: &mut [FoundCycle],
    gas_price_wei: Option<U256>,
    token_to_matic_rates: Option<&FxHashMap<TokenIndex, U256>>,
) {
    for cycle in cycles.iter_mut() {
        let mut log_weight = 0.0;
        let mut cum_fee = 0u32;
        let mut missing_spot = 0u32;
        for edge in &cycle.edges {
            cum_fee = cum_fee.saturating_add(clamp_fee_bps(edge.fee_bps));
            let spot = table.ensure_edge(arena, edge);
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
        let gas_penalty = gas_price_wei
            .filter(|p| !p.is_zero())
            .map(|price| gas_log_penalty_for_cycle(&cycle.edges, price, token_to_matic_rates, arena, cycle.start_token))
            .unwrap_or(0.0);
        log_weight += hop_penalty(cycle.hop_count) + missing_penalty + gas_penalty;
        cycle.log_weight = log_weight;
        cycle.score = log_weight;
        cycle.cumulative_fee_bps = cum_fee;
    }
}

/// Apply spot-derived log weights to a routing graph adjacency list.
pub fn reweight_graph_with_spot(arena: &StateArena, graph: &RoutingGraph) -> Vec<Vec<GraphEdge>> {
    let table = SpotTable::build_for_graph(arena, graph);
    graph
        .adjacency
        .iter()
        .map(|edges| {
            edges
                .iter()
                .map(|ge| GraphEdge {
                    edge: ge.edge,
                    log_weight: compute_edge_log_weight_with_table(&table, &ge.edge),
                })
                .collect()
        })
        .collect()
}

pub fn finalize_enumerated_cycles(
    _arena: &StateArena,
    cycles: Vec<FoundCycle>,
    max_cycles: usize,
) -> Vec<FoundCycle> {
    let mut out = crate::pipeline::cycle_filter::dedupe_cycles_by_fingerprint(cycles);
    if out.len() > max_cycles {
        out = crate::pipeline::cycle_finder::apply_hop_stratified_cap(out, max_cycles);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::V2PoolState;
    use crate::pipeline::graph::{build_graph, pool_meta_from_pair};
    use alloy::primitives::Address;

    #[test]
    fn rescored_v2_cycle_has_negative_log_weight() {
        let mut arena = StateArena::new();
        let t0 = arena.register_token(Address::repeat_byte(1));
        let t1 = arena.register_token(Address::repeat_byte(2));
        let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
        let p = arena.register_pool(
            Address::repeat_byte(0x10),
            PoolState::V2(V2PoolState {
                reserve0: reserve,
                reserve1: reserve * U256::from(2u8),
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
        let edge = Edge {
            pool_index: p,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let spot = compute_spot_price(&arena, &edge);
        assert!(spot > 1.0);
        let weight = compute_edge_log_weight_with_state(&arena, &edge, Some(spot));
        assert!(weight < 0.0);

        let pools = vec![pool_meta_from_pair(
            p,
            ProtocolType::UniswapV2,
            t0,
            t1,
            Some(30),
        )];
        let _graph = build_graph(&arena, &pools);
        let mut cycles = vec![FoundCycle {
            start_token: t0,
            edges: vec![edge].into(),
            hop_count: 1,
            log_weight: 0.0,
            cumulative_fee_bps: 30,
            score: 0.0,
        }];
        rescore_cycles_by_spot_price(&arena, &mut cycles);
        assert!(cycles[0].score < 0.0);
    }

    #[test]
    fn spot_table_reuses_v2_entries() {
        let mut arena = StateArena::new();
        let t0 = arena.register_token(Address::repeat_byte(1));
        let t1 = arena.register_token(Address::repeat_byte(2));
        let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
        let p = arena.register_pool(
            Address::repeat_byte(0x10),
            PoolState::V2(V2PoolState {
                reserve0: reserve,
                reserve1: reserve * U256::from(2u8),
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
        let edge = Edge {
            pool_index: p,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let mut table = SpotTable::new(arena.pool_count());
        let a = table.ensure_edge(&arena, &edge);
        let b = table.ensure_edge(&arena, &edge);
        assert_eq!(a, b);
        assert!(a > 0.0);
    }
}
