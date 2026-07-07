use crate::core::constants::{BPS_SCALE, MAX_POOL_TOKENS};
use crate::core::types::{
    ConcentratedLiquidityPoolState, Edge, FlashLoanSource, FoundCycle, PoolState, ProtocolType,
    TokenIndex,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_finder::clamp_fee_bps;
use crate::pipeline::local_sim::simulate_hop_amount_out;
use crate::pipeline::sim_sanity::min_economic_amount_in;
use crate::pipeline::types::RoutingGraph;
use crate::services::execution::profit::flash_loan_fee_bps;
use crate::util::u256_to_f64;
use alloy::primitives::Address;
use alloy::primitives::U256;
use rustc_hash::FxHashMap;
use std::sync::Arc;

pub const SPOT_PROBE: U256 = U256::from_limbs([1_000_000_000_000, 0, 0, 0]); // 1e12 wei
const Q96_F64: f64 = 79228162514264337593543950336.0; // 2^96

const HOP_PENALTIES: [f64; 9] = [0.0, 0.0, 0.0, 0.01, 0.03, 0.08, 0.15, 0.30, 0.50];

/// Discourage long routes in log-weight scoring (gas + execution risk).
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

/// Dense spot cache keyed by `(pool_index, token_in_idx, token_out_idx)`.
#[derive(Debug, Clone)]
pub struct SpotTable {
    values: Vec<f64>,
}

const SPOT_SLOTS_PER_POOL: usize = MAX_POOL_TOKENS * MAX_POOL_TOKENS;

impl SpotTable {
    #[inline]
    fn slot(edge: &Edge) -> Option<usize> {
        let tin = edge.token_in_idx as usize;
        let tout = edge.token_out_idx as usize;
        if tin >= MAX_POOL_TOKENS || tout >= MAX_POOL_TOKENS {
            return None;
        }
        Some(edge.pool_index.0 as usize * SPOT_SLOTS_PER_POOL + tin * MAX_POOL_TOKENS + tout)
    }

    #[must_use]
    pub fn new(pool_count: usize) -> Self {
        Self {
            values: vec![f64::NAN; pool_count.max(1) * SPOT_SLOTS_PER_POOL],
        }
    }

    #[must_use]
    pub fn get(&self, edge: &Edge) -> f64 {
        let Some(slot) = Self::slot(edge) else {
            return 0.0;
        };
        self.values.get(slot).copied().unwrap_or_default()
    }

    pub fn set(&mut self, edge: &Edge, spot: f64) {
        let Some(slot) = Self::slot(edge) else {
            return;
        };
        if let Some(v) = self.values.get_mut(slot) {
            *v = spot;
        }
    }

    pub fn ensure_edge(&mut self, arena: &StateArena, edge: &Edge) -> f64 {
        let Some(slot) = Self::slot(edge) else {
            return compute_spot_price(arena, edge);
        };
        if let Some(v) = self.values.get(slot) {
            if !v.is_nan() {
                return *v;
            }
        } else {
            return 0.0;
        }
        let spot = compute_spot_price(arena, edge);
        if let Some(v) = self.values.get_mut(slot) {
            *v = spot;
        }
        spot
    }

    #[must_use]
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

#[must_use]
pub fn edge_log_weight_from_spot(spot_price: f64, fee_bps: u32) -> f64 {
    if spot_price <= 0.0 || !spot_price.is_finite() {
        return compute_edge_log_weight(fee_bps);
    }
    -spot_price.ln()
}

#[must_use]
pub fn compute_spot_price(arena: &StateArena, edge: &Edge) -> f64 {
    let state = match arena.pool_state(edge.pool_index) {
        Some(s) if s.is_tradable() => s,
        _ => return 0.0,
    };
    spot_price_from_state(state, edge)
}

/// Compute spot price from an already-retrieved and validated pool state, skipping
/// the redundant arena lookup when the caller has already verified tradability.
#[must_use]
pub fn spot_price_from_state(state: &PoolState, edge: &Edge) -> f64 {
    match (state, edge.protocol) {
        (PoolState::V2(s), ProtocolType::UniswapV2) => v2_marginal_spot(s, edge),
        (PoolState::V3(s), ProtocolType::UniswapV3)
        | (PoolState::V4(s), ProtocolType::UniswapV4) => cl_marginal_spot(s, edge),
        _ => {
            let out = match simulate_hop_amount_out(state, edge, SPOT_PROBE) {
                Some(v) if !v.is_zero() => v,
                _ => return 0.0,
            };
            {
                let p = u256_to_f64(SPOT_PROBE);
                if p <= 0.0 { 0.0 } else { u256_to_f64(out) / p }
            }
        }
    }
}

#[must_use]
pub fn compute_edge_log_weight_with_table(table: &SpotTable, edge: &Edge) -> f64 {
    let spot = table.get(edge);
    if spot <= 0.0 {
        return compute_edge_log_weight(edge.fee_bps);
    }
    edge_log_weight_from_spot(spot, edge.fee_bps)
}

/// Convert expected route gas + flash-loan fee into a log-weight penalty for cycle ranking.
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
    // Express drag as a fractional cost on a unit trade: ln(1 + cost/token_value).
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
        let cached = table.get(edge);
        let spot = if !cached.is_nan() {
            cached
        } else {
            let val = spot_price_from_state(state, edge);
            table.set(edge, val);
            val
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

/// Rescore cycles with spot prices and optional gas penalty (lower score = better).
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

/// Rescore shared cycles in place (COW per entry when snapshot still references them).
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
pub fn finalize_enumerated_cycles(
    _arena: &StateArena,
    cycles: Vec<FoundCycle>,
    max_cycles: usize,
) -> Vec<FoundCycle> {
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
