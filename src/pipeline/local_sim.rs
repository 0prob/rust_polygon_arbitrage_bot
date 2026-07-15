use crate::core::constants::{
    GAS_BALANCER_DIRECT_BATCH, GAS_BALANCER_HOP, GAS_CURVE_HOP, GAS_DODO_HOP, GAS_V2_HOP,
    GAS_V3_BASE, GAS_V4_BASE, GAS_WOOFI_HOP, HOP_CAP_USIZE,
};
use crate::core::math::balancer::simulate_balancer_swap;
use crate::pipeline::curve_sim::curve_hop_amount_out;
use crate::core::math::dodo::get_dodo_amount_out;
use crate::core::math::uniswap_v2::simulate_v2_swap;
use crate::core::math::uniswap_v3::simulate_v3_swap;
use crate::core::math::woofi::get_woofi_amount_out;
use crate::core::types::{
    Edge, PoolState, ProtocolType, RouteSimulationResult, hop_amounts_zeroed,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::spot_price::spot_probe_for_token;
use crate::pipeline::types::MinimalSimResult;
use alloy::primitives::U256;
use rustc_hash::FxHashMap;

/// Per-hop gas estimate for route ranking (matches simulation constants).
#[must_use]
pub fn estimate_hop_gas(protocol: ProtocolType) -> u32 {
    match protocol {
        ProtocolType::UniswapV2 => GAS_V2_HOP,
        ProtocolType::UniswapV3 => GAS_V3_BASE,
        ProtocolType::UniswapV4 => GAS_V4_BASE,
        ProtocolType::CurveStable | ProtocolType::CurveCrypto => GAS_CURVE_HOP,
        ProtocolType::BalancerV2 => GAS_BALANCER_HOP,
        ProtocolType::Dodo => GAS_DODO_HOP,
        ProtocolType::Woofi => GAS_WOOFI_HOP,
    }
}

/// Hop gas budget for ranking: one `batchSwap` for pure Balancer direct routes, else per-hop sum.
#[must_use]
pub fn route_hop_gas_budget(edges: &[Edge]) -> u32 {
    if crate::pipeline::route_calls::balancer_direct_batch_eligible(edges) {
        return GAS_BALANCER_DIRECT_BATCH;
    }
    edges.iter().map(|e| estimate_hop_gas(e.protocol)).sum()
}

/// Route gas for ranking: static hop budget vs walked hop gas (V3 tick crosses), whichever is higher.
#[must_use]
fn finalize_route_total_gas(edges: &[Edge], walked_hop_gas: u32) -> u32 {
    let hop_count = edges.len();
    if hop_count == 0 {
        return crate::services::execution::gas::ROUTE_EXECUTION_GAS_OVERHEAD;
    }
    let hop_budget = route_hop_gas_budget(edges);
    let static_gas = crate::services::execution::gas::estimate_route_gas_from_hops_evm(
        hop_budget,
        hop_count,
        hop_count as u32,
    );
    if walked_hop_gas == 0 || crate::pipeline::route_calls::balancer_direct_batch_eligible(edges) {
        return static_gas;
    }
    let dynamic = crate::services::execution::gas::estimate_route_gas_from_hops(
        walked_hop_gas,
        hop_count,
    )
    .saturating_add(crate::services::execution::gas::estimate_route_storage_gas(
        hop_count,
        hop_count as u32,
    ));
    static_gas.max(dynamic)
}

/// Conservative gas units for a full route (overhead + per-hop + tick premium + storage reads).
#[must_use]
pub fn estimate_route_gas(edges: &[Edge]) -> u32 {
    if edges.is_empty() {
        return crate::services::execution::gas::ROUTE_EXECUTION_GAS_OVERHEAD;
    }
    let hop_gas = route_hop_gas_budget(edges);
    let cold_slots = edges.len() as u32;
    crate::services::execution::gas::estimate_route_gas_from_hops_evm(
        hop_gas,
        edges.len(),
        cold_slots,
    )
}

#[derive(Debug, Clone, Copy)]
struct HopResult {
    amount_out: U256,
    gas: u32,
}

/// Per-hop shallow caps for CL routes only; other protocols use `U256::MAX`.
#[inline]
fn route_shallow_caps_with(
    edges: &[Edge],
    mut probe_for_token: impl FnMut(crate::core::types::TokenIndex) -> U256,
) -> [U256; HOP_CAP_USIZE] {
    let mut caps = [U256::MAX; HOP_CAP_USIZE];
    let mut token_caps: FxHashMap<crate::core::types::TokenIndex, U256> = FxHashMap::default();
    for (i, edge) in edges.iter().enumerate() {
        if matches!(
            edge.protocol,
            ProtocolType::UniswapV3 | ProtocolType::UniswapV4
        ) {
            caps[i] = *token_caps
                .entry(edge.token_in)
                .or_insert_with(|| probe_for_token(edge.token_in));
        }
    }
    caps
}

fn route_shallow_caps(arena: &StateArena, edges: &[Edge]) -> [U256; HOP_CAP_USIZE] {
    route_shallow_caps_with(edges, |token| spot_probe_for_token(arena, token))
}

#[inline]
fn route_has_cl_hop(edges: &[Edge]) -> bool {
    edges.iter().any(|edge| {
        matches!(
            edge.protocol,
            ProtocolType::UniswapV3 | ProtocolType::UniswapV4
        )
    })
}

fn simulate_hop(
    state: &PoolState,
    edge: &Edge,
    amount_in: U256,
    shallow_cap: U256,
) -> Option<HopResult> {
    if amount_in.is_zero() {
        return Some(HopResult {
            amount_out: U256::ZERO,
            gas: 0,
        });
    }
    if matches!(
        edge.protocol,
        ProtocolType::UniswapV3 | ProtocolType::UniswapV4
    ) && cl_hop_exceeds_shallow_cap(amount_in, shallow_cap)
    {
        return None;
    }

    match (state, edge.protocol) {
        (PoolState::V2(s), ProtocolType::UniswapV2) => {
            if amount_in >= v2_reserve_in(s, edge.zero_for_one) {
                return None;
            }
            let out = simulate_v2_swap(s, amount_in, edge.zero_for_one, Some(edge.fee_bps));
            Some(HopResult {
                amount_out: out,
                gas: GAS_V2_HOP,
            })
        }
        (PoolState::V3(s), ProtocolType::UniswapV3)
        | (PoolState::V4(s), ProtocolType::UniswapV4) => {
            let r = simulate_v3_swap(s, amount_in, edge.zero_for_one, Some(edge.fee_bps));
            if r.shallow {
                return None;
            }
            Some(HopResult {
                amount_out: r.amount_out,
                gas: r.gas_estimate,
            })
        }
        (PoolState::Curve(s), ProtocolType::CurveStable | ProtocolType::CurveCrypto) => {
            let out = curve_hop_amount_out(
                s,
                edge.protocol,
                amount_in,
                edge.token_in_idx as usize,
                edge.token_out_idx as usize,
            )?;
            Some(HopResult {
                amount_out: out,
                gas: GAS_CURVE_HOP,
            })
        }
        (PoolState::Balancer(s), ProtocolType::BalancerV2) => {
            let out = simulate_balancer_swap(
                s,
                amount_in,
                edge.token_in_idx as usize,
                edge.token_out_idx as usize,
            );
            Some(HopResult {
                amount_out: out,
                gas: GAS_BALANCER_HOP,
            })
        }
        (PoolState::Dodo(s), ProtocolType::Dodo) => {
            let out = get_dodo_amount_out(s, amount_in, edge.zero_for_one);
            Some(HopResult {
                amount_out: out,
                gas: GAS_DODO_HOP,
            })
        }
        (PoolState::Woofi(s), ProtocolType::Woofi) => {
            let n_bases = s.base_states.len();
            let in_is_quote = edge.token_in_idx as usize >= n_bases;
            let out_is_quote = edge.token_out_idx as usize >= n_bases;
            let base_in = if in_is_quote {
                None
            } else {
                Some(edge.token_in_idx as usize)
            };
            let base_out = if out_is_quote {
                None
            } else {
                Some(edge.token_out_idx as usize)
            };
            let out =
                get_woofi_amount_out(s, amount_in, in_is_quote, out_is_quote, base_in, base_out);
            Some(HopResult {
                amount_out: out,
                gas: GAS_WOOFI_HOP,
            })
        }
        _ => None,
    }
}

/// Quote a single hop output for calldata encoding (reuses pipeline math).
#[must_use]
pub fn simulate_hop_amount_out(state: &PoolState, edge: &Edge, amount_in: U256) -> Option<U256> {
    simulate_hop_amount_out_with_cap(state, edge, amount_in, U256::MAX)
}

#[must_use]
pub fn simulate_hop_amount_out_with_cap(
    state: &PoolState,
    edge: &Edge,
    amount_in: U256,
    shallow_cap: U256,
) -> Option<U256> {
    simulate_hop(state, edge, amount_in, shallow_cap).map(|h| h.amount_out)
}

fn cl_hop_tickless(state: &PoolState) -> bool {
    matches!(
        state,
        PoolState::V3(s) | PoolState::V4(s) if s.ticks.is_empty()
    )
}

#[inline]
fn cl_hop_exceeds_shallow_cap(amount_in: U256, shallow_cap: U256) -> bool {
    shallow_cap < U256::MAX && amount_in > shallow_cap
}

#[inline]
fn v2_reserve_in(state: &crate::core::types::V2PoolState, zero_for_one: bool) -> U256 {
    if zero_for_one {
        state.reserve0
    } else {
        state.reserve1
    }
}

/// Max trade size with faithful CL simulation. `None` = full tick coverage.
/// `Some(0)` = at least one CL hop lacks tick coverage and must not be quoted.
#[must_use]
pub fn cl_amount_cap(arena: &StateArena, edges: &[Edge]) -> Option<U256> {
    for edge in edges {
        if !matches!(
            edge.protocol,
            ProtocolType::UniswapV3 | ProtocolType::UniswapV4
        ) {
            continue;
        }
        let Some(state) = arena.pool_state(edge.pool_index) else {
            return Some(U256::ZERO);
        };
        if cl_hop_tickless(state) {
            return Some(U256::ZERO);
        }
    }
    None
}

/// Max gross-profit erosion (bps) tolerated between eval and post-refresh resim.
const RESIM_PROFIT_DRIFT_BPS: u64 = 1000;
/// Max per-hop amount drift (bps) tolerated between eval and post-refresh resim.
const RESIM_HOP_DRIFT_BPS: u64 = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HopFidelityReject {
    MissingPool(usize),
    PoolLocked(usize),
    ShallowCl(usize),
    V2ReserveExhausted(usize),
}

/// Counters from `route_hop_fidelity_reject_profiled` (CL depth sims are the expensive path).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct HopFidelityProfile {
    pub hops_checked: u32,
    pub cl_depth_sims: u32,
}

/// Drift metrics from a resim compare (populated even when the gate passes).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ResimFidelityProfile {
    pub profit_drift_bps: u64,
    pub max_hop_drift_bps: u64,
}

fn cl_hop_shallow_at_amount(
    state: &PoolState,
    edge: &Edge,
    hop_probe: U256,
    amount_in: U256,
    profile: Option<&mut HopFidelityProfile>,
) -> bool {
    if cl_hop_tickless(state) {
        return false;
    }
    if cl_hop_exceeds_shallow_cap(amount_in, hop_probe) {
        return true;
    }
    if let Some(p) = profile {
        p.cl_depth_sims = p.cl_depth_sims.saturating_add(1);
    }
    match state {
        PoolState::V3(s) | PoolState::V4(s) => {
            simulate_v3_swap(s, amount_in, edge.zero_for_one, Some(edge.fee_bps)).shallow
        }
        _ => false,
    }
}

#[inline]
fn u256_to_bps_u64(v: U256) -> u64 {
    if v > U256::from(u64::MAX) {
        u64::MAX
    } else {
        v.as_limbs()[0]
    }
}

fn hop_amount_within_drift(baseline: U256, refreshed: U256, max_drift_bps: u64) -> bool {
    if baseline == refreshed {
        return true;
    }
    if baseline.is_zero() {
        return refreshed.is_zero();
    }
    let (lo, hi) = if baseline >= refreshed {
        (refreshed, baseline)
    } else {
        (baseline, refreshed)
    };
    let drift_bps = (hi - lo) * U256::from(10_000u64) / lo;
    drift_bps <= U256::from(max_drift_bps)
}

/// Per-hop CL fidelity: each V3/V4 hop uses its own decimal-aware spot probe.
#[must_use]
pub fn route_hop_fidelity_ok(arena: &StateArena, edges: &[Edge], hop_amounts: &[U256]) -> bool {
    route_hop_fidelity_reject(arena, edges, hop_amounts).is_none()
}

/// Fidelity after `simulate_route_detailed` / `walk_route_hops` on the same arena (skips redundant CL depth sims).
#[must_use]
pub fn route_hop_fidelity_ok_after_walk(
    arena: &StateArena,
    edges: &[Edge],
    hop_amounts: &[U256],
) -> bool {
    route_hop_fidelity_reject_profiled(arena, edges, hop_amounts, None, true, None).is_none()
}

/// First hop that fails tick-depth, tradability, or reserve-depth checks, if any.
#[must_use]
pub fn route_hop_fidelity_reject(
    arena: &StateArena,
    edges: &[Edge],
    hop_amounts: &[U256],
) -> Option<HopFidelityReject> {
    route_hop_fidelity_reject_profiled(arena, edges, hop_amounts, None, false, None)
}

#[must_use]
pub fn route_hop_fidelity_reject_profiled(
    arena: &StateArena,
    edges: &[Edge],
    hop_amounts: &[U256],
    mut profile: Option<&mut HopFidelityProfile>,
    cl_depth_already_verified: bool,
    precomputed_hop_probes: Option<&[U256; HOP_CAP_USIZE]>,
) -> Option<HopFidelityReject> {
    let hop_probes = if let Some(caps) = precomputed_hop_probes {
        *caps
    } else if route_has_cl_hop(edges) {
        route_shallow_caps(arena, edges)
    } else {
        [U256::MAX; HOP_CAP_USIZE]
    };
    for (i, edge) in edges.iter().enumerate() {
        if let Some(p) = profile.as_deref_mut() {
            p.hops_checked = p.hops_checked.saturating_add(1);
        }
        let amount_in = hop_amounts.get(i).copied().unwrap_or(U256::ZERO);
        let Some(state) = arena.pool_state(edge.pool_index) else {
            return Some(HopFidelityReject::MissingPool(i));
        };
        if !state.is_tradable() {
            return Some(HopFidelityReject::PoolLocked(i));
        }
        match (state, edge.protocol) {
            (PoolState::V2(s), ProtocolType::UniswapV2) => {
                let (reserve_in, _reserve_out) = if edge.zero_for_one {
                    (s.reserve0, s.reserve1)
                } else {
                    (s.reserve1, s.reserve0)
                };
                if amount_in >= reserve_in {
                    return Some(HopFidelityReject::V2ReserveExhausted(i));
                }
            }
            (
                PoolState::V3(_) | PoolState::V4(_),
                ProtocolType::UniswapV3 | ProtocolType::UniswapV4,
            ) if cl_hop_tickless(state) => {
                return Some(HopFidelityReject::ShallowCl(i));
            }
            (
                PoolState::V3(_) | PoolState::V4(_),
                ProtocolType::UniswapV3 | ProtocolType::UniswapV4,
            ) if !cl_depth_already_verified
                && cl_hop_shallow_at_amount(
                    state,
                    edge,
                    hop_probes[i],
                    amount_in,
                    profile.as_deref_mut(),
                ) =>
            {
                return Some(HopFidelityReject::ShallowCl(i));
            }
            (
                PoolState::V3(_) | PoolState::V4(_),
                ProtocolType::UniswapV3 | ProtocolType::UniswapV4,
            ) if cl_depth_already_verified
                && cl_hop_exceeds_shallow_cap(amount_in, hop_probes[i]) =>
            {
                return Some(HopFidelityReject::ShallowCl(i));
            }
            _ => {
                if !amount_in.is_zero() && simulate_hop_amount_out(state, edge, amount_in).is_none()
                {
                    return Some(HopFidelityReject::PoolLocked(i));
                }
            }
        }
    }
    None
}

/// Post-refresh resim must stay profitable and keep hop amounts aligned with eval.
#[must_use]
pub fn route_resim_fidelity_ok(
    baseline: &RouteSimulationResult,
    refreshed: &RouteSimulationResult,
) -> bool {
    route_resim_fidelity_reject(baseline, refreshed).is_none()
}

#[must_use]
pub fn route_resim_fidelity_reject(
    baseline: &RouteSimulationResult,
    refreshed: &RouteSimulationResult,
) -> Option<&'static str> {
    let mut profile = ResimFidelityProfile::default();
    route_resim_fidelity_reject_profiled(baseline, refreshed, &mut profile)
}

#[must_use]
pub fn route_resim_fidelity_reject_profiled(
    baseline: &RouteSimulationResult,
    refreshed: &RouteSimulationResult,
    profile: &mut ResimFidelityProfile,
) -> Option<&'static str> {
    if refreshed.profit.is_zero() {
        return Some("resim unprofitable");
    }
    if baseline.hop_amounts.len() != refreshed.hop_amounts.len() {
        return Some("hop count mismatch");
    }
    if !baseline.profit.is_zero() {
        if refreshed.profit >= baseline.profit {
            profile.profit_drift_bps = 0;
        } else {
            let lost = baseline.profit - refreshed.profit;
            let bps = lost * U256::from(10_000u64) / baseline.profit;
            profile.profit_drift_bps = u256_to_bps_u64(bps);
        }
        let min_profit = baseline.profit * U256::from(10_000u64 - RESIM_PROFIT_DRIFT_BPS)
            / U256::from(10_000u64);
        if refreshed.profit < min_profit {
            return Some("profit drift");
        }
    }
    for i in 0..baseline.hop_amounts.len() {
        let b = baseline.hop_amounts[i];
        let r = refreshed.hop_amounts[i];
        if b != r && !b.is_zero() {
            let (lo, hi) = if b >= r { (r, b) } else { (b, r) };
            let drift = u256_to_bps_u64((hi - lo) * U256::from(10_000u64) / lo);
            profile.max_hop_drift_bps = profile.max_hop_drift_bps.max(drift);
        }
        if !hop_amount_within_drift(b, r, RESIM_HOP_DRIFT_BPS) {
            return Some("hop amount drift");
        }
    }
    None
}

#[must_use]
/// Precomputed CL shallow caps for Brent — avoids rebuilding per `simulate_route_minimal` call.
#[must_use]
pub fn precompute_route_shallow_caps(
    arena: &StateArena,
    edges: &[Edge],
) -> Option<[U256; HOP_CAP_USIZE]> {
    route_has_cl_hop(edges).then(|| route_shallow_caps(arena, edges))
}

fn walk_route_hops(
    arena: &StateArena,
    edges: &[Edge],
    amount_in: U256,
    mut hop_amounts: Option<&mut [U256]>,
    precomputed_shallow_caps: Option<&[U256; HOP_CAP_USIZE]>,
) -> Option<(U256, u32)> {
    if edges.len() > HOP_CAP_USIZE {
        return None;
    }
    let mut current = amount_in;
    let mut total_gas = 0u32;
    let shallow_caps = if let Some(caps) = precomputed_shallow_caps {
        *caps
    } else if route_has_cl_hop(edges) {
        route_shallow_caps(arena, edges)
    } else {
        [U256::MAX; HOP_CAP_USIZE]
    };
    if let Some(amounts) = hop_amounts.as_deref_mut() {
        *amounts.first_mut()? = amount_in;
    }

    for (i, edge) in edges.iter().enumerate() {
        let state = arena.pool_state(edge.pool_index)?;
        if !state.is_tradable() {
            return None;
        }
        let shallow_cap = shallow_caps[i];
        let hop = simulate_hop(state, edge, current, shallow_cap)?;
        if current > U256::ZERO && hop.amount_out.is_zero() {
            return None;
        }
        current = hop.amount_out;
        total_gas += hop.gas;
        if let Some(amounts) = hop_amounts.as_deref_mut() {
            *amounts.get_mut(i + 1)? = current;
        }
    }

    Some((current, total_gas))
}

#[inline]
fn route_edges_simulatable(edges: &[Edge]) -> bool {
    !edges.is_empty()
        && edges.len() <= HOP_CAP_USIZE
        && edges
            .windows(2)
            .all(|pair| pair[0].token_out == pair[1].token_in)
}

pub fn simulate_route_minimal(
    arena: &StateArena,
    edges: &[Edge],
    amount_in: U256,
) -> Option<MinimalSimResult> {
    simulate_route_minimal_with_caps(arena, edges, amount_in, None)
}

#[must_use]
pub fn simulate_route_minimal_with_caps(
    arena: &StateArena,
    edges: &[Edge],
    amount_in: U256,
    precomputed_shallow_caps: Option<&[U256; HOP_CAP_USIZE]>,
) -> Option<MinimalSimResult> {
    if !route_edges_simulatable(edges) {
        return None;
    }
    if amount_in.is_zero() {
        return Some(MinimalSimResult {
            profit: U256::ZERO,
            amount_out: U256::ZERO,
            total_gas: finalize_route_total_gas(edges, 0),
        });
    }
    let (amount_out, walked_gas) =
        walk_route_hops(arena, edges, amount_in, None, precomputed_shallow_caps)?;
    let profit = amount_out.saturating_sub(amount_in);
    let total_gas = finalize_route_total_gas(edges, walked_gas);
    Some(MinimalSimResult {
        profit,
        amount_out,
        total_gas,
    })
}

/// Full hop trace for calldata encoding and profit assessment.
#[must_use]
pub fn simulate_route_detailed(
    arena: &StateArena,
    edges: &[Edge],
    amount_in: U256,
) -> Option<RouteSimulationResult> {
    let hop_count = edges.len();
    if !route_edges_simulatable(edges) {
        return None;
    }
    if amount_in.is_zero() {
        return Some(RouteSimulationResult {
            amount_in: U256::ZERO,
            amount_out: U256::ZERO,
            profit: U256::ZERO,
            profitable: false,
            hop_amounts: hop_amounts_zeroed(hop_count),
            total_gas: finalize_route_total_gas(edges, 0),
            hop_count: hop_count as u32,
        });
    }
    let mut hop_amounts = hop_amounts_zeroed(hop_count);
    let (amount_out, walked_gas) = walk_route_hops(
        arena,
        edges,
        amount_in,
        Some(&mut hop_amounts),
        None,
    )?;
    let profit = amount_out.saturating_sub(amount_in);
    let total_gas = finalize_route_total_gas(edges, walked_gas);
    Some(RouteSimulationResult {
        amount_in,
        amount_out,
        profit,
        profitable: profit > U256::ZERO,
        hop_amounts,
        total_gas,
        hop_count: hop_count as u32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::Edge;
    use alloy::primitives::Address;

    #[test]
    fn oversized_routes_fail_closed() {
        let arena = StateArena::default();
        let edge = Edge {
            pool_index: crate::core::types::PoolIndex(0),
            token_in: crate::core::types::TokenIndex(0),
            token_out: crate::core::types::TokenIndex(1),
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let edges = vec![edge; HOP_CAP_USIZE + 1];
        assert!(simulate_route_minimal(&arena, &edges, U256::ZERO).is_none());
        assert!(simulate_route_detailed(&arena, &edges, U256::ZERO).is_none());
    }

    #[test]
    fn test_estimate_hop_gas_v2() {
        assert!(estimate_hop_gas(ProtocolType::UniswapV2) > 0);
    }

    #[test]
    fn zero_amount_minimal_sim_skips_walk() {
        use crate::core::types::V2PoolState;
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: U256::from(1_000_000u64),
                reserve1: U256::from(1_000_000u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 1,
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
        let sim = simulate_route_minimal(&arena, &[edge], U256::ZERO).expect("zero sim");
        assert!(sim.profit.is_zero());
        assert!(sim.amount_out.is_zero());
    }

    #[test]
    fn disconnected_hops_fail_closed_even_for_zero_amount() {
        use crate::core::types::V2PoolState;
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let t2 = arena.register_token(Address::from([3u8; 20]));
        let state = Arc::new(PoolState::V2(V2PoolState {
            reserve0: U256::from(1_000_000u64),
            reserve1: U256::from(1_000_000u64),
            fee: U256::from(997u64),
            fee_denominator: U256::from(1_000u64),
            block_timestamp_last: 1,
        }));
        let first_pool = arena.register_pool(Address::from([4u8; 20]), Arc::clone(&state));
        let second_pool = arena.register_pool(Address::from([5u8; 20]), state);
        let edges = [
            Edge {
                pool_index: first_pool,
                token_in: t0,
                token_out: t1,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: second_pool,
                token_in: t2,
                token_out: t0,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
        ];

        assert!(simulate_route_minimal(&arena, &edges, U256::ZERO).is_none());
        assert!(simulate_route_detailed(&arena, &edges, U256::from(100u64)).is_none());
    }

    #[test]
    fn v3_route_gas_does_not_double_count_base_cost() {
        use crate::core::types::{V3PoolState, V3Tick};
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000_000_000_000_000u128,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![V3Tick {
                    tick: -60_000,
                    liquidity_gross: 1,
                    liquidity_net: 0,
                }]),
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

        let amount_in = crate::pipeline::spot_price::spot_probe_for_token(&arena, t0);
        let sim = simulate_route_minimal(&arena, &[edge], amount_in).expect("simulation");
        let expected = crate::services::execution::gas::estimate_route_gas_from_hops_evm(
            crate::core::constants::GAS_V3_BASE,
            1,
            1,
        );
        assert_eq!(sim.total_gas, expected);
    }

    #[test]
    fn balancer_direct_batch_uses_single_batch_gas_not_per_hop_sum() {
        use crate::core::constants::GAS_BALANCER_DIRECT_BATCH;
        use crate::services::execution::gas::estimate_route_gas_from_hops_evm;

        let edges = [
            Edge {
                pool_index: crate::core::types::PoolIndex(0),
                token_in: crate::core::types::TokenIndex(0),
                token_out: crate::core::types::TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::BalancerV2,
                fee_bps: 0,
                zero_for_one: true,
            },
            Edge {
                pool_index: crate::core::types::PoolIndex(1),
                token_in: crate::core::types::TokenIndex(1),
                token_out: crate::core::types::TokenIndex(0),
                token_in_idx: 1,
                token_out_idx: 0,
                protocol: ProtocolType::BalancerV2,
                fee_bps: 0,
                zero_for_one: true,
            },
        ];
        let batch = estimate_route_gas(&edges);
        let per_hop = estimate_route_gas_from_hops_evm(GAS_BALANCER_HOP * 2, 2, 2);
        assert_eq!(route_hop_gas_budget(&edges), GAS_BALANCER_DIRECT_BATCH);
        assert!(batch < per_hop / 2);
    }

    #[test]
    fn route_gas_formula_matches_executor_overhead_model() {
        use crate::core::constants::GAS_V2_HOP;
        use crate::services::execution::gas::estimate_route_gas_from_hops_evm;

        let edges = [
            Edge {
                pool_index: crate::core::types::PoolIndex(0),
                token_in: crate::core::types::TokenIndex(0),
                token_out: crate::core::types::TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: crate::core::types::PoolIndex(1),
                token_in: crate::core::types::TokenIndex(1),
                token_out: crate::core::types::TokenIndex(0),
                token_in_idx: 1,
                token_out_idx: 0,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
        ];
        let hop_gas = GAS_V2_HOP * 2;
        let expected = estimate_route_gas_from_hops_evm(hop_gas, 2, 2);
        assert_eq!(estimate_route_gas(&edges), expected);
        assert!(expected > hop_gas);
    }

    #[test]
    fn v2_hop_fails_closed_when_amount_exhausts_reserve_in() {
        use crate::core::types::V2PoolState;

        let state = PoolState::V2(V2PoolState {
            reserve0: U256::from(1_000_000u64),
            reserve1: U256::from(2_000_000u64),
            fee: U256::from(997u64),
            fee_denominator: U256::from(1_000u64),
            block_timestamp_last: 0,
        });
        let edge = Edge {
            pool_index: crate::core::types::PoolIndex(0),
            token_in: crate::core::types::TokenIndex(0),
            token_out: crate::core::types::TokenIndex(1),
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        assert!(simulate_hop_amount_out(&state, &edge, U256::from(999_999u64)).is_some());
        assert!(simulate_hop_amount_out(&state, &edge, U256::from(1_000_000u64)).is_none());
    }

    #[test]
    fn cl_hop_rejects_amount_above_explicit_shallow_cap() {
        use crate::core::types::{V3PoolState, V3Tick};
        use std::sync::Arc;

        let state = PoolState::V3(V3PoolState {
            sqrt_price_x96: U256::from(1u128 << 96),
            liquidity: 10_000_000_000_000u128,
            tick: 0,
            fee: U256::from(3000u32),
            tick_spacing: 60,
            unlocked: true,
            fee_protocol: 0,
            observation_cardinality: 1,
            ticks: Arc::from(vec![V3Tick {
                tick: -60,
                liquidity_gross: 10_000_000_000_000,
                liquidity_net: 10_000_000_000_000,
            }]),
        });
        let edge = Edge {
            pool_index: crate::core::types::PoolIndex(0),
            token_in: crate::core::types::TokenIndex(0),
            token_out: crate::core::types::TokenIndex(1),
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: true,
        };
        let cap = U256::from(1_000u64);
        assert!(simulate_hop_amount_out_with_cap(&state, &edge, cap, cap).is_some());
        assert!(simulate_hop_amount_out_with_cap(&state, &edge, cap + U256::ONE, cap).is_none());
    }

    #[test]
    fn shallow_caps_probe_each_token_once_per_route() {
        use crate::core::types::{TokenIndex, V3PoolState};
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let token_a = arena.register_token(Address::from([1u8; 20]));
        let token_b = arena.register_token(Address::from([2u8; 20]));
        let pool_0 = arena.register_pool(
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
                ticks: Arc::from(Vec::new()),
            })),
        );
        let pool_1 = arena.register_pool(
            Address::from([4u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(Vec::new()),
            })),
        );
        let edges = [
            Edge {
                pool_index: pool_0,
                token_in: token_a,
                token_out: token_b,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV3,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: pool_1,
                token_in: token_a,
                token_out: token_b,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV4,
                fee_bps: 30,
                zero_for_one: true,
            },
        ];
        let mut probes = 0u32;
        let caps = route_shallow_caps_with(&edges, |token: TokenIndex| {
            probes += 1;
            if token == token_a {
                U256::from(111u64)
            } else {
                U256::from(222u64)
            }
        });
        assert_eq!(probes, 1);
        assert_eq!(caps[0], U256::from(111u64));
        assert_eq!(caps[1], U256::from(111u64));
    }

    #[test]
    fn cl_amount_cap_none_when_ticks_present() {
        use crate::core::types::{V3PoolState, V3Tick};
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: u128::MAX / 2,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(
                    (1..=32)
                        .map(|step| V3Tick {
                            tick: -(step * 60),
                            liquidity_gross: 1_000_000,
                            liquidity_net: 0,
                        })
                        .collect::<Vec<_>>(),
                ),
            })),
        );
        let edges = [Edge {
            pool_index: pool,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: true,
        }];
        assert!(cl_amount_cap(&arena, &edges).is_none());
    }

    #[test]
    fn hop_fidelity_rejects_shallow_cl_on_intermediate_hop_amount() {
        use crate::core::types::V3PoolState;
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let t2 = arena.register_token(Address::from([3u8; 20]));
        let v2_pool = arena.register_pool(
            Address::from([4u8; 20]),
            Arc::new(PoolState::V2(crate::core::types::V2PoolState {
                reserve0: U256::from(1_000_000u64) * U256::from(10u128.pow(18)),
                reserve1: U256::from(1_000_000u64) * U256::from(10u128.pow(18)),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        );
        let v3_pool = arena.register_pool(
            Address::from([5u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 0,
                ticks: Arc::from(Vec::new()),
            })),
        );
        let edges = [
            Edge {
                pool_index: v2_pool,
                token_in: t0,
                token_out: t1,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: v3_pool,
                token_in: t1,
                token_out: t2,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV3,
                fee_bps: 30,
                zero_for_one: true,
            },
        ];
        let probe = crate::pipeline::spot_price::spot_probe_for_token(&arena, t0);
        let mut first_only = hop_amounts_zeroed(edges.len());
        first_only[0] = probe;
        assert!(!route_hop_fidelity_ok(&arena, &edges, &first_only));
        let mut hop_amounts = hop_amounts_zeroed(edges.len());
        hop_amounts[0] = probe;
        hop_amounts[1] = U256::from(10u128.pow(18));
        assert_eq!(
            route_hop_fidelity_reject(&arena, &edges, &hop_amounts),
            Some(HopFidelityReject::ShallowCl(1))
        );
    }

    #[test]
    fn hop_fidelity_rejects_loaded_cl_with_exhausted_tick_window_below_probe() {
        use crate::core::types::{V3PoolState, V3Tick};
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
                tick_spacing: 1,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![V3Tick {
                    tick: -1,
                    liquidity_gross: 1_000_000,
                    liquidity_net: 1_000_000,
                }]),
            })),
        );
        let edges = [Edge {
            pool_index: pool,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: true,
        }];
        let mut hop_amounts = hop_amounts_zeroed(edges.len());
        hop_amounts[0] = U256::from(100u64);

        assert_eq!(
            route_hop_fidelity_reject(&arena, &edges, &hop_amounts),
            Some(HopFidelityReject::ShallowCl(0))
        );
    }

    #[test]
    fn resim_fidelity_rejects_profit_drift() {
        let baseline = RouteSimulationResult {
            amount_in: U256::from(1000u64),
            amount_out: U256::from(1100u64),
            profit: U256::from(100u64),
            profitable: true,
            hop_amounts: hop_amounts_zeroed(1),
            total_gas: 0,
            hop_count: 1,
        };
        let refreshed = RouteSimulationResult {
            profit: U256::from(40u64),
            ..baseline.clone()
        };
        assert_eq!(
            route_resim_fidelity_reject(&baseline, &refreshed),
            Some("profit drift")
        );
    }

    #[test]
    fn resim_fidelity_rejects_hop_count_mismatch() {
        let baseline = RouteSimulationResult {
            amount_in: U256::from(1000u64),
            amount_out: U256::from(1100u64),
            profit: U256::from(100u64),
            profitable: true,
            hop_amounts: hop_amounts_zeroed(2),
            total_gas: 0,
            hop_count: 2,
        };
        let refreshed = RouteSimulationResult {
            hop_amounts: hop_amounts_zeroed(1),
            hop_count: 1,
            ..baseline.clone()
        };
        assert_eq!(
            route_resim_fidelity_reject(&baseline, &refreshed),
            Some("hop count mismatch")
        );
    }

    #[test]
    fn cl_amount_cap_is_zero_when_tickless() {
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
                observation_cardinality: 0,
                ticks: Arc::from(Vec::new()),
            })),
        );
        let edges = [Edge {
            pool_index: pool,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: true,
        }];
        assert_eq!(cl_amount_cap(&arena, &edges), Some(U256::ZERO));
        let mut hop_amounts = hop_amounts_zeroed(edges.len());
        hop_amounts[0] = crate::pipeline::spot_price::spot_probe_for_token(&arena, t0);
        assert_eq!(
            route_hop_fidelity_reject(&arena, &edges, &hop_amounts),
            Some(HopFidelityReject::ShallowCl(0))
        );
    }

    #[test]
    fn print_calldata_layout() {
        use crate::abis::{ExecutorCall, IArbExecutor};
        use crate::services::execution::calldata::{
            build_packed_route_payload, pack_executor_calls,
        };
        use alloy::sol_types::SolCall;

        let a1: Address = "0x0000000000000000000000000000010000000001"
            .parse()
            .expect("test address a1 should parse");
        let a2: Address = "0x0000000000000000000000000000010000000002"
            .parse()
            .expect("test address a2 should parse");
        let a3: Address = "0x0000000000000000000000000000010000000003"
            .parse()
            .expect("test address a3 should parse");

        let calls = vec![ExecutorCall {
            target: a1,
            value: U256::ZERO,
            data: vec![0xde, 0xad].into(),
        }];
        let packed_calls = pack_executor_calls(&calls).expect("test calls should pack");
        let route_hash = crate::services::execution::calldata::compute_route_hash(&packed_calls);
        let (packed_route, _) = build_packed_route_payload(
            a3,
            U256::from(1000u64),
            a2,
            U256::from(100u64),
            U256::from(9999999999u64),
            &calls,
        )
        .expect("test route payload should build");

        let cd = IArbExecutor::executeArbCall {
            packedRoute: packed_route.clone(),
        }
        .abi_encode();
        assert!(!cd.is_empty());
        assert_ne!(route_hash, alloy::primitives::B256::ZERO);
        assert!(!packed_route.is_empty());
    }
}
