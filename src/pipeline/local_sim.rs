use crate::core::constants::{
    GAS_BALANCER_HOP, GAS_CURVE_HOP, GAS_DODO_HOP, GAS_V2_HOP, GAS_V3_BASE, GAS_V4_BASE,
    GAS_WOOFI_HOP,
};
use crate::core::math::balancer::simulate_balancer_swap;
use crate::core::math::curve::get_curve_stable_amount_out;
use crate::core::math::dodo::get_dodo_amount_out;
use crate::core::math::uniswap_v2::simulate_v2_swap;
use crate::core::math::uniswap_v3::simulate_v3_swap;
use crate::core::math::woofi::get_woofi_amount_out;
use crate::core::types::{
    Edge, PoolState, ProtocolType, RouteSimulationResult, hop_amounts_zeroed,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::MinimalSimResult;
use alloy::primitives::U256;

use crate::services::execution::gas::estimate_route_gas_from_hops;

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

/// Conservative gas units for a full route (overhead + per-hop + tick premium for CL).
#[must_use]
pub fn estimate_route_gas(edges: &[Edge]) -> u32 {
    if edges.is_empty() {
        return crate::services::execution::gas::ROUTE_EXECUTION_GAS_OVERHEAD;
    }
    let hop_gas: u32 = edges.iter().map(|e| estimate_hop_gas(e.protocol)).sum();
    estimate_route_gas_from_hops(hop_gas, edges.len())
}

#[derive(Debug, Clone, Copy)]
struct HopResult {
    amount_out: U256,
    gas: u32,
}

fn simulate_hop(state: &PoolState, edge: &Edge, amount_in: U256) -> Option<HopResult> {
    if amount_in.is_zero() {
        return Some(HopResult {
            amount_out: U256::ZERO,
            gas: 0,
        });
    }

    match (state, edge.protocol) {
        (PoolState::V2(s), ProtocolType::UniswapV2) => {
            let out = simulate_v2_swap(s, amount_in, edge.zero_for_one, Some(edge.fee_bps));
            Some(HopResult {
                amount_out: out,
                gas: GAS_V2_HOP,
            })
        }
        (PoolState::V3(s), ProtocolType::UniswapV3)
        | (PoolState::V4(s), ProtocolType::UniswapV4) => {
            let r = simulate_v3_swap(s, amount_in, edge.zero_for_one, Some(edge.fee_bps));
            if r.shallow && amount_in > crate::pipeline::spot_price::SPOT_PROBE {
                return None;
            }
            Some(HopResult {
                amount_out: r.amount_out,
                gas: r.gas_estimate,
            })
        }
        (PoolState::Curve(s), ProtocolType::CurveStable) => {
            let out = get_curve_stable_amount_out(
                s,
                amount_in,
                edge.token_in_idx as usize,
                edge.token_out_idx as usize,
            );
            Some(HopResult {
                amount_out: out,
                gas: GAS_CURVE_HOP,
            })
        }
        (PoolState::Curve(s), ProtocolType::CurveCrypto) => {
            let out = crate::core::math::curve_crypto::get_curve_crypto_amount_out(
                s,
                amount_in,
                edge.token_in_idx as usize,
                edge.token_out_idx as usize,
            );
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
    simulate_hop(state, edge, amount_in).map(|h| h.amount_out)
}

fn cl_hop_tickless(state: &PoolState) -> bool {
    matches!(
        state,
        PoolState::V3(s) | PoolState::V4(s) if s.ticks.is_empty()
    )
}

/// Max trade size with faithful CL simulation. `None` = full tick coverage.
/// `Some(SPOT_PROBE)` = at least one CL hop lacks ticks (shallow above probe).
#[must_use]
pub fn cl_amount_cap(arena: &StateArena, edges: &[Edge]) -> Option<U256> {
    let mut tickless = false;
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
            tickless = true;
        }
    }
    tickless.then_some(crate::pipeline::spot_price::SPOT_PROBE)
}

/// Max gross-profit erosion (bps) tolerated between eval and post-refresh resim.
const RESIM_PROFIT_DRIFT_BPS: u64 = 5000;
/// Max per-hop amount drift (bps) tolerated between eval and post-refresh resim.
const RESIM_HOP_DRIFT_BPS: u64 = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HopFidelityReject {
    MissingPool(usize),
    PoolLocked(usize),
    ShallowCl(usize),
}

fn cl_hop_shallow_at_amount(state: &PoolState, edge: &Edge, amount_in: U256) -> bool {
    match state {
        PoolState::V3(s) | PoolState::V4(s) if s.ticks.is_empty() => true,
        PoolState::V3(s) | PoolState::V4(s) => {
            simulate_v3_swap(s, amount_in, edge.zero_for_one, Some(edge.fee_bps)).shallow
        }
        _ => false,
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

/// Per-hop CL fidelity: each V3/V4 hop is checked at its simulated input, not route start.
#[must_use]
pub fn route_hop_fidelity_ok(
    arena: &StateArena,
    edges: &[Edge],
    hop_amounts: &[U256],
    spot_probe: U256,
) -> bool {
    route_hop_fidelity_reject(arena, edges, hop_amounts, spot_probe).is_none()
}

/// First CL hop that fails tick-depth or tradability checks above `spot_probe`, if any.
#[must_use]
pub fn route_hop_fidelity_reject(
    arena: &StateArena,
    edges: &[Edge],
    hop_amounts: &[U256],
    spot_probe: U256,
) -> Option<HopFidelityReject> {
    for (i, edge) in edges.iter().enumerate() {
        if !matches!(
            edge.protocol,
            ProtocolType::UniswapV3 | ProtocolType::UniswapV4
        ) {
            continue;
        }
        let amount_in = hop_amounts.get(i).copied().unwrap_or(U256::ZERO);
        if amount_in <= spot_probe {
            continue;
        }
        let Some(state) = arena.pool_state(edge.pool_index) else {
            return Some(HopFidelityReject::MissingPool(i));
        };
        if !state.is_tradable() {
            return Some(HopFidelityReject::PoolLocked(i));
        }
        if cl_hop_shallow_at_amount(state, edge, amount_in) {
            return Some(HopFidelityReject::ShallowCl(i));
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
    if refreshed.profit.is_zero() {
        return Some("resim unprofitable");
    }
    if baseline.hop_amounts.len() != refreshed.hop_amounts.len() {
        return Some("hop count mismatch");
    }
    if !baseline.profit.is_zero() {
        let min_profit = baseline.profit * U256::from(10_000u64 - RESIM_PROFIT_DRIFT_BPS)
            / U256::from(10_000u64);
        if refreshed.profit < min_profit {
            return Some("profit drift");
        }
    }
    for i in 0..baseline.hop_amounts.len() {
        let b = baseline.hop_amounts[i];
        let r = refreshed.hop_amounts[i];
        if !hop_amount_within_drift(b, r, RESIM_HOP_DRIFT_BPS) {
            return Some("hop amount drift");
        }
    }
    None
}

#[must_use]
pub fn simulate_route_minimal(
    arena: &StateArena,
    edges: &[Edge],
    amount_in: U256,
) -> Option<MinimalSimResult> {
    let mut current = amount_in;
    let mut total_gas = 0u32;

    for edge in edges {
        let state = arena.pool_state(edge.pool_index)?;
        if !state.is_tradable() {
            return None;
        }
        let hop = simulate_hop(state, edge, current)?;
        if current > U256::ZERO && hop.amount_out.is_zero() {
            return None;
        }
        current = hop.amount_out;
        total_gas += hop.gas;
    }

    let profit = current.saturating_sub(amount_in);
    let total_gas = estimate_route_gas_from_hops(total_gas, edges.len());
    Some(MinimalSimResult {
        profit,
        amount_out: current,
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
    let mut hop_amounts = hop_amounts_zeroed(hop_count);
    hop_amounts[0] = amount_in;
    let mut total_gas = 0u32;
    let mut current = amount_in;

    for (i, edge) in edges.iter().enumerate() {
        let state = arena.pool_state(edge.pool_index)?;
        if !state.is_tradable() {
            return None;
        }
        let hop = simulate_hop(state, edge, current)?;
        if current > U256::ZERO && hop.amount_out.is_zero() {
            return None;
        }
        current = hop.amount_out;
        hop_amounts[i + 1] = current;
        total_gas += hop.gas;
    }

    let profit = current.saturating_sub(amount_in);
    let total_gas = estimate_route_gas_from_hops(total_gas, hop_count);
    Some(RouteSimulationResult {
        amount_in,
        amount_out: current,
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
    fn test_estimate_hop_gas_v2() {
        assert!(estimate_hop_gas(ProtocolType::UniswapV2) > 0);
    }

    #[test]
    fn v3_route_gas_does_not_double_count_base_cost() {
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
                ticks: Arc::from(Vec::new()),
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

        let amount_in = crate::pipeline::spot_price::SPOT_PROBE;
        let sim = simulate_route_minimal(&arena, &[edge], amount_in).expect("simulation");
        let expected = crate::services::execution::gas::estimate_route_gas_from_hops(
            crate::core::constants::GAS_V3_BASE,
            1,
        );
        assert_eq!(sim.total_gas, expected);
    }

    #[test]
    fn route_gas_formula_matches_executor_overhead_model() {
        use crate::core::constants::GAS_V2_HOP;
        use crate::services::execution::gas::{
            PER_HOP_EXECUTOR_GAS_OVERHEAD, ROUTE_EXECUTION_GAS_OVERHEAD,
        };

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
        let expected =
            GAS_V2_HOP * 2 + ROUTE_EXECUTION_GAS_OVERHEAD + 2 * PER_HOP_EXECUTOR_GAS_OVERHEAD;
        assert_eq!(estimate_route_gas(&edges), expected);
        // Typical 2-hop Polygon flash route: ~350k–550k before GasOracle uplift.
        assert!((350_000..=550_000).contains(&expected));
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
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![V3Tick {
                    tick: -60,
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
        let probe = crate::pipeline::spot_price::SPOT_PROBE;
        // First assertion: only first hop matters (matches old route_cl_fidelity_ok semantics)
        let mut first_only = hop_amounts_zeroed(edges.len());
        first_only[0] = probe;
        assert!(route_hop_fidelity_ok(&arena, &edges, &first_only, probe));
        let mut hop_amounts = hop_amounts_zeroed(edges.len());
        hop_amounts[0] = probe;
        hop_amounts[1] = U256::from(10u128.pow(18));
        assert_eq!(
            route_hop_fidelity_reject(&arena, &edges, &hop_amounts, probe),
            Some(HopFidelityReject::ShallowCl(1))
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
    fn cl_amount_cap_spot_probe_when_tickless() {
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
        assert_eq!(
            cl_amount_cap(&arena, &edges),
            Some(crate::pipeline::spot_price::SPOT_PROBE)
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
