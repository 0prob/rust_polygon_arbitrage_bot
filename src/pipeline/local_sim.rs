use ruint::aliases::U256;
use tracing::instrument;

use crate::core::math::balancer::simulate_balancer_swap;
use crate::core::math::curve::get_curve_stable_amount_out;
use crate::core::math::dodo::simulate_dodo_swap;
use crate::core::math::uniswap_v2::simulate_v2_swap;
use crate::core::math::uniswap_v3::simulate_v3_swap;
use crate::core::math::woofi::simulate_woofi_swap;
use crate::core::types::{Edge, PoolState, ProtocolType, RouteSimulationResult};
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::MinimalSimResult;

const GAS_V2: u32 = 100_000;
const GAS_V3_BASE: u32 = 185_000;
const GAS_V4_BASE: u32 = 200_000;
const GAS_CURVE: u32 = 250_000;
const GAS_BALANCER: u32 = 220_000;
const GAS_DODO: u32 = 180_000;
const GAS_WOOFI: u32 = 150_000;
use crate::services::execution::gas::estimate_route_gas_from_hops;

/// Per-hop gas estimate for route ranking (matches simulation constants).
pub fn estimate_hop_gas(protocol: ProtocolType) -> u32 {
    match protocol {
        ProtocolType::UniswapV2 => GAS_V2,
        ProtocolType::UniswapV3 => GAS_V3_BASE,
        ProtocolType::UniswapV4 => GAS_V4_BASE,
        ProtocolType::CurveStable | ProtocolType::CurveCrypto => GAS_CURVE,
        ProtocolType::BalancerV2 => GAS_BALANCER,
        ProtocolType::Dodo => GAS_DODO,
        ProtocolType::Woofi => GAS_WOOFI,
    }
}

/// Conservative gas units for a full route (overhead + per-hop + tick premium for CL).
pub fn estimate_route_gas(edges: &[Edge]) -> u32 {
    if edges.is_empty() {
        return crate::services::execution::gas::ROUTE_EXECUTION_GAS_OVERHEAD;
    }
    let hop_gas: u32 = edges.iter().map(|e| estimate_hop_gas(e.protocol)).sum();
    finalize_route_gas(hop_gas, edges.len())
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
                gas: GAS_V2,
            })
        }
        (PoolState::V3(s), ProtocolType::UniswapV3)
        | (PoolState::V4(s), ProtocolType::UniswapV4) => {
            let r = simulate_v3_swap(s, amount_in, edge.zero_for_one, Some(edge.fee_bps));
            let base_gas = if edge.protocol == ProtocolType::UniswapV3 {
                GAS_V3_BASE
            } else {
                GAS_V4_BASE
            };
            Some(HopResult {
                amount_out: r.amount_out,
                gas: base_gas + r.gas_estimate,
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
                gas: GAS_CURVE,
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
                gas: GAS_CURVE,
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
                gas: GAS_BALANCER,
            })
        }
        (PoolState::Dodo(s), ProtocolType::Dodo) => {
            let out = simulate_dodo_swap(s, amount_in, edge.zero_for_one);
            Some(HopResult {
                amount_out: out,
                gas: GAS_DODO,
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
                simulate_woofi_swap(s, amount_in, in_is_quote, out_is_quote, base_in, base_out);
            Some(HopResult {
                amount_out: out,
                gas: GAS_WOOFI,
            })
        }
        _ => None,
    }
}

fn finalize_route_gas(hop_gas: u32, hop_count: usize) -> u32 {
    estimate_route_gas_from_hops(hop_gas, hop_count)
}

/// Quote a single hop output for calldata encoding (reuses pipeline math).
pub fn simulate_hop_amount_out(state: &PoolState, edge: &Edge, amount_in: U256) -> Option<U256> {
    simulate_hop(state, edge, amount_in).map(|h| h.amount_out)
}

/// Fast round-trip simulation for Brent / profit probes (no path metadata).
#[cfg_attr(
    feature = "trace-sim",
    instrument(
        skip(arena, edges),
        level = "trace",
        fields(hop_count = edges.len(), amount_in = %amount_in, profit = tracing::field::Empty, total_gas = tracing::field::Empty)
    )
)]
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
    let total_gas = finalize_route_gas(total_gas, edges.len());
    #[cfg(feature = "trace-sim")]
    {
        tracing::Span::current().record("profit", tracing::field::display(&profit));
        tracing::Span::current().record("total_gas", total_gas);
    }
    Some(MinimalSimResult {
        profit,
        amount_out: current,
        total_gas,
    })
}

/// Full hop trace for calldata encoding and profit assessment.
#[instrument(
    skip(arena, edges),
    fields(hop_count = edges.len(), amount_in = %amount_in, profit = tracing::field::Empty, total_gas = tracing::field::Empty)
)]
pub fn simulate_route_detailed(
    arena: &StateArena,
    edges: &[Edge],
    amount_in: U256,
) -> Option<RouteSimulationResult> {
    let hop_count = edges.len();
    let mut hop_amounts = vec![U256::ZERO; hop_count + 1];
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
    let total_gas = finalize_route_gas(total_gas, hop_count);
    tracing::Span::current().record("profit", tracing::field::display(&profit));
    tracing::Span::current().record("total_gas", total_gas);
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
    use crate::core::types::V2PoolState;
    use alloy::primitives::Address;

    #[test]
    fn round_trip_v2_triangle() {
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

        let p01 = arena.register_pool(Address::repeat_byte(0x10), v2(reserve, reserve));
        let p12 = arena.register_pool(Address::repeat_byte(0x11), v2(reserve, reserve));
        let p20 = arena.register_pool(Address::repeat_byte(0x12), v2(reserve, reserve));

        let edges = vec![
            Edge {
                pool_index: p01,
                token_in: t0,
                token_out: t1,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: p12,
                token_in: t1,
                token_out: t2,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: p20,
                token_in: t2,
                token_out: t0,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
        ];

        let amount_in = U256::from(10u128).pow(U256::from(18));
        let sim = simulate_route_minimal(&arena, &edges, amount_in).expect("sim");
        assert!(sim.amount_out > U256::ZERO);
        assert!(sim.total_gas > crate::services::execution::gas::ROUTE_EXECUTION_GAS_OVERHEAD);
    }
}
