use ruint::aliases::U256;

use crate::core::constants::BPS_SCALE;
use crate::core::types::Edge;
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::simulate_route_minimal;
use crate::pipeline::types::MinimalSimResult;

fn marginal_shortfall_bps(base_profit: U256, base_amount: U256, probe_profit: U256, probe_amount: U256) -> u64 {
    if base_profit.is_zero() || base_amount.is_zero() || probe_profit.is_zero() {
        return 10_000;
    }
    let base_marginal = (base_profit * U256::from(1_000_000u64)) / base_amount;
    let probe_marginal = (probe_profit * U256::from(1_000_000u64)) / probe_amount;
    if probe_marginal >= base_marginal {
        return 0;
    }
    let shortfall = base_marginal - probe_marginal;
    let bps = (shortfall * BPS_SCALE / base_marginal.max(U256::from(1u8)))
        .min(BPS_SCALE - U256::from(1u8));
    u64::try_from(bps).unwrap_or(10_000)
}

/// Probe +1% input size and measure profit-per-wei degradation (depth-based impact).
pub fn depth_impact_slippage_bps(arena: &StateArena, edges: &[Edge], amount_in: U256) -> u64 {
    depth_impact_slippage_bps_with_base(arena, edges, amount_in, None)
}

/// Like [`depth_impact_slippage_bps`] but reuses a known base simulation when available.
pub fn depth_impact_slippage_bps_with_base(
    arena: &StateArena,
    edges: &[Edge],
    amount_in: U256,
    base_sim: Option<&MinimalSimResult>,
) -> u64 {
    if amount_in.is_zero() || edges.is_empty() {
        return 0;
    }

    let base_profit = if let Some(sim) = base_sim {
        if sim.profit.is_zero() {
            return 10_000;
        }
        sim.profit
    } else {
        let Some(base) = simulate_route_minimal(arena, edges, amount_in) else {
            return 10_000;
        };
        if base.profit.is_zero() {
            return 10_000;
        }
        base.profit
    };

    let probe_in = amount_in.saturating_mul(U256::from(10_100u64)) / BPS_SCALE;
    if probe_in == amount_in {
        return 0;
    }
    let Some(probe) = simulate_route_minimal(arena, edges, probe_in) else {
        return 10_000;
    };

    marginal_shortfall_bps(base_profit, amount_in, probe.profit, probe_in)
}

pub fn effective_slippage_bps(configured_bps: u64, depth_bps: u64) -> u64 {
    configured_bps.max(depth_bps).min(9_999)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{Edge, PoolState, ProtocolType, V2PoolState};
    use alloy::primitives::Address;

    #[test]
    fn zero_on_symmetric_liquidity() {
        let mut arena = StateArena::new();
        let t0 = arena.register_token(Address::repeat_byte(1));
        let t1 = arena.register_token(Address::repeat_byte(2));
        let reserve = U256::from(10u128).pow(U256::from(24));
        let p = arena.register_pool(
            Address::repeat_byte(0x10),
            PoolState::V2(V2PoolState {
                reserve0: reserve,
                reserve1: reserve,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
        let edges = vec![
            Edge {
                pool_index: p,
                token_in: t0,
                token_out: t1,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: p,
                token_in: t1,
                token_out: t0,
                token_in_idx: 1,
                token_out_idx: 0,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
        ];
        let amount = U256::from(10u128).pow(U256::from(18));
        let depth = depth_impact_slippage_bps(&arena, &edges, amount);
        // Round-trip with fees is unprofitable (profit=0) → max conservative slippage.
        assert_eq!(depth, 10_000);
    }
}
