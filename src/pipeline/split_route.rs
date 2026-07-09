use alloy::primitives::U256;

use crate::core::types::Edge;
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::simulate_hop_amount_out;
/// Evaluate splitting `amount_in` across two parallel hops (same `token_in` → `token_out`).
#[must_use]
pub fn simulate_two_way_split(
    arena: &StateArena,
    edge_a: &Edge,
    edge_b: &Edge,
    amount_in: U256,
    split_to_b_bps: u64,
) -> Option<U256> {
    if amount_in.is_zero() || split_to_b_bps > 10_000 {
        return None;
    }
    if edge_a.token_in != edge_b.token_in || edge_a.token_out != edge_b.token_out {
        return None;
    }
    let to_b = amount_in * U256::from(split_to_b_bps) / U256::from(10_000u64);
    let to_a = amount_in.saturating_sub(to_b);
    let state_a = arena.pool_state(edge_a.pool_index)?;
    let state_b = arena.pool_state(edge_b.pool_index)?;
    let out_a = simulate_hop_amount_out(state_a, edge_a, to_a)?;
    let out_b = simulate_hop_amount_out(state_b, edge_b, to_b)?;
    Some(out_a.saturating_add(out_b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::constants::MIN_HOP_TOKEN_BALANCE;
    use crate::core::types::{Edge, PoolState, ProtocolType, V2PoolState};
    use alloy::primitives::Address;
    use std::sync::Arc;

    #[test]
    fn split_uses_both_pools() {
        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let p0 = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: MIN_HOP_TOKEN_BALANCE * U256::from(1000u64),
                reserve1: MIN_HOP_TOKEN_BALANCE * U256::from(2000u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1000u64),
                block_timestamp_last: 0,
            })),
        );
        let p1 = arena.register_pool(
            Address::from([4u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: MIN_HOP_TOKEN_BALANCE * U256::from(1500u64),
                reserve1: MIN_HOP_TOKEN_BALANCE * U256::from(2500u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1000u64),
                block_timestamp_last: 0,
            })),
        );
        let edge_a = Edge {
            pool_index: p0,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let edge_b = Edge {
            pool_index: p1,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let amount = U256::from(10u128.pow(18));
        let single = simulate_hop_amount_out(arena.pool_state(p0).expect("pool"), &edge_a, amount)
            .expect("single");
        let split = simulate_two_way_split(&arena, &edge_a, &edge_b, amount, 5000).expect("split");
        assert!(split > U256::ZERO);
        assert!(split >= single / U256::from(2u64));
    }
}
