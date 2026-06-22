use alloy::dyn_abi::DynSolValue;
use alloy::primitives::{Address, I256, Signed, U256, Uint};
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, IUniswapV4PoolManager, V4PoolKey};
use crate::core::constants::UNISWAP_V4_POOL_MANAGER;
use crate::core::math::uniswap_v3::resolve_v3_fee_pips;
use crate::core::types::PoolState;
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::approvals::encode_approve_if_needed;
use crate::services::execution::calldata::types::CalldataHop;

/// Encode a Uniswap V4 hop into executor calls
///
/// Returns a vector containing:
/// 1. An approval call if needed (via IArbExecutor.approveIfNeeded)
/// 2. A lock call to the V4 PoolManager
pub fn encode_v4_hop(
    hop: &CalldataHop,
    recipient: Address,
    arena: &StateArena,
) -> anyhow::Result<Vec<ExecutorCall>> {
    use crate::core::math::tick_math::{MAX_SQRT_RATIO, MIN_SQRT_RATIO};

    let pool_manager: Address = UNISWAP_V4_POOL_MANAGER;
    let (fee, tick_spacing, hooks) = v4_static_fields(arena, hop);

    if hooks != Address::ZERO {
        anyhow::bail!("v4 hook pools are not supported");
    }

    let (pool_key, zero_for_one) =
        build_v4_pool_key(hop.token_in, hop.token_out, fee, tick_spacing, hooks);
    let sqrt_limit = if zero_for_one {
        MIN_SQRT_RATIO + U256::from(1u8)
    } else {
        MAX_SQRT_RATIO - U256::from(1u8)
    };

    let lock_inner = DynSolValue::Tuple(vec![
        DynSolValue::Tuple(vec![
            DynSolValue::Address(pool_key.currency0),
            DynSolValue::Address(pool_key.currency1),
            DynSolValue::Uint(U256::from(pool_key.fee), 24),
            DynSolValue::Int(I256::from(pool_key.tickSpacing), 24),
            DynSolValue::Address(pool_key.hooks),
        ]),
        DynSolValue::Bool(zero_for_one),
        DynSolValue::Int(I256::ZERO - I256::from(hop.amount_in), 128),
        DynSolValue::Uint(sqrt_limit, 160),
    ])
    .abi_encode();

    let lock = IUniswapV4PoolManager::lockCall {
        data: lock_inner.into(),
    };

    Ok(vec![
        encode_approve_if_needed(recipient, hop.token_in, pool_manager, hop.amount_in),
        ExecutorCall {
            target: pool_manager,
            value: U256::ZERO,
            data: lock.abi_encode().into(),
        },
    ])
}

/// Helper: Build V4 pool key from token pair
fn build_v4_pool_key(
    token_in: Address,
    token_out: Address,
    fee: u32,
    tick_spacing: i32,
    hooks: Address,
) -> (V4PoolKey, bool) {
    let (currency0, currency1) = if token_in < token_out {
        (token_in, token_out)
    } else {
        (token_out, token_in)
    };
    let zero_for_one = token_in == currency0;
    (
        V4PoolKey {
            currency0,
            currency1,
            fee: Uint::from(fee),
            tickSpacing: Signed::try_from(tick_spacing).unwrap_or(Signed::ZERO),
            hooks,
        },
        zero_for_one,
    )
}

/// Helper: Get static fields for V4 pool
fn v4_static_fields(arena: &StateArena, hop: &CalldataHop) -> (u32, i32, Address) {
    let hooks = hop.hooks.unwrap_or(Address::ZERO);
    match arena.pool_state(hop.edge.pool_index) {
        Some(PoolState::V4(s)) => {
            let fee = resolve_v3_fee_pips(s.fee, Some(hop.edge.fee_bps))
                .min(U256::from(0xffffffu32))
                .to::<u32>();
            (fee, s.tick_spacing, hooks)
        }
        _ => (hop.edge.fee_bps.saturating_mul(100), 60, hooks),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{Edge, PoolIndex, TokenIndex};

    fn create_test_hop() -> CalldataHop {
        CalldataHop {
            edge: Edge {
                pool_index: PoolIndex(0),
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: crate::core::types::ProtocolType::UniswapV4,
                fee_bps: 100,
                zero_for_one: true,
            },
            pool_address: Address::repeat_byte(0x01),
            token_in: Address::repeat_byte(0x02),
            token_out: Address::repeat_byte(0x03),
            amount_in: U256::from(1000),
            amount_out: U256::from(900),
            pool_id: None,
            protocol_label: None,
            router: None,
            hooks: None,
        }
    }

    #[test]
    fn encode_v4_hop_returns_two_calls() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_v4_hop(&hop, recipient, &arena);

        assert!(result.is_ok());
        let calls = result.unwrap();
        assert_eq!(
            calls.len(),
            2,
            "V4 hop should generate exactly 2 calls (approval + lock)"
        );
    }

    #[test]
    fn encode_v4_hop_first_call_is_approval() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_v4_hop(&hop, recipient, &arena);
        let calls = result.unwrap();

        // First call should be approval to executor
        assert_eq!(
            calls[0].target, recipient,
            "First call should target executor (recipient)"
        );
        assert_eq!(
            calls[0].value,
            U256::ZERO,
            "Approval should have zero value"
        );
    }

    #[test]
    fn encode_v4_hop_second_call_is_lock() {
        let hop = create_test_hop();
        let recipient = Address::repeat_byte(0x04);
        let arena = StateArena::new();

        let result = encode_v4_hop(&hop, recipient, &arena);
        let calls = result.unwrap();

        // Second call should be to pool manager
        let pool_manager: Address = UNISWAP_V4_POOL_MANAGER;
        assert_eq!(
            calls[1].target, pool_manager,
            "Second call should target V4 PoolManager"
        );
        assert_eq!(
            calls[1].value,
            U256::ZERO,
            "Lock call should have zero value"
        );
    }

    #[test]
    fn build_v4_pool_key_orders_currencies() {
        let token1 = Address::repeat_byte(0x01);
        let token2 = Address::repeat_byte(0x02);

        let (key, _) = build_v4_pool_key(token2, token1, 3000, 60, Address::ZERO);

        // Currency0 should be less than currency1
        assert!(key.currency0 < key.currency1);
    }

    #[test]
    fn build_v4_pool_key_detects_zero_for_one() {
        let token1 = Address::repeat_byte(0x01);
        let token2 = Address::repeat_byte(0x02);

        let (_, zero_for_one_case1) = build_v4_pool_key(token1, token2, 3000, 60, Address::ZERO);
        let (_, zero_for_one_case2) = build_v4_pool_key(token2, token1, 3000, 60, Address::ZERO);

        // One should be true and one false
        assert_ne!(zero_for_one_case1, zero_for_one_case2);
    }
}
