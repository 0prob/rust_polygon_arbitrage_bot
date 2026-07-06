use alloy::primitives::{Address, U256};
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, ICurveCryptoPool, ICurveStableNgPool, ICurveStablePool};
use crate::core::types::ProtocolType;
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::CalldataHop;
use crate::services::execution::calldata::approvals::encode_approve_if_needed;

use super::shared::{compute_min_out, curve_uses_receiver};

/// Encode a Curve pool hop into executor calls
///
/// Supports three Curve pool types:
/// - StableSwap_NG (uses receiver parameter)
/// - Crypto pools (different interface)
/// - Standard StableSwap pools
pub fn encode_curve_hop(
    hop: &CalldataHop,
    recipient: Address,
    arena: &StateArena,
    slippage_bps: u64,
) -> anyhow::Result<Vec<ExecutorCall>> {
    if hop.edge.token_in_idx == hop.edge.token_out_idx {
        anyhow::bail!("curve hop token indices must differ");
    }

    let min_dy = compute_min_out(arena, hop, slippage_bps, "curve")?;

    let i = hop.edge.token_in_idx as i128;
    let j = hop.edge.token_out_idx as i128;
    let mut calls = vec![encode_approve_if_needed(
        hop.token_in,
        hop.pool_address,
        hop.amount_in,
    )];

    let exchange_data = if curve_uses_receiver(hop.pool_type.as_deref()) {
        ICurveStableNgPool::exchangeCall {
            i,
            j,
            dx: hop.amount_in,
            min_dy,
            receiver: recipient,
        }
        .abi_encode()
    } else if matches!(hop.edge.protocol, ProtocolType::CurveCrypto) {
        ICurveCryptoPool::exchangeCall {
            i: U256::from(hop.edge.token_in_idx),
            j: U256::from(hop.edge.token_out_idx),
            dx: hop.amount_in,
            min_dy,
        }
        .abi_encode()
    } else {
        ICurveStablePool::exchangeCall {
            i,
            j,
            dx: hop.amount_in,
            min_dy,
        }
        .abi_encode()
    };

    calls.push(ExecutorCall {
        target: hop.pool_address,
        value: U256::ZERO,
        data: exchange_data.into(),
    });
    Ok(calls)
}
