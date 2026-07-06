use alloy::primitives::{Address, U256};
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, IDodoPool};
use crate::services::execution::calldata::CalldataHop;
use crate::services::execution::calldata::approvals::encode_token_transfer;

/// Encode a DODO pool hop into executor calls
///
/// Returns a vector containing:
/// 1. A transfer call to move tokens to the pool (via transferAll or explicit transfer)
/// 2. A swap call to the DODO pool (sellBase or sellQuote depending on direction)
pub fn encode_dodo_hop(
    hop: &CalldataHop,
    recipient: Address,
    use_transfer_all: bool,
) -> anyhow::Result<Vec<ExecutorCall>> {
    Ok(vec![
        encode_token_transfer(
            recipient,
            hop.token_in,
            hop.pool_address,
            hop.amount_in,
            use_transfer_all,
        ),
        ExecutorCall {
            target: hop.pool_address,
            value: U256::ZERO,
            data: if hop.edge.zero_for_one {
                IDodoPool::sellBaseCall { to: recipient }.abi_encode()
            } else {
                IDodoPool::sellQuoteCall { to: recipient }.abi_encode()
            }
            .into(),
        },
    ])
}
