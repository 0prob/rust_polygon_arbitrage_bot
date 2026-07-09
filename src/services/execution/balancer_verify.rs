use std::time::Duration;

use alloy::network::Ethereum;
use alloy::primitives::{Address, I256, U256};
use alloy::providers::Provider;
use alloy::sol_types::SolCall;
use tokio::time::timeout;

use crate::abis::IBalancerVault;
use crate::core::constants::BALANCER_VAULT;
use crate::services::execution::calldata::CalldataHop;
use crate::services::execution::calldata::encoders::balancer::build_balancer_batch_swap_request;
use crate::services::execution::profit::on_chain_min_profit_for_route;
use crate::core::types::FlashLoanSource;

const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
const BALANCER_GIVEN_IN: u8 = 0;

fn positive_delta(delta: I256) -> Option<U256> {
    if delta <= I256::ZERO {
        return None;
    }
    U256::try_from(delta).ok()
}

/// On-chain profit for an `executeArbDirect` batch route via vault `queryBatchSwap`.
pub async fn query_balancer_batch_profit<P: Provider<Ethereum>>(
    provider: &P,
    executor: Address,
    hops: &[CalldataHop],
    profit_token: Address,
) -> Option<U256> {
    let req = build_balancer_batch_swap_request(hops, executor).ok()?;
    let idx = req.assets.iter().position(|a| *a == profit_token)?;
    let call = IBalancerVault::queryBatchSwapCall {
        kind: BALANCER_GIVEN_IN,
        swaps: req.swaps,
        assets: req.assets,
        funds: req.funds,
    };
    let tx = alloy::rpc::types::TransactionRequest::default()
        .to(BALANCER_VAULT)
        .input(call.abi_encode().into());
    let output = match timeout(QUERY_TIMEOUT, provider.call(tx)).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(e)) => {
            crate::debug!("queryBatchSwap RPC error: {e:#}");
            return None;
        }
        Err(_) => {
            crate::debug!("queryBatchSwap timed out after {}s", QUERY_TIMEOUT.as_secs());
            return None;
        }
    };
    let deltas = IBalancerVault::queryBatchSwapCall::abi_decode_returns(&output).ok()?;
    let delta = deltas.get(idx).copied()?;
    positive_delta(delta).or_else(|| {
        crate::debug!("queryBatchSwap non-positive delta for profit token: {delta}");
        None
    })
}

/// Reject Direct routes when vault simulation shows less profit than calldata minProfit floor.
#[must_use]
pub fn batch_profit_covers_min(
    on_chain_profit: U256,
    modeled_gross: U256,
    amount_in: U256,
    slippage_bps: u64,
) -> bool {
    let Some(min_profit) =
        on_chain_min_profit_for_route(modeled_gross, amount_in, slippage_bps, FlashLoanSource::Direct)
    else {
        return false;
    };
    on_chain_profit >= min_profit
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positive_delta_rejects_non_positive() {
        assert_eq!(positive_delta(I256::ZERO), None);
        assert_eq!(positive_delta(I256::MINUS_ONE), None);
        assert_eq!(positive_delta(I256::ONE), Some(U256::from(1u8)));
    }
}