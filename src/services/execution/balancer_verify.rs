use std::time::Duration;

use alloy::network::Ethereum;
use alloy::primitives::{Address, I256, U256};
use alloy::providers::Provider;
use alloy::sol_types::SolCall;
use tokio::time::timeout;

use crate::abis::IBalancerVault;
use crate::core::constants::BALANCER_VAULT;
use crate::core::math::balancer::exceeds_balancer_max_in_ratio;
use crate::core::types::{FlashLoanSource, PoolState};
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::CalldataHop;
use crate::services::execution::calldata::encoders::balancer::build_balancer_batch_swap_request;
use crate::services::execution::profit::on_chain_min_profit_for_route;

const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
const BALANCER_GIVEN_IN: u8 = 0;

fn positive_delta(delta: I256) -> Option<U256> {
    if delta <= I256::ZERO {
        return None;
    }
    U256::try_from(delta).ok()
}

/// Outcome of vault `queryBatchSwap` simulation for an `executeArbDirect` batch route.
pub enum BatchQueryOutcome {
    Profit(U256),
    NonPositiveDelta(I256),
    RpcError(String),
    Timeout,
    BuildFailed,
    DecodeFailed,
}

/// True when every hop amount stays within the vault `MAX_IN_RATIO` (30%) limit.
#[must_use]
pub fn balancer_batch_within_max_in_ratio(arena: &StateArena, hops: &[CalldataHop]) -> bool {
    hops.iter().all(|hop| {
        let Some(PoolState::Balancer(state)) = arena.pool_state(hop.edge.pool_index) else {
            return false;
        };
        let in_idx = hop.edge.token_in_idx as usize;
        state
            .balances
            .get(in_idx)
            .is_some_and(|bal| !exceeds_balancer_max_in_ratio(hop.amount_in, *bal))
    })
}

/// On-chain profit for an `executeArbDirect` batch route via vault `queryBatchSwap`.
pub async fn query_balancer_batch_profit<P: Provider<Ethereum>>(
    provider: &P,
    executor: Address,
    hops: &[CalldataHop],
    profit_token: Address,
) -> BatchQueryOutcome {
    let Some(req) = build_balancer_batch_swap_request(hops, executor).ok() else {
        return BatchQueryOutcome::BuildFailed;
    };
    let Some(idx) = req.assets.iter().position(|a| *a == profit_token) else {
        return BatchQueryOutcome::BuildFailed;
    };
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
        Ok(Err(e)) => return BatchQueryOutcome::RpcError(format!("{e:#}")),
        Err(_) => return BatchQueryOutcome::Timeout,
    };
    let Ok(deltas) = IBalancerVault::queryBatchSwapCall::abi_decode_returns(&output) else {
        return BatchQueryOutcome::DecodeFailed;
    };
    let Some(delta) = deltas.get(idx).copied() else {
        return BatchQueryOutcome::DecodeFailed;
    };
    match positive_delta(delta) {
        Some(profit) => BatchQueryOutcome::Profit(profit),
        None => BatchQueryOutcome::NonPositiveDelta(delta),
    }
}

/// Reject Direct routes when vault simulation shows less profit than calldata minProfit floor.
#[must_use]
pub fn batch_profit_covers_min(
    on_chain_profit: U256,
    modeled_gross: U256,
    amount_in: U256,
    slippage_bps: u64,
    hop_count: u32,
) -> bool {
    let Some(min_profit) = on_chain_min_profit_for_route(
        modeled_gross,
        amount_in,
        slippage_bps,
        hop_count,
        FlashLoanSource::Direct,
    ) else {
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

    #[test]
    fn exceeds_max_in_ratio_constant_matches_vault() {
        use crate::core::math::balancer::exceeds_balancer_max_in_ratio;
        let bal = U256::from(1_000_000u64);
        assert!(!exceeds_balancer_max_in_ratio(
            U256::from(300_000u64),
            bal
        ));
        assert!(exceeds_balancer_max_in_ratio(
            U256::from(300_001u64),
            bal
        ));
    }
}
