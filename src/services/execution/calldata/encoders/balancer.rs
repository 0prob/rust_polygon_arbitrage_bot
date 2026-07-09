use alloy::primitives::{Address, Bytes, I256, U256};
use alloy::sol_types::SolCall;
use rustc_hash::FxHashMap;

use crate::abis::{
    BalancerBatchSwapStep, BalancerFundManagement, BalancerSingleSwap, ExecutorCall, IBalancerVault,
};
use crate::core::constants::BALANCER_VAULT;
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::CalldataHop;
use crate::services::execution::calldata::approvals::encode_approve_if_needed;

use super::shared::resolve_balancer_pool_id;

const BALANCER_GIVEN_IN: u8 = 0;

/// Encode a Balancer Vault hop into executor calls
///
/// Returns a vector containing:
/// 1. An approval call if needed to the Balancer Vault
/// 2. A swap call to the vault's single swap function
pub fn encode_balancer_hop(
    hop: &CalldataHop,
    recipient: Address,
    arena: &StateArena,
    slippage_bps: u64,
    deadline: U256,
) -> anyhow::Result<Vec<ExecutorCall>> {
    let pool_id = resolve_balancer_pool_id(hop.pool_address, hop.pool_id)?;
    let limit = super::shared::compute_min_out(arena, hop, slippage_bps, "balancer")?;

    let swap = IBalancerVault::swapCall {
        singleSwap: BalancerSingleSwap {
            poolId: pool_id,
            kind: BALANCER_GIVEN_IN,
            assetIn: hop.token_in,
            assetOut: hop.token_out,
            amount: hop.amount_in,
            userData: Bytes::default(),
        },
        funds: BalancerFundManagement {
            sender: recipient,
            fromInternalBalance: false,
            recipient,
            toInternalBalance: false,
        },
        limit,
        deadline,
    };

    Ok(vec![
        encode_approve_if_needed(hop.token_in, BALANCER_VAULT, hop.amount_in),
        ExecutorCall {
            target: BALANCER_VAULT,
            value: U256::ZERO,
            data: swap.abi_encode().into(),
        },
    ])
}

pub struct BalancerBatchSwapRequest {
    pub swaps: Vec<BalancerBatchSwapStep>,
    pub assets: Vec<Address>,
    pub funds: BalancerFundManagement,
}

/// Build vault `batchSwap` / `queryBatchSwap` parameters for an all-Balancer route.
pub fn build_balancer_batch_swap_request(
    hops: &[CalldataHop],
    recipient: Address,
) -> anyhow::Result<BalancerBatchSwapRequest> {
    if hops.is_empty() {
        anyhow::bail!("balancer batch route requires at least one hop");
    }

    let mut assets: Vec<Address> = Vec::new();
    let mut asset_index = FxHashMap::default();
    for hop in hops {
        for token in [hop.token_in, hop.token_out] {
            if let std::collections::hash_map::Entry::Vacant(e) = asset_index.entry(token) {
                e.insert(assets.len());
                assets.push(token);
            }
        }
    }

    let mut swaps = Vec::with_capacity(hops.len());
    for (i, hop) in hops.iter().enumerate() {
        let pool_id = resolve_balancer_pool_id(hop.pool_address, hop.pool_id)?;
        let asset_in_index = *asset_index
            .get(&hop.token_in)
            .ok_or_else(|| anyhow::anyhow!("missing asset index for token_in"))?;
        let asset_out_index = *asset_index
            .get(&hop.token_out)
            .ok_or_else(|| anyhow::anyhow!("missing asset index for token_out"))?;
        swaps.push(BalancerBatchSwapStep {
            poolId: pool_id,
            assetInIndex: U256::from(asset_in_index),
            assetOutIndex: U256::from(asset_out_index),
            amount: if i == 0 { hop.amount_in } else { U256::ZERO },
            userData: Bytes::default(),
        });
    }

    Ok(BalancerBatchSwapRequest {
        swaps,
        assets,
        funds: BalancerFundManagement {
            sender: recipient,
            fromInternalBalance: false,
            recipient,
            toInternalBalance: false,
        },
    })
}

/// Encode an all-Balancer route as one vault `batchSwap` flash-swap call for `executeArbDirect`.
pub fn encode_balancer_batch_route(
    hops: &[CalldataHop],
    recipient: Address,
    deadline: U256,
) -> anyhow::Result<Vec<ExecutorCall>> {
    let req = build_balancer_batch_swap_request(hops, recipient)?;
    // Flash-swap limits: zero means the vault may pull owed tokens at settlement.
    let limits = vec![I256::ZERO; req.assets.len()];
    let batch = IBalancerVault::batchSwapCall {
        kind: BALANCER_GIVEN_IN,
        swaps: req.swaps,
        assets: req.assets,
        funds: req.funds,
        limits,
        deadline,
    };

    Ok(vec![
        ExecutorCall {
            target: BALANCER_VAULT,
            value: U256::ZERO,
            data: batch.abi_encode().into(),
        },
    ])
}
