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
///
/// `executor` is always the vault `sender` (tokens are approved from the
/// ArbExecutor). `recipient` may be the next V2 pair for direct-out funding —
/// do **not** set sender=recipient when direct-out (vault would pull from the
/// pair, which has no balance/approval).
pub fn encode_balancer_hop(
    hop: &CalldataHop,
    recipient: Address,
    executor: Address,
    arena: &StateArena,
    slippage_bps: u64,
    deadline: U256,
) -> anyhow::Result<Vec<ExecutorCall>> {
    let pool_id = resolve_balancer_pool_id(hop.pool_address, hop.pool_id)?;
    // Weighted multi-token single swaps overquote vs vault by >50 bps on some
    // pools (BAL#507 = SWAP_LIMIT). Shared floor with assess / V2 / Curve; take
    // the tighter of re-quote vs conservative_execution_hops amount_out.
    let bps = slippage_bps.max(crate::core::constants::EXECUTION_MIN_SLIPPAGE_BPS);
    let limit = {
        let quoted = super::shared::compute_min_out(arena, hop, bps, "balancer")?;
        if hop.amount_out.is_zero() {
            quoted
        } else {
            hop.amount_out.min(quoted)
        }
    };

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
            sender: executor,
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

/// Limits for Vault `batchSwap` flash routes. Vault checks `delta <= limit` per asset;
/// non-input assets must use `0` (not `I256::MIN`) so zero-net intermediates pass.
fn build_flash_swap_limits(assets: &[Address], hops: &[CalldataHop]) -> Vec<I256> {
    let mut limits = vec![I256::ZERO; assets.len()];
    if let Some(first) = hops.first()
        && let Some(idx) = assets.iter().position(|a| *a == first.token_in)
    {
        limits[idx] = I256::MAX;
    }
    limits
}

/// Encode an all-Balancer route as one vault `batchSwap` flash-swap call for `executeArbDirect`.
///
/// `recipient` is the ArbExecutor (flash credit + final settlement). Batch funds use the same
/// address as sender and recipient — there is no mid-route direct-out to a V2 pair.
pub fn encode_balancer_batch_route(
    hops: &[CalldataHop],
    recipient: Address,
    deadline: U256,
) -> anyhow::Result<Vec<ExecutorCall>> {
    let req = build_balancer_batch_swap_request(hops, recipient)?;
    let limits = build_flash_swap_limits(&req.assets, hops);
    let batch = IBalancerVault::batchSwapCall {
        kind: BALANCER_GIVEN_IN,
        swaps: req.swaps,
        assets: req.assets,
        funds: req.funds,
        limits,
        deadline,
    };

    Ok(vec![ExecutorCall {
        target: BALANCER_VAULT,
        value: U256::ZERO,
        data: batch.abi_encode().into(),
    }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{Edge, PoolIndex, ProtocolType, TokenIndex};
    use alloy::primitives::address;

    fn hop(token_in: Address, token_out: Address, amount_in: u128) -> CalldataHop {
        CalldataHop {
            edge: Edge {
                pool_index: PoolIndex(0),
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                fee_bps: 30,
                zero_for_one: true,
                protocol: ProtocolType::BalancerV2,
            },
            pool_address: address!("0x1111111111111111111111111111111111111111"),
            token_in,
            token_out,
            amount_in: U256::from(amount_in),
            amount_out: U256::from(1u64),
            pool_id: None,
            protocol_label: None,
            pool_type: None,
            router: None,
            hooks: None,
        }
    }

    #[test]
    fn flash_swap_limits_cap_input_and_open_outputs() {
        let wmatic = address!("0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270");
        let usdc = address!("0x2791bca1f2de4661ed88a30c99a7a9489c09eb3f");
        let hops = [hop(wmatic, usdc, 1_000), hop(usdc, wmatic, 1_000)];
        let assets = vec![wmatic, usdc];
        let limits = build_flash_swap_limits(&assets, &hops);
        assert_eq!(limits[0], I256::MAX);
        assert_eq!(limits[1], I256::ZERO);
    }

    #[test]
    fn single_swap_sender_is_executor_when_direct_out_to_next_pool() {
        use crate::core::types::{BalancerPoolKind, BalancerPoolState, PoolState};
        use alloy::primitives::FixedBytes;
        use std::sync::Arc;

        let executor = address!("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        let next_v2 = address!("0x2222222222222222222222222222222222222222");
        let t_in = address!("0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270");
        let t_out = address!("0x2791bca1f2de4661ed88a30c99a7a9489c09eb3f");
        let pool = address!("0x1111111111111111111111111111111111111111");
        let pool_id = FixedBytes::from([0xab; 32]);

        let mut arena = StateArena::default();
        let i0 = arena.register_token(t_in);
        let i1 = arena.register_token(t_out);
        let one = U256::from(10u64).pow(U256::from(18u64));
        let bal = U256::from(1_000_000u64) * one;
        let pool_index = arena.register_pool(
            pool,
            Arc::new(PoolState::Balancer(BalancerPoolState {
                pool_id: Some(pool_id),
                tokens: vec![t_in, t_out],
                balances: vec![bal, bal + U256::from(1u64)],
                weights: vec![one / U256::from(2u64), one / U256::from(2u64)],
                scaling_factors: vec![one, one],
                amp: U256::ZERO,
                amp_precision: U256::from(1_000u64),
                fee: U256::from(1_000_000_000_000_000u64),
                pool_type: BalancerPoolKind::Weighted,
                linear: None,
                bpt_index: None,
                is_updating: false,
                last_change_block: 0,
            })),
        );
        let hop = CalldataHop {
            edge: Edge {
                pool_index,
                token_in: i0,
                token_out: i1,
                token_in_idx: 0,
                token_out_idx: 1,
                fee_bps: 30,
                zero_for_one: true,
                protocol: ProtocolType::BalancerV2,
            },
            pool_address: pool,
            token_in: t_in,
            token_out: t_out,
            amount_in: U256::from(1_000u64),
            amount_out: U256::from(900u64),
            pool_id: Some(pool_id),
            protocol_label: None,
            pool_type: None,
            router: None,
            hooks: None,
        };
        let calls =
            encode_balancer_hop(&hop, next_v2, executor, &arena, 50, U256::from(1u64)).expect("enc");
        // vault.swap calldata: after selector, singleSwap then funds.sender (address word)
        // Funds starts after SingleSwap: poolId(32)+kind(32)+assetIn(32)+assetOut(32)+amount(32)+userData offset/len
        // Easier: search for executor and next_v2 in the payload.
        let data = calls[1].data.as_ref();
        assert_eq!(calls[1].target, BALANCER_VAULT);
        let has_exec = data.windows(20).any(|w| w == executor.as_slice());
        let has_next = data.windows(20).any(|w| w == next_v2.as_slice());
        assert!(has_exec, "sender must encode executor");
        assert!(has_next, "recipient may be next V2 for direct-out");
        // sender word must not equal recipient-only: both present when direct-out.
        assert_ne!(executor, next_v2);
    }
}
