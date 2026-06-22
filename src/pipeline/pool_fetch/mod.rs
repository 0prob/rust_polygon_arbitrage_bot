mod decode;
mod plans;

use std::sync::Arc;

use alloy::network::Ethereum;
use alloy::primitives::U256;
use alloy::providers::Provider;
use tracing::debug;

use crate::abis::{IWoofiPool, IWooracle};
use crate::core::types::{PoolState, ProtocolType, WoofiBaseTokenState, WoofiPoolState};
use crate::pipeline::multicall::execute_multicall;
use crate::services::discovery::DiscoveredPool;
use crate::services::state_cache::StateCache;

pub use decode::{decode_v2_reserves, decode_v3_slot0};

use decode::decode_plan;
use plans::{PoolFetchPlan, build_plan};

async fn fetch_woofi_pool<P: Provider<Ethereum>>(
    provider: &P,
    pool: &DiscoveredPool,
) -> Option<PoolState> {
    let contract = IWoofiPool::new(pool.address, provider);
    let quote = contract.quoteToken().call().await.ok()?;
    let wooracle_addr = contract.wooracle().call().await.ok()?;
    if quote.is_zero() || wooracle_addr.is_zero() {
        return None;
    }
    let wooracle = IWooracle::new(wooracle_addr, provider);
    let mut base_states = Vec::new();
    let mut quote_reserve = U256::ZERO;
    for token in &pool.tokens {
        let info = contract.tokenInfos(*token).call().await.ok()?;
        if *token == quote {
            quote_reserve = U256::from(info.reserve);
            continue;
        }
        let oracle = wooracle.state(*token).call().await.ok()?;
        if !oracle.woFeasible {
            continue;
        }
        base_states.push(WoofiBaseTokenState {
            price: U256::from(oracle.price),
            spread: U256::from(oracle.spread),
            coeff: U256::from(oracle.coeff),
            reserve: U256::from(info.reserve),
            base_dec: U256::from(10u128).pow(U256::from(18)),
            quote_dec: U256::from(10u128).pow(U256::from(18)),
            price_dec: U256::from(10u128).pow(U256::from(18)),
            fee_rate: U256::from(info.feeRate),
            max_gamma: U256::from(info.maxGamma),
            max_notional_swap: U256::from(info.maxNotionalSwap),
        });
    }
    if quote_reserve.is_zero() || base_states.is_empty() {
        return None;
    }
    Some(PoolState::Woofi(WoofiPoolState {
        quote_reserve,
        base_states,
        fee: U256::ZERO,
    }))
}

pub async fn fetch_pools_batched<P: Provider<Ethereum> + Clone>(
    provider: P,
    cache: Arc<StateCache>,
    pools: &[&DiscoveredPool],
    max_multicall_calls: usize,
) -> usize {
    let max_calls = max_multicall_calls.max(1);
    let mut plans = Vec::new();
    let mut woofi_targets = Vec::new();
    for pool in pools {
        match pool.protocol {
            ProtocolType::Woofi => woofi_targets.push(*pool),
            _ => {
                if let Some(plan) = build_plan(pool) {
                    plans.push(plan);
                }
            }
        }
    }

    let mut updated = 0usize;
    let mut batches: Vec<Vec<PoolFetchPlan>> = Vec::new();
    let mut batch: Vec<PoolFetchPlan> = Vec::new();
    let mut batch_calls = 0usize;

    for plan in plans {
        let n = plan.calls.len();
        if batch_calls + n > max_calls && !batch.is_empty() {
            batches.push(std::mem::take(&mut batch));
            batch_calls = 0;
        }
        batch_calls += n;
        batch.push(plan);
    }
    if !batch.is_empty() {
        batches.push(batch);
    }

    if !batches.is_empty() {
        let mut iter = batches.into_iter();
        while let Some(first) = iter.next() {
            if let Some(second) = iter.next() {
                let (u0, u1) = tokio::join!(
                    execute_plan_batch(&provider, &first, cache.as_ref()),
                    execute_plan_batch(&provider, &second, cache.as_ref()),
                );
                updated += u0 + u1;
            } else {
                updated += execute_plan_batch(&provider, &first, cache.as_ref()).await;
            }
        }
    }

    let woofi_count = woofi_targets.len();
    for pool in woofi_targets {
        if let Some(state) = fetch_woofi_pool(&provider, pool).await {
            cache.insert(pool.address, state);
            updated += 1;
        } else {
            cache.insert(pool.address, PoolState::Invalid);
        }
    }

    debug!(updated, woofi = woofi_count, "multicall pool fetch");
    updated
}

async fn execute_plan_batch<P: Provider<Ethereum>>(
    provider: &P,
    plans: &[PoolFetchPlan],
    cache: &StateCache,
) -> usize {
    let mut items = Vec::new();
    let mut spans: Vec<(&PoolFetchPlan, usize, usize)> = Vec::new();
    for plan in plans {
        let start = items.len();
        items.extend_from_slice(&plan.calls);
        spans.push((plan, start, items.len()));
    }

    let Ok(results) = execute_multicall(provider, &items).await else {
        return 0;
    };

    let mut updated = 0usize;
    for (plan, start, end) in spans {
        let slice = &results[start..end];
        if let Some(state) = decode_plan(plan, slice) {
            cache.insert(plan.pool.address, state);
            updated += 1;
        } else {
            cache.insert(plan.pool.address, PoolState::Invalid);
        }
    }
    updated
}
