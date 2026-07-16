mod decode;
mod plans;

use std::sync::Arc;

use alloy::network::Ethereum;
use alloy::primitives::{Address, FixedBytes, U256};
use alloy::providers::Provider;
use alloy::sol_types::SolCall;

use crate::abis::{IBalancerPool, IERC20Metadata, IWoofiPool, IWooracle};
use crate::core::types::{PoolState, ProtocolType, WoofiBaseTokenState, WoofiPoolState};
use crate::infra::pool_meta_cache::PoolMetaCache;
use crate::pipeline::abi_cache::{
    BALANCER_POOL_ID, ERC20_DECIMALS, WOOFI_QUOTE_TOKEN, WOOFI_WOORACLE,
};
use crate::pipeline::multicall::{
    MulticallItem, encode_call, execute_multicall_at_chunked, plan_batch_call_budget,
};
use crate::services::discovery::DiscoveredPool;
use crate::services::execution::rpc_errors::is_rpc_rate_limited;
use crate::services::state_cache::StateCache;

use decode::decode_plan;
use plans::{PoolFetchPlan, build_plan_with_pool_id};

#[derive(Clone)]
struct WoofiMeta {
    address: Address,
    tokens: Vec<Address>,
    quote: Address,
    wooracle: Address,
}

async fn fetch_woofi_pools_batched<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: &P,
    pools: &[&DiscoveredPool],
    block_number: Option<u64>,
    max_chunk: usize,
    meta_cache: &PoolMetaCache,
) -> Vec<(Address, Option<PoolState>)> {
    if pools.is_empty() {
        return Vec::new();
    }

    let mut phase1 = Vec::with_capacity(pools.len() * 2);
    let mut owners: Vec<Address> = Vec::with_capacity(pools.len());
    let mut cached_metas: Vec<Option<WoofiMeta>> = Vec::with_capacity(pools.len());
    let mut woofi_pools: Vec<&DiscoveredPool> = Vec::with_capacity(pools.len());
    for pool in pools {
        owners.push(pool.address);
        if pool.protocol == ProtocolType::Woofi {
            woofi_pools.push(*pool);
        }
        if let Some((quote, wooracle)) = meta_cache.woofi_meta(&pool.address) {
            cached_metas.push(Some(WoofiMeta {
                address: pool.address,
                tokens: pool.tokens.clone(),
                quote,
                wooracle,
            }));
            continue;
        }
        cached_metas.push(None);
        phase1.push(MulticallItem {
            target: pool.address,
            data: WOOFI_QUOTE_TOKEN.clone(),
        });
        phase1.push(MulticallItem {
            target: pool.address,
            data: WOOFI_WOORACLE.clone(),
        });
    }

    let phase1_results = if phase1.is_empty() {
        Vec::new()
    } else {
        match execute_multicall_at_chunked(
            provider.clone(),
            Arc::from(phase1),
            block_number,
            max_chunk,
        )
        .await
        {
            Ok(r) => r,
            Err(error) => {
                crate::debug!("woofi state phase1 failed: {error:#}");
                return owners.into_iter().map(|addr| (addr, None)).collect();
            }
        }
    };

    let mut metas = Vec::with_capacity(pools.len());
    let mut phase1_idx = 0usize;
    for (i, pool) in pools.iter().enumerate() {
        if let Some(cached) = &cached_metas[i] {
            metas.push(cached.clone());
            continue;
        }
        let quote = phase1_results
            .get(phase1_idx * 2)
            .and_then(|r| r.as_ref())
            .and_then(|b| IWoofiPool::quoteTokenCall::abi_decode_returns(b).ok())
            .unwrap_or(Address::ZERO);
        let wooracle = phase1_results
            .get(phase1_idx * 2 + 1)
            .and_then(|r| r.as_ref())
            .and_then(|b| IWoofiPool::wooracleCall::abi_decode_returns(b).ok())
            .unwrap_or(Address::ZERO);
        phase1_idx += 1;
        if quote.is_zero() || wooracle.is_zero() {
            crate::debug!(
                "woofi state metadata invalid: pool={} quote={} oracle={}",
                pool.address,
                quote,
                wooracle
            );
            continue;
        }
        meta_cache.set_woofi_meta(&pool.address, quote, wooracle);
        metas.push(WoofiMeta {
            address: pool.address,
            tokens: pool.tokens.clone(),
            quote,
            wooracle,
        });
    }

    if metas.is_empty() {
        return owners.into_iter().map(|addr| (addr, None)).collect();
    }

    let mut phase2 = Vec::new();
    let mut spans: Vec<(usize, usize, usize)> = Vec::with_capacity(metas.len());
    for (meta_idx, meta) in metas.iter().enumerate() {
        let start = phase2.len();
        // Quote reserve: indexer rows often omit the quote token from `tokens`.
        phase2.push(MulticallItem {
            target: meta.address,
            data: encode_call(&IWoofiPool::tokenInfosCall { base: meta.quote }),
        });
        phase2.push(MulticallItem {
            target: meta.quote,
            data: ERC20_DECIMALS.clone(),
        });
        for token in &meta.tokens {
            if *token == meta.quote {
                continue;
            }
            phase2.push(MulticallItem {
                target: meta.address,
                data: encode_call(&IWoofiPool::tokenInfosCall { base: *token }),
            });
            phase2.push(MulticallItem {
                target: meta.wooracle,
                data: encode_call(&IWooracle::stateCall { base: *token }),
            });
            phase2.push(MulticallItem {
                target: *token,
                data: ERC20_DECIMALS.clone(),
            });
            phase2.push(MulticallItem {
                target: meta.wooracle,
                data: encode_call(&IWooracle::decimalsCall { base: *token }),
            });
        }
        spans.push((meta_idx, start, phase2.len()));
    }

    let phase2_results = match execute_multicall_at_chunked(
        provider.clone(),
        Arc::from(phase2),
        block_number,
        max_chunk,
    )
    .await
    {
        Ok(results) => results,
        Err(error) => {
            crate::debug!("woofi state phase2 failed: {error:#}");
            return owners.into_iter().map(|addr| (addr, None)).collect();
        }
    };

    let mut out: Vec<(Address, Option<PoolState>)> =
        owners.into_iter().map(|addr| (addr, None)).collect();
    let mut resolved = rustc_hash::FxHashMap::default();

    for (meta_idx, start, _end) in spans {
        let meta = &metas[meta_idx];
        let mut base_states = Vec::new();
        let mut state_tokens = Vec::new();
        let mut quote_reserve = U256::ZERO;
        let mut cursor = start;
        if let Some(quote_bytes) = phase2_results.get(cursor).and_then(|r| r.as_ref())
            && let Ok(info) = IWoofiPool::tokenInfosCall::abi_decode_returns(quote_bytes)
        {
            quote_reserve = U256::from(info.reserve);
        }
        cursor += 1;
        let quote_dec = phase2_results
            .get(cursor)
            .and_then(|r| r.as_ref())
            .and_then(|bytes| IERC20Metadata::decimalsCall::abi_decode_returns(bytes).ok());
        cursor += 1;
        for token_addr in meta.tokens.iter().filter(|t| **t != meta.quote) {
            let base = phase2_results.get(cursor).and_then(|r| r.as_ref());
            let oracle = phase2_results.get(cursor + 1).and_then(|r| r.as_ref());
            let base_dec = phase2_results
                .get(cursor + 2)
                .and_then(|r| r.as_ref())
                .and_then(|bytes| IERC20Metadata::decimalsCall::abi_decode_returns(bytes).ok());
            let price_dec = phase2_results
                .get(cursor + 3)
                .and_then(|r| r.as_ref())
                .and_then(|bytes| IWooracle::decimalsCall::abi_decode_returns(bytes).ok());
            cursor += 4;
            let Some(info_bytes) = base else { continue };
            let Ok(info) = IWoofiPool::tokenInfosCall::abi_decode_returns(info_bytes) else {
                continue;
            };
            if !info.enabled {
                continue;
            }
            let Some(oracle_bytes) = oracle else { continue };
            let Ok(oracle_state) = IWooracle::stateCall::abi_decode_returns(oracle_bytes) else {
                continue;
            };
            if !oracle_state.woFeasible {
                continue;
            }
            let (Some(base_dec), Some(quote_dec), Some(price_dec)) =
                (base_dec, quote_dec, price_dec)
            else {
                continue;
            };
            base_states.push(WoofiBaseTokenState {
                price: U256::from(oracle_state.price),
                spread: U256::from(oracle_state.spread),
                coeff: U256::from(oracle_state.coeff),
                reserve: U256::from(info.reserve),
                base_dec: crate::util::ten_pow_u256_cached(base_dec),
                quote_dec: crate::util::ten_pow_u256_cached(quote_dec),
                price_dec: crate::util::ten_pow_u256_cached(price_dec),
                fee_rate: U256::from(info.feeRate),
                max_gamma: U256::from(info.maxGamma),
                max_notional_swap: U256::from(info.maxNotionalSwap),
            });
            state_tokens.push(*token_addr);
        }
        let state = if quote_reserve.is_zero() || base_states.is_empty() {
            None
        } else {
            state_tokens.push(meta.quote);
            Some(PoolState::Woofi(WoofiPoolState {
                tokens: state_tokens,
                quote_reserve,
                base_states,
                fee: U256::ZERO,
            }))
        };
        resolved.insert(meta.address, state);
    }

    for entry in &mut out {
        if let Some(state) = resolved.remove(&entry.0) {
            entry.1 = state;
        }
    }
    out
}

async fn hydrate_balancer_pool_ids<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: &P,
    pools: &[&DiscoveredPool],
    block_number: Option<u64>,
    meta_cache: &PoolMetaCache,
) -> rustc_hash::FxHashMap<Address, FixedBytes<32>> {
    let unverified: Vec<&DiscoveredPool> = pools
        .iter()
        .copied()
        .filter(|p| p.protocol == ProtocolType::BalancerV2 && !p.pool_id_verified)
        .collect();

    let mut out = rustc_hash::FxHashMap::with_capacity_and_hasher(
        unverified.len(),
        rustc_hash::FxBuildHasher,
    );
    let mut needs_rpc: Vec<&DiscoveredPool> = Vec::new();

    for pool in &unverified {
        if let Some(id) = meta_cache.balancer_pool_id(&pool.address) {
            out.insert(pool.address, id);
        } else {
            needs_rpc.push(pool);
        }
    }

    if needs_rpc.is_empty() {
        return out;
    }

    let items: Vec<MulticallItem> = needs_rpc
        .iter()
        .map(|pool| MulticallItem {
            target: pool.address,
            data: BALANCER_POOL_ID.clone(),
        })
        .collect();

    let Ok(results) = execute_multicall_at_chunked(
        provider.clone(),
        Arc::from(items),
        block_number,
        crate::pipeline::multicall::MULTICALL_CHUNK,
    )
    .await
    else {
        return out;
    };

    for (pool, bytes) in needs_rpc.iter().zip(results.iter()) {
        let Some(bytes) = bytes.as_ref() else {
            continue;
        };
        if let Ok(id) = IBalancerPool::getPoolIdCall::abi_decode_returns(bytes) {
            meta_cache.set_balancer_pool_id(&pool.address, id);
            out.insert(pool.address, id);
        }
    }
    out
}

fn balancer_pool_id<'a>(
    pool: &'a DiscoveredPool,
    hydrated: &'a rustc_hash::FxHashMap<Address, FixedBytes<32>>,
) -> Option<FixedBytes<32>> {
    hydrated.get(&pool.address).copied().or(pool.pool_id)
}

async fn apply_woofi_results<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: &P,
    woofi_pools: &[&DiscoveredPool],
    block_number: Option<u64>,
    chunk_size: usize,
    cache: &StateCache,
    meta_cache: &PoolMetaCache,
) -> usize {
    if woofi_pools.is_empty() {
        return 0;
    }
    let mut updated = 0usize;
    for (addr, state) in
        fetch_woofi_pools_batched(provider, woofi_pools, block_number, chunk_size, meta_cache).await
    {
        match state {
            Some(s) => {
                cache.insert(addr, s);
                updated += 1;
            }
            None => {
                cache.insert(addr, PoolState::Invalid);
            }
        }
    }
    updated
}

/// Concurrent plan-batch RPCs (bounded) — complements per-chunk multicall parallelism.
const MAX_PARALLEL_PLAN_BATCHES: usize = 2;

async fn run_plan_batches<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: P,
    batches: Vec<Vec<PoolFetchPlan>>,
    cache: Arc<StateCache>,
    block_number: Option<u64>,
    chunk_size: usize,
    batch_pace_ms: u64,
) -> usize {
    if batches.is_empty() {
        return 0;
    }
    if batches.len() == 1 {
        return execute_plan_batch(
            &provider,
            &batches[0],
            cache.as_ref(),
            block_number,
            chunk_size,
        )
        .await;
    }

    let pacing = tokio::time::Duration::from_millis(batch_pace_ms);
    // Stagger spawns do not pace in-flight RPC when batches run in parallel — serialize when pacing is on.
    if batch_pace_ms > 0 {
        let mut updated = 0usize;
        for (i, batch) in batches.into_iter().enumerate() {
            if i > 0 {
                tokio::time::sleep(pacing).await;
            }
            updated +=
                execute_plan_batch(&provider, &batch, cache.as_ref(), block_number, chunk_size)
                    .await;
        }
        return updated;
    }

    let sem = Arc::new(tokio::sync::Semaphore::new(
        MAX_PARALLEL_PLAN_BATCHES.min(batches.len()),
    ));
    let mut tasks = tokio::task::JoinSet::new();
    for batch in batches {
        let provider = provider.clone();
        let cache = Arc::clone(&cache);
        let sem = Arc::clone(&sem);
        tasks.spawn(async move {
            let Ok(_permit) = sem.acquire().await else {
                crate::warn!("plan batch skipped: fetch semaphore closed");
                return 0usize;
            };
            execute_plan_batch(&provider, &batch, cache.as_ref(), block_number, chunk_size).await
        });
    }
    let mut updated = 0usize;
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(n) => updated += n,
            Err(e) => crate::warn!("plan batch task failed: {e:#}"),
        }
    }
    updated
}

pub async fn fetch_pools_batched<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: P,
    cache: Arc<StateCache>,
    pools: &[&DiscoveredPool],
    max_multicall_calls: usize,
    batch_pace_ms: u64,
    block_number: Option<u64>,
    meta_cache: &PoolMetaCache,
) -> usize {
    let chunk_size = max_multicall_calls.max(1);
    let plan_batch_calls = plan_batch_call_budget(chunk_size);
    let needs_balancer_hydrate = pools
        .iter()
        .any(|p| p.protocol == ProtocolType::BalancerV2 && !p.pool_id_verified);
    let hydrate_started = crate::util::now_ms();
    let balancer_ids = if needs_balancer_hydrate {
        hydrate_balancer_pool_ids(&provider, pools, block_number, meta_cache).await
    } else {
        rustc_hash::FxHashMap::default()
    };
    let balancer_hydrate_ms = crate::util::now_ms().saturating_sub(hydrate_started);

    let mut woofi_pools: Vec<&DiscoveredPool> = Vec::with_capacity(pools.len());
    for pool in pools {
        if pool.protocol == ProtocolType::Woofi {
            woofi_pools.push(*pool);
        }
    }

    let mut protocol_work = rustc_hash::FxHashMap::default();
    let mut plans = Vec::with_capacity(pools.len().saturating_sub(woofi_pools.len()));
    for pool in pools {
        if pool.protocol == ProtocolType::Woofi {
            continue;
        }
        let pool_id = balancer_pool_id(pool, &balancer_ids);
        if let Some(plan) = build_plan_with_pool_id(pool, pool_id) {
            let entry = protocol_work
                .entry(pool.protocol)
                .or_insert((0usize, 0usize));
            entry.0 += 1;
            entry.1 += plan.calls.len();
            plans.push(plan);
        } else {
            cache.insert(pool.address, PoolState::Invalid);
        }
    }

    let plan_count = plans.len();
    let plan_calls = plans.iter().map(|plan| plan.calls.len()).sum::<usize>();
    let expected_chunks = plans
        .iter()
        .map(|plan| plan.calls.len().div_ceil(chunk_size))
        .sum::<usize>();
    let mut batches: Vec<Vec<PoolFetchPlan>> = Vec::new();
    if !plans.is_empty() {
        let mut batch: Vec<PoolFetchPlan> = Vec::with_capacity(plans.len());
        let mut batch_calls = 0usize;

        for plan in plans {
            let n = plan.calls.len();
            if batch_calls + n > plan_batch_calls && !batch.is_empty() {
                batches.push(std::mem::take(&mut batch));
                batch_calls = 0;
            }
            batch_calls += n;
            batch.push(plan);
        }
        if !batch.is_empty() {
            batches.push(batch);
        }
    }

    let plan_batches = batches.len();
    let provider_woofi = provider.clone();
    let (woofi_result, plan_result) = tokio::join!(
        async {
            let started = crate::util::now_ms();
            let updated = apply_woofi_results(
                &provider_woofi,
                &woofi_pools,
                block_number,
                chunk_size,
                cache.as_ref(),
                meta_cache,
            )
            .await;
            (updated, crate::util::now_ms().saturating_sub(started))
        },
        async {
            let started = crate::util::now_ms();
            let updated = run_plan_batches(
                provider,
                batches,
                Arc::clone(&cache),
                block_number,
                chunk_size,
                batch_pace_ms,
            )
            .await;
            (updated, crate::util::now_ms().saturating_sub(started))
        },
    );

    let (woofi_updated, woofi_ms) = woofi_result;
    let (plan_updated, plan_ms) = plan_result;
    crate::debug!(
        "pool fetch work: pools={} protocol_work={protocol_work:?} woofi_pools={} woofi_updated={woofi_updated} woofi_ms={woofi_ms} balancer_hydrate={} balancer_hydrate_ms={balancer_hydrate_ms} plans={plan_count} plan_calls={plan_calls} plan_batches={plan_batches} expected_chunks={expected_chunks} plan_updated={plan_updated} plan_ms={plan_ms}",
        pools.len(),
        woofi_pools.len(),
        needs_balancer_hydrate,
    );

    woofi_updated + plan_updated
}

async fn execute_plan_batch<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: &P,
    plans: &[PoolFetchPlan],
    cache: &StateCache,
    block_number: Option<u64>,
    chunk_size: usize,
) -> usize {
    if plans.is_empty() {
        return 0;
    }
    let mut work: Vec<(usize, usize)> = vec![(0, plans.len())];
    let mut updated = 0usize;
    while let Some((start, end)) = work.pop() {
        if start >= end {
            continue;
        }
        let slice = &plans[start..end];
        match execute_plan_batch_inner(provider, slice, cache, block_number, chunk_size).await {
            Ok(n) => updated += n,
            Err(e) if is_rpc_rate_limited(&e) => {
                crate::warn!(
                    "plan batch abort: rate limited (updated={updated}, remaining_pools={})",
                    end.saturating_sub(start)
                );
                break;
            }
            Err(_e) if slice.len() > 1 => {
                let mid = start + slice.len() / 2;
                work.push((mid, end));
                work.push((start, mid));
            }
            Err(_e) => {
                crate::warn!(
                    "multicall batch failed for pool {} ({} calls)",
                    slice[0].pool.address,
                    slice[0].calls.len()
                );
            }
        }
    }
    updated
}

async fn execute_plan_batch_inner<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: &P,
    plans: &[PoolFetchPlan],
    cache: &StateCache,
    block_number: Option<u64>,
    chunk_size: usize,
) -> anyhow::Result<usize> {
    let total_calls: usize = plans.iter().map(|p| p.calls.len()).sum();
    let mut items = Vec::with_capacity(total_calls);
    let mut spans: Vec<(&PoolFetchPlan, usize, usize)> = Vec::with_capacity(plans.len());
    for plan in plans {
        let start = items.len();
        items.extend_from_slice(&plan.calls);
        spans.push((plan, start, items.len()));
    }

    let item_count = items.len();
    let results =
        execute_multicall_at_chunked(provider.clone(), Arc::from(items), block_number, chunk_size)
            .await
            .inspect_err(|e| {
                crate::warn!(
                    "multicall batch failed ({item_count} items, {} pools): {e:#}",
                    plans.len()
                );
            })?;

    let mut updated = 0usize;
    for (plan, start, end) in spans {
        let Some(slice) = results.get(start..end) else {
            crate::warn!(
                "multicall batch returned {} results for {} requested items",
                results.len(),
                item_count
            );
            return Ok(updated);
        };
        if let Some(state) = decode_plan(plan, slice) {
            cache.insert(plan.pool.address, state);
            updated += 1;
        } else {
            cache.insert(plan.pool.address, PoolState::Invalid);
        }
    }
    Ok(updated)
}
