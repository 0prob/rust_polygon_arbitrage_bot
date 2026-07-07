use alloy::primitives::{Address, FixedBytes};
use rustc_hash::FxHashSet;
use std::sync::Arc;

use crate::core::constants::UNISWAP_V4_POOL_MANAGER;
use crate::core::types::{FoundCycle, PoolIndex, ProtocolType, V3Tick};
use crate::core::v4_storage::{
    compute_v4_tick_bitmap_slot, compute_v4_tick_info_slot, decode_v4_tick_liquidity,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::PoolMeta;

const MAX_TICK_POOLS: usize = 200;

/// Collect (algebra, algebra_integral) pool address sets from metas for tick enrichment.
/// Integral pools are also algebra (use special tick path) but require different decode ABI.
#[must_use]
pub fn collect_algebra_pools(
    arena: &StateArena,
    pool_metas: &[PoolMeta],
) -> (FxHashSet<Address>, FxHashSet<Address>) {
    let mut algebra_pools = FxHashSet::default();
    let mut algebra_integral_pools = FxHashSet::default();
    // ponytail: pre-size for common case - most pools aren't algebra
    algebra_pools.reserve(32);
    algebra_integral_pools.reserve(16);
    for meta in pool_metas {
        let Some(label) = meta.protocol_label.as_deref() else {
            continue;
        };
        let Some(addr) = arena.pool_address(meta.pool_index) else {
            continue;
        };
        if crate::core::protocol::is_algebra_integral_protocol_label(label) {
            algebra_integral_pools.insert(addr);
        }
        if crate::core::protocol::is_algebra_protocol_label(label) {
            algebra_pools.insert(addr);
        }
    }
    (algebra_pools, algebra_integral_pools)
}

#[must_use]
pub fn collect_v3_pool_addresses(arena: &StateArena, cycles: &[FoundCycle]) -> Vec<Address> {
    let mut out = Vec::with_capacity(cycles.len().min(MAX_TICK_POOLS));
    let mut seen: FxHashSet<Address> = FxHashSet::default();
    'cycles: for cycle in cycles {
        for edge in &cycle.edges {
            if edge.protocol != ProtocolType::UniswapV3 {
                continue;
            }
            let Some(addr) = arena.pool_address(edge.pool_index) else {
                continue;
            };
            if seen.insert(addr) {
                out.push(addr);
                if out.len() >= MAX_TICK_POOLS {
                    break 'cycles;
                }
            }
        }
    }
    out
}

#[must_use]
pub fn collect_v4_tick_targets(
    cycles: &[FoundCycle],
    pool_metas: &[PoolMeta],
) -> Vec<(PoolIndex, FixedBytes<32>)> {
    let meta_by_pool: rustc_hash::FxHashMap<PoolIndex, &PoolMeta> =
        pool_metas.iter().map(|m| (m.pool_index, m)).collect();
    let mut out = Vec::new();
    let mut seen: FxHashSet<PoolIndex> = FxHashSet::default();
    'cycles: for cycle in cycles {
        for edge in &cycle.edges {
            if edge.protocol != ProtocolType::UniswapV4 {
                continue;
            }
            if !seen.insert(edge.pool_index) {
                continue;
            }
            let Some(meta) = meta_by_pool.get(&edge.pool_index) else {
                continue;
            };
            let Some(pool_id) = meta.pool_id else {
                continue;
            };
            out.push((edge.pool_index, pool_id));
            if out.len() >= MAX_TICK_POOLS {
                break 'cycles;
            }
        }
    }
    out
}

pub async fn enrich_v3_ticks<
    P: alloy::providers::Provider<alloy::network::Ethereum> + Clone + Send + 'static,
>(
    provider: &P,
    arena: &mut StateArena,
    pool_addresses: &[Address],
    word_range: i16,
    algebra_pools: &FxHashSet<Address>,
    algebra_integral_pools: &FxHashSet<Address>,
    block_number: Option<u64>,
) -> usize {
    use alloy::sol_types::SolCall;

    use crate::abis::ITickLens;
    use crate::core::constants::TICK_LENS_POLYGON;
    use crate::pipeline::multicall::{MulticallItem, encode_call};

    if pool_addresses.is_empty() {
        return 0;
    }
    let tick_lens = TICK_LENS_POLYGON;
    let word_count = word_range.saturating_mul(2).saturating_add(1) as usize;
    let mut items = Vec::with_capacity(pool_addresses.len().saturating_mul(word_count));
    let mut spans: Vec<(usize, usize, PoolIndex)> = Vec::with_capacity(pool_addresses.len());

    let mut algebra_targets = Vec::new();
    for &pool in pool_addresses {
        let Some(idx) = arena.address_to_pool().get(&pool).copied() else {
            continue;
        };
        let (tick, spacing) = match arena.pool_state(idx) {
            Some(crate::core::types::PoolState::V3(s)) => (s.tick, s.tick_spacing),
            _ => continue,
        };
        let center_word = (tick / spacing.max(1)) >> 8;
        let word_min = center_word - word_range as i32;
        let word_max = center_word + word_range as i32;
        let start = items.len();
        if algebra_pools.contains(&pool) {
            algebra_targets.push((pool, idx, spacing, word_min, word_max));
            continue;
        }
        for word in word_min..=word_max {
            items.push(MulticallItem {
                target: tick_lens,
                data: encode_call(&ITickLens::getPopulatedTicksInWordCall {
                    pool,
                    tickBitmapIndex: word as i16,
                }),
            });
        }
        spans.push((start, items.len(), idx));
    }

    if items.is_empty() && algebra_targets.is_empty() {
        return 0;
    }

    let mut updated = 0usize;
    if !items.is_empty() {
        let Ok(results) =
            crate::pipeline::multicall::execute_multicall_at(provider, &items, block_number).await
        else {
            return enrich_algebra_ticks(
                provider,
                arena,
                &algebra_targets,
                algebra_integral_pools,
                block_number,
            )
            .await;
        };

        for (start, end, idx) in spans {
            let mut ticks: Vec<V3Tick> = Vec::new();
            for bytes in results[start..end].iter().flatten() {
                if let Ok(populated) =
                    ITickLens::getPopulatedTicksInWordCall::abi_decode_returns(bytes)
                {
                    for pt in populated {
                        ticks.push(V3Tick {
                            tick: pt.tick.as_i32(),
                            liquidity_gross: pt.liquidityGross,
                            liquidity_net: pt.liquidityNet,
                        });
                    }
                }
            }
            if ticks.is_empty() {
                continue;
            }
            ticks.sort_unstable();
            if let Some(crate::core::types::PoolState::V3(s)) = arena.pool_state_mut(idx) {
                s.ticks = Arc::from(ticks);
                updated += 1;
            }
        }
    }
    updated += enrich_algebra_ticks(
        provider,
        arena,
        &algebra_targets,
        algebra_integral_pools,
        block_number,
    )
    .await;
    updated
}

pub async fn enrich_v4_ticks<
    P: alloy::providers::Provider<alloy::network::Ethereum> + Clone + Send + 'static,
>(
    provider: &P,
    arena: &mut StateArena,
    targets: &[(PoolIndex, FixedBytes<32>)],
    word_range: i16,
    block_number: Option<u64>,
) -> usize {
    use crate::pipeline::abi_cache::{decode_abi_word, encode_extsload};
    use crate::pipeline::multicall::{MulticallItem, execute_multicall_at};
    use alloy::primitives::U256;

    if targets.is_empty() {
        return 0;
    }

    let manager = UNISWAP_V4_POOL_MANAGER;
    let mut bitmap_calls = Vec::new();
    let mut spans = Vec::new();
    for &(idx, pool_id) in targets {
        let Some(crate::core::types::PoolState::V4(s)) = arena.pool_state(idx) else {
            continue;
        };
        let spacing = s.tick_spacing.max(1);
        let center_word = (s.tick / spacing) >> 8;
        let word_min = center_word - word_range as i32;
        let word_max = center_word + word_range as i32;
        let start = bitmap_calls.len();
        for word in word_min..=word_max {
            let slot = compute_v4_tick_bitmap_slot(&pool_id, word as i16);
            bitmap_calls.push(MulticallItem {
                target: manager,
                data: encode_extsload(slot),
            });
        }
        spans.push((idx, pool_id, spacing, word_min, start, bitmap_calls.len()));
    }
    if bitmap_calls.is_empty() {
        return 0;
    }

    let Ok(bitmaps) = execute_multicall_at(provider, &bitmap_calls, block_number).await else {
        return 0;
    };

    let mut tick_calls = Vec::new();
    let mut tick_owners = Vec::new();
    for (idx, pool_id, spacing, word_min, start, end) in spans {
        for (offset, bytes) in bitmaps[start..end].iter().enumerate() {
            let Some(bytes) = bytes else {
                continue;
            };
            let Some(bitmap) = decode_abi_word(bytes) else {
                continue;
            };
            for bit in 0..256u16 {
                if ((bitmap >> bit) & U256::from(1u8)).is_zero() {
                    continue;
                }
                let compressed = (word_min + offset as i32) * 256 + i32::from(bit);
                let tick = compressed.saturating_mul(spacing);
                let clamped = tick.clamp(-8_388_608, 8_388_607);
                let slot = compute_v4_tick_info_slot(&pool_id, clamped);
                tick_calls.push(MulticallItem {
                    target: manager,
                    data: encode_extsload(slot),
                });
                tick_owners.push((idx, clamped));
            }
        }
    }

    if tick_calls.is_empty() {
        return 0;
    }

    let Ok(states) = execute_multicall_at(provider, &tick_calls, block_number).await else {
        return 0;
    };

    let mut grouped: rustc_hash::FxHashMap<PoolIndex, Vec<V3Tick>> =
        rustc_hash::FxHashMap::default();
    for ((idx, tick), bytes) in tick_owners.into_iter().zip(states) {
        let Some(bytes) = bytes else {
            continue;
        };
        let Some(raw) = decode_abi_word(&bytes) else {
            continue;
        };
        let (liquidity_gross, liquidity_net) = decode_v4_tick_liquidity(raw);
        if liquidity_gross > 0 {
            grouped.entry(idx).or_default().push(V3Tick {
                tick,
                liquidity_gross,
                liquidity_net,
            });
        }
    }

    let mut updated = 0;
    for (idx, mut ticks) in grouped {
        ticks.sort_unstable();
        if let Some(crate::core::types::PoolState::V4(state)) = arena.pool_state_mut(idx) {
            state.ticks = Arc::from(ticks);
            updated += 1;
        }
    }
    updated
}

async fn enrich_algebra_ticks<
    P: alloy::providers::Provider<alloy::network::Ethereum> + Clone + Send + 'static,
>(
    provider: &P,
    arena: &mut StateArena,
    targets: &[(Address, PoolIndex, i32, i32, i32)],
    integral_pools: &FxHashSet<Address>,
    block_number: Option<u64>,
) -> usize {
    use crate::abis::{IAlgebraIntegralPool, IAlgebraPool};
    use crate::pipeline::multicall::{MulticallItem, encode_call, execute_multicall_at};
    use alloy::primitives::U256;
    use alloy::sol_types::SolCall;

    let word_count: usize = targets
        .iter()
        .map(|(_, _, _, word_min, word_max)| (word_max - word_min + 1) as usize)
        .sum();
    let mut bitmap_calls = Vec::with_capacity(word_count);
    let mut spans = Vec::with_capacity(targets.len());
    for &(pool, idx, spacing, word_min, word_max) in targets {
        let start = bitmap_calls.len();
        for word in word_min..=word_max {
            bitmap_calls.push(MulticallItem {
                target: pool,
                data: encode_call(&IAlgebraPool::tickTableCall {
                    wordPosition: word as i16,
                }),
            });
        }
        spans.push((pool, idx, spacing, word_min, start, bitmap_calls.len()));
    }
    if bitmap_calls.is_empty() {
        return 0;
    }
    let Ok(bitmaps) = execute_multicall_at(provider, &bitmap_calls, block_number).await else {
        return 0;
    };
    let mut tick_calls = Vec::new();
    let mut tick_owners = Vec::new();
    for (pool, idx, spacing, word_min, start, end) in spans {
        for (offset, bytes) in bitmaps[start..end].iter().enumerate() {
            let Some(bytes) = bytes else {
                continue;
            };
            let Ok(bitmap) = IAlgebraPool::tickTableCall::abi_decode_returns(bytes) else {
                continue;
            };
            let bitmap = U256::from(bitmap);
            for bit in 0..256u16 {
                if ((bitmap >> bit) & U256::from(1u8)).is_zero() {
                    continue;
                }
                let compressed = (word_min + offset as i32) * 256 + i32::from(bit);
                let tick = compressed.saturating_mul(spacing);
                let clamped: i32 = tick.clamp(-8_388_608, 8_388_607);
                tick_calls.push(MulticallItem {
                    target: pool,
                    data: encode_call(&IAlgebraPool::ticksCall {
                        tick: clamped
                            .try_into()
                            .expect("clamped Algebra tick must fit in I24"),
                    }),
                });
                tick_owners.push((pool, idx, clamped));
            }
        }
    }
    let Ok(states) = execute_multicall_at(provider, &tick_calls, block_number).await else {
        return 0;
    };
    let mut grouped: rustc_hash::FxHashMap<PoolIndex, Vec<V3Tick>> =
        rustc_hash::FxHashMap::default();
    for ((pool, idx, tick), bytes) in tick_owners.into_iter().zip(states) {
        let Some(bytes) = bytes else {
            continue;
        };
        let tick_entry = if integral_pools.contains(&pool) {
            let Ok(state) = IAlgebraIntegralPool::ticksCall::abi_decode_returns(&bytes) else {
                continue;
            };
            let Ok(liquidity_gross) = u128::try_from(state.liquidityTotal) else {
                continue;
            };
            if liquidity_gross == 0 {
                continue;
            }
            V3Tick {
                tick,
                liquidity_gross,
                liquidity_net: state.liquidityDelta,
            }
        } else {
            let Ok(state) = IAlgebraPool::ticksCall::abi_decode_returns(&bytes) else {
                continue;
            };
            if !state.initialized || state.liquidityTotal == 0 {
                continue;
            }
            V3Tick {
                tick,
                liquidity_gross: state.liquidityTotal,
                liquidity_net: state.liquidityDelta,
            }
        };
        grouped.entry(idx).or_default().push(tick_entry);
    }
    let mut updated = 0;
    for (idx, mut ticks) in grouped {
        ticks.sort_unstable();
        if let Some(crate::core::types::PoolState::V3(state)) = arena.pool_state_mut(idx) {
            state.ticks = Arc::from(ticks);
            updated += 1;
        }
    }
    updated
}
