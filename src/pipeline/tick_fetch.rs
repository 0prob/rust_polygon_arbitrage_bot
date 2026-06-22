use alloy::primitives::Address;

use crate::core::types::{FoundCycle, ProtocolType};
use crate::pipeline::arena::StateArena;

const MAX_TICK_POOLS: usize = 24;

pub fn collect_v3_pool_addresses(arena: &StateArena, cycles: &[FoundCycle]) -> Vec<Address> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    'cycles: for cycle in cycles {
        for edge in &cycle.edges {
            if !matches!(
                edge.protocol,
                ProtocolType::UniswapV3 | ProtocolType::UniswapV4
            ) {
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

pub async fn enrich_v3_ticks<P: alloy::providers::Provider<alloy::network::Ethereum>>(
    provider: &P,
    arena: &mut StateArena,
    pool_addresses: &[Address],
    word_range: i16,
) -> usize {
    use alloy::sol_types::SolCall;

    use crate::abis::ITickLens;
    use crate::core::constants::TICK_LENS_POLYGON;
use crate::core::types::V3Tick;
use crate::pipeline::multicall::{MulticallItem, encode_call, execute_multicall};

    if pool_addresses.is_empty() {
        return 0;
    }
    let tick_lens = TICK_LENS_POLYGON;
    let mut items = Vec::new();
    let mut spans: Vec<(Address, usize, usize)> = Vec::new();

    for &pool in pool_addresses {
        let Some(idx) = arena.address_to_pool().get(&pool).copied() else {
            continue;
        };
        let (tick, spacing) = match arena.pool_state(idx) {
            Some(crate::core::types::PoolState::V3(s)) => (s.tick, s.tick_spacing),
            Some(crate::core::types::PoolState::V4(s)) => (s.tick, s.tick_spacing),
            _ => continue,
        };
        let center_word = (tick / spacing.max(1)) >> 8;
        let word_min = center_word - word_range as i32;
        let word_max = center_word + word_range as i32;
        let start = items.len();
        for word in word_min..=word_max {
            items.push(MulticallItem {
                target: tick_lens,
                data: encode_call(&ITickLens::getPopulatedTicksInWordCall {
                    pool,
                    tickBitmapIndex: word as i16,
                }),
            });
        }
        spans.push((pool, start, items.len()));
    }

    if items.is_empty() {
        return 0;
    }

    let Ok(results) = execute_multicall(provider, &items).await else {
        return 0;
    };

    let mut updated = 0usize;
    for (pool, start, end) in spans {
        let mut ticks: Vec<V3Tick> = Vec::new();
        for bytes in results[start..end].iter().flatten() {
            if let Ok(populated) = ITickLens::getPopulatedTicksInWordCall::abi_decode_returns(bytes)
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
        ticks.sort_by_key(|t| t.tick);
        let Some(idx) = arena.address_to_pool().get(&pool).copied() else {
            continue;
        };
        match arena.pool_state_mut(idx) {
            Some(crate::core::types::PoolState::V3(s)) => {
                s.ticks = ticks.into_boxed_slice();
                updated += 1;
            }
            Some(crate::core::types::PoolState::V4(s)) => {
                s.ticks = ticks.into_boxed_slice();
                updated += 1;
            }
            _ => {}
        }
    }
    updated
}
