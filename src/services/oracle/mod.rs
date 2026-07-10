pub mod price_oracle;
pub mod rates;

pub use rates::{
    has_reliable_matic_rate, resolve_token_to_matic_rate, resolve_token_to_matic_rate_or_bootstrap,
};

use std::sync::Arc;

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::primitives::U256;
use alloy::providers::Provider;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};

use crate::core::constants::{POLYGON_HUB_TOKENS, WMATIC};
use crate::core::types::{PoolTokenAddrs, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::services::discovery::TokenMeta;

use self::price_oracle::{PriceOracle, token_usd_to_matic_rate_per_unit};

#[must_use]
pub fn token_decimals_map(metas: &[TokenMeta]) -> FxHashMap<Address, u8> {
    let mut out = FxHashMap::with_capacity_and_hasher(metas.len(), FxBuildHasher);
    for meta in metas {
        out.insert(meta.address, meta.decimals);
    }
    out
}

#[must_use]
pub fn resolve_token_decimals(token: Address, hints: &FxHashMap<Address, u8>) -> u8 {
    hints.get(&token).copied().unwrap_or_else(|| {
        // Assume 18 when TokenMeta is incomplete.
        // Tokens without decimals are excluded from the routing graph by
        // resolvable_token_set -> has_known_decimals, so this path only
        // fires for non-routed code.
        crate::warn!(
            "token decimals missing from metadata — assuming 18 (token={})",
            token
        );
        18
    })
}

#[must_use]
pub fn resolve_token_decimals_for_index(
    token: TokenIndex,
    arena: &StateArena,
    hints: &FxHashMap<Address, u8>,
) -> u8 {
    let idx = token.0 as usize;
    if idx < arena.token_count() as usize {
        return arena.token_decimals(token);
    }
    // Tests / partial arenas without sync still resolve through the address map.
    arena
        .token_address(token)
        .map_or(18, |addr| resolve_token_decimals(addr, hints))
}

/// Builds the set of token addresses whose MATIC rate is at or above the dust floor.
#[must_use]
pub fn resolvable_token_set(
    rates: &FxHashMap<TokenIndex, U256>,
    arena: &StateArena,
) -> FxHashSet<Address> {
    rates
        .iter()
        .filter(|(_, rate)| **rate >= crate::core::constants::MIN_TOKEN_TO_MATIC_RATE)
        .filter_map(|(token, _)| arena.token_address(*token))
        .collect()
}

/// Extend graph resolvability to tokens that share a tradable pool with a hub or
/// already-priced token. Keeps profit conversion on oracle rates; this only grows
/// routing connectivity for long-tail spokes.
pub fn expand_hub_spoke_resolvable(
    resolvable: &mut FxHashSet<Address>,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    arena: &StateArena,
) {
    for hub in POLYGON_HUB_TOKENS {
        resolvable.insert(hub);
    }
    if pool_metas.is_empty() {
        return;
    }

    let mut pool_tokens: Vec<PoolTokenAddrs> = Vec::with_capacity(pool_metas.len());
    let mut token_to_pools: FxHashMap<Address, Vec<usize>> =
        FxHashMap::with_capacity_and_hasher(pool_metas.len().saturating_mul(2), FxBuildHasher);
    for (pool_idx, meta) in pool_metas.iter().enumerate() {
        let addrs: PoolTokenAddrs = meta
            .tokens
            .iter()
            .filter_map(|t| arena.token_address(*t))
            .collect();
        for addr in &addrs {
            token_to_pools.entry(*addr).or_default().push(pool_idx);
        }
        pool_tokens.push(addrs);
    }

    let mut queue: std::collections::VecDeque<Address> = resolvable.iter().copied().collect();
    while let Some(seed) = queue.pop_front() {
        let Some(pool_ids) = token_to_pools.get(&seed) else {
            continue;
        };
        for &pool_idx in pool_ids {
            for addr in &pool_tokens[pool_idx] {
                if resolvable.insert(*addr) {
                    queue.push_back(*addr);
                }
            }
        }
    }
}

/// Merge oracle rates without cloning `prior` when `fresh` is empty or unchanged.
#[must_use]
pub fn merge_token_rates(
    prior: &Arc<FxHashMap<TokenIndex, U256>>,
    fresh: FxHashMap<TokenIndex, U256>,
) -> Arc<FxHashMap<TokenIndex, U256>> {
    if fresh.is_empty() {
        return Arc::clone(prior);
    }
    if prior.is_empty() {
        return Arc::new(fresh);
    }
    let needs_merge = fresh
        .iter()
        .any(|(token, rate)| prior.get(token) != Some(rate));
    if !needs_merge {
        return Arc::clone(prior);
    }
    let mut merged =
        FxHashMap::with_capacity_and_hasher(prior.len().saturating_add(fresh.len()), FxBuildHasher);
    merged.extend(prior.iter().map(|(&k, &v)| (k, v)));
    merged.extend(fresh);
    Arc::new(merged)
}

pub async fn enrich_token_to_matic_rates<P, I>(
    oracle: &PriceOracle,
    arena: &StateArena,
    tokens: I,
    provider: Option<&P>,
) -> FxHashMap<TokenIndex, U256>
where
    P: Provider<Ethereum> + Clone + Send + 'static,
    I: IntoIterator<Item = TokenIndex>,
{
    let tokens: Vec<TokenIndex> = tokens.into_iter().collect();
    let addrs = token_addresses(arena, &tokens);
    oracle.prefetch_token_usd(&addrs, provider).await;
    let matic_usd = match oracle.cached_matic_usd() {
        Some(usd) => usd,
        None if provider.is_some() => oracle.get_matic_usd(provider).await,
        None => oracle.get_matic_usd_offline().await,
    };
    build_token_to_matic_rates(oracle, arena, &tokens, matic_usd)
}

/// Pyth + in-memory cache only (no Chainlink RPC). Used when state RPC is down.
pub async fn enrich_token_to_matic_rates_offline<I>(
    oracle: &PriceOracle,
    arena: &StateArena,
    tokens: I,
) -> FxHashMap<TokenIndex, U256>
where
    I: IntoIterator<Item = TokenIndex>,
{
    let tokens: Vec<TokenIndex> = tokens.into_iter().collect();
    let addrs = token_addresses(arena, &tokens);
    oracle.prefetch_token_usd_offline(&addrs).await;
    let matic_usd = oracle.get_matic_usd_offline().await;
    build_token_to_matic_rates(oracle, arena, &tokens, matic_usd)
}

fn token_addresses(arena: &StateArena, tokens: &[TokenIndex]) -> Vec<Address> {
    let mut addrs: Vec<Address> = tokens
        .iter()
        .filter_map(|idx| arena.token_address(*idx))
        .collect();
    addrs.sort_unstable();
    addrs.dedup();
    addrs
}

fn build_token_to_matic_rates(
    oracle: &PriceOracle,
    arena: &StateArena,
    tokens: &[TokenIndex],
    matic_usd: f64,
) -> FxHashMap<TokenIndex, U256> {
    let wmatic = WMATIC;
    let rate_one = crate::core::constants::RATE_PRECISION;
    let mut out = FxHashMap::with_capacity_and_hasher(tokens.len(), FxBuildHasher);
    for idx in tokens {
        let Some(addr) = arena.token_address(*idx) else {
            continue;
        };
        let rate = if addr == wmatic {
            oracle.token_matic_rate_per_unit_integer(&wmatic)
        } else {
            oracle.token_matic_rate_per_unit_integer(&addr).or_else(|| {
                if matic_usd > 0.0 {
                    oracle
                        .token_usd(&addr)
                        .map(|usd| token_usd_to_matic_rate_per_unit(usd, matic_usd))
                } else {
                    None
                }
            })
        }
        .filter(|r| *r >= crate::core::constants::MIN_TOKEN_TO_MATIC_RATE);
        if let Some(rate) = rate {
            // ponytail: log source path — integer (Chainlink) vs f64 (Pyth) for debugging
            let path =
                if addr == wmatic || oracle.token_matic_rate_per_unit_integer(&addr).is_some() {
                    "int"
                } else {
                    "f64"
                };
            crate::trace!("rate {} {rate} path={path}", addr);
            out.insert(*idx, rate);
        }
    }
    // ponytail: fallback when no oracle feeds — mark WMATIC as self-resolving
    // so that WMATIC-paired pools become routable. Without this, zero oracle
    // feeds = zero resolvable tokens = zero cycles.
    if out.is_empty() {
        for idx in tokens {
            if arena.token_address(*idx).is_some_and(|a| a == wmatic) {
                out.insert(*idx, rate_one);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolIndex, ProtocolType, TokenIndex};
    use crate::pipeline::arena::StateArena;
    use crate::pipeline::types::PoolMeta;

    #[test]
    fn merge_token_rates_skips_clone_when_fresh_is_redundant() {
        let prior = Arc::new(FxHashMap::from_iter([
            (TokenIndex(0), U256::from(1_000u64)),
            (TokenIndex(1), U256::from(2_000u64)),
        ]));
        let merged = merge_token_rates(
            &prior,
            FxHashMap::from_iter([(TokenIndex(0), U256::from(1_000u64))]),
        );
        assert!(Arc::ptr_eq(&prior, &merged));
        let changed = merge_token_rates(
            &prior,
            FxHashMap::from_iter([(TokenIndex(0), U256::from(9_000u64))]),
        );
        assert!(!Arc::ptr_eq(&prior, &changed));
        assert_eq!(
            changed.get(&TokenIndex(0)).copied(),
            Some(U256::from(9_000u64))
        );
        assert_eq!(
            changed.get(&TokenIndex(1)).copied(),
            Some(U256::from(2_000u64))
        );
    }

    #[test]
    fn expand_hub_spoke_reaches_long_tail_without_decimals_map() {
        let mut arena = StateArena::default();
        let hub = POLYGON_HUB_TOKENS[0];
        let tail = Address::from([9u8; 20]);
        let hub_idx = arena.register_token(hub);
        let tail_idx = arena.register_token(tail);
        let pool_idx = PoolIndex(0);
        let metas = [PoolMeta {
            pool_index: pool_idx,
            protocol: ProtocolType::UniswapV2,
            tokens: vec![hub_idx, tail_idx],
            fee_bps: 30,
            bpt_index: None,
            pool_id: None,
            protocol_label: None,
            pool_type: None,
            hooks: None,
            tick_spacing: None,
        }];

        let mut resolvable = FxHashSet::default();
        expand_hub_spoke_resolvable(&mut resolvable, &metas, &arena);
        assert!(resolvable.contains(&hub));
        assert!(resolvable.contains(&tail));
    }
}
