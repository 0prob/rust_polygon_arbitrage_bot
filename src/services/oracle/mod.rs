pub mod price_oracle;
pub mod rates;

pub use rates::{
    has_reliable_matic_rate, resolve_token_to_matic_rate, resolve_token_to_matic_rate_or_bootstrap,
};

use std::sync::Arc;
use std::time::Instant;

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::primitives::U256;
use alloy::providers::Provider;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};

use crate::core::constants::{MAX_SUPPORTED_TOKEN_DECIMALS, POLYGON_HUB_TOKENS, WMATIC};
use crate::core::types::{FoundCycle, PoolTokenAddrs, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::services::discovery::TokenMeta;

use self::price_oracle::{PriceOracle, token_usd_to_matic_rate_per_unit};

/// MATIC/USD for flash borrow caps — see [`PriceOracle::ensure_matic_usd_for_flash_cap`].
pub async fn ensure_matic_usd_for_flash_cap<P>(
    oracle: &PriceOracle,
    state_provider: Option<&P>,
) -> Option<f64>
where
    P: Provider<Ethereum>,
{
    oracle.ensure_matic_usd_for_flash_cap(state_provider).await
}

#[must_use]
pub fn token_decimals_map(metas: &[TokenMeta]) -> FxHashMap<Address, u8> {
    let mut out = FxHashMap::with_capacity_and_hasher(metas.len(), FxBuildHasher);
    for meta in metas {
        if meta.decimals <= MAX_SUPPORTED_TOKEN_DECIMALS {
            out.insert(meta.address, meta.decimals);
        }
    }
    out
}

/// Execution paths require explicit, bounded decimal metadata for every route token.
#[must_use]
pub fn cycle_tokens_have_known_decimals(
    cycle: &FoundCycle,
    arena: &StateArena,
    hints: &FxHashMap<Address, u8>,
) -> bool {
    !cycle.edges.is_empty()
        && arena
            .token_address(cycle.start_token)
            .and_then(|address| hints.get(&address))
            .is_some_and(|decimals| *decimals <= MAX_SUPPORTED_TOKEN_DECIMALS)
        && cycle.edges.iter().all(|edge| {
            [edge.token_in, edge.token_out].into_iter().all(|token| {
                arena
                    .token_address(token)
                    .and_then(|address| hints.get(&address))
                    .is_some_and(|decimals| *decimals <= MAX_SUPPORTED_TOKEN_DECIMALS)
            })
        })
}

#[must_use]
pub fn resolve_token_decimals(token: Address, hints: &FxHashMap<Address, u8>) -> u8 {
    hints.get(&token).copied().unwrap_or_else(|| {
        crate::debug!("token decimals missing from metadata — assuming 18 (token={token})");
        18
    })
}

/// Arena-registered tokens with no entry in the discovery decimals map.
#[must_use]
pub fn arena_tokens_without_decimal_hints(
    arena: &StateArena,
    hints: &FxHashMap<Address, u8>,
) -> usize {
    let mut missing = 0usize;
    for i in 0..arena.token_count() {
        let Some(addr) = arena.token_address(TokenIndex(i)) else {
            continue;
        };
        if !hints.contains_key(&addr) {
            missing += 1;
        }
    }
    missing
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RateEnrichStats {
    pub requested: usize,
    pub resolved: usize,
    pub chainlink: usize,
    pub pyth_or_float: usize,
    pub unresolved: usize,
}

pub async fn enrich_token_to_matic_rates<P, I>(
    oracle: &PriceOracle,
    arena: &StateArena,
    tokens: I,
    provider: Option<&P>,
) -> (FxHashMap<TokenIndex, U256>, RateEnrichStats)
where
    P: Provider<Ethereum> + Clone + Send + 'static,
    I: IntoIterator<Item = TokenIndex>,
{
    let tokens = dedupe_token_indices(tokens);
    let addrs = prefetch_addrs_for_rates(arena, &tokens);
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
) -> (FxHashMap<TokenIndex, U256>, RateEnrichStats)
where
    I: IntoIterator<Item = TokenIndex>,
{
    let tokens = dedupe_token_indices(tokens);
    let addrs = prefetch_addrs_for_rates(arena, &tokens);
    oracle.prefetch_token_usd_offline(&addrs).await;
    let matic_usd = oracle.get_matic_usd_offline().await;
    build_token_to_matic_rates(oracle, arena, &tokens, matic_usd)
}

fn dedupe_token_indices<I>(tokens: I) -> Vec<TokenIndex>
where
    I: IntoIterator<Item = TokenIndex>,
{
    let mut seen = FxHashSet::default();
    tokens.into_iter().filter(|t| seen.insert(*t)).collect()
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

/// Cycle tokens plus static hub feeds — one prefetch warms profit conversion
/// for spoke tokens even when they are not on the current cycle list.
fn prefetch_addrs_for_rates(arena: &StateArena, tokens: &[TokenIndex]) -> Vec<Address> {
    let mut addrs = token_addresses(arena, tokens);
    addrs.extend(POLYGON_HUB_TOKENS);
    addrs.sort_unstable();
    addrs.dedup();
    addrs
}

fn build_token_to_matic_rates(
    oracle: &PriceOracle,
    arena: &StateArena,
    tokens: &[TokenIndex],
    matic_usd: f64,
) -> (FxHashMap<TokenIndex, U256>, RateEnrichStats) {
    let wmatic = WMATIC;
    let rate_one = crate::core::constants::RATE_PRECISION;
    let mut stats = RateEnrichStats {
        requested: tokens.len(),
        ..RateEnrichStats::default()
    };
    let mut out = FxHashMap::with_capacity_and_hasher(tokens.len(), FxBuildHasher);
    for idx in tokens {
        let Some(addr) = arena.token_address(*idx) else {
            stats.unresolved += 1;
            continue;
        };
        let chainlink = if addr == wmatic {
            oracle.token_matic_rate_per_unit_integer(&wmatic)
        } else {
            oracle.token_matic_rate_per_unit_integer(&addr)
        };
        let rate = chainlink
            .or_else(|| {
                if matic_usd > 0.0 {
                    oracle
                        .token_usd(&addr)
                        .map(|usd| token_usd_to_matic_rate_per_unit(usd, matic_usd))
                } else {
                    None
                }
            })
            .filter(|r| *r >= crate::core::constants::MIN_TOKEN_TO_MATIC_RATE);
        if let Some(rate) = rate {
            if chainlink.is_some() || addr == wmatic {
                stats.chainlink += 1;
            } else {
                stats.pyth_or_float += 1;
            }
            stats.resolved += 1;
            out.insert(*idx, rate);
        } else {
            stats.unresolved += 1;
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
    if !out.is_empty() {
        *oracle.rates_updated_at.write() = Some(Instant::now());
    }
    if stats.requested > 0 {
        crate::debug!(
            "token rates enrich: requested={} resolved={} chainlink={} pyth_or_float={} unresolved={}",
            stats.requested,
            stats.resolved,
            stats.chainlink,
            stats.pyth_or_float,
            stats.unresolved
        );
    }
    (out, stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{CycleEdges, Edge, FoundCycle, PoolIndex, ProtocolType, TokenIndex};
    use crate::pipeline::arena::StateArena;
    use crate::pipeline::types::PoolMeta;

    #[test]
    fn arena_tokens_without_decimal_hints_counts_gaps() {
        let mut arena = StateArena::default();
        let a: Address = "0x00000000000000000000000000000000000000aa"
            .parse()
            .expect("addr");
        let b: Address = "0x00000000000000000000000000000000000000bb"
            .parse()
            .expect("addr");
        arena.register_token(a);
        arena.register_token(b);
        let mut hints = FxHashMap::default();
        hints.insert(a, 18);
        assert_eq!(arena_tokens_without_decimal_hints(&arena, &hints), 1);
    }

    #[test]
    fn execution_cycles_require_known_bounded_decimals_for_every_token() {
        let mut arena = StateArena::default();
        let a = Address::with_last_byte(1);
        let b = Address::with_last_byte(2);
        let c = Address::with_last_byte(3);
        let a_idx = arena.register_token(a);
        let b_idx = arena.register_token(b);
        let c_idx = arena.register_token(c);
        let edge = |token_in, token_out| Edge {
            pool_index: PoolIndex(0),
            token_in,
            token_out,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let cycle = FoundCycle {
            start_token: a_idx,
            edges: CycleEdges::from(vec![
                edge(a_idx, b_idx),
                edge(b_idx, c_idx),
                edge(c_idx, a_idx),
            ]),
            hop_count: 3,
            log_weight: 0.0,
            cumulative_fee_bps: 90,
            score: 0.0,
            cycle_ratio: U256::ONE,
        };
        let mut hints = FxHashMap::default();
        hints.insert(a, 0);
        hints.insert(b, 6);
        assert!(!cycle_tokens_have_known_decimals(&cycle, &arena, &hints));
        hints.insert(c, 18);
        assert!(cycle_tokens_have_known_decimals(&cycle, &arena, &hints));
        hints.insert(c, 31);
        assert!(!cycle_tokens_have_known_decimals(&cycle, &arena, &hints));
    }

    #[test]
    fn dedupe_token_indices_drops_duplicates() {
        let v = dedupe_token_indices([TokenIndex(1), TokenIndex(1), TokenIndex(2), TokenIndex(2)]);
        assert_eq!(v.len(), 2);
    }

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
    fn prefetch_addrs_for_rates_includes_hubs() {
        use alloy::primitives::address;
        let mut arena = StateArena::default();
        let usdc = address!("0x2791bca1f2de4661ed88a30c99a7a9489c09eb3f");
        let usdc_idx = arena.register_token(usdc);
        let addrs = prefetch_addrs_for_rates(&arena, &[usdc_idx]);
        assert!(addrs.contains(&usdc));
        assert!(addrs.contains(&POLYGON_HUB_TOKENS[0]));
        assert!(addrs.len() >= POLYGON_HUB_TOKENS.len());
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
