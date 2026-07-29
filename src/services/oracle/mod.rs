pub mod auto_feeds;
pub mod feed_audit;
pub mod feed_verify;
pub mod hub_path_rates;
pub mod lst_rates;
pub mod price_oracle;
pub mod pyth_catalog;
pub mod rates;
pub mod token_labels;

pub use auto_feeds::{
    AUTO_FEED_SCAN_BATCH, default_auto_feeds_path, load_and_apply_auto_feeds,
    note_unmapped_addresses, pending_auto_feed_count, spawn_auto_feed_sidecar,
};
pub use feed_audit::{
    CURATED_POLYGON_TOKEN_HINTS, UsdFeedScanRow, UsdFeedScanStatus, default_runtime_demand_path,
    hint_label, load_runtime_demand_snapshot, log_ranked_unmapped_demand,
    parse_runtime_demand_from_log, persist_runtime_demand_snapshot, record_unmapped_token_demand,
    scan_addresses_for_usd_feeds, snapshot_runtime_unmapped_demand, token_symbol_label,
};
pub use feed_verify::{
    ProposedPythFeed, VerifiedPythFeed, format_config_pyth_feeds, parse_proposed_pyth_feed_lines,
    verify_proposed_pyth_feeds,
};
pub use price_oracle::{OracleFeedSources, builtin_chainlink_feed, builtin_pyth_feed_id};
pub use pyth_catalog::{
    pick_best_rr_candidate_for_hint, pick_best_usd_candidate_for_hint, pyth_symbol_matches_hint,
};

pub use hub_path_rates::HubPathRateParams;
pub use hub_path_rates::hub_path_matic_rates_batch;
pub use rates::{
    has_reliable_matic_rate, resolve_token_to_matic_rate, resolve_token_to_matic_rate_or_bootstrap,
};

use std::sync::Arc;

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::primitives::U256;
use alloy::providers::Provider;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};

use crate::core::constants::{
    MAX_SUPPORTED_TOKEN_DECIMALS, MIN_TOKEN_TO_MATIC_RATE, POLYGON_HUB_TOKENS, RATE_PRECISION,
    WMATIC,
};
use crate::core::types::{FoundCycle, PoolTokenAddrs, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::services::discovery::TokenMeta;

use self::price_oracle::{PriceOracle, token_usd_to_matic_rate_per_unit};
use crate::pipeline::sim_sanity::matic_usd_for_flash_cap;
use crate::pipeline::types::RoutingGraph;

/// Hint from HF eval, else live oracle refresh — for dispatch flash-cap sizing.
pub async fn resolve_matic_usd_for_flash_dispatch<P>(
    oracle: &PriceOracle,
    hint: Option<f64>,
    provider: &P,
) -> Option<f64>
where
    P: Provider<Ethereum> + Clone + Send + 'static,
{
    if let Some(hint) = hint.and_then(matic_usd_for_flash_cap) {
        // Only trust the HF hint when it still matches the oracle cache (same tick / no drift).
        if oracle
            .resolve_matic_usd_cached()
            .and_then(matic_usd_for_flash_cap)
            .is_some_and(|cached| (cached - hint).abs() < f64::EPSILON)
        {
            return Some(hint);
        }
    }
    oracle.ensure_matic_usd_for_flash_cap(Some(provider)).await
}

#[inline]
fn matic_usd_for_lf_rate_enrich(raw: f64, context: &'static str) -> f64 {
    match matic_usd_for_flash_cap(raw) {
        Some(usd) => usd,
        None => {
            crate::warn!(
                "{context}: MATIC/USD not usable for flash cap (raw={raw}) — token/MATIC rates may be incomplete"
            );
            0.0
        }
    }
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

/// Explicit decimals from discovery/on-chain map only (no arena 18-default).
#[must_use]
pub fn explicit_decimals_for_index(
    token: TokenIndex,
    arena: &StateArena,
    hints: &FxHashMap<Address, u8>,
) -> Option<u8> {
    arena
        .token_address(token)
        .and_then(|addr| known_token_decimals(addr, hints))
}

/// Execution paths require explicit, bounded decimal metadata for every route token.
#[must_use]
pub fn cycle_tokens_have_known_decimals(
    cycle: &FoundCycle,
    arena: &StateArena,
    hints: &FxHashMap<Address, u8>,
) -> bool {
    if cycle.edges.is_empty() {
        return false;
    }
    if explicit_decimals_for_index(cycle.start_token, arena, hints).is_none() {
        return false;
    }
    cycle.edges.iter().all(|edge| {
        [edge.token_in, edge.token_out]
            .into_iter()
            .all(|token| explicit_decimals_for_index(token, arena, hints).is_some())
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

/// Addresses in the arena lacking discovery/on-chain decimal metadata (capped).
#[must_use]
pub fn arena_missing_decimal_addresses(
    arena: &StateArena,
    hints: &FxHashMap<Address, u8>,
    limit: usize,
) -> Vec<Address> {
    if limit == 0 {
        return Vec::new();
    }
    let mut missing = Vec::new();
    let mut hubs = Vec::new();
    for i in 0..arena.token_count() {
        let Some(addr) = arena.token_address(TokenIndex(i)) else {
            continue;
        };
        if hints.contains_key(&addr) {
            continue;
        }
        if crate::core::constants::is_polygon_hub_token(addr) {
            hubs.push(addr);
        } else {
            missing.push(addr);
        }
    }
    hubs.extend(missing);
    hubs.truncate(limit);
    hubs
}

#[must_use]
pub fn resolve_token_decimals_for_index(
    token: TokenIndex,
    arena: &StateArena,
    hints: &FxHashMap<Address, u8>,
) -> u8 {
    explicit_decimals_for_index(token, arena, hints).unwrap_or_else(|| {
        arena
            .token_address(token)
            .map(|addr| resolve_token_decimals(addr, hints))
            .unwrap_or(18)
    })
}

/// Explicit decimals from discovery/on-chain enrichment only (no 18-decimal guess).
#[must_use]
pub fn known_token_decimals(token: Address, hints: &FxHashMap<Address, u8>) -> Option<u8> {
    hints
        .get(&token)
        .copied()
        .filter(|d| *d <= MAX_SUPPORTED_TOKEN_DECIMALS)
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

/// Spoke expansion: tokens sharing a pool with a hub or already-priced token.
///
/// Connectivity only — does not invent MATIC rates. Wired into
/// [`crate::pipeline::graph::GraphBuildGate::spoke_connectivity`] for graph admission.
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

/// Replace rates for tokens refreshed this LF pass and retain unrelated cached rates.
///
/// Soft cap on retained non-refreshed prior rates (unbounded growth otherwise).
const MERGED_RATES_SOFT_CAP: usize = 2_048;

/// Merge LF enrich output into the prior rate map.
///
/// Non-refreshed priors are retained because enrich only covers cycle+hub tokens.
/// A refreshed token missing from `fresh` is dropped so a failed USD refresh
/// cannot make an old rate look current in the next HF snapshot.
///
/// `retain_stale_prior=false` forces a rebuild (new Arc) when the snapshot aged
/// past quote TTL — it no longer wipes non-refreshed spokes.
#[must_use]
pub fn merge_token_rates(
    prior: &Arc<FxHashMap<TokenIndex, U256>>,
    refreshed_tokens: &FxHashSet<TokenIndex>,
    fresh: FxHashMap<TokenIndex, U256>,
    retain_stale_prior: bool,
) -> Arc<FxHashMap<TokenIndex, U256>> {
    if prior.is_empty() {
        return Arc::new(fresh);
    }
    let needs_merge = !retain_stale_prior
        || fresh
            .iter()
            .any(|(token, rate)| prior.get(token) != Some(rate))
        || refreshed_tokens
            .iter()
            .any(|token| prior.contains_key(token) != fresh.contains_key(token));
    if !needs_merge {
        return Arc::clone(prior);
    }
    let retained_prior = prior
        .iter()
        .filter(|(token, _)| !refreshed_tokens.contains(token))
        .count();
    let dropped_refreshed = refreshed_tokens
        .iter()
        .filter(|t| prior.contains_key(t) && !fresh.contains_key(t))
        .count();
    let fresh_len = fresh.len();
    let mut merged =
        FxHashMap::with_capacity_and_hasher(prior.len().saturating_add(fresh.len()), FxBuildHasher);
    // Keep non-refreshed priors first; fresh overwrites successful refreshes.
    merged.extend(
        prior
            .iter()
            .filter(|(token, _)| !refreshed_tokens.contains(token))
            .map(|(&token, &rate)| (token, rate)),
    );
    merged.extend(fresh);
    // Soft-cap: drop excess priors not touched this tick.
    if merged.len() > MERGED_RATES_SOFT_CAP {
        let overflow = merged.len() - MERGED_RATES_SOFT_CAP;
        let drop_keys: Vec<TokenIndex> = merged
            .keys()
            .copied()
            .filter(|t| !refreshed_tokens.contains(t))
            .take(overflow)
            .collect();
        for k in drop_keys {
            merged.remove(&k);
        }
    }
    if dropped_refreshed > 0 || !retain_stale_prior {
        crate::info!(
            "token rates merge: prior={} fresh={} retained_prior={} dropped_refreshed={} merged={} retain_stale={}",
            prior.len(),
            fresh_len,
            retained_prior,
            dropped_refreshed,
            merged.len(),
            retain_stale_prior
        );
    }
    Arc::new(merged)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RateEnrichStats {
    pub requested: usize,
    pub resolved: usize,
    /// Integer rates from on-chain Chainlink multicall.
    pub chainlink: usize,
    /// Integer rates whose USD raw last came from Hermes Pyth.
    pub pyth_integer: usize,
    /// Float USD÷MATIC fallback (rare when integer+hub both miss).
    pub pyth_or_float: usize,
    /// On-chain LST exchange rate × (implicit) POL backing.
    pub lst: usize,
    pub hub_path: usize,
    pub unresolved: usize,
}

/// LF/HF snapshot inputs for base-price enrichment (hub-path + oracle).
#[derive(Debug, Clone, Copy, Default)]
pub struct RateEnrichContext<'a> {
    pub graph: Option<&'a RoutingGraph>,
    pub hub_path: HubPathRateParams,
    /// Discovery/on-chain decimals for hub probe scaling (avoids arena default-18 skew).
    pub token_decimals: Option<&'a FxHashMap<Address, u8>>,
    /// Pool metas for V4 lazy hub-exit resolution in hub-path rates.
    pub pool_metas: Option<&'a [crate::pipeline::types::PoolMeta]>,
}

pub async fn enrich_token_to_matic_rates<P, I>(
    oracle: &PriceOracle,
    arena: &StateArena,
    tokens: I,
    provider: Option<&P>,
    ctx: RateEnrichContext<'_>,
) -> (FxHashMap<TokenIndex, U256>, RateEnrichStats)
where
    P: Provider<Ethereum> + Clone + Send + 'static,
    I: IntoIterator<Item = TokenIndex>,
{
    let tokens = dedupe_token_indices(tokens);
    let hub_rates = hub_rates_for_tokens(
        arena,
        ctx.graph,
        &tokens,
        ctx.hub_path,
        ctx.token_decimals,
        ctx.pool_metas,
    );
    let lst_rates = match provider {
        Some(p) => lst_rates::fetch_lst_matic_rates(arena, &tokens, p).await,
        None => FxHashMap::default(),
    };
    let addrs = prefetch_addrs_for_oracle_fallback(arena, oracle, &tokens, &hub_rates);
    oracle.prefetch_token_usd(&addrs, provider).await;
    let matic_usd = matic_usd_raw_for_lf_enrich(oracle, provider, true).await;
    build_token_to_matic_rates(oracle, arena, &tokens, matic_usd, &hub_rates, &lst_rates)
}

/// Pyth + in-memory cache only (no Chainlink RPC). Used when state RPC is down.
pub async fn enrich_token_to_matic_rates_offline<I>(
    oracle: &PriceOracle,
    arena: &StateArena,
    tokens: I,
    ctx: RateEnrichContext<'_>,
) -> (FxHashMap<TokenIndex, U256>, RateEnrichStats)
where
    I: IntoIterator<Item = TokenIndex>,
{
    let tokens = dedupe_token_indices(tokens);
    let hub_rates = hub_rates_for_tokens(
        arena,
        ctx.graph,
        &tokens,
        ctx.hub_path,
        ctx.token_decimals,
        ctx.pool_metas,
    );
    let addrs = prefetch_addrs_for_oracle_fallback(arena, oracle, &tokens, &hub_rates);
    oracle.prefetch_token_usd_offline(&addrs).await;
    let matic_usd = matic_usd_raw_for_lf_enrich_offline(oracle).await;
    // ponytail: LST views need RPC — offline keeps hub/Pyth only.
    build_token_to_matic_rates(
        oracle,
        arena,
        &tokens,
        matic_usd,
        &hub_rates,
        &FxHashMap::default(),
    )
}

async fn matic_usd_raw_for_lf_enrich<P>(
    oracle: &PriceOracle,
    provider: Option<&P>,
    allow_rpc: bool,
) -> f64
where
    P: Provider<Ethereum> + Clone + Send + 'static,
{
    let raw = match oracle.resolve_matic_usd_cached() {
        Some(usd) => usd,
        None if allow_rpc && provider.is_some() => oracle.get_matic_usd(provider).await,
        None => oracle.get_matic_usd_offline().await,
    };
    matic_usd_for_lf_rate_enrich(raw, "LF rate enrich")
}

async fn matic_usd_raw_for_lf_enrich_offline(oracle: &PriceOracle) -> f64 {
    let raw = match oracle.resolve_matic_usd_cached() {
        Some(usd) => usd,
        None => oracle.get_matic_usd_offline().await,
    };
    matic_usd_for_lf_rate_enrich(raw, "LF offline rate enrich")
}

fn hub_rates_for_tokens(
    arena: &StateArena,
    graph: Option<&RoutingGraph>,
    tokens: &[TokenIndex],
    params: HubPathRateParams,
    token_decimals: Option<&FxHashMap<Address, u8>>,
    pool_metas: Option<&[crate::pipeline::types::PoolMeta]>,
) -> FxHashMap<TokenIndex, U256> {
    match graph {
        Some(g) => hub_path_matic_rates_batch(arena, g, tokens, params, token_decimals, pool_metas),
        None => FxHashMap::default(),
    }
}

/// Oracle prefetch for hub tokens and tokens without a usable hub-path rate.
///
/// Long-tail tokens that already have a hub-path rate skip Hermes/Chainlink —
/// `build_token_to_matic_rates` prefers hub when oracle is absent, and CL
/// divergence override only matters for tokens we still fetch.
fn prefetch_addrs_for_oracle_fallback(
    arena: &StateArena,
    oracle: &PriceOracle,
    tokens: &[TokenIndex],
    hub_rates: &FxHashMap<TokenIndex, U256>,
) -> Vec<Address> {
    let mut addrs = Vec::new();
    for idx in tokens {
        let Some(addr) = arena.token_address(*idx) else {
            continue;
        };
        let has_hub = hub_rates
            .get(idx)
            .is_some_and(|r| *r >= MIN_TOKEN_TO_MATIC_RATE);
        // Hub tokens always prefetch (integer CL preferred). Long-tail with hub: skip.
        if has_hub && !crate::core::constants::is_polygon_hub_token(addr) {
            continue;
        }
        if oracle.has_configured_feed(&addr) || !has_hub {
            addrs.push(addr);
        }
    }
    addrs.extend(POLYGON_HUB_TOKENS);
    addrs.sort_unstable();
    addrs.dedup();
    addrs
}

fn dedupe_token_indices<I>(tokens: I) -> Vec<TokenIndex>
where
    I: IntoIterator<Item = TokenIndex>,
{
    let mut seen = FxHashSet::default();
    tokens.into_iter().filter(|t| seen.insert(*t)).collect()
}

#[cfg(test)]
/// Cycle tokens plus static hub feeds — one prefetch warms profit conversion
/// for spoke tokens even when they are not on the current cycle list.
fn prefetch_addrs_for_rates(arena: &StateArena, tokens: &[TokenIndex]) -> Vec<Address> {
    let mut addrs: Vec<Address> = tokens
        .iter()
        .filter_map(|idx| arena.token_address(*idx))
        .collect();
    addrs.extend(POLYGON_HUB_TOKENS);
    addrs.sort_unstable();
    addrs.dedup();
    addrs
}

/// Wrapped native is 1:1 with MATIC for routing when USD feeds exist (integer or float).
fn insert_missing_wmatic_self_rates(
    oracle: &PriceOracle,
    arena: &StateArena,
    matic_usd: f64,
    out: &mut FxHashMap<TokenIndex, U256>,
) {
    let wmatic = WMATIC;
    let rate_one = RATE_PRECISION;
    for i in 0..arena.token_count() {
        let idx = TokenIndex(i);
        if out.contains_key(&idx) {
            continue;
        }
        let Some(addr) = arena.token_address(idx) else {
            continue;
        };
        if addr != wmatic {
            continue;
        }
        let rate = oracle
            .token_matic_rate_per_unit_integer(&wmatic)
            .or_else(|| {
                if matic_usd > 0.0 {
                    oracle
                        .fresh_token_usd(&wmatic)
                        .map(|usd| token_usd_to_matic_rate_per_unit(usd, matic_usd))
                } else {
                    None
                }
            })
            .filter(|r| *r >= MIN_TOKEN_TO_MATIC_RATE)
            .unwrap_or({
                if matic_usd > 0.0 {
                    rate_one
                } else {
                    U256::ZERO
                }
            });
        if rate >= MIN_TOKEN_TO_MATIC_RATE {
            out.insert(idx, rate);
        }
    }
}

/// Relative divergence in bps: `|a - b| * 10_000 / max(a, b)`.
#[must_use]
pub(crate) fn rates_diverge_bps(a: U256, b: U256) -> u64 {
    let (hi, lo) = if a >= b { (a, b) } else { (b, a) };
    if hi.is_zero() {
        return 0;
    }
    let delta = hi - lo;
    u64::try_from((delta * U256::from(10_000u64) / hi).min(U256::from(10_000u64))).unwrap_or(10_000)
}

fn build_token_to_matic_rates(
    oracle: &PriceOracle,
    arena: &StateArena,
    tokens: &[TokenIndex],
    matic_usd: f64,
    hub_rates: &FxHashMap<TokenIndex, U256>,
    lst_rates: &FxHashMap<TokenIndex, U256>,
) -> (FxHashMap<TokenIndex, U256>, RateEnrichStats) {
    let wmatic = WMATIC;
    let rate_one = crate::core::constants::RATE_PRECISION;
    let mut stats = RateEnrichStats {
        requested: tokens.len(),
        ..RateEnrichStats::default()
    };
    let mut addrs: Vec<Address> = Vec::with_capacity(tokens.len());
    for idx in tokens {
        if let Some(addr) = arena.token_address(*idx) {
            addrs.push(addr);
        }
    }
    let integer_by_addr = oracle.integer_matic_rates_batch(&addrs);
    let mut out = FxHashMap::with_capacity_and_hasher(tokens.len(), FxBuildHasher);
    for idx in tokens {
        let Some(addr) = arena.token_address(*idx) else {
            stats.unresolved += 1;
            continue;
        };
        if addr == wmatic {
            stats.resolved += 1;
            out.insert(*idx, rate_one);
            continue;
        }
        let hub = hub_rates
            .get(idx)
            .copied()
            .filter(|r| *r >= MIN_TOKEN_TO_MATIC_RATE);
        let lst = lst_rates
            .get(idx)
            .copied()
            .filter(|r| *r >= MIN_TOKEN_TO_MATIC_RATE);
        let chainlink = integer_by_addr
            .get(&addr)
            .copied()
            .filter(|r| *r >= MIN_TOKEN_TO_MATIC_RATE);
        // Priority: CL/Pyth integer > LST exchange rate > hub path > float.
        // LSTs must not use DEX spot as the gas/profit base price.
        if let Some(cl) = chainlink {
            if let Some(hub_rate) = hub
                && !crate::core::constants::is_polygon_hub_token(addr)
                && lst.is_none()
                && rates_diverge_bps(cl, hub_rate) > 2_000
            {
                // Long-tail: executable pool basis when Chainlink diverges >20%.
                stats.hub_path += 1;
                stats.resolved += 1;
                out.insert(*idx, hub_rate);
                continue;
            }
            if oracle.is_pyth_sourced(&addr) {
                stats.pyth_integer += 1;
            } else {
                stats.chainlink += 1;
            }
            stats.resolved += 1;
            out.insert(*idx, cl);
            continue;
        }
        if let Some(lst_rate) = lst {
            stats.lst += 1;
            stats.resolved += 1;
            out.insert(*idx, lst_rate);
            continue;
        }
        if let Some(hub_rate) = hub {
            stats.hub_path += 1;
            stats.resolved += 1;
            out.insert(*idx, hub_rate);
            continue;
        }
        let float_oracle_rate = oracle.has_configured_feed(&addr).then(|| {
            if matic_usd > 0.0 {
                oracle
                    .fresh_token_usd(&addr)
                    .map(|usd| token_usd_to_matic_rate_per_unit(usd, matic_usd))
                    .filter(|r| *r >= MIN_TOKEN_TO_MATIC_RATE)
            } else {
                None
            }
        });
        if let Some(Some(rate)) = float_oracle_rate {
            stats.pyth_or_float += 1;
            stats.resolved += 1;
            out.insert(*idx, rate);
        } else {
            stats.unresolved += 1;
        }
    }
    insert_missing_wmatic_self_rates(oracle, arena, matic_usd, &mut out);
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
    if stats.requested > 0 && (stats.unresolved > 0 || stats.requested > 32) {
        crate::info!(
            "token rates enrich: requested={} resolved={} chainlink={} pyth_integer={} float={} lst={} hub_path={} unresolved={}",
            stats.requested,
            stats.resolved,
            stats.chainlink,
            stats.pyth_integer,
            stats.pyth_or_float,
            stats.lst,
            stats.hub_path,
            stats.unresolved
        );
    } else if stats.requested > 0 {
        crate::debug!(
            "token rates enrich: requested={} resolved={} chainlink={} pyth_integer={} float={} lst={} hub_path={} unresolved={}",
            stats.requested,
            stats.resolved,
            stats.chainlink,
            stats.pyth_integer,
            stats.pyth_or_float,
            stats.lst,
            stats.hub_path,
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
    use alloy::primitives::address;

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
    fn resolve_token_decimals_prefers_hints_over_arena_default() {
        let mut arena = StateArena::default();
        let token = Address::with_last_byte(7);
        let idx = arena.register_token(token);
        let mut hints = FxHashMap::default();
        hints.insert(token, 6);
        assert_eq!(resolve_token_decimals_for_index(idx, &arena, &hints), 6);
    }

    #[test]
    fn known_token_decimals_rejects_unbounded_metadata() {
        let mut hints = FxHashMap::default();
        let token = Address::with_last_byte(8);
        hints.insert(token, 31);
        assert!(known_token_decimals(token, &hints).is_none());
        hints.insert(token, 6);
        assert_eq!(known_token_decimals(token, &hints), Some(6));
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
        let refreshed = FxHashSet::from_iter([TokenIndex(0)]);
        let merged = merge_token_rates(
            &prior,
            &refreshed,
            FxHashMap::from_iter([(TokenIndex(0), U256::from(1_000u64))]),
            true,
        );
        assert!(Arc::ptr_eq(&prior, &merged));
        let changed = merge_token_rates(
            &prior,
            &refreshed,
            FxHashMap::from_iter([(TokenIndex(0), U256::from(9_000u64))]),
            true,
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
    fn merge_token_rates_drops_prior_when_refresh_unresolved() {
        let prior = Arc::new(FxHashMap::from_iter([
            (TokenIndex(0), U256::from(1_000u64)),
            (TokenIndex(1), U256::from(2_000u64)),
        ]));
        let refreshed = FxHashSet::from_iter([TokenIndex(0)]);

        let merged = merge_token_rates(&prior, &refreshed, FxHashMap::default(), true);

        assert!(merged.get(&TokenIndex(0)).is_none());
        assert_eq!(
            merged.get(&TokenIndex(1)).copied(),
            Some(U256::from(2_000u64))
        );
    }

    #[test]
    fn merge_token_rates_keeps_non_refreshed_when_snapshot_aged() {
        // retain_stale_prior=false used to wipe spoke rates not in this tick's
        // enrich set — that killed hub-path spokes when LF spacing > quote TTL.
        let prior = Arc::new(FxHashMap::from_iter([(
            TokenIndex(1),
            U256::from(2_000u64),
        )]));
        let refreshed = FxHashSet::from_iter([TokenIndex(0)]);
        let merged = merge_token_rates(
            &prior,
            &refreshed,
            FxHashMap::from_iter([(TokenIndex(0), U256::from(1_000u64))]),
            false,
        );
        assert_eq!(
            merged.get(&TokenIndex(0)).copied(),
            Some(U256::from(1_000u64))
        );
        assert_eq!(
            merged.get(&TokenIndex(1)).copied(),
            Some(U256::from(2_000u64))
        );
    }

    #[test]
    fn rates_diverge_bps_is_relative_to_larger() {
        assert_eq!(
            rates_diverge_bps(U256::from(100u64), U256::from(80u64)),
            2_000
        );
        assert_eq!(rates_diverge_bps(U256::from(100u64), U256::from(100u64)), 0);
    }

    #[test]
    fn prefetch_addrs_for_rates_includes_hubs() {
        let mut arena = StateArena::default();
        let usdc = address!("0x2791bca1f2de4661ed88a30c99a7a9489c09eb3f");
        let usdc_idx = arena.register_token(usdc);
        let addrs = prefetch_addrs_for_rates(&arena, &[usdc_idx]);
        assert!(addrs.contains(&usdc));
        assert!(addrs.contains(&POLYGON_HUB_TOKENS[0]));
        assert!(addrs.len() >= POLYGON_HUB_TOKENS.len());
    }

    #[test]
    fn insert_missing_wmatic_self_rates_when_matic_usd_known() {
        use super::price_oracle::PriceOracle;

        let mut arena = StateArena::default();
        let wmatic_idx = arena.register_token(WMATIC);
        let oracle = PriceOracle::new(
            reqwest::Client::new(),
            "https://hermes.pyth.network".to_string(),
            10_000,
        );
        let mut out = FxHashMap::default();
        insert_missing_wmatic_self_rates(&oracle, &arena, 0.5, &mut out);
        assert_eq!(out.get(&wmatic_idx).copied(), Some(RATE_PRECISION));
    }

    #[test]
    fn lst_exchange_rate_wins_over_hub_path() {
        use super::price_oracle::PriceOracle;

        let mut arena = StateArena::default();
        let token: Address = "0x3A58a54C066FdC0f2D55FC9C89F0415C92eBf3C4"
            .parse()
            .expect("stmatic");
        let idx = arena.register_token(token);
        let oracle = PriceOracle::new(
            reqwest::Client::new(),
            "https://hermes.pyth.network".to_string(),
            10_000,
        );
        let hub_rate = RATE_PRECISION * U256::from(2u64);
        let lst_rate = RATE_PRECISION + U256::from(1u64);
        let hub_rates = FxHashMap::from_iter([(idx, hub_rate)]);
        let lst_rates = FxHashMap::from_iter([(idx, lst_rate)]);
        let (out, stats) =
            build_token_to_matic_rates(&oracle, &arena, &[idx], 0.5, &hub_rates, &lst_rates);
        assert_eq!(out.get(&idx).copied(), Some(lst_rate));
        assert_eq!(stats.lst, 1);
        assert_eq!(stats.hub_path, 0);
    }

    #[test]
    fn hub_path_rate_wins_over_configured_float_oracle_leverage() {
        use super::price_oracle::PriceOracle;

        let mut arena = StateArena::default();
        let token: Address = "0x45c32fA6DF82ead1e2EF74d32b0366496F5fDe09"
            .parse()
            .expect("frax");
        let idx = arena.register_token(token);
        let oracle = PriceOracle::new(
            reqwest::Client::new(),
            "https://hermes.pyth.network".to_string(),
            10_000,
        );
        oracle.seed_float_usd_for_test(token, 1.0);

        let hub_rate = RATE_PRECISION * U256::from(3u64);
        let hub_rates = FxHashMap::from_iter([(idx, hub_rate)]);
        let (out, stats) = build_token_to_matic_rates(
            &oracle,
            &arena,
            &[idx],
            0.5,
            &hub_rates,
            &FxHashMap::default(),
        );

        assert_eq!(out.get(&idx).copied(), Some(hub_rate));
        assert_eq!(stats.hub_path, 1);
        assert_eq!(stats.pyth_or_float, 0);
    }

    #[test]
    fn unconfigured_token_skips_generic_oracle_leverage_without_hub_path() {
        use super::price_oracle::PriceOracle;

        let mut arena = StateArena::default();
        let token = Address::from([0x77u8; 20]);
        let idx = arena.register_token(token);
        let oracle = PriceOracle::new(
            reqwest::Client::new(),
            "https://hermes.pyth.network".to_string(),
            10_000,
        );
        oracle.seed_float_usd_for_test(token, 2.0);

        let (out, stats) = build_token_to_matic_rates(
            &oracle,
            &arena,
            &[idx],
            0.5,
            &FxHashMap::default(),
            &FxHashMap::default(),
        );

        assert!(!out.contains_key(&idx));
        assert_eq!(stats.unresolved, 1);
        assert_eq!(stats.pyth_or_float, 0);
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
