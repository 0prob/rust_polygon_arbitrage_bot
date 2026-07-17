use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use alloy::primitives::Address;
use rustc_hash::FxHashMap;

use crate::core::types::{Edge, PoolIndex, PoolState, ProtocolType, TokenIndex};
use crate::services::discovery::{DiscoveredPool, discovered_to_pool_meta};
use crate::services::state_cache::StateCache;
use rustc_hash::FxHasher;
use std::hash::Hasher;

/// Discovery protocol rewritten to match fetched arena state family.
static META_PROTOCOL_CORRECTED: AtomicU32 = AtomicU32::new(0);

/// How many pool metas had discovery protocol rewritten from arena state.
#[must_use]
pub fn meta_protocol_corrected_count() -> u32 {
    META_PROTOCOL_CORRECTED.load(Ordering::Relaxed)
}

#[derive(Debug, Default, Clone)]
struct ArenaInner {
    tokens: Vec<Address>,
    /// Parallel to `tokens` — O(1) decimals lookup by `TokenIndex`.
    token_decimals: Vec<u8>,
    pools: Vec<Arc<PoolState>>,
    pool_addresses: Vec<Address>,
    address_to_pool: FxHashMap<Address, PoolIndex>,
    address_to_token: FxHashMap<Address, TokenIndex>,
    /// Cached layout fingerprint — refreshed on register, not on every LF read.
    layout_fingerprint: u64,
}

fn sync_pool_graph_eligible(
    state: &PoolState,
    pool: &DiscoveredPool,
    decimal_hints: Option<&FxHashMap<Address, u8>>,
) -> bool {
    let bpt_hint = match state {
        PoolState::Balancer(b) => b.bpt_index,
        _ => None,
    };
    let token_count = match state {
        PoolState::Balancer(b) if !b.tokens.is_empty() => b.tokens.len(),
        PoolState::Woofi(w) if !w.tokens.is_empty() => w.tokens.len(),
        _ => pool.tokens.len(),
    };
    // Missing decimal hints used to fail closed (`known_token_decimals?` → None)
    // and permanently exclude live Uni V3 venues from the arena/universe. Fall
    // back to 18 like the no-hints path; still reject unbounded metadata.
    let pair_input_decimals = pool.tokens.first().zip(pool.tokens.get(1)).and_then(
        |(a0, a1)| match decimal_hints {
            Some(h) => {
                let d0 = h.get(a0).copied().unwrap_or(18);
                let d1 = h.get(a1).copied().unwrap_or(18);
                let max = crate::core::constants::MAX_SUPPORTED_TOKEN_DECIMALS;
                if d0 > max || d1 > max {
                    None
                } else {
                    Some((d0, d1))
                }
            }
            None => Some((18, 18)),
        },
    );
    crate::pipeline::graph::pool_state_graph_eligible(
        None,
        state,
        pool.protocol,
        token_count,
        bpt_hint,
        pool.fee_bps,
        pair_input_decimals,
    )
}

/// How much of the current arena layout is a stable prefix of the new tradable set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArenaReuse {
    /// Full membership match — update Arcs only.
    Exact,
    /// First `prefix_len` pools match order+eligibility; append the rest.
    Prefix { prefix_len: usize },
    /// Must wipe and rebuild (reorder, shrink, or ineligible prefix entry).
    Rebuild,
}

/// Fast-path arena refresh when tradable membership, order, and eligibility are unchanged
/// or only extended by higher discovery-index pools (append-only growth).
fn arena_reuse_for_tradable(
    inner: &ArenaInner,
    tradable: &[(usize, Address, Arc<PoolState>)],
    pools: &[DiscoveredPool],
    decimal_hints: Option<&FxHashMap<Address, u8>>,
) -> ArenaReuse {
    let existing = inner.pools.len();
    if existing == 0 {
        return if tradable.is_empty() {
            ArenaReuse::Exact
        } else {
            ArenaReuse::Rebuild
        };
    }
    if tradable.len() < existing {
        return ArenaReuse::Rebuild;
    }
    for (i, (idx, address, state)) in tradable.iter().take(existing).enumerate() {
        if inner.pool_addresses.get(i) != Some(address) {
            return ArenaReuse::Rebuild;
        }
        let Some(pool) = pools.get(*idx) else {
            return ArenaReuse::Rebuild;
        };
        if !sync_pool_graph_eligible(state.as_ref(), pool, decimal_hints) {
            return ArenaReuse::Rebuild;
        }
        if !inner.address_to_pool.contains_key(address) {
            return ArenaReuse::Rebuild;
        }
    }
    if tradable.len() == existing {
        ArenaReuse::Exact
    } else {
        ArenaReuse::Prefix {
            prefix_len: existing,
        }
    }
}

fn compute_layout_fingerprint(inner: &ArenaInner) -> u64 {
    let mut h = FxHasher::default();
    h.write_u32(inner.tokens.len() as u32);
    for addr in &inner.tokens {
        h.write(addr.as_slice());
    }
    h.write_usize(inner.pools.len());
    for addr in &inner.pool_addresses {
        h.write(addr.as_slice());
    }
    h.finish()
}

/// Contiguous memory store for tokens and pool states.
///
/// Heavy vectors live behind `Arc` so HF ticks can clone cheaply and overlay hot
/// pool states from cache without copying the full arena.
#[derive(Debug)]
pub struct StateArena {
    inner: Arc<ArenaInner>,
    /// Per-pool hot overlay indexed by `PoolIndex.0` (O(1) lookup on HF path).
    hot_overlay: Vec<Option<Arc<PoolState>>>,
    hot_revisions: Vec<u64>,
}

impl Default for StateArena {
    fn default() -> Self {
        let mut inner = ArenaInner::default();
        inner.layout_fingerprint = compute_layout_fingerprint(&inner);
        Self {
            inner: Arc::new(inner),
            hot_overlay: Vec::new(),
            hot_revisions: Vec::new(),
        }
    }
}

impl Clone for StateArena {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            hot_overlay: Vec::new(),
            hot_revisions: Vec::new(),
        }
    }
}

impl StateArena {
    #[must_use]
    pub fn pool_count(&self) -> usize {
        self.inner.pools.len()
    }

    /// Fingerprint of token/pool address layout keyed by arena index.
    ///
    /// Graph-cache invalidation uses this so a stable pool count with a changed
    /// membership order cannot reuse stale edges that point at the wrong pools.
    #[must_use]
    pub fn routing_layout_fingerprint(&self) -> u64 {
        self.inner.layout_fingerprint
    }

    #[must_use]
    pub fn token_count(&self) -> u32 {
        self.inner.tokens.len() as u32
    }

    #[must_use]
    pub fn address_to_pool(&self) -> &FxHashMap<Address, PoolIndex> {
        &self.inner.address_to_pool
    }

    #[inline]
    #[must_use]
    pub fn token_address(&self, index: TokenIndex) -> Option<Address> {
        self.inner.tokens.get(index.0 as usize).copied()
    }

    #[inline]
    #[must_use]
    pub fn token_decimals(&self, index: TokenIndex) -> u8 {
        self.inner
            .token_decimals
            .get(index.0 as usize)
            .copied()
            .unwrap_or(18)
    }

    #[must_use]
    pub fn address_to_token(&self) -> &FxHashMap<Address, TokenIndex> {
        &self.inner.address_to_token
    }

    #[inline]
    pub fn pool_state(&self, index: PoolIndex) -> Option<&PoolState> {
        let idx = index.0 as usize;
        if let Some(state) = self.hot_overlay.get(idx).and_then(Option::as_ref) {
            return Some(state.as_ref());
        }
        self.inner.pools.get(idx).map(std::convert::AsRef::as_ref)
    }

    #[must_use]
    pub fn route_state_revision(&self, edges: &[Edge]) -> Option<u64> {
        let mut h = FxHasher::default();
        h.write_usize(edges.len());
        for edge in edges {
            let index = edge.pool_index.0 as usize;
            self.hot_overlay.get(index)?.as_ref()?;
            let revision = *self.hot_revisions.get(index)?;
            if revision == 0 {
                return None;
            }
            h.write_u32(edge.pool_index.0);
            h.write_u64(revision);
        }
        Some(h.finish())
    }

    pub fn pool_state_mut(&mut self, index: PoolIndex) -> Option<&mut PoolState> {
        let idx = index.0 as usize;
        let inner = Arc::make_mut(&mut self.inner);
        if let Some(overlay) = self.hot_overlay.get_mut(idx)
            && let Some(hot) = overlay.take()
            && let Some(slot) = inner.pools.get_mut(idx)
        {
            *slot = hot;
        }
        if let Some(slot) = inner.pools.get_mut(idx) {
            Some(Arc::make_mut(slot))
        } else {
            None
        }
    }

    pub fn register_token(&mut self, address: Address) -> TokenIndex {
        self.register_token_with_hints(address, None)
    }

    fn register_token_with_hints(
        &mut self,
        address: Address,
        hints: Option<&FxHashMap<Address, u8>>,
    ) -> TokenIndex {
        self.register_token_with_hints_inner(address, hints, true)
    }

    /// `touch_layout`: recompute fingerprint on new token inserts. Bulk sync
    /// defers this to one final pass (avoids O(n²) hashing during rebuild).
    fn register_token_with_hints_inner(
        &mut self,
        address: Address,
        hints: Option<&FxHashMap<Address, u8>>,
        touch_layout: bool,
    ) -> TokenIndex {
        if let Some(&idx) = self.inner.address_to_token.get(&address) {
            if let Some(hints) = hints
                && let Some(&dec) = hints.get(&address)
            {
                // Skip COW+write when the decimal is already correct.
                let needs_update = self
                    .inner
                    .token_decimals
                    .get(idx.0 as usize)
                    .is_some_and(|slot| *slot != dec);
                if needs_update {
                    let inner = Arc::make_mut(&mut self.inner);
                    if let Some(slot) = inner.token_decimals.get_mut(idx.0 as usize) {
                        *slot = dec;
                    }
                }
            }
            return idx;
        }
        let inner = Arc::make_mut(&mut self.inner);
        let idx = TokenIndex(inner.tokens.len() as u32);
        let dec = hints.and_then(|m| m.get(&address)).copied().unwrap_or(18);
        inner.tokens.push(address);
        inner.token_decimals.push(dec);
        inner.address_to_token.insert(address, idx);
        if touch_layout {
            inner.layout_fingerprint = compute_layout_fingerprint(inner);
        }
        idx
    }

    #[inline]
    #[must_use]
    pub fn pool_address(&self, index: PoolIndex) -> Option<Address> {
        self.inner.pool_addresses.get(index.0 as usize).copied()
    }

    fn clear_hot_slot(&mut self, index: PoolIndex) {
        if let Some(slot) = self.hot_overlay.get_mut(index.0 as usize) {
            *slot = None;
        }
        if let Some(revision) = self.hot_revisions.get_mut(index.0 as usize) {
            *revision = 0;
        }
    }

    pub fn register_pool(&mut self, address: Address, state: Arc<PoolState>) -> PoolIndex {
        self.register_pool_inner(address, state, true)
    }

    fn register_pool_inner(
        &mut self,
        address: Address,
        state: Arc<PoolState>,
        touch_layout: bool,
    ) -> PoolIndex {
        if let Some(&idx) = self.inner.address_to_pool.get(&address) {
            // Skip COW when the Arc pointer is already the same (common on stable
            // LF ticks that re-sync unchanged cache entries).
            let same = self
                .inner
                .pools
                .get(idx.0 as usize)
                .is_some_and(|slot| Arc::ptr_eq(slot, &state));
            if !same {
                let inner = Arc::make_mut(&mut self.inner);
                if let Some(slot) = inner.pools.get_mut(idx.0 as usize) {
                    *slot = state;
                }
            }
            self.clear_hot_slot(idx);
            return idx;
        }
        let inner = Arc::make_mut(&mut self.inner);
        let idx = PoolIndex(inner.pools.len() as u32);
        inner.pools.push(state);
        inner.pool_addresses.push(address);
        inner.address_to_pool.insert(address, idx);
        if touch_layout {
            inner.layout_fingerprint = compute_layout_fingerprint(inner);
        }
        idx
    }

    fn recompute_layout_fingerprint(&mut self) {
        let inner = Arc::make_mut(&mut self.inner);
        inner.layout_fingerprint = compute_layout_fingerprint(inner);
    }

    fn ensure_hot_storage(&mut self) {
        let pool_count = self.inner.pools.len();
        if self.hot_overlay.len() != pool_count {
            self.hot_overlay.resize(pool_count, None);
            self.hot_revisions.resize(pool_count, 0);
        }
    }

    /// Overlay fresh pool states from cache (HF hot-path; Arc clone only).
    pub fn apply_hot_cache(&mut self, cache: &StateCache, addresses: &[Address]) -> u64 {
        if addresses.is_empty() {
            return cache.generation();
        }
        self.ensure_hot_storage();
        let mut unique: Vec<Address> = addresses.to_vec();
        if unique.len() > 1 {
            unique.sort_unstable();
            unique.dedup();
        }
        let (states, generation) = cache.get_arcs_with_generation(&unique);
        let mut fresh: FxHashMap<Address, (Arc<PoolState>, u64)> =
            FxHashMap::with_capacity_and_hasher(states.len(), Default::default());
        for (address, state, revision) in states {
            fresh.insert(address, (state, revision));
        }
        for address in unique {
            let Some(&idx) = self.inner.address_to_pool.get(&address) else {
                continue;
            };
            if let Some((state, revision)) = fresh.get(&address) {
                let slot = idx.0 as usize;
                // Cache multicall states are tickless; keep LF/dispatch tick arrays when
                // the price level is unchanged so HF probe does not classify every CL
                // hop as shallow_cl after apply_hot_cache.
                let prior = self
                    .hot_overlay
                    .get(slot)
                    .and_then(|overlay| overlay.as_deref())
                    .or_else(|| self.inner.pools.get(slot).map(std::convert::AsRef::as_ref));
                // Reject protocol-family swaps (live: V2 edges + V3 overlay → UnsupportedState).
                // Keep the prior family until a matching refresh arrives.
                if let Some(prior_state) = prior
                    && !pool_state_family_compatible(prior_state, state.as_ref())
                {
                    continue;
                }
                let merged = preserve_cl_ticks_on_replace(prior, Arc::clone(state));
                if let Some(overlay) = self.hot_overlay.get_mut(slot) {
                    *overlay = Some(merged);
                }
                if let Some(hot_revision) = self.hot_revisions.get_mut(slot) {
                    *hot_revision = *revision;
                }
            } else {
                self.clear_hot_slot(idx);
            }
        }
        generation
    }

    /// Register tradable pools only — walks the cache tradable set (~19k) instead
    /// of scanning the full discovery list (~263k) and taking one read lock per pool.
    pub fn sync_from_discovery(
        &mut self,
        cache: &StateCache,
        pools: &[DiscoveredPool],
        address_index: &FxHashMap<Address, usize>,
        decimal_hints: Option<&FxHashMap<Address, u8>>,
    ) -> Vec<crate::pipeline::types::PoolMeta> {
        let tradable = cache.tradable_by_discovery_index(address_index);

        match arena_reuse_for_tradable(&self.inner, &tradable, pools, decimal_hints) {
            ArenaReuse::Exact => {
                return self.sync_tradable_inplace(&tradable, pools, decimal_hints);
            }
            ArenaReuse::Prefix { prefix_len } => {
                return self.sync_tradable_append(&tradable, pools, decimal_hints, prefix_len);
            }
            ArenaReuse::Rebuild => {}
        }

        // LF clones the arena Arc before sync. Using `Arc::make_mut` here would
        // deep-clone the previous multi-thousand-pool layout just to discard it.
        // Replace the Arc so rebuild starts from an empty uniquely-owned inner.
        let expected_tokens = tradable
            .iter()
            .map(|(idx, _, _)| pools.get(*idx).map_or(2, |pool| pool.tokens.len()))
            .sum::<usize>()
            .max(1);
        let mut fresh = ArenaInner::default();
        fresh.tokens.reserve(expected_tokens);
        fresh.token_decimals.reserve(expected_tokens);
        fresh.pools.reserve(tradable.len());
        fresh.pool_addresses.reserve(tradable.len());
        fresh.address_to_pool.reserve(tradable.len());
        fresh.address_to_token.reserve(expected_tokens);
        self.inner = Arc::new(fresh);
        self.hot_overlay.clear();
        self.hot_revisions.clear();

        let mut metas = Vec::with_capacity(tradable.len().max(1));
        for (idx, _address, state) in tradable {
            let Some(pool) = pools.get(idx) else {
                continue;
            };
            // On rebuild we still filter — some tradable cache entries fail
            // graph-eligibility (e.g. missing pair decimals).
            if !sync_pool_graph_eligible(state.as_ref(), pool, decimal_hints) {
                continue;
            }
            metas.push(self.append_synced_pool(
                pool,
                state,
                decimal_hints,
                /*touch_layout=*/ false,
            ));
        }
        // One layout fingerprint for the full bulk rebuild (not per insert).
        self.recompute_layout_fingerprint();
        metas
    }

    /// Prefix-stable growth: refresh existing pools in place, append new ones.
    /// Preserves `PoolIndex` for the shared prefix so graph edges stay valid.
    fn sync_tradable_append(
        &mut self,
        tradable: &[(usize, Address, Arc<PoolState>)],
        pools: &[DiscoveredPool],
        decimal_hints: Option<&FxHashMap<Address, u8>>,
        prefix_len: usize,
    ) -> Vec<crate::pipeline::types::PoolMeta> {
        debug_assert!(prefix_len <= tradable.len());
        debug_assert_eq!(prefix_len, self.inner.pools.len());

        // Refresh prefix (same as exact reuse).
        let mut metas = self.sync_tradable_inplace(&tradable[..prefix_len], pools, decimal_hints);
        metas.reserve(tradable.len().saturating_sub(prefix_len));

        // Append only the newly tradable tail without rehashing the prefix.
        let extra_tokens: usize = tradable[prefix_len..]
            .iter()
            .map(|(idx, _, _)| pools.get(*idx).map_or(2, |pool| pool.tokens.len()))
            .sum();
        {
            let inner = Arc::make_mut(&mut self.inner);
            let grow = tradable.len() - prefix_len;
            inner.pools.reserve(grow);
            inner.pool_addresses.reserve(grow);
            inner.address_to_pool.reserve(grow);
            inner.tokens.reserve(extra_tokens);
            inner.token_decimals.reserve(extra_tokens);
            inner.address_to_token.reserve(extra_tokens);
        }

        for (idx, _address, state) in &tradable[prefix_len..] {
            let Some(pool) = pools.get(*idx) else {
                continue;
            };
            if !sync_pool_graph_eligible(state.as_ref(), pool, decimal_hints) {
                continue;
            }
            metas.push(self.append_synced_pool(
                pool,
                Arc::clone(state),
                decimal_hints,
                /*touch_layout=*/ false,
            ));
        }
        self.recompute_layout_fingerprint();
        metas
    }

    /// Layout-stable fast path: refresh pool Arcs + metas without rehashing tokens.
    fn sync_tradable_inplace(
        &mut self,
        tradable: &[(usize, Address, Arc<PoolState>)],
        pools: &[DiscoveredPool],
        decimal_hints: Option<&FxHashMap<Address, u8>>,
    ) -> Vec<crate::pipeline::types::PoolMeta> {
        let mut metas = Vec::with_capacity(tradable.len().max(1));
        for (i, (idx, _address, state)) in tradable.iter().enumerate() {
            let Some(pool) = pools.get(*idx) else {
                continue;
            };
            // Reusable check already proved eligibility; only refresh state + meta.
            let pool_index = PoolIndex(i as u32);
            let same = self
                .inner
                .pools
                .get(i)
                .is_some_and(|slot| Arc::ptr_eq(slot, state));
            if !same {
                let prior = self.inner.pools.get(i).cloned();
                let inner = Arc::make_mut(&mut self.inner);
                if let Some(slot) = inner.pools.get_mut(i) {
                    // Cache states rarely carry CL ticks (LF hydrates them on the
                    // arena). Preserve ticks when the price level is unchanged so
                    // we skip a full TickLens multicall every LF pass.
                    *slot = preserve_cl_ticks_on_replace(prior.as_deref(), Arc::clone(state));
                }
            }
            self.clear_hot_slot(pool_index);

            // Tokens already registered; register only updates decimal hints if needed.
            let token_indices =
                self.token_indices_for_pool(state.as_ref(), pool, decimal_hints, false);
            metas.push(pool_meta_from_synced(
                pool,
                pool_index,
                &token_indices,
                state.as_ref(),
            ));
        }
        metas
    }

    fn append_synced_pool(
        &mut self,
        pool: &DiscoveredPool,
        state: Arc<PoolState>,
        decimal_hints: Option<&FxHashMap<Address, u8>>,
        touch_layout: bool,
    ) -> crate::pipeline::types::PoolMeta {
        let pool_index =
            self.register_pool_inner(pool.address, Arc::clone(&state), touch_layout);
        let token_indices =
            self.token_indices_for_pool(state.as_ref(), pool, decimal_hints, touch_layout);
        pool_meta_from_synced(pool, pool_index, &token_indices, state.as_ref())
    }

    fn token_indices_for_pool(
        &mut self,
        state: &PoolState,
        pool: &DiscoveredPool,
        decimal_hints: Option<&FxHashMap<Address, u8>>,
        touch_layout: bool,
    ) -> Vec<TokenIndex> {
        // ponytail: borrow token addresses instead of cloning Vec — most
        // pools use the discovery order, and state-hydrated tokens (Balancer,
        // Woofi) can be borrowed directly. Dodo always allocates: meta order
        // must be [base, quote] so token_in_idx 0 ⇔ sellBase matches sim,
        // capacity, and encode (indexer discovery order is not authoritative).
        let token_addrs: &[Address];
        let dodo_owned; // extends lifetime of the Dodo [base, quote] vec
        match state {
            PoolState::Balancer(b) if !b.tokens.is_empty() => {
                token_addrs = &b.tokens;
            }
            PoolState::Woofi(w) if !w.tokens.is_empty() => {
                token_addrs = &w.tokens;
            }
            PoolState::Dodo(d) if !d.base_token.is_zero() && !d.quote_token.is_zero() => {
                dodo_owned = vec![d.base_token, d.quote_token];
                token_addrs = &dodo_owned;
            }
            _ => token_addrs = &pool.tokens,
        }
        let mut token_indices = Vec::with_capacity(token_addrs.len());
        for &addr in token_addrs {
            token_indices.push(self.register_token_with_hints_inner(
                addr,
                decimal_hints,
                touch_layout,
            ));
        }
        token_indices
    }
}

fn pool_meta_from_synced(
    pool: &DiscoveredPool,
    pool_index: PoolIndex,
    token_indices: &[TokenIndex],
    state: &PoolState,
) -> crate::pipeline::types::PoolMeta {
    let mut meta = discovered_to_pool_meta(pool, pool_index, token_indices);
    // Discovery labels can disagree with the fetched state family (live: V2 meta
    // on V3 arena → graph/probe UnsupportedState). Prefer arena family; keep
    // `protocol_label` for Algebra/factory selection at execution time.
    if !crate::pipeline::local_sim::protocol_matches_pool_state(meta.protocol, state) {
        let corrected =
            crate::pipeline::local_sim::protocol_from_pool_state(state, meta.protocol);
        if corrected != meta.protocol {
            META_PROTOCOL_CORRECTED.fetch_add(1, Ordering::Relaxed);
            meta.protocol = corrected;
        }
    }
    if meta.protocol == ProtocolType::BalancerV2
        && let PoolState::Balancer(b) = state
    {
        // Use the hydrated Vault result for phantom-BPT exclusion.
        meta.bpt_index = b.bpt_index;
        if let Some(id) = b.pool_id {
            meta.pool_id = Some(id);
        }
    }
    meta
}

/// Same simulation family (V2/V3/V4/Curve/…) — blocks hot-cache kind swaps.
#[inline]
fn pool_state_family_compatible(prior: &PoolState, incoming: &PoolState) -> bool {
    matches!(
        (prior, incoming),
        (PoolState::Invalid, _)
            | (_, PoolState::Invalid)
            | (PoolState::V2(_), PoolState::V2(_))
            | (PoolState::V3(_), PoolState::V3(_))
            | (PoolState::V4(_), PoolState::V4(_))
            | (PoolState::Curve(_), PoolState::Curve(_))
            | (PoolState::Balancer(_), PoolState::Balancer(_))
            | (PoolState::Dodo(_), PoolState::Dodo(_))
            | (PoolState::Woofi(_), PoolState::Woofi(_))
    )
}

/// Keep LF-hydrated tick arrays when replacing a cache Arc that has no ticks.
fn preserve_cl_ticks_on_replace(
    prior: Option<&PoolState>,
    incoming: Arc<PoolState>,
) -> Arc<PoolState> {
    let Some(prior) = prior else {
        return incoming;
    };
    // Keep tick arrays when the concentrated price level is unchanged. Global
    // liquidity may move on refresh without invalidating the bitmap; a tick or
    // sqrt move fails closed (incoming tickless state wins until re-enrich).
    match (prior, incoming.as_ref()) {
        (PoolState::V3(old), PoolState::V3(new))
            if !old.ticks.is_empty()
                && new.ticks.is_empty()
                && old.tick == new.tick
                && old.sqrt_price_x96 == new.sqrt_price_x96 =>
        {
            let mut merged = new.clone();
            merged.ticks = Arc::clone(&old.ticks);
            Arc::new(PoolState::V3(merged))
        }
        (PoolState::V4(old), PoolState::V4(new))
            if !old.ticks.is_empty()
                && new.ticks.is_empty()
                && old.tick == new.tick
                && old.sqrt_price_x96 == new.sqrt_price_x96 =>
        {
            let mut merged = new.clone();
            merged.ticks = Arc::clone(&old.ticks);
            Arc::new(PoolState::V4(merged))
        }
        _ => incoming,
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;
    use crate::core::types::{
        CycleEdges, DodoPoolState, DodoRState, Edge, ProtocolType, TokenIndex, V2PoolState,
        WoofiBaseTokenState, WoofiPoolState,
    };
    use alloy::primitives::U256;

    const MIN_HOP_TOKEN_BALANCE: U256 = U256::from_limbs([1_000_000_000_000_000, 0, 0, 0]);

    fn v2_state() -> Arc<PoolState> {
        Arc::new(PoolState::V2(V2PoolState {
            reserve0: MIN_HOP_TOKEN_BALANCE,
            reserve1: MIN_HOP_TOKEN_BALANCE + U256::from(1u64),
            fee: U256::from(997u64),
            fee_denominator: U256::from(1_000u64),
            block_timestamp_last: 1,
        }))
    }

    #[test]
    fn routing_layout_fingerprint_changes_when_pool_membership_changes() {
        let addr_a = Address::from([1u8; 20]);
        let addr_b = Address::from([2u8; 20]);
        let mut arena = StateArena::default();
        let fp_empty = arena.routing_layout_fingerprint();
        let _ = arena.register_pool(addr_a, v2_state());
        let fp_a = arena.routing_layout_fingerprint();
        assert_ne!(fp_empty, fp_a);
        let _ = arena.register_pool(addr_b, v2_state());
        let fp_ab = arena.routing_layout_fingerprint();
        assert_ne!(fp_a, fp_ab);
    }

    #[test]
    fn routing_layout_fingerprint_tracks_full_address_bytes() {
        let mut arena = StateArena::default();
        let addr_a = Address::from([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ]);
        let addr_b = Address::from([
            9, 8, 7, 6, 5, 4, 3, 2, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ]);
        let fp_a = {
            let _ = arena.register_pool(addr_a, v2_state());
            arena.routing_layout_fingerprint()
        };
        let fp_b = {
            let mut arena = StateArena::default();
            let _ = arena.register_pool(addr_b, v2_state());
            arena.routing_layout_fingerprint()
        };
        assert_ne!(fp_a, fp_b);
    }

    #[test]
    fn hot_overlay_overrides_canonical_pool_state() {
        let addr = Address::from([9u8; 20]);
        let mut arena = StateArena::default();
        let idx = arena.register_pool(addr, v2_state());

        arena.hot_overlay.resize(1, None);
        arena.hot_overlay[0] = Some(Arc::new(PoolState::V2(V2PoolState {
            reserve0: U256::from(9_999u64),
            reserve1: U256::from(2_000u64),
            fee: U256::from(997u64),
            fee_denominator: U256::from(1_000u64),
            block_timestamp_last: 1,
        })));

        let state = arena.pool_state(idx).expect("pool state");
        let PoolState::V2(s) = state else {
            panic!("expected v2");
        };
        assert_eq!(s.reserve0, U256::from(9_999u64));
    }

    #[test]
    fn woofi_uses_hydrated_token_order() {
        let pool_address = Address::with_last_byte(10);
        let base = Address::with_last_byte(1);
        let quote = Address::with_last_byte(2);
        let stale_discovery_token = Address::with_last_byte(3);
        let cache = StateCache::default();
        cache.insert(
            pool_address,
            PoolState::Woofi(WoofiPoolState {
                tokens: vec![base, quote],
                quote_reserve: MIN_HOP_TOKEN_BALANCE,
                base_states: vec![WoofiBaseTokenState {
                    price: U256::from(1u8),
                    spread: U256::ZERO,
                    coeff: U256::ZERO,
                    reserve: MIN_HOP_TOKEN_BALANCE,
                    base_dec: U256::from(1u8),
                    quote_dec: U256::from(1u8),
                    price_dec: U256::from(1u8),
                    fee_rate: U256::ZERO,
                    max_gamma: U256::ZERO,
                    max_notional_swap: U256::ZERO,
                }],
                fee: U256::ZERO,
            }),
        );
        let discovered = [DiscoveredPool {
            pool_key: pool_address.to_string(),
            address: pool_address,
            protocol: ProtocolType::Woofi,
            protocol_label: "WOOFI".into(),
            tokens: vec![quote, stale_discovery_token, base],
            fee_bps: 0,
            tick_spacing: None,
            pool_id: None,
            pool_id_verified: false,
            hooks: None,
            pool_type: None,
            created_block: 1,
        }];

        let address_index = discovered
            .iter()
            .enumerate()
            .map(|(idx, pool)| (pool.address, idx))
            .collect();
        let mut arena = StateArena::default();
        let metas = arena.sync_from_discovery(&cache, &discovered, &address_index, None);
        assert_eq!(metas.len(), 1);
        let addresses: Vec<_> = metas[0]
            .tokens
            .iter()
            .filter_map(|index| arena.token_address(*index))
            .collect();
        assert_eq!(addresses, vec![base, quote]);
    }

    #[test]
    fn dodo_meta_is_base_quote_even_when_discovery_order_is_reversed() {
        let pool_address = Address::with_last_byte(11);
        let base = Address::with_last_byte(0x0b);
        let quote = Address::with_last_byte(0x0a); // address-sorted quote < base
        let cache = StateCache::default();
        cache.insert(
            pool_address,
            PoolState::Dodo(DodoPoolState {
                base_reserve: MIN_HOP_TOKEN_BALANCE,
                quote_reserve: MIN_HOP_TOKEN_BALANCE,
                base_token: base,
                quote_token: quote,
                base_target: MIN_HOP_TOKEN_BALANCE,
                quote_target: MIN_HOP_TOKEN_BALANCE,
                r_state: DodoRState::One,
                i: U256::from(1u64) << 18,
                k: U256::ZERO,
                lp_fee_rate: U256::ZERO,
                mt_fee_rate: U256::ZERO,
            }),
        );
        // Indexer listed quote before base — must not become meta order.
        let discovered = [DiscoveredPool {
            pool_key: pool_address.to_string(),
            address: pool_address,
            protocol: ProtocolType::Dodo,
            protocol_label: "DODO_V2".into(),
            tokens: vec![quote, base],
            fee_bps: 0,
            tick_spacing: None,
            pool_id: None,
            pool_id_verified: false,
            hooks: None,
            pool_type: None,
            created_block: 1,
        }];
        let address_index = discovered
            .iter()
            .enumerate()
            .map(|(idx, pool)| (pool.address, idx))
            .collect();
        let mut arena = StateArena::default();
        let metas = arena.sync_from_discovery(&cache, &discovered, &address_index, None);
        assert_eq!(metas.len(), 1);
        let addresses: Vec<_> = metas[0]
            .tokens
            .iter()
            .filter_map(|index| arena.token_address(*index))
            .collect();
        assert_eq!(addresses, vec![base, quote]);
    }

    #[test]
    fn clone_drops_hot_overlay() {
        let addr = Address::from([8u8; 20]);
        let mut arena = StateArena::default();
        let idx = arena.register_pool(addr, v2_state());
        arena.hot_overlay.resize(1, None);
        arena.hot_overlay[0] = Some(v2_state());

        let cloned = arena.clone();
        assert!(cloned.hot_overlay.is_empty());
        assert!(cloned.pool_state(idx).is_some());
    }

    #[test]
    fn pool_state_mut_promotes_hot_overlay() {
        let addr = Address::from([7u8; 20]);
        let mut arena = StateArena::default();
        let idx = arena.register_pool(addr, v2_state());
        arena.hot_overlay.resize(1, None);
        arena.hot_overlay[0] = Some(Arc::new(PoolState::V2(V2PoolState {
            reserve0: U256::from(5_000u64),
            reserve1: U256::from(2_000u64),
            fee: U256::from(997u64),
            fee_denominator: U256::from(1_000u64),
            block_timestamp_last: 1,
        })));

        let state = arena.pool_state_mut(idx).expect("mutable state");
        let PoolState::V2(s) = state else {
            panic!("expected v2");
        };
        s.reserve0 = U256::from(6_000u64);
        assert!(arena.hot_overlay[0].is_none());
        let PoolState::V2(updated) = arena.pool_state(idx).expect("updated state") else {
            panic!("expected v2");
        };
        assert_eq!(updated.reserve0, U256::from(6_000u64));
    }

    #[test]
    fn apply_hot_cache_rejects_protocol_family_swap() {
        use crate::core::types::V3PoolState;
        let addr = Address::from([15u8; 20]);
        let mut arena = StateArena::default();
        let idx = arena.register_pool(addr, v2_state());
        let cache = StateCache::default();
        cache.insert(
            addr,
            PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u64) << 96,
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Default::default(),
            }),
        );
        arena.apply_hot_cache(&cache, &[addr]);
        assert!(
            matches!(arena.pool_state(idx), Some(PoolState::V2(_))),
            "V3 overlay must not replace V2 base state"
        );
    }

    #[test]
    fn apply_hot_cache_overlays_by_pool_index() {
        let addr = Address::from([5u8; 20]);
        let mut arena = StateArena::default();
        let idx = arena.register_pool(addr, v2_state());

        let cache = StateCache::default();
        cache.insert(
            addr,
            PoolState::V2(V2PoolState {
                reserve0: U256::from(42_000u64),
                reserve1: U256::from(2_000u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 2,
            }),
        );
        arena.apply_hot_cache(&cache, &[addr]);

        let PoolState::V2(s) = arena.pool_state(idx).expect("overlay state") else {
            panic!("expected v2");
        };
        assert_eq!(s.reserve0, U256::from(42_000u64));
        assert_eq!(s.block_timestamp_last, 2);
    }

    #[test]
    fn route_state_revision_ignores_unrelated_pool_updates() {
        let route_addr = Address::with_last_byte(1);
        let unrelated_addr = Address::with_last_byte(2);
        let mut arena = StateArena::default();
        let route_pool = arena.register_pool(route_addr, v2_state());
        let _ = arena.register_pool(unrelated_addr, v2_state());
        let cache = StateCache::default();
        cache.insert(route_addr, (*v2_state()).clone());
        cache.insert(unrelated_addr, (*v2_state()).clone());
        let addresses = [route_addr, unrelated_addr];
        arena.apply_hot_cache(&cache, &addresses);
        let edges: CycleEdges = [Edge {
            pool_index: route_pool,
            token_in: TokenIndex(0),
            token_out: TokenIndex(1),
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        }]
        .into_iter()
        .collect();
        let before = arena
            .route_state_revision(&edges)
            .expect("hot route revision");

        cache.insert(unrelated_addr, (*v2_state()).clone());
        arena.apply_hot_cache(&cache, &addresses);
        assert_eq!(arena.route_state_revision(&edges), Some(before));

        cache.insert(route_addr, (*v2_state()).clone());
        arena.apply_hot_cache(&cache, &addresses);
        assert_ne!(arena.route_state_revision(&edges), Some(before));
    }

    #[test]
    fn sync_skips_tradable_pools_without_graph_edges() {
        let pool_address = Address::with_last_byte(21);
        let a = Address::with_last_byte(22);
        let b = Address::with_last_byte(23);
        let funded = MIN_HOP_TOKEN_BALANCE;
        let dust = U256::ZERO;
        let cache = StateCache::default();
        cache.insert(
            pool_address,
            PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: dust,
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 1,
            }),
        );
        let discovered = [DiscoveredPool {
            pool_key: pool_address.to_string(),
            address: pool_address,
            protocol: ProtocolType::UniswapV2,
            protocol_label: "TEST_V2".into(),
            tokens: vec![a, b],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            pool_id_verified: false,
            hooks: None,
            pool_type: None,
            created_block: 1,
        }];
        let address_index = discovered
            .iter()
            .enumerate()
            .map(|(idx, pool)| (pool.address, idx))
            .collect();
        let mut arena = StateArena::default();
        let metas = arena.sync_from_discovery(&cache, &discovered, &address_index, None);
        assert!(metas.is_empty());
    }

    #[test]
    fn sync_hydrates_token_decimals_from_hints() {
        let pool_address = Address::with_last_byte(11);
        let usdc = Address::with_last_byte(6);
        let weth = Address::with_last_byte(18);
        let cache = StateCache::default();
        cache.insert(
            pool_address,
            PoolState::V2(V2PoolState {
                reserve0: MIN_HOP_TOKEN_BALANCE,
                reserve1: MIN_HOP_TOKEN_BALANCE + U256::from(1u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 1,
            }),
        );
        let discovered = [DiscoveredPool {
            pool_key: pool_address.to_string(),
            address: pool_address,
            protocol: ProtocolType::UniswapV2,
            protocol_label: "UNI-V2".into(),
            tokens: vec![usdc, weth],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            pool_id_verified: false,
            hooks: None,
            pool_type: None,
            created_block: 1,
        }];
        let hints: FxHashMap<_, _> = [(usdc, 6u8), (weth, 18u8)].into_iter().collect();
        let address_index = discovered
            .iter()
            .enumerate()
            .map(|(idx, pool)| (pool.address, idx))
            .collect();
        let mut arena = StateArena::default();
        let metas = arena.sync_from_discovery(&cache, &discovered, &address_index, Some(&hints));
        assert_eq!(metas.len(), 1);
        let usdc_idx = metas[0].tokens[0];
        let weth_idx = metas[0].tokens[1];
        assert_eq!(arena.token_decimals(usdc_idx), 6);
        assert_eq!(arena.token_decimals(weth_idx), 18);
    }

    #[test]
    fn sync_skips_two_token_pool_when_decimal_hints_incomplete() {
        use crate::core::constants::MIN_HOP_TOKEN_BALANCE;
        use crate::core::types::V2PoolState;

        let usdc = Address::from([1u8; 20]);
        let weth = Address::from([2u8; 20]);
        let pool_address = Address::from([3u8; 20]);
        let cache = StateCache::default();
        cache.insert(
            pool_address,
            PoolState::V2(V2PoolState {
                reserve0: MIN_HOP_TOKEN_BALANCE,
                reserve1: MIN_HOP_TOKEN_BALANCE + U256::from(1u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 1,
            }),
        );
        let discovered = [DiscoveredPool {
            pool_key: pool_address.to_string(),
            address: pool_address,
            protocol: ProtocolType::UniswapV2,
            protocol_label: "UNI-V2".into(),
            tokens: vec![usdc, weth],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            pool_id_verified: false,
            hooks: None,
            pool_type: None,
            created_block: 1,
        }];
        let hints: FxHashMap<_, _> = [(usdc, 6u8)].into_iter().collect();
        let address_index = discovered
            .iter()
            .enumerate()
            .map(|(idx, pool)| (pool.address, idx))
            .collect();
        let mut arena = StateArena::default();
        let metas = arena.sync_from_discovery(&cache, &discovered, &address_index, Some(&hints));
        assert!(
            metas.is_empty(),
            "missing weth decimals must not admit pool"
        );
    }

    #[test]
    fn apply_hot_cache_clears_overlay_when_cache_entry_missing() {
        let addr = Address::from([4u8; 20]);
        let mut arena = StateArena::default();
        let idx = arena.register_pool(addr, v2_state());
        let cache = StateCache::default();
        cache.insert(addr, (*v2_state()).clone());
        arena.apply_hot_cache(&cache, &[addr]);
        assert!(arena.hot_overlay[0].is_some());

        cache.remove(&addr);
        arena.apply_hot_cache(&cache, &[addr]);
        assert!(arena.hot_overlay[0].is_none());
        assert!(arena.pool_state(idx).is_some());
    }

    #[test]
    fn sync_preserves_v3_ticks_when_price_level_unchanged() {
        use crate::core::types::{V3PoolState, V3Tick};

        let pool = Address::with_last_byte(90);
        let token_a = Address::with_last_byte(91);
        let token_b = Address::with_last_byte(92);
        let ticks: Arc<[V3Tick]> = Arc::from([
            V3Tick {
                tick: -60,
                liquidity_gross: 1,
                liquidity_net: 1,
            },
            V3Tick {
                tick: 60,
                liquidity_gross: 1,
                liquidity_net: -1,
            },
        ]);
        let mut with_ticks = V3PoolState {
            sqrt_price_x96: U256::from(1u128 << 96),
            liquidity: 1_000_000,
            tick: 0,
            fee: U256::from(3000u32),
            tick_spacing: 60,
            unlocked: true,
            fee_protocol: 0,
            observation_cardinality: 1,
            ticks: Arc::clone(&ticks),
        };
        let cache = StateCache::default();
        // First insert with ticks via direct arena register, then sync from
        // cache (no ticks) with same price level.
        let mut arena = StateArena::default();
        let idx = arena.register_pool(pool, Arc::new(PoolState::V3(with_ticks.clone())));
        with_ticks.ticks = Arc::from([]);
        cache.insert(pool, PoolState::V3(with_ticks));
        let discovered = [DiscoveredPool {
            pool_key: pool.to_string(),
            address: pool,
            protocol: ProtocolType::UniswapV3,
            protocol_label: "V3".into(),
            tokens: vec![token_a, token_b],
            fee_bps: 30,
            tick_spacing: Some(60),
            pool_id: None,
            pool_id_verified: false,
            hooks: None,
            pool_type: None,
            created_block: 1,
        }];
        let index = discovered
            .iter()
            .enumerate()
            .map(|(i, p)| (p.address, i))
            .collect();
        let metas = arena.sync_from_discovery(&cache, &discovered, &index, None);
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].pool_index, idx);
        let PoolState::V3(s) = arena.pool_state(idx).expect("state") else {
            panic!("expected v3");
        };
        assert_eq!(s.ticks.len(), 2, "ticks should survive cache re-sync");
        assert_eq!(s.ticks[0].tick, -60);
    }

    #[test]
    fn apply_hot_cache_preserves_v3_ticks_across_liquidity_refresh() {
        use crate::core::types::{V3PoolState, V3Tick};

        let pool = Address::with_last_byte(93);
        let ticks: Arc<[V3Tick]> = Arc::from([V3Tick {
            tick: 0,
            liquidity_gross: 10,
            liquidity_net: 0,
        }]);
        let with_ticks = V3PoolState {
            sqrt_price_x96: U256::from(1u128 << 96),
            liquidity: 1_000_000,
            tick: 0,
            fee: U256::from(3000u32),
            tick_spacing: 60,
            unlocked: true,
            fee_protocol: 0,
            observation_cardinality: 1,
            ticks: Arc::clone(&ticks),
        };
        let mut arena = StateArena::default();
        let idx = arena.register_pool(pool, Arc::new(PoolState::V3(with_ticks)));
        let cache = StateCache::default();
        // HF multicall refresh: same tick/sqrt, new liquidity, empty ticks.
        cache.insert(
            pool,
            PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_100_000,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from([]),
            }),
        );
        arena.apply_hot_cache(&cache, &[pool]);
        let PoolState::V3(s) = arena.pool_state(idx).expect("overlay") else {
            panic!("expected v3");
        };
        assert_eq!(s.liquidity, 1_100_000);
        assert_eq!(s.ticks.len(), 1, "LF ticks must survive HF hot-cache overlay");
        assert_eq!(s.ticks[0].tick, 0);
    }

    #[test]
    fn sync_appends_when_tradable_extends_existing_prefix() {
        let pool_a = Address::with_last_byte(80);
        let pool_b = Address::with_last_byte(81);
        let token_a = Address::with_last_byte(82);
        let token_b = Address::with_last_byte(83);
        let funded = MIN_HOP_TOKEN_BALANCE;
        let cache = StateCache::default();
        let v2 = |ts| {
            PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::from(1u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: ts,
            })
        };
        cache.insert(pool_a, v2(1));
        let discovered = [
            DiscoveredPool {
                pool_key: pool_a.to_string(),
                address: pool_a,
                protocol: ProtocolType::UniswapV2,
                protocol_label: "A".into(),
                tokens: vec![token_a, token_b],
                fee_bps: 30,
                tick_spacing: None,
                pool_id: None,
                pool_id_verified: false,
                hooks: None,
                pool_type: None,
                created_block: 1,
            },
            DiscoveredPool {
                pool_key: pool_b.to_string(),
                address: pool_b,
                protocol: ProtocolType::UniswapV2,
                protocol_label: "B".into(),
                tokens: vec![token_a, token_b],
                fee_bps: 30,
                tick_spacing: None,
                pool_id: None,
                pool_id_verified: false,
                hooks: None,
                pool_type: None,
                created_block: 2,
            },
        ];
        let index = discovered
            .iter()
            .enumerate()
            .map(|(i, p)| (p.address, i))
            .collect();
        let mut arena = StateArena::default();
        // First sync: only pool A is tradable.
        let metas_a = arena.sync_from_discovery(&cache, &discovered[..1], &index, None);
        assert_eq!(metas_a.len(), 1);
        let idx_a = metas_a[0].pool_index;
        let fp_a = arena.routing_layout_fingerprint();

        // Second sync: pool B becomes tradable — append, preserve A's index.
        cache.insert(pool_b, v2(2));
        let metas_ab = arena.sync_from_discovery(&cache, &discovered, &index, None);
        assert_eq!(metas_ab.len(), 2);
        assert_eq!(metas_ab[0].pool_index, idx_a);
        assert_eq!(arena.pool_address(idx_a), Some(pool_a));
        assert_eq!(arena.pool_address(PoolIndex(1)), Some(pool_b));
        assert_ne!(arena.routing_layout_fingerprint(), fp_a);
        assert_eq!(arena.pool_count(), 2);
    }

    #[test]
    fn sync_rebuild_after_shared_clone_replaces_without_stale_layout() {
        // LF does `ctx.arena.lock().clone()` before sync. Rebuild must drop the
        // shared Arc (not make_mut-clone-then-clear) and produce a clean layout.
        let pool_a = Address::with_last_byte(70);
        let pool_b = Address::with_last_byte(71);
        let token_a = Address::with_last_byte(72);
        let token_b = Address::with_last_byte(73);
        let funded = MIN_HOP_TOKEN_BALANCE;
        let cache = StateCache::default();
        cache.insert(
            pool_a,
            PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::from(1u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 1,
            }),
        );
        let discovered_a = [DiscoveredPool {
            pool_key: pool_a.to_string(),
            address: pool_a,
            protocol: ProtocolType::UniswapV2,
            protocol_label: "A".into(),
            tokens: vec![token_a, token_b],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            pool_id_verified: false,
            hooks: None,
            pool_type: None,
            created_block: 1,
        }];
        let index_a = discovered_a
            .iter()
            .enumerate()
            .map(|(i, p)| (p.address, i))
            .collect();
        let mut shared = StateArena::default();
        shared.sync_from_discovery(&cache, &discovered_a, &index_a, None);
        let layout_a = shared.routing_layout_fingerprint();
        assert_eq!(shared.pool_count(), 1);

        // Simulate LF: clone (shared Arc) then rebuild with a different set.
        let mut local = shared.clone();
        cache.insert(
            pool_b,
            PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::from(2u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 2,
            }),
        );
        let discovered_b = [DiscoveredPool {
            pool_key: pool_b.to_string(),
            address: pool_b,
            protocol: ProtocolType::UniswapV2,
            protocol_label: "B".into(),
            tokens: vec![token_a, token_b],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            pool_id_verified: false,
            hooks: None,
            pool_type: None,
            created_block: 2,
        }];
        let index_b = discovered_b
            .iter()
            .enumerate()
            .map(|(i, p)| (p.address, i))
            .collect();
        let metas = local.sync_from_discovery(&cache, &discovered_b, &index_b, None);
        assert_eq!(metas.len(), 1);
        assert_eq!(local.pool_count(), 1);
        assert_eq!(local.pool_address(metas[0].pool_index), Some(pool_b));
        assert_ne!(local.routing_layout_fingerprint(), layout_a);
        // Original shared arena must remain untouched (COW replace, not mutate-in-place).
        assert_eq!(shared.pool_count(), 1);
        assert_eq!(shared.pool_address(PoolIndex(0)), Some(pool_a));
        assert_eq!(shared.routing_layout_fingerprint(), layout_a);
    }

    #[test]
    fn sync_reuses_arena_when_tradable_layout_unchanged() {
        let pool_address = Address::with_last_byte(50);
        let token_a = Address::with_last_byte(51);
        let token_b = Address::with_last_byte(52);
        let funded = MIN_HOP_TOKEN_BALANCE;
        let cache = StateCache::default();
        let v2 = PoolState::V2(V2PoolState {
            reserve0: funded,
            reserve1: funded + U256::from(1u64),
            fee: U256::from(997u64),
            fee_denominator: U256::from(1_000u64),
            block_timestamp_last: 1,
        });
        cache.insert(pool_address, v2.clone());
        let discovered = [DiscoveredPool {
            pool_key: pool_address.to_string(),
            address: pool_address,
            protocol: ProtocolType::UniswapV2,
            protocol_label: "V2".into(),
            tokens: vec![token_a, token_b],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            pool_id_verified: false,
            hooks: None,
            pool_type: None,
            created_block: 1,
        }];
        let address_index = discovered
            .iter()
            .enumerate()
            .map(|(i, p)| (p.address, i))
            .collect();
        let mut arena = StateArena::default();
        let metas_a = arena.sync_from_discovery(&cache, &discovered, &address_index, None);
        assert_eq!(metas_a.len(), 1);
        let pool_idx = metas_a[0].pool_index;
        cache.insert(
            pool_address,
            PoolState::V2(V2PoolState {
                reserve0: funded + U256::from(100u64),
                reserve1: funded + U256::from(2u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 9,
            }),
        );
        let metas_b = arena.sync_from_discovery(&cache, &discovered, &address_index, None);
        assert_eq!(metas_b.len(), 1);
        assert_eq!(metas_b[0].pool_index, pool_idx);
        let PoolState::V2(updated) = arena.pool_state(pool_idx).expect("state") else {
            panic!("expected v2");
        };
        assert_eq!(updated.block_timestamp_last, 9);
    }

    #[test]
    fn sync_rebuilds_when_tradable_pool_becomes_ineligible() {
        let pool_address = Address::with_last_byte(60);
        let token_a = Address::with_last_byte(61);
        let token_b = Address::with_last_byte(62);
        let funded = MIN_HOP_TOKEN_BALANCE;
        let cache = StateCache::default();
        cache.insert(
            pool_address,
            PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::from(1u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 1,
            }),
        );
        let discovered = [DiscoveredPool {
            pool_key: pool_address.to_string(),
            address: pool_address,
            protocol: ProtocolType::UniswapV2,
            protocol_label: "V2".into(),
            tokens: vec![token_a, token_b],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            pool_id_verified: false,
            hooks: None,
            pool_type: None,
            created_block: 1,
        }];
        let address_index = discovered
            .iter()
            .enumerate()
            .map(|(i, p)| (p.address, i))
            .collect();
        let mut arena = StateArena::default();
        assert_eq!(
            arena.sync_from_discovery(&cache, &discovered, &address_index, None).len(),
            1
        );
        cache.insert(
            pool_address,
            PoolState::V2(V2PoolState {
                reserve0: U256::ZERO,
                reserve1: U256::ZERO,
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 2,
            }),
        );
        assert!(
            arena
                .sync_from_discovery(&cache, &discovered, &address_index, None)
                .is_empty()
        );
        assert_eq!(arena.pool_count(), 0);
    }

    #[test]
    fn sync_rebuild_clears_hot_overlay_vectors() {
        let pool_a = Address::with_last_byte(30);
        let pool_b = Address::with_last_byte(31);
        let token_a = Address::with_last_byte(40);
        let token_b = Address::with_last_byte(41);
        let funded = MIN_HOP_TOKEN_BALANCE;
        let cache = StateCache::default();
        cache.insert(
            pool_a,
            PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::from(1u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 1,
            }),
        );
        let discovered_a = [DiscoveredPool {
            pool_key: pool_a.to_string(),
            address: pool_a,
            protocol: ProtocolType::UniswapV2,
            protocol_label: "A".into(),
            tokens: vec![token_a, token_b],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            pool_id_verified: false,
            hooks: None,
            pool_type: None,
            created_block: 1,
        }];
        let index_a = discovered_a
            .iter()
            .enumerate()
            .map(|(i, p)| (p.address, i))
            .collect();
        let mut arena = StateArena::default();
        arena.sync_from_discovery(&cache, &discovered_a, &index_a, None);
        arena.hot_overlay.resize(1, Some(v2_state()));
        arena.hot_revisions.resize(1, 99);

        cache.insert(
            pool_b,
            PoolState::V2(V2PoolState {
                reserve0: funded,
                reserve1: funded + U256::from(1u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 2,
            }),
        );
        let discovered_b = [DiscoveredPool {
            pool_key: pool_b.to_string(),
            address: pool_b,
            protocol: ProtocolType::UniswapV2,
            protocol_label: "B".into(),
            tokens: vec![token_a, token_b],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            pool_id_verified: false,
            hooks: None,
            pool_type: None,
            created_block: 2,
        }];
        let index_b = discovered_b
            .iter()
            .enumerate()
            .map(|(i, p)| (p.address, i))
            .collect();
        arena.sync_from_discovery(&cache, &discovered_b, &index_b, None);
        assert!(arena.hot_overlay.is_empty());
        assert!(arena.hot_revisions.is_empty());
        assert_eq!(arena.pool_count(), 1);
    }

    #[test]
    fn register_pool_update_clears_hot_overlay() {
        let addr = Address::from([6u8; 20]);
        let mut arena = StateArena::default();
        let idx = arena.register_pool(addr, v2_state());
        arena.hot_overlay.resize(1, None);
        arena.hot_overlay[0] = Some(v2_state());

        let _ = arena.register_pool(addr, v2_state());
        assert!(arena.hot_overlay[0].is_none());
        assert_eq!(idx, PoolIndex(0));
    }
}
