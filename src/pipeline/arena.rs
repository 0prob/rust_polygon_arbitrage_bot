use std::sync::Arc;

use alloy::primitives::Address;
use rustc_hash::FxHashMap;

use crate::core::types::{PoolIndex, PoolState, ProtocolType, TokenIndex};
use crate::services::discovery::{DiscoveredPool, discovered_to_pool_meta};
use crate::services::state_cache::StateCache;
use rustc_hash::FxHasher;
use std::hash::Hasher;

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
}

impl Default for StateArena {
    fn default() -> Self {
        let mut inner = ArenaInner::default();
        inner.layout_fingerprint = compute_layout_fingerprint(&inner);
        Self {
            inner: Arc::new(inner),
            hot_overlay: Vec::new(),
        }
    }
}

impl Clone for StateArena {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            hot_overlay: Vec::new(),
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
        if let Some(Some(state)) = self.hot_overlay.get(idx) {
            return Some(state.as_ref());
        }
        self.inner.pools.get(idx).map(std::convert::AsRef::as_ref)
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
        if let Some(&idx) = self.inner.address_to_token.get(&address) {
            if let Some(hints) = hints
                && let Some(&dec) = hints.get(&address)
            {
                let inner = Arc::make_mut(&mut self.inner);
                if let Some(slot) = inner.token_decimals.get_mut(idx.0 as usize) {
                    *slot = dec;
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
        inner.layout_fingerprint = compute_layout_fingerprint(inner);
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
    }

    pub fn register_pool(&mut self, address: Address, state: Arc<PoolState>) -> PoolIndex {
        if let Some(&idx) = self.inner.address_to_pool.get(&address) {
            let inner = Arc::make_mut(&mut self.inner);
            if let Some(slot) = inner.pools.get_mut(idx.0 as usize) {
                *slot = state;
            }
            self.clear_hot_slot(idx);
            return idx;
        }
        let inner = Arc::make_mut(&mut self.inner);
        let idx = PoolIndex(inner.pools.len() as u32);
        inner.pools.push(state);
        inner.pool_addresses.push(address);
        inner.address_to_pool.insert(address, idx);
        inner.layout_fingerprint = compute_layout_fingerprint(inner);
        idx
    }

    /// Overlay fresh pool states from cache (HF hot-path; Arc clone only).
    pub fn apply_hot_cache(&mut self, cache: &StateCache, addresses: &[Address]) -> u64 {
        let pool_count = self.inner.pools.len();
        if self.hot_overlay.len() != pool_count {
            self.hot_overlay.resize(pool_count, None);
        }
        let (states, generation) = cache.get_arcs_with_generation(addresses);
        for (address, state) in states {
            let Some(&idx) = self.inner.address_to_pool.get(&address) else {
                continue;
            };
            let slot = idx.0 as usize;
            if let Some(overlay) = self.hot_overlay.get_mut(slot) {
                *overlay = Some(state);
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

        let reusable = self.inner.pools.len() == tradable.len()
            && tradable
                .iter()
                .all(|(_, address, _)| self.inner.address_to_pool.contains_key(address));
        if !reusable {
            *Arc::make_mut(&mut self.inner) = ArenaInner::default();
            let inner = Arc::make_mut(&mut self.inner);
            let expected_tokens = tradable
                .iter()
                .map(|(idx, _, _)| pools.get(*idx).map_or(2, |pool| pool.tokens.len()))
                .sum::<usize>()
                .max(1);
            inner.tokens.reserve(expected_tokens);
            inner.token_decimals.reserve(expected_tokens);
            inner.pools.reserve(tradable.len());
            inner.pool_addresses.reserve(tradable.len());
            inner.address_to_pool.reserve(tradable.len());
            inner.address_to_token.reserve(expected_tokens);
        }

        let mut metas = Vec::with_capacity(tradable.len().max(1));
        for (idx, _address, state) in tradable {
            let Some(pool) = pools.get(idx) else {
                continue;
            };
            let bpt_hint = match state.as_ref() {
                PoolState::Balancer(b) => b.bpt_index,
                _ => None,
            };
            let token_count = match state.as_ref() {
                PoolState::Balancer(b) if !b.tokens.is_empty() => b.tokens.len(),
                PoolState::Woofi(w) if !w.tokens.is_empty() => w.tokens.len(),
                _ => pool.tokens.len(),
            };
            if !crate::pipeline::graph::pool_state_graph_eligible(
                state.as_ref(),
                pool.protocol,
                token_count,
                bpt_hint,
                pool.fee_bps,
            ) {
                continue;
            }
            // ponytail: borrow token addresses instead of cloning Vec — most
            // pools use the discovery order, and state-hydrated tokens (Balancer,
            // Woofi) can be borrowed directly. Dodo canonicalization is the only
            // case that allocates (rare — discovery order matches on-chain 99%+).
            let token_addrs: &[Address];
            let dodo_owned; // extends lifetime of the rare Dodo canonicalization vec
            match state.as_ref() {
                PoolState::Balancer(b) if !b.tokens.is_empty() => {
                    token_addrs = &b.tokens;
                }
                PoolState::Woofi(w) if !w.tokens.is_empty() => {
                    token_addrs = &w.tokens;
                }
                PoolState::Dodo(d)
                    if pool.tokens.len() == 2
                        && !d.base_token.is_zero()
                        && !d.quote_token.is_zero() =>
                {
                    if pool.tokens[0] == d.quote_token {
                        dodo_owned = vec![d.base_token, d.quote_token];
                        token_addrs = &dodo_owned;
                    } else {
                        token_addrs = &pool.tokens;
                    }
                }
                _ => token_addrs = &pool.tokens,
            }
            let pool_index = self.register_pool(pool.address, Arc::clone(&state));
            let mut token_indices = Vec::with_capacity(token_addrs.len());
            for &addr in token_addrs {
                token_indices.push(self.register_token_with_hints(addr, decimal_hints));
            }
            let mut meta = discovered_to_pool_meta(pool, pool_index, &token_indices);
            if pool.protocol == ProtocolType::BalancerV2
                && let PoolState::Balancer(b) = state.as_ref()
            {
                // Use the hydrated Vault result for phantom-BPT exclusion.
                meta.bpt_index = b.bpt_index;
                if let Some(id) = b.pool_id {
                    meta.pool_id = Some(id);
                }
            }
            metas.push(meta);
        }
        metas
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;
    use crate::core::types::{V2PoolState, WoofiBaseTokenState, WoofiPoolState};
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
