use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy::primitives::Address;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::infra::hasura::{DiscoveryCursor, HasuraClient};
use crate::infra::rpc::RpcPool;
use crate::pipeline::fetcher::fetch_missing_pool_states;
use crate::services::discovery::{DiscoveredPool, TokenMeta};
use crate::services::state_cache::StateCache;
use crate::util::now_ms;
use crate::error::ArbError;

/// Re-fetch token metadata every N discovery ticks.
const TOKEN_META_REFRESH_INTERVAL: u64 = 10;

/// Remove a pool from the discovered list after this many consecutive
/// fetch classifications as invalid / never-fetched.
const MAX_INVALID_FETCHES: u32 = 30;

#[derive(Default)]
struct DiscoveryState {
    discovered: Arc<Vec<DiscoveredPool>>,
    token_metas: Vec<TokenMeta>,
    discovery_cursor: DiscoveryCursor,
    last_discovery_ms: u64,
    hot_addresses: Vec<Address>,
    invalid_fetch_count: HashMap<Address, u32>,
}

impl DiscoveryState {
    fn rebuild_discovered(&mut self, new_pools: Vec<DiscoveredPool>, cursor: DiscoveryCursor) {
        self.discovered = Arc::new(new_pools);
        self.discovery_cursor = cursor;
    }
}

pub struct StateRefreshService {
    config: Arc<AppConfig>,
    hasura: HasuraClient,
    cache: Arc<StateCache>,
    rpc: Arc<RpcPool>,
    discovery_state: parking_lot::RwLock<DiscoveryState>,
    discovery_count: AtomicU64,
    lf_pass_count: AtomicU64,
}

impl StateRefreshService {
    pub fn new(config: Arc<AppConfig>, cache: Arc<StateCache>, rpc: Arc<RpcPool>) -> Result<Self, ArbError> {
        let hasura = HasuraClient::new(config.hasura_url.clone(), config.hasura_secret.clone())?;
        Ok(Self {
            config,
            hasura,
            cache,
            rpc,
            discovery_state: parking_lot::RwLock::new(DiscoveryState::default()),
            discovery_count: AtomicU64::new(0),
            lf_pass_count: AtomicU64::new(0),
        })
    }

    pub fn lf_tick(&self) -> u64 {
        self.lf_pass_count.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn hot_addresses(&self) -> Vec<Address> {
        self.discovery_state.read().hot_addresses.clone()
    }

    pub fn set_hot_addresses(&self, addrs: Vec<Address>) {
        self.discovery_state.write().hot_addresses = addrs;
    }

    /// All discovered pools that passed `is_routable_pool` at ingest time.
    pub fn discovered_pools(&self) -> Arc<Vec<DiscoveredPool>> {
        Arc::clone(&self.discovery_state.read().discovered)
    }

    pub fn discovered_pools_raw(&self) -> Vec<DiscoveredPool> {
        (*self.discovery_state.read().discovered).clone()
    }

    pub fn routable_pool_count(&self) -> usize {
        self.discovery_state.read().discovered.len()
    }

    pub fn cache_size(&self) -> usize {
        self.cache.len()
    }

    pub async fn maybe_discover(&self) -> anyhow::Result<usize> {
        let elapsed = now_ms().saturating_sub(self.discovery_state.read().last_discovery_ms);
        if elapsed < self.config.discovery_interval_ms {
            return Ok(0);
        }

        let cursor = self.discovery_state.read().discovery_cursor.clone();
        let result = self.hasura.discover_pools(&cursor).await?;
        self.discovery_state.write().last_discovery_ms = now_ms();
        self.discovery_count.fetch_add(1, Ordering::Relaxed);

        if result.pools.is_empty() && result.complete {
            self.refresh_token_metas_if_due().await;
            return Ok(0);
        }

        let added = {
            let mut state = self.discovery_state.write();
            let start_len = state.discovered.len();
            let mut seen: std::collections::HashSet<_> = state
                .discovered
                .iter()
                .map(|p| p.pool_key.clone())
                .collect();
            let mut added = 0usize;
            let mut new_discovered = Vec::with_capacity(start_len + result.pools.len());
            new_discovered.extend_from_slice(&state.discovered);
            for pool in result.pools {
                if !crate::services::discovery::is_routable_pool(&pool) {
                    continue;
                }
                if seen.insert(pool.pool_key.clone()) {
                    new_discovered.push(pool);
                    added += 1;
                }
            }
            state.rebuild_discovered(new_discovered, result.cursor.clone());
            added
        };

        if added > 0 || !result.complete {
            let total = self.routable_pool_count();
            let cursor = self.discovery_state.read().discovery_cursor.clone();
            info!(
                added,
                total,
                last_block = cursor.last_block,
                complete = result.complete,
                pending_cursor = cursor.cursor_id.is_some(),
                "hasura pool discovery"
            );
        }

        if self.discovery_state.read().token_metas.is_empty() {
            self.refresh_token_metas().await;
        } else {
            self.refresh_token_metas_if_due().await;
        }

        Ok(added)
    }

    async fn refresh_token_metas(&self) {
        match self.hasura.fetch_token_metas().await {
            Ok(metas) => {
                let count = metas.len();
                self.discovery_state.write().token_metas = metas;
                info!(count, "token metadata refreshed");
            }
            Err(e) => warn!(error = %e, "token metadata refresh failed"),
        }
    }

    async fn refresh_token_metas_if_due(&self) {
        let count = self.discovery_count.load(Ordering::Relaxed);
        if count.is_multiple_of(TOKEN_META_REFRESH_INTERVAL) {
            self.refresh_token_metas().await;
        }
    }

    /// Remove pools that have been continuously classified as invalid
    /// for more than MAX_INVALID_FETCHES consecutive fetch cycles.
    /// Should be called after `refresh_pool_states` so pruning decisions
    /// are based on fresh cache state.
    pub fn prune_dead_pools(&self) {
        let addresses: Vec<Address> = self
            .discovery_state
            .read()
            .discovered
            .iter()
            .map(|p| p.address)
            .collect();
        let (_, invalid, _) = self.cache.classify_for_fetch(&addresses);

        let mut state = self.discovery_state.write();
        let mut to_remove: Vec<Address> = Vec::new();

        let invalid_set: std::collections::HashSet<Address> =
            invalid.into_iter().copied().collect();

        for addr in &addresses {
            if !invalid_set.contains(addr) {
                state.invalid_fetch_count.remove(addr);
            }
        }

        for addr in &invalid_set {
            let entry = state.invalid_fetch_count.entry(*addr).or_insert(0);
            *entry += 1;
            if *entry >= MAX_INVALID_FETCHES {
                to_remove.push(*addr);
            }
        }

        if to_remove.is_empty() {
            return;
        }

        let before = state.discovered.len();
        let retained: Vec<DiscoveredPool> = state
            .discovered
            .iter()
            .filter(|p| !to_remove.contains(&p.address))
            .cloned()
            .collect();
        let removed = before - retained.len();
        state.discovered = Arc::new(retained);

        for addr in &to_remove {
            state.invalid_fetch_count.remove(addr);
        }

        if removed > 0 {
            info!(
                removed,
                remaining = state.discovered.len(),
                "pruned dead pools"
            );
        }
    }

    pub fn token_metas(&self) -> Vec<TokenMeta> {
        self.discovery_state.read().token_metas.clone()
    }

    pub fn token_decimals_map(&self) -> HashMap<Address, u8> {
        crate::services::oracle::token_decimals_map(&self.token_metas())
    }

    pub async fn refresh_pool_states(&self, max_pools: usize) -> anyhow::Result<usize> {
        let provider = match self.rpc.connect_state() {
            Ok(p) => p,
            Err(_) => {
                warn!("no state RPC configured — skipping pool state refresh");
                return Ok(0);
            }
        };
        let pools = self.discovered_pools();
        let hot = self.hot_addresses();
        let updated = fetch_missing_pool_states(
            provider,
            Arc::clone(&self.cache),
            &pools,
            max_pools,
            self.config.max_multicall_calls as usize,
            &hot,
        )
        .await;
        Ok(updated)
    }

    /// LF refresh batch size: full sweep on first tick and every N ticks, hot-pool-only otherwise.
    pub fn lf_refresh_batch(&self, pass: u64) -> usize {
        let pipeline = &self.config.pipeline;
        let hot_len = self.hot_addresses().len();
        let full_sweep = pass == 1 || pass.is_multiple_of(pipeline.lf_full_sweep_interval);
        if full_sweep {
            pipeline.lf_bootstrap_batch
        } else {
            pipeline.lf_hot_batch.max(hot_len).min(pipeline.lf_bootstrap_batch)
        }
    }
}
