use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use alloy::eips::BlockNumberOrTag;
use alloy::primitives::Address;
use alloy::primitives::B256;
use alloy::providers::Provider;
use alloy::rpc::types::BlockId;
use alloy::sol_types::SolCall;
use anyhow::Context;
use rustc_hash::FxHashMap;

use crate::config::AppConfig;
use crate::core::constants::POLYGON_CHAIN_ID;
use crate::infra::pg::{DiscoveryCursor, DiscoveryResult, PgClient, PoolMetaKeyset};
use crate::infra::pool_meta_cache::PoolMetaCache;
use crate::infra::rpc::RpcPool;
use crate::pipeline::fetcher::fetch_missing_pool_states_indexed;
use crate::services::balancer_backend::enrich_polygon_balancer_pool_ids;
use crate::services::discovery::{
    DiscoveredPool, TokenMeta, is_routable_pool, retain_routable_pool, unknown_tokens_from_pools,
};
use crate::services::index_diag::{
    log_index_summary, record_index_bootstrap_page, record_index_discovery_notify,
    record_index_discovery_skipped_tick, record_index_incremental_rows,
};
use crate::services::pipeline_survival::{ParseStats, log_index_parse_stats};
use crate::services::state_cache::StateCache;
use crate::util::now_ms;

/// Remove a pool from the discovered list after this many consecutive
/// fetch classifications as invalid / never-fetched.
const MAX_INVALID_FETCHES: u32 = 30;

/// Outcome of a targeted or batch pool-state refresh.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PoolRefreshResult {
    /// Pools whose on-chain state was written into the cache this call.
    pub updated: usize,
    /// A fetch was attempted (stale/missing targets existed) but `updated` may still be 0.
    pub attempted: bool,
    /// Requested pools that resolved in the discovery index.
    pub matched: usize,
}

impl PoolRefreshResult {
    /// Route pools are present in discovery; cached arena state may still be used when `updated` is 0.
    #[must_use]
    pub fn can_use_cached_state(&self) -> bool {
        self.matched > 0
    }

    /// HF tick may skip dispatch pool refresh when prefetch warmed cache or nothing was stale.
    #[must_use]
    pub fn prefetch_tick_succeeded(&self) -> bool {
        !self.attempted || self.updated > 0
    }
}

/// Minimum interval between indexer lag checks.
const INDEXER_HEALTH_CHECK_INTERVAL_MS: u64 = 5_000;

/// Run dead-pool pruning every N LF passes.
const PRUNE_INTERVAL: u64 = 10;

/// ERC20 `decimals()` lookups per discovery tick (multicall chunks internally).
const DECIMALS_ENRICH_BATCH: usize = 512;

#[derive(Default)]
struct DiscoveryState {
    discovered: Arc<Vec<DiscoveredPool>>,
    pool_key_index: FxHashMap<String, usize>,
    address_index: FxHashMap<Address, usize>,
    token_metas: Arc<Vec<TokenMeta>>,
    token_decimals: Arc<FxHashMap<Address, u8>>,
    discovery_cursor: DiscoveryCursor,
    last_discovery_ms: u64,
    hot_addresses: Arc<Vec<Address>>,
    invalid_fetch_count: FxHashMap<Address, u32>,
    bootstrap_parse_stats: Option<ParseStats>,
}

pub struct StateRefreshService {
    config: Arc<AppConfig>,
    pg: PgClient,
    cache: Arc<StateCache>,
    rpc: Arc<RpcPool>,
    pool_meta_cache: Arc<PoolMetaCache>,
    discovery_state: parking_lot::RwLock<DiscoveryState>,
    discovery_count: AtomicU64,
    token_metadata_loaded: AtomicBool,
    discovery_skipped_ticks: AtomicU64,
    indexer_lag_blocks: AtomicU64,
    indexer_stale: AtomicBool,
    last_indexer_block: AtomicU64,
    last_indexer_check_ms: AtomicU64,
    last_state_block: AtomicU64,
    last_state_hash: parking_lot::RwLock<Option<B256>>,
    routable_pool_count: AtomicUsize,
    routable_pool_count_generation: AtomicU64,
    fetch_never_scan_offset: AtomicUsize,
    /// Set to true by the LISTEN/NOTIFY task when a pool_meta_channel notification arrives.
    /// Cleared by `maybe_discover` after triggering an early incremental refresh.
    pg_notify_pending: Arc<AtomicBool>,
}

impl StateRefreshService {
    pub fn new(
        config: Arc<AppConfig>,
        cache: Arc<StateCache>,
        rpc: Arc<RpcPool>,
    ) -> anyhow::Result<Self> {
        let pg = PgClient::new(config.pg_url.clone())
            .with_context(|| "failed to connect to PostgreSQL")?;
        let pool_meta_cache = Arc::new(PoolMetaCache::new(PathBuf::from(
            &config.pipeline.pool_meta_cache_path,
        )));
        Ok(Self {
            config,
            pg,
            cache,
            rpc,
            pool_meta_cache,
            discovery_state: parking_lot::RwLock::new(DiscoveryState::default()),
            discovery_count: AtomicU64::new(0),
            token_metadata_loaded: AtomicBool::new(false),
            discovery_skipped_ticks: AtomicU64::new(0),
            indexer_lag_blocks: AtomicU64::new(0),
            indexer_stale: AtomicBool::new(false),
            last_indexer_block: AtomicU64::new(0),
            last_indexer_check_ms: AtomicU64::new(0),
            last_state_block: AtomicU64::new(0),
            last_state_hash: parking_lot::RwLock::new(None),
            routable_pool_count: AtomicUsize::new(0),
            routable_pool_count_generation: AtomicU64::new(0),
            fetch_never_scan_offset: AtomicUsize::new(0),
            pg_notify_pending: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Returns a shareable flag reference for the LISTEN/NOTIFY task to set on notification.
    pub fn notify_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.pg_notify_pending)
    }

    pub async fn probe_pool_meta_count(&self) -> anyhow::Result<u64> {
        self.pg.probe_pool_meta_count().await
    }

    /// Logs postgres row count asynchronously (reuses the discovery connection pool).
    pub fn spawn_connectivity_probe(self: Arc<Self>) {
        tokio::spawn(async move {
            match self.probe_pool_meta_count().await {
                Ok(count) => crate::info!("postgres connected pool_meta_rows={count}"),
                Err(e) => crate::warn!("postgres probe failed: {e}"),
            }
        });
    }

    pub fn hot_addresses(&self) -> Arc<Vec<Address>> {
        Arc::clone(&self.discovery_state.read().hot_addresses)
    }

    pub fn set_hot_addresses(&self, addrs: Vec<Address>) {
        self.discovery_state.write().hot_addresses = Arc::new(addrs);
    }

    pub fn discovered_pools(&self) -> Arc<Vec<DiscoveredPool>> {
        Arc::clone(&self.discovery_state.read().discovered)
    }

    pub fn discovered_pool_count(&self) -> usize {
        self.discovery_state.read().discovered.len()
    }

    /// Discovered pools with tradable on-chain state in cache (routing arena input).
    pub fn routable_pool_count(&self) -> usize {
        let generation = self.cache.generation();
        if self.routable_pool_count_generation.load(Ordering::Acquire) == generation {
            return self.routable_pool_count.load(Ordering::Relaxed);
        }

        let count = {
            let state = self.discovery_state.read();
            self.cache.count_tradable_in_discovery(&state.address_index)
        };
        self.routable_pool_count.store(count, Ordering::Relaxed);
        self.routable_pool_count_generation
            .store(generation, Ordering::Release);
        count
    }

    pub fn cache_size(&self) -> usize {
        self.cache.len()
    }

    pub fn is_indexer_stale(&self) -> bool {
        self.indexer_stale.load(Ordering::Relaxed)
    }

    pub fn indexer_lag_blocks(&self) -> u64 {
        self.indexer_lag_blocks.load(Ordering::Relaxed)
    }

    pub fn last_indexer_block(&self) -> u64 {
        self.last_indexer_block.load(Ordering::Relaxed)
    }

    pub fn last_state_block(&self) -> u64 {
        self.last_state_block.load(Ordering::Acquire)
    }

    pub fn last_state_hash(&self) -> Option<B256> {
        *self.last_state_hash.read()
    }

    #[inline]
    fn chain_head_fallback(&self, indexer_block: u64) -> u64 {
        let state_block = self.last_state_block();
        if state_block > 0 {
            state_block.max(indexer_block)
        } else {
            crate::warn!("no RPC for chain head — using indexer progress block only");
            indexer_block
        }
    }

    pub async fn maybe_refresh_indexer_health(&self) {
        let now = now_ms();
        let last = self.last_indexer_check_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < INDEXER_HEALTH_CHECK_INTERVAL_MS {
            return;
        }
        if self
            .last_indexer_check_ms
            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        if let Err(_e) = self.refresh_indexer_health().await {
            crate::warn!("indexer health check failed: {_e:?}");
        }
    }

    async fn refresh_indexer_health(&self) -> anyhow::Result<()> {
        let chain_id = POLYGON_CHAIN_ID;
        let Some(progress) = self.pg.fetch_indexer_progress(chain_id).await? else {
            crate::warn!("indexer progress unavailable from PostgreSQL");
            return Ok(());
        };

        let head = if let Some(source) = progress.source_block.filter(|b| *b > 0) {
            source
        } else if let Ok(provider) = self.rpc.connect_state() {
            provider
                .get_block_number()
                .await
                .unwrap_or_else(|_| self.chain_head_fallback(progress.last_processed_block))
        } else {
            self.chain_head_fallback(progress.last_processed_block)
        };

        let lag = head.saturating_sub(progress.last_processed_block);
        self.indexer_lag_blocks.store(lag, Ordering::Relaxed);
        self.last_indexer_block
            .store(progress.last_processed_block, Ordering::Relaxed);

        if progress.is_ready == Some(false) {
            let was_stale = self.indexer_stale.swap(false, Ordering::Relaxed);
            if was_stale {
                crate::info!(
                    "indexer historical backfill — execution lag gate suspended (lag={lag}, indexer_block={})",
                    progress.last_processed_block
                );
            }
            return Ok(());
        }

        let max_lag = self.config.pipeline.indexer_max_lag_blocks;
        let stale = lag > max_lag;
        let was_stale = self.indexer_stale.swap(stale, Ordering::Relaxed);

        if stale {
            crate::warn!(
                "indexer lag exceeds threshold (lag={lag}, max_lag={max_lag}, head={head}, indexer_block={})",
                progress.last_processed_block
            );
        } else if was_stale {
            crate::info!(
                "indexer caught up — execution gate cleared (lag={lag}, head={head}, indexer_block={})",
                progress.last_processed_block
            );
        } else if lag > max_lag / 2 {
            crate::warn!(
                "indexer lag elevated (lag={lag}, max_lag={max_lag}, head={head}, indexer_block={})",
                progress.last_processed_block
            );
        }

        Ok(())
    }

    pub async fn maybe_discover(&self) -> anyhow::Result<usize> {
        self.maybe_refresh_indexer_health().await;

        let (cursor, is_bootstrap) = {
            let state = self.discovery_state.read();
            let elapsed = now_ms().saturating_sub(state.last_discovery_ms);
            let cursor = state.discovery_cursor.clone();
            let is_bootstrap = cursor.last_block == 0;

            // Check the LISTEN/NOTIFY flag: if a pool_meta_channel notification arrived
            // since last discovery, skip the interval gate and refresh immediately.
            let notify_pending = self.pg_notify_pending.swap(false, Ordering::AcqRel);
            if notify_pending {
                record_index_discovery_notify();
            }

            if !is_bootstrap && !notify_pending && elapsed < self.config.discovery_interval_ms {
                let skipped = self.discovery_skipped_ticks.fetch_add(1, Ordering::Relaxed) + 1;
                record_index_discovery_skipped_tick();
                if skipped <= 2 || skipped.is_multiple_of(60) {
                    crate::debug!(
                        "discovery skipped: elapsed_ms={elapsed} interval_ms={} notify_pending={notify_pending}",
                        self.config.discovery_interval_ms
                    );
                }
                return Ok(0);
            }
            (cursor, is_bootstrap)
        };

        let tick_started = now_ms();
        let pg_started = now_ms();
        let mut result = if is_bootstrap {
            crate::info!("starting postgres pool bootstrap");
            self.discover_bootstrap().await?
        } else {
            self.discover_incremental(&cursor).await?
        };
        let pg_ms = now_ms().saturating_sub(pg_started);
        let batch_pools = result.pools.len();

        let balancer_started = now_ms();
        if let Some(endpoint) = self.config.pipeline.balancer_backend_url.as_deref() {
            let needs_ids = result.pools.iter().any(|p| {
                p.protocol == crate::core::types::ProtocolType::BalancerV2 && p.pool_id.is_none()
            });
            if needs_ids {
                match enrich_polygon_balancer_pool_ids(endpoint, &mut result.pools).await {
                    Ok(count) if count > 0 => {
                        crate::info!("Balancer backend enriched {count} Polygon pool IDs");
                    }
                    Ok(_) => {}
                    Err(error) => crate::warn!(
                        "Balancer backend enrichment failed; using on-chain fallback: {error:#}"
                    ),
                }
            }
        }
        let balancer_ms = now_ms().saturating_sub(balancer_started);

        self.discovery_state.write().last_discovery_ms = now_ms();
        self.discovery_count.fetch_add(1, Ordering::Relaxed);

        if !self.token_metadata_loaded.load(Ordering::Acquire) {
            self.refresh_token_metas().await;
        }

        let decimals_started = now_ms();
        if !result.pools.is_empty() {
            self.enrich_token_decimals_from_pools(&result.pools).await;
        }
        let decimals_ms = now_ms().saturating_sub(decimals_started);

        let merge_started = now_ms();
        let (added, updated) = {
            if result.pools.is_empty() && result.complete {
                {
                    let mut state = self.discovery_state.write();
                    state.discovery_cursor = result.cursor.clone();
                }
                let discovered_total = self.discovered_pool_count();
                if discovered_total == 0 {
                    crate::warn!(
                        "pool discovery returned zero routable pools (last_block={}, complete={})",
                        result.cursor.last_block,
                        result.complete
                    );
                } else {
                    crate::debug!(
                        "incremental discovery: no new pools (discovered={discovered_total}, last_block={})",
                        result.cursor.last_block
                    );
                }
                if !self.token_metadata_loaded.load(Ordering::Acquire) {
                    self.refresh_token_metas().await;
                }
                return Ok(0);
            }
            let mut state = self.discovery_state.write();
            let mut added = 0usize;
            let mut updated = 0usize;
            // Move the index map out (no data clone) first.
            let mut index = std::mem::take(&mut state.pool_key_index);
            let mut address_index = std::mem::take(&mut state.address_index);
            {
                // Then make_mut on discovered; its borrow lives only inside this block.
                let discovered = Arc::make_mut(&mut state.discovered);
                for pool in result.pools {
                    if !is_routable_pool(&pool) {
                        continue;
                    }
                    if let Some(&idx) = index.get(&pool.pool_key) {
                        discovered[idx] = pool;
                        address_index.insert(discovered[idx].address, idx);
                        updated += 1;
                    } else {
                        let idx = discovered.len();
                        index.insert(pool.pool_key.clone(), idx);
                        address_index.insert(pool.address, idx);
                        discovered.push(pool);
                        added += 1;
                    }
                }
            }
            state.pool_key_index = index;
            state.address_index = address_index;
            state.discovery_cursor = result.cursor.clone();
            (added, updated)
        };
        let merge_ms = now_ms().saturating_sub(merge_started);

        if added > 0 || updated > 0 || !result.complete || is_bootstrap {
            crate::debug!(
                "discovery timing: bootstrap={is_bootstrap} pg_ms={pg_ms} balancer_ms={balancer_ms} \
                 merge_ms={merge_ms} decimals_ms={decimals_ms} batch_pools={batch_pools} added={added} updated={updated} \
                 total_ms={}",
                now_ms().saturating_sub(tick_started)
            );
        }

        if added > 0 || updated > 0 || !result.complete {
            let _total = self.discovered_pool_count();
            let _cursor = self.discovery_state.read().discovery_cursor.clone();
            crate::info!(
                "pool discovery (added={added}, updated={updated}, discovered={_total}, last_block={}, last_updated_block={}, complete={})",
                _cursor.last_block,
                _cursor.last_updated_block,
                result.complete,
            );
            log_index_summary();
        }

        if !self.token_metadata_loaded.load(Ordering::Acquire) {
            self.refresh_token_metas().await;
        }
        self.routable_pool_count_generation
            .store(0, Ordering::Release);

        Ok(added)
    }

    async fn discover_bootstrap(&self) -> anyhow::Result<DiscoveryResult> {
        let batch = self.config.discovery_bootstrap_batch.max(1);
        let mut all_pools: Vec<DiscoveredPool> = Vec::new();
        let mut keyset = PoolMetaKeyset::default();
        let mut parse_stats = ParseStats::default();
        loop {
            let (page, next, has_more, page_stats) =
                self.pg.fetch_pool_meta_page(&keyset, batch as u64).await?;
            record_index_bootstrap_page();
            all_pools.extend(page.into_iter().filter_map(retain_routable_pool));
            merge_parse_stats(&mut parse_stats, &page_stats);
            if !has_more {
                break;
            }
            keyset = next;
            crate::debug!(
                "pg bootstrap page (batch={batch}, total={}, max_block={})",
                all_pools.len(),
                keyset.created_block,
            );
        }
        log_index_parse_stats(&parse_stats);
        log_index_summary();
        // SQL returns ORDER BY "createdBlock", id — last pool's block is max
        let max_block = keyset.created_block.max(0) as u64;
        crate::info!(
            "pg bootstrap loaded {} pools (max_block={max_block})",
            all_pools.len()
        );
        self.discovery_state.write().bootstrap_parse_stats = Some(parse_stats);
        Ok(DiscoveryResult {
            pools: all_pools,
            cursor: DiscoveryCursor {
                last_block: max_block,
                last_updated_block: max_block,
            },
            complete: true,
        })
    }

    async fn discover_incremental(
        &self,
        cursor: &DiscoveryCursor,
    ) -> anyhow::Result<DiscoveryResult> {
        let mut work_cursor = cursor.clone();
        let mut pools = Vec::new();
        let mut parse_stats = ParseStats::default();
        loop {
            let (page, last_block, last_updated_block, has_more, page_stats) =
                self.pg.fetch_pool_meta_incremental(&work_cursor).await?;
            record_index_incremental_rows(
                u32::try_from(
                    page_stats.parsed.values().sum::<usize>()
                        + page_stats.rejected.values().sum::<usize>(),
                )
                .unwrap_or(u32::MAX),
            );
            merge_parse_stats(&mut parse_stats, &page_stats);
            pools.extend(page.into_iter().filter_map(retain_routable_pool));
            work_cursor = DiscoveryCursor {
                last_block: last_block.max(work_cursor.last_block),
                last_updated_block: last_updated_block
                    .max(work_cursor.last_updated_block)
                    .max(last_block),
            };
            if !has_more {
                break;
            }
            crate::debug!(
                "pg incremental page (total={}, last_block={}, last_updated_block={})",
                pools.len(),
                work_cursor.last_block,
                work_cursor.last_updated_block,
            );
        }

        if !pools.is_empty() || parse_stats.rejected.values().sum::<usize>() > 0 {
            log_index_parse_stats(&parse_stats);
        }

        Ok(DiscoveryResult {
            pools,
            cursor: work_cursor,
            complete: true,
        })
    }

    fn set_token_metas(&self, metas: Vec<TokenMeta>) {
        let decimals = crate::services::oracle::token_decimals_map(&metas);
        let mut state = self.discovery_state.write();
        state.token_metas = Arc::new(metas);
        state.token_decimals = Arc::new(decimals);
    }

    async fn refresh_token_metas(&self) {
        match self.pg.fetch_all_token_metas().await {
            Ok(metas) => {
                let _count = metas.len();
                self.set_token_metas(metas);
                self.token_metadata_loaded.store(true, Ordering::Release);
                crate::info!("token metadata refreshed: {_count}");
            }
            Err(_e) => crate::warn!("token metadata refresh failed: {_e:?}"),
        }
    }

    async fn enrich_token_decimals_from_pools(&self, pools: &[DiscoveredPool]) {
        let missing = {
            let state = self.discovery_state.read();
            unknown_tokens_from_pools(pools, state.token_decimals.as_ref(), DECIMALS_ENRICH_BATCH)
        };

        if missing.is_empty() {
            return;
        }

        let Ok(provider) = self.rpc.connect_state() else {
            crate::warn!(
                "token decimals enrich skipped: state RPC unavailable (missing={})",
                missing.len()
            );
            return;
        };

        use crate::abis::IERC20Metadata;
        use crate::pipeline::abi_cache::ERC20_DECIMALS;
        use crate::pipeline::multicall::{MulticallItem, execute_multicall};
        let batch: Vec<MulticallItem> = missing
            .iter()
            .map(|addr| MulticallItem {
                target: *addr,
                data: ERC20_DECIMALS.clone(),
            })
            .collect();

        let Ok(results) = execute_multicall(&provider, &batch).await else {
            crate::warn!(
                "token decimals enrich multicall failed (missing={})",
                missing.len()
            );
            return;
        };

        let mut new_entries: Vec<TokenMeta> = Vec::with_capacity(results.len());
        for (addr, result) in missing.iter().zip(results) {
            let Some(decimals) = result
                .as_ref()
                .and_then(|b| IERC20Metadata::decimalsCall::abi_decode_returns(b).ok())
                .filter(|&d| d <= crate::core::constants::MAX_SUPPORTED_TOKEN_DECIMALS)
            else {
                continue;
            };
            new_entries.push(TokenMeta {
                address: *addr,
                decimals,
            });
        }

        let added = new_entries.len();
        if added > 0 {
            let mut state = self.discovery_state.write();
            for entry in &new_entries {
                Arc::make_mut(&mut state.token_decimals).insert(entry.address, entry.decimals);
            }
            let metas = Arc::make_mut(&mut state.token_metas);
            for entry in new_entries {
                if !metas.iter().any(|m| m.address == entry.address) {
                    metas.push(entry);
                }
            }
            crate::debug!("token decimals on-chain: enriched={added}");
        }
    }

    pub fn prune_dead_pools_if_due(&self, lf_pass: u64) {
        if lf_pass != 1 && !lf_pass.is_multiple_of(PRUNE_INTERVAL) {
            return;
        }
        self.prune_dead_pools();
    }

    fn prune_dead_pools(&self) {
        let invalid_set: rustc_hash::FxHashSet<Address> = {
            let state = self.discovery_state.read();
            self.cache
                .pools_past_invalid_retry_indexed(&state.address_index)
                .into_iter()
                .collect()
        };

        let mut state = self.discovery_state.write();
        let mut to_remove: Vec<Address> = Vec::new();

        state
            .invalid_fetch_count
            .retain(|addr, _| invalid_set.contains(addr));

        for addr in &invalid_set {
            let entry = state.invalid_fetch_count.entry(*addr).or_insert(0);
            *entry = entry.saturating_add(1);
            if *entry >= MAX_INVALID_FETCHES {
                to_remove.push(*addr);
            }
        }

        if to_remove.is_empty() {
            return;
        }

        let before = state.discovered.len();
        let retain_filter: rustc_hash::FxHashSet<Address> =
            rustc_hash::FxHashSet::from_iter(to_remove.iter().copied());
        Arc::make_mut(&mut state.discovered).retain(|p| !retain_filter.contains(&p.address));
        let (key_index, address_index) = rebuild_discovery_indexes(state.discovered.as_ref());
        state.pool_key_index = key_index;
        state.address_index = address_index;

        for addr in &to_remove {
            state.invalid_fetch_count.remove(addr);
            self.cache.remove(addr);
        }

        let removed = before - state.discovered.len();
        if removed > 0 {
            crate::info!(
                "pruned dead pools (removed={removed}, remaining={})",
                state.discovered.len()
            );
        }
    }

    pub fn token_metas(&self) -> Arc<Vec<TokenMeta>> {
        Arc::clone(&self.discovery_state.read().token_metas)
    }

    pub fn token_decimals_map(&self) -> Arc<FxHashMap<Address, u8>> {
        Arc::clone(&self.discovery_state.read().token_decimals)
    }

    /// Register tradable cached pools into a fresh arena without scanning the full
    /// discovery set (~263k) on every LF pass.
    pub fn sync_routable_arena(
        &self,
        arena: &mut crate::pipeline::arena::StateArena,
        decimal_hints: Option<&FxHashMap<Address, u8>>,
    ) -> Vec<crate::pipeline::types::PoolMeta> {
        let state = self.discovery_state.read();
        arena.sync_from_discovery(
            &self.cache,
            state.discovered.as_ref(),
            &state.address_index,
            decimal_hints,
        )
    }

    pub async fn refresh_pool_states(&self, max_pools: usize) -> anyhow::Result<PoolRefreshResult> {
        let pools = self.discovered_pools();
        let hot = self.hot_addresses();
        crate::debug!(
            "state refresh: {} pools, {} hot, max_pools={}",
            pools.len(),
            hot.len(),
            max_pools
        );
        self.refresh_pools_impl(pools.as_ref(), max_pools, hot.as_ref())
            .await
    }

    pub async fn refresh_pool_states_for(
        &self,
        addresses: &[Address],
        max_pools: usize,
    ) -> anyhow::Result<PoolRefreshResult> {
        if addresses.is_empty() || max_pools == 0 {
            return Ok(PoolRefreshResult::default());
        }
        let (addrs, selected_pools) = {
            let state = self.discovery_state.read();
            let address_index = &state.address_index;
            // ponytail: sort+dedupe addresses once, then do direct discovery-index lookups.
            // That keeps targeted refresh bounded by requested addresses instead of
            // scanning the full discovery list on every call.
            let addrs = dedupe_sorted_addresses(addresses);
            let mut selected_pools: Vec<DiscoveredPool> = Vec::with_capacity(addrs.len().min(64));
            for addr in &addrs {
                let Some(&idx) = address_index.get(addr) else {
                    continue;
                };
                let Some(pool) = state.discovered.get(idx) else {
                    continue;
                };
                selected_pools.push(pool.clone());
            }
            (addrs, selected_pools)
        };
        if selected_pools.is_empty() {
            return Ok(PoolRefreshResult::default());
        }
        self.refresh_pools_impl(&selected_pools, max_pools, &addrs)
            .await
    }

    async fn refresh_pools_impl(
        &self,
        pools: &[DiscoveredPool],
        max_pools: usize,
        hot: &[Address],
    ) -> anyhow::Result<PoolRefreshResult> {
        let matched = pools.len();
        let candidates = self.rpc.state_url_candidates();
        if candidates.is_empty() {
            crate::warn!("no state RPC configured — skipping pool state refresh");
            return Ok(PoolRefreshResult {
                matched,
                ..PoolRefreshResult::default()
            });
        }

        let cached_block = {
            let cached = self.last_state_block();
            (cached > 0).then_some(cached)
        };

        let mut total_updated = 0usize;
        let mut fetch_attempted = false;
        let refresh_started = now_ms();
        let mut rpc_head_ms = 0u64;
        let mut fetch_ms = 0u64;
        let mut hash_ms = 0u64;
        let mut rpc_attempts = 0usize;
        let mut last_pinned_block = None;
        let address_index = self.discovery_state.read().address_index.clone();
        for (idx, url) in candidates.iter().enumerate() {
            let provider = match self.rpc.connect_state_at(url) {
                Ok(p) => p,
                Err(_e) => {
                    crate::warn!("state RPC connect failed: {_e:?} (url_index={idx})");
                    self.rpc.deprioritize_state_url(url);
                    continue;
                }
            };
            rpc_attempts += 1;
            let provider_for_hash = provider.clone();
            let head_started = now_ms();
            let pinned_block = provider.get_block_number().await.ok().or(cached_block);
            rpc_head_ms = rpc_head_ms.saturating_add(now_ms().saturating_sub(head_started));
            last_pinned_block = pinned_block;
            let fetch_started = now_ms();
            let fetch_result = fetch_missing_pool_states_indexed(
                provider,
                Arc::clone(&self.cache),
                pools,
                &address_index,
                &self.fetch_never_scan_offset,
                max_pools,
                self.config.max_multicall_calls as usize,
                self.config.rpc.batch_pace_ms,
                hot,
                pinned_block,
                &self.pool_meta_cache,
            )
            .await;
            fetch_ms = fetch_ms.saturating_add(now_ms().saturating_sub(fetch_started));
            let updated = fetch_result.updated;
            total_updated = total_updated.saturating_add(updated);
            fetch_attempted |= fetch_result.attempted;
            if updated > 0 {
                if let Some(block) = pinned_block {
                    self.last_state_block.store(block, Ordering::Release);
                }
                let hash_started = now_ms();
                let pinned_hash = match pinned_block {
                    Some(block) => provider_for_hash
                        .get_block(BlockId::Number(BlockNumberOrTag::Number(block)))
                        .await
                        .ok()
                        .flatten()
                        .map(|b| b.header.hash),
                    None => None,
                };
                hash_ms = hash_ms.saturating_add(now_ms().saturating_sub(hash_started));
                if let Some(hash) = pinned_hash {
                    *self.last_state_hash.write() = Some(hash);
                }
                if idx > 0 {
                    crate::info!(
                        "state RPC fallback succeeded (url_index={idx}, updated={updated})"
                    );
                }
                if !fetch_result.requires_provider_fallback() {
                    break;
                }
            }
            if !fetch_result.attempted {
                break;
            }
            self.rpc.deprioritize_state_url(url);
            if idx + 1 < candidates.len() {
                if fetch_result.rate_limited {
                    crate::warn!(
                        "state RPC rate limited after partial refresh — trying fallback (url_index={idx}, updated={updated})"
                    );
                } else {
                    crate::warn!(
                        "state RPC returned no pool updates — trying fallback (url_index={idx})"
                    );
                }
            }
        }
        let refresh_ms = now_ms().saturating_sub(refresh_started);
        if refresh_ms >= self.config.lf_interval_ms {
            crate::warn!(
                "state refresh overrun: total_ms={refresh_ms} interval_ms={} matched={matched} updated={total_updated} attempted={fetch_attempted} rpc_attempts={rpc_attempts}/{} head_ms={rpc_head_ms} fetch_ms={fetch_ms} hash_ms={hash_ms} pinned_block={last_pinned_block:?}",
                self.config.lf_interval_ms,
                candidates.len(),
            );
        }
        Ok(PoolRefreshResult {
            updated: total_updated,
            attempted: fetch_attempted,
            matched,
        })
    }

    pub fn bootstrap_parse_stats(&self) -> Option<ParseStats> {
        self.discovery_state.read().bootstrap_parse_stats.clone()
    }

    #[must_use]
    pub fn is_discovery_bootstrapping(&self) -> bool {
        self.discovery_state.read().discovery_cursor.last_block == 0
    }

    pub fn lf_refresh_batch(&self, pass: u64) -> usize {
        refresh_batch_for(pass, self.cache.len(), &self.config.pipeline)
    }
}

fn refresh_batch_for(
    pass: u64,
    cache_size: usize,
    pipeline: &crate::config::PipelineConfig,
) -> usize {
    let bootstrap_batch = pipeline.lf_bootstrap_batch;
    let warm_cache_target = bootstrap_batch.saturating_mul(4);
    let full_sweep = pass == 1
        || cache_size < warm_cache_target
        || pass.is_multiple_of(pipeline.lf_full_sweep_interval);
    if full_sweep {
        bootstrap_batch
    } else {
        pipeline.lf_hot_batch.min(bootstrap_batch)
    }
}

fn dedupe_sorted_addresses(addresses: &[Address]) -> Vec<Address> {
    let mut addrs: Vec<Address> = addresses.to_vec();
    addrs.sort_unstable();
    addrs.dedup();
    addrs
}

fn rebuild_discovery_indexes(
    pools: &[DiscoveredPool],
) -> (FxHashMap<String, usize>, FxHashMap<Address, usize>) {
    let mut pool_key_index =
        FxHashMap::with_capacity_and_hasher(pools.len(), rustc_hash::FxBuildHasher);
    let mut address_index =
        FxHashMap::with_capacity_and_hasher(pools.len(), rustc_hash::FxBuildHasher);
    for (idx, pool) in pools.iter().enumerate() {
        pool_key_index.insert(pool.pool_key.clone(), idx);
        address_index.insert(pool.address, idx);
    }
    (pool_key_index, address_index)
}

fn merge_parse_stats(acc: &mut ParseStats, page: &ParseStats) {
    for (label, count) in &page.parsed {
        *acc.parsed.entry(label.clone()).or_default() += count;
    }
    for (label, count) in &page.rejected {
        *acc.rejected.entry(label.clone()).or_default() += count;
    }
}

#[cfg(test)]
mod tests {
    use super::{dedupe_sorted_addresses, refresh_batch_for};
    use crate::config::AppConfig;
    use alloy::primitives::Address;

    #[test]
    fn keeps_bootstrap_batch_until_cache_is_warm() {
        let config = AppConfig::default();
        assert_eq!(refresh_batch_for(2, 3_000, &config.pipeline), 3_000);
        assert_eq!(refresh_batch_for(2, 11_999, &config.pipeline), 3_000);
        assert_eq!(refresh_batch_for(2, 12_000, &config.pipeline), 500);
    }

    #[test]
    fn prefetch_tick_succeeded_when_nothing_stale_or_updates_applied() {
        use super::PoolRefreshResult;

        assert!(PoolRefreshResult {
            attempted: false,
            updated: 0,
            matched: 12,
        }
        .prefetch_tick_succeeded());
        assert!(PoolRefreshResult {
            attempted: true,
            updated: 3,
            matched: 12,
        }
        .prefetch_tick_succeeded());
        assert!(!PoolRefreshResult {
            attempted: true,
            updated: 0,
            matched: 12,
        }
        .prefetch_tick_succeeded());
    }

    #[test]
    fn dedupe_sorted_addresses_removes_duplicates() {
        let a = Address::with_last_byte(1);
        let b = Address::with_last_byte(2);
        let deduped = dedupe_sorted_addresses(&[b, a, b, a, b]);
        assert_eq!(deduped, vec![a, b]);
    }
}
