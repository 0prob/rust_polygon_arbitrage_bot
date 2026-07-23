use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use alloy::eips::BlockNumberOrTag;
use alloy::primitives::Address;
use alloy::primitives::B256;
use alloy::providers::Provider;
use alloy::rpc::types::BlockId;
use alloy::sol_types::SolCall;
use anyhow::Context;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use tokio::time::timeout;

use crate::config::AppConfig;
use crate::core::constants::POLYGON_CHAIN_ID;
use crate::infra::pg::{DiscoveryCursor, DiscoveryResult, PgClient, PoolMetaKeyset};
use crate::infra::pool_meta_cache::PoolMetaCache;
use crate::infra::rpc::RpcPool;
use crate::pipeline::fetcher::{fetch_missing_pool_states_indexed, fetch_pool_states_at_addresses};
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
/// Live: eth_blockNumber hung 5–11s (head_ms) and blew the 4s refresh interval
/// with updated=0 — pin to cache on timeout instead of blocking the LF tick.
const RPC_HEAD_TIMEOUT: Duration = Duration::from_millis(1_500);

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
    /// Shared so LF/HF readers can Arc-clone without copying ~80k entries each tick.
    address_index: Arc<FxHashMap<Address, usize>>,
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
    discovery_state: RwLock<DiscoveryState>,
    discovery_count: AtomicU64,
    token_metadata_loaded: AtomicBool,
    discovery_skipped_ticks: AtomicU64,
    indexer_lag_blocks: AtomicU64,
    indexer_stale: AtomicBool,
    last_indexer_block: AtomicU64,
    last_indexer_check_ms: AtomicU64,
    last_state_block: AtomicU64,
    last_state_hash: RwLock<Option<B256>>,
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
        let pg = PgClient::new(config.pg_url.clone()).context("failed to connect to PostgreSQL")?;
        let pool_meta_cache = Arc::new(PoolMetaCache::new(PathBuf::from(
            &config.pipeline.pool_meta_cache_path,
        )));
        Ok(Self {
            config,
            pg,
            cache,
            rpc,
            pool_meta_cache,
            discovery_state: RwLock::new(DiscoveryState::default()),
            discovery_count: AtomicU64::new(0),
            token_metadata_loaded: AtomicBool::new(false),
            discovery_skipped_ticks: AtomicU64::new(0),
            indexer_lag_blocks: AtomicU64::new(0),
            indexer_stale: AtomicBool::new(false),
            last_indexer_block: AtomicU64::new(0),
            last_indexer_check_ms: AtomicU64::new(0),
            last_state_block: AtomicU64::new(0),
            last_state_hash: RwLock::new(None),
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

    /// Topic-observed addresses that are in the discovery index and still routable
    /// (drops disabled QS/Uni V2 spam that can never enter the arena).
    #[must_use]
    pub fn filter_observed_live_routable(&self, observed: &[Address]) -> Vec<Address> {
        let state = self.discovery_state.read();
        let mut out = Vec::with_capacity(observed.len().min(64));
        let mut seen = rustc_hash::FxHashSet::default();
        for addr in observed {
            if !seen.insert(*addr) {
                continue;
            }
            let Some(&idx) = state.address_index.get(addr) else {
                continue;
            };
            let Some(pool) = state.discovered.get(idx) else {
                continue;
            };
            if !is_routable_pool(pool) {
                continue;
            }
            if !crate::services::partial_cache::is_streamable_protocol(pool.protocol) {
                continue;
            }
            out.push(*addr);
        }
        out
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
            self.cache
                .count_tradable_in_discovery(state.address_index.as_ref())
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
        let (cursor, is_bootstrap, notify_pending) = {
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
            (cursor, is_bootstrap, notify_pending)
        };

        // Indexer lag is irrelevant until pools exist — skip the serial PG/RPC hit
        // on the critical first-bootstrap path.
        if !is_bootstrap {
            self.maybe_refresh_indexer_health().await;
        }

        let tick_started = now_ms();
        let pg_started = now_ms();
        // Empty discovery + notify: re-keyset so pools with historical createdBlock land.
        let force_bootstrap = is_bootstrap || (notify_pending && self.discovered_pool_count() == 0);
        let mut result = if force_bootstrap {
            crate::info!("starting postgres pool bootstrap");
            // Overlap token-meta PG fetch with keyset pool pages (independent queries).
            let need_metas = !self.token_metadata_loaded.load(Ordering::Acquire);
            let boot = if need_metas {
                let (boot, ()) =
                    tokio::join!(self.discover_bootstrap(), self.refresh_token_metas());
                boot
            } else {
                self.discover_bootstrap().await
            };
            match boot {
                Ok(r) => r,
                Err(e) => {
                    // Restore wake so LISTEN is not lost on transient PG errors.
                    if notify_pending {
                        self.pg_notify_pending.store(true, Ordering::Release);
                    }
                    return Err(e);
                }
            }
        } else {
            match self.discover_incremental(&cursor).await {
                Ok(r) => r,
                Err(e) => {
                    if notify_pending {
                        self.pg_notify_pending.store(true, Ordering::Release);
                    }
                    return Err(e);
                }
            }
        };
        let pg_ms = now_ms().saturating_sub(pg_started);
        let batch_pools = result.pools.len();

        self.discovery_state.write().last_discovery_ms = now_ms();
        self.discovery_count.fetch_add(1, Ordering::Relaxed);

        // Incremental path (or bootstrap that skipped the join) still needs metas once.
        if !self.token_metadata_loaded.load(Ordering::Acquire) {
            self.refresh_token_metas().await;
        }

        // Snapshot missing decimals before Balancer mutates/filters pools so both can run
        // concurrently (decimals only need token addresses).
        let missing_decimals = if result.pools.is_empty() {
            Vec::new()
        } else {
            let state = self.discovery_state.read();
            unknown_tokens_from_pools(
                &result.pools,
                state.token_decimals.as_ref(),
                DECIMALS_ENRICH_BATCH,
            )
        };

        let enrich_started = now_ms();
        let endpoint = self.config.pipeline.balancer_backend_url.clone();
        let has_balancer = endpoint.is_some()
            && result
                .pools
                .iter()
                .any(|p| p.protocol == crate::core::types::ProtocolType::BalancerV2);
        let (balancer_ms, decimals_ms) = tokio::join!(
            async {
                let started = now_ms();
                if has_balancer && let Some(ref endpoint) = endpoint {
                    match enrich_polygon_balancer_pool_ids(endpoint, &mut result.pools).await {
                        Ok((enriched, filtered)) => {
                            if enriched > 0 {
                                crate::info!(
                                    "Balancer backend enriched {enriched} Polygon pool IDs"
                                );
                            }
                            if filtered > 0 {
                                crate::info!(
                                    "Balancer backend filtered {filtered} non-tradable Polygon pools \
                                     (swap disabled or recovery mode)"
                                );
                            }
                        }
                        Err(error) => crate::warn!(
                            "Balancer backend enrichment failed; using on-chain fallback: {error:#}"
                        ),
                    }
                }
                now_ms().saturating_sub(started)
            },
            async {
                let started = now_ms();
                self.enrich_missing_token_decimals(missing_decimals).await;
                now_ms().saturating_sub(started)
            },
        );
        let enrich_wall_ms = now_ms().saturating_sub(enrich_started);

        let merge_started = now_ms();
        let (added, updated, replaced_addresses) = {
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
                return Ok(0);
            }
            let mut state = self.discovery_state.write();
            let mut added = 0usize;
            let mut updated = 0usize;
            let mut replaced_addresses = Vec::new();
            // Move indexes out (no data clone) first.
            let mut index = std::mem::take(&mut state.pool_key_index);
            let mut address_index = std::mem::take(&mut state.address_index);
            {
                // Then make_mut on discovered; its borrow lives only inside this block.
                // Pools already passed retain_routable_pool in bootstrap/incremental.
                let discovered = Arc::make_mut(&mut state.discovered);
                let address_index = Arc::make_mut(&mut address_index);
                for pool in result.pools {
                    if let Some(&idx) = index.get(&pool.pool_key) {
                        if let Some(old_address) =
                            replace_discovered_pool(discovered, address_index, idx, pool)
                        {
                            replaced_addresses.push(old_address);
                        }
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
            (added, updated, replaced_addresses)
        };
        for address in replaced_addresses {
            self.cache.remove(&address);
        }
        let merge_ms = now_ms().saturating_sub(merge_started);

        if added > 0 || updated > 0 || !result.complete || is_bootstrap {
            crate::debug!(
                "discovery timing: bootstrap={is_bootstrap} pg_ms={pg_ms} balancer_ms={balancer_ms} \
                 decimals_ms={decimals_ms} enrich_wall_ms={enrich_wall_ms} merge_ms={merge_ms} \
                 batch_pools={batch_pools} added={added} updated={updated} total_ms={}",
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

        self.routable_pool_count_generation
            .store(0, Ordering::Release);

        Ok(added)
    }

    async fn discover_bootstrap(&self) -> anyhow::Result<DiscoveryResult> {
        let batch = self.config.discovery_bootstrap_batch.max(1);
        let mut all_pools: Vec<DiscoveredPool> = Vec::new();
        let mut keyset = PoolMetaKeyset::default();
        let mut parse_stats = ParseStats::default();
        let bootstrap_started = now_ms();
        loop {
            let (page, next, has_more, page_stats) =
                self.pg.fetch_pool_meta_page(&keyset, batch as u64).await?;
            record_index_bootstrap_page();
            all_pools.extend(page.into_iter().filter_map(retain_routable_pool));
            merge_parse_stats(&mut parse_stats, &page_stats);
            // Always advance — previously only updated when has_more, so a single-page
            // (or final page) left last_block=0 and re-bootstrapped every LF tick.
            keyset = next;
            if !has_more {
                break;
            }
            crate::debug!(
                "pg bootstrap page (batch={batch}, total={}, max_block={}, elapsed_ms={})",
                all_pools.len(),
                keyset.created_block,
                now_ms().saturating_sub(bootstrap_started),
            );
        }
        log_index_parse_stats(&parse_stats);
        log_index_summary();
        let max_from_pools = all_pools.iter().map(|p| p.created_block).max().unwrap_or(0);
        let max_block = bootstrap_cursor_block(keyset.created_block, max_from_pools);
        crate::info!(
            "pg bootstrap loaded {} pools (max_block={max_block})",
            all_pools.len()
        );
        self.discovery_state.write().bootstrap_parse_stats = Some(parse_stats);
        Ok(DiscoveryResult {
            pools: all_pools,
            cursor: DiscoveryCursor {
                last_block: max_block,
                last_block_id: keyset.id,
                last_updated_block: max_block,
                last_updated_id: String::new(),
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
            let (page, next_cursor, has_more, page_stats) =
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
            work_cursor = next_cursor;
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

    async fn enrich_missing_token_decimals(&self, missing: Vec<Address>) {
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
                .pools_past_invalid_retry_indexed(state.address_index.as_ref())
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
        self.sync_routable_arena_gated(arena, decimal_hints, false)
            .metas
    }

    /// `freeze_append`: skip new arena membership while graph attach catch-up runs.
    pub fn sync_routable_arena_gated(
        &self,
        arena: &mut crate::pipeline::arena::StateArena,
        decimal_hints: Option<&FxHashMap<Address, u8>>,
        freeze_append: bool,
    ) -> crate::pipeline::arena::ArenaSyncReport {
        let (discovered, address_index) = {
            let state = self.discovery_state.read();
            (
                Arc::clone(&state.discovered),
                Arc::clone(&state.address_index),
            )
        };
        arena.sync_from_discovery_gated(
            &self.cache,
            discovered.as_ref(),
            address_index.as_ref(),
            decimal_hints,
            freeze_append,
        )
    }

    pub async fn refresh_pool_states(&self, max_pools: usize) -> anyhow::Result<PoolRefreshResult> {
        let pools = self.discovered_pools();
        let hot = self.hot_addresses();
        let address_index = Arc::clone(&self.discovery_state.read().address_index);
        crate::debug!(
            "state refresh: {} pools, {} hot, max_pools={}",
            pools.len(),
            hot.len(),
            max_pools
        );
        self.refresh_pools_impl(pools.as_ref(), max_pools, hot.as_ref(), address_index, None)
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
        // ponytail: Arc-clone discovery + address-only fetch — no DiscoveredPool clones.
        let (discovered, address_index) = {
            let state = self.discovery_state.read();
            (
                Arc::clone(&state.discovered),
                Arc::clone(&state.address_index),
            )
        };
        let mut addrs = dedupe_sorted_addresses(addresses);
        addrs.retain(|addr| address_index.contains_key(addr));
        addrs.truncate(max_pools);
        if addrs.is_empty() {
            return Ok(PoolRefreshResult::default());
        }
        // `hot` unused when `initial_addrs` seeds address-only fetch.
        self.refresh_pools_impl(discovered.as_ref(), max_pools, &[], address_index, Some(addrs))
            .await
    }

    async fn refresh_pools_impl(
        &self,
        pools: &[DiscoveredPool],
        max_pools: usize,
        hot: &[Address],
        address_index: Arc<FxHashMap<Address, usize>>,
        initial_addrs: Option<Vec<Address>>,
    ) -> anyhow::Result<PoolRefreshResult> {
        let matched = initial_addrs.as_ref().map_or(pools.len(), Vec::len);
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
        let prior_block = self.last_state_block.load(Ordering::Acquire);
        let prior_hash = *self.last_state_hash.read();

        let mut total_updated = 0usize;
        let mut fetch_attempted = false;
        let refresh_started = now_ms();
        let mut rpc_head_ms = 0u64;
        let mut fetch_ms = 0u64;
        let mut hash_ms = 0u64;
        let mut rpc_attempts = 0usize;
        // Always resolve head on the first working URL so pool state is not pinned
        // to a stale block. Fallback URLs re-query head. Skip block-hash RPC when
        // the pinned block is unchanged and we already have a hash.
        let mut last_pinned_block = cached_block;
        let mut pinned_block: Option<u64> = None;
        // After a partial/rate-limited attempt, retry only still-needed addresses
        // from that batch — do not re-select a fresh max_pools window.
        // Targeted refresh seeds this so the first pass is address-only (no clone of pools).
        let mut retry_addrs: Option<Vec<Address>> = initial_addrs;
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
            if pinned_block.is_none() || idx > 0 {
                let head_started = now_ms();
                pinned_block = match timeout(RPC_HEAD_TIMEOUT, provider.get_block_number()).await {
                    Ok(Ok(n)) => Some(n),
                    Ok(Err(_)) => cached_block,
                    Err(_) => {
                        crate::debug!(
                            "state RPC head timed out after {}ms (url_index={idx}) — using cached block",
                            RPC_HEAD_TIMEOUT.as_millis()
                        );
                        cached_block
                    }
                };
                rpc_head_ms = rpc_head_ms.saturating_add(now_ms().saturating_sub(head_started));
            }
            last_pinned_block = pinned_block;
            let fetch_started = now_ms();
            let pace = crate::infra::rpc_budget::effective_batch_pace_ms(
                url,
                self.config.rpc.batch_pace_ms,
            );
            let fetch_result = crate::infra::rpc_budget::scope_rpc_budget(url, async {
                if let Some(ref addrs) = retry_addrs {
                    fetch_pool_states_at_addresses(
                        provider,
                        Arc::clone(&self.cache),
                        pools,
                        &address_index,
                        addrs,
                        self.config.max_multicall_calls as usize,
                        pace,
                        pinned_block,
                        &self.pool_meta_cache,
                    )
                    .await
                } else {
                    fetch_missing_pool_states_indexed(
                        provider,
                        Arc::clone(&self.cache),
                        pools,
                        &address_index,
                        &self.fetch_never_scan_offset,
                        max_pools,
                        self.config.max_multicall_calls as usize,
                        pace,
                        hot,
                        pinned_block,
                        &self.pool_meta_cache,
                    )
                    .await
                }
            })
            .await;
            fetch_ms = fetch_ms.saturating_add(now_ms().saturating_sub(fetch_started));
            let updated = fetch_result.updated;
            total_updated = total_updated.saturating_add(updated);
            fetch_attempted |= fetch_result.attempted;
            if updated > 0 {
                // Healthy responses clear cool-off so rank/primary recover after transient fails.
                self.rpc.clear_state_url_penalty(url);
                if let Some(block) = pinned_block {
                    self.last_state_block.store(block, Ordering::Release);
                    let need_hash = prior_hash.is_none() || block != prior_block;
                    if need_hash {
                        let hash_started = now_ms();
                        let pinned_hash = provider_for_hash
                            .get_block(BlockId::Number(BlockNumberOrTag::Number(block)))
                            .await
                            .ok()
                            .flatten()
                            .map(|b| b.header.hash);
                        hash_ms = hash_ms.saturating_add(now_ms().saturating_sub(hash_started));
                        if let Some(hash) = pinned_hash {
                            *self.last_state_hash.write() = Some(hash);
                        }
                    }
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
            if fetch_result.rate_limited {
                self.rpc.deprioritize_state_url_rate_limited(url);
            } else {
                self.rpc.deprioritize_state_url(url);
            }
            if idx + 1 < candidates.len() {
                // Within-call URL fallback: retry Invalid/missing too.
                // `needs_fetch` alone skips fresh Invalid (invalid_retry_ttl) and
                // would abort fallback with remaining=0 after a failed primary URL.
                let remaining: Vec<Address> = fetch_result
                    .targeted
                    .iter()
                    .copied()
                    .filter(|addr| match self.cache.get(addr) {
                        Some(state) if state.is_tradable() => self.cache.needs_fetch(addr),
                        _ => true,
                    })
                    .collect();
                if fetch_result.rate_limited {
                    crate::warn!(
                        "state RPC rate limited after partial refresh — trying fallback (url_index={idx}, updated={updated}, remaining={})",
                        remaining.len()
                    );
                } else {
                    crate::warn!(
                        "state RPC returned no pool updates — trying fallback (url_index={idx}, remaining={})",
                        remaining.len()
                    );
                }
                if remaining.is_empty() {
                    break;
                }
                retry_addrs = Some(remaining);
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
) -> (FxHashMap<String, usize>, Arc<FxHashMap<Address, usize>>) {
    let mut pool_key_index =
        FxHashMap::with_capacity_and_hasher(pools.len(), rustc_hash::FxBuildHasher);
    let mut address_index =
        FxHashMap::with_capacity_and_hasher(pools.len(), rustc_hash::FxBuildHasher);
    for (idx, pool) in pools.iter().enumerate() {
        pool_key_index.insert(pool.pool_key.clone(), idx);
        address_index.insert(pool.address, idx);
    }
    (pool_key_index, Arc::new(address_index))
}

fn replace_discovered_pool(
    discovered: &mut [DiscoveredPool],
    address_index: &mut FxHashMap<Address, usize>,
    idx: usize,
    pool: DiscoveredPool,
) -> Option<Address> {
    let old_address = discovered[idx].address;
    let replaced_address = (old_address != pool.address).then_some(old_address);
    if replaced_address.is_some() {
        address_index.remove(&old_address);
    }
    discovered[idx] = pool;
    address_index.insert(discovered[idx].address, idx);
    replaced_address
}

fn merge_parse_stats(acc: &mut ParseStats, page: &ParseStats) {
    for (label, count) in &page.parsed {
        *acc.parsed.entry(label.clone()).or_default() += count;
    }
    for (label, count) in &page.rejected {
        *acc.rejected.entry(label.clone()).or_default() += count;
    }
}

/// Watermark after a completed keyset bootstrap. Always ≥1 so `last_block == 0`
/// (still bootstrapping) clears even when PoolMeta is empty.
#[must_use]
fn bootstrap_cursor_block(keyset_created: i32, pool_created_max: u64) -> u64 {
    (keyset_created.max(0) as u64).max(pool_created_max).max(1)
}

#[cfg(test)]
mod tests {
    use super::{dedupe_sorted_addresses, refresh_batch_for, replace_discovered_pool};
    use crate::config::AppConfig;
    use crate::core::types::ProtocolType;
    use crate::services::discovery::DiscoveredPool;
    use alloy::primitives::Address;
    use rustc_hash::FxHashMap;

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

        assert!(
            PoolRefreshResult {
                attempted: false,
                updated: 0,
                matched: 12,
            }
            .prefetch_tick_succeeded()
        );
        assert!(
            PoolRefreshResult {
                attempted: true,
                updated: 3,
                matched: 12,
            }
            .prefetch_tick_succeeded()
        );
        assert!(
            !PoolRefreshResult {
                attempted: true,
                updated: 0,
                matched: 12,
            }
            .prefetch_tick_succeeded()
        );
    }

    #[test]
    fn dedupe_sorted_addresses_removes_duplicates() {
        let a = Address::with_last_byte(1);
        let b = Address::with_last_byte(2);
        let deduped = dedupe_sorted_addresses(&[b, a, b, a, b]);
        assert_eq!(deduped, vec![a, b]);
    }

    #[test]
    fn bootstrap_cursor_leaves_bootstrapping_mode() {
        // Empty table / single-page default keyset must not stick at 0.
        assert_eq!(super::bootstrap_cursor_block(0, 0), 1);
        assert_eq!(super::bootstrap_cursor_block(12_345, 0), 12_345);
        assert_eq!(super::bootstrap_cursor_block(100, 999), 999);
    }

    #[test]
    fn replacing_pool_removes_old_address_index() {
        let old = Address::with_last_byte(1);
        let new = Address::with_last_byte(2);
        let mut pools = vec![test_pool(old)];
        let mut index = FxHashMap::default();
        index.insert(old, 0);
        assert_eq!(
            replace_discovered_pool(&mut pools, &mut index, 0, test_pool(new)),
            Some(old)
        );
        assert!(!index.contains_key(&old));
        assert_eq!(index.get(&new), Some(&0));
    }

    fn test_pool(address: Address) -> DiscoveredPool {
        DiscoveredPool {
            pool_key: "pool".into(),
            address,
            protocol: ProtocolType::UniswapV2,
            protocol_label: "UNISWAP_V2".into(),
            tokens: vec![Address::with_last_byte(3), Address::with_last_byte(4)],
            fee_bps: 30,
            tick_spacing: None,
            pool_id: None,
            pool_id_verified: false,
            hooks: None,
            pool_type: None,
            created_block: 1,
        }
    }
}
