use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::sol_types::SolCall;
use anyhow::Context;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::config::AppConfig;
use crate::core::constants::POLYGON_CHAIN_ID;
use crate::infra::pg::{DiscoveryCursor, DiscoveryResult, PgClient, PoolMetaKeyset};
use crate::infra::pool_meta_cache::PoolMetaCache;
use crate::infra::rpc::RpcPool;
use crate::pipeline::fetcher::fetch_missing_pool_states;
use crate::services::balancer_backend::enrich_polygon_balancer_pool_ids;
use crate::services::discovery::{DiscoveredPool, TokenMeta, is_routable_pool};
use crate::services::pipeline_survival::{ParseStats, log_index_parse_stats};
use crate::services::state_cache::StateCache;
use crate::util::now_ms;

/// Remove a pool from the discovered list after this many consecutive
/// fetch classifications as invalid / never-fetched.
const MAX_INVALID_FETCHES: u32 = 30;

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
    indexer_lag_blocks: AtomicU64,
    indexer_stale: AtomicBool,
    last_indexer_block: AtomicU64,
    last_indexer_check_ms: AtomicU64,
    last_state_block: AtomicU64,
    /// Set to true by the LISTEN/NOTIFY task when a pool_meta_channel notification arrives.
    /// Cleared by `maybe_discover` after triggering an early incremental refresh.
    pg_notify_pending: Arc<AtomicBool>,
}

impl StateRefreshService {
    pub fn new(config: Arc<AppConfig>, cache: Arc<StateCache>, rpc: Arc<RpcPool>) -> anyhow::Result<Self> {
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
            indexer_lag_blocks: AtomicU64::new(0),
            indexer_stale: AtomicBool::new(false),
            last_indexer_block: AtomicU64::new(0),
            last_indexer_check_ms: AtomicU64::new(0),
            last_state_block: AtomicU64::new(0),
            pg_notify_pending: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Returns a shareable flag reference for the LISTEN/NOTIFY task to set on notification.
    pub fn notify_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.pg_notify_pending)
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
        let state = self.discovery_state.read();
        self.cache
            .count_tradable_iter(state.discovered.iter().map(|p| &p.address))
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

            if !is_bootstrap && !notify_pending && elapsed < self.config.discovery_interval_ms {
                return Ok(0);
            }
            (cursor, is_bootstrap)
        };

        let mut result = if is_bootstrap {
            crate::info!("starting postgres pool bootstrap");
            self.discover_bootstrap().await?
        } else {
            self.discover_incremental(&cursor).await?
        };

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

        self.discovery_state.write().last_discovery_ms = now_ms();
        self.discovery_count.fetch_add(1, Ordering::Relaxed);

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
                if self.discovery_state.read().token_metas.is_empty() {
                    self.refresh_token_metas().await;
                }
                return Ok(0);
            }
            let mut state = self.discovery_state.write();
            let mut added = 0usize;
            let mut updated = 0usize;
            // Move the index map out (no data clone) first.
            let mut index = std::mem::take(&mut state.pool_key_index);
            {
                // Then make_mut on discovered; its borrow lives only inside this block.
                let discovered = Arc::make_mut(&mut state.discovered);
                for pool in result.pools {
                    if !is_routable_pool(&pool) {
                        continue;
                    }
                    if let Some(&idx) = index.get(&pool.pool_key) {
                        discovered[idx] = pool;
                        updated += 1;
                    } else {
                        index.insert(pool.pool_key.clone(), discovered.len());
                        discovered.push(pool);
                        added += 1;
                    }
                }
            }
            state.pool_key_index = index;
            state.discovery_cursor = result.cursor.clone();
            (added, updated)
        };

        if added > 0 || updated > 0 || !result.complete {
            let _total = self.discovered_pool_count();
            let _cursor = self.discovery_state.read().discovery_cursor.clone();
            crate::info!(
                "pool discovery (added={added}, updated={updated}, discovered={_total}, last_block={}, last_updated_block={}, complete={})",
                _cursor.last_block,
                _cursor.last_updated_block,
                result.complete,
            );
        }

        if self.discovery_state.read().token_metas.is_empty() {
            self.refresh_token_metas().await;
        }
        self.enrich_token_decimals_onchain().await;

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
            all_pools.extend(page);
            merge_parse_stats(&mut parse_stats, &page_stats);
            if !has_more {
                break;
            }
            keyset = next;
            crate::info!(
                "pg bootstrap page (batch={batch}, total={}, max_block={})",
                all_pools.len(),
                keyset.created_block,
            );
        }
        log_index_parse_stats(&parse_stats);
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
        let (pools, last_block, last_updated_block) =
            self.pg.fetch_pool_meta_incremental(cursor).await?;

        Ok(DiscoveryResult {
            pools,
            cursor: DiscoveryCursor {
                last_block: last_block.max(cursor.last_block),
                last_updated_block: last_updated_block
                    .max(cursor.last_updated_block)
                    .max(last_block),
            },
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
                crate::info!("token metadata refreshed: {_count}");
            }
            Err(_e) => crate::warn!("token metadata refresh failed: {_e:?}"),
        }
    }

    async fn enrich_token_decimals_onchain(&self) {
        let missing = {
            let state = self.discovery_state.read();
            if state.discovered.is_empty() {
                return;
            }
            let known = state.token_decimals.as_ref();
            let mut missing_set = FxHashSet::default();
            'scan: for pool in state.discovered.iter() {
                for addr in &pool.tokens {
                    if known.contains_key(addr) {
                        continue;
                    }
                    missing_set.insert(*addr);
                    if missing_set.len() >= DECIMALS_ENRICH_BATCH {
                        break 'scan;
                    }
                }
            }
            missing_set.into_iter().collect::<Vec<_>>()
        };

        if missing.is_empty() {
            return;
        }

        let Ok(provider) = self.rpc.connect_state() else {
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
            return;
        };

        let mut new_entries: Vec<TokenMeta> = Vec::with_capacity(results.len());
        for (addr, result) in missing.iter().zip(results) {
            let dec = result
                .as_ref()
                .and_then(|b| IERC20Metadata::decimalsCall::abi_decode_returns(b).ok())
                .filter(|&d| d <= 30)
                .unwrap_or(18);
            new_entries.push(TokenMeta {
                address: *addr,
                decimals: dec,
            });
        }

        let added = new_entries.len();
        if added > 0 {
            let mut state = self.discovery_state.write();
            for entry in &new_entries {
                Arc::make_mut(&mut state.token_decimals).insert(entry.address, entry.decimals);
            }
            Arc::make_mut(&mut state.token_metas).extend(new_entries);
            crate::info!("enriched token decimals on-chain: {}", added);
        }
    }

    pub fn prune_dead_pools_if_due(&self, lf_pass: u64) {
        if lf_pass != 1 && !lf_pass.is_multiple_of(PRUNE_INTERVAL) {
            return;
        }
        self.prune_dead_pools();
    }

    fn prune_dead_pools(&self) {
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

        let invalid_set: rustc_hash::FxHashSet<Address> = invalid.into_iter().copied().collect();

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
        let retain_filter: rustc_hash::FxHashSet<Address> = to_remove.iter().copied().collect();
        Arc::make_mut(&mut state.discovered).retain(|p| !retain_filter.contains(&p.address));
        state.pool_key_index = state
            .discovered
            .iter()
            .enumerate()
            .map(|(i, p)| (p.pool_key.clone(), i))
            .collect();

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

    pub async fn refresh_pool_states(&self, max_pools: usize) -> anyhow::Result<usize> {
        let pools = self.discovered_pools();
        let hot = self.hot_addresses();
        crate::debug!(
            "state refresh: {} pools, {} hot, max_pools={}",
            pools.len(),
            hot.len(),
            max_pools
        );
        self.refresh_pools_impl(&pools, max_pools, hot.as_ref())
            .await
    }

    pub async fn refresh_pool_states_for(
        &self,
        addresses: &[Address],
        max_pools: usize,
    ) -> anyhow::Result<usize> {
        if addresses.is_empty() || max_pools == 0 {
            return Ok(0);
        }
        let all = self.discovered_pools();
        // ponytail: sort+two-pointer avoids HashMap allocation. discovered_pools() returns
        // a stable-ordered Arc<Vec>. Sorting addresses once + linear merge is faster than
        // building a HashMap from all discovered pools (which can be 10k+ entries).
        let mut addrs: Vec<Address> = addresses.to_vec();
        addrs.sort_unstable();
        let mut pools: Vec<DiscoveredPool> = Vec::with_capacity(addrs.len().min(64));
        let mut ai = 0usize;
        let mut pi = 0usize;
        while ai < addrs.len() && pi < all.len() {
            match addrs[ai].cmp(&all[pi].address) {
                std::cmp::Ordering::Equal => {
                    pools.push(all[pi].clone());
                    ai += 1;
                    pi += 1;
                }
                std::cmp::Ordering::Less => ai += 1,
                std::cmp::Ordering::Greater => pi += 1,
            }
        }
        if pools.is_empty() {
            return Ok(0);
        }
        self.refresh_pools_impl(&pools, max_pools, addresses).await
    }

    async fn refresh_pools_impl(
        &self,
        pools: &[DiscoveredPool],
        max_pools: usize,
        hot: &[Address],
    ) -> anyhow::Result<usize> {
        let candidates = self.rpc.state_url_candidates();
        if candidates.is_empty() {
            crate::warn!("no state RPC configured — skipping pool state refresh");
            return Ok(0);
        }

        let cached_block = {
            let cached = self.last_state_block();
            (cached > 0).then_some(cached)
        };

        let mut total_updated = 0usize;
        for (idx, url) in candidates.iter().enumerate() {
            let provider = match self.rpc.connect_state_at(url) {
                Ok(p) => p,
                Err(_e) => {
                    crate::warn!("state RPC connect failed: {_e:?} (url_index={idx})");
                    self.rpc.deprioritize_state_url(url);
                    continue;
                }
            };
            let pinned_block = provider.get_block_number().await.ok().or(cached_block);
            let (updated, attempted) = fetch_missing_pool_states(
                provider,
                Arc::clone(&self.cache),
                pools,
                max_pools,
                self.config.max_multicall_calls as usize,
                self.config.rpc.batch_pace_ms,
                hot,
                pinned_block,
                &self.pool_meta_cache,
            )
            .await;
            total_updated = updated;
            if updated > 0 {
                if let Some(block) = pinned_block {
                    self.last_state_block.store(block, Ordering::Release);
                }
                if idx > 0 {
                    crate::info!(
                        "state RPC fallback succeeded (url_index={idx}, updated={updated})"
                    );
                }
                break;
            }
            if !attempted {
                break;
            }
            self.rpc.deprioritize_state_url(url);
            if idx + 1 < candidates.len() {
                crate::warn!(
                    "state RPC returned no pool updates — trying fallback (url_index={idx})"
                );
            }
        }
        Ok(total_updated)
    }

    pub fn bootstrap_parse_stats(&self) -> Option<ParseStats> {
        self.discovery_state.read().bootstrap_parse_stats.clone()
    }

    #[must_use]
    pub fn is_discovery_bootstrapping(&self) -> bool {
        self.discovery_state.read().discovery_cursor.last_block == 0
    }

    pub fn lf_refresh_batch(&self, pass: u64) -> usize {
        let pipeline = &self.config.pipeline;
        let hot_len = self.discovery_state.read().hot_addresses.len();
        let full_sweep = pass == 1 || pass.is_multiple_of(pipeline.lf_full_sweep_interval);
        if full_sweep {
            pipeline.lf_bootstrap_batch
        } else {
            pipeline
                .lf_hot_batch
                .max(hot_len)
                .min(pipeline.lf_bootstrap_batch)
        }
    }
}

fn merge_parse_stats(acc: &mut ParseStats, page: &ParseStats) {
    for (label, count) in &page.parsed {
        *acc.parsed.entry(label.clone()).or_default() += count;
    }
    for (label, count) in &page.rejected {
        *acc.rejected.entry(label.clone()).or_default() += count;
    }
}
