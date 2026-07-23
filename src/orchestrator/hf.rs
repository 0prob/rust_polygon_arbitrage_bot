use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use alloy::primitives::{Address, U256};
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::time::timeout;

use crate::config::AppConfig;
use crate::config::WalletSecrets;
use crate::core::constants::BPS_SCALE;
use crate::core::constants::MIN_TOKEN_TO_MATIC_RATE;
use crate::core::types::{Edge, FlashLoanSource, FoundCycle, ProtocolType};
use crate::infra::hypersync::HyperSyncService;
use crate::infra::rpc::RpcPool;
use crate::orchestrator::hf_eval::HfEvalResult;
use crate::orchestrator::hf_eval::{HfEvalInputOwned, rescore_rank_and_evaluate_async};
use crate::orchestrator::hf_execute::{
    cycle_tickless_cl_all_on_miss_cooldown, dispatch_profitable_candidates,
    drain_cooldown_stuck_tickless_cycles, filter_balancer_onchain_verified,
    hydrate_tickless_cl_for_cycles, probe_near_miss_balancer, refresh_and_resim_profitable,
};
use crate::orchestrator::ui_hook::SharedUiHook;
use crate::pipeline::arena::StateArena;
use crate::pipeline::sim_sanity::{matic_usd_for_flash_cap, min_economic_amount_in};
use crate::pipeline::types::{PoolMeta, compare_cycle_score};
use crate::services::execution::flash_liquidity::FlashLiquidityCache;
use crate::services::execution::flash_liquidity::{
    collect_flash_tokens_for_cycle, route_is_balancer_only,
};
use crate::services::execution::{
    ExecutionService, GasOracle, compute_assessment_gas_price, hash_cycle_edges,
    rotate_cycle_to_start,
};
use crate::services::hf_snapshot::SnapshotStore;
use crate::services::oracle::ensure_matic_usd_for_flash_cap;
use crate::services::oracle::has_reliable_matic_rate;
use crate::services::oracle::price_oracle::PriceOracle;
use crate::services::oracle::{resolve_token_decimals_for_index, resolve_token_to_matic_rate};
use crate::services::partial_cache::PartialPoolCache;
use crate::services::state_cache::StateCache;
use crate::services::state_refresh::{PoolRefreshResult, StateRefreshService};
use crate::util::now_ms;
use crate::util::ten_pow_u256;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};

pub struct HfContext {
    pub config: Arc<AppConfig>,
    pub refresh: Arc<StateRefreshService>,
    pub cache: Arc<StateCache>,
    pub partial_cache: Arc<PartialPoolCache>,
    pub snapshots: Arc<SnapshotStore>,
    pub execution: Arc<ExecutionService>,
    pub gas_oracle: Arc<GasOracle>,
    pub price_oracle: Arc<PriceOracle>,
    pub wallet: Arc<WalletSecrets>,
    pub rpc: Arc<RpcPool>,
    pub hypersync: Option<Arc<HyperSyncService>>,
    pub shutdown: watch::Receiver<bool>,
    pub ui_hook: SharedUiHook,
    pub inactive_rotation: Mutex<InactiveCycleRotation>,
}

/// Compact HF assess/dispatch row for the TUI (built only from already-evaluated results).
#[derive(Debug, Clone)]
pub struct HfCandidateUiRow {
    pub fingerprint: u64,
    pub hops: u32,
    pub route: String,
    pub amount_in: U256,
    pub amount_out: U256,
    pub gross_profit: U256,
    pub net_profit_matic_wei: U256,
    pub gas: u32,
    pub flash: FlashLoanSource,
    pub should_execute: bool,
    pub reject_reason: Option<String>,
    pub slip_bps: u64,
    /// True when this row is a near-miss (positive net but gate rejected), not a dispatch queue entry.
    pub near_miss: bool,
}

pub struct HfTickResult {
    pub cycles_considered: usize,
    pub profitable_count: usize,
    pub best_profit: U256,
    pub elapsed_ms: u64,
    /// Dispatch queue (and optional single near-miss when queue empty). Cheap summaries only.
    pub candidates: Arc<[HfCandidateUiRow]>,
}

impl Default for HfTickResult {
    fn default() -> Self {
        Self {
            cycles_considered: 0,
            profitable_count: 0,
            best_profit: U256::ZERO,
            elapsed_ms: 0,
            candidates: Arc::from([]),
        }
    }
}

fn hf_eval_to_ui_row(
    arena: &StateArena,
    pool_metas: &[PoolMeta],
    result: &HfEvalResult,
    near_miss: bool,
) -> HfCandidateUiRow {
    HfCandidateUiRow {
        fingerprint: result.route_fingerprint,
        hops: result.cycle.edge_hops(),
        route: near_miss_route_summary(arena, &result.cycle, pool_metas),
        amount_in: result.sim.amount_in,
        amount_out: result.sim.amount_out,
        gross_profit: result.assessment.gross_profit,
        net_profit_matic_wei: result.assessment.net_profit_after_gas_matic_wei,
        gas: result.sim.total_gas,
        flash: result.flash_source,
        should_execute: result.assessment.should_execute,
        reject_reason: result.assessment.reject_reason.clone(),
        slip_bps: result.effective_slippage_bps,
        near_miss,
    }
}

/// Build TUI rows from the post-verify dispatch list (+ optional near-miss when empty).
fn build_hf_candidate_ui_rows(
    arena: &StateArena,
    pool_metas: &[PoolMeta],
    dispatch: &[HfEvalResult],
    near_miss: Option<&HfEvalResult>,
) -> Vec<HfCandidateUiRow> {
    let mut out = Vec::with_capacity(
        dispatch
            .len()
            .saturating_add(usize::from(near_miss.is_some())),
    );
    for result in dispatch {
        out.push(hf_eval_to_ui_row(arena, pool_metas, result, false));
    }
    if out.is_empty()
        && let Some(miss) = near_miss
    {
        out.push(hf_eval_to_ui_row(arena, pool_metas, miss, true));
    }
    out
}

const HF_ACTIVITY_WINDOW_MS: u64 = 15_000;
const HF_SUMMARY_INTERVAL_MS: u64 = 15_000;
const HF_BEST_EVAL_INTERVAL_MS: u64 = 60_000;
/// Rank+Brent is CPU-local; 30s hid hung worker threads and blocked the HF semaphore.
const HF_EVAL_BUDGET: Duration = Duration::from_millis(2_500);
/// Absolute wall for one HF tick (prep + hydrate + eval). Live stream ticks hit
/// 9–10s when residual TickLens ate the full prep and eval had no deadline.
const HF_TICK_HARD_BUDGET: Duration = Duration::from_millis(3_500);
static HF_SUMMARY_LOG_AT: AtomicU64 = AtomicU64::new(0);
static HF_BEST_EVAL_LOG_AT: AtomicU64 = AtomicU64::new(0);
static HF_ORACLE_SKIP_LOG_AT: AtomicU64 = AtomicU64::new(0);
const HF_ORACLE_SKIP_INTERVAL_MS: u64 = 30_000;
/// MATIC/USD refresh can hold singleflight longer than cache TTL; HF may use slightly stale price.
const HF_MATIC_STALE_WARN_MS: u64 = 45_000;
// 750ms timed out on WMATIC cold-cache (live: skip_flash_source=6, fresh=false).
/// Fallback when caller has no pool budget (should be rare).
const HF_FLASH_PREFETCH_BUDGET_MS: u64 = 800;
/// Cap on waiting for another task's flash multicall. Live ticks burned full
/// `HF_PREFETCH_BUDGET_MS` (2.5s) waiting for dust tokens while WMATIC was fresh.
const HF_FLASH_INFLIGHT_WAIT_CAP: Duration = Duration::from_millis(350);
/// Stream path already has WSS state — don't spend the full prep budget on flash.
const HF_STREAM_FLASH_BUDGET_CAP: Duration = Duration::from_millis(400);
/// Skip probe-tick hydrate when residual prep cannot finish one pool.
/// Must match `HF_PROBE_TICK_MS_PER_POOL` (300ms) — 900ms floor carved up to 3 pools.
const HF_PROBE_HYDRATE_MIN_BUDGET: Duration = Duration::from_millis(300);
/// Cap so TickLens cannot burn the whole prep wall; 900ms → up to 3 pools.
const HF_PROBE_HYDRATE_MAX_BUDGET: Duration = Duration::from_millis(900);
/// Stream HF can fire every ~100–200ms; TickLens hydrate must not.
const PROBE_HYDRATE_MIN_GAP_MS: u64 = 1_500;

#[inline]
fn prep_remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

/// Carve hydrate budget out of a shared prep residual so pool/flash/recovery
/// cannot leave < one-pool budget (live: residual 0 → hydrate skip → cl_tickless).
/// Reserves up to MAX when the stage is large enough for multi-pool TickLens.
#[inline]
fn reserve_hydrate_budget(stage: Duration) -> (Duration /* work */, Duration /* hydrate */) {
    if stage >= HF_PROBE_HYDRATE_MIN_BUDGET {
        let hydrate = stage.min(HF_PROBE_HYDRATE_MAX_BUDGET);
        (stage.saturating_sub(hydrate), hydrate)
    } else {
        (stage, Duration::ZERO)
    }
}

/// Hub tokens that block probe ranking when cold (WMATIC ColdCache → empty ranks).
/// Hubs that are cold (missing / past full TTL) — soft-stale hubs (75–100% TTL)
/// must not block HF; spawn/background refreshes them.
fn flash_blocking_stale(stale: &[Address], flash: &FlashLiquidityCache) -> Vec<Address> {
    stale
        .iter()
        .copied()
        .filter(|addr| {
            crate::core::constants::is_polygon_hub_token(*addr) && !flash.has_fresh_entry(*addr)
        })
        .collect()
}

async fn hf_pool_prefetch(
    refresh: &StateRefreshService,
    hot_pools: &[Address],
    prefetch_count: usize,
) -> anyhow::Result<PoolRefreshResult> {
    refresh
        .refresh_pool_states_for(hot_pools, prefetch_count)
        .await
}

async fn hf_flash_prefetch_stale(
    flash_cache: &FlashLiquidityCache,
    rpc: &RpcPool,
    flash_token_list: &[Address],
    budget: Duration,
) {
    if flash_token_list.is_empty() {
        return;
    }
    flash_cache.track_hot_tokens(flash_token_list);
    // Must match flash_liquidity::stale_tokens (75% TTL). Full-TTL has_fresh_entry
    // skipped refresh in the last 7.5s → probe ColdCache on WMATIC (live).
    let mut stale = flash_cache.tokens_needing_refresh(flash_token_list);
    if stale.is_empty() {
        return;
    }
    // Hub tokens first — WMATIC ColdCache emptied probe ranks while dust tokens refreshed.
    stale.sort_by_key(|addr| u8::from(!crate::core::constants::is_polygon_hub_token(*addr)));
    // Share residual HF prep budget (was a hard 2500ms that bloated ticks).
    let flash_budget = if budget.is_zero() {
        Duration::from_millis(HF_FLASH_PREFETCH_BUDGET_MS)
    } else {
        budget
    };
    if flash_budget.is_zero() {
        return;
    }
    let stale_n = stale.len();
    let fresh_n = flash_token_list.len().saturating_sub(stale_n);
    let blocking = flash_blocking_stale(&stale, flash_cache);
    let blocking_n = blocking.len();
    // Dust-only or soft-stale hubs: defer to spawn/background (live: 638ms
    // timeout on tokens=1 WMATIC that was still within full TTL).
    if blocking.is_empty() && stale_n < 4 {
        crate::debug!("flash loan: hf_prefetch defer non-cold stale={stale_n} fresh={fresh_n}");
        return;
    }
    let deadline = Instant::now() + flash_budget;
    let Some(_inflight) = flash_cache.try_acquire_refresh_inflight() else {
        // Background refresh owns the multicall. Only block on hubs — live
        // waited 2500ms for 2/3 dust tokens while WMATIC was already fresh.
        if blocking.is_empty() {
            crate::debug!(
                "flash loan: hf_prefetch skip inflight wait non-hub stale={stale_n} fresh={fresh_n}"
            );
            return;
        }
        let wait_cap = flash_budget.min(HF_FLASH_INFLIGHT_WAIT_CAP);
        let wait_deadline = Instant::now() + wait_cap;
        while Instant::now() < wait_deadline {
            if blocking
                .iter()
                .all(|addr| flash_cache.has_fresh_entry(*addr))
            {
                crate::info!(
                    "flash loan: hf_prefetch waited inflight ok hubs={blocking_n} stale={stale_n} fresh={fresh_n}"
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let still_hubs = blocking
            .iter()
            .filter(|addr| !flash_cache.has_fresh_entry(**addr))
            .count();
        crate::info!(
            "flash loan: hf_prefetch waited inflight timeout_ms={} hub_still={still_hubs}/{blocking_n} stale={stale_n}",
            wait_cap.as_millis(),
        );
        return;
    };
    // Tight budget: refresh hubs only so WMATIC lands; dust rides background.
    let refresh_set: &[Address] =
        if flash_budget <= HF_FLASH_INFLIGHT_WAIT_CAP && !blocking.is_empty() {
            &blocking
        } else {
            &stale
        };
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return;
    }
    match timeout(
        remaining,
        flash_cache.refresh_with_fallback(rpc, refresh_set),
    )
    .await
    {
        Ok(Ok(generation)) => {
            let wmatic = crate::core::constants::WMATIC;
            let wmatic_fresh = flash_cache.has_fresh_entry(wmatic);
            let wmatic_liq = flash_cache.snapshot(wmatic);
            crate::info!(
                "flash loan: hf_prefetch ok stale={stale_n} refresh={} fresh={fresh_n} generation={generation} wmatic_fresh={wmatic_fresh} wmatic_bal={} wmatic_aave={} wmatic_listed={}",
                refresh_set.len(),
                wmatic_liq.balancer,
                wmatic_liq.aave,
                wmatic_liq.aave_listed,
            );
        }
        Ok(Err(e)) => {
            crate::info!("flash loan: hf_prefetch fail stale={stale_n} fresh={fresh_n} err={e:#}");
        }
        Err(_) => crate::info!(
            "flash loan: hf_prefetch timeout_ms={} stale={stale_n} fresh={fresh_n}",
            flash_budget.as_millis(),
        ),
    }
}

fn collect_hf_flash_token_list(
    arena: &StateArena,
    cycles: &[Arc<FoundCycle>],
) -> (FxHashSet<Address>, Vec<Address>) {
    let mut seen = FxHashSet::default();
    let mut list = Vec::new();
    // Always warm WMATIC first — probe ranks ColdCache it when start-rotate /
    // concurrent ticks race past a cycle-only prefetch set.
    for &hub in &crate::core::constants::POLYGON_HUB_TOKENS
        [..crate::core::constants::POLYGON_HUB_TOKENS.len().min(4)]
    {
        if seen.insert(hub) {
            list.push(hub);
        }
    }
    for c in cycles {
        collect_flash_tokens_for_cycle(arena, c.as_ref(), &mut seen, &mut list);
    }
    (seen, list)
}

#[derive(Default)]
pub struct InactiveCycleRotation {
    snapshot_generation: u64,
    next_inactive: usize,
}

impl InactiveCycleRotation {
    fn offset_for(&mut self, snapshot_generation: u64, inactive_len: usize) -> usize {
        if self.snapshot_generation != snapshot_generation {
            self.snapshot_generation = snapshot_generation;
            self.next_inactive = 0;
        }
        if inactive_len == 0 {
            0
        } else {
            self.next_inactive % inactive_len
        }
    }

    fn advance(&mut self, snapshot_generation: u64, inactive_len: usize, served: usize) {
        if self.snapshot_generation == snapshot_generation && inactive_len > 0 {
            self.next_inactive = (self.next_inactive + served) % inactive_len;
        }
    }
}

fn inactive_indices(offset: usize, inactive_len: usize, take: usize) -> Vec<usize> {
    (0..take.min(inactive_len))
        .map(|index| (offset + index) % inactive_len)
        .collect()
}

fn hot_pools_arc_from_set(
    mut hot_pools_set: FxHashSet<Address>,
    partial_cache: &PartialPoolCache,
    stream_triggered: bool,
    stream_enabled: bool,
) -> Arc<[Address]> {
    if stream_triggered && stream_enabled {
        for addr in partial_cache.dirty_addresses() {
            hot_pools_set.insert(addr);
        }
    }
    hot_pools_set.into_iter().collect::<Vec<_>>().into()
}

/// Re-read LF snapshot and rebuild HF cycle/pool selection when generation advanced.
#[allow(clippy::too_many_arguments)]
fn hf_reselect_from_snapshot(
    ctx: &HfContext,
    rescore_cap: usize,
    selection_generation: u64,
    snap_cycle_count: &mut usize,
    token_to_matic_rates: &mut Arc<rustc_hash::FxHashMap<crate::core::types::TokenIndex, U256>>,
    token_decimals: &mut Arc<FxHashMap<alloy::primitives::Address, u8>>,
    pool_metas_for_dispatch: &mut Arc<Vec<PoolMeta>>,
    arena_base: &mut StateArena,
    snap_state_block: &mut u64,
    snap_state_hash: &mut Option<alloy::primitives::B256>,
    cycles: &mut Vec<Arc<FoundCycle>>,
    quarantine_skipped: &mut usize,
    rate_skipped: &mut usize,
    tickless_stuck_skipped: &mut usize,
    protocol_mismatch_skipped: &mut usize,
    v2_dead_skipped: &mut usize,
    micro_dead_skipped: &mut usize,
    bal_floor_dead_skipped: &mut usize,
    inactive_len: &mut usize,
    inactive_selected: &mut usize,
    stream_triggered: bool,
    stream_enabled: bool,
) -> Result<(u64, Arc<[Address]>), HfTickResult> {
    let snap = ctx.snapshots.read();
    *token_to_matic_rates = Arc::clone(&snap.token_to_matic_rates);
    *token_decimals = Arc::clone(&snap.token_decimals);
    *pool_metas_for_dispatch = Arc::clone(&snap.pool_metas);
    *arena_base = snap.arena.clone();
    *snap_cycle_count = snap.cycles.len();
    let new_generation = snap.generation;
    *snap_state_block = snap.state_block;
    *snap_state_hash = snap.state_hash;
    let inactive_offset = ctx
        .inactive_rotation
        .lock()
        .offset_for(new_generation, *snap_cycle_count);
    let selected = select_cycles_for_rescore(
        &snap.cycles,
        arena_base,
        snap.pool_metas.as_ref(),
        &ctx.partial_cache,
        &ctx.execution,
        token_to_matic_rates,
        token_decimals,
        rescore_cap,
        inactive_offset,
    );
    drop(snap);
    // Do not clobber a prior non-empty selection with []. Empty reselect after
    // LF snap advance aborted stream ticks post-filter (live: 72 stream
    // filter logs → 0 stream hf-tick ends; timer ticks alone finished).
    if selected.cycles.is_empty() {
        return Err(HfTickResult {
            cycles_considered: 0,
            profitable_count: 0,
            best_profit: U256::ZERO,
            elapsed_ms: 0,
            candidates: Arc::from([]),
        });
    }
    *cycles = selected.cycles;
    *quarantine_skipped = selected.quarantine_skipped;
    *rate_skipped = selected.rate_skipped;
    *tickless_stuck_skipped = selected.tickless_stuck_skipped;
    *protocol_mismatch_skipped = selected.protocol_mismatch_skipped;
    *v2_dead_skipped = selected.v2_dead_skipped;
    *micro_dead_skipped = selected.micro_dead_skipped;
    *bal_floor_dead_skipped = selected.bal_floor_dead_skipped;
    *inactive_len = selected.inactive_len;
    *inactive_selected = selected.inactive_selected;
    let _ = selection_generation;
    Ok((
        new_generation,
        hot_pools_arc_from_set(
            selected.hot_pools,
            &ctx.partial_cache,
            stream_triggered,
            stream_enabled,
        ),
    ))
}

fn stream_pending_pools(partial_cache: &PartialPoolCache, hot_pools: &[Address]) -> Vec<Address> {
    partial_cache
        .dirty_addresses()
        .into_iter()
        .filter(|address| hot_pools.iter().any(|hot| hot == address))
        .collect()
}

/// Prefer `start_token` when priced; otherwise rotate to the first hop token with a rate.
fn cycle_with_reliable_start(
    cycle: &Arc<FoundCycle>,
    token_to_matic_rates: &rustc_hash::FxHashMap<crate::core::types::TokenIndex, U256>,
) -> Option<Arc<FoundCycle>> {
    if has_reliable_matic_rate(cycle.start_token, token_to_matic_rates) {
        return Some(Arc::clone(cycle));
    }
    for edge in &cycle.edges {
        if has_reliable_matic_rate(edge.token_in, token_to_matic_rates) {
            return rotate_cycle_to_start(cycle, edge.token_in).map(Arc::new);
        }
    }
    None
}

/// True when any start-rotation of `edges` is quarantined. Assess may Aave-rotate
/// after select; `cycle_key` includes hop order + vault idxs so a single fp miss
/// lets underwater cooldowns leak back into `probe_kept` (`evaluated=0`).
///
/// Batches all rotation fingerprints into one quarantine map read.
fn quarantine_all_edge_rotations(execution: &ExecutionService, edges: &[crate::core::types::Edge]) {
    let n = edges.len();
    if n == 0 {
        return;
    }
    let mut rotated = crate::core::types::CycleEdges::from_slice(edges);
    for _ in 0..n {
        execution.quarantine_stale_route(hash_cycle_edges(&rotated));
        rotated.rotate_left(1);
    }
}

fn cycle_edges_quarantined(
    execution: &ExecutionService,
    edges: &[crate::core::types::Edge],
) -> bool {
    let n = edges.len();
    if n == 0 {
        return false;
    }
    // from_slice: Copy-optimal without smallvec `specialization` (servo docs).
    let mut rotated = crate::core::types::CycleEdges::from_slice(edges);
    let mut fps =
        smallvec::SmallVec::<[u64; crate::core::constants::HOP_CAP_USIZE]>::with_capacity(n);
    for _ in 0..n {
        fps.push(hash_cycle_edges(&rotated));
        rotated.rotate_left(1);
    }
    execution.any_quarantined(&fps)
}

fn warn_hf_oracle_skip(message: &str) {
    let now = now_ms();
    let last = HF_ORACLE_SKIP_LOG_AT.load(Ordering::Relaxed);
    if now.saturating_sub(last) < HF_ORACLE_SKIP_INTERVAL_MS {
        return;
    }
    HF_ORACLE_SKIP_LOG_AT.store(now, Ordering::Relaxed);
    crate::warn!("{message}");
}

fn should_log_hf_summary() -> bool {
    let now = now_ms();
    let last = HF_SUMMARY_LOG_AT.load(Ordering::Relaxed);
    if now.saturating_sub(last) < HF_SUMMARY_INTERVAL_MS {
        return false;
    }
    HF_SUMMARY_LOG_AT.store(now, Ordering::Relaxed);
    true
}

fn near_miss_verify_provider(
    rpc: &RpcPool,
    execution_mode: &str,
) -> anyhow::Result<alloy::providers::DynProvider> {
    if crate::config::is_dry_run_mode(execution_mode) {
        rpc.connect_state().or_else(|_| rpc.connect_simulation())
    } else {
        rpc.connect_simulation()
    }
}

fn should_log_best_eval() -> bool {
    let now = now_ms();
    let last = HF_BEST_EVAL_LOG_AT.load(Ordering::Relaxed);
    if now.saturating_sub(last) < HF_BEST_EVAL_INTERVAL_MS {
        return false;
    }
    HF_BEST_EVAL_LOG_AT.store(now, Ordering::Relaxed);
    true
}

struct BestEvalDiag {
    fp: u64,
    hops: u32,
    /// For underwater quarantine of all start-rotations (not just `fp`).
    edges: crate::core::types::CycleEdges,
    route: String,
    /// Full pool addresses (+ V2 reserve snapshot) for sticky-edge diagnosis.
    pools: String,
    input: U256,
    search_low: U256,
    raw_sim_gas: u32,
    assessed_gas: u32,
    gas_basis: &'static str,
    sim_scale_bps: u64,
    gas_base_fee_wei: U256,
    gas_priority_fee_wei: U256,
    gas_snapshot_age_ms: Option<u64>,
    gas_price_gwei: f64,
    gross: U256,
    net_matic: U256,
    gas_cost_wei: U256,
    /// Tokens short of covering gas after flash+slip (0 = at/above breakeven).
    gas_shortfall_tokens: U256,
    /// MATIC-wei short of covering gas (cross-token comparable).
    gas_shortfall_matic_wei: U256,
    /// available_matic / gas_cost in bps (10000 = breakeven before min-profit).
    gas_cover_bps: u64,
    slippage_bps: u64,
    slippage: U256,
    flash_fee: U256,
    reject: Option<String>,
}

fn log_best_eval_diagnostic(diag: &BestEvalDiag) {
    let reason = diag.reject.as_deref().unwrap_or("unknown");
    crate::info!(
        "hf best-eval: fp={} hops={} route={} pools={} input={} search_low={} raw_sim_gas={} assessed_gas={} gas_basis={} sim_scale_bps={} gas_base_fee_wei={} gas_priority_fee_wei={} gas_snapshot_age_ms={:?} gas_price_gwei={:.3} gross={} net_matic={} gas_cost_wei={} gas_shortfall_tokens={} gas_shortfall_matic_wei={} gas_cover_bps={} slippage_bps={} slippage={} flash_fee={} reject={}",
        diag.fp,
        diag.hops,
        diag.route,
        diag.pools,
        diag.input,
        diag.search_low,
        diag.raw_sim_gas,
        diag.assessed_gas,
        diag.gas_basis,
        diag.sim_scale_bps,
        diag.gas_base_fee_wei,
        diag.gas_priority_fee_wei,
        diag.gas_snapshot_age_ms,
        diag.gas_price_gwei,
        diag.gross,
        diag.net_matic,
        diag.gas_cost_wei,
        diag.gas_shortfall_tokens,
        diag.gas_shortfall_matic_wei,
        diag.gas_cover_bps,
        diag.slippage_bps,
        diag.slippage,
        diag.flash_fee,
        reason,
    );
}

/// Max HF slots for activity-hot cycles; remainder stays for quality inactive.
/// Hard ceiling 16: rescore_cap=150 used to admit 50 live slots and leave
/// inactive_selected=0 (live: 187 actives, probe window all WSS dust).
#[must_use]
fn hf_activity_slot_cap(rescore_cap: usize) -> usize {
    rescore_cap.saturating_div(5).clamp(4, 16).min(rescore_cap)
}

fn sample_proto_mismatch(
    arena: &crate::pipeline::arena::StateArena,
    pool_metas: &[PoolMeta],
    cycle: &FoundCycle,
    stage: &'static str,
) {
    use crate::pipeline::types::pool_meta_at;
    use std::sync::atomic::{AtomicU64, Ordering};
    static LAST_MS: AtomicU64 = AtomicU64::new(0);
    let now = now_ms();
    let prev = LAST_MS.load(Ordering::Relaxed);
    if now.saturating_sub(prev) < 2_000 {
        return;
    }
    if LAST_MS
        .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    if let Some((hop, expected, actual)) =
        crate::pipeline::local_sim::first_protocol_state_mismatch(arena, &cycle.edges)
    {
        crate::info!(
            "proto mismatch sample: stage={stage} hop={hop} edge={expected:?} arena={actual:?} hops={}",
            cycle.edges.len()
        );
        return;
    }
    if stage == "meta_match" || stage == "uni_realign" {
        for (hop, edge) in cycle.edges.iter().enumerate() {
            if !matches!(
                edge.protocol,
                ProtocolType::UniswapV2 | ProtocolType::UniswapV3 | ProtocolType::UniswapV4
            ) {
                continue;
            }
            let tin = arena.token_address(edge.token_in);
            let tout = arena.token_address(edge.token_out);
            let Some(m) = pool_meta_at(pool_metas, edge.pool_index) else {
                crate::info!(
                    "proto mismatch sample: stage={stage} hop={hop} tin={tin:?} tout={tout:?} idxs={}/{} meta=None pool={:?}",
                    edge.token_in_idx,
                    edge.token_out_idx,
                    arena.pool_address(edge.pool_index),
                );
                return;
            };
            let meta: Vec<Address> = m
                .tokens
                .iter()
                .filter_map(|&t| arena.token_address(t))
                .collect();
            let tin_ok = tin.is_some_and(|a| meta.contains(&a));
            let tout_ok = tout.is_some_and(|a| meta.contains(&a));
            // Address continuity — TokenIndex inequality false-positives aliases.
            let hop_break = hop > 0
                && cycle.edges.get(hop - 1).is_some_and(|prev| {
                    let po = arena.token_address(prev.token_out);
                    match (po, tin) {
                        (Some(a), Some(b)) => a != b,
                        _ => prev.token_out != edge.token_in,
                    }
                });
            if tin_ok && tout_ok && !hop_break {
                continue;
            }
            let arena_kind = arena
                .pool_state(edge.pool_index)
                .map(|s| match s {
                    crate::core::types::PoolState::V2(_) => "v2",
                    crate::core::types::PoolState::V3(_) => "v3",
                    crate::core::types::PoolState::V4(_) => "v4",
                    crate::core::types::PoolState::Curve(_) => "curve",
                    crate::core::types::PoolState::Balancer(_) => "bal",
                    crate::core::types::PoolState::Dodo(_) => "dodo",
                    crate::core::types::PoolState::Woofi(_) => "woofi",
                    crate::core::types::PoolState::Invalid => "invalid",
                })
                .unwrap_or("none");
            crate::info!(
                "proto mismatch sample: stage={stage} hop={hop} proto={:?} arena={arena_kind} tin={tin:?} tout={tout:?} idxs={}/{} tin_ok={tin_ok} tout_ok={tout_ok} hop_break={hop_break} meta={meta:?} pool={:?}",
                edge.protocol,
                edge.token_in_idx,
                edge.token_out_idx,
                arena.pool_address(edge.pool_index),
            );
            return;
        }
    }
    let hops: String = cycle
        .edges
        .iter()
        .map(|e| format!("{:?}", e.protocol))
        .collect::<Vec<_>>()
        .join(">");
    crate::info!("proto mismatch sample: stage={stage} (no state mismatch) route=[{hops}]");
}

fn sample_multi_realign_fail(arena: &crate::pipeline::arena::StateArena, cycle: &FoundCycle) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static LAST_MS: AtomicU64 = AtomicU64::new(0);
    let now = now_ms();
    let prev = LAST_MS.load(Ordering::Relaxed);
    if now.saturating_sub(prev) < 2_000 {
        return;
    }
    if LAST_MS
        .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    for (hop, edge) in cycle.edges.iter().enumerate() {
        if !matches!(
            edge.protocol,
            ProtocolType::BalancerV2 | ProtocolType::Woofi | ProtocolType::Dodo
        ) {
            continue;
        }
        let tin = arena.token_address(edge.token_in);
        let tout = arena.token_address(edge.token_out);
        let Some(state) = arena.pool_state(edge.pool_index) else {
            crate::info!(
                "multi realign sample: hop={hop} proto={:?} tin={tin:?} tout={tout:?} state=None pool={:?}",
                edge.protocol,
                arena.pool_address(edge.pool_index),
            );
            return;
        };
        let mut probe = *edge;
        if crate::pipeline::local_sim::realign_multi_token_edge(arena, state, &mut probe) {
            continue;
        }
        let vault: Vec<_> = match state {
            crate::core::types::PoolState::Balancer(s) => s.tokens.clone(),
            crate::core::types::PoolState::Woofi(s) => s.tokens.clone(),
            crate::core::types::PoolState::Dodo(s) => vec![s.base_token, s.quote_token],
            _ => Vec::new(),
        };
        crate::info!(
            "multi realign sample: hop={hop} proto={:?} tin={tin:?} tout={tout:?} idxs={}/{} vault={vault:?} pool={:?}",
            edge.protocol,
            edge.token_in_idx,
            edge.token_out_idx,
            arena.pool_address(edge.pool_index),
        );
        return;
    }
    crate::info!(
        "multi realign sample: (no failing multi hop) hops={}",
        cycle.edges.len()
    );
}

fn sample_hop_break(
    arena: &crate::pipeline::arena::StateArena,
    cycle: &FoundCycle,
    break_at: usize,
) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static LAST_MS: AtomicU64 = AtomicU64::new(0);
    let now = now_ms();
    let prev = LAST_MS.load(Ordering::Relaxed);
    if now.saturating_sub(prev) < 2_000 {
        return;
    }
    if LAST_MS
        .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    let Some(prev_e) = cycle.edges.get(break_at) else {
        return;
    };
    let Some(next_e) = cycle.edges.get(break_at + 1) else {
        return;
    };
    let hops: String = cycle
        .edges
        .iter()
        .map(|e| format!("{:?}", e.protocol))
        .collect::<Vec<_>>()
        .join(">");
    crate::info!(
        "hop break sample: at={break_at} prev={:?} out={:?}/{:?} next={:?} in={:?}/{:?} route=[{hops}]",
        prev_e.protocol,
        prev_e.token_out,
        arena.token_address(prev_e.token_out),
        next_e.protocol,
        next_e.token_in,
        arena.token_address(next_e.token_in),
    );
}

fn cycle_activity_score(
    cycle: &FoundCycle,
    arena: &crate::pipeline::arena::StateArena,
    partial_cache: &PartialPoolCache,
    activity_now: u64,
) -> u64 {
    cycle
        .edges
        .iter()
        .filter_map(|edge| arena.pool_address(edge.pool_index))
        .filter_map(|address| partial_cache.get(&address))
        .map(|state| {
            if activity_now.saturating_sub(state.patched_at_ms) <= HF_ACTIVITY_WINDOW_MS {
                // Require activity_count>0 (WSS patch or wake_* stamp). Do not
                // treat seed_from_state_cache's patched_at-only refresh as live —
                // actscore max(1) made ~30 zero-profit "active" cycles crowd the
                // window while sticky inactive V3 still won best-eval.
                state.activity_count
            } else {
                0
            }
        })
        .sum()
}

struct RescoreSelection {
    cycles: Vec<Arc<FoundCycle>>,
    hot_pools: FxHashSet<Address>,
    quarantine_skipped: usize,
    rate_skipped: usize,
    /// Cycles skipped because every tickless CL hop is on HF tick-miss cooldown.
    tickless_stuck_skipped: usize,
    /// Edge protocol disagrees with arena `PoolState` (would be probe UnsupportedState).
    protocol_mismatch_skipped: usize,
    /// Hop-0 UniswapV2 reserve ≤ start-token micro probe (would be probe `v2_reserve`).
    v2_dead_skipped: usize,
    /// Micro-probe `ZeroOutput` / Balancer MAX_IN (would empty-rank as those reasons).
    micro_dead_skipped: usize,
    /// Balancer route infeasible at economic floor (would be Brent `bal_bounds_fail`).
    bal_floor_dead_skipped: usize,
    /// Live-touching cycles culled before activity partition.
    live_drop_proto: usize,
    live_drop_tickless: usize,
    live_drop_quarantine: usize,
    live_drop_multi: usize,
    live_drop_rate: usize,
    activity_candidates: usize,
    activity_selected: usize,
    inactive_len: usize,
    inactive_selected: usize,
}

#[allow(clippy::too_many_arguments)]
fn select_cycles_for_rescore(
    snap_cycles: &[Arc<FoundCycle>],
    arena: &crate::pipeline::arena::StateArena,
    pool_metas: &[PoolMeta],
    partial_cache: &PartialPoolCache,
    execution: &ExecutionService,
    token_to_matic_rates: &rustc_hash::FxHashMap<crate::core::types::TokenIndex, U256>,
    token_decimals: &rustc_hash::FxHashMap<Address, u8>,
    rescore_cap: usize,
    inactive_offset: usize,
) -> RescoreSelection {
    if rescore_cap == 0 {
        return RescoreSelection {
            cycles: Vec::new(),
            hot_pools: FxHashSet::default(),
            quarantine_skipped: 0,
            rate_skipped: 0,
            tickless_stuck_skipped: 0,
            protocol_mismatch_skipped: 0,
            v2_dead_skipped: 0,
            micro_dead_skipped: 0,
            bal_floor_dead_skipped: 0,
            live_drop_proto: 0,
            live_drop_tickless: 0,
            live_drop_quarantine: 0,
            live_drop_multi: 0,
            live_drop_rate: 0,
            activity_candidates: 0,
            activity_selected: 0,
            inactive_len: 0,
            inactive_selected: 0,
        };
    }

    let activity_now = now_ms();
    // Partition during the filter pass: actives (score>0) and inactives separately so we
    // never full-sort the combined list. Hot routes are activity-first; cold routes rotate.
    let mut active: Vec<(Arc<FoundCycle>, u64)> =
        Vec::with_capacity(snap_cycles.len().min(rescore_cap.saturating_mul(2)));
    let mut inactive: Vec<Arc<FoundCycle>> = Vec::with_capacity(snap_cycles.len());
    let mut quarantine_skipped = 0usize;
    let mut rate_skipped = 0usize;
    let mut tickless_stuck_skipped = 0usize;
    let mut protocol_mismatch_skipped = 0usize;
    let mut v2_dead_skipped = 0usize;
    let mut micro_dead_skipped = 0usize;
    let mut bal_floor_dead_skipped = 0usize;
    let mut live_drop_proto = 0usize;
    let mut live_drop_tickless = 0usize;
    let mut live_drop_quarantine = 0usize;
    let mut live_drop_multi = 0usize;
    let mut live_drop_rate = 0usize;
    // Pools from live cycles culled by proto gates — keep in hot set so
    // dirty∩sel can land and stream ticks prefetch instead of skip.
    let mut live_anchor_hot = FxHashSet::with_capacity_and_hasher(32, FxBuildHasher);
    let anchor_live = |edges: &[Edge], hot: &mut FxHashSet<Address>| {
        for edge in edges {
            if let Some(addr) = arena.pool_address(edge.pool_index) {
                hot.insert(addr);
            }
        }
    };
    for cycle in snap_cycles {
        // Pool set unchanged by heal/realign — cheap live flag for drop staging.
        let live = cycle_activity_score(cycle.as_ref(), arena, partial_cache, activity_now) > 0;
        // Skip only when every tickless CL hop is on miss cooldown. Skipping any
        // CL pool on cooldown (even with LF ticks) wiped the HF window
        // (`selected=0`, tickless_stuck≫candidates) whenever a hub pool missed.
        // Live WSS: do not tickless-cull. Miss cooldowns wiped stream ticks
        // (`selected=0`, hot_pools=0) while wake_hf=true; hydrate+probe still useful.
        if cycle_tickless_cl_all_on_miss_cooldown(arena, cycle.as_ref(), pool_metas) {
            if live {
                live_drop_tickless += 1; // counted, still admitted
            } else {
                tickless_stuck_skipped += 1;
                continue;
            }
        }
        // Cheap reject before remap/heal — raw fingerprint still covers cooldowns
        // stamped before vault-idx realign. Remapped fps checked after heal.
        if cycle_edges_quarantined(execution, &cycle.edges) {
            quarantine_skipped += 1;
            if live {
                // Anchor for stream wake, but do not probe — livehold was re-admitting
                // chronic underwater dust as best-eval (same fp ~369 cover forever).
                live_drop_quarantine += 1;
                anchor_live(&cycle.edges, &mut live_anchor_hot);
            }
            continue;
        }
        let Some(ready) = cycle_with_reliable_start(cycle, token_to_matic_rates) else {
            rate_skipped += 1;
            if live {
                live_drop_rate += 1;
            }
            continue;
        };
        // Recover Balancer/Woofi vault-index skew (meta vs getPoolTokens) before
        // quarantine/micro-dead — otherwise we only prune recoverable liquidity.
        let pre_multi = Arc::clone(&ready);
        let ready = match crate::pipeline::local_sim::realign_multi_token_found_cycle(arena, ready)
        {
            Some(ready) => ready,
            None if live => {
                // livehold: vault token absent/unroutable on cold arena wiped
                // live_touch→active (live: touch=22 drop_multi=19). Probe still
                // TokenMismatch-filters; keep the WSS-touched cycle in-window.
                live_drop_multi += 1;
                sample_multi_realign_fail(arena, pre_multi.as_ref());
                pre_multi
            }
            None => {
                micro_dead_skipped += 1;
                sample_multi_realign_fail(arena, pre_multi.as_ref());
                // Stale Balancer/Woofi edges (tokens ∉ vault) refill every tick as
                // micro_dead~100 — cool all rotations so select's quarantine gate bites.
                quarantine_all_edge_rotations(execution, &pre_multi.edges);
                continue;
            }
        };
        // Cached-cycle protocol tags can lag hot-cache family flips (V2→V3).
        let Some(ready) = crate::pipeline::local_sim::heal_cycle_edge_protocols(arena, ready)
        else {
            protocol_mismatch_skipped += 1;
            if live {
                live_drop_proto += 1;
                // Anchor only — do not quarantine (cools wipe the next LF pin window).
                anchor_live(&cycle.edges, &mut live_anchor_hot);
            } else {
                // Family-poisoned edges refill every tick until LF layout fp invalidates
                // the cycle cache (live: BalancerV2×V3 heal storms).
                quarantine_all_edge_rotations(execution, &cycle.edges);
            }
            sample_proto_mismatch(arena, pool_metas, cycle.as_ref(), "heal");
            continue;
        };
        // Remap stale Uni TokenIndex endpoints from PoolMeta legs before reject.
        let healed = ready;
        let ready = match crate::pipeline::local_sim::realign_uni_cycle_from_pool_meta(
            arena,
            pool_metas,
            Arc::clone(&healed),
        ) {
            Some(c) => c,
            None if live
                && crate::pipeline::local_sim::first_hop_continuity_break_in_arena(
                    arena,
                    &healed.edges,
                )
                .is_none()
                && crate::pipeline::local_sim::cycle_edges_match_arena_state(
                    arena,
                    &healed.edges,
                ) =>
            {
                // ponytail: match LF obs bypass — meta TokenIndex drift with
                // continuous arena hops (live: soft_keep pins → live_drop_proto).
                healed
            }
            None => {
                // Never livehold into probe: mismatch burns the window. Anchor pools
                // so stream dirty∩sel can prefetch instead of skip.
                protocol_mismatch_skipped += 1;
                if live {
                    live_drop_proto += 1;
                    anchor_live(&healed.edges, &mut live_anchor_hot);
                } else {
                    quarantine_all_edge_rotations(execution, &healed.edges);
                }
                sample_proto_mismatch(arena, pool_metas, healed.as_ref(), "uni_realign");
                continue;
            }
        };
        if !crate::pipeline::local_sim::cycle_edges_match_arena_state(arena, &ready.edges) {
            protocol_mismatch_skipped += 1;
            if live {
                live_drop_proto += 1;
                anchor_live(&ready.edges, &mut live_anchor_hot);
            } else {
                quarantine_all_edge_rotations(execution, &ready.edges);
            }
            sample_proto_mismatch(arena, pool_metas, ready.as_ref(), "arena_match");
            continue;
        }
        // Stale V2/V3/V4 TokenIndex vs refreshed PoolMeta — sim invents profit.
        if !crate::pipeline::local_sim::cycle_v2_edges_match_pool_meta(
            arena,
            pool_metas,
            &ready.edges,
        ) {
            protocol_mismatch_skipped += 1;
            if live {
                live_drop_proto += 1;
                anchor_live(&ready.edges, &mut live_anchor_hot);
            } else {
                quarantine_all_edge_rotations(execution, &ready.edges);
            }
            sample_proto_mismatch(arena, pool_metas, ready.as_ref(), "meta_match");
            continue;
        }
        // Probe `InvalidRoute` is hop TokenIndex discontinuity — cull here so sticky
        // Balancer mixes never burn the probe window (live: invalid=scanned).
        if let Some(break_at) =
            crate::pipeline::local_sim::first_hop_continuity_break_in_arena(arena, &ready.edges)
        {
            protocol_mismatch_skipped += 1;
            if live {
                live_drop_proto += 1;
                anchor_live(&ready.edges, &mut live_anchor_hot);
            } else {
                quarantine_all_edge_rotations(execution, &ready.edges);
            }
            sample_hop_break(arena, ready.as_ref(), break_at);
            continue;
        }
        // Quarantine keys are assess/best-eval fingerprints (post start-rotation /
        // vault-idx remap). Raw already checked; only remapped rotations remain.
        if cycle_edges_quarantined(execution, &ready.edges) {
            quarantine_skipped += 1;
            if live {
                live_drop_quarantine += 1;
                anchor_live(&ready.edges, &mut live_anchor_hot);
            }
            continue;
        }
        // Structural V2 dust (either reserve < 1e6 wei) — apply before live bypass.
        // Activity path skips micro/economic probes; mid-route dust still ranked
        // (live: 0x5efc r1=45433 → best-eval cover≈2348).
        if crate::pipeline::local_sim::v2_any_hop_dust_reserves(arena, &ready.edges).is_some() {
            v2_dead_skipped += 1;
            continue;
        }
        // Activity first: live-touching cycles were dying as micro_dead before
        // score (livehold: cycles_touching=22 → selected=4 active=0). Fresh WSS
        // moves can look shallow on stale local sim — still probe them.
        let score = cycle_activity_score(ready.as_ref(), arena, partial_cache, activity_now);
        let start_decimals =
            resolve_token_decimals_for_index(ready.start_token, arena, token_decimals);
        let micro_probe = if start_decimals >= 6 {
            crate::util::ten_pow_u256_cached(start_decimals - 6)
        } else {
            U256::from(1u64)
        };
        let start_rate = resolve_token_to_matic_rate(ready.start_token, token_to_matic_rates);
        let economic_floor = min_economic_amount_in(start_decimals, start_rate);
        // Walk succeeds with gross==0 at micro+floor → spot phantoms. Apply before
        // the live bypass so WSS-touched zero_profit cannot fill the probe window
        // (live: active_selected=50 inactive=0 with 187 actives). Skip 1-hop
        // stubs (tests / incomplete cycles) — need a closed path to judge profit.
        if ready.edges.len() >= 2
            && let Some(micro) =
                crate::pipeline::local_sim::simulate_route_minimal(arena, &ready.edges, micro_probe)
            && micro.profit.is_zero()
        {
            let floor_zero = crate::pipeline::local_sim::simulate_route_minimal(
                arena,
                &ready.edges,
                economic_floor,
            )
            .is_none_or(|s| s.profit.is_zero());
            if floor_zero {
                micro_dead_skipped += 1;
                quarantine_all_edge_rotations(execution, &ready.edges);
                continue;
            }
        }
        if score > 0 {
            // Live WSS: do not micro/bal-floor prune. Stale local sim looked shallow
            // and wiped live_touch→active (live: touch=38 active=0). Proto/heal gates
            // above already dropped UnsupportedState; hydrate+probe filter dust.
            active.push((ready, score));
            continue;
        }
        // Insane gross at micro or economic floor → probe `sanity` phantoms
        // (residual after micro-only prune was mostly InsaneProfitMatic).
        if crate::pipeline::local_sim::probe_insane_gross_phantom(
            arena,
            &ready.edges,
            micro_probe,
            start_decimals,
            start_rate,
        ) || crate::pipeline::local_sim::probe_insane_gross_phantom(
            arena,
            &ready.edges,
            economic_floor,
            start_decimals,
            start_rate,
        ) {
            micro_dead_skipped += 1;
            continue;
        }
        // Drop hop-0 dust V2 before they crowd the HF probe window (live empty
        // ranks were ~75% `v2_reserve` after unsupported cleared).
        if crate::pipeline::local_sim::first_v2_hop_below_reserve(arena, &ready.edges, micro_probe)
            .is_some()
        {
            v2_dead_skipped += 1;
            continue;
        }
        if crate::pipeline::local_sim::micro_probe_liquidity_dead(arena, &ready.edges, micro_probe)
            .is_some()
        {
            micro_dead_skipped += 1;
            continue;
        }
        // After rank probe skips below-floor dust, micro-only survivors fail as
        // v2_reserve/shallow_cl/bal_max_in — prune at economic floor here.
        match crate::pipeline::local_sim::economic_floor_liquidity_dead(
            arena,
            &ready.edges,
            economic_floor,
        ) {
            Some(crate::pipeline::local_sim::MinimalSimFailure::BalancerMaxInRatio { .. }) => {
                bal_floor_dead_skipped += 1;
                continue;
            }
            Some(crate::pipeline::local_sim::MinimalSimFailure::V2ReserveExhausted { .. }) => {
                v2_dead_skipped += 1;
                continue;
            }
            Some(_) => {
                micro_dead_skipped += 1;
                continue;
            }
            None => {}
        }
        inactive.push(ready);
    }

    let activity_candidates = active.len();
    let inactive_len = inactive.len();
    if activity_candidates == 0 && inactive_len == 0 {
        return RescoreSelection {
            cycles: Vec::new(),
            hot_pools: live_anchor_hot,
            quarantine_skipped,
            rate_skipped,
            tickless_stuck_skipped,
            protocol_mismatch_skipped,
            v2_dead_skipped,
            micro_dead_skipped,
            bal_floor_dead_skipped,
            live_drop_proto,
            live_drop_tickless,
            live_drop_quarantine,
            live_drop_multi,
            live_drop_rate,
            activity_candidates: 0,
            activity_selected: 0,
            inactive_len: 0,
            inactive_selected: 0,
        };
    }

    // Prefer cycle quality among live routes so the 16-slot cap surfaces
    // high cycle_ratio WSS edges instead of highest activity_count dust.
    active.sort_by(|a, b| {
        compare_cycle_score(a.0.as_ref(), b.0.as_ref()).then_with(|| b.1.cmp(&a.1))
    });
    // Inactive: quality order so rotation windows still prefer stronger cycle_ratio first.
    inactive.sort_by(|a, b| compare_cycle_score(a.as_ref(), b.as_ref()));

    // Cap live/active slots — actscore filled rescore_cap with seed-stamped
    // zero_profit cycles (probe_kept=0 on 30/31) and starved quality inactive
    // near_net / sticky absolute-gross candidates.
    let activity_selected = activity_candidates.min(hf_activity_slot_cap(rescore_cap));
    let inactive_slots = rescore_cap.saturating_sub(activity_selected);
    let inactive_selected = inactive_slots.min(inactive_len);

    let mut cycles = Vec::with_capacity(activity_selected + inactive_selected);
    let mut hot_pools = FxHashSet::with_capacity_and_hasher(
        (activity_selected + inactive_selected).saturating_mul(3),
        FxBuildHasher,
    );

    for (cycle, _) in active.into_iter().take(activity_selected) {
        for edge in &cycle.edges {
            if let Some(addr) = arena.pool_address(edge.pool_index) {
                hot_pools.insert(addr);
            }
        }
        cycles.push(cycle);
    }
    for index in inactive_indices(inactive_offset, inactive_len, inactive_selected) {
        let cycle = Arc::clone(&inactive[index]);
        for edge in &cycle.edges {
            if let Some(addr) = arena.pool_address(edge.pool_index) {
                hot_pools.insert(addr);
            }
        }
        cycles.push(cycle);
    }
    hot_pools.extend(live_anchor_hot);

    RescoreSelection {
        cycles,
        hot_pools,
        quarantine_skipped,
        rate_skipped,
        tickless_stuck_skipped,
        protocol_mismatch_skipped,
        v2_dead_skipped,
        micro_dead_skipped,
        bal_floor_dead_skipped,
        live_drop_proto,
        live_drop_tickless,
        live_drop_quarantine,
        live_drop_multi,
        live_drop_rate,
        activity_candidates,
        activity_selected,
        inactive_len,
        inactive_selected,
    }
}

pub async fn run_hf_tick(
    ctx: Arc<HfContext>,
    mut stream_triggered: bool,
) -> anyhow::Result<HfTickResult> {
    // Re-latch WSS notifies at tick start so a false schedule race cannot
    // drop stream mode after promote already observed patches.
    if ctx.partial_cache.trigger().take_stream_triggered() {
        stream_triggered = true;
    }
    if ctx.refresh.is_indexer_stale() && ctx.config.pipeline.indexer_pause_on_lag {
        ctx.refresh.maybe_refresh_indexer_health().await;
        if ctx.refresh.is_indexer_stale() {
            crate::services::index_diag::record_indexer_stale_gate();
            crate::warn!("hf tick skipped: indexer lag exceeds threshold");
            return Ok(HfTickResult {
                cycles_considered: 0,
                profitable_count: 0,
                best_profit: U256::ZERO,
                elapsed_ms: 0,
                candidates: Arc::from([]),
            });
        }
    }

    let start = now_ms();
    let pipeline = &ctx.config.pipeline;
    let rescore_cap = pipeline.hf_score_cap;
    let sim_cap = pipeline.hf_sim_cap;

    let snap = ctx.snapshots.read();
    if snap.cycles.is_empty() {
        return Ok(HfTickResult {
            cycles_considered: 0,
            profitable_count: 0,
            best_profit: U256::ZERO,
            elapsed_ms: now_ms().saturating_sub(start),
            candidates: Arc::from([]),
        });
    }

    let snap_generation = snap.generation;
    let mut token_to_matic_rates = Arc::clone(&snap.token_to_matic_rates);
    let mut token_decimals = Arc::clone(&snap.token_decimals);
    let mut pool_metas_for_dispatch = Arc::clone(&snap.pool_metas);
    let mut arena_base = snap.arena.clone();
    let mut snap_cycle_count = snap.cycles.len();
    let mut selection_generation = snap_generation;
    let inactive_offset = ctx
        .inactive_rotation
        .lock()
        .offset_for(selection_generation, snap_cycle_count);
    let selection = select_cycles_for_rescore(
        &snap.cycles,
        &arena_base,
        pool_metas_for_dispatch.as_ref(),
        &ctx.partial_cache,
        &ctx.execution,
        &token_to_matic_rates,
        &token_decimals,
        rescore_cap,
        inactive_offset,
    );
    let mut cycles = selection.cycles;
    let hot_pools_set = selection.hot_pools;
    let mut quarantine_skipped = selection.quarantine_skipped;
    let mut rate_skipped = selection.rate_skipped;
    let mut tickless_stuck_skipped = selection.tickless_stuck_skipped;
    let mut protocol_mismatch_skipped = selection.protocol_mismatch_skipped;
    let mut v2_dead_skipped = selection.v2_dead_skipped;
    let mut micro_dead_skipped = selection.micro_dead_skipped;
    let mut bal_floor_dead_skipped = selection.bal_floor_dead_skipped;
    let live_drop_proto = selection.live_drop_proto;
    let live_drop_tickless = selection.live_drop_tickless;
    let live_drop_quarantine = selection.live_drop_quarantine;
    let live_drop_multi = selection.live_drop_multi;
    let live_drop_rate = selection.live_drop_rate;
    let activity_candidates = selection.activity_candidates;
    let activity_selected = selection.activity_selected;
    let mut inactive_len = selection.inactive_len;
    let mut inactive_selected = selection.inactive_selected;
    let mut snap_state_block = snap.state_block;
    let mut snap_state_hash = snap.state_hash;
    drop(snap);
    // Throttle only — stream_triggered used to bypass and spam INFO every wssobs tick.
    let log_hf_summary = should_log_hf_summary();
    if log_hf_summary {
        // live_touch: snap cycles with recent WSS activity (ignores micro/bal filters).
        let live_touch = if activity_candidates == 0 {
            let now = now_ms();
            let snap = ctx.snapshots.read();
            snap.cycles
                .iter()
                .take(256)
                .filter(|c| {
                    cycle_activity_score(c.as_ref(), &arena_base, &ctx.partial_cache, now) > 0
                })
                .count()
        } else {
            activity_candidates
        };
        crate::info!(
            "hf cycle filter: snap={snap_cycle_count} selected={} stream_triggered={} quarantine_skip={quarantine_skipped} rate_skip={rate_skipped} tickless_stuck_skip={tickless_stuck_skipped} proto_mismatch_skip={protocol_mismatch_skipped} v2_dead_skip={v2_dead_skipped} micro_dead_skip={micro_dead_skipped} bal_floor_dead_skip={bal_floor_dead_skipped} active_candidates={activity_candidates} live_touch={live_touch} live_drop_proto={live_drop_proto} live_drop_tickless={live_drop_tickless} live_drop_quarantine={live_drop_quarantine} live_drop_multi={live_drop_multi} live_drop_rate={live_drop_rate} active_selected={activity_selected} inactive_candidates={inactive_len} inactive_selected={inactive_selected} inactive_offset={inactive_offset} hot_pools={} rescore_cap={rescore_cap}",
            cycles.len(),
            u8::from(stream_triggered),
            hot_pools_set.len(),
        );
    } else if stream_triggered {
        // Every 2–3ms stream tick was INFO-logging here and saturating the
        // 8k log queue — hf tick ends / evals were dropped (live: 93 filters,
        // 47 dropped_events storms, only 2 stream tick-end lines survived).
        static STREAM_FILTER_LOG_AT: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let now = now_ms();
        let prev = STREAM_FILTER_LOG_AT.load(Ordering::Relaxed);
        if now.saturating_sub(prev) >= 2_000
            && STREAM_FILTER_LOG_AT
                .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            crate::info!(
                "hf cycle filter: snap={snap_cycle_count} selected={} stream_triggered=1 active_candidates={activity_candidates} active_selected={activity_selected} inactive_selected={inactive_selected} hot_pools={}",
                cycles.len(),
                hot_pools_set.len(),
            );
        }
    }
    if cycles.is_empty() {
        if log_hf_summary {
            crate::info!(
                "hf tick: 0 cycles after filter (snap={snap_cycle_count}, quarantine={quarantine_skipped}, no_rate={rate_skipped}, stream_triggered={stream_triggered})"
            );
        } else if stream_triggered {
            crate::info!(
                "hf tick: 0 cycles after filter (snap={snap_cycle_count}, quarantine={quarantine_skipped}, stream_triggered=1)"
            );
        }
        // Do not re-notify here — that looped promote storms with selected=0
        // while topic spam never entered the arena universe (V2 disabled).
        return Ok(HfTickResult {
            cycles_considered: 0,
            profitable_count: 0,
            best_profit: U256::ZERO,
            elapsed_ms: now_ms().saturating_sub(start),
            candidates: Arc::from([]),
        });
    }
    // Stream wakes with no live-scoring cycles just re-eval sticky inactive
    // dust in 2–3ms (live: 44 stream ticks, active=0). Leave inactive
    // rotation for the timer path — unless dirty hits anchored/selected pools
    // (prefetch can heal meta for the next LF pin).
    if stream_triggered && activity_candidates == 0 {
        let dirty = ctx.partial_cache.dirty_addresses();
        let overlap = dirty.iter().filter(|a| hot_pools_set.contains(*a)).count();
        // Observed venues land in stream_universe before they appear in the
        // sticky selected set (live: dirty_in_sel=0 while dirty∩universe>0).
        let dirty_in_universe = dirty
            .iter()
            .filter(|a| ctx.partial_cache.in_stream_universe(a))
            .count();
        if overlap == 0 && dirty_in_universe == 0 {
            static SKIP_LOG_AT: AtomicU64 = AtomicU64::new(0);
            let now = now_ms();
            let prev = SKIP_LOG_AT.load(Ordering::Relaxed);
            if now.saturating_sub(prev) >= 5_000
                && SKIP_LOG_AT
                    .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                crate::info!(
                    "hf tick skipped: stream wake with no active cycles (selected={} dirty={} sel_hot={} dirty_in_sel={} dirty_in_uni=0)",
                    cycles.len(),
                    dirty.len(),
                    hot_pools_set.len(),
                    overlap,
                );
            }
            return Ok(HfTickResult {
                cycles_considered: 0,
                profitable_count: 0,
                best_profit: U256::ZERO,
                elapsed_ms: now_ms().saturating_sub(start),
                candidates: Arc::from([]),
            });
        }
        // Universe-only dirty: sticky inactive selected rarely touches those
        // venues (live: dirty_in_uni=10 eval sticky dust). Pull snap cycles
        // that actually include the dirty pools; else skip.
        if overlap == 0 && dirty_in_universe > 0 {
            let dirty_uni: FxHashSet<Address> = dirty
                .iter()
                .copied()
                .filter(|a| ctx.partial_cache.in_stream_universe(a))
                .collect();
            let snap = ctx.snapshots.read();
            let mut prefer = Vec::with_capacity(rescore_cap.min(32));
            for c in snap.cycles.iter() {
                let touches = c.edges.iter().any(|e| {
                    arena_base
                        .pool_address(e.pool_index)
                        .is_some_and(|a| dirty_uni.contains(&a))
                });
                if !touches {
                    continue;
                }
                prefer.push(Arc::clone(c));
                if prefer.len() >= rescore_cap.min(32) {
                    break;
                }
            }
            drop(snap);
            if prefer.is_empty() {
                static SKIP_UNI_AT: AtomicU64 = AtomicU64::new(0);
                let now = now_ms();
                let prev = SKIP_UNI_AT.load(Ordering::Relaxed);
                if now.saturating_sub(prev) >= 5_000
                    && SKIP_UNI_AT
                        .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok()
                {
                    crate::info!(
                        "hf tick skipped: dirty_in_uni={dirty_in_universe} but no snap cycle touches dirty (selected={} dirty={})",
                        cycles.len(),
                        dirty.len(),
                    );
                }
                return Ok(HfTickResult {
                    cycles_considered: 0,
                    profitable_count: 0,
                    best_profit: U256::ZERO,
                    elapsed_ms: now_ms().saturating_sub(start),
                    candidates: Arc::from([]),
                });
            }
            crate::info!(
                "hf tick: stream wake dirty_in_uni={dirty_in_universe} — swapped in {} dirty-touching snap cycles (was selected={})",
                prefer.len(),
                cycles.len(),
            );
            cycles = prefer;
        } else {
            crate::info!(
                "hf tick: stream wake active=0 dirty_in_sel={overlap} dirty_in_uni={dirty_in_universe} — prefetch path (selected={} dirty={} sel_hot={})",
                cycles.len(),
                dirty.len(),
                hot_pools_set.len(),
            );
        }
    }

    let mut hot_pools = hot_pools_arc_from_set(
        hot_pools_set,
        &ctx.partial_cache,
        stream_triggered,
        pipeline.stream_enabled,
    );

    let Some(gas_snapshot) = ctx.gas_oracle.loaded_snapshot() else {
        crate::warn!("hf tick skipped: gas oracle has no fee snapshot yet");
        return Ok(HfTickResult {
            cycles_considered: 0,
            profitable_count: 0,
            best_profit: U256::ZERO,
            elapsed_ms: now_ms().saturating_sub(start),
            candidates: Arc::from([]),
        });
    };
    // Spot tip for ranking/assess; submit still uses compute_conservative_gas_price.
    let gas_price = compute_assessment_gas_price(gas_snapshot);
    let gas_snapshot_age_ms = ctx.gas_oracle.snapshot_age_ms();

    if ctx.snapshots.generation() != selection_generation {
        match hf_reselect_from_snapshot(
            &ctx,
            rescore_cap,
            selection_generation,
            &mut snap_cycle_count,
            &mut token_to_matic_rates,
            &mut token_decimals,
            &mut pool_metas_for_dispatch,
            &mut arena_base,
            &mut snap_state_block,
            &mut snap_state_hash,
            &mut cycles,
            &mut quarantine_skipped,
            &mut rate_skipped,
            &mut tickless_stuck_skipped,
            &mut protocol_mismatch_skipped,
            &mut v2_dead_skipped,
            &mut micro_dead_skipped,
            &mut bal_floor_dead_skipped,
            &mut inactive_len,
            &mut inactive_selected,
            stream_triggered,
            pipeline.stream_enabled,
        ) {
            Ok((new_gen, hot)) => {
                selection_generation = new_gen;
                hot_pools = hot;
            }
            Err(_) if cycles.is_empty() => {
                if should_log_hf_summary() || stream_triggered {
                    crate::info!(
                        "hf tick: 0 cycles after refresh (snap={snap_cycle_count}, quarantine={quarantine_skipped}, no_rate={rate_skipped}, stream_triggered={})",
                        u8::from(stream_triggered),
                    );
                }
                return Ok(HfTickResult {
                    cycles_considered: 0,
                    profitable_count: 0,
                    best_profit: U256::ZERO,
                    elapsed_ms: now_ms().saturating_sub(start),
                    candidates: Arc::from([]),
                });
            }
            Err(_) => {
                // Prior selection intact — empty reselect must not kill the tick.
                crate::info!(
                    "hf reselect empty: keeping prior selected={} stream_triggered={}",
                    cycles.len(),
                    u8::from(stream_triggered),
                );
            }
        }
    }

    let prefetch_count = pipeline.hf_prefetch_count.min(hot_pools.len().max(1));
    let skip_prefetch = stream_triggered && pipeline.stream_enabled;
    let mut prefetch_ok = skip_prefetch;
    // One shared prep wall for recovery + pool + flash + probe hydrate.
    // Stages used to each take HF_PREFETCH_BUDGET_MS (live: pool 194 + probe 2501 = 2.7s).
    let prep_budget = Duration::from_millis(pipeline.hf_prefetch_budget_ms.max(1));
    let prep_deadline = Instant::now() + prep_budget;
    // Whole-tick wall (prep can be 2.5s; leave room for eval without 10s tails).
    let tick_deadline = Instant::now() + HF_TICK_HARD_BUDGET;
    let pool_prefetch_started = now_ms();

    if stream_triggered && pipeline.stream_enabled {
        let flushed = ctx
            .partial_cache
            .flush_to_state_cache(&ctx.cache, hot_pools.as_ref());
        let pending_pools = stream_pending_pools(&ctx.partial_cache, hot_pools.as_ref());
        if !pending_pools.is_empty() {
            crate::info!(
                "stream state catch-up: flushed={flushed} unseeded_hot={} — refreshing before HF eval",
                pending_pools.len(),
            );
            // Do not burn the hydrate floor on recovery (live: recovery 2.5s →
            // residual 0 → cl_tickless, or full residual TickLens timeout 2.5s).
            let (recovery_budget, _) = reserve_hydrate_budget(prep_remaining(prep_deadline));
            let recovery_budget = recovery_budget.min(prep_remaining(tick_deadline));
            if recovery_budget.is_zero() {
                crate::warn!(
                    "stream recovery skipped: no prep after hydrate reserve (pending={})",
                    pending_pools.len()
                );
            } else {
                match timeout(
                    recovery_budget,
                    ctx.refresh
                        .refresh_pool_states_for(&pending_pools, pending_pools.len()),
                )
                .await
                {
                    Ok(Ok(result)) => {
                        let recovered = ctx
                            .partial_cache
                            .flush_to_state_cache(&ctx.cache, &pending_pools);
                        let remaining =
                            stream_pending_pools(&ctx.partial_cache, hot_pools.as_ref());
                        if remaining.is_empty() {
                            prefetch_ok = result.prefetch_tick_succeeded();
                            crate::info!(
                                "stream state recovery: refreshed={} flushed={recovered}",
                                pending_pools.len()
                            );
                        } else {
                            crate::warn!(
                                "hf tick skipped: stream state recovery incomplete (remaining={})",
                                remaining.len()
                            );
                            return Ok(HfTickResult {
                                cycles_considered: cycles.len(),
                                profitable_count: 0,
                                best_profit: U256::ZERO,
                                elapsed_ms: now_ms().saturating_sub(start),
                                candidates: Arc::from([]),
                            });
                        }
                    }
                    Ok(Err(e)) => {
                        crate::warn!("hf tick skipped: stream state recovery failed: {e:#}");
                        return Ok(HfTickResult {
                            cycles_considered: cycles.len(),
                            profitable_count: 0,
                            best_profit: U256::ZERO,
                            elapsed_ms: now_ms().saturating_sub(start),
                            candidates: Arc::from([]),
                        });
                    }
                    Err(_) => {
                        crate::warn!(
                            "hf tick skipped: stream state recovery timed out after {}ms",
                            recovery_budget.as_millis()
                        );
                        return Ok(HfTickResult {
                            cycles_considered: cycles.len(),
                            profitable_count: 0,
                            best_profit: U256::ZERO,
                            elapsed_ms: now_ms().saturating_sub(start),
                            candidates: Arc::from([]),
                        });
                    }
                }
            }
        } else if flushed > 0 {
            prefetch_ok = true;
        }
    }

    let mut pool_prefetch_ms = 0;
    let mut flash_prefetch_ms = 0;
    let flash_token_list = collect_hf_flash_token_list(&arena_base, &cycles).1;
    let flash_cache = Arc::clone(&ctx.execution.flash_liquidity);
    let rpc = Arc::clone(&ctx.rpc);
    let refresh = Arc::clone(&ctx.refresh);
    // Residual prep budget for pool/flash (shared deadline — no stage stacking).
    let stage_budget = prep_remaining(prep_deadline).min(prep_remaining(tick_deadline));
    // Reserve hydrate floor from *both* pool and flash. Live: pool_budget used the
    // full stage (flash alone reserved 1.4s) → residual 0 after a 1.3s multicall →
    // hydrate skipped → cl_tickless/shallow_cl emptied probe_kept.
    let (work_budget, hydrate_reserved) = reserve_hydrate_budget(stage_budget);
    // Preserve the carved floor as an absolute budget — do not re-derive from
    // prep_deadline residual later (oracle/reselect ate it → hydrate never ran).
    let hydrate_floor = hydrate_reserved;
    let flash_budget = if skip_prefetch {
        work_budget.min(HF_STREAM_FLASH_BUDGET_CAP)
    } else {
        work_budget
    };

    if skip_prefetch || hot_pools.is_empty() {
        let flash_prefetch_started = now_ms();
        hf_flash_prefetch_stale(
            flash_cache.as_ref(),
            rpc.as_ref(),
            &flash_token_list,
            flash_budget,
        )
        .await;
        flash_prefetch_ms = now_ms().saturating_sub(flash_prefetch_started);
        if !flash_token_list.is_empty() {
            flash_cache.spawn_refresh_if_stale(Arc::clone(&rpc), &flash_token_list);
        }
    } else if !work_budget.is_zero() {
        let hot = Arc::clone(&hot_pools);
        let pool_budget = work_budget;
        let pool_fut = async {
            let pool_prefetch_started = now_ms();
            let result = timeout(
                pool_budget,
                hf_pool_prefetch(refresh.as_ref(), hot.as_ref(), prefetch_count),
            )
            .await;
            (result, now_ms().saturating_sub(pool_prefetch_started))
        };
        let flash_fut = async {
            let flash_prefetch_started = now_ms();
            hf_flash_prefetch_stale(
                flash_cache.as_ref(),
                rpc.as_ref(),
                &flash_token_list,
                flash_budget,
            )
            .await;
            now_ms().saturating_sub(flash_prefetch_started)
        };
        let ((pool_out, pool_ms), flash_ms) = tokio::join!(pool_fut, flash_fut);
        pool_prefetch_ms = pool_ms;
        flash_prefetch_ms = flash_ms;
        match pool_out {
            Ok(Ok(result)) => prefetch_ok = result.prefetch_tick_succeeded(),
            Ok(Err(e)) => crate::debug!("hf prefetch failed: {e:#}"),
            Err(_) => crate::debug!("hf prefetch timed out after {}ms", pool_budget.as_millis()),
        }
        if !flash_token_list.is_empty() {
            flash_cache.spawn_refresh_if_stale(Arc::clone(&rpc), &flash_token_list);
        }
    }

    let prefetch_wall_ms = now_ms().saturating_sub(pool_prefetch_started);

    if ctx.snapshots.generation() != selection_generation {
        crate::debug!(
            "hf snap: generation advanced during prefetch ({selection_generation} -> {})",
            ctx.snapshots.generation()
        );
        match hf_reselect_from_snapshot(
            &ctx,
            rescore_cap,
            selection_generation,
            &mut snap_cycle_count,
            &mut token_to_matic_rates,
            &mut token_decimals,
            &mut pool_metas_for_dispatch,
            &mut arena_base,
            &mut snap_state_block,
            &mut snap_state_hash,
            &mut cycles,
            &mut quarantine_skipped,
            &mut rate_skipped,
            &mut tickless_stuck_skipped,
            &mut protocol_mismatch_skipped,
            &mut v2_dead_skipped,
            &mut micro_dead_skipped,
            &mut bal_floor_dead_skipped,
            &mut inactive_len,
            &mut inactive_selected,
            stream_triggered,
            pipeline.stream_enabled,
        ) {
            Ok((new_gen, hot)) => {
                selection_generation = new_gen;
                hot_pools = hot;
                // Reselect can introduce start tokens absent from the prefetched set.
                let flash_token_list = collect_hf_flash_token_list(&arena_base, &cycles).1;
                let (reselect_flash, _) = reserve_hydrate_budget(
                    prep_remaining(prep_deadline).min(prep_remaining(tick_deadline)),
                );
                if !reselect_flash.is_zero() {
                    hf_flash_prefetch_stale(
                        flash_cache.as_ref(),
                        rpc.as_ref(),
                        &flash_token_list,
                        reselect_flash,
                    )
                    .await;
                }
            }
            Err(_) if cycles.is_empty() => {
                if should_log_hf_summary() || stream_triggered {
                    crate::info!(
                        "hf tick: 0 cycles after prefetch reselect (snap={snap_cycle_count}, stream_triggered={})",
                        u8::from(stream_triggered),
                    );
                }
                return Ok(HfTickResult {
                    cycles_considered: snap_cycle_count,
                    profitable_count: 0,
                    best_profit: U256::ZERO,
                    elapsed_ms: now_ms().saturating_sub(start),
                    candidates: Arc::from([]),
                });
            }
            Err(_) => {
                crate::info!(
                    "hf prefetch-reselect empty: keeping prior selected={} stream_triggered={}",
                    cycles.len(),
                    u8::from(stream_triggered),
                );
            }
        }
    }

    let mut arena = arena_base;
    let evaluation_state_generation = arena.apply_hot_cache_unique(&ctx.cache, hot_pools.as_ref());
    // Select used the LF snapshot arena; hot overlay can empty V2 reserves /
    // Balancer maxIn — drop those before hydrate/probe burns the window.
    let before_hot_filter = cycles.len();
    cycles.retain(|cycle| {
        let start_decimals =
            resolve_token_decimals_for_index(cycle.start_token, &arena, &token_decimals);
        let micro_probe = if start_decimals >= 6 {
            crate::util::ten_pow_u256_cached(start_decimals - 6)
        } else {
            U256::from(1u64)
        };
        let start_rate = resolve_token_to_matic_rate(cycle.start_token, &token_to_matic_rates);
        let economic_floor = min_economic_amount_in(start_decimals, start_rate);
        if crate::pipeline::local_sim::first_v2_hop_below_reserve(&arena, &cycle.edges, micro_probe)
            .is_some()
        {
            v2_dead_skipped += 1;
            return false;
        }
        match crate::pipeline::local_sim::economic_floor_liquidity_dead(
            &arena,
            &cycle.edges,
            economic_floor,
        ) {
            Some(crate::pipeline::local_sim::MinimalSimFailure::V2ReserveExhausted { .. }) => {
                v2_dead_skipped += 1;
                false
            }
            Some(crate::pipeline::local_sim::MinimalSimFailure::BalancerMaxInRatio { .. }) => {
                bal_floor_dead_skipped += 1;
                false
            }
            Some(_) => {
                micro_dead_skipped += 1;
                false
            }
            None => true,
        }
    });
    let hot_cache_dropped = before_hot_filter.saturating_sub(cycles.len());
    if log_hf_summary {
        crate::info!(
            "hf eval input: stream_triggered={stream_triggered} snap_generation={selection_generation} state_generation={evaluation_state_generation} state_block={} hot_pools={} hot_cache_drop={hot_cache_dropped} gas_snapshot_age_ms={gas_snapshot_age_ms:?}",
            ctx.refresh.last_state_block(),
            hot_pools.len(),
        );
    }

    let flash_policy = ctx.config.flash_policy;
    let state_provider = ctx.rpc.connect_state().ok();
    let state_provider_ref = state_provider.as_ref();
    let oracle_started = now_ms();
    let matic_usd = match ctx
        .price_oracle
        .resolve_matic_usd_cached()
        .and_then(matic_usd_for_flash_cap)
        .or_else(|| {
            let (raw, age) = ctx.price_oracle.last_known_matic_usd()?;
            if age.as_millis() > HF_MATIC_STALE_WARN_MS as u128 {
                warn_hf_oracle_skip(&format!(
                    "hf eval using stale MATIC/USD while refresh runs (age_ms={})",
                    age.as_millis()
                ));
            }
            matic_usd_for_flash_cap(raw)
        }) {
        Some(usd) => usd,
        None => {
            let oracle_budget = Duration::from_millis(400).min(prep_remaining(tick_deadline));
            if oracle_budget.is_zero() {
                warn_hf_oracle_skip("hf eval skipped: tick budget exhausted before MATIC/USD");
                return Ok(HfTickResult {
                    cycles_considered: cycles.len(),
                    profitable_count: 0,
                    best_profit: U256::ZERO,
                    elapsed_ms: now_ms().saturating_sub(start),
                    candidates: Arc::from([]),
                });
            }
            match timeout(
                oracle_budget,
                ensure_matic_usd_for_flash_cap(&ctx.price_oracle, state_provider_ref),
            )
            .await
            {
                Ok(Some(usd)) => usd,
                Ok(None) => {
                    warn_hf_oracle_skip(
                        "hf eval skipped: MATIC/USD oracle unavailable for flash loan cap",
                    );
                    return Ok(HfTickResult {
                        cycles_considered: cycles.len(),
                        profitable_count: 0,
                        best_profit: U256::ZERO,
                        elapsed_ms: now_ms().saturating_sub(start),
                        candidates: Arc::from([]),
                    });
                }
                Err(_) => {
                    warn_hf_oracle_skip("hf eval skipped: MATIC/USD oracle refresh timed out");
                    return Ok(HfTickResult {
                        cycles_considered: cycles.len(),
                        profitable_count: 0,
                        best_profit: U256::ZERO,
                        elapsed_ms: now_ms().saturating_sub(start),
                        candidates: Arc::from([]),
                    });
                }
            }
        }
    };
    let oracle_ms = now_ms().saturating_sub(oracle_started);

    // Drop cooldown-stuck tickless cycles *before* hydrate so residual prep is not
    // burned re-fetching pools already known empty this minute.
    let stuck_before_hydrate =
        drain_cooldown_stuck_tickless_cycles(&arena, &mut cycles, pool_metas_for_dispatch.as_ref());

    // Hot-cache refresh drops CL ticks on price moves; hydrate tickless pools on
    // the selected HF set before probe ranking (otherwise cl_tickless dominates).
    // Use the carved hydrate_floor — prep_deadline residual is often 0 here after
    // flash/oracle even when reserve_hydrate_budget held 900ms at stage start.
    let probe_tick_budget = hydrate_floor
        .min(HF_PROBE_HYDRATE_MAX_BUDGET)
        .min(prep_remaining(tick_deadline));
    let probe_tick_started = now_ms();
    let probe_pool_cap =
        crate::orchestrator::hf_execute::probe_tick_pool_cap_for_budget(probe_tick_budget);
    static LAST_PROBE_HYDRATE_MS: AtomicU64 = AtomicU64::new(0);
    static HYDRATE_INFO_LOG_AT: AtomicU64 = AtomicU64::new(0);
    static HYDRATE_SKIP_LOG_AT: AtomicU64 = AtomicU64::new(0);
    let last_hydrate = LAST_PROBE_HYDRATE_MS.load(Ordering::Relaxed);
    let hydrate_gap_ok =
        probe_tick_started.saturating_sub(last_hydrate) >= PROBE_HYDRATE_MIN_GAP_MS;
    // Use latest block for tick lens: hot-cache overlay may be newer than
    // `last_state_block`, and pinning there yields empty bitmaps (loaded=0).
    let hydrate_stats = if !hydrate_gap_ok {
        crate::debug!(
            "hf probe-tick hydrate skipped: gap {}ms < {}ms",
            probe_tick_started.saturating_sub(last_hydrate),
            PROBE_HYDRATE_MIN_GAP_MS
        );
        crate::orchestrator::hf_execute::ProbeTickHydrateStats::default()
    } else if probe_tick_budget.is_zero() || probe_pool_cap == 0 {
        let now = now_ms();
        let last = HYDRATE_SKIP_LOG_AT.load(Ordering::Relaxed);
        if now.saturating_sub(last) >= 5_000
            && HYDRATE_SKIP_LOG_AT
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            crate::info!(
                "hf probe-tick hydrate skipped: floor={}ms residual_prep={}ms cap={probe_pool_cap} (need ≥{}ms)",
                hydrate_floor.as_millis(),
                prep_remaining(prep_deadline).as_millis(),
                HF_PROBE_HYDRATE_MIN_BUDGET.as_millis()
            );
        }
        crate::orchestrator::hf_execute::ProbeTickHydrateStats::default()
    } else {
        LAST_PROBE_HYDRATE_MS.store(probe_tick_started, Ordering::Relaxed);
        match timeout(
            probe_tick_budget,
            hydrate_tickless_cl_for_cycles(
                ctx.rpc.as_ref(),
                &mut arena,
                &cycles,
                pool_metas_for_dispatch.as_ref(),
                ctx.config.oracle.tick_word_range,
                None,
                probe_pool_cap,
            ),
        )
        .await
        {
            Ok(stats) => stats,
            Err(_) => {
                // Cool only the budget-scaled set that was in-flight — not the
                // full hard cap (live: cooled 20–45 pools ×10s after every timeout).
                let cooled = crate::orchestrator::hf_execute::mark_probe_hydrate_timeout_cooldown(
                    &arena,
                    &cycles,
                    pool_metas_for_dispatch.as_ref(),
                    probe_pool_cap,
                );
                crate::warn!(
                    "hf probe-tick hydrate timed out after {}ms — cooled {cooled} pools (cap={probe_pool_cap})",
                    probe_tick_budget.as_millis()
                );
                crate::orchestrator::hf_execute::ProbeTickHydrateStats::default()
            }
        }
    };
    let probe_tick_ms = now_ms().saturating_sub(probe_tick_started);
    // Cooldown no-ops (tickless>0 but fetch=0) dominate stream ticks — INFO only on
    // real loads or rate-limited summary (live: 1212 hydrate INFO / run).
    let hydrate_loaded = hydrate_stats.v3_loaded > 0 || hydrate_stats.v4_loaded > 0;
    let hydrate_did_work = hydrate_stats.v3_needed > 0
        || hydrate_stats.v4_needed > 0
        || hydrate_loaded
        || hydrate_stats.cycles_tickless_after != hydrate_stats.cycles_tickless_before;
    if hydrate_did_work {
        let now = now_ms();
        let last_info = HYDRATE_INFO_LOG_AT.load(Ordering::Relaxed);
        let rate_ok = now.saturating_sub(last_info) >= 5_000;
        if hydrate_loaded || rate_ok {
            if rate_ok {
                HYDRATE_INFO_LOG_AT.store(now, Ordering::Relaxed);
            }
            crate::info!(
                "hf probe-tick hydrate: cycles_tickless={}->{} v3_total={} v3_fetch={} v3_loaded={} (empty={} incomplete={} algebra={} seeded={}) v4_total={} v4_fetch={} v4_loaded={} ms={}",
                hydrate_stats.cycles_tickless_before,
                hydrate_stats.cycles_tickless_after,
                hydrate_stats.v3_total,
                hydrate_stats.v3_needed,
                hydrate_stats.v3_loaded,
                hydrate_stats.v3_empty,
                hydrate_stats.v3_incomplete,
                hydrate_stats.v3_algebra_loaded,
                hydrate_stats.v3_seeded,
                hydrate_stats.v4_total,
                hydrate_stats.v4_needed,
                hydrate_stats.v4_loaded,
                probe_tick_ms,
            );
        } else {
            crate::debug!(
                "hf probe-tick hydrate: cycles_tickless={}->{} v3_fetch={} v3_loaded={} v4_fetch={} v4_loaded={} ms={}",
                hydrate_stats.cycles_tickless_before,
                hydrate_stats.cycles_tickless_after,
                hydrate_stats.v3_needed,
                hydrate_stats.v3_loaded,
                hydrate_stats.v4_needed,
                hydrate_stats.v4_loaded,
                probe_tick_ms,
            );
        }
    } else if hydrate_stats.cycles_tickless_before > 0
        || hydrate_stats.v3_total > 0
        || hydrate_stats.v4_total > 0
    {
        crate::debug!(
            "hf probe-tick hydrate idle: cycles_tickless={} v3_total={} v4_total={} ms={}",
            hydrate_stats.cycles_tickless_before,
            hydrate_stats.v3_total,
            hydrate_stats.v4_total,
            probe_tick_ms,
        );
    }
    // After hydrate+cooldown mark, drop newly stuck empties (pre-drain handled above).
    let stuck_after_hydrate =
        drain_cooldown_stuck_tickless_cycles(&arena, &mut cycles, pool_metas_for_dispatch.as_ref());
    let stuck_tickless = stuck_before_hydrate.saturating_add(stuck_after_hydrate);
    if stuck_tickless > 0 {
        // Stream ticks can drain the same stuck set every ~100ms — rate-limit noise.
        static TICKLESS_SKIP_LOG_AT: AtomicU64 = AtomicU64::new(0);
        let now = crate::util::now_ms();
        let last = TICKLESS_SKIP_LOG_AT.load(Ordering::Relaxed);
        if now.saturating_sub(last) >= HF_SUMMARY_INTERVAL_MS
            && TICKLESS_SKIP_LOG_AT
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            crate::info!(
                "hf skip cooldown-stuck tickless cycles: removed={stuck_tickless} (pre={} post={stuck_after_hydrate}) remaining={}",
                stuck_before_hydrate,
                cycles.len()
            );
        }
    }

    let matic_usd_chainlink = ctx.price_oracle.fresh_matic_usd_chainlink_raw();
    let dispatch_token_to_matic_rates = Arc::clone(&token_to_matic_rates);
    let dispatch_token_decimals = Arc::clone(&token_decimals);
    let reassess_ctx = Arc::new(HfEvalInputOwned {
        arena: Arc::new(arena),
        token_to_matic_rates,
        token_decimals,
        gas_oracle: Arc::clone(&ctx.gas_oracle),
        state_generation: evaluation_state_generation,
        brent_iters: ctx.config.routing.brent_search_iterations,
        min_profit_matic: ctx.config.min_profit_matic,
        min_profit_roi_bps: ctx.config.execution.min_profit_roi_bps,
        gas_price,
        slippage_bps: ctx.config.execution.slippage_bps,
        flash_policy,
        max_flash_loan_usd: ctx.config.execution.max_flash_loan_usd,
        matic_usd,
        matic_usd_chainlink,
        safety_multiplier_bps: ctx.config.execution.profit_safety_multiplier_bps,
        profit_priority_alpha_bps: ctx.config.execution.profit_priority_fee_alpha_bps,
        flash_liquidity: Arc::clone(&ctx.execution.flash_liquidity),
        execution: Arc::clone(&ctx.execution),
    });
    let cycles_considered = cycles.len();
    let eval_started = now_ms();
    let eval_budget = HF_EVAL_BUDGET.min(prep_remaining(tick_deadline));
    if eval_budget.is_zero() {
        crate::warn!(
            "hf eval skipped: tick hard budget exhausted (elapsed={}ms, cycles={cycles_considered})",
            now_ms().saturating_sub(start)
        );
        return Ok(HfTickResult {
            cycles_considered,
            profitable_count: 0,
            best_profit: U256::ZERO,
            elapsed_ms: now_ms().saturating_sub(start),
            candidates: Arc::from([]),
        });
    }
    let (eval_results, mut eval_arena, probe_kept) = match timeout(
        eval_budget,
        rescore_rank_and_evaluate_async(cycles, Arc::clone(&reassess_ctx), sim_cap),
    )
    .await
    {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            crate::warn!(
                "hf eval timed out after {}ms (cycles={cycles_considered}, sim_cap={sim_cap})",
                eval_budget.as_millis()
            );
            return Ok(HfTickResult {
                cycles_considered,
                profitable_count: 0,
                best_profit: U256::ZERO,
                elapsed_ms: now_ms().saturating_sub(start),
                candidates: Arc::from([]),
            });
        }
    };
    ctx.inactive_rotation
        .lock()
        .advance(selection_generation, inactive_len, inactive_selected);
    let eval_ms = now_ms().saturating_sub(eval_started);
    let eval_count = eval_results.len();

    let mut profitable: Vec<HfEvalResult> = Vec::new();
    let mut best_profit_matic = U256::ZERO;
    let mut best_near_miss: Option<HfEvalResult> = None;
    let mut best_gross_diag: Option<BestEvalDiag> = None;
    let mut best_gross_probe: Option<HfEvalResult> = None;
    let mut zero_net_rejects = 0usize;
    let mut positive_net_rejects = 0usize;
    let mut cover_n = 0u32;
    let mut cover_max_bps = 0u64;
    let mut cover_sum_bps = 0u64;
    let mut cover_ge_1000 = 0u32;
    let mut cover_ge_5000 = 0u32;
    // Peak cover even when dust gates skip best_gross_diag (live: max_bps=644 best_fp=none).
    let mut cover_peak_fp = 0u64;
    let mut cover_peak_avail = U256::ZERO;
    let mut cover_peak_input_matic = U256::ZERO;
    let mut cover_peak_edges: Option<crate::core::types::CycleEdges> = None;

    for result in eval_results {
        let matic = result.assessment.net_profit_after_gas_matic_wei;
        if matic > best_profit_matic {
            best_profit_matic = matic;
        }
        let assessment = &result.assessment;
        // Prefer closest-to-breakeven in MATIC (token shortfall is not cross-token comparable).
        let available_for_gas = assessment
            .gross_profit
            .saturating_sub(assessment.flash_loan_fee)
            .saturating_sub(assessment.slippage_deduction);
        let gas_shortfall_tokens = assessment
            .gas_cost_in_tokens
            .saturating_sub(available_for_gas);
        let start_decimals = resolve_token_decimals_for_index(
            result.cycle.start_token,
            eval_arena.as_ref(),
            reassess_ctx.token_decimals.as_ref(),
        );
        let start_rate = resolve_token_to_matic_rate(
            result.cycle.start_token,
            reassess_ctx.token_to_matic_rates.as_ref(),
        );
        let scale = ten_pow_u256(start_decimals);
        let available_matic = if start_rate >= MIN_TOKEN_TO_MATIC_RATE && !scale.is_zero() {
            available_for_gas
                .checked_mul(start_rate)
                .map(|v| v / scale)
                .unwrap_or(U256::ZERO)
        } else {
            U256::ZERO
        };
        let gas_shortfall_matic = assessment.gas_cost_wei.saturating_sub(available_matic);
        let gas_cover_bps = if assessment.gas_cost_wei.is_zero() {
            0u64
        } else {
            available_matic
                .checked_mul(U256::from(10_000u64))
                .map(|v| v / assessment.gas_cost_wei)
                .and_then(|v| u64::try_from(v).ok())
                .unwrap_or(u64::MAX)
        };
        let input_matic = if start_rate >= MIN_TOKEN_TO_MATIC_RATE && !scale.is_zero() {
            result
                .sim
                .amount_in
                .checked_mul(start_rate)
                .map(|v| v / scale)
                .unwrap_or(U256::ZERO)
        } else {
            U256::ZERO
        };
        if !assessment.gross_profit.is_zero() {
            cover_n = cover_n.saturating_add(1);
            cover_sum_bps = cover_sum_bps.saturating_add(gas_cover_bps);
            if gas_cover_bps >= cover_max_bps {
                cover_max_bps = gas_cover_bps;
                cover_peak_fp = result.route_fingerprint;
                cover_peak_avail = available_matic;
                cover_peak_input_matic = input_matic;
                cover_peak_edges = Some(result.cycle.edges.clone());
            }
            if gas_cover_bps >= 1_000 {
                cover_ge_1000 = cover_ge_1000.saturating_add(1);
            }
            if gas_cover_bps >= 5_000 {
                cover_ge_5000 = cover_ge_5000.saturating_add(1);
            }
        }
        // Prefer absolute MATIC available toward gas. Cover% alone lets USDT-wei
        // dust (live: input≈8e3, cover_bps≥5000) beat ~0.008 MATIC V3 edges.
        let better_near_breakeven = best_gross_diag.as_ref().is_none_or(|best| {
            prefer_near_miss_by_absolute_matic(
                available_matic,
                gas_shortfall_matic,
                gas_cover_bps,
                best.gas_cost_wei
                    .saturating_sub(best.gas_shortfall_matic_wei),
                best.gas_shortfall_matic_wei,
                best.gas_cover_bps,
            )
        });
        // Rank by absolute MATIC (≥0.001). Thin high-cover dust still wins when no
        // larger edge exists — chronic uq (thin floor 0.05) cools it; do not silence
        // best-eval/cover-dist logging by gating rank at 0.05 (live: zero diag ticks).
        // Skip sub-economic sizes and sub-0.05 MATIC notional (live: USDT input=7917
        // passed economic=1000 and won cover≈687 with gross=1620).
        let economic_floor =
            crate::pipeline::sim_sanity::min_economic_amount_in(start_decimals, start_rate);
        // Keep near-miss visibility (≥0.001 MATIC avail) but require ≥0.05 MATIC
        // notional input so USDT dust (input≈7914) cannot monopolize best-eval.
        if better_near_breakeven
            && !assessment.gross_profit.is_zero()
            && available_matic >= U256::from(10u128.pow(15))
            && result.sim.amount_in >= economic_floor
            && input_matic >= U256::from(5u128 * 10u128.pow(16))
        {
            let observed_gas = ctx.gas_oracle.observed_route_gas(result.route_fingerprint);
            best_gross_diag = Some(BestEvalDiag {
                fp: result.route_fingerprint,
                hops: result.cycle.edge_hops(),
                edges: result.cycle.edges.clone(),
                route: near_miss_route_summary(
                    eval_arena.as_ref(),
                    &result.cycle,
                    pool_metas_for_dispatch.as_ref(),
                ),
                pools: best_eval_pools_summary(eval_arena.as_ref(), &result.cycle),
                input: result.sim.amount_in,
                search_low: result.opt.search_low,
                raw_sim_gas: result.sim.total_gas,
                assessed_gas: observed_gas.unwrap_or_else(|| {
                    ctx.gas_oracle
                        .route_gas_or_heuristic(result.route_fingerprint, result.sim.total_gas)
                }),
                gas_basis: if observed_gas.is_some() {
                    "observed_route"
                } else {
                    "scaled_heuristic"
                },
                sim_scale_bps: ctx.gas_oracle.sim_scale_bps(),
                gas_base_fee_wei: gas_snapshot.base_fee,
                gas_priority_fee_wei: gas_snapshot.priority_fee,
                gas_snapshot_age_ms,
                gas_price_gwei: crate::util::u256_to_f64(gas_price) / 1e9,
                gross: assessment.gross_profit,
                net_matic: assessment.net_profit_after_gas_matic_wei,
                gas_cost_wei: assessment.gas_cost_wei,
                gas_shortfall_tokens,
                gas_shortfall_matic_wei: gas_shortfall_matic,
                gas_cover_bps,
                slippage_bps: result.effective_slippage_bps,
                slippage: assessment.slippage_deduction,
                flash_fee: assessment.flash_loan_fee,
                reject: assessment.reject_reason.clone(),
            });
            if route_is_balancer_only(&result.cycle) {
                best_gross_probe = Some(result.clone());
            }
        }
        if assessment.should_execute {
            profitable.push(result);
        } else {
            if matic.is_zero() {
                zero_net_rejects += 1;
            } else {
                positive_net_rejects += 1;
                let dominated = best_near_miss
                    .as_ref()
                    .is_none_or(|best| matic > best.assessment.net_profit_after_gas_matic_wei);
                if dominated {
                    best_near_miss = Some(result);
                }
            }
        }
    }

    let mut skip_dispatch_refresh = prefetch_ok;
    let mut dispatch_state_generation = evaluation_state_generation;
    let mut dispatch_state_block = if snap_state_block > 0 {
        snap_state_block
    } else {
        ctx.refresh.last_state_block()
    };
    let mut dispatch_state_hash = snap_state_hash;
    let verify_started = now_ms();
    if !profitable.is_empty()
        && let Some(executor) = ctx.config.execution.executor_address
    {
        let sim_provider = match ctx.rpc.connect_simulation() {
            Ok(p) => Some(p),
            Err(e) => {
                crate::warn!(
                    "dropping {} profitable routes: simulation RPC unavailable for pre-dispatch resim ({e:#})",
                    profitable.len()
                );
                profitable.clear();
                None
            }
        };
        if let Some(sim_provider) = sim_provider {
            let (resimmed, resim_refreshed, resim_generation) = refresh_and_resim_profitable(
                &ctx.refresh,
                &ctx.cache,
                Arc::make_mut(&mut eval_arena),
                profitable,
                reassess_ctx.as_ref(),
            )
            .await;
            profitable = resimmed;
            skip_dispatch_refresh = prefetch_ok || resim_refreshed;
            if resim_refreshed {
                dispatch_state_generation = resim_generation;
                dispatch_state_block = ctx.refresh.last_state_block();
                dispatch_state_hash = ctx.refresh.last_state_hash();
                if dispatch_state_block == 0 {
                    crate::warn!("dropping refreshed routes: state refresh has no pinned block");
                    profitable.clear();
                }
            }
            let operator = ctx.wallet.operator_address(executor);
            profitable = filter_balancer_onchain_verified(
                Arc::clone(&ctx.execution),
                eval_arena.as_ref(),
                profitable,
                &sim_provider,
                executor,
                operator,
                pool_metas_for_dispatch.as_ref(),
                ctx.config.execution.slippage_bps,
                Arc::clone(&reassess_ctx),
                dispatch_state_block,
            )
            .await;
            best_profit_matic = profitable
                .iter()
                .map(|r| r.assessment.net_profit_after_gas_matic_wei)
                .max()
                .unwrap_or(U256::ZERO);
        }
    }
    let verify_ms = now_ms().saturating_sub(verify_started);

    profitable.sort_unstable_by(|a, b| {
        let a_direct = route_is_balancer_only(&a.cycle);
        let b_direct = route_is_balancer_only(&b.cycle);
        b_direct.cmp(&a_direct).then_with(|| {
            b.assessment
                .net_profit_after_gas_matic_wei
                .cmp(&a.assessment.net_profit_after_gas_matic_wei)
        })
    });
    profitable.truncate(pipeline.hf_max_dispatch);
    let profitable_count = profitable.len();
    let elapsed_ms = now_ms().saturating_sub(start);

    if cycles_considered > 0 {
        // Reuse the early throttle flag — a second should_log_hf_summary() would miss.
        // Always emit full timing on slow ticks (live stream 9–10s lines lacked stages).
        let log_summary = profitable_count > 0 || log_hf_summary || elapsed_ms >= 250;
        if log_summary {
            crate::info!(
                "hf tick: {profitable_count} profitable of {cycles_considered} cycles ({elapsed_ms}ms, best_profit_matic={best_profit_matic}, probe_kept={probe_kept}, evaluated={eval_count}, timing_ms=prefetch_wall:{prefetch_wall_ms},pool:{pool_prefetch_ms},flash:{flash_prefetch_ms},oracle:{oracle_ms},probe:{probe_tick_ms},eval:{eval_ms},verify:{verify_ms}, stream_triggered={stream_triggered}, pool_prefetch_ok={prefetch_ok})"
            );
        } else if stream_triggered {
            // INFO not debug — at RPBOT_LOG=info, debug hid all stream completions
            // (live: rate-limited stream filters fired, zero stream tick-end lines).
            crate::info!(
                "hf tick: {profitable_count} profitable of {cycles_considered} cycles ({elapsed_ms}ms, probe_kept={probe_kept}, evaluated={eval_count}, stream_triggered=1)"
            );
        }
        if profitable_count == 0 && eval_count > 0 {
            // INFO when a near-miss exists — debug hid positive_net / high-cover ticks
            // at RPBOT_LOG=info (live: safety-floor rejects were invisible).
            let near = best_gross_diag
                .as_ref()
                .is_some_and(|d| d.gas_cover_bps >= 1_000 || !d.net_matic.is_zero());
            if near || ((log_summary || stream_triggered) && positive_net_rejects > 0) {
                crate::info!(
                    "hf assess summary: zero_net={zero_net_rejects} positive_net={positive_net_rejects}"
                );
            } else if log_summary || stream_triggered {
                crate::debug!(
                    "hf assess summary: zero_net={zero_net_rejects} positive_net={positive_net_rejects}"
                );
            }
        }
        if eval_count == 0 {
            crate::debug!(
                "hf assess: 0/{cycles_considered} routes produced assessments (sim_cap={sim_cap})"
            );
        } else if profitable_count == 0 {
            // Underwater quarantine must not sit behind the positive-net near_miss
            // branch — that else-if starved sticky V3↔V3 (~350 cover_bps) rotation
            // whenever any tiny positive-net reject existed in the same tick.
            // Also ignore best_profit_matic: positive_net rejects bump it above zero
            // even when profitable_count==0, which previously skipped quarantine.
            if let Some(ref diag) = best_gross_diag {
                let available_matic = diag
                    .gas_cost_wei
                    .saturating_sub(diag.gas_shortfall_matic_wei);
                if ctx.execution.quarantine_chronic_gas_underwater(
                    diag.fp,
                    diag.gas_cover_bps,
                    available_matic,
                ) {
                    // Cool rotations too — sticky 2-hop V2 dust returned as a
                    // different fp with the same edges after single-fp cool.
                    quarantine_all_edge_rotations(&ctx.execution, &diag.edges);
                    crate::info!(
                        "hf underwater quarantine: fp={} cover_bps={} (+rotations)",
                        diag.fp,
                        diag.gas_cover_bps
                    );
                }
                // Always log near-misses ≥4% cover with real MATIC — rate-limit hid
                // live ~490bps / 0.017 MATIC while sticky ~370 filled the slot.
                let diag_available = diag
                    .gas_cost_wei
                    .saturating_sub(diag.gas_shortfall_matic_wei);
                if (diag.gas_cover_bps >= 400 && diag_available >= U256::from(10u128.pow(15)))
                    || should_log_best_eval()
                {
                    log_best_eval_diagnostic(diag);
                    let cover_avg = if cover_n == 0 {
                        0
                    } else {
                        cover_sum_bps / u64::from(cover_n)
                    };
                    crate::info!(
                        "hf cover-dist: n={cover_n} max_bps={cover_max_bps} avg_bps={cover_avg} ge_1000={cover_ge_1000} ge_5000={cover_ge_5000} best_fp={}",
                        diag.fp,
                    );
                    if let Some(ref probe) = best_gross_probe
                        && ctx.execution.should_log_near_miss(
                            probe.route_fingerprint,
                            probe.assessment.net_profit_after_gas_matic_wei,
                        )
                        && let Some(executor) = ctx.config.execution.executor_address
                        && let Ok(sim_provider) =
                            near_miss_verify_provider(&ctx.rpc, &ctx.config.execution.mode)
                    {
                        probe_near_miss_balancer(
                            &ctx.execution,
                            eval_arena.as_ref(),
                            probe,
                            pool_metas_for_dispatch.as_ref(),
                            &sim_provider,
                            executor,
                            dispatch_state_block,
                        )
                        .await;
                    }
                }
            } else if cover_n > 0 {
                // Sub-diag-gate dust still ranked (live: peak_avail≈6.8e14 / cover≈77
                // after sticky cool) — cool it so near_net stops filling the window.
                if let Some(ref edges) = cover_peak_edges
                    && ctx.execution.quarantine_chronic_gas_underwater(
                        cover_peak_fp,
                        cover_max_bps,
                        cover_peak_avail,
                    )
                {
                    quarantine_all_edge_rotations(&ctx.execution, edges);
                    crate::info!(
                        "hf underwater quarantine: fp={cover_peak_fp} cover_bps={cover_max_bps} avail={cover_peak_avail} (+rotations, peak-no-diag)"
                    );
                }
                if should_log_best_eval() {
                    let cover_avg = cover_sum_bps / u64::from(cover_n);
                    crate::info!(
                        "hf cover-dist: n={cover_n} max_bps={cover_max_bps} avg_bps={cover_avg} ge_1000={cover_ge_1000} ge_5000={cover_ge_5000} best_fp=none peak_fp={cover_peak_fp} peak_avail_matic={cover_peak_avail} peak_input_matic={cover_peak_input_matic}"
                    );
                }
            }
            // Rate-limit diagnostic + on-chain vault probe together. Without this,
            // a sticky Balancer near-miss re-queries every HF tick (~200ms).
            if let Some(ref near_miss) = best_near_miss
                && ctx.execution.should_log_near_miss(
                    near_miss.route_fingerprint,
                    near_miss.assessment.net_profit_after_gas_matic_wei,
                )
            {
                log_near_miss_diagnostic(
                    &ctx.execution,
                    near_miss,
                    eval_arena.as_ref(),
                    pool_metas_for_dispatch.as_ref(),
                    ctx.config.execution.profit_safety_multiplier_bps,
                    ctx.config.min_profit_matic,
                );
                if route_is_balancer_only(&near_miss.cycle)
                    && let Some(executor) = ctx.config.execution.executor_address
                    && let Ok(sim_provider) =
                        near_miss_verify_provider(&ctx.rpc, &ctx.config.execution.mode)
                {
                    probe_near_miss_balancer(
                        &ctx.execution,
                        eval_arena.as_ref(),
                        near_miss,
                        pool_metas_for_dispatch.as_ref(),
                        &sim_provider,
                        executor,
                        dispatch_state_block,
                    )
                    .await;
                }
            }
        }
    }

    // UI rows from already-evaluated results only (no extra sims). Built before
    // dispatch moves `profitable`; cost is O(dispatch size) short strings.
    let candidates: Arc<[HfCandidateUiRow]> = Arc::from(build_hf_candidate_ui_rows(
        eval_arena.as_ref(),
        pool_metas_for_dispatch.as_ref(),
        &profitable,
        best_near_miss.as_ref(),
    ));

    if profitable_count > 0 {
        dispatch_profitable_candidates(
            &ctx,
            Arc::make_mut(&mut eval_arena),
            profitable,
            crate::orchestrator::hf_execute::DispatchInputs {
                pool_metas: pool_metas_for_dispatch.as_ref(),
                token_to_matic_rates: dispatch_token_to_matic_rates.as_ref(),
                token_decimals: dispatch_token_decimals.as_ref(),
                state_generation: dispatch_state_generation,
                state_block: dispatch_state_block,
                state_hash: dispatch_state_hash,
                skip_dispatch_refresh,
                matic_usd: matic_usd_for_flash_cap(matic_usd),
            },
        )
        .await;
    }

    let tick_result = HfTickResult {
        cycles_considered,
        profitable_count,
        best_profit: best_profit_matic,
        elapsed_ms,
        candidates,
    };

    ctx.ui_hook.on_hf_tick(&tick_result, cycles_considered);
    if let Some(fee) = ctx.gas_oracle.snapshot() {
        let gwei = crate::util::u256_to_f64(fee.base_fee + fee.priority_fee) / 1e9;
        ctx.ui_hook.on_gas_update(gwei);
    }

    Ok(tick_result)
}

fn protocol_tag(protocol: ProtocolType) -> &'static str {
    match protocol {
        ProtocolType::UniswapV2 => "V2",
        ProtocolType::UniswapV3 => "V3",
        ProtocolType::UniswapV4 => "V4",
        ProtocolType::BalancerV2 => "BAL",
        ProtocolType::CurveStable => "CRV-S",
        ProtocolType::CurveCrypto => "CRV-C",
        ProtocolType::Dodo => "DODO",
        ProtocolType::Woofi => "WOOFI",
    }
}

fn best_eval_pools_summary(arena: &StateArena, cycle: &FoundCycle) -> String {
    use std::fmt::Write;
    let mut buf = String::with_capacity(cycle.edges.len().saturating_mul(80).max(80));
    for (i, edge) in cycle.edges.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        match arena.pool_address(edge.pool_index) {
            Some(addr) => {
                let _ = write!(buf, "{addr}");
            }
            None => {
                let _ = write!(buf, "p{}", edge.pool_index.0);
            }
        }
        if let Some(crate::core::types::PoolState::V2(s)) = arena.pool_state(edge.pool_index) {
            let _ = write!(buf, "(r0={}/r1={})", s.reserve0, s.reserve1);
        }
    }
    buf
}

fn near_miss_route_summary(
    arena: &StateArena,
    cycle: &FoundCycle,
    pool_metas: &[PoolMeta],
) -> String {
    // ponytail: single String alloc with write! avoids per-format! allocs
    let cap = cycle.edges.len().saturating_mul(64).max(64);
    let mut buf = String::with_capacity(cap);
    use std::fmt::Write;
    if let Some(addr) = arena.token_address(cycle.start_token) {
        // Write first 6 hex chars directly without intermediate hex::encode.
        let bytes = addr.as_slice();
        let _ = write!(
            buf,
            "0x{:02x}{:02x}..{:02x}{:02x}",
            bytes[0], bytes[1], bytes[2], bytes[3]
        );
    } else {
        let _ = write!(buf, "t{}", cycle.start_token.0);
    }
    for edge in &cycle.edges {
        let tag = crate::pipeline::types::pool_meta_at(pool_metas, edge.pool_index)
            .map(|m| protocol_tag(m.protocol))
            .unwrap_or_else(|| protocol_tag(edge.protocol));
        let _ = write!(buf, "->{tag}:");
        // Pool address (was token_out — live sticky diag looked like WMATIC↔token loops).
        if let Some(addr) = arena.pool_address(edge.pool_index) {
            let bytes = addr.as_slice();
            let _ = write!(
                buf,
                "0x{:02x}{:02x}..{:02x}{:02x}",
                bytes[0], bytes[1], bytes[2], bytes[3]
            );
        } else {
            let _ = write!(buf, "p{}", edge.pool_index.0);
        }
    }
    buf
}

fn log_near_miss_diagnostic(
    _execution: &ExecutionService,
    result: &HfEvalResult,
    arena: &StateArena,
    pool_metas: &[PoolMeta],
    safety_bps: u64,
    min_profit_matic: U256,
) {
    // Caller owns rate-limiting via `should_log_near_miss` (shared with vault probe).
    let assessment = &result.assessment;
    let net_matic = assessment.net_profit_after_gas_matic_wei;
    let safety_floor = crate::services::execution::profit::safety_floor_matic_wei(
        assessment.gas_cost_wei,
        safety_bps,
    );
    let gap = safety_floor.saturating_sub(net_matic);
    let roi_bps = (assessment.roi * f64::from(BPS_SCALE)).round() as u64;
    let reason = assessment.reject_reason.as_deref().unwrap_or("unknown");
    crate::info!(
        "hf near-miss: fp={} hops={} score={:.4} route={} input={} gross={} net_matic={} safety_floor={} gap={} min_profit={} roi_bps={} gas_cost_wei={} slippage={} flash_fee={} reject={}",
        result.route_fingerprint,
        result.cycle.edge_hops(),
        result.cycle.score,
        near_miss_route_summary(arena, &result.cycle, pool_metas),
        result.opt.optimal_input,
        assessment.gross_profit,
        net_matic,
        safety_floor,
        gap,
        min_profit_matic,
        roi_bps,
        assessment.gas_cost_wei,
        assessment.slippage_deduction,
        assessment.flash_loan_fee,
        reason,
    );
}

/// Rank near-miss candidates by absolute MATIC available toward gas, then shortfall,
/// then cover bps. Cover% alone promotes dust inputs with tiny gas denominators.
#[must_use]
fn prefer_near_miss_by_absolute_matic(
    available_matic: U256,
    shortfall_matic: U256,
    cover_bps: u64,
    best_available_matic: U256,
    best_shortfall_matic: U256,
    best_cover_bps: u64,
) -> bool {
    available_matic > best_available_matic
        || (available_matic == best_available_matic
            && (shortfall_matic < best_shortfall_matic
                || (shortfall_matic == best_shortfall_matic && cover_bps > best_cover_bps)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::constants::{WMATIC, is_polygon_hub_token};
    use crate::core::types::{CycleEdges, Edge, PoolIndex, PoolState, TokenIndex, V2PoolState};
    use crate::services::partial_cache::SlimPoolState;

    #[test]
    fn flash_blocking_stale_keeps_only_cold_hubs() {
        let dust = Address::repeat_byte(0xab);
        let stale = vec![dust, WMATIC, Address::repeat_byte(0xcd)];
        let flash = FlashLiquidityCache::new();
        let blocking = flash_blocking_stale(&stale, &flash);
        assert_eq!(blocking, vec![WMATIC]);
        assert!(is_polygon_hub_token(WMATIC));
        assert!(!is_polygon_hub_token(dust));
    }

    #[test]
    fn flash_blocking_stale_empty_when_only_dust() {
        let stale = vec![Address::repeat_byte(0x11), Address::repeat_byte(0x22)];
        let flash = FlashLiquidityCache::new();
        assert!(flash_blocking_stale(&stale, &flash).is_empty());
    }

    #[test]
    fn prep_remaining_zero_after_deadline() {
        let past = Instant::now() - Duration::from_millis(5);
        assert!(prep_remaining(past).is_zero());
        let future = Instant::now() + Duration::from_secs(10);
        assert!(prep_remaining(future) > Duration::from_secs(1));
    }

    #[test]
    fn reserve_hydrate_keeps_one_pool_floor() {
        let stage = Duration::from_millis(2_500);
        let (work, hydrate) = reserve_hydrate_budget(stage);
        assert_eq!(hydrate, HF_PROBE_HYDRATE_MAX_BUDGET);
        assert_eq!(work + hydrate, stage);
        // Full MAX residual admits pool cap (3 × 300ms).
        assert_eq!(
            crate::orchestrator::hf_execute::probe_tick_pool_cap_for_budget(hydrate),
            3
        );

        let tight = Duration::from_millis(200);
        let (work_t, hydrate_t) = reserve_hydrate_budget(tight);
        assert!(hydrate_t.is_zero());
        assert_eq!(work_t, tight);
        assert_eq!(
            crate::orchestrator::hf_execute::probe_tick_pool_cap_for_budget(hydrate_t),
            0
        );

        let exact = HF_PROBE_HYDRATE_MIN_BUDGET;
        let (work_e, hydrate_e) = reserve_hydrate_budget(exact);
        assert!(work_e.is_zero());
        assert_eq!(hydrate_e, HF_PROBE_HYDRATE_MIN_BUDGET);
        assert_eq!(
            crate::orchestrator::hf_execute::probe_tick_pool_cap_for_budget(hydrate_e),
            1
        );
    }

    #[test]
    fn near_miss_prefers_absolute_matic_over_cover_bps_dust() {
        // Live balremap: USDT dust cover_bps=7800 vs V3 edge ~0.008 MATIC @ ~350 bps.
        let dust_avail = U256::from(1_000u64); // wei-scale dust
        let v3_avail = U256::from(8_000_000_000_000_000u64); // 0.008 MATIC
        assert!(prefer_near_miss_by_absolute_matic(
            v3_avail,
            U256::from(200_000_000_000_000_000u64),
            350,
            dust_avail,
            U256::from(1u64),
            7_800,
        ));
        assert!(!prefer_near_miss_by_absolute_matic(
            dust_avail,
            U256::from(1u64),
            7_800,
            v3_avail,
            U256::from(200_000_000_000_000_000u64),
            350,
        ));
    }

    fn cycle(pool_index: PoolIndex, score: f64) -> Arc<FoundCycle> {
        Arc::new(FoundCycle {
            start_token: TokenIndex(0),
            edges: CycleEdges::from_slice(&[Edge {
                pool_index,
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            }]),
            hop_count: 1,
            log_weight: score,
            cumulative_fee_bps: 30,
            score,
            cycle_ratio: U256::ZERO,
        })
    }

    fn v2_state() -> Arc<PoolState> {
        // Reserves must clear default-18-dec micro probe (1e12) and economic floor.
        Arc::new(PoolState::V2(V2PoolState {
            reserve0: U256::from(10u64).pow(U256::from(24u64)),
            reserve1: U256::from(10u64).pow(U256::from(24u64)),
            fee: U256::from(997u64),
            fee_denominator: U256::from(1_000u64),
            block_timestamp_last: 1,
        }))
    }

    /// Edge tokens must resolve to addresses or the pool-meta gate rejects the
    /// cycle as protocol_mismatch. t0 < t1 keeps zero_for_one=true canonical.
    fn arena_with_edge_tokens() -> StateArena {
        let mut arena = StateArena::default();
        arena.register_token(Address::from([0xaa; 20]));
        arena.register_token(Address::from([0xbb; 20]));
        arena
    }

    /// 1:1 MATIC rate keeps the economic floor (1e17) far below v2_state reserves
    /// (1e24); the minimum rate would inflate the floor to 1e29 and cull as v2_dead.
    fn one_to_one_rates() -> rustc_hash::FxHashMap<TokenIndex, U256> {
        let mut rates = rustc_hash::FxHashMap::default();
        rates.insert(TokenIndex(0), U256::from(10u64).pow(U256::from(18u64)));
        rates
    }

    fn hot_slim_state(activity_count: u64) -> SlimPoolState {
        SlimPoolState {
            protocol: ProtocolType::UniswapV2,
            sqrt_price_x96: U256::ZERO,
            liquidity: 0,
            tick: 0,
            reserve0: U256::from(10u64).pow(U256::from(24u64)),
            reserve1: U256::from(10u64).pow(U256::from(24u64)),
            patched_at_ms: now_ms(),
            activity_count,
        }
    }

    #[test]
    fn select_prefers_all_actives_before_inactive_rotation() {
        let mut arena = arena_with_edge_tokens();
        let addresses: Vec<_> = (1u8..=4)
            .map(|id| {
                let address = Address::from([id; 20]);
                arena.register_pool(address, v2_state());
                address
            })
            .collect();
        let partial_cache = PartialPoolCache::new();
        // Pools 0 and 1 are hot; 2 and 3 are cold.
        for &addr in &addresses[..2] {
            partial_cache.seed(addr, hot_slim_state(3));
        }
        let cycles: Vec<_> = [1.0, 2.0, 3.0, 4.0]
            .into_iter()
            .enumerate()
            .map(|(index, score)| cycle(PoolIndex(index as u32), score))
            .collect();
        let rates = one_to_one_rates();

        let decimals = FxHashMap::default();
        let selected = select_cycles_for_rescore(
            &cycles,
            &arena,
            &[],
            &partial_cache,
            &ExecutionService::default(),
            &rates,
            &decimals,
            3,
            0,
        );

        assert_eq!(selected.activity_candidates, 2);
        assert_eq!(selected.activity_selected, 2);
        assert_eq!(selected.inactive_selected, 1);
        assert_eq!(selected.cycles.len(), 3);
        // Two hot pools first (indices 0,1), then one inactive via rotation.
        let selected_pools: Vec<u32> = selected
            .cycles
            .iter()
            .map(|c| c.edges[0].pool_index.0)
            .collect();
        assert!(selected_pools[..2].contains(&0));
        assert!(selected_pools[..2].contains(&1));
    }

    #[test]
    fn hf_activity_slot_cap_leaves_inactive_room() {
        assert_eq!(hf_activity_slot_cap(12), 4); // max(4, 12/5)
        assert_eq!(hf_activity_slot_cap(30), 6);
        assert_eq!(hf_activity_slot_cap(150), 16); // hard ceiling
        assert_eq!(hf_activity_slot_cap(3), 3); // floor clamped by cap
        assert_eq!(hf_activity_slot_cap(0), 0);
    }

    #[test]
    fn activity_rank_considers_routes_outside_the_static_top_three() {
        let mut arena = arena_with_edge_tokens();
        let addresses: Vec<_> = (1u8..=4)
            .map(|id| {
                let address = Address::from([id; 20]);
                arena.register_pool(address, v2_state());
                address
            })
            .collect();
        let partial_cache = PartialPoolCache::new();
        partial_cache.seed(addresses[3], hot_slim_state(1));
        let cycles: Vec<_> = [1.0, 2.0, 3.0, 4.0]
            .into_iter()
            .enumerate()
            .map(|(index, score)| cycle(PoolIndex(index as u32), score))
            .collect();
        let rates = one_to_one_rates();

        let decimals = FxHashMap::default();
        let selected = select_cycles_for_rescore(
            &cycles,
            &arena,
            &[],
            &partial_cache,
            &ExecutionService::default(),
            &rates,
            &decimals,
            1,
            0,
        );

        assert_eq!(selected.cycles.len(), 1);
        assert_eq!(selected.cycles[0].score, 4.0);
    }

    #[test]
    fn inactive_rotation_wraps_without_duplicates_and_resets_on_snapshot_change() {
        let mut rotation = InactiveCycleRotation::default();
        assert_eq!(inactive_indices(rotation.offset_for(1, 5), 5, 3), [0, 1, 2]);
        rotation.advance(1, 5, 3);
        assert_eq!(inactive_indices(rotation.offset_for(1, 5), 5, 3), [3, 4, 0]);
        assert_eq!(inactive_indices(rotation.offset_for(2, 5), 5, 3), [0, 1, 2]);
    }

    #[test]
    fn stream_pending_pools_only_contains_dirty_selected_pools() {
        let partial = PartialPoolCache::new();
        let selected = Address::with_last_byte(1);
        let unselected = Address::with_last_byte(2);
        for pool in [selected, unselected] {
            partial.apply_patch(
                pool,
                crate::services::partial_cache::LogPatch::V2Reserves {
                    reserve0: U256::from(10u8),
                    reserve1: U256::from(20u8),
                },
                1,
            );
        }

        assert_eq!(stream_pending_pools(&partial, &[selected]), vec![selected]);
    }
}
