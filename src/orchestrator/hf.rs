use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy::primitives::{Address, U256};
use tokio::sync::watch;
use tokio::time::timeout;

use crate::config::AppConfig;
use crate::config::WalletSecrets;
use crate::core::constants::BPS_SCALE;
use crate::core::constants::MIN_TOKEN_TO_MATIC_RATE;
use crate::core::types::{FlashLoanSource, FoundCycle, ProtocolType};
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
use rustc_hash::{FxHashMap, FxHashSet};

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
    pub inactive_rotation: parking_lot::Mutex<InactiveCycleRotation>,
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
    pub candidates: Vec<HfCandidateUiRow>,
}

impl Default for HfTickResult {
    fn default() -> Self {
        Self {
            cycles_considered: 0,
            profitable_count: 0,
            best_profit: U256::ZERO,
            elapsed_ms: 0,
            candidates: Vec::new(),
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

const HF_ACTIVITY_WINDOW_MS: u64 = 300_000;
const HF_SUMMARY_INTERVAL_MS: u64 = 15_000;
const HF_BEST_EVAL_INTERVAL_MS: u64 = 60_000;
const HF_EVAL_BUDGET: Duration = Duration::from_secs(30);
static HF_SUMMARY_LOG_AT: AtomicU64 = AtomicU64::new(0);
static HF_BEST_EVAL_LOG_AT: AtomicU64 = AtomicU64::new(0);
static HF_ORACLE_SKIP_LOG_AT: AtomicU64 = AtomicU64::new(0);
const HF_ORACLE_SKIP_INTERVAL_MS: u64 = 30_000;
/// MATIC/USD refresh can hold singleflight longer than cache TTL; HF may use slightly stale price.
const HF_MATIC_STALE_WARN_MS: u64 = 45_000;
const HF_FLASH_PREFETCH_BUDGET_MS: u64 = 750;

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
) {
    if flash_token_list.is_empty() {
        return;
    }
    flash_cache.track_hot_tokens(flash_token_list);
    let stale: Vec<Address> = flash_token_list
        .iter()
        .copied()
        .filter(|addr| !flash_cache.has_fresh_entry(*addr))
        .collect();
    if stale.is_empty() {
        return;
    }
    let flash_budget = Duration::from_millis(HF_FLASH_PREFETCH_BUDGET_MS);
    let stale_n = stale.len();
    let fresh_n = flash_token_list.len().saturating_sub(stale_n);
    let Some(_inflight) = flash_cache.try_acquire_refresh_inflight() else {
        crate::debug!("flash loan: hf_prefetch skipped stale={stale_n} (refresh inflight)");
        return;
    };
    match timeout(flash_budget, flash_cache.refresh_with_fallback(rpc, &stale)).await {
        Ok(Ok(generation)) => {
            crate::info!(
                "flash loan: hf_prefetch ok stale={stale_n} fresh={fresh_n} generation={generation}"
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
    if cycles.is_empty() {
        return Err(HfTickResult {
            cycles_considered: 0,
            profitable_count: 0,
            best_profit: U256::ZERO,
            elapsed_ms: 0,
            candidates: Vec::new(),
        });
    }
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
fn cycle_edges_quarantined(
    execution: &ExecutionService,
    edges: &[crate::core::types::Edge],
) -> bool {
    let n = edges.len();
    if n == 0 {
        return false;
    }
    let mut rotated: smallvec::SmallVec<[crate::core::types::Edge; 8]> =
        edges.iter().copied().collect();
    for _ in 0..n {
        if execution.is_route_quarantined(hash_cycle_edges(&rotated)) {
            return true;
        }
        rotated.rotate_left(1);
    }
    false
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
    route: String,
    input: U256,
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
        "hf best-eval: fp={} hops={} route={} input={} raw_sim_gas={} assessed_gas={} gas_basis={} sim_scale_bps={} gas_base_fee_wei={} gas_priority_fee_wei={} gas_snapshot_age_ms={:?} gas_price_gwei={:.3} gross={} net_matic={} gas_cost_wei={} gas_shortfall_tokens={} gas_shortfall_matic_wei={} gas_cover_bps={} slippage_bps={} slippage={} flash_fee={} reject={}",
        diag.fp,
        diag.hops,
        diag.route,
        diag.input,
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
#[must_use]
fn hf_activity_slot_cap(rescore_cap: usize) -> usize {
    rescore_cap.saturating_div(3).max(8).min(rescore_cap)
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
    activity_candidates: usize,
    activity_selected: usize,
    inactive_len: usize,
    inactive_selected: usize,
}

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
    for cycle in snap_cycles {
        // Skip only when every tickless CL hop is on miss cooldown. Skipping any
        // CL pool on cooldown (even with LF ticks) wiped the HF window
        // (`selected=0`, tickless_stuck≫candidates) whenever a hub pool missed.
        if cycle_tickless_cl_all_on_miss_cooldown(arena, cycle.as_ref(), pool_metas) {
            tickless_stuck_skipped += 1;
            continue;
        }
        let Some(ready) = cycle_with_reliable_start(cycle, token_to_matic_rates) else {
            rate_skipped += 1;
            continue;
        };
        // Recover Balancer/Woofi vault-index skew (meta vs getPoolTokens) before
        // quarantine/micro-dead — otherwise we only prune recoverable liquidity.
        let Some(ready) = crate::pipeline::local_sim::realign_multi_token_found_cycle(arena, ready)
        else {
            micro_dead_skipped += 1;
            continue;
        };
        // Cached-cycle protocol tags can lag hot-cache family flips (V2→V3).
        let Some(ready) = crate::pipeline::local_sim::heal_cycle_edge_protocols(arena, ready)
        else {
            protocol_mismatch_skipped += 1;
            continue;
        };
        // Remap stale Uni TokenIndex endpoints from PoolMeta legs before reject.
        let Some(ready) =
            crate::pipeline::local_sim::realign_uni_cycle_from_pool_meta(arena, pool_metas, ready)
        else {
            protocol_mismatch_skipped += 1;
            continue;
        };
        if !crate::pipeline::local_sim::cycle_edges_match_arena_state(arena, &ready.edges) {
            protocol_mismatch_skipped += 1;
            continue;
        }
        // Stale V2/V3/V4 TokenIndex vs refreshed PoolMeta — sim invents profit.
        if !crate::pipeline::local_sim::cycle_v2_edges_match_pool_meta(
            arena,
            pool_metas,
            &ready.edges,
        ) {
            protocol_mismatch_skipped += 1;
            continue;
        }
        // Quarantine keys are assess/best-eval fingerprints (post start-rotation /
        // vault-idx remap). Check every hop-start rotation of remapped + raw edges.
        if cycle_edges_quarantined(execution, &ready.edges)
            || cycle_edges_quarantined(execution, &cycle.edges)
        {
            quarantine_skipped += 1;
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
        if score > 0 {
            // Live WSS: skip insane-gross phantom only (stale sim can look "too good").
            // Still drop micro-shallow/mismatch — they never rank after hydrate either and
            // crowded the probe window (protoheal: shallow_cl/mismatch empty ranks).
            if crate::pipeline::local_sim::micro_probe_liquidity_dead(
                arena,
                &ready.edges,
                micro_probe,
            )
            .is_some()
            {
                micro_dead_skipped += 1;
                continue;
            }
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
            hot_pools: FxHashSet::default(),
            quarantine_skipped,
            rate_skipped,
            tickless_stuck_skipped,
            protocol_mismatch_skipped,
            v2_dead_skipped,
            micro_dead_skipped,
            bal_floor_dead_skipped,
            activity_candidates: 0,
            activity_selected: 0,
            inactive_len: 0,
            inactive_selected: 0,
        };
    }

    // Activity desc, then cycle quality. Only sort the active partition.
    active.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| compare_cycle_score(a.0.as_ref(), b.0.as_ref()))
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
        Default::default(),
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
                candidates: Vec::new(),
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
            candidates: Vec::new(),
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
    let activity_candidates = selection.activity_candidates;
    let activity_selected = selection.activity_selected;
    let mut inactive_len = selection.inactive_len;
    let mut inactive_selected = selection.inactive_selected;
    let mut snap_state_block = snap.state_block;
    let mut snap_state_hash = snap.state_hash;
    drop(snap);
    let log_hf_summary = should_log_hf_summary() || stream_triggered;
    if log_hf_summary {
        crate::info!(
            "hf cycle filter: snap={snap_cycle_count} selected={} quarantine_skip={quarantine_skipped} rate_skip={rate_skipped} tickless_stuck_skip={tickless_stuck_skipped} proto_mismatch_skip={protocol_mismatch_skipped} v2_dead_skip={v2_dead_skipped} micro_dead_skip={micro_dead_skipped} bal_floor_dead_skip={bal_floor_dead_skipped} active_candidates={activity_candidates} active_selected={activity_selected} inactive_candidates={inactive_len} inactive_selected={inactive_selected} inactive_offset={inactive_offset} hot_pools={} rescore_cap={rescore_cap}",
            cycles.len(),
            hot_pools_set.len(),
        );
    }
    if cycles.is_empty() {
        if log_hf_summary {
            crate::info!(
                "hf tick: 0 cycles after filter (snap={snap_cycle_count}, quarantine={quarantine_skipped}, no_rate={rate_skipped}, stream_triggered={stream_triggered})"
            );
        }
        // Do not re-notify here — that looped promote storms with selected=0
        // while topic spam never entered the arena universe (V2 disabled).
        return Ok(HfTickResult {
            cycles_considered: 0,
            profitable_count: 0,
            best_profit: U256::ZERO,
            elapsed_ms: now_ms().saturating_sub(start),
            candidates: Vec::new(),
        });
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
            candidates: Vec::new(),
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
            Err(_) => {
                if should_log_hf_summary() {
                    crate::info!(
                        "hf tick: 0 cycles after refresh (snap={snap_cycle_count}, quarantine={quarantine_skipped}, no_rate={rate_skipped})"
                    );
                }
                return Ok(HfTickResult {
                    cycles_considered: 0,
                    profitable_count: 0,
                    best_profit: U256::ZERO,
                    elapsed_ms: now_ms().saturating_sub(start),
                    candidates: Vec::new(),
                });
            }
        }
    }

    let prefetch_count = pipeline.hf_prefetch_count.min(hot_pools.len().max(1));
    let skip_prefetch = stream_triggered && pipeline.stream_enabled;
    let mut prefetch_ok = skip_prefetch;
    let pool_prefetch_budget = Duration::from_millis(pipeline.hf_prefetch_budget_ms.max(1));
    let pool_prefetch_started = now_ms();

    if stream_triggered && pipeline.stream_enabled {
        let flushed = ctx
            .partial_cache
            .flush_to_state_cache(&ctx.cache, hot_pools.as_ref());
        let pending_pools = stream_pending_pools(&ctx.partial_cache, hot_pools.as_ref());
        if !pending_pools.is_empty() {
            crate::warn!(
                "stream flush incomplete: flushed={flushed} hot_dirty_pending={} — refreshing before HF eval",
                pending_pools.len(),
            );
            let recovery_budget = Duration::from_millis(pipeline.hf_prefetch_budget_ms.max(1));
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
                    let remaining = stream_pending_pools(&ctx.partial_cache, hot_pools.as_ref());
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
                            candidates: Vec::new(),
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
                        candidates: Vec::new(),
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
                        candidates: Vec::new(),
                    });
                }
            }
        } else if flushed > 0 {
            prefetch_ok = true;
        }
    }

    let flash_prefetch_started = now_ms();
    let flash_token_list = collect_hf_flash_token_list(&arena_base, &cycles).1;
    let flash_cache = Arc::clone(&ctx.execution.flash_liquidity);
    let rpc = Arc::clone(&ctx.rpc);
    let refresh = Arc::clone(&ctx.refresh);

    if skip_prefetch || hot_pools.is_empty() {
        hf_flash_prefetch_stale(flash_cache.as_ref(), rpc.as_ref(), &flash_token_list).await;
        if !flash_token_list.is_empty() {
            flash_cache.spawn_refresh_if_stale(rpc, &flash_token_list);
        }
    } else {
        let hot = Arc::clone(&hot_pools);
        let pool_fut = async {
            timeout(
                pool_prefetch_budget,
                hf_pool_prefetch(refresh.as_ref(), hot.as_ref(), prefetch_count),
            )
            .await
        };
        let flash_fut =
            hf_flash_prefetch_stale(flash_cache.as_ref(), rpc.as_ref(), &flash_token_list);
        let (pool_out, _) = tokio::join!(pool_fut, flash_fut);
        match pool_out {
            Ok(Ok(result)) => prefetch_ok = result.prefetch_tick_succeeded(),
            Ok(Err(e)) => crate::debug!("hf prefetch failed: {e:#}"),
            Err(_) => crate::debug!(
                "hf prefetch timed out after {}ms",
                pool_prefetch_budget.as_millis()
            ),
        }
        if !flash_token_list.is_empty() {
            flash_cache.spawn_refresh_if_stale(rpc, &flash_token_list);
        }
    }

    let pool_prefetch_ms = now_ms().saturating_sub(pool_prefetch_started);
    let flash_prefetch_ms = now_ms().saturating_sub(flash_prefetch_started);

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
            }
            Err(_) => {
                return Ok(HfTickResult {
                    cycles_considered: snap_cycle_count,
                    profitable_count: 0,
                    best_profit: U256::ZERO,
                    elapsed_ms: now_ms().saturating_sub(start),
                    candidates: Vec::new(),
                });
            }
        }
    }

    let mut arena = arena_base;
    let evaluation_state_generation = arena.apply_hot_cache(&ctx.cache, hot_pools.as_ref());
    if log_hf_summary {
        crate::info!(
            "hf eval input: stream_triggered={stream_triggered} snap_generation={selection_generation} state_generation={evaluation_state_generation} state_block={} hot_pools={} gas_snapshot_age_ms={gas_snapshot_age_ms:?}",
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
        None => match timeout(
            Duration::from_millis(800),
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
                    candidates: Vec::new(),
                });
            }
            Err(_) => {
                warn_hf_oracle_skip("hf eval skipped: MATIC/USD oracle refresh timed out");
                return Ok(HfTickResult {
                    cycles_considered: cycles.len(),
                    profitable_count: 0,
                    best_profit: U256::ZERO,
                    elapsed_ms: now_ms().saturating_sub(start),
                    candidates: Vec::new(),
                });
            }
        },
    };
    let oracle_ms = now_ms().saturating_sub(oracle_started);

    // Hot-cache refresh drops CL ticks on price moves; hydrate tickless pools on
    // the selected HF set before probe ranking (otherwise cl_tickless dominates).
    // Same budget as pool prefetch — the old 900ms hard cap ignored HF_PREFETCH_BUDGET_MS.
    let probe_tick_budget =
        Duration::from_millis(ctx.config.pipeline.hf_prefetch_budget_ms.max(200));
    let probe_tick_started = now_ms();
    // Use latest block for tick lens: hot-cache overlay may be newer than
    // `last_state_block`, and pinning there yields empty bitmaps (loaded=0).
    let hydrate_stats = match timeout(
        probe_tick_budget,
        hydrate_tickless_cl_for_cycles(
            ctx.rpc.as_ref(),
            &mut arena,
            &cycles,
            pool_metas_for_dispatch.as_ref(),
            ctx.config.oracle.tick_word_range,
            None,
        ),
    )
    .await
    {
        Ok(stats) => stats,
        Err(_) => {
            // Cool the attempted targets — without this the next tick re-burns
            // the full budget on the same pools (median tick pinned at budget).
            let cooled = crate::orchestrator::hf_execute::mark_probe_hydrate_timeout_cooldown(
                &arena,
                &cycles,
                pool_metas_for_dispatch.as_ref(),
            );
            crate::warn!(
                "hf probe-tick hydrate timed out after {}ms — cooled {cooled} pools",
                probe_tick_budget.as_millis()
            );
            crate::orchestrator::hf_execute::ProbeTickHydrateStats::default()
        }
    };
    let probe_tick_ms = now_ms().saturating_sub(probe_tick_started);
    if hydrate_stats.v3_total > 0
        || hydrate_stats.v4_total > 0
        || hydrate_stats.cycles_tickless_before > 0
    {
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
    }
    // After hydrate+cooldown mark, drop cycles whose CL hops are empty and already
    // known-dead this minute so probe/Brent budget is not spent on dust-only phantoms.
    let stuck_tickless =
        drain_cooldown_stuck_tickless_cycles(&arena, &mut cycles, pool_metas_for_dispatch.as_ref());
    if stuck_tickless > 0 {
        crate::info!(
            "hf skip cooldown-stuck tickless cycles: removed={stuck_tickless} remaining={}",
            cycles.len()
        );
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
    let (eval_results, mut eval_arena, probe_kept) = match timeout(
        HF_EVAL_BUDGET,
        rescore_rank_and_evaluate_async(cycles, Arc::clone(&reassess_ctx), sim_cap),
    )
    .await
    {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            crate::warn!(
                "hf eval timed out after {}ms (cycles={cycles_considered}, sim_cap={sim_cap})",
                HF_EVAL_BUDGET.as_millis()
            );
            return Ok(HfTickResult {
                cycles_considered,
                profitable_count: 0,
                best_profit: U256::ZERO,
                elapsed_ms: now_ms().saturating_sub(start),
                candidates: Vec::new(),
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
        if !assessment.gross_profit.is_zero() {
            cover_n = cover_n.saturating_add(1);
            cover_sum_bps = cover_sum_bps.saturating_add(gas_cover_bps);
            cover_max_bps = cover_max_bps.max(gas_cover_bps);
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
        if better_near_breakeven && !assessment.gross_profit.is_zero() {
            let observed_gas = ctx.gas_oracle.observed_route_gas(result.route_fingerprint);
            best_gross_diag = Some(BestEvalDiag {
                fp: result.route_fingerprint,
                hops: result.cycle.edge_hops(),
                route: near_miss_route_summary(
                    eval_arena.as_ref(),
                    &result.cycle,
                    pool_metas_for_dispatch.as_ref(),
                ),
                input: result.sim.amount_in,
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
        let log_summary = profitable_count > 0 || stream_triggered || should_log_hf_summary();
        if log_summary {
            crate::info!(
                "hf tick: {profitable_count} profitable of {cycles_considered} cycles ({elapsed_ms}ms, best_profit_matic={best_profit_matic}, probe_kept={probe_kept}, evaluated={eval_count}, timing_ms=pool:{pool_prefetch_ms},flash:{flash_prefetch_ms},oracle:{oracle_ms},probe:{probe_tick_ms},eval:{eval_ms},verify:{verify_ms}, stream_triggered={stream_triggered}, pool_prefetch_ok={prefetch_ok})"
            );
            if profitable_count == 0 && eval_count > 0 {
                crate::info!(
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
                if ctx
                    .execution
                    .quarantine_chronic_gas_underwater(diag.fp, diag.gas_cover_bps)
                {
                    crate::info!(
                        "hf underwater quarantine: fp={} cover_bps={}",
                        diag.fp,
                        diag.gas_cover_bps
                    );
                }
                if should_log_best_eval() {
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
    let candidates = build_hf_candidate_ui_rows(
        eval_arena.as_ref(),
        pool_metas_for_dispatch.as_ref(),
        &profitable,
        best_near_miss.as_ref(),
    );

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
        if let Some(addr) = arena.token_address(edge.token_out) {
            let bytes = addr.as_slice();
            let _ = write!(
                buf,
                "0x{:02x}{:02x}..{:02x}{:02x}",
                bytes[0], bytes[1], bytes[2], bytes[3]
            );
        } else {
            let _ = write!(buf, "t{}", edge.token_out.0);
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
    use crate::core::types::{CycleEdges, Edge, PoolIndex, PoolState, TokenIndex, V2PoolState};
    use crate::services::partial_cache::SlimPoolState;

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
        assert_eq!(hf_activity_slot_cap(12), 8); // max(8, 12/3)
        assert_eq!(hf_activity_slot_cap(30), 10);
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
