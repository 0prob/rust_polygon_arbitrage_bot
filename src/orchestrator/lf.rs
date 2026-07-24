use std::sync::Arc;

use alloy::primitives::{Address, U256};
use anyhow::Context;
use parking_lot::Mutex;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};
use smallvec::SmallVec;
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::time::{Duration, MissedTickBehavior, interval};

use crate::config::AppConfig;
use crate::core::constants::{HOP_CAP, POLYGON_HUB_TOKENS};
use crate::core::math::fixed_point::ONE;
use crate::core::types::{PoolIndex, TokenIndex};
use crate::infra::rpc::RpcPool;
use crate::orchestrator::ui_hook::SharedUiHook;
use crate::pipeline::arena::StateArena;
use crate::pipeline::cycle_filter::{ProbeContext, cycle_key};
use crate::pipeline::cycle_finder::CYCLE_ENUM_PATCH_BUDGET;
use crate::pipeline::cycle_search::{find_cycles_for_mode, find_cycles_for_mode_with_budget};
use crate::pipeline::graph::{
    GraphBuildGate, attach_missing_eligible_pools_with_gate, attach_pool_to_graph,
    build_graph_with_gate, count_graph_eligible_pools_with_gate, funded_token_indices,
    has_missing_eligible_pools_with_gate, refresh_graph_cycle_coverage, rescore_pools_in_place,
    routing_token_at_leg,
};
use crate::pipeline::graph_cache::GraphCache;
use crate::pipeline::spot_price::{
    SpotTable, finalize_enumerated_cycles, min_profitable_cycle_ratio,
    rescore_cycles_with_table_and_gas,
};
use crate::pipeline::tick_fetch::{
    collect_v3_pool_addresses, collect_v4_tick_targets, hydrate_cl_ticks_with_rpc_fallback,
};
use crate::pipeline::types::CycleSearchPass;
use crate::services::execution::GasOracle;
use crate::services::execution::flash_liquidity::FlashLiquidityCache;
use crate::services::hf_snapshot::SnapshotStore;
use crate::services::oracle::price_oracle::PriceOracle;
use crate::services::oracle::{
    HubPathRateParams, RateEnrichContext, arena_missing_decimal_addresses,
    arena_tokens_without_decimal_hints, enrich_token_to_matic_rates,
    enrich_token_to_matic_rates_offline, expand_hub_spoke_resolvable, has_reliable_matic_rate,
    hub_path_matic_rates_batch, merge_token_rates, resolvable_token_set,
};
use crate::services::partial_cache::{
    PartialPoolCache, StreamAddressSet, select_stream_targets_with_epoch,
};
use crate::services::pipeline_survival::PipelineSurvival;
use crate::services::state_cache::StateCache;
use crate::services::state_refresh::StateRefreshService;

struct LfCpuWork {
    graph_cache: Arc<Mutex<GraphCache>>,
    cache: Arc<StateCache>,
    arena: StateArena,
    pool_metas: Arc<Vec<crate::pipeline::types::PoolMeta>>,
    dirty_pools: Vec<PoolIndex>,
    state_generation: u64,
    lf_pass: u64,
    max_paths: usize,
    max_hops: u32,
    cycle_finder: crate::config::CycleFinderMode,
    /// Prior-tick MATIC rates — enough for economic probe sizing at enumeration.
    prior_rates: Arc<FxHashMap<TokenIndex, U256>>,
    token_decimals: Arc<FxHashMap<Address, u8>>,
    gas_price_wei: Option<U256>,
    flash_liquidity: Arc<FlashLiquidityCache>,
    /// Topic-observed pools just admitted — growth rebuilds keep the cycle cache,
    /// so force an incremental refind or those venues never appear on HF cycles.
    force_cycle_refind: bool,
    /// Arena indices for `force_cycle_refind` — pinned through diversity selection.
    observed_pool_indices: Vec<PoolIndex>,
}

struct LfCpuResult {
    graph: Arc<crate::pipeline::types::RoutingGraph>,
    cycles: Arc<Vec<crate::core::types::FoundCycle>>,
    /// Cycle enumeration only (0 when graph cache reused).
    enumeration_ms: u64,
    enumerated_cycles: usize,
    /// Early attach hit ATTACH_MISSING_CAP — skip same-tick post-rate attach/refind.
    attach_hit_cap: bool,
}

/// Keep cycles that touch freshly observed pools, then fill remaining slots with
/// the normal protocol-diverse selection (so live WSS venues are not dropped).
fn pin_cycles_touching_pools(
    cycles: Vec<crate::core::types::FoundCycle>,
    pin_pools: &[PoolIndex],
    max_cycles: usize,
    // Exclusive observed admit: never fill snap with non-pin cycles (they
    // burn prune + empty the publish window).
    pins_only: bool,
) -> Vec<crate::core::types::FoundCycle> {
    if max_cycles == 0 || cycles.is_empty() || pin_pools.is_empty() {
        return finalize_enumerated_cycles(cycles, max_cycles);
    }
    let pin: rustc_hash::FxHashSet<PoolIndex> = pin_pools.iter().copied().collect();
    let mut pinned = Vec::new();
    let mut rest = Vec::with_capacity(cycles.len());
    for cycle in cycles {
        if cycle
            .edges
            .iter()
            .any(|edge| pin.contains(&edge.pool_index))
        {
            pinned.push(cycle);
        } else {
            rest.push(cycle);
        }
    }
    if pinned.is_empty() {
        crate::info!(
            "stream observed-live: enum_touch=0/{} pin_pools={} (DFS found none)",
            rest.len(),
            pin_pools.len()
        );
        if pins_only {
            return Vec::new();
        }
        return finalize_enumerated_cycles(rest, max_cycles);
    }
    let enum_touch = pinned.len();
    let is_uni_only = |c: &crate::core::types::FoundCycle| {
        c.edges.iter().all(|e| {
            matches!(
                e.protocol,
                crate::core::types::ProtocolType::UniswapV2
                    | crate::core::types::ProtocolType::UniswapV3
                    | crate::core::types::ProtocolType::UniswapV4
            )
        })
    };
    let uni_only = pinned.iter().filter(|c| is_uni_only(c)).count();
    let ratio_gt_one = pinned.iter().filter(|c| c.cycle_ratio > ONE).count();
    // Prefer Uni-only + ratio>ONE pins — Balancer mixes and ONE-inject junk
    // burn HF probe (live: touch=32 → drop_obs=32; near_net with cover≪gas).
    pinned.sort_by(|a, b| {
        is_uni_only(b)
            .cmp(&is_uni_only(a))
            .then_with(|| (b.cycle_ratio > ONE).cmp(&(a.cycle_ratio > ONE)))
            .then_with(|| crate::pipeline::types::compare_cycle_score(a, b))
    });
    pinned.truncate(max_cycles);
    let pin_kept = pinned.len();
    if !pins_only {
        let mut seen: rustc_hash::FxHashSet<u64> = pinned
            .iter()
            .map(|c| crate::pipeline::cycle_filter::cycle_key(&c.edges))
            .collect();
        let fill = finalize_enumerated_cycles(rest, max_cycles.saturating_sub(pinned.len()));
        for cycle in fill {
            let key = crate::pipeline::cycle_filter::cycle_key(&cycle.edges);
            if seen.insert(key) {
                pinned.push(cycle);
                if pinned.len() >= max_cycles {
                    break;
                }
            }
        }
    }
    crate::info!(
        "stream observed-live: enum_touch={enum_touch} uni_only={uni_only} ratio_gt_one={ratio_gt_one} pinned_cycles={pin_kept} total={} (cap={max_cycles})",
        pinned.len()
    );
    pinned
}

/// Token endpoints of observed pools — seed incremental DFS (hub starts miss peripherals).
/// Prefer arena/vault legs (graph edges) over discovery meta order — Balancer meta
/// often lists 2 tokens while Enter edges sit on other vault tokens (live:
/// in_graph=N/N enum_touch=0 with first-hop pin).
/// When `graph` is set, also seed every token that already has a live edge into a pin
/// (live: pin_covered>0 first_hop_pin=0 — Enter sits off meta/arena seed set).
fn observed_pool_start_tokens(
    arena: &StateArena,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    pools: &[PoolIndex],
    graph: Option<&crate::pipeline::types::RoutingGraph>,
) -> Vec<TokenIndex> {
    if pools.is_empty() {
        return Vec::new();
    }
    let want: rustc_hash::FxHashSet<PoolIndex> = pools.iter().copied().collect();
    let mut out = Vec::new();
    let mut seen = rustc_hash::FxHashSet::default();
    for meta in pool_metas {
        if !want.contains(&meta.pool_index) {
            continue;
        }
        let mut added = false;
        if let Some(state) = arena.pool_state(meta.pool_index) {
            for leg in funded_token_indices(state, meta) {
                if let Some(t) = routing_token_at_leg(arena, state, meta, leg as usize)
                    && seen.insert(t)
                {
                    out.push(t);
                    added = true;
                }
            }
        }
        if !added {
            for &t in &meta.tokens {
                if seen.insert(t) {
                    out.push(t);
                }
            }
        }
    }
    if let Some(graph) = graph {
        let token_n = graph.token_count as usize;
        for (ti, edges) in graph.adjacency.iter().enumerate().take(token_n) {
            if edges.iter().any(|ge| {
                want.contains(&ge.edge.pool_index)
                    && crate::pipeline::cycle_finder::is_live_graph_edge(ge)
            }) {
                let t = TokenIndex(ti as u32);
                if seen.insert(t) {
                    out.push(t);
                }
            }
        }
    }
    out
}

/// Drop cycles that touch dust V2 hops before they poison the HF window.
///
/// Graph eligibility now matches the HF floor, but cached routes and mid-tick
/// reserve drains still leave `v2_dead_skip` hundreds of dead cycles per tick
/// (live: snap=325 v2_dead=311 selected=6).
fn prune_dust_v2_cycles(
    arena: &crate::pipeline::arena::StateArena,
    cycles: Vec<crate::core::types::FoundCycle>,
) -> Vec<crate::core::types::FoundCycle> {
    let before = cycles.len();
    if before == 0 {
        return cycles;
    }
    let kept: Vec<_> = cycles
        .into_iter()
        .filter(|c| crate::pipeline::local_sim::v2_any_hop_dust_reserves(arena, &c.edges).is_none())
        .collect();
    let dropped = before.saturating_sub(kept.len());
    if dropped > 0 {
        crate::info!(
            "lf cycle prune: v2_dust_drop={dropped} keep={} (graph/HF V2 floor)",
            kept.len()
        );
    }
    kept
}

/// Like [`prune_dust_v2_cycles`] but avoids clone when the cache is already clean.
fn prune_dust_v2_cycles_arc(
    arena: &crate::pipeline::arena::StateArena,
    cycles: std::sync::Arc<Vec<crate::core::types::FoundCycle>>,
) -> std::sync::Arc<Vec<crate::core::types::FoundCycle>> {
    if cycles.is_empty()
        || cycles.iter().all(|c| {
            crate::pipeline::local_sim::v2_any_hop_dust_reserves(arena, &c.edges).is_none()
        })
    {
        return cycles;
    }
    std::sync::Arc::new(prune_dust_v2_cycles(
        arena,
        cycles.iter().cloned().collect(),
    ))
}

fn cycle_search_passes(max_hops: u32, max_paths: usize) -> SmallVec<[CycleSearchPass; 2]> {
    let max_hops = max_hops.clamp(2, HOP_CAP);
    let mut passes: SmallVec<[CycleSearchPass; 2]> = SmallVec::new();
    if max_hops <= 4 {
        // One pass through the configured hop cap — avoids spending half the shared
        // DFS deadline on a 3-only tranche when max_hops is 4 (common on Polygon).
        passes.push(CycleSearchPass {
            max_hops,
            max_cycles: max_paths,
        });
        return passes;
    }

    passes.push(CycleSearchPass {
        max_hops: 3,
        max_cycles: max_paths / 2,
    });
    passes.push(CycleSearchPass {
        max_hops,
        max_cycles: max_paths,
    });
    passes
}

fn spoke_connectivity_set(
    rates: &FxHashMap<TokenIndex, U256>,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    arena: &StateArena,
) -> Arc<FxHashSet<Address>> {
    let mut resolvable = resolvable_token_set(rates, arena);
    expand_hub_spoke_resolvable(&mut resolvable, pool_metas, arena);
    Arc::new(resolvable)
}

fn lf_graph_build_gate(work: &LfCpuWork) -> GraphBuildGate {
    GraphBuildGate {
        token_to_matic_rates: Arc::clone(&work.prior_rates),
        flash: work.flash_liquidity.load(),
        flash_ttl: work.flash_liquidity.ttl(),
        spoke_connectivity: Some(spoke_connectivity_set(
            work.prior_rates.as_ref(),
            work.pool_metas.as_ref(),
            &work.arena,
        )),
    }
}

/// Merge LF-tick dirty pools with any cache writes that landed while the CPU job was queued.
fn merge_dirty_pool_indices(mut base: Vec<PoolIndex>, extra: Vec<PoolIndex>) -> Vec<PoolIndex> {
    if extra.is_empty() {
        return base;
    }
    if base.is_empty() {
        return extra;
    }
    // O(n+m) vs Vec::contains O(n*m) when both sides are large.
    let mut seen: FxHashSet<u32> =
        FxHashSet::with_capacity_and_hasher(base.len() + extra.len(), FxBuildHasher);
    for idx in &base {
        seen.insert(idx.0);
    }
    for idx in extra {
        if seen.insert(idx.0) {
            base.push(idx);
        }
    }
    base
}

/// Overlay cache-hot pool states onto the LF arena snapshot before graph rescoring.
fn overlay_dirty_cache_states(work: &mut LfCpuWork) {
    let fresh_dirty = work
        .cache
        .take_dirty_pool_indices(work.arena.address_to_pool());
    work.dirty_pools = merge_dirty_pool_indices(std::mem::take(&mut work.dirty_pools), fresh_dirty);
    if work.dirty_pools.is_empty() {
        work.state_generation = work.cache.generation();
        return;
    }
    let addrs: Vec<Address> = work
        .dirty_pools
        .iter()
        .filter_map(|idx| work.arena.pool_address(*idx))
        .collect();
    if !addrs.is_empty() {
        work.arena.apply_hot_cache(&work.cache, &addrs);
    }
    work.state_generation = work.cache.generation();
}

fn run_lf_cpu_work(mut work: LfCpuWork) -> LfCpuResult {
    overlay_dirty_cache_states(&mut work);
    let routable_count = work.pool_metas.len();
    let graph_gate = lf_graph_build_gate(&work);
    let gate_ref = graph_gate.active().then_some(&graph_gate);
    let layout_fp = work.arena.routing_layout_fingerprint();
    let eligible_count =
        count_graph_eligible_pools_with_gate(&work.arena, work.pool_metas.as_ref(), gate_ref);

    // Snapshot decisions without holding lock for duration of heavy work.
    // Pure eligible-pool growth (`connectivity_stale`) is handled by attach_missing
    // below — OR-ing it into needs_rebuild forced a full rebuild every LF warmup tick.
    let (needs_rebuild, connectivity_stale) = {
        let gc = work.graph_cache.lock();
        (
            gc.needs_connectivity_rebuild(routable_count, layout_fp),
            gc.connectivity_stale(eligible_count),
        )
    };

    let graph_action;
    // Capture cycle-cache validity *before* rebuild store can replace graph meta.
    let (cycle_cache_valid, prior_cycles) = {
        let gc = work.graph_cache.lock();
        let family_prefix = work
            .arena
            .routing_family_prefix_fingerprint(gc.cached_pool_count());
        (
            gc.cycle_cache_still_valid(routable_count, layout_fp, family_prefix),
            gc.cycles(),
        )
    };
    let mut graph = if needs_rebuild {
        graph_action = if connectivity_stale {
            "rebuild_eligible_growth"
        } else {
            "rebuild"
        };
        if connectivity_stale {
            let gc = work.graph_cache.lock();
            crate::debug!(
                "lf graph rebuild: eligible_pools={eligible_count} cached_eligible={}",
                gc.cached_eligible_pool_count()
            );
        }
        let build_started = crate::util::now_ms();
        let unpriced_pools = gate_ref.map_or(0, |gate| {
            crate::pipeline::graph::count_graph_eligible_unpriced_pools(
                &work.arena,
                work.pool_metas.as_ref(),
                gate,
            )
        });
        // Build outside lock to keep critical section short.
        let g = Arc::new(build_graph_with_gate(
            &work.arena,
            work.pool_metas.as_ref(),
            gate_ref,
        ));
        let build_ms = crate::util::now_ms().saturating_sub(build_started);
        let stats = crate::pipeline::graph::topology_stats(g.as_ref());
        stats.log_summary(graph_action);
        crate::info!(
            "graph build: ms={build_ms} eligible={eligible_count} unpriced_pools={unpriced_pools} routable_metas={routable_count}",
        );
        let mut gc = work.graph_cache.lock();
        // Keep prior cycles when PoolIndex layout is still valid (growth). Clearing
        // them here forced a full 1s DFS every warmup rebuild.
        let keep_cycles = if cycle_cache_valid {
            prior_cycles.clone()
        } else {
            None
        };
        gc.store(
            Arc::clone(&g),
            keep_cycles,
            routable_count,
            layout_fp,
            work.state_generation,
            eligible_count,
            work.arena.routing_family_prefix_fingerprint(routable_count),
        );
        g
    } else {
        graph_action = "cache";
        let mut gc = work.graph_cache.lock();
        if gc.graph().is_none() {
            let gg = Arc::new(build_graph_with_gate(
                &work.arena,
                work.pool_metas.as_ref(),
                gate_ref,
            ));
            gc.store(
                Arc::clone(&gg),
                None,
                routable_count,
                layout_fp,
                work.state_generation,
                eligible_count,
                work.arena.routing_family_prefix_fingerprint(routable_count),
            );
        }
        if work.state_generation != gc.cached_state_generation() {
            // Use helper: mutates under &mut self when rc==1 (no prior .graph() clone in scope)
            // avoiding the full RoutingGraph data clone on every state-gen change.
            gc.rescore_dirty_and_update(
                &work.arena,
                &work.dirty_pools,
                work.pool_metas.len(),
                work.state_generation,
                layout_fp,
                routable_count,
                eligible_count,
            )
            .or_else(|| gc.graph())
            .unwrap_or_else(|| {
                let gg = Arc::new(build_graph_with_gate(
                    &work.arena,
                    work.pool_metas.as_ref(),
                    gate_ref,
                ));
                gc.store(
                    Arc::clone(&gg),
                    None,
                    routable_count,
                    layout_fp,
                    work.state_generation,
                    eligible_count,
                    work.arena.routing_family_prefix_fingerprint(routable_count),
                );
                gg
            })
        } else {
            gc.graph().unwrap_or_else(|| {
                let gg = Arc::new(build_graph_with_gate(
                    &work.arena,
                    work.pool_metas.as_ref(),
                    gate_ref,
                ));
                gc.store(
                    Arc::clone(&gg),
                    None,
                    routable_count,
                    layout_fp,
                    work.state_generation,
                    eligible_count,
                    work.arena.routing_family_prefix_fingerprint(routable_count),
                );
                gg
            })
        }
    };

    // Quiet ticks: no rebuild / eligibility growth — skip O(n) scan.
    // Dirty pools are rescored above; observed pins use force_attach below.
    // Skip catchup attach on observed force-refind — it burned 256 slots + graph
    // churn before exclusive DFS (live: attached=256 dfs=0 enum_ms≈2).
    let (cached_eligible, attach_catchup_pending) = {
        let gc = work.graph_cache.lock();
        (gc.cached_eligible_pool_count(), gc.attach_catchup_pending())
    };
    let catchup_due = needs_rebuild || connectivity_stale || eligible_count > cached_eligible;
    // Observed force-refind: skip catchup attach (pins use force_attach below).
    let scan_missing = catchup_due && !work.force_cycle_refind;
    // Only a capped attach can leave this latch stale; ordinary force ticks skip the O(pool) scan.
    if catchup_due && work.force_cycle_refind && attach_catchup_pending {
        // If nothing is actually missing, drop the latch — otherwise freeze-era
        // defer left catchup_pending stuck across force ticks (arena growth halted
        // when freeze was on; latch still poisons connectivity_stale).
        if !has_missing_eligible_pools_with_gate(
            &work.arena,
            work.pool_metas.as_ref(),
            graph.as_ref(),
            gate_ref,
        ) {
            work.graph_cache.lock().set_attach_catchup_pending(false);
        }
        crate::info!(
            "lf attach_missing defer: force_refind=true stale={connectivity_stale} eligible={eligible_count} lf_pass={}",
            work.lf_pass
        );
    }
    let obs_in_graph_before = work
        .observed_pool_indices
        .iter()
        .filter(|&&p| graph.pool_has_live_edges(p))
        .count();
    let mut attach_hit_cap = false;
    let missing_graph_pools = if scan_missing
        && has_missing_eligible_pools_with_gate(
            &work.arena,
            work.pool_metas.as_ref(),
            graph.as_ref(),
            gate_ref,
        ) {
        crate::info!(
            "lf attach_missing scan: rebuild={needs_rebuild} stale={connectivity_stale} eligible={eligible_count} cached_eligible={cached_eligible} lf_pass={}",
            work.lf_pass
        );
        let g = Arc::make_mut(&mut graph);
        let report = attach_missing_eligible_pools_with_gate(
            &work.arena,
            g,
            work.pool_metas.as_ref(),
            gate_ref,
        );
        attach_hit_cap = report.hit_cap;
        // Cap leaves remainder — keep connectivity_stale true across LF passes
        // even after rescore credits full eligible_count.
        work.graph_cache
            .lock()
            .set_attach_catchup_pending(report.hit_cap);
        if report.hit_cap || report.attached_pools > 0 {
            crate::info!(
                "lf attach_missing catchup: attached={} live_after={} hit_cap={} missing_after={} missing_sample={:?} eligible={eligible_count} lf_pass={}",
                report.attached_pools,
                report.live_after,
                report.hit_cap,
                report.missing_after,
                report.missing_sample,
                work.lf_pass
            );
        }
        report.attached_pools
    } else {
        // Clear latch only when we actually scanned and found nothing missing.
        // Observed force ticks skip the scan — must not drop catchup_pending.
        if scan_missing {
            let mut gc = work.graph_cache.lock();
            if gc.attach_catchup_pending() {
                gc.set_attach_catchup_pending(false);
            }
        }
        0
    };
    // Observed WSS pools: gate can reject unpriced spokes while attach_missing
    // still attaches unrelated eligible pools (live: in_graph=0/1 attached=N).
    let mut force_attached = 0usize;
    if work.force_cycle_refind && !work.observed_pool_indices.is_empty() {
        let need_force: Vec<PoolIndex> = work
            .observed_pool_indices
            .iter()
            .copied()
            .filter(|&p| !graph.pool_has_live_edges(p))
            .collect();
        if !need_force.is_empty() {
            let g = Arc::make_mut(&mut graph);
            let mut attached = Vec::new();
            let mut rescored_stubs = 0usize;
            for meta in work.pool_metas.as_ref() {
                if !need_force.contains(&meta.pool_index) {
                    continue;
                }
                if g.pool_has_edges(meta.pool_index) {
                    // Dead stubs: price in place (no strip/reattach thrash).
                    attached.push(meta.pool_index);
                    rescored_stubs += 1;
                    continue;
                }
                // ponytail: bypass pricing gate for topic-live venues only
                if attach_pool_to_graph(g, &work.arena, meta, None) {
                    attached.push(meta.pool_index);
                }
            }
            // Direct stubs need rescore before they count as live.
            if !attached.is_empty() {
                let _ = rescore_pools_in_place(&work.arena, g, &attached);
                refresh_graph_cycle_coverage(g);
            }
            let forced: Vec<PoolIndex> = attached
                .iter()
                .copied()
                .filter(|&p| g.pool_has_live_edges(p))
                .collect();
            force_attached = forced.len();
            if force_attached > 0 {
                crate::info!(
                    "stream observed-live: force_attach={force_attached}/{} stubs={} rescored_stubs={rescored_stubs} lf_pass={}",
                    need_force.len(),
                    attached.len(),
                    work.lf_pass
                );
            } else if let Some(&pi) = need_force.first() {
                let reason = work
                    .pool_metas
                    .iter()
                    .find(|m| m.pool_index == pi)
                    .map(|_m| match work.arena.pool_state(pi) {
                        None => "no_state",
                        Some(s) if !s.is_tradable() => "untradable",
                        Some(_) if attached.contains(&pi) => "dead_after_rescore",
                        Some(_) => "inadmissible",
                    })
                    .unwrap_or("no_meta");
                crate::info!(
                    "stream observed-live: force_attach_fail={}/{} sample={reason} stubs={} rescored_stubs={rescored_stubs} lf_pass={}",
                    need_force.len(),
                    need_force.len(),
                    attached.len(),
                    work.lf_pass
                );
            }
        }
    }
    let missing_graph_pools = missing_graph_pools + force_attached;
    if missing_graph_pools > 0 {
        let stats = crate::pipeline::graph::topology_stats(graph.as_ref());
        stats.log_summary("patch_attach");
        crate::debug!(
            "lf graph patch: attached {missing_graph_pools} eligible pools missing from cached adjacency"
        );
        // Preserve cycle cache through attach — short incremental DFS merges new routes.
        let keep_cycles = if cycle_cache_valid {
            prior_cycles.clone()
        } else {
            None
        };
        work.graph_cache.lock().store(
            Arc::clone(&graph),
            keep_cycles,
            routable_count,
            layout_fp,
            work.state_generation,
            eligible_count,
            work.arena.routing_family_prefix_fingerprint(routable_count),
        );
    }

    let cached_cycles = {
        let gc = work.graph_cache.lock();
        gc.cycles()
    }
    .or(prior_cycles);
    let need_cycle_refind = {
        let gc = work.graph_cache.lock();
        // Newly attached pools are invisible to cached cycles until we re-enumerate.
        // Growth rebuilds no longer clear the cycle cache when indices stay valid.
        // Capped catchup alone: store graph, defer enum to interval / final uncapped
        // attach / observed force (live: every catchup tick burned ~1s DFS).
        (needs_rebuild && !cycle_cache_valid)
            || (missing_graph_pools > 0
                && (work.force_cycle_refind || force_attached > 0 || !attach_hit_cap))
            || work.force_cycle_refind
            || gc.cycles().is_none()
            || cached_cycles.is_none()
            || gc.needs_cycle_refind(
                routable_count,
                layout_fp,
                work.state_generation,
                work.dirty_pools.len(),
                work.arena.pool_count(),
            )
    };
    // Incremental only when topology unchanged. Fresh attaches need the full budget —
    // patch DFS under-explores new adjacency (live: cycles_touching=0 after attach).
    let incremental_refind = !needs_rebuild
        && cycle_cache_valid
        && cached_cycles.as_ref().is_some_and(|c| !c.is_empty())
        && missing_graph_pools == 0
        && (work.force_cycle_refind || connectivity_stale);
    let mut enumeration_ms = 0u64;
    let (cycles, enumerated_cycles) = if need_cycle_refind {
        crate::debug!(
            "lf cycle refind: pass={} dirty_pools={} state_gen={} incremental={} attached={} rebuild={}",
            work.lf_pass,
            work.dirty_pools.len(),
            work.state_generation,
            incremental_refind,
            missing_graph_pools,
            needs_rebuild
        );
        let passes = cycle_search_passes(work.max_hops, work.max_paths);
        // ponytail: defer priced-start when seeding observed endpoints — prior_rates
        // lack peripheral tokens and finalize_cycles was wiping enum_touch to 0.
        let probe_ctx = ProbeContext {
            token_to_matic_rates: if work.force_cycle_refind {
                None
            } else {
                Some(work.prior_rates.as_ref())
            },
            token_decimals: Some(work.token_decimals.as_ref()),
            gas_price_wei: work.gas_price_wei,
        };
        // Arena tokens are append-only; a cached graph may lag. Grow token slots
        // before DFS/BF so used_tokens / dist arrays cover every TokenIndex.
        if work.arena.token_count() > graph.token_count {
            Arc::make_mut(&mut graph).ensure_token_capacity(work.arena.token_count());
        }
        // Fresh attaches leave Direct ratio=0 until rescore; exclusive inject
        // walks them with ONE fillers that rank as ZeroProfit. Rescore pins first.
        if work.force_cycle_refind && !work.observed_pool_indices.is_empty() {
            let g = Arc::make_mut(&mut graph);
            let _ = rescore_pools_in_place(&work.arena, g, &work.observed_pool_indices);
        }
        let enum_started = crate::util::now_ms();
        let obs_starts = observed_pool_start_tokens(
            &work.arena,
            work.pool_metas.as_ref(),
            &work.observed_pool_indices,
            Some(graph.as_ref()),
        );
        let mut first_hop_pin = 0usize;
        let mut pin_covered = 0usize;
        if !work.observed_pool_indices.is_empty() {
            let obs_in_graph = work
                .observed_pool_indices
                .iter()
                .filter(|&&p| graph.pool_has_live_edges(p))
                .count();
            let pin: FxHashSet<PoolIndex> = work.observed_pool_indices.iter().copied().collect();
            // Count opening Enter/Direct edges from seed tokens into pin pools.
            for &t in &obs_starts {
                let Some(edges) = graph.adjacency.get(t.0 as usize) else {
                    continue;
                };
                for ge in edges {
                    if pin.contains(&ge.edge.pool_index)
                        && crate::pipeline::cycle_finder::is_live_graph_edge(ge)
                    {
                        first_hop_pin = first_hop_pin.saturating_add(1);
                    }
                }
            }
            pin_covered = graph.coverage.as_ref().map_or(0, |cov| {
                work.observed_pool_indices
                    .iter()
                    .filter(|p| cov.pool_indices.contains(&p.0))
                    .count()
            });
            crate::info!(
                "stream observed-live: dfs_seed tokens={} pools={} in_graph={obs_in_graph}/{} (pre_attach={obs_in_graph_before}) attached={missing_graph_pools} force_attach={force_attached} first_hop_pin={first_hop_pin} pin_covered={pin_covered} incremental={incremental_refind} exclusive={} lf_pass={}",
                obs_starts.len(),
                work.observed_pool_indices.len(),
                work.observed_pool_indices.len(),
                work.force_cycle_refind,
                work.lf_pass
            );
        }
        // Exclusive obs starts when admitting — hub fill was starving SharedCycleCap
        // (live: in_graph=N/N enum_touch=0 after attach).
        // Bridge-only pins (pin_covered=0) cannot form multi-pool cycles; exclusive
        // DFS always returns raw=0 and used to wipe the snap when prior was empty.
        let pin_bridge_only =
            work.force_cycle_refind && !work.observed_pool_indices.is_empty() && pin_covered == 0;
        let exclusive_obs = work.force_cycle_refind && !obs_starts.is_empty() && !pin_bridge_only;
        let first_hop = if exclusive_obs {
            work.observed_pool_indices.as_slice()
        } else {
            &[]
        };
        // Skip atomic prefilter on exclusive obs — pin cycles are for hot-path
        // coverage, not profit rank (live: first_hop_pin>0 → enum_touch=0).
        let atomic_prefilter = !exclusive_obs;
        let outcome = if pin_bridge_only {
            crate::info!(
                "stream observed-live: pin_bridge_skip first_hop_pin={first_hop_pin} pin_pools={} — no multi-pool close lf_pass={}",
                work.observed_pool_indices.len(),
                work.lf_pass
            );
            crate::pipeline::cycle_search::CycleSearchOutcome {
                cycles: Vec::new(),
                diag: crate::pipeline::cycle_search::CycleSearchDiagnostics {
                    mode: work.cycle_finder,
                    start_tokens: obs_starts.len(),
                    ..Default::default()
                },
            }
        } else if incremental_refind {
            find_cycles_for_mode_with_budget(
                work.cycle_finder,
                &work.arena,
                &graph,
                work.pool_metas.as_ref(),
                passes.as_slice(),
                atomic_prefilter,
                Some(&probe_ctx),
                CYCLE_ENUM_PATCH_BUDGET,
                &obs_starts,
                exclusive_obs,
                first_hop,
                exclusive_obs,
            )
        } else if obs_starts.is_empty() {
            find_cycles_for_mode(
                work.cycle_finder,
                &work.arena,
                &graph,
                work.pool_metas.as_ref(),
                passes.as_slice(),
                true,
                Some(&probe_ctx),
            )
        } else {
            find_cycles_for_mode_with_budget(
                work.cycle_finder,
                &work.arena,
                &graph,
                work.pool_metas.as_ref(),
                passes.as_slice(),
                atomic_prefilter,
                Some(&probe_ctx),
                crate::pipeline::cycle_finder::CYCLE_ENUM_TIME_BUDGET,
                &obs_starts,
                exclusive_obs,
                first_hop,
                exclusive_obs,
            )
        };
        // ponytail: dropped relax-uni-or-pin retry — live still dfs=0 (sparse pin
        // close); keeping-prior already covers empty exclusive.
        if exclusive_obs || work.force_cycle_refind || pin_bridge_only {
            crate::info!(
                "stream observed-live: cycle_search raw={} dedupe={} out={} dfs={} starts={} enum_ms={} lf_pass={}",
                outcome.diag.raw_collected,
                outcome.diag.post_dedupe,
                outcome.diag.post_prefilter,
                outcome.diag.dfs_raw,
                outcome.diag.start_tokens,
                outcome.diag.enumerate_ms,
                work.lf_pass
            );
        } else {
            outcome.diag.log_summary();
        }
        let mut result = outcome.cycles;
        // Merge cache on observed admit. Exclusive: only Uni-only cached
        // cycles that already touch a pin pool (full Uni cache re-poisoned
        // pins; skipping cache zeroed enum_touch when DFS closed none).
        // Bridge-only pins: keep full prior cache (no pin filter — there is no close).
        if (incremental_refind || work.force_cycle_refind || pin_bridge_only)
            && let Some(cached) = cached_cycles.as_ref()
        {
            if exclusive_obs {
                let pin: rustc_hash::FxHashSet<PoolIndex> =
                    work.observed_pool_indices.iter().copied().collect();
                result.extend(
                    cached
                        .iter()
                        .filter(|c| {
                            c.edges.iter().any(|e| pin.contains(&e.pool_index))
                                && c.edges.iter().all(|e| {
                                    matches!(
                                        e.protocol,
                                        crate::core::types::ProtocolType::UniswapV2
                                            | crate::core::types::ProtocolType::UniswapV3
                                            | crate::core::types::ProtocolType::UniswapV4
                                    )
                                })
                        })
                        .cloned(),
                );
            } else {
                result.reserve(cached.len());
                result.extend(cached.iter().cloned());
            }
        }
        if work.lf_pass <= 2 || work.lf_pass.is_multiple_of(30) {
            let mut hop_hist = [0u32; HOP_CAP as usize + 1];
            for c in &result {
                let h = c.edge_hops().min(HOP_CAP) as usize;
                hop_hist[h] = hop_hist[h].saturating_add(1);
            }
            crate::debug!(
                "cycle search hops: max_hops={} passes={} pre_diversity={} by_hop={hop_hist:?}",
                work.max_hops,
                passes.len(),
                result.len()
            );
        }
        enumeration_ms = crate::util::now_ms().saturating_sub(enum_started);
        let enumerated_cycles = result.len();
        // pins_only only when exclusive pin search ran; bridge-skip keeps full prior.
        let pins_only = exclusive_obs;
        let diversified = if work.observed_pool_indices.is_empty() || pin_bridge_only {
            finalize_enumerated_cycles(result, work.max_paths)
        } else {
            pin_cycles_touching_pools(
                result,
                &work.observed_pool_indices,
                work.max_paths,
                pins_only,
            )
        };
        // Exclusive / bridge pin miss used to store [] and wipe a good prior snap
        // (live: cycles_touching=6 → next admit 0/0).
        let cycles = if (exclusive_obs || pin_bridge_only) && diversified.is_empty() {
            if let Some(prior) = cached_cycles.as_ref().filter(|c| !c.is_empty()) {
                crate::info!(
                    "stream observed-live: exclusive empty — keeping prior cache cycles={} lf_pass={}",
                    prior.len(),
                    work.lf_pass
                );
                // Re-publish a pruned copy so dust V2 does not linger forever.
                prune_dust_v2_cycles_arc(&work.arena, Arc::clone(prior))
            } else {
                // Prior wiped (e.g. old post-rate store) — refill with hub DFS so
                // we do not publish an empty snap (live: cycles=0 until next interval).
                crate::info!(
                    "stream observed-live: exclusive empty no prior — hub refill lf_pass={}",
                    work.lf_pass
                );
                let refill = find_cycles_for_mode(
                    work.cycle_finder,
                    &work.arena,
                    &graph,
                    work.pool_metas.as_ref(),
                    passes.as_slice(),
                    true,
                    Some(&probe_ctx),
                );
                Arc::new(prune_dust_v2_cycles(
                    &work.arena,
                    finalize_enumerated_cycles(refill.cycles, work.max_paths),
                ))
            }
        } else {
            Arc::new(prune_dust_v2_cycles(&work.arena, diversified))
        };
        if work.lf_pass <= 2 || work.lf_pass.is_multiple_of(30) {
            crate::debug!(
                "cycle search diversity: cap={} post_diversity={}",
                work.max_paths,
                cycles.len()
            );
        }
        work.graph_cache.lock().store(
            Arc::clone(&graph),
            Some(Arc::clone(&cycles)),
            routable_count,
            layout_fp,
            work.state_generation,
            eligible_count,
            work.arena.routing_family_prefix_fingerprint(routable_count),
        );
        (cycles, enumerated_cycles)
    } else {
        let cached = cached_cycles.unwrap_or_default();
        // Rescore may have killed V2 legs mid-cache — drop dust before HF sees them.
        let pre_prune = cached.len();
        let pruned = prune_dust_v2_cycles_arc(&work.arena, cached);
        if work.lf_pass <= 2 || work.lf_pass.is_multiple_of(30) {
            crate::debug!(
                "lf cycle cache: pass={} cycles={} (pre_prune={pre_prune})",
                work.lf_pass,
                pruned.len(),
            );
        }
        // Keep cache metadata current even when cycles are reused.
        work.graph_cache.lock().store(
            Arc::clone(&graph),
            Some(Arc::clone(&pruned)),
            routable_count,
            layout_fp,
            work.state_generation,
            eligible_count,
            work.arena.routing_family_prefix_fingerprint(routable_count),
        );
        let enumerated_cycles = pruned.len();
        (pruned, enumerated_cycles)
    };

    if graph_action == "cache"
        && missing_graph_pools == 0
        && (work.lf_pass <= 2 || work.lf_pass.is_multiple_of(30))
    {
        crate::pipeline::graph::topology_stats(graph.as_ref()).log_summary("cache_hit");
    }

    LfCpuResult {
        graph,
        cycles,
        enumeration_ms,
        enumerated_cycles,
        attach_hit_cap,
    }
}

async fn run_lf_cpu_async(work: LfCpuWork) -> anyhow::Result<LfCpuResult> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    // spawn alone binds pool TLS for nested par_iter/join (no nested install).
    crate::util::lf_cpu_pool().spawn(move || {
        let _ = tx.send(run_lf_cpu_work(work));
    });
    rx.await.context("lf cpu worker dropped")
}

pub struct LfContext {
    pub config: Arc<AppConfig>,
    pub refresh: Arc<StateRefreshService>,
    pub cache: Arc<StateCache>,
    pub snapshots: Arc<SnapshotStore>,
    pub stream_addresses: StreamAddressSet,
    pub partial_cache: Arc<PartialPoolCache>,
    pub price_oracle: Arc<PriceOracle>,
    pub gas_oracle: Arc<GasOracle>,
    pub rpc: Arc<RpcPool>,
    pub graph_cache: Arc<Mutex<GraphCache>>,
    pub arena: Arc<Mutex<StateArena>>,
    pub tick_lock: Arc<AsyncMutex<()>>,
    pub ui_hook: SharedUiHook,
    pub flash_liquidity: Arc<FlashLiquidityCache>,
}

pub async fn run_lf_tick(ctx: &LfContext, shutdown: &watch::Receiver<bool>) -> anyhow::Result<()> {
    if *shutdown.borrow() {
        return Ok(());
    }
    let lf_started = crate::util::now_ms();
    let lf_pass = ctx.graph_cache.lock().advance_pass();
    let refresh_batch = ctx.refresh.lf_refresh_batch(lf_pass);
    if lf_pass <= 2 || lf_pass.is_multiple_of(30) {
        crate::info!("lf refresh: pass={lf_pass} batch={refresh_batch}");
    }

    let discovery_started = crate::util::now_ms();
    let _ = ctx.refresh.maybe_discover().await?;
    let discovery_ms = crate::util::now_ms().saturating_sub(discovery_started);

    // Promote topic-observed venues into this tick's refresh before arena sync.
    // Filter to discovery-routable streamable pools — live wssobs was dominated by
    // disabled QS/Uni V2 (`0x882df4…`) that can never match the index / arena.
    let observed_raw = ctx.partial_cache.take_observed_live();
    let observed = ctx.refresh.filter_observed_live_routable(&observed_raw);
    if !observed_raw.is_empty() {
        crate::debug!(
            "stream observed-live: topic_n={} routable={} skipped={}",
            observed_raw.len(),
            observed.len(),
            observed_raw.len().saturating_sub(observed.len())
        );
    }
    if !observed.is_empty() {
        let mut hot = ctx.refresh.hot_addresses().as_ref().to_vec();
        hot.extend(observed.iter().copied());
        hot.sort_unstable();
        hot.dedup();
        ctx.refresh.set_hot_addresses(hot);
    }

    let refresh_started = crate::util::now_ms();
    let refresh_result = ctx.refresh.refresh_pool_states(refresh_batch).await?;
    let refreshed_pools = refresh_result.updated;
    let refresh_ms = crate::util::now_ms().saturating_sub(refresh_started);
    if lf_pass <= 2 || lf_pass.is_multiple_of(10) {
        crate::info!(
            "lf refresh result: pass={lf_pass} batch={refresh_batch} updated={} attempted={} refresh_ms={refresh_ms} routable_pools={}",
            refresh_result.updated,
            refresh_result.attempted,
            ctx.refresh.routable_pool_count()
        );
    }
    ctx.refresh.prune_dead_pools_if_due(lf_pass);

    let pools = ctx.refresh.discovered_pools();
    if pools.is_empty() {
        if ctx.refresh.is_discovery_bootstrapping() {
            crate::info!("lf tick waiting for postgres bootstrap to finish");
        } else {
            crate::warn!("lf tick skipped: no discovered pools after bootstrap");
        }
        ctx.ui_hook.on_lf_complete(0, 0, pools.len());
        return Ok(());
    }
    if lf_pass <= 2 {
        crate::services::discovery::log_protocol_distribution(&pools);
    }

    let mut arena = ctx.arena.lock().clone();
    let arena_pools_before = arena.pool_count();
    crate::debug!(
        "lf sync: discovered={}, cache_size={}",
        pools.len(),
        ctx.cache.len()
    );
    let mut decimals = ctx.refresh.token_decimals_map();
    // Backfill decimals for tokens already in the arena (discovery only enriches
    // the current PG batch). Hub-first cap keeps free-tier multicall productive.
    if lf_pass == 1 || lf_pass.is_multiple_of(5) {
        let gap = arena_missing_decimal_addresses(&arena, decimals.as_ref(), 256);
        if !gap.is_empty() {
            let n = gap.len();
            ctx.refresh.enrich_token_decimals(gap).await;
            decimals = ctx.refresh.token_decimals_map();
            crate::debug!("token decimals lf backfill: requested={n} map={}", decimals.len());
        }
    }
    let arena_sync_started = crate::util::now_ms();
    // ponytail: no freeze_append — latch+force_refind defer stuck arena at 1019
    // forever (live cycle97). ARENA_APPEND_CAP paces growth vs attach.
    let arena_sync =
        ctx.refresh
            .sync_routable_arena_gated(&mut arena, Some(decimals.as_ref()), false);
    // Persist immediately — end-of-tick writeback was skipped on shutdown returns
    // and left the next LF cloning an empty arena (live: indices_rebuilt every
    // few passes with growing pool_metas from cold Rebuild).
    *ctx.arena.lock() = arena.clone();
    if arena_sync.indices_rebuilt {
        // Growth+layout-fp change used to keep poison cycles (live: snap=187
        // selected=0 proto_mismatch=187 after arena Rebuild).
        ctx.graph_cache.lock().invalidate_cycles();
        crate::info!(
            "lf arena sync: indices_rebuilt=true before={} pool_metas={} (cycle cache cleared)",
            arena_pools_before,
            arena_sync.metas.len()
        );
    }
    if !arena_sync.invalidated_pool_indices.is_empty() {
        crate::info!(
            "lf arena sync: invalidated={} preserved_indices=true arena_pools={}",
            arena_sync.invalidated_pool_indices.len(),
            arena.pool_count()
        );
    }
    let invalidated_pool_indices = arena_sync.invalidated_pool_indices;
    let pool_metas = Arc::new(arena_sync.metas);
    let arena_sync_ms = crate::util::now_ms().saturating_sub(arena_sync_started);
    if lf_pass <= 2 || lf_pass.is_multiple_of(30) {
        let hints_missing = arena_tokens_without_decimal_hints(&arena, decimals.as_ref());
        crate::debug!(
            "token arena: tokens={} decimals_map={} missing_hints={} pool_metas={}",
            arena.token_count(),
            decimals.len(),
            hints_missing,
            pool_metas.len()
        );
    }
    let max_paths = ctx.config.routing.enumeration_max_paths as usize;
    let max_hops = ctx.config.routing.max_hops;

    let prior_rates = ctx.snapshots.token_to_matic_rates();

    let state_provider = ctx.rpc.connect_state().ok();
    let state_generation = ctx.cache.generation();
    let dirty_pools = merge_dirty_pool_indices(
        ctx.cache.take_dirty_pool_indices(arena.address_to_pool()),
        invalidated_pool_indices,
    );
    let gas_price_wei = ctx.gas_oracle.conservative_gas_price();
    if gas_price_wei.is_none() && lf_pass <= 2 {
        crate::debug!("lf cycle prefilter: gas oracle not warm yet (no gas floor at enumeration)");
    }
    let observed_pool_indices: Vec<PoolIndex> = {
        let map = arena.address_to_pool();
        observed
            .iter()
            .filter_map(|addr| map.get(addr).copied())
            .collect()
    };
    let force_cycle_refind = !observed_pool_indices.is_empty();
    // Keep a copy — post-CPU `finalize_enumerated_cycles` was dropping pinned
    // live cycles (forceenum: pinned_cycles≤14 in CPU, cycles_touching=0 after).
    let observed_pin = observed_pool_indices.clone();
    let cpu_work = LfCpuWork {
        graph_cache: Arc::clone(&ctx.graph_cache),
        cache: Arc::clone(&ctx.cache),
        arena: arena.clone(),
        pool_metas: Arc::clone(&pool_metas),
        dirty_pools,
        state_generation,
        lf_pass,
        max_paths,
        max_hops,
        cycle_finder: ctx.config.routing.cycle_finder,
        prior_rates: Arc::clone(&prior_rates),
        token_decimals: Arc::clone(&decimals),
        gas_price_wei,
        flash_liquidity: Arc::clone(&ctx.flash_liquidity),
        force_cycle_refind,
        observed_pool_indices,
    };
    // ponytail: skip enrich_all_token_to_matic_rates — prior_rates from last tick
    // is sufficient for resolvable set computation. Cycle-token enrichment below
    // provides fresh rates for profit evaluation, saving one oracle RPC pass.
    let cpu_started = crate::util::now_ms();
    let cpu = run_lf_cpu_async(cpu_work).await;
    let cpu = cpu?;
    let cpu_ms = crate::util::now_ms().saturating_sub(cpu_started);
    if *shutdown.borrow() {
        return Ok(());
    }

    let resolvable_count =
        spoke_connectivity_set(prior_rates.as_ref(), pool_metas.as_ref(), &arena).len();

    let cycles_arc = cpu.cycles;
    let mut routing_graph = cpu.graph;
    let cycle_search_ms = cpu.enumeration_ms;
    let enumerated_cycles = cpu.enumerated_cycles;

    // ponytail: collect unique cycle tokens without intermediate HashSet→Vec copy
    let mut cycle_tokens: Vec<TokenIndex> =
        Vec::with_capacity(cycles_arc.len().saturating_mul(4).max(16));
    let mut cycle_tokens_set = rustc_hash::FxHashSet::default();
    for c in cycles_arc.iter() {
        if cycle_tokens_set.insert(c.start_token) {
            cycle_tokens.push(c.start_token);
        }
        for e in &c.edges {
            if cycle_tokens_set.insert(e.token_in) {
                cycle_tokens.push(e.token_in);
            }
            if cycle_tokens_set.insert(e.token_out) {
                cycle_tokens.push(e.token_out);
            }
        }
    }
    for hub in POLYGON_HUB_TOKENS {
        if let Some(&idx) = arena.address_to_token().get(&hub)
            && cycle_tokens_set.insert(idx)
        {
            cycle_tokens.push(idx);
        }
    }

    let ticks_started = crate::util::now_ms();
    let mut v3_tick_targets = 0usize;
    let mut v3_ticks_loaded = 0usize;
    let mut v3_ticks_ms = 0u64;
    let mut v4_tick_targets = 0usize;
    let mut v4_ticks_loaded = 0usize;
    let mut v4_ticks_ms = 0u64;
    if state_provider.is_some() {
        let tick_pools = collect_v3_pool_addresses(&arena, cycles_arc.as_ref());
        let v4_tick_pools = collect_v4_tick_targets(cycles_arc.as_ref(), pool_metas.as_ref());
        let state_block = ctx.refresh.last_state_block();
        let pinned_block = (state_block > 0).then_some(state_block);
        // Only hydrate pools missing ticks — arena sync now preserves CL ticks
        // when sqrt_price/liquidity/tick are unchanged, so most LF passes skip RPC.
        let tick_pools_needed: Vec<Address> = tick_pools
            .iter()
            .copied()
            .filter(|addr| {
                // Skip pools that stayed empty after a recent full hydrate.
                if crate::pipeline::tick_fetch::is_empty_tick_on_cooldown(*addr) {
                    return false;
                }
                let Some(&idx) = arena.address_to_pool().get(addr) else {
                    return false;
                };
                match arena.pool_state(idx) {
                    Some(crate::core::types::PoolState::V3(s)) => s.ticks.is_empty(),
                    _ => true,
                }
            })
            .collect();
        let v4_tick_pools_needed: Vec<_> = v4_tick_pools
            .iter()
            .copied()
            .filter(|(idx, pool_id)| {
                // Skip pools that stayed empty after a recent full hydrate.
                if crate::pipeline::tick_fetch::is_empty_v4_tick_on_cooldown(*pool_id) {
                    return false;
                }
                match arena.pool_state(*idx) {
                    Some(crate::core::types::PoolState::V4(s)) => s.ticks.is_empty(),
                    _ => true,
                }
            })
            .collect();
        // Log how many cycle CL pools still need hydration (not the full set).
        v3_tick_targets = tick_pools_needed.len();
        v4_tick_targets = v4_tick_pools_needed.len();
        if !tick_pools_needed.is_empty() || !v4_tick_pools_needed.is_empty() {
            let (algebra_pools, algebra_integral_pools) =
                crate::pipeline::tick_fetch::collect_algebra_pools(
                    &arena,
                    pool_metas.as_ref(),
                    &tick_pools_needed,
                );
            // Clear once before URL fallback — retry must not wipe a family that
            // already hydrated when the other family's RPC failed.
            crate::pipeline::tick_fetch::clear_v3_pool_ticks(&mut arena, &tick_pools_needed);
            crate::pipeline::tick_fetch::clear_v4_pool_ticks(&mut arena, &v4_tick_pools_needed);
            let pass = hydrate_cl_ticks_with_rpc_fallback(
                &ctx.rpc,
                &mut arena,
                &tick_pools_needed,
                &v4_tick_pools_needed,
                &algebra_pools,
                &algebra_integral_pools,
                ctx.config.oracle.tick_word_range,
                ctx.config.oracle.tick_word_range,
                true,
                pinned_block,
                "LF tick hydration",
            )
            .await;
            v3_ticks_loaded = pass.v3_loaded;
            v4_ticks_loaded = pass.v4_loaded;
            v3_ticks_ms = pass.v3_ms;
            v4_ticks_ms = pass.v4_ms;
        } // tick_pools_needed non-empty
    }
    let ticks_ms = crate::util::now_ms().saturating_sub(ticks_started);

    let finalize_started = crate::util::now_ms();
    // Unique owner after CPU worker → mutate in place (no deep copy).
    let capped = Arc::unwrap_or_clone(cycles_arc);
    // Hold topic-live cycles aside before post-hydrate dead prune — rescore was
    // zeroing them out (repin: CPU pinned_cycles≤23, post-filter touching=0).
    let observed_pin_set: rustc_hash::FxHashSet<PoolIndex> = observed_pin.iter().copied().collect();
    let pre_hold_touching = if observed_pin_set.is_empty() {
        0
    } else {
        capped
            .iter()
            .filter(|cycle| {
                cycle
                    .edges
                    .iter()
                    .any(|edge| observed_pin_set.contains(&edge.pool_index))
            })
            .count()
    };
    if !observed_pin.is_empty() {
        crate::info!(
            "stream observed-live: pre_hold_touching={pre_hold_touching}/{} lf_pass={lf_pass}",
            capped.len()
        );
    }
    let (mut live_held, mut capped) = if observed_pin_set.is_empty() {
        (Vec::new(), capped)
    } else {
        capped.into_iter().partition(|cycle| {
            cycle
                .edges
                .iter()
                .any(|edge| observed_pin_set.contains(&edge.pool_index))
        })
    };
    let mut table = SpotTable::new(arena.pool_count());
    table.populate_from_graph(&routing_graph);
    // Spot gas for ranking (no 12.5% submit buffer) so near-miss 2-hops aren't
    // crowded out by deep ratio-only dust that cannot clear live gas.
    let rank_gas = ctx
        .gas_oracle
        .loaded_snapshot()
        .map(crate::services::execution::gas::compute_assessment_gas_price)
        .or(gas_price_wei);
    rescore_cycles_with_table_and_gas(
        &arena,
        &table,
        &mut capped,
        rank_gas,
        Some(prior_rates.as_ref()),
        Some(decimals.as_ref()),
        None,
    );
    // Prune dead / underwater / fee-drag losers (hop-scaled floor for 3+ hops).
    capped.retain(|c| {
        if c.score >= crate::pipeline::cycle_finder::DEAD_EDGE_LOG_WEIGHT {
            return false;
        }
        if c.cycle_ratio.is_zero() {
            return false;
        }
        c.cycle_ratio >= min_profitable_cycle_ratio(c.edge_hops())
    });
    // Protocol diversity (buckets ranked by gas-aware execution score).
    capped = finalize_enumerated_cycles(capped, max_paths.saturating_sub(live_held.len()));
    if !live_held.is_empty() {
        // Rescore held pins with post-hydrate weights; still skip dead-prune so WSS
        // topic-live cycles survive a transient zero after tick hydrate.
        rescore_cycles_with_table_and_gas(
            &arena,
            &table,
            &mut live_held,
            rank_gas,
            Some(prior_rates.as_ref()),
            Some(decimals.as_ref()),
            None,
        );
        let held_gt_one = live_held.iter().filter(|c| c.cycle_ratio > ONE).count();
        crate::info!(
            "stream observed-live: live_held_ratio_gt_one={held_gt_one}/{} lf_pass={lf_pass}",
            live_held.len()
        );
        live_held.sort_by(crate::pipeline::types::compare_cycle_execution);
        live_held.truncate(32);
        let mut seen: rustc_hash::FxHashSet<u64> = live_held
            .iter()
            .map(|c| crate::pipeline::cycle_filter::cycle_key(&c.edges))
            .collect();
        let mut merged = live_held;
        for cycle in capped {
            let key = crate::pipeline::cycle_filter::cycle_key(&cycle.edges);
            if seen.insert(key) {
                merged.push(cycle);
                if merged.len() >= max_paths {
                    break;
                }
            }
        }
        crate::debug!(
            "stream observed-live: live_held={} snap_total={}",
            merged
                .iter()
                .filter(|c| {
                    c.edges
                        .iter()
                        .any(|e| observed_pin_set.contains(&e.pool_index))
                })
                .count(),
            merged.len()
        );
        capped = merged;
    }
    let finalize_ms = crate::util::now_ms().saturating_sub(finalize_started);

    let rates_started = crate::util::now_ms();
    let rate_ctx = RateEnrichContext {
        graph: Some(&routing_graph),
        hub_path: HubPathRateParams {
            enabled: ctx.config.oracle.hub_path_rates,
            max_hops: ctx.config.oracle.hub_path_max_hops.max(1),
        },
        token_decimals: Some(decimals.as_ref()),
        pool_metas: Some(pool_metas.as_ref()),
    };
    let (fresh_rates, rate_stats) = if let Some(ref provider) = state_provider {
        enrich_token_to_matic_rates(
            &ctx.price_oracle,
            &arena,
            cycle_tokens.iter().copied(),
            Some(provider),
            rate_ctx,
        )
        .await
    } else {
        enrich_token_to_matic_rates_offline(
            &ctx.price_oracle,
            &arena,
            cycle_tokens.iter().copied(),
            rate_ctx,
        )
        .await
    };
    // Age stamp for observability — merge always retains non-refreshed priors now;
    // retain_stale_prior=false only forces a rebuild (new Arc) when the snapshot aged.
    let retain_stale_prior = ctx
        .snapshots
        .read()
        .rates_built_at
        .is_some_and(|t| t.elapsed() <= Duration::from_millis(ctx.config.oracle.cache_ttl_ms));
    let mut rates = merge_token_rates(
        &prior_rates,
        &cycle_tokens_set,
        fresh_rates,
        retain_stale_prior,
    );
    let rates_built_at = std::time::Instant::now();
    // Trigger on content change (Arc identity), not map length — swap-in/out rated
    // tokens can keep the same len while eligibility changes. Run before priced-start
    // filter so same-tick hub rates can keep cycles.
    // Skip when early attach already hit cap — another 128 + full enum same tick
    // (live: post-rate attached=128 after cycle_search_ms≈1000) just burns budget;
    // catch-up continues next LF pass with merged rates already in the snapshot.
    if !cpu.attach_hit_cap && !Arc::ptr_eq(&rates, &prior_rates) {
        let post_gate = GraphBuildGate {
            token_to_matic_rates: Arc::clone(&rates),
            flash: ctx.flash_liquidity.load(),
            flash_ttl: ctx.flash_liquidity.ttl(),
            spoke_connectivity: Some(spoke_connectivity_set(
                rates.as_ref(),
                pool_metas.as_ref(),
                &arena,
            )),
        };
        if post_gate.active()
            && has_missing_eligible_pools_with_gate(
                &arena,
                pool_metas.as_ref(),
                routing_graph.as_ref(),
                Some(&post_gate),
            )
        {
            let layout_fp = arena.routing_layout_fingerprint();
            let routable_count = pool_metas.len();
            let eligible =
                count_graph_eligible_pools_with_gate(&arena, pool_metas.as_ref(), Some(&post_gate));
            let state_generation = ctx.cache.generation();
            let g = Arc::make_mut(&mut routing_graph);
            let report = attach_missing_eligible_pools_with_gate(
                &arena,
                g,
                pool_metas.as_ref(),
                Some(&post_gate),
            );
            if report.attached_pools > 0 {
                {
                    let mut gc = ctx.graph_cache.lock();
                    // Keep cycles — wiping here left exclusive pin-miss ticks with
                    // empty prior (live: cycles=108 → next obs pin_covered=0 → 0).
                    gc.store_graph_keep_cycles(
                        Arc::clone(&routing_graph),
                        routable_count,
                        layout_fp,
                        state_generation,
                        eligible,
                        arena.routing_family_prefix_fingerprint(routable_count),
                    );
                    gc.set_attach_catchup_pending(report.hit_cap);
                }
                crate::info!(
                    "lf graph post-rate patch: attached={} hit_cap={} missing_after={} missing_sample={:?} merged_rates={} prior_rates={}",
                    report.attached_pools,
                    report.hit_cap,
                    report.missing_after,
                    report.missing_sample,
                    rates.len(),
                    prior_rates.len()
                );
                // Same-tick hub recompute for tokens that only became reachable after attach.
                if ctx.config.oracle.hub_path_rates {
                    let unresolved: Vec<TokenIndex> = cycle_tokens
                        .iter()
                        .copied()
                        .filter(|t| !has_reliable_matic_rate(*t, rates.as_ref()))
                        .collect();
                    if !unresolved.is_empty() {
                        let hub_more = hub_path_matic_rates_batch(
                            &arena,
                            routing_graph.as_ref(),
                            &unresolved,
                            HubPathRateParams {
                                enabled: true,
                                max_hops: ctx.config.oracle.hub_path_max_hops.max(1),
                            },
                            Some(decimals.as_ref()),
                            Some(pool_metas.as_ref()),
                        );
                        let mut added = 0usize;
                        {
                            let map = Arc::make_mut(&mut rates);
                            for token in unresolved {
                                if let Some(&rate) = hub_more.get(&token) {
                                    map.insert(token, rate);
                                    added += 1;
                                }
                            }
                        }
                        if added > 0 {
                            crate::info!("lf hub post-attach: added_rates={added}");
                        }
                    }
                }
                // Bounded refind so newly attached pools can contribute cycles this tick.
                if arena.token_count() > routing_graph.token_count {
                    Arc::make_mut(&mut routing_graph).ensure_token_capacity(arena.token_count());
                }
                let passes = cycle_search_passes(max_hops, max_paths);
                let defer_priced = !observed_pin.is_empty();
                let probe_ctx = ProbeContext {
                    token_to_matic_rates: if defer_priced {
                        None
                    } else {
                        Some(rates.as_ref())
                    },
                    token_decimals: Some(decimals.as_ref()),
                    gas_price_wei,
                };
                // ponytail: same seed as pre-rate DFS — `&[]` hub-only missed
                // observed peripherals (live: cycles_touching then prune drop_obs).
                let obs_starts = observed_pool_start_tokens(
                    &arena,
                    pool_metas.as_ref(),
                    &observed_pin,
                    Some(routing_graph.as_ref()),
                );
                let exclusive_obs = defer_priced && !obs_starts.is_empty();
                let first_hop = if exclusive_obs {
                    observed_pin.as_slice()
                } else {
                    &[]
                };
                // Primary enum already spent the full budget; post-rate is a patch.
                let outcome = find_cycles_for_mode_with_budget(
                    ctx.config.routing.cycle_finder,
                    &arena,
                    routing_graph.as_ref(),
                    pool_metas.as_ref(),
                    passes.as_slice(),
                    !exclusive_obs,
                    Some(&probe_ctx),
                    CYCLE_ENUM_PATCH_BUDGET,
                    &obs_starts,
                    exclusive_obs,
                    first_hop,
                    exclusive_obs,
                );
                if !outcome.cycles.is_empty() {
                    let mut seen: FxHashSet<u64> =
                        capped.iter().map(|c| cycle_key(&c.edges)).collect();
                    let before = capped.len();
                    for cycle in outcome.cycles {
                        if !seen.insert(cycle_key(&cycle.edges)) {
                            continue;
                        }
                        capped.push(cycle);
                        if capped.len() >= max_paths {
                            break;
                        }
                    }
                    let added = capped.len().saturating_sub(before);
                    if added > 0 {
                        crate::info!(
                            "lf post-rate cycle refind: added={added} total={}",
                            capped.len()
                        );
                    }
                }
            }
        }
    }
    // Priced-start filter can drop live-held cycles whose start token is still
    // warming — pull them aside, filter the rest, then restore priced live ones.
    let mut live_for_rates = Vec::new();
    if !observed_pin_set.is_empty() {
        capped.retain(|cycle| {
            let touch = cycle
                .edges
                .iter()
                .any(|edge| observed_pin_set.contains(&edge.pool_index));
            if touch {
                live_for_rates.push(cycle.clone());
                false
            } else {
                true
            }
        });
    }
    crate::pipeline::cycle_filter::retain_cycles_with_priced_start_in(
        &mut capped,
        rates.as_ref(),
        Some(&arena),
    );
    // Observed pins: try rotate/priced-start but keep unpriced survivors so
    // stream_universe / dirty_in_sel can see the live venue.
    let live_unpriced_backup = live_for_rates.clone();
    crate::pipeline::cycle_filter::retain_cycles_with_priced_start_in(
        &mut live_for_rates,
        rates.as_ref(),
        Some(&arena),
    );
    if live_for_rates.is_empty() && !live_unpriced_backup.is_empty() {
        crate::info!(
            "stream observed-live: priced_start dropped all {} live pins — restoring unpriced lf_pass={lf_pass}",
            live_unpriced_backup.len()
        );
        live_for_rates = live_unpriced_backup;
    }
    if !live_for_rates.is_empty() {
        let mut seen: rustc_hash::FxHashSet<u64> = live_for_rates
            .iter()
            .map(|c| crate::pipeline::cycle_filter::cycle_key(&c.edges))
            .collect();
        let mut merged = live_for_rates;
        for cycle in capped {
            let key = crate::pipeline::cycle_filter::cycle_key(&cycle.edges);
            if seen.insert(key) {
                merged.push(cycle);
                if merged.len() >= max_paths {
                    break;
                }
            }
        }
        capped = merged;
    }
    if !observed_pin.is_empty() {
        let touching = capped
            .iter()
            .filter(|cycle| {
                cycle
                    .edges
                    .iter()
                    .any(|edge| observed_pin_set.contains(&edge.pool_index))
            })
            .count();
        crate::info!(
            "stream observed-live: cycles_touching={touching}/{} observed_pools={} lf_pass={lf_pass}",
            capped.len(),
            observed_pin.len()
        );
    }
    // Drop family/meta-poisoned cycles before HF publish (live: snap refill burned
    // every live_touch as drop_proto). Match HF select gate order so observed pins
    // aren't culled for missing multi-token vault realign.
    let before_publish = capped.len();
    let mut drop_multi = 0usize;
    let mut drop_heal = 0usize;
    let mut drop_uni = 0usize;
    let mut drop_uni_both = 0usize;
    let mut drop_arena = 0usize;
    let mut drop_meta = 0usize;
    let mut drop_hop = 0usize;
    let mut drop_obs = 0usize;
    let mut soft_keep_obs = 0usize;
    let prior_capped = capped;
    let mut kept = Vec::with_capacity(prior_capped.len());
    for cycle in &prior_capped {
        let obs_touch = !observed_pin_set.is_empty()
            && cycle
                .edges
                .iter()
                .any(|e| observed_pin_set.contains(&e.pool_index));
        // ponytail: observed pins stay in snap for stream_universe / HF anchor
        // even when meta gates fail (live: drop_obs==touching → dirty_in_sel=0).
        let soft_obs = |kept: &mut Vec<_>, drop_obs: &mut usize, soft_keep_obs: &mut usize| {
            *drop_obs += 1;
            *soft_keep_obs += 1;
            kept.push(cycle.clone());
        };
        let ready = match crate::pipeline::local_sim::realign_multi_token_found_cycle(
            &arena,
            Arc::new(cycle.clone()),
        ) {
            Some(c) => c,
            None if obs_touch => {
                // Soft-keep observed multi for dirty_in_sel / universe; HF still
                // drops them at live realign. Hard-drop wiped enum_touch=21 pins.
                drop_multi += 1;
                soft_obs(&mut kept, &mut drop_obs, &mut soft_keep_obs);
                continue;
            }
            None => {
                drop_multi += 1;
                continue;
            }
        };
        let Some(ready) = crate::pipeline::local_sim::heal_cycle_edge_protocols(&arena, ready)
        else {
            // Family poison (Balancer×V3) — do not soft-keep; family_prefix
            // cycle invalidation must drop these from cache.
            drop_heal += 1;
            if obs_touch {
                drop_obs += 1;
            }
            continue;
        };
        let ready = match crate::pipeline::local_sim::realign_uni_cycle_from_pool_meta(
            &arena,
            pool_metas.as_ref(),
            Arc::clone(&ready),
        ) {
            Some(c) => c,
            None => {
                drop_uni += 1;
                let both_foreign = crate::pipeline::local_sim::uni_cycle_has_both_foreign_edge(
                    &arena,
                    pool_metas.as_ref(),
                    &ready.edges,
                );
                if both_foreign {
                    drop_uni_both += 1;
                    // Obs Uni pins from cache often have TokenIndex drift on both
                    // legs (live: enum_touch=2 → uni_both drop_obs=2 emptied).
                    // Soft-keep when hops still connect so stream_universe sees them.
                    let hop_ok = crate::pipeline::local_sim::first_hop_continuity_break_in_arena(
                        &arena,
                        &ready.edges,
                    )
                    .is_none();
                    if obs_touch && hop_ok {
                        soft_obs(&mut kept, &mut drop_obs, &mut soft_keep_obs);
                    } else if obs_touch {
                        drop_obs += 1;
                    }
                    continue;
                }
                // Obs Uni pin: keep healed when hops+arena already ok (meta
                // TokenIndex drift). Hop-broken pins are wrong-pool — hard-drop
                // (soft-keep burned snap slots with live_touch=0).
                let hop_break = crate::pipeline::local_sim::first_hop_continuity_break_in_arena(
                    &arena,
                    &ready.edges,
                );
                let arena_ok =
                    crate::pipeline::local_sim::cycle_edges_match_arena_state(&arena, &ready.edges);
                let meta_ok = crate::pipeline::local_sim::cycle_v2_edges_match_pool_meta(
                    &arena,
                    pool_metas.as_ref(),
                    &ready.edges,
                );
                if obs_touch {
                    static UNI_FAIL_SAMPLE: std::sync::atomic::AtomicU32 =
                        std::sync::atomic::AtomicU32::new(0);
                    if UNI_FAIL_SAMPLE
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                        .is_multiple_of(8)
                    {
                        crate::info!(
                            "obs uni realign fail: hop_break={hop_break:?} arena_ok={arena_ok} meta_ok={meta_ok} hops={} edges={}",
                            ready.edge_hops(),
                            ready.edges.len()
                        );
                    }
                }
                if obs_touch && hop_break.is_none() && arena_ok {
                    ready
                } else if obs_touch && hop_break.is_none() {
                    soft_obs(&mut kept, &mut drop_obs, &mut soft_keep_obs);
                    continue;
                } else if obs_touch {
                    drop_obs += 1;
                    continue;
                } else {
                    continue;
                }
            }
        };
        if !crate::pipeline::local_sim::cycle_edges_match_arena_state(&arena, &ready.edges) {
            drop_arena += 1;
            if obs_touch {
                soft_obs(&mut kept, &mut drop_obs, &mut soft_keep_obs);
            }
            continue;
        }
        if !crate::pipeline::local_sim::cycle_v2_edges_match_pool_meta(
            &arena,
            pool_metas.as_ref(),
            &ready.edges,
        ) {
            drop_meta += 1;
            if obs_touch {
                soft_obs(&mut kept, &mut drop_obs, &mut soft_keep_obs);
            }
            continue;
        }
        if crate::pipeline::local_sim::first_hop_continuity_break_in_arena(&arena, &ready.edges)
            .is_some()
        {
            drop_hop += 1;
            if obs_touch {
                soft_obs(&mut kept, &mut drop_obs, &mut soft_keep_obs);
            }
            continue;
        }
        kept.push(Arc::unwrap_or_clone(ready));
    }
    // ponytail: never publish an empty snap after prune (live: kept=0 starved HF).
    let pruned_stale = before_publish.saturating_sub(kept.len());
    let emptied = kept.is_empty() && before_publish > 0;
    if emptied {
        crate::warn!(
            "lf publish prune emptied snap — keeping prior cycles before_publish={before_publish} lf_pass={lf_pass}"
        );
        capped = prior_capped;
    } else {
        capped = kept;
    }
    let touching_after = if observed_pin_set.is_empty() {
        0
    } else {
        capped
            .iter()
            .filter(|c| {
                c.edges
                    .iter()
                    .any(|e| observed_pin_set.contains(&e.pool_index))
            })
            .count()
    };
    if pruned_stale > 0 || emptied || (!observed_pin.is_empty() && touching_after == 0) {
        crate::info!(
            "lf publish prune: dropped_stale={pruned_stale} kept={} emptied={emptied} observed_touching={touching_after}/{} drop_obs={drop_obs} soft_keep_obs={soft_keep_obs} reasons(multi={drop_multi} heal={drop_heal} uni={drop_uni} uni_both={drop_uni_both} arena={drop_arena} meta={drop_meta} hop={drop_hop}) lf_pass={lf_pass}",
            capped.len(),
            observed_pin.len()
        );
    }
    crate::services::oracle::record_unmapped_token_demand(
        &ctx.price_oracle,
        &arena,
        pool_metas.as_ref(),
        &capped,
    );
    crate::services::oracle::log_ranked_unmapped_demand(&ctx.price_oracle, lf_pass, &rate_stats);
    if lf_pass <= 2 || lf_pass.is_multiple_of(30) {
        crate::debug!(
            "token rates: cycle_tokens={} prior_rates={} merged_rates={} {:?}",
            cycle_tokens.len(),
            prior_rates.len(),
            rates.len(),
            rate_stats
        );
    }
    let cycle_and_rates_ms = crate::util::now_ms().saturating_sub(cpu_started);
    let post_cpu_ms = cycle_and_rates_ms.saturating_sub(cpu_ms);
    let rates_ms = crate::util::now_ms().saturating_sub(rates_started);

    let hot: Vec<Address> = {
        let set: rustc_hash::FxHashSet<Address> = capped
            .iter()
            .flat_map(|c| c.edges.iter())
            .filter_map(|e| arena.pool_address(e.pool_index))
            .collect();
        let mut hot = Vec::with_capacity(set.len());
        hot.extend(set);
        hot
    };

    let _cycle_count = capped.len();
    let _pool_count = pool_metas.len();
    let graph_pool_count = routing_graph.active_pool_count();
    crate::info!(
        "lf tick: cycles={}, enumerated_cycles={}, cycle_search_ms={}, arena_pools={}, graph_pools={}, discovered={}, resolvable_tokens={}",
        _cycle_count,
        enumerated_cycles,
        cycle_search_ms,
        _pool_count,
        graph_pool_count,
        pools.len(),
        resolvable_count
    );
    crate::debug!(
        "lf latency: total_ms={} discovery_ms={} refresh_ms={} arena_sync_ms={} cpu_ms={} post_cpu_ms={} ticks_ms={} v3_tick_targets={} v3_ticks_loaded={} v3_ticks_ms={} v4_tick_targets={} v4_ticks_loaded={} v4_ticks_ms={} finalize_ms={} cycle_and_rates_ms={} rates_ms={} refreshed_pools={} cycle_search_ms={}",
        crate::util::now_ms().saturating_sub(lf_started),
        discovery_ms,
        refresh_ms,
        arena_sync_ms,
        cpu_ms,
        post_cpu_ms,
        ticks_ms,
        v3_tick_targets,
        v3_ticks_loaded,
        v3_ticks_ms,
        v4_tick_targets,
        v4_ticks_loaded,
        v4_ticks_ms,
        finalize_ms,
        cycle_and_rates_ms,
        rates_ms,
        refreshed_pools,
        cycle_search_ms,
    );

    // ponytail: funnel logger off by default — enable with RPBOT_PIPELINE_SURVIVAL=1.
    if std::env::var_os("RPBOT_PIPELINE_SURVIVAL").is_some_and(|v| v != "0")
        && (lf_pass <= 3 || lf_pass.is_multiple_of(10))
    {
        let mut survival = PipelineSurvival::from_lf_tick(
            &pools,
            &ctx.cache,
            pool_metas.as_ref(),
            routing_graph.as_ref(),
        );
        if let Some(stats) = ctx.refresh.bootstrap_parse_stats() {
            survival = survival.with_index_stats(&stats);
        }
        survival.log_summary(lf_pass);
    }
    if lf_pass <= 3 || lf_pass.is_multiple_of(10) {
        crate::services::index_diag::log_index_summary();
    }

    let stream_targets = ctx.config.pipeline.stream_enabled.then(|| {
        let force = ctx.stream_addresses.force_replace_pending();
        let epoch = if force {
            ctx.stream_addresses.reselect_epoch()
        } else {
            0
        };
        let demote: Vec<_> = if force {
            ctx.stream_addresses.read().clone()
        } else {
            Vec::new()
        };
        let cap = ctx.config.pipeline.stream_max_pools;
        let centrality_key =
            arena.routing_layout_fingerprint() ^ ctx.cache.generation().rotate_left(17);
        let mut targets = select_stream_targets_with_epoch(
            &pools,
            &hot,
            Some(routing_graph.as_ref()),
            pool_metas.as_ref(),
            &arena,
            &ctx.partial_cache,
            cap,
            crate::util::now_ms(),
            epoch,
            &demote,
            centrality_key,
        );
        // Pin topic-observed routable pools that made it into the arena this tick
        // so the next Sync/Swap sets wake_hf=true (interest path).
        if !observed.is_empty() {
            let addr_to_pool = arena.address_to_pool();
            let mut seen: rustc_hash::FxHashSet<Address> = targets.iter().copied().collect();
            for addr in &observed {
                if targets.len() >= cap {
                    break;
                }
                if addr_to_pool.contains_key(addr) && seen.insert(*addr) {
                    targets.push(*addr);
                }
            }
        }
        targets
    });

    if *shutdown.borrow() {
        return Ok(());
    }

    // Universe = streamable pools that appear in published HF cycles.
    // Arena-wide metas woke HF on interest/universe patches with
    // active_candidates=0 (dirty never landed in snap edges).
    let stream_universe: Option<Vec<_>> = ctx.config.pipeline.stream_enabled.then(|| {
        let mut addrs = Vec::with_capacity(capped.len().saturating_mul(3) + observed.len());
        let mut seen = rustc_hash::FxHashSet::with_capacity_and_hasher(
            capped.len() * 2,
            rustc_hash::FxBuildHasher,
        );
        for cycle in &capped {
            for edge in &cycle.edges {
                if !crate::services::partial_cache::is_streamable_protocol(edge.protocol) {
                    continue;
                }
                let Some(addr) = arena.pool_address(edge.pool_index) else {
                    continue;
                };
                if seen.insert(addr) {
                    addrs.push(addr);
                }
            }
        }
        // Topic-observed arena hits join universe even when prune culled their
        // cycles (live: drop_obs==touching → never wake_hf on the live venue).
        for &addr in &observed {
            if seen.insert(addr) {
                addrs.push(addr);
            }
        }
        addrs
    });

    // Topic-observed V3 pools admitted this tick — nudge HF after snapshot publish
    // so we do not wait for a second Swap (live: wake_hf stayed 0 on one-shot venues).
    let observed_in_arena: Vec<Address> = if observed.is_empty() {
        Vec::new()
    } else {
        let map = arena.address_to_pool();
        observed
            .iter()
            .filter(|addr| map.contains_key(*addr))
            .copied()
            .collect()
    };

    *ctx.arena.lock() = arena.clone();
    let graph_active_by_protocol = Arc::new(
        crate::services::pipeline_survival::graph_active_protocol_counts(
            pool_metas.as_ref(),
            routing_graph.as_ref(),
        ),
    );
    ctx.snapshots
        .publish(crate::services::hf_snapshot::HfSnapshot {
            state_block: ctx.refresh.last_state_block(),
            state_hash: ctx.refresh.last_state_hash(),
            cycles: capped.into_iter().map(Arc::new).collect(),
            token_to_matic_rates: rates,
            token_decimals: decimals,
            pool_metas,
            arena,
            discovered_pools: Arc::clone(&pools),
            graph_active_by_protocol,
            rates_built_at: Some(rates_built_at),
            ..Default::default()
        });
    // Publish first so the TUI poller cannot pair fresh LF metrics with the
    // previous snapshot generation.
    ctx.ui_hook
        .on_lf_complete(_cycle_count, cycle_search_ms, pools.len());

    // Keep topic-observed routable pools in the refresh hot set (cycle-only `hot`
    // used to wipe them every LF tick).
    let mut publish_hot = hot;
    publish_hot.extend(observed.iter().copied());
    publish_hot.sort_unstable();
    publish_hot.dedup();
    ctx.refresh.set_hot_addresses(publish_hot);

    if let Some(mut targets) = stream_targets {
        // Pin cycle-covered pools into the WSS interest set — otherwise Sync/Swap
        // never lands on universe pools (live: wake_true=0 with cycle universe).
        if let Some(ref universe) = stream_universe {
            let cap = ctx.config.pipeline.stream_max_pools;
            let mut pinned = Vec::with_capacity(cap.min(universe.len() + targets.len()));
            let mut seen = rustc_hash::FxHashSet::with_capacity_and_hasher(
                pinned.capacity(),
                rustc_hash::FxBuildHasher,
            );
            for addr in universe.iter().copied().chain(targets.drain(..)) {
                if pinned.len() >= cap {
                    break;
                }
                if seen.insert(addr) {
                    pinned.push(addr);
                }
            }
            targets = pinned;
        }
        // Apply hysteresis first so retain/seed match the addresses WSS actually
        // watches (replace may keep the prior set on small top-N churn).
        let force = ctx.stream_addresses.force_replace_pending();
        // Log score-order sample before replace() sorts by address.
        let sample_n = targets.len().min(5);
        let score_sample: Vec<String> = targets
            .iter()
            .take(sample_n)
            .map(|a| format!("{a}"))
            .collect();
        let replaced = ctx.stream_addresses.replace(targets);
        if replaced {
            crate::info!(
                "stream targets updated: pools={} force={} epoch={} top=[{}]",
                ctx.stream_addresses.read().len(),
                force,
                ctx.stream_addresses.reselect_epoch(),
                score_sample.join(",")
            );
        }
        let tracked: Vec<_> = ctx.stream_addresses.read().clone();
        if let Some(universe) = stream_universe {
            if !observed.is_empty() {
                ctx.partial_cache.note_sticky_observed(&observed);
            }
            ctx.partial_cache.set_stream_universe(&universe);
        }
        ctx.partial_cache.retain_tracked(&tracked);
        ctx.partial_cache
            .seed_from_state_cache(&ctx.cache, &tracked, crate::util::now_ms());
    }

    // Same-tick HF nudge: do not wait for a second V3 Swap after arena admission.
    let nudged = if !observed_in_arena.is_empty() {
        ctx.partial_cache.wake_dirty_pools(&observed_in_arena);
        true
    } else if !observed.is_empty() {
        ctx.partial_cache.wake_for_admitted_observed(&observed);
        false
    } else {
        false
    };
    if !observed.is_empty() {
        crate::debug!(
            "stream observed-live: arena_hit={}/{} nudged_hf={}",
            observed_in_arena.len(),
            observed.len(),
            u8::from(nudged)
        );
    }

    Ok(())
}

#[must_use]
pub fn spawn_lf_background(
    lf_ctx: Arc<LfContext>,
    lf_interval_ms: u64,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut timer = interval(Duration::from_millis(lf_interval_ms.max(1)));
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                _ = timer.tick() => {
                    let Ok(guard) = lf_ctx.tick_lock.try_lock() else {
                        crate::debug!("lf tick skipped: previous pass still running");
                        continue;
                    };
                    if let Err(e) = run_lf_tick(&lf_ctx, &shutdown).await {
                        crate::warn!("lf tick failed: {e}");
                    }
                    drop(guard);
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::cycle_search_passes;
    use crate::core::constants::HOP_CAP;

    #[test]
    fn short_hop_search_uses_single_pass_through_configured_cap() {
        for max_hops in [2, 3, 4] {
            let passes = cycle_search_passes(max_hops, 5_000);
            assert_eq!(passes.len(), 1);
            assert_eq!(passes[0].max_hops, max_hops);
            assert_eq!(passes[0].max_cycles, 5_000);
        }
    }

    #[test]
    fn long_hop_search_prioritizes_short_routes_then_expands() {
        let passes = cycle_search_passes(5, 5_000);
        assert_eq!(passes.len(), 2);
        assert_eq!(passes[0].max_hops, 3);
        assert_eq!(passes[0].max_cycles, 2_500);
        assert_eq!(passes[1].max_hops, 5);
        assert_eq!(passes[1].max_cycles, 5_000);
    }

    #[test]
    fn hop_search_is_defensively_bounded_by_storage_capacity() {
        let passes = cycle_search_passes(u32::MAX, 5_000);
        assert_eq!(passes.last().map(|pass| pass.max_hops), Some(HOP_CAP));
    }
}
