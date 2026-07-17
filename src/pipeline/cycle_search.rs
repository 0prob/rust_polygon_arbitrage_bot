use std::sync::Arc;
use std::time::Duration;

use rayon::join;

use crate::config::CycleFinderMode;
use crate::core::types::FoundCycle;
use crate::pipeline::arena::StateArena;
use crate::pipeline::bellman_ford::find_cycles_bellman_ford_multi_pass_with_adj;
use crate::pipeline::cycle_filter::{
    PrefilterDiagnostics, ProbeContext, dedupe_cycles_by_edges,
    prefilter_cycles_by_atomic_sim_with_context_and_diag, retain_cycles_with_priced_start,
};
use crate::pipeline::cycle_finder::{
    CYCLE_ENUM_TIME_BUDGET, find_cycles_multi_pass_with_prep_budget, index_pool_metas,
    prepare_active_graph,
};
use crate::pipeline::types::{CycleSearchPass, RoutingGraph};
use crate::pipeline::weighted_graph::build_weighted_adjacency;

fn split_hybrid_budget(total: usize, hub_heavy: bool) -> (usize, usize) {
    if total == 0 {
        return (0, 0);
    }
    if hub_heavy {
        // Bellman-Ford only walks Direct edges; V4/Balancer hub-spoke pools need DFS.
        let bf = (total / 16).max(1).min(total);
        (total.saturating_sub(bf), bf)
    } else {
        let dfs = total.div_ceil(2);
        (dfs, total.saturating_sub(dfs))
    }
}

#[derive(Debug, Clone, Default)]
pub struct CycleSearchDiagnostics {
    pub mode: CycleFinderMode,
    pub raw_collected: usize,
    pub post_dedupe: usize,
    pub post_prefilter: usize,
    pub dfs_raw: usize,
    pub bf_raw: usize,
    pub hub_heavy: bool,
    pub start_tokens: usize,
    pub enumerate_ms: u64,
    pub finalize_ms: u64,
    pub prefilter: PrefilterDiagnostics,
}

impl CycleSearchDiagnostics {
    pub fn log_summary(&self) {
        crate::info!(
            "cycle search: mode={:?} raw={} dedupe={} prefilter={} dfs={} bf={} hub_heavy={} starts={} enum_ms={} finalize_ms={}",
            self.mode,
            self.raw_collected,
            self.post_dedupe,
            self.post_prefilter,
            self.dfs_raw,
            self.bf_raw,
            self.hub_heavy,
            self.start_tokens,
            self.enumerate_ms,
            self.finalize_ms,
        );
        self.prefilter.log_summary();
    }
}

pub struct CycleSearchOutcome {
    pub cycles: Vec<FoundCycle>,
    pub diag: CycleSearchDiagnostics,
}

fn finalize_cycles(
    arena: &StateArena,
    cycles: Vec<FoundCycle>,
    passes: &[CycleSearchPass],
    atomic_prefilter: bool,
    probe_ctx: Option<&ProbeContext<'_>>,
    diag: &mut CycleSearchDiagnostics,
) -> Vec<FoundCycle> {
    diag.raw_collected = cycles.len();
    let mut merged = dedupe_cycles_by_edges(cycles);
    diag.post_dedupe = merged.len();
    if let Some(rates) = probe_ctx.and_then(|c| c.token_to_matic_rates) {
        let before = merged.len();
        retain_cycles_with_priced_start(&mut merged, rates);
        let pruned = before.saturating_sub(merged.len());
        if pruned > 0 {
            crate::debug!("cycle search: pruned_unpriced={pruned}");
        }
    }
    let max_keep = passes.iter().map(|p| p.max_cycles).max().unwrap_or(0);
    let out = if atomic_prefilter {
        let (out, prefilter_diag) = prefilter_cycles_by_atomic_sim_with_context_and_diag(
            arena, merged, max_keep, probe_ctx,
        );
        diag.prefilter = prefilter_diag;
        out
    } else {
        let mut out = merged;
        if out.len() > max_keep {
            out.truncate(max_keep);
        }
        out
    };
    diag.post_prefilter = out.len();
    out
}

/// Dispatch cycle search by configured finder mode.
#[must_use]
pub fn find_cycles_for_mode(
    mode: CycleFinderMode,
    arena: &StateArena,
    graph: &RoutingGraph,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    passes: &[CycleSearchPass],
    atomic_prefilter: bool,
    probe_ctx: Option<&ProbeContext<'_>>,
) -> CycleSearchOutcome {
    find_cycles_for_mode_with_budget(
        mode,
        arena,
        graph,
        pool_metas,
        passes,
        atomic_prefilter,
        probe_ctx,
        CYCLE_ENUM_TIME_BUDGET,
    )
}

/// Like [`find_cycles_for_mode`] with an explicit DFS wall-clock budget.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn find_cycles_for_mode_with_budget(
    mode: CycleFinderMode,
    arena: &StateArena,
    graph: &RoutingGraph,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    passes: &[CycleSearchPass],
    atomic_prefilter: bool,
    probe_ctx: Option<&ProbeContext<'_>>,
    enum_budget: Duration,
) -> CycleSearchOutcome {
    if passes.is_empty() {
        return CycleSearchOutcome {
            cycles: Vec::new(),
            diag: CycleSearchDiagnostics {
                mode,
                ..CycleSearchDiagnostics::default()
            },
        };
    }

    let enum_started = crate::util::now_ms();
    let mut diag = CycleSearchDiagnostics {
        mode,
        ..CycleSearchDiagnostics::default()
    };

    match mode {
        CycleFinderMode::Hybrid => find_cycles_hybrid_multi_pass(
            arena,
            graph,
            pool_metas,
            passes,
            atomic_prefilter,
            probe_ctx,
            enum_started,
            enum_budget,
            &mut diag,
        ),
        CycleFinderMode::Dfs => {
            let prep = prepare_active_graph(graph);
            let pool_index = index_pool_metas(pool_metas);
            let raw = find_cycles_multi_pass_with_prep_budget(
                graph,
                arena,
                &pool_index,
                &prep,
                passes,
                enum_budget,
            );
            diag.enumerate_ms = crate::util::now_ms().saturating_sub(enum_started);
            let finalize_started = crate::util::now_ms();
            let cycles =
                finalize_cycles(arena, raw, passes, atomic_prefilter, probe_ctx, &mut diag);
            diag.finalize_ms = crate::util::now_ms().saturating_sub(finalize_started);
            CycleSearchOutcome { cycles, diag }
        }
        CycleFinderMode::Johnson | CycleFinderMode::BellmanFord => {
            let adj = build_weighted_adjacency(graph);
            let raw = find_cycles_bellman_ford_multi_pass_with_adj(&adj, passes);
            diag.enumerate_ms = crate::util::now_ms().saturating_sub(enum_started);
            let finalize_started = crate::util::now_ms();
            let cycles =
                finalize_cycles(arena, raw, passes, atomic_prefilter, probe_ctx, &mut diag);
            diag.finalize_ms = crate::util::now_ms().saturating_sub(finalize_started);
            CycleSearchOutcome { cycles, diag }
        }
    }
}

/// Parallel DFS + Bellman-Ford, merged and atomically prefiltered.
#[allow(clippy::too_many_arguments)]
fn find_cycles_hybrid_multi_pass(
    arena: &StateArena,
    graph: &RoutingGraph,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    passes: &[CycleSearchPass],
    atomic_prefilter: bool,
    probe_ctx: Option<&ProbeContext<'_>>,
    enum_started: u64,
    enum_budget: Duration,
    diag: &mut CycleSearchDiagnostics,
) -> CycleSearchOutcome {
    if passes.is_empty() {
        return CycleSearchOutcome {
            cycles: Vec::new(),
            diag: diag.clone(),
        };
    }

    let prep = Arc::new(prepare_active_graph(graph));
    diag.hub_heavy = prep.hub_heavy;
    diag.start_tokens = prep.start_token_count();
    let hub_heavy = prep.hub_heavy;
    let dfs_budget: Vec<_> = passes
        .iter()
        .map(|p| {
            let (dfs, _) = split_hybrid_budget(p.max_cycles, hub_heavy);
            CycleSearchPass {
                max_hops: p.max_hops,
                max_cycles: dfs,
            }
        })
        .collect();
    let bf_budget: Vec<_> = passes
        .iter()
        .map(|p| {
            let (_, bf) = split_hybrid_budget(p.max_cycles, hub_heavy);
            CycleSearchPass {
                max_hops: p.max_hops,
                max_cycles: bf,
            }
        })
        .collect();
    let bf_enabled = bf_budget.iter().any(|p| p.max_cycles > 0);
    let pool_index = index_pool_metas(pool_metas);
    let prep_dfs = Arc::clone(&prep);

    let (mut dfs_cycles, mut bf_cycles) = join(
        || {
            find_cycles_multi_pass_with_prep_budget(
                graph,
                arena,
                &pool_index,
                prep_dfs.as_ref(),
                &dfs_budget,
                enum_budget,
            )
        },
        || {
            if !bf_enabled {
                return Vec::new();
            }
            let base_adj = build_weighted_adjacency(graph);
            find_cycles_bellman_ford_multi_pass_with_adj(&base_adj, &bf_budget)
        },
    );

    diag.dfs_raw = dfs_cycles.len();
    diag.bf_raw = bf_cycles.len();
    dfs_cycles.append(&mut bf_cycles);

    diag.enumerate_ms = crate::util::now_ms().saturating_sub(enum_started);
    let finalize_started = crate::util::now_ms();
    let cycles = finalize_cycles(arena, dfs_cycles, passes, atomic_prefilter, probe_ctx, diag);
    diag.finalize_ms = crate::util::now_ms().saturating_sub(finalize_started);
    CycleSearchOutcome {
        cycles,
        diag: diag.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_keep_uses_largest_pass_not_sum() {
        let passes = [
            CycleSearchPass {
                max_hops: 3,
                max_cycles: 2_500,
            },
            CycleSearchPass {
                max_hops: 5,
                max_cycles: 5_000,
            },
        ];
        assert_eq!(
            passes.iter().map(|p| p.max_cycles).max().unwrap_or(0),
            5_000
        );
    }

    #[test]
    fn split_hybrid_budget_uses_all_capacity() {
        assert_eq!(split_hybrid_budget(0, false), (0, 0));
        assert_eq!(split_hybrid_budget(1, false), (1, 0));
        assert_eq!(split_hybrid_budget(2, false), (1, 1));
        assert_eq!(split_hybrid_budget(3, false), (2, 1));
        assert_eq!(split_hybrid_budget(5, false), (3, 2));
    }

    #[test]
    fn hub_heavy_budget_favors_dfs() {
        assert_eq!(split_hybrid_budget(500, true), (469, 31));
        assert_eq!(split_hybrid_budget(8, true), (7, 1));
    }
}
