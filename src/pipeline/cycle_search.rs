use crate::config::CycleFinderMode;
use crate::core::types::FoundCycle;
use crate::pipeline::arena::StateArena;
use crate::pipeline::bellman_ford::find_cycles_bellman_ford_multi_pass_with_adj;
use crate::pipeline::cycle_filter::{
    ProbeContext, dedupe_cycles_by_edges, prefilter_cycles_by_atomic_sim_with_context,
};
use crate::pipeline::cycle_finder::find_cycles_multi_pass;
use crate::pipeline::types::{CycleSearchPass, RoutingGraph};
use crate::pipeline::weighted_graph::build_weighted_adjacency;
use rayon::join;

fn graph_hub_heavy(graph: &RoutingGraph) -> bool {
    let token_slots = graph.token_count as usize;
    let mut enter = 0usize;
    let mut direct = 0usize;
    for adj in graph.adjacency.iter().take(token_slots) {
        for ge in adj {
            match ge.phase {
                crate::pipeline::types::GraphHopPhase::EnterPool => enter += 1,
                crate::pipeline::types::GraphHopPhase::Direct => direct += 1,
                crate::pipeline::types::GraphHopPhase::ExitPool => {}
            }
        }
    }
    enter > direct
}

fn split_hybrid_budget(total: usize, hub_heavy: bool) -> (usize, usize) {
    if total == 0 {
        return (0, 0);
    }
    if hub_heavy {
        // ponytail: Bellman-Ford only walks Direct edges; hub-spoke pools need DFS budget.
        let bf = (total / 8).max(1).min(total);
        (total.saturating_sub(bf), bf)
    } else {
        let dfs = total.div_ceil(2);
        (dfs, total.saturating_sub(dfs))
    }
}

fn finalize_cycles(
    arena: &StateArena,
    cycles: Vec<FoundCycle>,
    passes: &[CycleSearchPass],
    atomic_prefilter: bool,
    probe_ctx: Option<&ProbeContext<'_>>,
) -> Vec<FoundCycle> {
    let merged = dedupe_cycles_by_edges(cycles);
    let max_keep = passes.iter().map(|p| p.max_cycles).max().unwrap_or(0);
    if atomic_prefilter {
        prefilter_cycles_by_atomic_sim_with_context(arena, merged, max_keep, probe_ctx)
    } else {
        let mut out = merged;
        if out.len() > max_keep {
            out.truncate(max_keep);
        }
        out
    }
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
) -> Vec<FoundCycle> {
    if passes.is_empty() {
        return Vec::new();
    }

    let cycles = match mode {
        CycleFinderMode::Hybrid => {
            return find_cycles_hybrid_multi_pass(
                arena,
                graph,
                pool_metas,
                passes,
                atomic_prefilter,
                probe_ctx,
            );
        }
        CycleFinderMode::Dfs => find_cycles_multi_pass(graph, arena, pool_metas, passes),
        // Johnson reweighting assumes the absence of negative cycles, but this
        // router deliberately searches for them. Fall back to the weighted
        // Bellman-Ford path instead of applying an invalid transform.
        CycleFinderMode::Johnson | CycleFinderMode::BellmanFord => {
            let adj = build_weighted_adjacency(graph);
            find_cycles_bellman_ford_multi_pass_with_adj(&adj, passes)
        }
    };
    finalize_cycles(arena, cycles, passes, atomic_prefilter, probe_ctx)
}

/// Parallel DFS + Johnson hub search + Bellman-Ford, merged and atomically prefiltered.
#[must_use]
pub fn find_cycles_hybrid_multi_pass(
    arena: &StateArena,
    graph: &RoutingGraph,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    passes: &[CycleSearchPass],
    atomic_prefilter: bool,
    probe_ctx: Option<&ProbeContext<'_>>,
) -> Vec<FoundCycle> {
    if passes.is_empty() {
        return Vec::new();
    }

    let hub_heavy = graph_hub_heavy(graph);
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
    let base_adj = build_weighted_adjacency(graph);

    let (mut dfs_cycles, mut bf_cycles) = join(
        || find_cycles_multi_pass(graph, arena, pool_metas, &dfs_budget),
        || find_cycles_bellman_ford_multi_pass_with_adj(&base_adj, &bf_budget),
    );

    dfs_cycles.append(&mut bf_cycles);

    finalize_cycles(arena, dfs_cycles, passes, atomic_prefilter, probe_ctx)
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
        assert_eq!(split_hybrid_budget(500, true), (438, 62));
        assert_eq!(split_hybrid_budget(8, true), (7, 1));
    }
}
