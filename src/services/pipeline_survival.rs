use std::collections::BTreeMap;

use rustc_hash::FxHashSet;

use crate::pipeline::types::{PoolMeta, RoutingGraph};
use crate::services::discovery::DiscoveredPool;
use crate::services::state_cache::StateCache;

#[derive(Debug, Clone, Default)]
pub struct ParseStats {
    pub parsed: BTreeMap<String, usize>,
    pub rejected: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Default)]
pub struct PipelineSurvival {
    pub index_parsed: BTreeMap<String, usize>,
    pub index_rejected: BTreeMap<String, usize>,
    pub discovered: BTreeMap<String, usize>,
    pub cached: BTreeMap<String, usize>,
    pub tradable: BTreeMap<String, usize>,
    pub arena: BTreeMap<String, usize>,
    pub arena_no_graph: BTreeMap<String, usize>,
    pub graph: BTreeMap<String, usize>,
    pub cycle_capable: BTreeMap<String, usize>,
}

impl PipelineSurvival {
    pub fn from_lf_tick(
        pools: &[DiscoveredPool],
        cache: &StateCache,
        pool_metas: &[PoolMeta],
        graph: &RoutingGraph,
    ) -> Self {
        let discovered = count_discovered(pools);
        let (cached, tradable) = count_cache_stages(pools, cache);
        let arena = count_metas(pool_metas);
        let (graph_counts, arena_no_graph) = count_graph_partition(pool_metas, graph);
        let cycle_capable = count_cycle_capable_pools(pool_metas, graph);
        Self {
            discovered,
            cached,
            tradable,
            arena,
            arena_no_graph,
            graph: graph_counts,
            cycle_capable,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn with_index_stats(mut self, stats: &ParseStats) -> Self {
        self.index_parsed = stats.parsed.clone();
        self.index_rejected = stats.rejected.clone();
        self
    }

    pub fn log_summary(&self, pass: u64) {
        let protocols = self.all_protocol_labels();
        if protocols.is_empty() {
            return;
        }

        crate::info!(
            "pipeline survival (pass={pass}): {} protocols tracked",
            protocols.len()
        );

        let mut totals = SurvivalTotals::default();
        for label in &protocols {
            let idx = self.index_parsed.get(label).copied().unwrap_or(0);
            let rej = self.index_rejected.get(label).copied().unwrap_or(0);
            let disc = self.discovered.get(label).copied().unwrap_or(0);
            let cached = self.cached.get(label).copied().unwrap_or(0);
            let tradable = self.tradable.get(label).copied().unwrap_or(0);
            let arena = self.arena.get(label).copied().unwrap_or(0);
            let no_graph = self.arena_no_graph.get(label).copied().unwrap_or(0);
            let graph = self.graph.get(label).copied().unwrap_or(0);
            let cyclic = self.cycle_capable.get(label).copied().unwrap_or(0);

            totals.index_parsed += idx;
            totals.index_rejected += rej;
            totals.discovered += disc;
            totals.cached += cached;
            totals.tradable += tradable;
            totals.arena += arena;
            totals.no_graph += no_graph;
            totals.graph += graph;
            totals.cycle_capable += cyclic;

            if disc == 0 && idx == 0 {
                continue;
            }

            crate::info!(
                "  {label}: index={idx}/{rej} disc={disc} cache={cached} tradable={tradable} arena={arena} no_graph={no_graph} graph={graph} cycle_capable={cyclic}"
            );
        }

        crate::info!(
            "pipeline totals: index={}/{} disc={} cache={} tradable={} arena={} no_graph={} graph={} cycle_capable={}",
            totals.index_parsed,
            totals.index_rejected,
            totals.discovered,
            totals.cached,
            totals.tradable,
            totals.arena,
            totals.no_graph,
            totals.graph,
            totals.cycle_capable,
        );
    }

    fn all_protocol_labels(&self) -> Vec<String> {
        let mut labels: FxHashSet<String> = FxHashSet::default();
        for map in [
            &self.index_parsed,
            &self.index_rejected,
            &self.discovered,
            &self.cached,
            &self.tradable,
            &self.arena,
            &self.arena_no_graph,
            &self.graph,
            &self.cycle_capable,
        ] {
            labels.extend(map.keys().cloned());
        }
        let mut out: Vec<String> = labels.into_iter().collect();
        out.sort();
        out
    }
}

#[derive(Default)]
struct SurvivalTotals {
    index_parsed: usize,
    index_rejected: usize,
    discovered: usize,
    cached: usize,
    tradable: usize,
    arena: usize,
    no_graph: usize,
    graph: usize,
    cycle_capable: usize,
}

fn count_discovered(pools: &[DiscoveredPool]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for pool in pools {
        *counts.entry(pool.protocol_label.clone()).or_default() += 1;
    }
    counts
}

fn count_cache_stages(
    pools: &[DiscoveredPool],
    cache: &StateCache,
) -> (BTreeMap<String, usize>, BTreeMap<String, usize>) {
    cache.count_discovery_stages_by_protocol(pools)
}

fn count_metas(metas: &[PoolMeta]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for meta in metas {
        let label = meta_label(meta);
        *counts.entry(label).or_default() += 1;
    }
    counts
}

fn pool_has_graph_edges(meta: &PoolMeta, graph: &RoutingGraph) -> bool {
    graph.pool_has_live_edges(meta.pool_index)
}

fn count_graph_partition(
    metas: &[PoolMeta],
    graph: &RoutingGraph,
) -> (BTreeMap<String, usize>, BTreeMap<String, usize>) {
    let mut graph_counts = BTreeMap::new();
    let mut arena_no_graph = BTreeMap::new();
    for meta in metas {
        let label = meta_label(meta);
        if pool_has_graph_edges(meta, graph) {
            *graph_counts.entry(label).or_default() += 1;
        } else {
            *arena_no_graph.entry(label).or_default() += 1;
        }
    }
    (graph_counts, arena_no_graph)
}

fn count_cycle_capable_pools(metas: &[PoolMeta], graph: &RoutingGraph) -> BTreeMap<String, usize> {
    let owned;
    let coverage = if let Some(arc) = &graph.coverage {
        arc.as_ref()
    } else {
        owned = crate::pipeline::cycle_finder::cycle_capable_coverage(graph);
        &owned
    };
    let mut counts = BTreeMap::new();
    for meta in metas {
        if coverage.pool_indices.contains(&meta.pool_index.0) {
            *counts.entry(meta_label(meta)).or_default() += 1;
        }
    }
    counts
}

pub fn record_pg_row(stats: &mut ParseStats, protocol: &str, parsed: bool) {
    let key = protocol.to_ascii_uppercase();
    if parsed {
        *stats.parsed.entry(key).or_default() += 1;
    } else {
        *stats.rejected.entry(key).or_default() += 1;
    }
}

pub fn log_index_parse_stats(stats: &ParseStats) {
    let parsed: usize = stats.parsed.values().sum();
    let rejected: usize = stats.rejected.values().sum();
    crate::info!(
        "index parse: parsed={parsed} rejected={rejected} across {} protocols",
        stats.parsed.len().max(stats.rejected.len())
    );
    for (label, count) in &stats.rejected {
        if *count > 0 {
            crate::debug!("index rejected: protocol={label} count={count}");
        }
    }
}

fn meta_label(meta: &PoolMeta) -> String {
    meta.protocol_label
        .clone()
        .unwrap_or_else(|| format!("{:?}", meta.protocol))
}

/// Live routing-graph pool counts keyed by protocol label (for TUI / diagnostics).
#[must_use]
pub fn graph_active_protocol_counts(
    metas: &[PoolMeta],
    graph: &RoutingGraph,
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for meta in metas {
        if graph.pool_has_live_edges(meta.pool_index) {
            *counts.entry(meta_label(meta)).or_default() += 1;
        }
    }
    counts
}
