use crate::core::types::PoolIndex;
use crate::pipeline::types::{GraphHopPhase, RoutingGraph};

#[derive(Debug, Clone, Copy, Default)]
struct GraphNodeAggregate {
    live_direct_edges: usize,
    dead_direct_edges: usize,
    hub_leg_edges: usize,
}

#[derive(Debug, Clone, Default)]
pub struct GraphAggregateIndex {
    pub live_direct_edges: usize,
    pub dead_direct_edges: usize,
    pub hub_leg_edges: usize,
    pub active_pools: usize,
}

#[derive(Debug, Clone, Default)]
pub struct GraphBaseView {
    node_aggregates: Vec<GraphNodeAggregate>,
    pool_live_edge_counts: Vec<usize>,
    aggregate_index: GraphAggregateIndex,
}

impl GraphBaseView {
    #[must_use]
    pub const fn aggregate_index(&self) -> &GraphAggregateIndex {
        &self.aggregate_index
    }

    pub(crate) fn build(graph: &RoutingGraph) -> Self {
        let mut view = Self {
            node_aggregates: Vec::with_capacity(graph.adjacency.len()),
            ..Self::default()
        };
        for node in 0..graph.adjacency.len() {
            let aggregate = Self::node_aggregate(graph, node);
            Self::add_node_aggregate(&mut view.aggregate_index, aggregate);
            view.node_aggregates.push(aggregate);
        }
        for adj in &graph.adjacency {
            for edge in adj {
                if crate::pipeline::cycle_finder::is_live_graph_edge(edge) {
                    let pool = edge.edge.pool_index.0 as usize;
                    if pool >= view.pool_live_edge_counts.len() {
                        view.pool_live_edge_counts.resize(pool + 1, 0);
                    }
                    view.pool_live_edge_counts[pool] += 1;
                }
            }
        }
        view.aggregate_index.active_pools = view
            .pool_live_edge_counts
            .iter()
            .filter(|&&count| count > 0)
            .count();
        view
    }

    pub(crate) fn patch(&mut self, graph: &RoutingGraph, nodes: &[usize], pools: &[PoolIndex]) {
        self.node_aggregates
            .resize(graph.adjacency.len(), GraphNodeAggregate::default());
        let mut dirty_nodes = nodes.to_vec();
        dirty_nodes.sort_unstable();
        dirty_nodes.dedup();
        for node in dirty_nodes {
            if node >= graph.adjacency.len() {
                continue;
            }
            let prior = self.node_aggregates[node];
            Self::subtract_node_aggregate(&mut self.aggregate_index, prior);
            let current = Self::node_aggregate(graph, node);
            Self::add_node_aggregate(&mut self.aggregate_index, current);
            self.node_aggregates[node] = current;
        }
        let mut dirty_pools: Vec<usize> = pools.iter().map(|pool| pool.0 as usize).collect();
        dirty_pools.sort_unstable();
        dirty_pools.dedup();
        for pool in dirty_pools {
            if pool >= self.pool_live_edge_counts.len() {
                self.pool_live_edge_counts.resize(pool + 1, 0);
            }
            let prior = self.pool_live_edge_counts[pool];
            let current = Self::live_edge_count_for_pool(graph, pool);
            if prior == 0 && current > 0 {
                self.aggregate_index.active_pools += 1;
            } else if prior > 0 && current == 0 {
                self.aggregate_index.active_pools =
                    self.aggregate_index.active_pools.saturating_sub(1);
            }
            self.pool_live_edge_counts[pool] = current;
        }
    }

    fn node_aggregate(graph: &RoutingGraph, node: usize) -> GraphNodeAggregate {
        let mut aggregate = GraphNodeAggregate::default();
        let token_node = node < graph.token_count as usize;
        let Some(edges) = graph.adjacency.get(node) else {
            return aggregate;
        };
        for edge in edges {
            match edge.phase {
                GraphHopPhase::Direct => {
                    if crate::pipeline::cycle_finder::is_live_graph_edge(edge) {
                        aggregate.live_direct_edges += 1;
                    } else {
                        aggregate.dead_direct_edges += 1;
                    }
                }
                GraphHopPhase::EnterPool | GraphHopPhase::ExitPool
                    if token_node && crate::pipeline::cycle_finder::is_live_graph_edge(edge) =>
                {
                    aggregate.hub_leg_edges += 1;
                }
                GraphHopPhase::EnterPool | GraphHopPhase::ExitPool => {}
            }
        }
        aggregate
    }

    fn live_edge_count_for_pool(graph: &RoutingGraph, pool: usize) -> usize {
        graph.pool_edge_positions.get(pool).map_or(0, |positions| {
            positions
                .iter()
                .filter(|&&(node, edge)| {
                    graph
                        .adjacency
                        .get(node)
                        .and_then(|edges| edges.get(edge))
                        .is_some_and(crate::pipeline::cycle_finder::is_live_graph_edge)
                })
                .count()
        })
    }

    fn add_node_aggregate(index: &mut GraphAggregateIndex, aggregate: GraphNodeAggregate) {
        index.live_direct_edges += aggregate.live_direct_edges;
        index.dead_direct_edges += aggregate.dead_direct_edges;
        index.hub_leg_edges += aggregate.hub_leg_edges;
    }

    fn subtract_node_aggregate(index: &mut GraphAggregateIndex, aggregate: GraphNodeAggregate) {
        index.live_direct_edges = index
            .live_direct_edges
            .saturating_sub(aggregate.live_direct_edges);
        index.dead_direct_edges = index
            .dead_direct_edges
            .saturating_sub(aggregate.dead_direct_edges);
        index.hub_leg_edges = index.hub_leg_edges.saturating_sub(aggregate.hub_leg_edges);
    }
}
