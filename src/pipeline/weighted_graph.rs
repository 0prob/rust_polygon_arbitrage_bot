use crate::core::types::Edge;
use crate::pipeline::cycle_finder::is_live_graph_edge;
use crate::pipeline::types::RoutingGraph;
use alloy::primitives::U256;

#[derive(Clone, Copy)]
pub struct WeightedEdge {
    pub edge: Edge,
    pub weight: f64,
    /// U256 fixed-point ratio (spot_price * (1-fee) / ONE), carried from GraphEdge
    /// so Bellman-Ford can compute precise cycle_ratio for discovered cycles.
    pub ratio: U256,
}

/// Build Johnson/Bellman-Ford adjacency from graph edge weights (already rescored).
#[must_use]
pub fn build_weighted_adjacency(graph: &RoutingGraph) -> Vec<Vec<WeightedEdge>> {
    let mut out = Vec::with_capacity(graph.token_count as usize);
    for edges in &graph.adjacency {
        let mut list = Vec::with_capacity(edges.len());
        for ge in edges {
            if !is_live_graph_edge(ge) {
                continue;
            }
            list.push(WeightedEdge {
                edge: ge.edge,
                weight: ge.log_weight,
                ratio: ge.ratio,
            });
        }
        out.push(list);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolIndex, ProtocolType, TokenIndex};
    use crate::pipeline::cycle_finder::DEAD_EDGE_LOG_WEIGHT;
    use crate::pipeline::types::GraphEdge;
    use alloy::primitives::U256;

    fn graph_edge(weight: f64) -> GraphEdge {
        GraphEdge {
            edge: crate::core::types::Edge {
                pool_index: PoolIndex(0),
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            log_weight: weight,
            ratio: U256::ZERO,
        }
    }

    #[test]
    fn weighted_adjacency_skips_dead_edges() {
        let mut graph = RoutingGraph::new(2);
        let mut live = graph_edge(-0.1);
        live.ratio = U256::from(1_000_000_000_000_000_000u64);
        graph.add_edge(TokenIndex(0), live);
        let mut dead = graph_edge(DEAD_EDGE_LOG_WEIGHT);
        dead.ratio = U256::ZERO;
        graph.add_edge(TokenIndex(1), dead);
        let adj = build_weighted_adjacency(&graph);
        assert_eq!(adj[0].len(), 1);
        assert!(adj[1].is_empty());
    }
}
