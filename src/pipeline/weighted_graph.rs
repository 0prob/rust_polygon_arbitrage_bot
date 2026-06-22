use crate::core::types::{Edge, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::spot_price::{SpotTable, compute_edge_log_weight_with_table};
use crate::pipeline::types::RoutingGraph;

#[derive(Clone, Copy)]
pub struct WeightedEdge {
    pub edge: Edge,
    pub weight: f64,
}

/// Build weighted adjacency from precomputed graph log weights (no SpotTable rebuild).
pub fn build_weighted_adjacency(arena: &StateArena, graph: &RoutingGraph) -> Vec<Vec<WeightedEdge>> {
    let mut out = vec![Vec::new(); graph.token_count as usize];
    for (from_idx, edges) in graph.adjacency.iter().enumerate() {
        let mut list = Vec::with_capacity(edges.len());
        for ge in edges {
            let tradable = arena
                .pool_state(ge.edge.pool_index)
                .map(|s| s.is_tradable())
                .unwrap_or(false);
            if !tradable {
                continue;
            }
            list.push(WeightedEdge {
                edge: ge.edge,
                weight: ge.log_weight,
            });
        }
        out[from_idx] = list;
    }
    out
}

/// Rebuild spot table and weights — used when graph weights may be stale vs arena.
pub fn build_weighted_adjacency_rescored(
    arena: &StateArena,
    graph: &RoutingGraph,
) -> Vec<Vec<WeightedEdge>> {
    let spot_table = SpotTable::build_for_graph(arena, graph);
    let mut out = vec![Vec::new(); graph.token_count as usize];
    for (from_idx, edges) in graph.adjacency.iter().enumerate() {
        let mut list = Vec::with_capacity(edges.len());
        for ge in edges {
            let tradable = arena
                .pool_state(ge.edge.pool_index)
                .map(|s| s.is_tradable())
                .unwrap_or(false);
            if !tradable {
                continue;
            }
            list.push(WeightedEdge {
                edge: ge.edge,
                weight: compute_edge_log_weight_with_table(&spot_table, &ge.edge),
            });
        }
        out[from_idx] = list;
    }
    out
}

/// Bellman-Ford potentials from a virtual super-source (Johnson phase 1).
pub fn compute_bf_potentials(adj: &[Vec<WeightedEdge>], token_count: usize) -> Vec<f64> {
    let n = token_count;
    let mut dist = vec![0.0f64; n];
    for _ in 0..n {
        let mut relaxed = false;
        for (u_idx, edges) in adj.iter().enumerate() {
            let u_dist = dist[u_idx];
            for we in edges {
                let v = we.edge.token_out.0 as usize;
                if v >= n {
                    continue;
                }
                let new_dist = u_dist + we.weight;
                if new_dist < dist[v] - 1e-12 {
                    dist[v] = new_dist;
                    relaxed = true;
                }
            }
        }
        if !relaxed {
            break;
        }
    }
    dist
}

/// Johnson reweight: w'(u,v) = w(u,v) + h[u] - h[v].
pub fn reweight_adjacency(
    adj: &[Vec<WeightedEdge>],
    potentials: &[f64],
) -> Vec<Vec<WeightedEdge>> {
    adj.iter()
        .enumerate()
        .map(|(u_idx, edges)| {
            edges
                .iter()
                .map(|we| {
                    let v = we.edge.token_out.0 as usize;
                    let h_u = potentials.get(u_idx).copied().unwrap_or(0.0);
                    let h_v = potentials.get(v).copied().unwrap_or(0.0);
                    WeightedEdge {
                        edge: we.edge,
                        weight: we.weight + h_u - h_v,
                    }
                })
                .collect()
        })
        .collect()
}

pub fn select_hub_tokens(adj: &[Vec<WeightedEdge>], max_hubs: usize) -> Vec<TokenIndex> {
    crate::pipeline::cycle_finder::prioritize_cycle_start_tokens_from_out_degrees(
        adj.iter().map(|edges| edges.len()),
    )
    .into_iter()
    .take(max_hubs)
    .collect()
}
