use smallvec::SmallVec;

use crate::core::types::{CycleEdges, Edge, FoundCycle, TokenIndex};
use crate::pipeline::cycle_finder::clamp_fee_bps;
use crate::pipeline::spot_price::hop_penalty;
use crate::pipeline::types::route_fingerprint;
use crate::pipeline::weighted_graph::WeightedEdge;

const MAX_ROUTE_CALLS: usize = 12;

pub fn route_call_count(edges: &[Edge]) -> usize {
    edges
        .iter()
        .map(|e| match e.protocol {
            crate::core::types::ProtocolType::UniswapV3
            | crate::core::types::ProtocolType::UniswapV4 => 1,
            _ => 2,
        })
        .sum()
}

pub fn is_simple_cycle(edges: &[Edge]) -> bool {
    if edges.len() < 2 {
        return false;
    }
    let start = edges[0].token_in;
    if edges.last().map(|e| e.token_out) != Some(start) {
        return false;
    }
    let mut pools: SmallVec<[u32; 8]> = SmallVec::new();
    let mut intermediates: SmallVec<[u32; 8]> = SmallVec::new();
    for (i, e) in edges.iter().enumerate() {
        if pools.contains(&e.pool_index.0) {
            return false;
        }
        pools.push(e.pool_index.0);
        if i < edges.len() - 1 {
            let mid = e.token_out;
            if mid == start || intermediates.contains(&mid.0) {
                return false;
            }
            intermediates.push(mid.0);
        }
    }
    true
}

/// Extract negative cycles reachable from `source` after a bounded Bellman-Ford relaxation.
pub fn collect_negative_cycles_from_source(
    source: TokenIndex,
    adj: &[Vec<WeightedEdge>],
    max_hops: u32,
    max_cycles: usize,
    found_keys: &mut rustc_hash::FxHashSet<u64>,
    cycles: &mut Vec<FoundCycle>,
    dist: &mut [f64],
    pred_node: &mut [Option<TokenIndex>],
    pred_edge: &mut [Option<WeightedEdge>],
    should_stop: &mut impl FnMut() -> bool,
) {
    dist.fill(f64::INFINITY);
    pred_node.fill(None);
    pred_edge.fill(None);
    dist[source.0 as usize] = 0.0;

    for _ in 0..max_hops {
        let mut relaxed = false;
        for (u_idx, edges) in adj.iter().enumerate() {
            let u_dist = dist[u_idx];
            if !u_dist.is_finite() {
                continue;
            }
            for we in edges {
                let v = we.edge.token_out;
                let new_dist = u_dist + we.weight;
                let old = dist[v.0 as usize];
                if new_dist < old - 1e-9 {
                    dist[v.0 as usize] = new_dist;
                    pred_node[v.0 as usize] = Some(TokenIndex(u_idx as u32));
                    pred_edge[v.0 as usize] = Some(*we);
                    relaxed = true;
                }
            }
        }
        if !relaxed {
            break;
        }
    }

    'outer: for (u_idx, edges) in adj.iter().enumerate() {
        if should_stop() || cycles.len() >= max_cycles {
            break;
        }
        let u_dist = dist[u_idx];
        if !u_dist.is_finite() {
            continue;
        }
        for we in edges {
            if should_stop() || cycles.len() >= max_cycles {
                break 'outer;
            }
            let v = we.edge.token_out;
            let v_dist = dist[v.0 as usize];
            if u_dist + we.weight >= v_dist - 1e-9 {
                continue;
            }

            let mut visited: SmallVec<[TokenIndex; 8]> = SmallVec::new();
            let mut curr = Some(TokenIndex(u_idx as u32));
            while let Some(c) = curr {
                if visited.contains(&c) {
                    break;
                }
                visited.push(c);
                curr = pred_node[c.0 as usize];
            }
            let Some(cycle_start) = curr else {
                continue;
            };

            let mut cycle_edges: CycleEdges = CycleEdges::new();
            let mut log_weight = 0.0;
            let mut cum_fee = 0u32;
            let mut trace = Some(cycle_start);
            while let Some(t) = trace {
                let Some(we_pred) = pred_edge[t.0 as usize] else {
                    break;
                };
                log_weight += we_pred.weight;
                cum_fee = cum_fee.saturating_add(clamp_fee_bps(we_pred.edge.fee_bps));
                cycle_edges.push(we_pred.edge);
                trace = pred_node[t.0 as usize];
                if trace == Some(cycle_start) {
                    break;
                }
                if cycle_edges.len() > max_hops as usize {
                    break;
                }
            }
            cycle_edges.reverse();
            if cycle_edges.is_empty() || !is_simple_cycle(&cycle_edges) {
                continue;
            }
            if route_call_count(&cycle_edges) > MAX_ROUTE_CALLS {
                continue;
            }
            let key = route_fingerprint(&cycle_edges);
            if found_keys.contains(&key) {
                continue;
            }
            found_keys.insert(key);

            let hop_count = cycle_edges.len() as u32;
            log_weight += hop_penalty(hop_count);
            cycles.push(FoundCycle {
                start_token: cycle_edges[0].token_in,
                edges: cycle_edges,
                hop_count,
                log_weight,
                cumulative_fee_bps: cum_fee,
                score: log_weight,
            });
        }
    }
}
