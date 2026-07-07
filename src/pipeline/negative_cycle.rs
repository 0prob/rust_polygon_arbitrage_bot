use crate::core::types::{CycleEdges, Edge, FoundCycle, TokenIndex};
use crate::pipeline::cycle_filter::cycle_key;
use crate::pipeline::cycle_finder::clamp_fee_bps;
use crate::pipeline::route_calls::{MAX_ROUTE_CALLS, estimate_packed_route_calls};
use crate::pipeline::spot_price::hop_penalty;
use crate::pipeline::weighted_graph::WeightedEdge;

fn is_simple_cycle(edges: &[Edge]) -> Option<usize> {
    let len = edges.len();
    if len < 2 {
        return None;
    }
    let start = edges[0].token_in;
    if edges.last().map(|e| e.token_out) != Some(start) {
        return None;
    }
    // Bitmask for O(1) duplicate-pool / duplicate-intermediate checks (HOP_CAP=8, u16 fits).
    let mut seen_pools: u16 = 0;
    let mut seen_tokens: u16 = 0;
    for (i, e) in edges.iter().enumerate() {
        let pool_bit = 1u16 << (e.pool_index.0 & 15);
        if seen_pools & pool_bit != 0 {
            return None;
        }
        seen_pools |= pool_bit;
        if i < len - 1 {
            let mid = e.token_out;
            let mid_bit = 1u16 << (mid.0 & 15);
            if mid == start || seen_tokens & mid_bit != 0 {
                return None;
            }
            seen_tokens |= mid_bit;
        }
    }
    let route_calls = estimate_packed_route_calls(edges);
    Some(route_calls)
}

/// Extract negative cycles reachable from `source` after a bounded Bellman-Ford relaxation.
#[allow(clippy::too_many_arguments)]
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
    active: &mut Vec<usize>,
    next_active: &mut Vec<usize>,
    in_next: &mut [bool],
    should_stop: &mut impl FnMut() -> bool,
) {
    dist.fill(f64::INFINITY);
    pred_node.fill(None);
    pred_edge.fill(None);
    dist[source.0 as usize] = 0.0;

    active.clear();
    next_active.clear();
    in_next.fill(false);
    active.push(source.0 as usize);

    for _ in 0..max_hops {
        if active.is_empty() {
            break;
        }
        next_active.clear();
        for &u in active.iter() {
            in_next[u] = false;
        }

        for &u_idx in active.iter() {
            let u_dist = dist[u_idx];
            for we in &adj[u_idx] {
                let v = we.edge.token_out.0 as usize;
                let new_dist = u_dist + we.weight;
                let old = dist[v];
                if new_dist < old - 1e-9 {
                    dist[v] = new_dist;
                    pred_node[v] = Some(TokenIndex(u_idx as u32));
                    pred_edge[v] = Some(*we);
                    if !in_next[v] {
                        in_next[v] = true;
                        next_active.push(v);
                    }
                }
            }
        }
        std::mem::swap(active, next_active);
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

            let mut visited_mask: u32 = 0;
            let mut curr = Some(TokenIndex(u_idx as u32));
            while let Some(c) = curr {
                let bit = 1u32 << (c.0 & 31);
                if visited_mask & bit != 0 {
                    break;
                }
                visited_mask |= bit;
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
            let Some(route_calls) = is_simple_cycle(&cycle_edges) else {
                continue;
            };
            if route_calls > MAX_ROUTE_CALLS {
                continue;
            }
            let key = cycle_key(&cycle_edges);
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
