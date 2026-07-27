use crate::core::math::fixed_point::ONE;
use crate::core::types::{CycleEdges, Edge, FoundCycle, TokenIndex};
use crate::pipeline::cycle_filter::cycle_key;
use crate::pipeline::cycle_finder::clamp_fee_bps;
use crate::pipeline::route_calls::{estimate_packed_route_calls, packed_calls_fit_executor};
use crate::pipeline::spot_price::{hop_penalty, min_profitable_cycle_ratio, mul_ratio_saturating};
use crate::pipeline::weighted_graph::WeightedEdge;
use alloy::primitives::U256;

/// Validate a closed simple cycle and return packed-call cost.
/// Paths are short (≤ HOP_CAP); linear scans beat HashSet allocs on the hot path.
fn is_simple_cycle(edges: &[Edge]) -> Option<usize> {
    let len = edges.len();
    if len < 2 {
        return None;
    }
    let start = edges[0].token_in;
    if edges.last().map(|e| e.token_out) != Some(start) {
        return None;
    }
    for i in 0..len {
        let pool = edges[i].pool_index.0;
        for e in edges.iter().take(i) {
            if e.pool_index.0 == pool {
                return None;
            }
        }
        if i < len - 1 {
            let mid = edges[i].token_out;
            if mid == start {
                return None;
            }
            for e in edges.iter().take(i) {
                if e.token_out == mid {
                    return None;
                }
            }
        }
    }
    Some(estimate_packed_route_calls(edges))
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
    dist_ratio: &mut [U256],
    pred_node: &mut [Option<TokenIndex>],
    pred_edge: &mut [Option<WeightedEdge>],
    active: &mut Vec<usize>,
    next_active: &mut Vec<usize>,
    in_next: &mut [bool],
    visited_scratch: &mut [u32],
    visited_gen: &mut u32,
    should_stop: &mut impl FnMut() -> bool,
) {
    let n = dist.len();
    let src = source.0 as usize;
    if src >= n {
        return;
    }

    let mut touched = Vec::<usize>::new();

    dist[src] = 0.0;
    dist_ratio[src] = ONE;
    touched.push(src);

    active.clear();
    next_active.clear();
    active.push(src);

    fn bf_eps(old: f64) -> f64 {
        f64::EPSILON * old.abs().max(1.0) * 2.0
    }

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
            let u_ratio = dist_ratio[u_idx];
            let Some(edges) = adj.get(u_idx) else {
                continue;
            };
            for we in edges {
                let v = we.edge.token_out.0 as usize;
                // Dead edges only — keep sub-ONE ratios so multi-hop arbs with
                // lossy intermediate hops remain discoverable via product ratio.
                if v >= n || we.ratio.is_zero() {
                    continue;
                }
                let new_dist = u_dist + we.weight;
                let old = dist[v];
                let new_ratio = mul_ratio_saturating(u_ratio, we.ratio);
                let ratio_improves = new_ratio > dist_ratio[v];
                if new_dist < old - bf_eps(old) || ratio_improves {
                    if !old.is_finite() {
                        touched.push(v);
                    }
                    dist[v] = new_dist;
                    dist_ratio[v] = new_ratio;
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

    // Only nodes reached from `source` can participate in a negative cycle here.
    // Full-adj scans were O(V+E) per source and dominated BF time on large graphs.
    'outer: for &u_idx in &touched {
        if should_stop() || cycles.len() >= max_cycles {
            break;
        }
        let u_dist = dist[u_idx];
        if !u_dist.is_finite() {
            continue;
        }
        let Some(edges) = adj.get(u_idx) else {
            continue;
        };
        for we in edges {
            if should_stop() || cycles.len() >= max_cycles {
                break 'outer;
            }
            if we.ratio.is_zero() {
                continue;
            }
            let v_idx = we.edge.token_out.0 as usize;
            if v_idx >= n {
                continue;
            }
            let v_dist = dist[v_idx];
            let edge_ratio_at_v = mul_ratio_saturating(dist_ratio[u_idx], we.ratio);
            let we_neg_via_ratio = edge_ratio_at_v > dist_ratio[v_idx];
            if u_dist + we.weight >= v_dist - bf_eps(v_dist) && !we_neg_via_ratio {
                continue;
            }

            *visited_gen = visited_gen.wrapping_add(1);
            if *visited_gen == 0 {
                visited_scratch.fill(0);
                *visited_gen = 1;
            }
            let generation = *visited_gen;
            let mut curr = Some(TokenIndex(u_idx as u32));
            while let Some(c) = curr {
                let idx = c.0 as usize;
                if idx >= visited_scratch.len() {
                    break;
                }
                if visited_scratch[idx] == generation {
                    break;
                }
                visited_scratch[idx] = generation;
                curr = pred_node[idx];
            }
            let Some(cycle_start) = curr else {
                continue;
            };

            let mut cycle_edges: CycleEdges = CycleEdges::new();
            let mut log_weight = 0.0;
            let mut cum_fee = 0u32;
            let mut product_ratio = ONE;
            let mut trace = Some(cycle_start);
            while let Some(t) = trace {
                let t_idx = t.0 as usize;
                if t_idx >= pred_edge.len() {
                    break;
                }
                let Some(we_pred) = pred_edge[t_idx] else {
                    break;
                };
                log_weight += we_pred.weight;
                cum_fee = cum_fee.saturating_add(clamp_fee_bps(we_pred.edge.fee_bps));
                product_ratio = mul_ratio_saturating(product_ratio, we_pred.ratio);
                cycle_edges.push(we_pred.edge);
                trace = pred_node[t_idx];
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
            if !packed_calls_fit_executor(route_calls)
                || product_ratio <= ONE
                || product_ratio < min_profitable_cycle_ratio(cycle_edges.len() as u32)
            {
                continue;
            }
            let key = cycle_key(&cycle_edges);
            if !found_keys.insert(key) {
                continue;
            }

            let hop_count = cycle_edges.len() as u32;
            log_weight += hop_penalty(hop_count);
            cycles.push(FoundCycle {
                start_token: cycle_edges[0].token_in,
                edges: cycle_edges,
                hop_count,
                log_weight,
                cumulative_fee_bps: cum_fee,
                score: log_weight,
                cycle_ratio: product_ratio,
            });
        }
    }

    for &idx in &touched {
        dist[idx] = f64::INFINITY;
        dist_ratio[idx] = ONE;
        pred_node[idx] = None;
        pred_edge[idx] = None;
        in_next[idx] = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolIndex, ProtocolType};
    use crate::pipeline::bellman_ford::find_cycles_bellman_ford_multi_pass_with_adj;
    use crate::pipeline::types::CycleSearchPass;

    fn we(pool: u32, tin: u32, tout: u32, weight: f64, ratio: u64) -> WeightedEdge {
        WeightedEdge {
            edge: Edge {
                pool_index: PoolIndex(pool),
                token_in: TokenIndex(tin),
                token_out: TokenIndex(tout),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            weight,
            ratio: U256::from(ratio),
        }
    }

    /// Classic 3-hop arb: two slightly lossy legs + one strong leg, product > 1.
    /// Previously BF skipped ratio < ONE edges and could not find this cycle.
    #[test]
    fn bf_finds_mixed_ratio_triangle() {
        // ratios: 0.99 * 0.99 * 1.05 ≈ 1.028
        let r99 = 990_000_000_000_000_000u64;
        let r105 = 1_050_000_000_000_000_000u64;
        let adj = vec![
            vec![we(1, 0, 1, 0.01, r99)],     // 0→1 lossy
            vec![we(2, 1, 2, 0.01, r99)],     // 1→2 lossy
            vec![we(3, 2, 0, -0.0488, r105)], // 2→0 strong
        ];
        let found = find_cycles_bellman_ford_multi_pass_with_adj(
            &adj,
            &[CycleSearchPass {
                max_hops: 3,
                max_cycles: 16,
            }],
        );
        assert!(
            !found.is_empty(),
            "expected mixed-ratio triangle arb, got none"
        );
        assert!(found.iter().any(|c| c.hop_count == 3));
        assert!(found.iter().all(|c| c.cycle_ratio > ONE));
    }
}
