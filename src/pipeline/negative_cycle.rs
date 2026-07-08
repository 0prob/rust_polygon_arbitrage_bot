use alloy::primitives::{U256, U512};
use crate::core::types::{CycleEdges, Edge, FoundCycle, TokenIndex};
use crate::core::math::fixed_point::{ONE, ONE_U512};
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
    // Check pool and token uniqueness via FxHashSet. At most HOP_CAP=8 entries
    // per set — allocation is negligible and avoids the 16-bit mask collision bug
    // that would miss duplicates when pool_index > 15 or token_index > 15.
    let mut seen_pools: rustc_hash::FxHashSet<u32> = rustc_hash::FxHashSet::default();
    let mut seen_tokens: rustc_hash::FxHashSet<u32> = rustc_hash::FxHashSet::default();
    for (i, e) in edges.iter().enumerate() {
        if !seen_pools.insert(e.pool_index.0) {
            return None;
        }
        if i < len - 1 {
            let mid = e.token_out;
            if mid == start || !seen_tokens.insert(mid.0) {
                return None;
            }
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
    // Track which entries were touched so we only reset those (avoids O(N) fill on
    // large arrays for each of BELLMAN_FORD_MAX_SOURCES=15 sources).
    // Use a single scratch Vec that doubles as both the "touched" set and the
    // reset list, allocated once and grown incrementally.
    let mut touched = Vec::<usize>::new();

    // Seed with source node.
    dist[source.0 as usize] = 0.0;
    touched.push(source.0 as usize);

    active.clear();
    next_active.clear();
    active.push(source.0 as usize);

    // Relative epsilon for Bellman-Ford comparisons: scaled by the magnitude
    // of the existing distance so tight arbitrage (log-weight ~ 1e-15) is not
    // masked by a hardcoded epsilon, while large distances don't trigger on
    // numerical noise. f64 has ~15-17 decimal digits of precision; 1e-12 ratio
    // per unit magnitude is conservative.
    fn bf_eps(old: f64) -> f64 {
        // 16 units-in-the-last-place instead of 256: tighter threshold for thin-margin
        // arb cycles (log-weight ~1e-15 per hop) while still above pure f64 arithmetic noise.
        f64::EPSILON * old.abs().max(1.0) * 16.0
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
            for we in &adj[u_idx] {
                let v = we.edge.token_out.0 as usize;
                let new_dist = u_dist + we.weight;
                let old = dist[v];
                if new_dist < old - bf_eps(old) {
                    if !old.is_finite() {
                        // First time reaching this node in this source run.
                        touched.push(v);
                    }
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
            if u_dist + we.weight >= v_dist - bf_eps(v_dist) {
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
            let mut product_ratio = ONE;
            let mut trace = Some(cycle_start);
            while let Some(t) = trace {
                let Some(we_pred) = pred_edge[t.0 as usize] else {
                    break;
                };
                log_weight += we_pred.weight;
                cum_fee = cum_fee.saturating_add(clamp_fee_bps(we_pred.edge.fee_bps));
                let p = U512::from(product_ratio) * U512::from(we_pred.ratio) / ONE_U512;
                product_ratio = if p > U512::from(U256::MAX) {
                    U256::ZERO
                } else {
                    crate::util::u512_to_u256(p)
                };
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
                cycle_ratio: product_ratio,
            });
        }
    }

    // Reset only the entries that were touched, avoiding O(N) fill on each source.
    for &idx in &touched {
        dist[idx] = f64::INFINITY;
        pred_node[idx] = None;
        pred_edge[idx] = None;
        in_next[idx] = false;
    }
}
