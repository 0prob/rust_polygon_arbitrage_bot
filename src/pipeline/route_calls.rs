use crate::core::types::{Edge, ProtocolType};

/// Huff ArbExecutor rejects routes with >= 12 packed calls.
pub const MAX_ROUTE_CALLS: usize = 12;
/// `executeArbDirect` batchSwap gas grows quickly; beyond this use per-hop flash routes.
pub const MAX_BALANCER_BATCH_HOPS: usize = 4;

/// Estimate executor packed calls for a route (V3=1, all other protocols=2).
#[must_use]
pub fn estimate_route_calls(edges: &[Edge]) -> usize {
    edges.iter().map(|e| estimate_hop_calls(e.protocol)).sum()
}

/// Pure Balancer routes eligible for `executeArbDirect` + one vault `batchSwap`.
#[must_use]
pub fn balancer_direct_batch_eligible(edges: &[Edge]) -> bool {
    !edges.is_empty()
        && edges.iter().all(|e| e.protocol == ProtocolType::BalancerV2)
        && edges.len() <= MAX_BALANCER_BATCH_HOPS
}

/// Exact packed-call count for execution gating when a route can collapse into a
/// Balancer batch call.
#[must_use]
pub fn estimate_packed_route_calls(edges: &[Edge]) -> usize {
    if balancer_direct_batch_eligible(edges) {
        return 1;
    }
    estimate_route_calls(edges)
}

#[must_use]
pub fn estimate_hop_calls(protocol: ProtocolType) -> usize {
    match protocol {
        ProtocolType::UniswapV3 => 1,
        _ => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolIndex, TokenIndex};

    fn edge(protocol: ProtocolType) -> Edge {
        Edge {
            pool_index: PoolIndex(0),
            token_in: TokenIndex(0),
            token_out: TokenIndex(1),
            token_in_idx: 0,
            token_out_idx: 1,
            fee_bps: 30,
            zero_for_one: true,
            protocol,
        }
    }

    #[test]
    fn v4_heavy_route_exceeds_executor_budget() {
        let edges: Vec<Edge> = std::iter::repeat_n(edge(ProtocolType::UniswapV4), 7).collect();
        assert_eq!(estimate_route_calls(&edges), 14);
        assert!(estimate_route_calls(&edges) > MAX_ROUTE_CALLS);
    }

    #[test]
    fn balancer_only_batch_route_counts_as_single_packed_call() {
        let edges: Vec<Edge> = std::iter::repeat_n(edge(ProtocolType::BalancerV2), 4).collect();
        assert_eq!(estimate_packed_route_calls(&edges), 1);
    }

    #[test]
    fn mixed_route_does_not_use_batch_shortcut() {
        let mut edges = vec![edge(ProtocolType::BalancerV2); 3];
        edges.push(edge(ProtocolType::UniswapV3));
        assert_eq!(estimate_packed_route_calls(&edges), 7);
    }

    #[test]
    fn balancer_batch_packed_count_beats_per_hop_sum() {
        let edges: Vec<Edge> = std::iter::repeat_n(edge(ProtocolType::BalancerV2), 4).collect();
        assert_eq!(estimate_packed_route_calls(&edges), 1);
        assert_eq!(estimate_route_calls(&edges), 8);
        let over_batch: Vec<Edge> = std::iter::repeat_n(edge(ProtocolType::BalancerV2), 5).collect();
        assert_eq!(estimate_packed_route_calls(&over_batch), 10);
    }
}
