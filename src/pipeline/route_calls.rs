use crate::core::types::{Edge, ProtocolType};

pub const MAX_EXECUTOR_CALLS: usize = 12;
/// `executeArbDirect` batchSwap gas grows quickly; beyond this use per-hop flash routes.
pub const MAX_BALANCER_BATCH_HOPS: usize = 4;

/// Estimate executor packed calls for a route.
#[must_use]
pub fn estimate_route_calls(edges: &[Edge]) -> usize {
    let mut calls = 0;
    let mut previous_v2 = false;
    for edge in edges {
        if edge.protocol == ProtocolType::UniswapV2 {
            calls += 1 + usize::from(!previous_v2);
            previous_v2 = true;
        } else {
            calls += estimate_hop_calls(edge.protocol);
            previous_v2 = false;
        }
    }
    calls
}

/// Pure Balancer routes eligible for `executeArbDirect` + one vault `batchSwap`.
#[must_use]
pub fn balancer_direct_batch_eligible(edges: &[Edge]) -> bool {
    !edges.is_empty()
        && edges.iter().all(|e| e.protocol == ProtocolType::BalancerV2)
        && edges.len() <= MAX_BALANCER_BATCH_HOPS
}

/// Pure DODO routes that use Balancer flash + DODO PMM hops (external DODO flash
/// disabled). Eligible for all-in gas seed — same hop cap as Direct batch.
#[must_use]
pub fn dodo_flash_batch_eligible(edges: &[Edge]) -> bool {
    !edges.is_empty()
        && edges.iter().all(|e| e.protocol == ProtocolType::Dodo)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteExecutorBudget {
    pub hops: usize,
    pub packed_calls: usize,
    pub per_hop_calls: usize,
    pub balancer_batch: bool,
}

#[must_use]
pub fn route_executor_budget(edges: &[Edge]) -> RouteExecutorBudget {
    RouteExecutorBudget {
        hops: edges.len(),
        packed_calls: estimate_packed_route_calls(edges),
        per_hop_calls: estimate_route_calls(edges),
        balancer_batch: balancer_direct_batch_eligible(edges),
    }
}

#[inline]
#[must_use]
pub const fn packed_calls_fit_executor(packed_calls: usize) -> bool {
    packed_calls <= MAX_EXECUTOR_CALLS
}

#[inline]
#[must_use]
pub fn route_fits_executor(edges: &[Edge]) -> bool {
    packed_calls_fit_executor(estimate_packed_route_calls(edges))
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
        assert!(!packed_calls_fit_executor(estimate_route_calls(&edges)));
    }

    #[test]
    fn v2_chain_uses_one_prefund_per_segment() {
        let eleven: Vec<Edge> = std::iter::repeat_n(edge(ProtocolType::UniswapV2), 11).collect();
        assert_eq!(estimate_route_calls(&eleven), 12);
        assert!(route_fits_executor(&eleven));

        let twelve: Vec<Edge> = std::iter::repeat_n(edge(ProtocolType::UniswapV2), 12).collect();
        assert_eq!(estimate_route_calls(&twelve), 13);
        assert!(!route_fits_executor(&twelve));
    }

    #[test]
    fn separated_v2_chains_each_need_a_prefund() {
        let edges = vec![
            edge(ProtocolType::UniswapV2),
            edge(ProtocolType::UniswapV2),
            edge(ProtocolType::UniswapV3),
            edge(ProtocolType::UniswapV2),
            edge(ProtocolType::UniswapV2),
        ];
        assert_eq!(estimate_route_calls(&edges), 7);
    }

    #[test]
    fn balancer_only_batch_route_counts_as_single_packed_call() {
        let edges: Vec<Edge> = std::iter::repeat_n(edge(ProtocolType::BalancerV2), 4).collect();
        assert_eq!(estimate_packed_route_calls(&edges), 1);
    }

    #[test]
    fn dodo_flash_batch_eligible_pure_dodo_only() {
        let dodo: Vec<Edge> = std::iter::repeat_n(edge(ProtocolType::Dodo), 2).collect();
        assert!(dodo_flash_batch_eligible(&dodo));
        let mixed = vec![edge(ProtocolType::Dodo), edge(ProtocolType::UniswapV2)];
        assert!(!dodo_flash_batch_eligible(&mixed));
        assert!(!balancer_direct_batch_eligible(&dodo));
    }

    #[test]
    fn mixed_route_does_not_use_batch_shortcut() {
        let mut edges = vec![edge(ProtocolType::BalancerV2); 3];
        edges.push(edge(ProtocolType::UniswapV3));
        assert_eq!(estimate_packed_route_calls(&edges), 7);
    }

    #[test]
    fn route_fits_executor_respects_packed_budget() {
        let v4_heavy: Vec<Edge> = std::iter::repeat_n(edge(ProtocolType::UniswapV4), 7).collect();
        assert!(!route_fits_executor(&v4_heavy));
        let budget = route_executor_budget(&v4_heavy);
        assert_eq!(budget.packed_calls, 14);
        let batch: Vec<Edge> = std::iter::repeat_n(edge(ProtocolType::BalancerV2), 4).collect();
        assert!(route_fits_executor(&batch));
        assert!(route_executor_budget(&batch).balancer_batch);
    }

    #[test]
    fn packed_executor_budget_includes_twelve_calls() {
        assert!(packed_calls_fit_executor(12));
        assert!(!packed_calls_fit_executor(13));
    }

    #[test]
    fn balancer_batch_packed_count_beats_per_hop_sum() {
        let edges: Vec<Edge> = std::iter::repeat_n(edge(ProtocolType::BalancerV2), 4).collect();
        assert_eq!(estimate_packed_route_calls(&edges), 1);
        assert_eq!(estimate_route_calls(&edges), 8);
        let over_batch: Vec<Edge> =
            std::iter::repeat_n(edge(ProtocolType::BalancerV2), 5).collect();
        assert_eq!(estimate_packed_route_calls(&over_batch), 10);
    }
}
