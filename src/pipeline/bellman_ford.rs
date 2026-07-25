use std::cell::RefCell;
use std::time::Duration;

use crate::core::types::{FoundCycle, TokenIndex};
use crate::pipeline::deadline::DeadlineGuard;
use crate::pipeline::negative_cycle::collect_negative_cycles_from_source;
use crate::pipeline::types::CycleSearchPass;
use crate::pipeline::weighted_graph::WeightedEdge;

const BELLMAN_FORD_MAX_SOURCES: usize = 15;
const BF_TIME_BUDGET: Duration = Duration::from_millis(175);

#[derive(Default)]
pub struct BfScratch {
    pub dist: Vec<f64>,
    pub dist_ratio: Vec<alloy::primitives::U256>,
    pub pred_node: Vec<Option<TokenIndex>>,
    pub pred_edge: Vec<Option<WeightedEdge>>,
    pub active: Vec<usize>,
    pub next_active: Vec<usize>,
    pub in_next: Vec<bool>,
    pub visited_scratch: Vec<u32>,
    pub visited_gen: u32,
}

impl BfScratch {
    pub fn prepare(&mut self, token_count: usize) {
        use crate::core::math::fixed_point::ONE;
        if self.dist.len() < token_count {
            self.dist.resize(token_count, f64::INFINITY);
            self.dist_ratio.resize(token_count, ONE);
            self.pred_node.resize(token_count, None);
            self.pred_edge.resize(token_count, None);
            self.in_next.resize(token_count, false);
            self.visited_scratch.resize(token_count, 0);
        } else {
            self.dist[..token_count].fill(f64::INFINITY);
            self.dist_ratio[..token_count].fill(ONE);
            self.pred_node[..token_count].fill(None);
            self.pred_edge[..token_count].fill(None);
            self.in_next[..token_count].fill(false);
        }
        self.active.clear();
        self.next_active.clear();
    }
}

thread_local! {
    static BF_SCRATCH: RefCell<BfScratch> = RefCell::new(BfScratch::default());
}

#[must_use]
pub fn find_cycles_bellman_ford_multi_pass_with_adj(
    adj: &[Vec<WeightedEdge>],
    passes: &[CycleSearchPass],
) -> Vec<FoundCycle> {
    if passes.is_empty() {
        return Vec::new();
    }
    let mut all = Vec::new();
    let mut seen = rustc_hash::FxHashSet::default();

    let token_count = adj.len();

    BF_SCRATCH.with(|scratch_cell| {
        let mut scratch = scratch_cell.borrow_mut();
        scratch.prepare(token_count);
        let BfScratch {
            dist,
            dist_ratio,
            pred_node,
            pred_edge,
            active,
            next_active,
            in_next,
            visited_scratch,
            visited_gen,
        } = &mut *scratch;

        let mut deadline = DeadlineGuard::new(BF_TIME_BUDGET);
        let sources: Vec<_> =
            crate::pipeline::cycle_finder::prioritize_cycle_start_tokens_from_out_degrees(
                adj.iter().map(Vec::len),
            )
            .into_iter()
            .take(BELLMAN_FORD_MAX_SOURCES)
            .collect();

        for pass in passes {
            for source in &sources {
                if deadline.tick() || all.len() >= pass.max_cycles {
                    break;
                }
                collect_negative_cycles_from_source(
                    *source,
                    adj,
                    pass.max_hops,
                    pass.max_cycles,
                    &mut seen,
                    &mut all,
                    dist,
                    dist_ratio,
                    pred_node,
                    pred_edge,
                    active,
                    next_active,
                    in_next,
                    visited_scratch,
                    visited_gen,
                    &mut || deadline.tick(),
                );
            }
            if deadline.tick() || all.len() >= pass.max_cycles {
                break;
            }
        }
        all
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{Edge, PoolIndex, ProtocolType, TokenIndex};
    use crate::pipeline::weighted_graph::WeightedEdge;
    use alloy::primitives::U256;

    #[test]
    fn test_empty_adj_returns_empty() {
        let adj: Vec<Vec<WeightedEdge>> = vec![];
        let r = find_cycles_bellman_ford_multi_pass_with_adj(&adj, &[]);
        assert!(r.is_empty());
    }

    /// Regression: stale edges with token_out beyond adj.len() must not panic.
    #[test]
    fn bf_skips_out_of_bounds_token_out() {
        let edge = Edge {
            pool_index: PoolIndex(0),
            token_in: TokenIndex(0),
            token_out: TokenIndex(1839),
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let adj = vec![
            vec![WeightedEdge {
                edge,
                weight: -0.01,
                ratio: U256::from(1_100_000_000_000_000_000u64),
            }],
            vec![],
        ];
        let r = find_cycles_bellman_ford_multi_pass_with_adj(
            &adj,
            &[CycleSearchPass {
                max_hops: 3,
                max_cycles: 8,
            }],
        );
        assert!(r.is_empty());
    }
}
