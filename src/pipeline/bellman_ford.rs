use std::time::Duration;

use crate::core::types::FoundCycle;
use crate::pipeline::deadline::DeadlineGuard;
use crate::pipeline::negative_cycle::collect_negative_cycles_from_source;
use crate::pipeline::types::CycleSearchPass;
use crate::pipeline::weighted_graph::WeightedEdge;

const BELLMAN_FORD_MAX_SOURCES: usize = 15;
const BF_TIME_BUDGET: Duration = Duration::from_millis(175);

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
    use crate::core::math::fixed_point::ONE;
    let mut dist = vec![f64::INFINITY; token_count];
    let mut dist_ratio = vec![ONE; token_count];
    let mut pred_node = vec![None; token_count];
    let mut pred_edge = vec![None; token_count];
    let mut active = Vec::with_capacity(token_count);
    let mut next_active = Vec::with_capacity(token_count);
    let mut in_next = vec![false; token_count];

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
                &mut dist,
                &mut dist_ratio,
                &mut pred_node,
                &mut pred_edge,
                &mut active,
                &mut next_active,
                &mut in_next,
                &mut || deadline.tick(),
            );
        }
        if deadline.tick() || all.len() >= pass.max_cycles {
            break;
        }
    }
    all
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::weighted_graph::WeightedEdge;

    #[test]
    fn test_empty_adj_returns_empty() {
        let adj: Vec<Vec<WeightedEdge>> = vec![];
        let r = find_cycles_bellman_ford_multi_pass_with_adj(&adj, &[]);
        assert!(r.is_empty());
    }
}
