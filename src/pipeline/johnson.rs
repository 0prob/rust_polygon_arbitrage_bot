use std::time::Duration;

use crate::core::types::FoundCycle;
use crate::pipeline::arena::StateArena;
use crate::pipeline::deadline::DeadlineGuard;
use crate::pipeline::negative_cycle::collect_negative_cycles_from_source;
use crate::pipeline::types::{CycleSearchPass, RoutingGraph};
use crate::pipeline::weighted_graph::{
    WeightedEdge, build_weighted_adjacency, compute_bf_potentials, reweight_adjacency,
    select_hub_tokens,
};

const JOHNSON_MAX_HUBS: usize = 40;
const JOHNSON_TIME_BUDGET: Duration = Duration::from_millis(1_500);

pub fn find_cycles_johnson_multi_pass_with_adj(
    graph: &RoutingGraph,
    base_adj: &[Vec<WeightedEdge>],
    reweighted_adj: &[Vec<WeightedEdge>],
    passes: &[CycleSearchPass],
) -> Vec<FoundCycle> {
    if passes.is_empty() {
        return Vec::new();
    }

    let hubs = select_hub_tokens(base_adj, JOHNSON_MAX_HUBS);

    let mut all = Vec::new();
    let mut seen = rustc_hash::FxHashSet::default();
    let token_count = graph.token_count as usize;
    let mut dist = vec![f64::INFINITY; token_count];
    let mut pred_node = vec![None; token_count];
    let mut pred_edge = vec![None; token_count];
    let mut deadline = DeadlineGuard::new(JOHNSON_TIME_BUDGET);

    for pass in passes {
        for hub in &hubs {
            if deadline.tick() || all.len() >= pass.max_cycles {
                break;
            }
            let mut stop = || deadline.tick();
            collect_negative_cycles_from_source(
                *hub,
                reweighted_adj,
                pass.max_hops,
                pass.max_cycles,
                &mut seen,
                &mut all,
                &mut dist,
                &mut pred_node,
                &mut pred_edge,
                &mut stop,
            );
        }
        if deadline.tick() {
            break;
        }
    }
    all
}

/// Johnson-style hub search: super-source potentials + reweighted per-hub Bellman-Ford.
pub fn find_cycles_johnson_multi_pass(
    arena: &StateArena,
    graph: &RoutingGraph,
    passes: &[CycleSearchPass],
) -> Vec<FoundCycle> {
    if passes.is_empty() {
        return Vec::new();
    }

    let base_adj = build_weighted_adjacency(arena, graph);
    let potentials = compute_bf_potentials(&base_adj, graph.token_count as usize);
    let adj = reweight_adjacency(&base_adj, &potentials);
    find_cycles_johnson_multi_pass_with_adj(graph, &base_adj, &adj, passes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolState, ProtocolType, V2PoolState};
    use crate::pipeline::graph::{build_graph, pool_meta_from_pair};
    use alloy::primitives::Address;
    use ruint::aliases::U256;

    #[test]
    fn johnson_finds_triangle() {
        let mut arena = StateArena::new();
        let t0 = arena.register_token(Address::repeat_byte(1));
        let t1 = arena.register_token(Address::repeat_byte(2));
        let t2 = arena.register_token(Address::repeat_byte(3));
        let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
        let v2 = |r0: U256, r1: U256| {
            PoolState::V2(V2PoolState {
                reserve0: r0,
                reserve1: r1,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            })
        };
        let p01 = arena.register_pool(
            Address::repeat_byte(0x10),
            v2(reserve, reserve * U256::from(2u8)),
        );
        let p12 = arena.register_pool(
            Address::repeat_byte(0x11),
            v2(reserve, reserve * U256::from(2u8)),
        );
        let p20 = arena.register_pool(
            Address::repeat_byte(0x12),
            v2(reserve * U256::from(2u8), reserve),
        );
        let pools = vec![
            pool_meta_from_pair(p01, ProtocolType::UniswapV2, t0, t1, Some(30)),
            pool_meta_from_pair(p12, ProtocolType::UniswapV2, t1, t2, Some(30)),
            pool_meta_from_pair(p20, ProtocolType::UniswapV2, t2, t0, Some(30)),
        ];
        let graph = build_graph(&arena, &pools);
        let cycles = find_cycles_johnson_multi_pass(
            &arena,
            &graph,
            &[CycleSearchPass {
                max_hops: 4,
                max_cycles: 50,
            }],
        );
        assert!(!cycles.is_empty());
        assert!(cycles.iter().any(|c| c.hop_count == 3));
    }
}
