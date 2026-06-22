use std::time::Duration;

use crate::core::types::FoundCycle;
use crate::pipeline::arena::StateArena;
use crate::pipeline::deadline::DeadlineGuard;
use crate::pipeline::negative_cycle::collect_negative_cycles_from_source;
use crate::pipeline::types::{CycleSearchPass, PoolMeta, RoutingGraph};
use crate::pipeline::weighted_graph::{WeightedEdge, build_weighted_adjacency, select_hub_tokens};

const BELLMAN_FORD_MAX_SOURCES: usize = 50;
const BF_TIME_BUDGET: Duration = Duration::from_millis(1_000);

pub use crate::pipeline::negative_cycle::{is_simple_cycle, route_call_count};

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
    let mut dist = vec![f64::INFINITY; token_count];
    let mut pred_node = vec![None; token_count];
    let mut pred_edge = vec![None; token_count];

    let mut deadline = DeadlineGuard::new(BF_TIME_BUDGET);
    let sources = select_hub_tokens(adj, BELLMAN_FORD_MAX_SOURCES);

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
                &mut pred_node,
                &mut pred_edge,
                &mut || deadline.tick(),
            );
        }
        if deadline.tick() {
            break;
        }
    }
    all
}

pub fn find_cycles_bellman_ford_multi_pass(
    arena: &StateArena,
    graph: &RoutingGraph,
    passes: &[CycleSearchPass],
) -> Vec<FoundCycle> {
    let adj = build_weighted_adjacency(arena, graph);
    find_cycles_bellman_ford_multi_pass_with_adj(&adj, passes)
}

pub fn find_cycles_bellman_ford(
    arena: &StateArena,
    pools: &[PoolMeta],
    max_hops: u32,
    max_cycles: usize,
) -> Vec<FoundCycle> {
    let graph = crate::pipeline::graph::build_graph(arena, pools);
    find_cycles_bellman_ford_multi_pass(
        arena,
        &graph,
        &[CycleSearchPass {
            max_hops,
            max_cycles,
        }],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolState, ProtocolType, V2PoolState};
    use crate::pipeline::graph::pool_meta_from_pair;
    use alloy::primitives::Address;
    use ruint::aliases::U256;

    #[test]
    fn finds_triangle_with_bellman_ford() {
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
        let cycles = find_cycles_bellman_ford(&arena, &pools, 4, 100);
        assert!(!cycles.is_empty());
        assert!(cycles.iter().any(|c| c.hop_count == 3));
    }
}
