use rayon::join;

use crate::core::types::FoundCycle;
use crate::pipeline::arena::StateArena;
use crate::pipeline::bellman_ford::find_cycles_bellman_ford_multi_pass_with_adj;
use crate::pipeline::cycle_filter::{
    ProbeContext, dedupe_cycles_by_fingerprint, prefilter_cycles_by_atomic_sim_with_context,
};
use crate::pipeline::cycle_finder::find_cycles_multi_pass;
use crate::pipeline::johnson::find_cycles_johnson_multi_pass_with_adj;
use crate::pipeline::types::{CycleSearchPass, RoutingGraph};
use crate::pipeline::weighted_graph::{
    build_weighted_adjacency, compute_bf_potentials, reweight_adjacency,
};

/// Parallel DFS + Johnson hub search + Bellman-Ford, merged and atomically pref filtered.
pub fn find_cycles_hybrid_multi_pass(
    arena: &StateArena,
    graph: &RoutingGraph,
    passes: &[CycleSearchPass],
    atomic_prefilter: bool,
) -> Vec<FoundCycle> {
    find_cycles_hybrid_multi_pass_with_context(arena, graph, passes, atomic_prefilter, None)
}

pub fn find_cycles_hybrid_multi_pass_with_context(
    arena: &StateArena,
    graph: &RoutingGraph,
    passes: &[CycleSearchPass],
    atomic_prefilter: bool,
    probe_ctx: Option<&ProbeContext<'_>>,
) -> Vec<FoundCycle> {
    if passes.is_empty() {
        return Vec::new();
    }

    let mut dfs_budget = Vec::with_capacity(passes.len());
    let mut johnson_budget = Vec::with_capacity(passes.len());
    let mut bf_budget = Vec::with_capacity(passes.len());
    for pass in passes {
        let third = pass.max_cycles / 3;
        let rem = pass.max_cycles.saturating_sub(third * 2);
        dfs_budget.push(CycleSearchPass {
            max_hops: pass.max_hops,
            max_cycles: third.max(1),
        });
        johnson_budget.push(CycleSearchPass {
            max_hops: pass.max_hops,
            max_cycles: third.max(1),
        });
        bf_budget.push(CycleSearchPass {
            max_hops: pass.max_hops,
            max_cycles: rem.max(1),
        });
    }

    let base_adj = build_weighted_adjacency(arena, graph);
    let potentials = compute_bf_potentials(&base_adj, graph.token_count as usize);
    let reweighted_adj = reweight_adjacency(&base_adj, &potentials);

    let (mut dfs_cycles, (mut johnson_cycles, mut bf_cycles)) = join(
        || find_cycles_multi_pass(arena, graph, &dfs_budget),
        || {
            join(
                || {
                    find_cycles_johnson_multi_pass_with_adj(
                        graph,
                        &base_adj,
                        &reweighted_adj,
                        &johnson_budget,
                    )
                },
                || find_cycles_bellman_ford_multi_pass_with_adj(&base_adj, &bf_budget),
            )
        },
    );

    dfs_cycles.append(&mut johnson_cycles);
    dfs_cycles.append(&mut bf_cycles);

    let merged = dedupe_cycles_by_fingerprint(dfs_cycles);
    let total_cap = passes.iter().map(|p| p.max_cycles).sum();
    if atomic_prefilter {
        prefilter_cycles_by_atomic_sim_with_context(arena, merged, total_cap, probe_ctx)
    } else {
        let mut out = merged;
        if out.len() > total_cap {
            out.truncate(total_cap);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolState, ProtocolType, V2PoolState};
    use crate::pipeline::graph::{build_graph, pool_meta_from_pair};
    use alloy::primitives::Address;
    use ruint::aliases::U256;

    #[test]
    fn hybrid_finds_triangle() {
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
        let cycles = find_cycles_hybrid_multi_pass(
            &arena,
            &graph,
            &[CycleSearchPass {
                max_hops: 4,
                max_cycles: 50,
            }],
            true,
        );
        assert!(!cycles.is_empty());
        assert!(cycles.iter().any(|c| c.hop_count == 3));
    }
}
