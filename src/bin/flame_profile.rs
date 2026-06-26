//! Tight CPU loops for flamegraph profiling of routing, spot-price, and swap math.
//!
//! Usage:
//!   cargo flamegraph --bin `flame_profile` -- routing
//!   cargo flamegraph --bin `flame_profile` -- math
//!   cargo flamegraph --bin `flame_profile` -- price
//!   cargo flamegraph --bin `flame_profile` -- all

use std::env;
use std::hint::black_box;

use alloy::primitives::Address;
use rpbot::core::types::{Edge, FoundCycle, PoolState, ProtocolType, V2PoolState};
use rpbot::pipeline::arena::StateArena;
use rpbot::pipeline::bellman_ford::find_cycles_bellman_ford;
use rpbot::pipeline::cycle_finder::find_cycles_multi_pass;
use rpbot::pipeline::graph::{build_graph, pool_meta_from_pair};
use rpbot::pipeline::cycle_search::find_cycles_hybrid_multi_pass;
use rpbot::pipeline::local_sim::simulate_route_minimal;
use rpbot::pipeline::spot_price::{compute_spot_price, rescore_cycles_by_spot_price};
use rpbot::pipeline::types::CycleSearchPass;
use ruint::aliases::U256;

const RING_SIZE: u8 = 64;
const ITERS: usize = 2_000;

fn build_ring() -> (
    StateArena,
    Vec<rpbot::pipeline::types::PoolMeta>,
    Vec<Edge>,
    U256,
) {
    let mut arena = StateArena::new();
    let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
    let v2 = |r0: U256, r1: U256| {
        PoolState::V2(V2PoolState {
            reserve0: r0,
            reserve1: r1,
            fee: U256::ZERO,
            fee_denominator: U256::ZERO,
        })
    };

    let mut tokens = Vec::with_capacity(RING_SIZE as usize);
    for i in 0..RING_SIZE {
        tokens.push(
            arena.register_token(Address::from_word(alloy::primitives::B256::from([
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                i + 1,
            ]))),
        );
    }

    let mut pools = Vec::with_capacity(RING_SIZE as usize);
    let mut route_edges = Vec::with_capacity(3);
    for i in 0..RING_SIZE {
        let t_in = tokens[usize::from(i)];
        let t_out = tokens[usize::from((i + 1) % RING_SIZE)];
        let skew = U256::from(u64::from(i % 5 + 1));
        let p = arena.register_pool(
            Address::from_word(alloy::primitives::B256::from([
                0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, i,
            ])),
            v2(reserve, reserve * skew),
        );
        pools.push(pool_meta_from_pair(
            p,
            ProtocolType::UniswapV2,
            t_in,
            t_out,
            Some(30),
        ));
        if i < 3 {
            route_edges.push(Edge {
                pool_index: p,
                token_in: t_in,
                token_out: t_out,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            });
        }
    }

    let amount_in = U256::from(10u128).pow(U256::from(18));
    (arena, pools, route_edges, amount_in)
}

fn profile_routing() {
    let (arena, pools, _, _) = build_ring();
    let graph = build_graph(&arena, &pools);
    let passes = vec![
        CycleSearchPass {
            max_hops: 4,
            max_cycles: 5_000,
        },
        CycleSearchPass {
            max_hops: 6,
            max_cycles: 10_000,
        },
    ];
    for _ in 0..ITERS {
        black_box(find_cycles_multi_pass(
            black_box(&arena),
            black_box(&graph),
            black_box(&passes),
        ));
        black_box(find_cycles_bellman_ford(
            black_box(&arena),
            black_box(&pools),
            6,
            10_000,
        ));
    }
}

fn profile_price() {
    let (arena, pools, _, _) = build_ring();
    let graph = build_graph(&arena, &pools);
    let passes = [CycleSearchPass {
        max_hops: 5,
        max_cycles: 2_000,
    }];
    let cycles = find_cycles_multi_pass(&arena, &graph, &passes);
    let mut rescored: Vec<FoundCycle> = cycles.clone();
    for _ in 0..ITERS {
        for ge in graph.adjacency.iter().flat_map(|adj| adj.iter()) {
            black_box(compute_spot_price(black_box(&arena), black_box(&ge.edge)));
        }
        rescored.clone_from(&cycles);
        rescore_cycles_by_spot_price(black_box(&arena), black_box(&mut rescored));
        black_box(&rescored);
    }
}

fn profile_math() {
    let (arena, _, route_edges, amount_in) = build_ring();
    for _ in 0..ITERS * 500 {
        black_box(simulate_route_minimal(
            black_box(&arena),
            black_box(&route_edges),
            amount_in,
        ));
    }
}

fn profile_coverage() {
    use rpbot::core::protocol::normalize_protocol;
    use rpbot::pipeline::cycle_filter::is_fully_simulable_route;
    use std::collections::BTreeMap;

    const LABELS: &[&str] = &[
        "UNISWAP_V2", "SUSHISWAP_V2", "QUICKSWAP_V2", "UNISWAP_V3", "SUSHISWAP_V3",
        "QUICKSWAP_V3", "KYBERSWAP_ELASTIC", "RAMSES_V3", "CURVE", "CURVE_CRYPTO",
        "BALANCER_V2", "DODO_V2", "UNISWAP_V4", "WOOFI", "UNKNOWN_DEX_XYZ",
    ];

    let mut norm_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for label in LABELS {
        let p = normalize_protocol(label);
        let key = format!("{p:?}");
        norm_map.entry(key).or_default().push((*label).to_string());
    }
    eprintln!("=== protocol normalization map ===");
    for (proto, labels) in &norm_map {
        eprintln!("  {proto}: {labels:?}");
    }

    let (arena, pools, route_edges, amount_in) = build_ring();
    let graph = build_graph(&arena, &pools);
    let passes = vec![CycleSearchPass {
        max_hops: 4,
        max_cycles: 500,
    }];
    let cycles = find_cycles_hybrid_multi_pass(&arena, &graph, &passes, true);
    eprintln!(
        "=== routing: {} pools, {} tokens, {} cycles after prefilter ===",
        pools.len(),
        arena.token_count(),
        cycles.len()
    );

    let mut sim_ok = 0u32;
    let mut sim_fail = 0u32;
    for cycle in &cycles {
        let sim = simulate_route_minimal(&arena, &cycle.edges, amount_in);
        if sim.is_some() {
            sim_ok += 1;
        } else {
            sim_fail += 1;
        }
    }
    eprintln!(
        "=== sim: ok={sim_ok} fail={sim_fail} route_simulable={} ===",
        is_fully_simulable_route(&route_edges)
    );

    let spot = compute_spot_price(&arena, &route_edges[0]);
    eprintln!("=== spot price sample (V2 hop): {spot} ===");
}

fn main() {
    let mode = env::args().nth(1).unwrap_or_else(|| "all".to_string());
    match mode.as_str() {
        "routing" => profile_routing(),
        "price" => profile_price(),
        "math" => profile_math(),
        "coverage" => profile_coverage(),
        "all" => {
            profile_routing();
            profile_price();
            profile_math();
        }
        other => {
            eprintln!("unknown mode {other:?}; use routing | price | math | coverage | all");
            std::process::exit(1);
        }
    }
}
