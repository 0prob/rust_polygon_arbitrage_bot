use std::hint::black_box;

use alloy::primitives::Address;
use rpbot::core::types::{PoolState, ProtocolType, V2PoolState};
use rpbot::pipeline::arena::StateArena;
use rpbot::pipeline::bellman_ford::find_cycles_bellman_ford;
use rpbot::pipeline::cycle_finder::find_cycles;
use rpbot::pipeline::graph::{build_graph, pool_meta_from_pair};
use rpbot::pipeline::johnson::find_cycles_johnson_multi_pass;
use rpbot::pipeline::types::CycleSearchPass;
use criterion::{Criterion, criterion_group, criterion_main};
use ruint::aliases::U256;

fn triangle_fixture() -> (StateArena, Vec<rpbot::pipeline::types::PoolMeta>) {
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
    (arena, pools)
}

fn bench_bellman_ford_triangle(c: &mut Criterion) {
    let (arena, pools) = triangle_fixture();
    c.bench_function("bellman_ford_triangle", |b| {
        b.iter(|| {
            black_box(find_cycles_bellman_ford(
                black_box(&arena),
                black_box(&pools),
                4,
                100,
            ))
        });
    });
}

fn bench_johnson_triangle(c: &mut Criterion) {
    let (arena, pools) = triangle_fixture();
    let graph = build_graph(&arena, &pools);
    c.bench_function("johnson_triangle", |b| {
        b.iter(|| {
            black_box(find_cycles_johnson_multi_pass(
                black_box(&arena),
                black_box(&graph),
                black_box(&[CycleSearchPass {
                    max_hops: 4,
                    max_cycles: 100,
                }]),
            ))
        });
    });
}

fn bench_parallel_dfs_hub(c: &mut Criterion) {
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
    let mut tokens = Vec::new();
    let mut pools = Vec::new();
    for i in 0..10u8 {
        tokens.push(arena.register_token(Address::repeat_byte(i + 1)));
    }
    let hub = tokens[0];
    for (i, &t) in tokens.iter().enumerate().skip(1) {
        let p = arena.register_pool(
            Address::repeat_byte(0x20 + i as u8),
            v2(reserve, reserve * U256::from(2u8)),
        );
        pools.push(pool_meta_from_pair(
            p,
            ProtocolType::UniswapV2,
            hub,
            t,
            Some(30),
        ));
    }
    let graph = build_graph(&arena, &pools);
    c.bench_function("parallel_dfs_hub", |b| {
        b.iter(|| black_box(find_cycles(black_box(&arena), black_box(&graph), 4, 500)))
    });
}

criterion_group!(
    benches,
    bench_bellman_ford_triangle,
    bench_johnson_triangle,
    bench_parallel_dfs_hub
);
criterion_main!(benches);
