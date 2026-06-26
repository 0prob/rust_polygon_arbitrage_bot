//! Scaling benchmarks for graph build, cycle find, and gas estimation at 1000+ pools.
use std::hint::black_box;
use std::sync::Arc;

use alloy::primitives::Address;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rpbot::core::types::{PoolState, ProtocolType, V2PoolState};
use rpbot::pipeline::arena::StateArena;
use rpbot::pipeline::cycle_finder::find_cycles_multi_pass;
use rpbot::pipeline::graph::{build_graph, pool_meta_from_pair, rescore_graph_in_place, rescore_pools_in_place};
use rpbot::pipeline::graph_cache::connectivity_fingerprint;
use rpbot::pipeline::local_sim::simulate_route_minimal;
use rpbot::pipeline::spot_price::rescore_cycles_by_spot_price;
use rpbot::pipeline::types::CycleSearchPass;
use rpbot::services::execution::gas::estimate_route_gas_from_hops;
use ruint::aliases::U256;

fn ring_fixture(
    n_tokens: usize,
    n_pools: usize,
) -> (StateArena, Vec<rpbot::pipeline::types::PoolMeta>) {
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

    let mut tokens = Vec::with_capacity(n_tokens);
    for i in 0..n_tokens {
        tokens.push(arena.register_token(Address::repeat_byte((i % 250 + 1) as u8)));
    }

    let mut pools = Vec::with_capacity(n_pools);
    for i in 0..n_pools {
        let t_in = tokens[i % n_tokens];
        let t_out = tokens[(i + 1) % n_tokens];
        let skew = U256::from((i % 7 + 1) as u64);
        let p = arena.register_pool(
            Address::from_word(alloy::primitives::B256::from([
                0x20,
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
                (i % 256) as u8,
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
    }
    (arena, pools)
}

fn bench_graph_build_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_build_scaling");
    for n in [100usize, 500, 1000, 2000] {
        let (arena, pools) = ring_fixture(n, n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(build_graph(black_box(&arena), black_box(&pools))));
        });
    }
    group.finish();
}

fn bench_graph_rescore_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_rescore_scaling");
    for n in [100usize, 500, 1000, 2000] {
        let (mut arena, pools) = ring_fixture(n, n);
        let mut graph = build_graph(&arena, &pools);
        // Perturb reserves so rescoring does real work.
        for i in 0..n.min(32) {
            if let Some(addr) = arena.pool_address(rpbot::core::types::PoolIndex(i as u32)) {
                let reserve = U256::from(2_000_000u128) * U256::from(10u128).pow(U256::from(18));
                arena.register_pool(
                    addr,
                    PoolState::V2(V2PoolState {
                        reserve0: reserve,
                        reserve1: reserve,
                        fee: U256::ZERO,
                        fee_denominator: U256::ZERO,
                    }),
                );
            }
        }
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                rescore_graph_in_place(black_box(&arena), black_box(&mut graph));
            });
        });
    }
    group.finish();
}

fn bench_cycle_find_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("cycle_find_scaling");
    group.sample_size(30);
    for n in [64usize, 128, 256] {
        let (arena, pools) = ring_fixture(n, n);
        let graph = Arc::new(build_graph(&arena, &pools));
        let passes = [CycleSearchPass {
            max_hops: 4,
            max_cycles: 500,
        }];
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                black_box(find_cycles_multi_pass(
                    black_box(&arena),
                    black_box(graph.as_ref()),
                    black_box(&passes),
                ))
            });
        });
    }
    group.finish();
}

fn bench_gas_estimate(c: &mut Criterion) {
    c.bench_function("gas_estimate_8hop", |b| {
        b.iter(|| {
            black_box(estimate_route_gas_from_hops(
                black_box(120_000),
                black_box(8),
            ))
        });
    });
}

fn bench_sim_route(c: &mut Criterion) {
    let (arena, pools) = ring_fixture(8, 8);
    let graph = build_graph(&arena, &pools);
    let passes = [CycleSearchPass {
        max_hops: 8,
        max_cycles: 10,
    }];
    let cycles = find_cycles_multi_pass(&arena, &graph, &passes);
    let Some(cycle) = cycles.first() else {
        return;
    };
    let amount = U256::from(10u128).pow(U256::from(18));
    c.bench_function("simulate_route_minimal_8hop", |b| {
        b.iter(|| {
            black_box(simulate_route_minimal(
                black_box(&arena),
                black_box(&cycle.edges),
                amount,
            ))
        });
    });
}

fn bench_connectivity_fingerprint(c: &mut Criterion) {
    let mut group = c.benchmark_group("connectivity_fingerprint");
    for n in [100usize, 1000, 5000] {
        let (arena, pools) = ring_fixture(n, n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                black_box(connectivity_fingerprint(
                    black_box(&arena),
                    black_box(&pools),
                ))
            });
        });
    }
    group.finish();
}

fn bench_cycle_rescore(c: &mut Criterion) {
    let (arena, pools) = ring_fixture(128, 128);
    let graph = build_graph(&arena, &pools);
    let passes = [CycleSearchPass {
        max_hops: 4,
        max_cycles: 200,
    }];
    let mut cycles = find_cycles_multi_pass(&arena, &graph, &passes);
    c.bench_function("rescore_cycles_200", |b| {
        b.iter(|| {
            rescore_cycles_by_spot_price(black_box(&arena), black_box(&mut cycles));
            black_box(&cycles);
        });
    });
}

fn bench_graph_partial_rescore(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph_partial_rescore");
    for n in [1000usize, 2000] {
        let (mut arena, pools) = ring_fixture(n, n);
        let mut graph = build_graph(&arena, &pools);
        let dirty_count = 8usize;
        let dirty: Vec<_> = (0..dirty_count)
            .map(|i| rpbot::core::types::PoolIndex(i as u32))
            .collect();
        for i in 0..dirty_count {
            if let Some(addr) = arena.pool_address(rpbot::core::types::PoolIndex(i as u32)) {
                let reserve = U256::from(2_000_000u128) * U256::from(10u128).pow(U256::from(18));
                arena.register_pool(
                    addr,
                    PoolState::V2(V2PoolState {
                        reserve0: reserve,
                        reserve1: reserve,
                        fee: U256::ZERO,
                        fee_denominator: U256::ZERO,
                    }),
                );
            }
        }
        group.bench_with_input(
            BenchmarkId::new("pools", format!("{n}/dirty={dirty_count}")),
            &n,
            |b, _| {
                b.iter(|| {
                    rescore_pools_in_place(black_box(&arena), black_box(&mut graph), black_box(&dirty));
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_graph_build_scaling,
    bench_graph_rescore_scaling,
    bench_graph_partial_rescore,
    bench_cycle_find_scaling,
    bench_gas_estimate,
    bench_sim_route,
    bench_connectivity_fingerprint,
    bench_cycle_rescore
);
criterion_main!(benches);
