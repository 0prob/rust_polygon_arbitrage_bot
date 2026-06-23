use std::hint::black_box;

use alloy::primitives::Address;
use rpbot::core::types::{FoundCycle, PoolState, ProtocolType, V2PoolState};
use rpbot::pipeline::arena::StateArena;
use rpbot::pipeline::cycle_filter::{dedupe_cycles_by_fingerprint, prefilter_cycles_by_atomic_sim};
use rpbot::pipeline::cycle_finder::find_cycles_multi_pass;
use rpbot::pipeline::graph::{build_graph, pool_meta_from_pair};
use rpbot::pipeline::types::CycleSearchPass;
use criterion::{Criterion, criterion_group, criterion_main};
use ruint::aliases::U256;

/// Build a ring graph and return the arena, graph, and discovered cycles.
fn ring_fixture(n: usize, max_hops: u32, max_cycles: usize) -> (StateArena, Vec<FoundCycle>) {
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
    let tokens: Vec<_> = (0..n)
        .map(|i| arena.register_token(Address::repeat_byte((i % 251 + 1) as u8)))
        .collect();
    let mut pools = Vec::with_capacity(n);
    for i in 0..n {
        let t_in = tokens[i];
        let t_out = tokens[(i + 1) % n];
        let p = arena.register_pool(
            Address::repeat_byte(0x20 + (i % 200) as u8),
            v2(reserve, reserve * U256::from((i % 5 + 1) as u64)),
        );
        pools.push(pool_meta_from_pair(p, ProtocolType::UniswapV2, t_in, t_out, Some(30)));
    }
    let graph = build_graph(&arena, &pools);
    let cycles = find_cycles_multi_pass(&arena, &graph, &[CycleSearchPass { max_hops, max_cycles }]);
    (arena, cycles)
}

fn bench_prefilter_atomic_sim(c: &mut Criterion) {
    let (_arena, cycles) = ring_fixture(80, 5, 10_000);
    let mut group = c.benchmark_group("prefilter_cycles_by_atomic_sim");
    for keep in [100usize, 500, 2000] {
        group.bench_function(format!("keep_{}", keep), |b| {
            let mut buf = cycles.clone();
            b.iter(|| {
                buf.clone_from(&cycles);
                black_box(prefilter_cycles_by_atomic_sim(black_box(&_arena), black_box(buf.clone()), keep));
            });
        });
    }
    group.finish();
}

fn bench_dedupe_fingerprint(c: &mut Criterion) {
    let (_arena, cycles) = ring_fixture(64, 4, 5_000);
    let mut group = c.benchmark_group("dedupe_cycles_by_fingerprint");
    group.bench_function("ring_5000", |b| {
        let mut buf = cycles.clone();
        b.iter(|| {
            buf.clone_from(&cycles);
            black_box(dedupe_cycles_by_fingerprint(black_box(buf.clone())));
        });
    });
    group.finish();
}

/// Benchmark with cycles that have many duplicate fingerprints (worst case for dedupe).
fn bench_dedupe_high_duplicates(c: &mut Criterion) {
    let (_arena, cycles) = ring_fixture(32, 4, 1_000);
    let dupe_cycles: Vec<FoundCycle> = cycles
        .iter()
        .flat_map(|c| {
            let mut a = c.clone();
            a.score -= 0.1;
            let mut b = c.clone();
            b.score += 0.05;
            [c.clone(), a, b]
        })
        .collect();

    c.bench_function("dedupe_high_duplicates", |b| {
        let mut buf = dupe_cycles.clone();
        b.iter(|| {
            buf.clone_from(&dupe_cycles);
            black_box(dedupe_cycles_by_fingerprint(black_box(buf.clone())));
        });
    });
}

criterion_group!(
    benches,
    bench_prefilter_atomic_sim,
    bench_dedupe_fingerprint,
    bench_dedupe_high_duplicates
);
criterion_main!(benches);
