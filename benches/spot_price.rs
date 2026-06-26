use std::hint::black_box;

use alloy::primitives::Address;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rpbot::core::types::{Edge, FoundCycle, PoolState, ProtocolType, V2PoolState};
use rpbot::pipeline::arena::StateArena;
use rpbot::pipeline::cycle_finder::find_cycles_multi_pass;
use rpbot::pipeline::graph::{build_graph, pool_meta_from_pair};
use rpbot::pipeline::spot_price::{compute_spot_price, rescore_cycles_by_spot_price};
use rpbot::pipeline::types::CycleSearchPass;
use ruint::aliases::U256;

const POOL_COUNT: usize = 64;
const RESERVE: u128 = 1_000_000;

fn reserve(scale: u64) -> U256 {
    U256::from(RESERVE) * U256::from(scale) * U256::from(10u128).pow(U256::from(18))
}

fn v2_state(r0: U256, r1: U256) -> PoolState {
    PoolState::V2(V2PoolState {
        reserve0: r0,
        reserve1: r1,
        fee: U256::ZERO,
        fee_denominator: U256::ZERO,
    })
}

/// Dense fixture: `POOL_COUNT` overlapping 3-hop triangles (V2), ~192 unique directed edges.
fn build_fixture() -> (
    StateArena,
    Vec<rpbot::pipeline::types::PoolMeta>,
    Vec<Edge>,
    Vec<FoundCycle>,
) {
    let mut arena = StateArena::new();
    let mut pools = Vec::with_capacity(POOL_COUNT);
    let mut all_edges = Vec::new();

    for i in 0..POOL_COUNT {
        let t0 = arena.register_token(Address::repeat_byte((i * 3) as u8 + 1));
        let t1 = arena.register_token(Address::repeat_byte((i * 3 + 1) as u8 + 1));
        let t2 = arena.register_token(Address::repeat_byte((i * 3 + 2) as u8 + 1));
        let skew = (i % 5 + 1) as u64;

        let p01 = arena.register_pool(
            Address::repeat_byte(0x10 + i as u8),
            v2_state(reserve(1), reserve(skew)),
        );
        let p12 = arena.register_pool(
            Address::repeat_byte(0x50 + i as u8),
            v2_state(reserve(1), reserve(skew + 1)),
        );
        let p20 = arena.register_pool(
            Address::repeat_byte(0x90 + i as u8),
            v2_state(reserve(skew + 2), reserve(1)),
        );

        pools.push(pool_meta_from_pair(
            p01,
            ProtocolType::UniswapV2,
            t0,
            t1,
            Some(30),
        ));
        pools.push(pool_meta_from_pair(
            p12,
            ProtocolType::UniswapV2,
            t1,
            t2,
            Some(30),
        ));
        pools.push(pool_meta_from_pair(
            p20,
            ProtocolType::UniswapV2,
            t2,
            t0,
            Some(30),
        ));

        let triangle = [
            Edge {
                pool_index: p01,
                token_in: t0,
                token_out: t1,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: p12,
                token_in: t1,
                token_out: t2,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: p20,
                token_in: t2,
                token_out: t0,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
        ];
        all_edges.extend_from_slice(&triangle);
    }

    let graph = build_graph(&arena, &pools);
    let passes = [CycleSearchPass {
        max_hops: 4,
        max_cycles: 5_000,
    }];
    let cycles = find_cycles_multi_pass(&arena, &graph, &passes);

    (arena, pools, all_edges, cycles)
}

fn bench_compute_spot_all_edges(c: &mut Criterion) {
    let (arena, _, edges, _) = build_fixture();
    c.bench_function("compute_spot_price_all_edges", |b| {
        b.iter(|| {
            for edge in &edges {
                black_box(compute_spot_price(black_box(&arena), black_box(edge)));
            }
        });
    });
}

fn bench_rescore_cycles(c: &mut Criterion) {
    let (arena, _, _, cycles) = build_fixture();
    assert!(!cycles.is_empty(), "fixture must yield cycles");

    let mut group = c.benchmark_group("rescore_cycles_by_spot_price");
    group.bench_with_input(
        BenchmarkId::new("cycles", cycles.len()),
        &cycles,
        |b, cycles| {
            let mut buf = cycles.clone();
            b.iter(|| {
                buf.clone_from(cycles);
                rescore_cycles_by_spot_price(black_box(&arena), black_box(&mut buf));
                black_box(&buf);
            });
        },
    );
    group.finish();
}

fn bench_hf_rescore_cap(c: &mut Criterion) {
    let (arena, _, _, mut cycles) = build_fixture();
    cycles.truncate(150);

    c.bench_function("hf_rescore_150_cycles", |b| {
        b.iter(|| {
            let mut buf = cycles.clone();
            rescore_cycles_by_spot_price(black_box(&arena), black_box(&mut buf));
            black_box(&buf);
        });
    });
}

criterion_group!(
    benches,
    bench_compute_spot_all_edges,
    bench_rescore_cycles,
    bench_hf_rescore_cap
);
criterion_main!(benches);
