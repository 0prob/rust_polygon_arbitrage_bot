use std::hint::black_box;

use alloy::primitives::Address;
use criterion::{Criterion, criterion_group, criterion_main};
use rpbot::core::types::{Edge, PoolState, ProtocolType, V2PoolState};
use rpbot::pipeline::arena::StateArena;
use rpbot::pipeline::local_sim::simulate_route_minimal;
use ruint::aliases::U256;

fn bench_v2_triangle_sim(c: &mut Criterion) {
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
    let p01 = arena.register_pool(Address::repeat_byte(0x10), v2(reserve, reserve));
    let p12 = arena.register_pool(Address::repeat_byte(0x11), v2(reserve, reserve));
    let p20 = arena.register_pool(Address::repeat_byte(0x12), v2(reserve, reserve));
    let edges = vec![
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
    let amount_in = U256::from(10u128).pow(U256::from(18));

    c.bench_function("v2_triangle_sim", |b| {
        b.iter(|| {
            black_box(simulate_route_minimal(
                black_box(&arena),
                black_box(&edges),
                amount_in,
            ))
        });
    });
}

criterion_group!(benches, bench_v2_triangle_sim);
criterion_main!(benches);
