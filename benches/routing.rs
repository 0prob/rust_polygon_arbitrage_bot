use alloy::primitives::{Address, U256};
use criterion::{Criterion, criterion_group, criterion_main};
use rpbot::config::CycleFinderMode;
use rpbot::core::constants::MIN_HOP_TOKEN_BALANCE;
use rpbot::core::math::uniswap_v2::simulate_v2_swap;
use rpbot::core::math::uniswap_v3::simulate_v3_swap;
use rpbot::core::types::{Edge, PoolState, ProtocolType, V2PoolState, V3PoolState, V3Tick};
use rpbot::pipeline::arena::StateArena;
use rpbot::pipeline::cycle_search::find_cycles_for_mode;
use rpbot::pipeline::graph::{build_graph, pool_meta_from_pair, rescore_graph_in_place};
use rpbot::pipeline::local_sim::simulate_route_minimal;
use rpbot::pipeline::ternary::optimize_cycle;
use rpbot::pipeline::types::CycleSearchPass;
use rustc_hash::FxHashMap;
use std::hint::black_box;
use std::sync::Arc;

fn bench_v2_swap(c: &mut Criterion) {
    let state = V2PoolState {
        reserve0: U256::from(10_000_000u64) * U256::from(10u128.pow(18)),
        reserve1: U256::from(20_000_000u64) * U256::from(10u128.pow(6)),
        fee: U256::from(997u64),
        fee_denominator: U256::from(1_000u64),
        block_timestamp_last: 0,
    };
    let amount = U256::from(10u128.pow(15));
    c.bench_function("simulate_v2_swap", |b| {
        b.iter(|| simulate_v2_swap(black_box(&state), black_box(amount), true, Some(30)));
    });
}

fn bench_v3_swap(c: &mut Criterion) {
    let state = V3PoolState {
        sqrt_price_x96: U256::from(1u128 << 96),
        liquidity: 10_000_000_000,
        tick: 0,
        fee: U256::from(3000u32),
        tick_spacing: 60,
        unlocked: true,
        fee_protocol: 0,
        observation_cardinality: 1,
        ticks: Arc::from(vec![
            V3Tick {
                tick: -60,
                liquidity_gross: 5_000_000_000,
                liquidity_net: 5_000_000_000,
            },
            V3Tick {
                tick: 60,
                liquidity_gross: 5_000_000_000,
                liquidity_net: 5_000_000_000,
            },
        ]),
    };
    let amount = U256::from(10u128.pow(15));
    c.bench_function("simulate_v3_swap_ticks", |b| {
        b.iter(|| simulate_v3_swap(black_box(&state), black_box(amount), true, Some(30)));
    });
}

fn bench_route_sim(c: &mut Criterion) {
    let mut arena = StateArena::default();
    let a = arena.register_token(Address::from([1u8; 20]));
    let b = arena.register_token(Address::from([2u8; 20]));
    let c_tok = arena.register_token(Address::from([3u8; 20]));
    let pools = [
        arena.register_pool(
            Address::from([4u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: MIN_HOP_TOKEN_BALANCE * U256::from(1000u64),
                reserve1: MIN_HOP_TOKEN_BALANCE * U256::from(2000u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        ),
        arena.register_pool(
            Address::from([5u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: MIN_HOP_TOKEN_BALANCE * U256::from(2000u64),
                reserve1: MIN_HOP_TOKEN_BALANCE * U256::from(1500u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        ),
        arena.register_pool(
            Address::from([6u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: MIN_HOP_TOKEN_BALANCE * U256::from(1500u64),
                reserve1: MIN_HOP_TOKEN_BALANCE * U256::from(1000u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        ),
    ];
    let metas = [
        pool_meta_from_pair(pools[0], ProtocolType::UniswapV2, a, b, 30),
        pool_meta_from_pair(pools[1], ProtocolType::UniswapV2, b, c_tok, 30),
        pool_meta_from_pair(pools[2], ProtocolType::UniswapV2, c_tok, a, 30),
    ];
    let graph = build_graph(&arena, &metas);
    let edges: Vec<Edge> = graph.adjacency[a.0 as usize]
        .iter()
        .chain(graph.adjacency[b.0 as usize].iter())
        .chain(graph.adjacency[c_tok.0 as usize].iter())
        .map(|ge| ge.edge)
        .collect();
    let amount = U256::from(10u128.pow(15));
    c.bench_function("simulate_route_3hop", |b| {
        b.iter(|| simulate_route_minimal(black_box(&arena), black_box(&edges), black_box(amount)));
    });
}

fn bench_graph_rescore(c: &mut Criterion) {
    let mut arena = StateArena::default();
    let mut metas = Vec::new();
    for i in 0..64u8 {
        let t0 = arena.register_token(Address::from([
            i, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, i,
        ]));
        let t1 = arena.register_token(Address::from([
            i, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, i,
        ]));
        let pool = arena.register_pool(
            Address::from([i, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, i]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: MIN_HOP_TOKEN_BALANCE * U256::from(u64::from(i) + 1),
                reserve1: MIN_HOP_TOKEN_BALANCE * U256::from(u64::from(i) + 2),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        );
        metas.push(pool_meta_from_pair(
            pool,
            ProtocolType::UniswapV2,
            t0,
            t1,
            30,
        ));
    }
    let mut graph = build_graph(&arena, &metas);
    c.bench_function("rescore_graph_64_pools", |b| {
        b.iter(|| rescore_graph_in_place(black_box(&arena), black_box(&mut graph)));
    });
}

fn bench_cycle_search(c: &mut Criterion) {
    let mut arena = StateArena::default();
    let a = arena.register_token(Address::from([1u8; 20]));
    let b = arena.register_token(Address::from([2u8; 20]));
    let c_tok = arena.register_token(Address::from([3u8; 20]));
    let pools = [
        arena.register_pool(
            Address::from([4u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: MIN_HOP_TOKEN_BALANCE * U256::from(1100u64),
                reserve1: MIN_HOP_TOKEN_BALANCE * U256::from(900u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        ),
        arena.register_pool(
            Address::from([5u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: MIN_HOP_TOKEN_BALANCE * U256::from(900u64),
                reserve1: MIN_HOP_TOKEN_BALANCE * U256::from(1100u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        ),
        arena.register_pool(
            Address::from([6u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: MIN_HOP_TOKEN_BALANCE * U256::from(1000u64),
                reserve1: MIN_HOP_TOKEN_BALANCE * U256::from(1000u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        ),
    ];
    let metas = [
        pool_meta_from_pair(pools[0], ProtocolType::UniswapV2, a, b, 30),
        pool_meta_from_pair(pools[1], ProtocolType::UniswapV2, b, c_tok, 30),
        pool_meta_from_pair(pools[2], ProtocolType::UniswapV2, c_tok, a, 30),
    ];
    let graph = build_graph(&arena, &metas);
    let passes = [CycleSearchPass {
        max_hops: 4,
        max_cycles: 500,
    }];
    c.bench_function("find_cycles_hybrid_3pool", |b| {
        b.iter(|| {
            find_cycles_for_mode(
                CycleFinderMode::Hybrid,
                black_box(&arena),
                black_box(&graph),
                black_box(&metas),
                black_box(&passes),
                false,
                None,
            )
            .cycles
        });
    });
}

fn bench_optimize_cycle(c: &mut Criterion) {
    let mut arena = StateArena::default();
    let a = arena.register_token(Address::from([1u8; 20]));
    let b = arena.register_token(Address::from([2u8; 20]));
    let pool = arena.register_pool(
        Address::from([3u8; 20]),
        Arc::new(PoolState::V2(V2PoolState {
            reserve0: MIN_HOP_TOKEN_BALANCE * U256::from(1100u64),
            reserve1: MIN_HOP_TOKEN_BALANCE * U256::from(900u64),
            fee: U256::from(997u64),
            fee_denominator: U256::from(1_000u64),
            block_timestamp_last: 0,
        })),
    );
    let pool2 = arena.register_pool(
        Address::from([4u8; 20]),
        Arc::new(PoolState::V2(V2PoolState {
            reserve0: MIN_HOP_TOKEN_BALANCE * U256::from(900u64),
            reserve1: MIN_HOP_TOKEN_BALANCE * U256::from(1100u64),
            fee: U256::from(997u64),
            fee_denominator: U256::from(1_000u64),
            block_timestamp_last: 0,
        })),
    );
    let cycle = rpbot::core::types::FoundCycle {
        start_token: a,
        edges: vec![
            Edge {
                pool_index: pool,
                token_in: a,
                token_out: b,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: pool2,
                token_in: b,
                token_out: a,
                token_in_idx: 1,
                token_out_idx: 0,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
        ]
        .into(),
        hop_count: 2,
        log_weight: -0.01,
        cumulative_fee_bps: 60,
        score: -0.01,
        cycle_ratio: U256::from(1_001_000_000_000_000_000u64),
    };
    let rates = FxHashMap::default();
    let decimals = FxHashMap::default();
    let profit_ctx = rpbot::services::execution::profit::ProfitEvalContext::for_cycle(
        a,
        &arena,
        &rates,
        &decimals,
        U256::from(30_000_000_000u64),
        50,
        rpbot::core::types::FlashLoanSource::Balancer,
    );
    c.bench_function("optimize_cycle_2hop", |b| {
        b.iter(|| {
            optimize_cycle(
                black_box(&arena),
                black_box(&cycle),
                &rates,
                &decimals,
                None,
                1.0,
                None,
                Some(8),
                None,
                &profit_ctx,
                None,
                None,
                None,
            )
        });
    });
}

criterion_group!(
    benches,
    bench_v2_swap,
    bench_v3_swap,
    bench_route_sim,
    bench_graph_rescore,
    bench_cycle_search,
    bench_optimize_cycle
);
criterion_main!(benches);
