use alloy::primitives::U256;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use rpbot::config::CycleFinderMode;
use rpbot::core::constants::MIN_HOP_TOKEN_BALANCE;
use rpbot::core::math::uniswap_v2::simulate_v2_swap;
use rpbot::core::math::uniswap_v3::simulate_v3_swap;
use rpbot::core::types::{Edge, ProtocolType, V2PoolState, V3PoolState, V3Tick};
use rpbot::pipeline::cycle_search::find_cycles_for_mode;
use rpbot::pipeline::graph::rescore_graph_in_place;
use rpbot::pipeline::local_sim::simulate_route_minimal;
use rpbot::pipeline::ternary::optimize_cycle;
use rpbot::pipeline::types::CycleSearchPass;
use rpbot::test_support::FixtureBuilder;
use rustc_hash::FxHashMap;
use std::hint::black_box;
use std::sync::Arc;

fn bench_swaps(c: &mut Criterion) {
    let mut group = c.benchmark_group("swap");

    let v2 = V2PoolState {
        reserve0: U256::from(10_000_000u64) * U256::from(10u128.pow(18)),
        reserve1: U256::from(20_000_000u64) * U256::from(10u128.pow(6)),
        fee: U256::from(997u64),
        fee_denominator: U256::from(1_000u64),
        block_timestamp_last: 0,
    };
    let amount = U256::from(10u128.pow(15));
    group.bench_function("v2", |b| {
        b.iter(|| simulate_v2_swap(black_box(&v2), black_box(amount), true, Some(30)));
    });

    let v3 = V3PoolState {
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
    group.bench_function("v3_ticks", |b| {
        b.iter(|| simulate_v3_swap(black_box(&v3), black_box(amount), true, Some(30), false));
    });

    group.finish();
}

fn bench_route_sim(c: &mut Criterion) {
    let mut fx = FixtureBuilder::new();
    let a = fx.token(1);
    let b = fx.token(2);
    let c_tok = fx.token(3);
    fx.v2_pool(
        4,
        ProtocolType::UniswapV2,
        a,
        b,
        MIN_HOP_TOKEN_BALANCE * U256::from(1000u64),
        MIN_HOP_TOKEN_BALANCE * U256::from(2000u64),
        30,
    );
    fx.v2_pool(
        5,
        ProtocolType::UniswapV2,
        b,
        c_tok,
        MIN_HOP_TOKEN_BALANCE * U256::from(2000u64),
        MIN_HOP_TOKEN_BALANCE * U256::from(1500u64),
        30,
    );
    fx.v2_pool(
        6,
        ProtocolType::UniswapV2,
        c_tok,
        a,
        MIN_HOP_TOKEN_BALANCE * U256::from(1500u64),
        MIN_HOP_TOKEN_BALANCE * U256::from(1000u64),
        30,
    );
    let graph = fx.build_graph();
    let edges: Vec<Edge> = graph.adjacency[a.0 as usize]
        .iter()
        .chain(graph.adjacency[b.0 as usize].iter())
        .chain(graph.adjacency[c_tok.0 as usize].iter())
        .map(|ge| ge.edge)
        .collect();
    let amount = U256::from(10u128.pow(15));

    let mut group = c.benchmark_group("route");
    group.throughput(Throughput::Elements(3));
    group.bench_function("simulate_3hop", |b| {
        b.iter(|| {
            simulate_route_minimal(black_box(&fx.arena), black_box(&edges), black_box(amount))
        });
    });
    group.finish();
}

fn bench_graph_rescore(c: &mut Criterion) {
    let mut fx = FixtureBuilder::new();
    for i in 0..64u8 {
        let t0 = fx.token(i);
        let t1 = fx.token(i + 64);
        fx.v2_pool(
            i + 128,
            ProtocolType::UniswapV2,
            t0,
            t1,
            MIN_HOP_TOKEN_BALANCE * U256::from(u64::from(i) + 1),
            MIN_HOP_TOKEN_BALANCE * U256::from(u64::from(i) + 2),
            30,
        );
    }
    let graph = fx.build_graph();

    let mut group = c.benchmark_group("graph");
    group.throughput(Throughput::Elements(64));
    // rescore mutates + may compact adjacency — clone per iteration (Criterion timing-loop guide).
    group.bench_function("rescore_64_pools", |b| {
        b.iter_batched(
            || graph.clone(),
            |mut g| rescore_graph_in_place(black_box(&fx.arena), black_box(&mut g)),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_cycle_search(c: &mut Criterion) {
    let mut fx = FixtureBuilder::new();
    let a = fx.token(1);
    let b = fx.token(2);
    let c_tok = fx.token(3);
    fx.v2_pool(
        4,
        ProtocolType::UniswapV2,
        a,
        b,
        MIN_HOP_TOKEN_BALANCE * U256::from(1100u64),
        MIN_HOP_TOKEN_BALANCE * U256::from(900u64),
        30,
    );
    fx.v2_pool(
        5,
        ProtocolType::UniswapV2,
        b,
        c_tok,
        MIN_HOP_TOKEN_BALANCE * U256::from(900u64),
        MIN_HOP_TOKEN_BALANCE * U256::from(1100u64),
        30,
    );
    fx.v2_pool(
        6,
        ProtocolType::UniswapV2,
        c_tok,
        a,
        MIN_HOP_TOKEN_BALANCE * U256::from(1000u64),
        MIN_HOP_TOKEN_BALANCE * U256::from(1000u64),
        30,
    );
    let graph = fx.build_graph();
    let passes = [CycleSearchPass {
        max_hops: 4,
        max_cycles: 500,
    }];

    let mut group = c.benchmark_group("search");
    // Drop Vec<FoundCycle> outside the timed loop (Criterion: iter_with_large_drop).
    group.bench_function("find_cycles_hybrid_3pool", |b| {
        b.iter_with_large_drop(|| {
            find_cycles_for_mode(
                CycleFinderMode::Hybrid,
                black_box(&fx.arena),
                black_box(&graph),
                black_box(fx.metas()),
                black_box(&passes),
                false,
                None,
            )
            .cycles
        });
    });
    group.finish();
}

fn bench_optimize_cycle(c: &mut Criterion) {
    let mut fx = FixtureBuilder::new();
    let a = fx.token(1);
    let b = fx.token(2);
    let pool = fx.v2_pool(
        3,
        ProtocolType::UniswapV2,
        a,
        b,
        MIN_HOP_TOKEN_BALANCE * U256::from(1100u64),
        MIN_HOP_TOKEN_BALANCE * U256::from(900u64),
        30,
    );
    let pool2 = fx.v2_pool(
        4,
        ProtocolType::UniswapV2,
        b,
        a,
        MIN_HOP_TOKEN_BALANCE * U256::from(900u64),
        MIN_HOP_TOKEN_BALANCE * U256::from(1100u64),
        30,
    );
    let arena = &fx.arena;
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
        arena,
        &rates,
        &decimals,
        U256::from(30_000_000_000u64),
        50,
        rpbot::core::types::FlashLoanSource::Balancer,
    );

    let mut group = c.benchmark_group("optimize");
    group.bench_function("cycle_2hop", |b| {
        b.iter(|| {
            optimize_cycle(
                black_box(arena),
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
    group.finish();
}

criterion_group!(
    benches,
    bench_swaps,
    bench_route_sim,
    bench_graph_rescore,
    bench_cycle_search,
    bench_optimize_cycle
);
criterion_main!(benches);
