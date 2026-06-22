use std::hint::black_box;
use std::sync::Arc;

use alloy::primitives::Address;
use rpbot::core::types::{PoolState, ProtocolType, V2PoolState};
use rpbot::orchestrator::hf_eval::{HfEvalInput, evaluate_cycles_parallel};
use rpbot::pipeline::arena::StateArena;
use rpbot::pipeline::graph::{build_graph, pool_meta_from_pair};
use rpbot::pipeline::spot_price::{SpotTable, rescore_cycles_with_table_and_gas};
use rpbot::pipeline::types::{compare_cycle_score, CycleSearchPass};
use rpbot::pipeline::cycle_finder::find_cycles_multi_pass;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ruint::aliases::U256;
use rustc_hash::FxHashMap;

fn ring_fixture(n_tokens: usize, n_pools: usize) -> (StateArena, rpbot::pipeline::types::RoutingGraph, Vec<rpbot::core::types::FoundCycle>) {
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
        tokens.push(arena.register_token(Address::repeat_byte((i + 1) as u8)));
    }

    let mut pools = Vec::new();
    for i in 0..n_pools.min(n_tokens) {
        let t_in = tokens[i];
        let t_out = tokens[(i + 1) % n_tokens];
        let skew = U256::from((i % 5 + 1) as u64);
        let p = arena.register_pool(
            Address::repeat_byte(0x20 + i as u8),
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

    let graph = build_graph(&arena, &pools);
    let passes = [CycleSearchPass {
        max_hops: 5,
        max_cycles: 500,
    }];
    let cycles = find_cycles_multi_pass(&arena, &graph, &passes);
    (arena, graph, cycles)
}

fn bench_hf_rescore_eval(c: &mut Criterion) {
    let (arena, _graph, cycles) = ring_fixture(32, 32);
    let gas = U256::from(30_000_000_000u64);
    let input = HfEvalInput {
        arena: &arena,
        token_to_matic_rates: &FxHashMap::default(),
        token_decimals: &std::collections::HashMap::new(),
        brent_iters: 8,
        min_profit_matic: U256::from(10u128).pow(U256::from(18)),
        gas_price: gas,
        slippage_bps: 50,
        flash_source: rpbot::core::types::FlashLoanSource::Balancer,
        max_flash_loan_usd: 50_000,
        min_profit_roi_bps: 0,
        safety_multiplier_bps: 1_000,
    };

    let mut group = c.benchmark_group("hf_rescore_eval");
    group.throughput(Throughput::Elements(cycles.len().max(1) as u64));
    group.bench_function("rescore_and_eval_100", |b| {
        let eval_cycles: Vec<_> = cycles.iter().take(100).cloned().collect();
        b.iter(|| {
            let mut cyc = eval_cycles.clone();
            let mut table = SpotTable::new(arena.pool_count());
            rescore_cycles_with_table_and_gas(
                black_box(&arena),
                &mut table,
                black_box(&mut cyc),
                Some(gas),
                None,
            );
            cyc.sort_by(compare_cycle_score);
            black_box(evaluate_cycles_parallel(black_box(&cyc), black_box(&input)));
        });
    });
    group.finish();
}

fn bench_arena_hot_patch(c: &mut Criterion) {
    let mut arena = StateArena::new();
    let cache = rpbot::services::state_cache::StateCache::default();
    let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
    let mut addrs = Vec::new();
    for i in 0..50u8 {
        let addr = Address::repeat_byte(0x40 + i);
        addrs.push(addr);
        cache.insert(
            addr,
            PoolState::V2(V2PoolState {
                reserve0: reserve,
                reserve1: reserve,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
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

    c.bench_function("apply_hot_cache_50", |b| {
        b.iter(|| {
            let mut a = arena.clone();
            a.apply_hot_cache(black_box(&cache), black_box(&addrs));
            black_box(&a);
        });
    });
}

fn bench_graph_fingerprint(c: &mut Criterion) {
    let cache = Arc::new(rpbot::services::state_cache::StateCache::default());
    let mut group = c.benchmark_group("graph_fingerprint");
    for n in [100usize, 1000, 5000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| black_box(rpbot::pipeline::graph_cache::pool_fingerprint(&cache, n)));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_hf_rescore_eval, bench_arena_hot_patch, bench_graph_fingerprint);
criterion_main!(benches);
