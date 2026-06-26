use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use rpbot::core::types::{Edge, PoolIndex, ProtocolType, TokenIndex};
use rpbot::pipeline::negative_cycle::{
    collect_negative_cycles_from_source, is_simple_cycle, route_call_count,
};
use rpbot::pipeline::weighted_graph::WeightedEdge;
use rustc_hash::FxHashSet;

/// Build a chain of edges forming a simple cycle.
fn make_simple_chain(len: usize) -> Vec<Edge> {
    let mut edges = Vec::with_capacity(len);
    for i in 0..len {
        edges.push(Edge {
            pool_index: PoolIndex(i as u32),
            token_in: TokenIndex(i as u32),
            token_out: TokenIndex((i + 1) as u32),
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        });
    }
    edges
}

fn bench_is_simple_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("is_simple_cycle");
    for len in [3usize, 5, 8, 12] {
        let edges = make_simple_chain(len);
        group.bench_function(format!("{}hops", len), |b| {
            b.iter(|| black_box(is_simple_cycle(black_box(&edges))))
        });
    }
    group.finish();
}

fn bench_is_simple_cycle_complex(c: &mut Criterion) {
    // Create a cycle with repeated token intermediates (worst case for SmallVec search).
    let mut edges = Vec::with_capacity(10);
    for i in 0..10 {
        let t_in = if i % 3 == 0 {
            TokenIndex(0)
        } else {
            TokenIndex(i as u32)
        };
        let t_out = if i % 3 == 2 {
            TokenIndex(10)
        } else {
            TokenIndex((i + 1) as u32)
        };
        edges.push(Edge {
            pool_index: PoolIndex(i as u32),
            token_in: t_in,
            token_out: t_out,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        });
    }
    c.bench_function("is_simple_cycle_worst_case", |b| {
        b.iter(|| black_box(is_simple_cycle(black_box(&edges))))
    });
}

fn bench_route_call_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_call_count");
    for len in [3usize, 6, 10] {
        let mut edges = make_simple_chain(len);
        // Mix in some V3 edges to test the protocol dispatch.
        if len > 2 {
            for e in &mut edges[1..len.min(4)] {
                e.protocol = ProtocolType::UniswapV3;
            }
        }
        group.bench_function(format!("{}hops_mixed", len), |b| {
            b.iter(|| black_box(route_call_count(black_box(&edges))))
        });
    }
    group.finish();
}

fn bench_collect_negative_cycles_from_source(c: &mut Criterion) {
    // Build a small adjacency list that will produce negative cycles.
    let n_tokens = 10usize;
    let mut adj: Vec<Vec<WeightedEdge>> = vec![Vec::new(); n_tokens];
    // Create a triangular negative cycle with edges 0->1, 1->2, 2->0.
    let edges = [
        (TokenIndex(0), TokenIndex(1), -1.0),
        (TokenIndex(1), TokenIndex(2), -2.0),
        (TokenIndex(2), TokenIndex(0), 2.0), // sum = -1.0 — negative cycle
    ];
    for (i, &(src, dst, w)) in edges.iter().enumerate() {
        adj[src.0 as usize].push(WeightedEdge {
            edge: Edge {
                pool_index: PoolIndex(i as u32),
                token_in: src,
                token_out: dst,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            weight: w,
        });
    }
    // Add non-negative edges for other tokens.
    for t in 3..n_tokens {
        adj[t - 1].push(WeightedEdge {
            edge: Edge {
                pool_index: PoolIndex(t as u32),
                token_in: TokenIndex(t as u32 - 1),
                token_out: TokenIndex(t as u32),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            weight: 0.5,
        });
    }

    let mut group = c.benchmark_group("collect_negative_cycles_from_source");
    for max_hops in [3u32, 5] {
        group.bench_function(format!("max_hops_{}", max_hops), |b| {
            b.iter(|| {
                let mut keys = FxHashSet::default();
                let mut cycles = Vec::new();
                let mut dist = vec![f64::INFINITY; n_tokens];
                let mut pred_node = vec![None; n_tokens];
                let mut pred_edge = vec![None; n_tokens];
                let mut should_stop = || false;
                black_box(collect_negative_cycles_from_source(
                    TokenIndex(0),
                    &adj,
                    max_hops,
                    100,
                    &mut keys,
                    &mut cycles,
                    &mut dist,
                    &mut pred_node,
                    &mut pred_edge,
                    &mut should_stop,
                ));
                cycles.len()
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_is_simple_cycle,
    bench_is_simple_cycle_complex,
    bench_route_call_count,
    bench_collect_negative_cycles_from_source
);
criterion_main!(benches);
