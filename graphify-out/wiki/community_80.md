# Community 80: make_simple_chain()

**Members:** 12

## Nodes

- **negative_cycle** (`benches_negative_cycle_rs`, File, degree: 11)
- **bench_collect_negative_cycles_from_source()** (`benches_negative_cycle_rs_bench_collect_negative_cycles_from_source`, Function, degree: 1)
- **bench_is_simple_cycle()** (`benches_negative_cycle_rs_bench_is_simple_cycle`, Function, degree: 2)
- **bench_is_simple_cycle_complex()** (`benches_negative_cycle_rs_bench_is_simple_cycle_complex`, Function, degree: 1)
- **bench_route_call_count()** (`benches_negative_cycle_rs_bench_route_call_count`, Function, degree: 2)
- **criterion::{Criterion, criterion_group, criterion_main}** (`benches_negative_cycle_rs_import_criterion_criterion_criterion_group_criterion_main`, Module, degree: 1)
- **rpbot::core::types::{Edge, PoolIndex, ProtocolType, TokenIndex}** (`benches_negative_cycle_rs_import_rpbot_core_types_edge_poolindex_protocoltype_tokenindex`, Module, degree: 1)
- **rpbot::pipeline::negative_cycle::{collect_negative_cycles_from_source, is_simple_cycle, route_call_count}** (`benches_negative_cycle_rs_import_rpbot_pipeline_negative_cycle_collect_negative_cycles_from_source_is_simple_cycle_route_call_count`, Module, degree: 1)
- **rpbot::pipeline::weighted_graph::WeightedEdge** (`benches_negative_cycle_rs_import_rpbot_pipeline_weighted_graph_weightededge`, Module, degree: 1)
- **rustc_hash::FxHashSet** (`benches_negative_cycle_rs_import_rustc_hash_fxhashset`, Module, degree: 1)
- **std::hint::black_box** (`benches_negative_cycle_rs_import_std_hint_black_box`, Module, degree: 1)
- **make_simple_chain()** (`benches_negative_cycle_rs_make_simple_chain`, Function, degree: 3)

## Relationships

- benches_negative_cycle_rs → benches_negative_cycle_rs_import_std_hint_black_box (imports)
- benches_negative_cycle_rs → benches_negative_cycle_rs_import_rpbot_core_types_edge_poolindex_protocoltype_tokenindex (imports)
- benches_negative_cycle_rs → benches_negative_cycle_rs_import_rpbot_pipeline_negative_cycle_collect_negative_cycles_from_source_is_simple_cycle_route_call_count (imports)
- benches_negative_cycle_rs → benches_negative_cycle_rs_import_rpbot_pipeline_weighted_graph_weightededge (imports)
- benches_negative_cycle_rs → benches_negative_cycle_rs_import_criterion_criterion_criterion_group_criterion_main (imports)
- benches_negative_cycle_rs → benches_negative_cycle_rs_import_rustc_hash_fxhashset (imports)
- benches_negative_cycle_rs → benches_negative_cycle_rs_make_simple_chain (defines)
- benches_negative_cycle_rs → benches_negative_cycle_rs_bench_is_simple_cycle (defines)
- benches_negative_cycle_rs → benches_negative_cycle_rs_bench_is_simple_cycle_complex (defines)
- benches_negative_cycle_rs → benches_negative_cycle_rs_bench_route_call_count (defines)
- benches_negative_cycle_rs → benches_negative_cycle_rs_bench_collect_negative_cycles_from_source (defines)
- benches_negative_cycle_rs_bench_is_simple_cycle → benches_negative_cycle_rs_make_simple_chain (calls)
- benches_negative_cycle_rs_bench_route_call_count → benches_negative_cycle_rs_make_simple_chain (calls)

