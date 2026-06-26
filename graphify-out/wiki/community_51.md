# Community 51: ring_fixture()

**Members:** 15

## Nodes

- **cycle_filter** (`benches_cycle_filter_rs`, File, degree: 14)
- **bench_dedupe_fingerprint()** (`benches_cycle_filter_rs_bench_dedupe_fingerprint`, Function, degree: 2)
- **bench_dedupe_high_duplicates()** (`benches_cycle_filter_rs_bench_dedupe_high_duplicates`, Function, degree: 2)
- **bench_prefilter_atomic_sim()** (`benches_cycle_filter_rs_bench_prefilter_atomic_sim`, Function, degree: 2)
- **alloy::primitives::Address** (`benches_cycle_filter_rs_import_alloy_primitives_address`, Module, degree: 1)
- **criterion::{Criterion, criterion_group, criterion_main}** (`benches_cycle_filter_rs_import_criterion_criterion_criterion_group_criterion_main`, Module, degree: 1)
- **rpbot::core::types::{FoundCycle, PoolState, ProtocolType, V2PoolState}** (`benches_cycle_filter_rs_import_rpbot_core_types_foundcycle_poolstate_protocoltype_v2poolstate`, Module, degree: 1)
- **rpbot::pipeline::arena::StateArena** (`benches_cycle_filter_rs_import_rpbot_pipeline_arena_statearena`, Module, degree: 1)
- **rpbot::pipeline::cycle_filter::{dedupe_cycles_by_fingerprint, prefilter_cycles_by_atomic_sim}** (`benches_cycle_filter_rs_import_rpbot_pipeline_cycle_filter_dedupe_cycles_by_fingerprint_prefilter_cycles_by_atomic_sim`, Module, degree: 1)
- **rpbot::pipeline::cycle_finder::find_cycles_multi_pass** (`benches_cycle_filter_rs_import_rpbot_pipeline_cycle_finder_find_cycles_multi_pass`, Module, degree: 1)
- **rpbot::pipeline::graph::{build_graph, pool_meta_from_pair}** (`benches_cycle_filter_rs_import_rpbot_pipeline_graph_build_graph_pool_meta_from_pair`, Module, degree: 1)
- **rpbot::pipeline::types::CycleSearchPass** (`benches_cycle_filter_rs_import_rpbot_pipeline_types_cyclesearchpass`, Module, degree: 1)
- **ruint::aliases::U256** (`benches_cycle_filter_rs_import_ruint_aliases_u256`, Module, degree: 1)
- **std::hint::black_box** (`benches_cycle_filter_rs_import_std_hint_black_box`, Module, degree: 1)
- **ring_fixture()** (`benches_cycle_filter_rs_ring_fixture`, Function, degree: 4)

## Relationships

- benches_cycle_filter_rs → benches_cycle_filter_rs_import_std_hint_black_box (imports)
- benches_cycle_filter_rs → benches_cycle_filter_rs_import_alloy_primitives_address (imports)
- benches_cycle_filter_rs → benches_cycle_filter_rs_import_rpbot_core_types_foundcycle_poolstate_protocoltype_v2poolstate (imports)
- benches_cycle_filter_rs → benches_cycle_filter_rs_import_rpbot_pipeline_arena_statearena (imports)
- benches_cycle_filter_rs → benches_cycle_filter_rs_import_rpbot_pipeline_cycle_filter_dedupe_cycles_by_fingerprint_prefilter_cycles_by_atomic_sim (imports)
- benches_cycle_filter_rs → benches_cycle_filter_rs_import_rpbot_pipeline_cycle_finder_find_cycles_multi_pass (imports)
- benches_cycle_filter_rs → benches_cycle_filter_rs_import_rpbot_pipeline_graph_build_graph_pool_meta_from_pair (imports)
- benches_cycle_filter_rs → benches_cycle_filter_rs_import_rpbot_pipeline_types_cyclesearchpass (imports)
- benches_cycle_filter_rs → benches_cycle_filter_rs_import_criterion_criterion_criterion_group_criterion_main (imports)
- benches_cycle_filter_rs → benches_cycle_filter_rs_import_ruint_aliases_u256 (imports)
- benches_cycle_filter_rs → benches_cycle_filter_rs_ring_fixture (defines)
- benches_cycle_filter_rs → benches_cycle_filter_rs_bench_prefilter_atomic_sim (defines)
- benches_cycle_filter_rs → benches_cycle_filter_rs_bench_dedupe_fingerprint (defines)
- benches_cycle_filter_rs → benches_cycle_filter_rs_bench_dedupe_high_duplicates (defines)
- benches_cycle_filter_rs_bench_prefilter_atomic_sim → benches_cycle_filter_rs_ring_fixture (calls)
- benches_cycle_filter_rs_bench_dedupe_fingerprint → benches_cycle_filter_rs_ring_fixture (calls)
- benches_cycle_filter_rs_bench_dedupe_high_duplicates → benches_cycle_filter_rs_ring_fixture (calls)

