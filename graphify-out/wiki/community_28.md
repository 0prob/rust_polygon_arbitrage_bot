# Community 28: ring_fixture() (28)

**Members:** 18

## Nodes

- **hf_tick** (`benches_hf_tick_rs`, File, degree: 17)
- **bench_arena_hot_patch()** (`benches_hf_tick_rs_bench_arena_hot_patch`, Function, degree: 1)
- **bench_graph_fingerprint()** (`benches_hf_tick_rs_bench_graph_fingerprint`, Function, degree: 1)
- **bench_hf_rescore_eval()** (`benches_hf_tick_rs_bench_hf_rescore_eval`, Function, degree: 2)
- **alloy::primitives::Address** (`benches_hf_tick_rs_import_alloy_primitives_address`, Module, degree: 1)
- **criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main}** (`benches_hf_tick_rs_import_criterion_benchmarkid_criterion_throughput_criterion_group_criterion_main`, Module, degree: 1)
- **rpbot::core::types::{PoolState, ProtocolType, V2PoolState}** (`benches_hf_tick_rs_import_rpbot_core_types_poolstate_protocoltype_v2poolstate`, Module, degree: 1)
- **rpbot::orchestrator::hf_eval::{HfEvalInput, evaluate_cycles_parallel}** (`benches_hf_tick_rs_import_rpbot_orchestrator_hf_eval_hfevalinput_evaluate_cycles_parallel`, Module, degree: 1)
- **rpbot::pipeline::arena::StateArena** (`benches_hf_tick_rs_import_rpbot_pipeline_arena_statearena`, Module, degree: 1)
- **rpbot::pipeline::cycle_finder::find_cycles_multi_pass** (`benches_hf_tick_rs_import_rpbot_pipeline_cycle_finder_find_cycles_multi_pass`, Module, degree: 1)
- **rpbot::pipeline::graph::{build_graph, pool_meta_from_pair}** (`benches_hf_tick_rs_import_rpbot_pipeline_graph_build_graph_pool_meta_from_pair`, Module, degree: 1)
- **rpbot::pipeline::spot_price::{SpotTable, rescore_cycles_with_table_and_gas}** (`benches_hf_tick_rs_import_rpbot_pipeline_spot_price_spottable_rescore_cycles_with_table_and_gas`, Module, degree: 1)
- **rpbot::pipeline::types::{compare_cycle_score, CycleSearchPass}** (`benches_hf_tick_rs_import_rpbot_pipeline_types_compare_cycle_score_cyclesearchpass`, Module, degree: 1)
- **ruint::aliases::U256** (`benches_hf_tick_rs_import_ruint_aliases_u256`, Module, degree: 1)
- **rustc_hash::FxHashMap** (`benches_hf_tick_rs_import_rustc_hash_fxhashmap`, Module, degree: 1)
- **std::hint::black_box** (`benches_hf_tick_rs_import_std_hint_black_box`, Module, degree: 1)
- **std::sync::Arc** (`benches_hf_tick_rs_import_std_sync_arc`, Module, degree: 1)
- **ring_fixture()** (`benches_hf_tick_rs_ring_fixture`, Function, degree: 2)

## Relationships

- benches_hf_tick_rs → benches_hf_tick_rs_import_std_hint_black_box (imports)
- benches_hf_tick_rs → benches_hf_tick_rs_import_std_sync_arc (imports)
- benches_hf_tick_rs → benches_hf_tick_rs_import_alloy_primitives_address (imports)
- benches_hf_tick_rs → benches_hf_tick_rs_import_rpbot_core_types_poolstate_protocoltype_v2poolstate (imports)
- benches_hf_tick_rs → benches_hf_tick_rs_import_rpbot_orchestrator_hf_eval_hfevalinput_evaluate_cycles_parallel (imports)
- benches_hf_tick_rs → benches_hf_tick_rs_import_rpbot_pipeline_arena_statearena (imports)
- benches_hf_tick_rs → benches_hf_tick_rs_import_rpbot_pipeline_graph_build_graph_pool_meta_from_pair (imports)
- benches_hf_tick_rs → benches_hf_tick_rs_import_rpbot_pipeline_spot_price_spottable_rescore_cycles_with_table_and_gas (imports)
- benches_hf_tick_rs → benches_hf_tick_rs_import_rpbot_pipeline_types_compare_cycle_score_cyclesearchpass (imports)
- benches_hf_tick_rs → benches_hf_tick_rs_import_rpbot_pipeline_cycle_finder_find_cycles_multi_pass (imports)
- benches_hf_tick_rs → benches_hf_tick_rs_import_criterion_benchmarkid_criterion_throughput_criterion_group_criterion_main (imports)
- benches_hf_tick_rs → benches_hf_tick_rs_import_ruint_aliases_u256 (imports)
- benches_hf_tick_rs → benches_hf_tick_rs_import_rustc_hash_fxhashmap (imports)
- benches_hf_tick_rs → benches_hf_tick_rs_ring_fixture (defines)
- benches_hf_tick_rs → benches_hf_tick_rs_bench_hf_rescore_eval (defines)
- benches_hf_tick_rs → benches_hf_tick_rs_bench_arena_hot_patch (defines)
- benches_hf_tick_rs → benches_hf_tick_rs_bench_graph_fingerprint (defines)
- benches_hf_tick_rs_bench_hf_rescore_eval → benches_hf_tick_rs_ring_fixture (calls)

