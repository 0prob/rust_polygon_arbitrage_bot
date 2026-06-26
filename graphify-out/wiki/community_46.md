# Community 46: bench_gas_estimate()

**Members:** 16

## Nodes

- **scaling** (`benches_scaling_rs`, File, degree: 22)
- **bench_gas_estimate()** (`benches_scaling_rs_bench_gas_estimate`, Function, degree: 1)
- **alloy::primitives::Address** (`benches_scaling_rs_import_alloy_primitives_address`, Module, degree: 1)
- **criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main}** (`benches_scaling_rs_import_criterion_benchmarkid_criterion_throughput_criterion_group_criterion_main`, Module, degree: 1)
- **rpbot::core::types::{PoolState, ProtocolType, V2PoolState}** (`benches_scaling_rs_import_rpbot_core_types_poolstate_protocoltype_v2poolstate`, Module, degree: 1)
- **rpbot::pipeline::arena::StateArena** (`benches_scaling_rs_import_rpbot_pipeline_arena_statearena`, Module, degree: 1)
- **rpbot::pipeline::cycle_finder::find_cycles_multi_pass** (`benches_scaling_rs_import_rpbot_pipeline_cycle_finder_find_cycles_multi_pass`, Module, degree: 1)
- **rpbot::pipeline::graph::{build_graph, pool_meta_from_pair, rescore_graph_in_place}** (`benches_scaling_rs_import_rpbot_pipeline_graph_build_graph_pool_meta_from_pair_rescore_graph_in_place`, Module, degree: 1)
- **rpbot::pipeline::graph_cache::connectivity_fingerprint** (`benches_scaling_rs_import_rpbot_pipeline_graph_cache_connectivity_fingerprint`, Module, degree: 1)
- **rpbot::pipeline::local_sim::simulate_route_minimal** (`benches_scaling_rs_import_rpbot_pipeline_local_sim_simulate_route_minimal`, Module, degree: 1)
- **rpbot::pipeline::spot_price::rescore_cycles_by_spot_price** (`benches_scaling_rs_import_rpbot_pipeline_spot_price_rescore_cycles_by_spot_price`, Module, degree: 1)
- **rpbot::pipeline::types::CycleSearchPass** (`benches_scaling_rs_import_rpbot_pipeline_types_cyclesearchpass`, Module, degree: 1)
- **rpbot::services::execution::gas::estimate_route_gas_from_hops** (`benches_scaling_rs_import_rpbot_services_execution_gas_estimate_route_gas_from_hops`, Module, degree: 1)
- **ruint::aliases::U256** (`benches_scaling_rs_import_ruint_aliases_u256`, Module, degree: 1)
- **std::hint::black_box** (`benches_scaling_rs_import_std_hint_black_box`, Module, degree: 1)
- **std::sync::Arc** (`benches_scaling_rs_import_std_sync_arc`, Module, degree: 1)

## Relationships

- benches_scaling_rs → benches_scaling_rs_import_std_hint_black_box (imports)
- benches_scaling_rs → benches_scaling_rs_import_std_sync_arc (imports)
- benches_scaling_rs → benches_scaling_rs_import_alloy_primitives_address (imports)
- benches_scaling_rs → benches_scaling_rs_import_criterion_benchmarkid_criterion_throughput_criterion_group_criterion_main (imports)
- benches_scaling_rs → benches_scaling_rs_import_rpbot_core_types_poolstate_protocoltype_v2poolstate (imports)
- benches_scaling_rs → benches_scaling_rs_import_rpbot_pipeline_arena_statearena (imports)
- benches_scaling_rs → benches_scaling_rs_import_rpbot_pipeline_cycle_finder_find_cycles_multi_pass (imports)
- benches_scaling_rs → benches_scaling_rs_import_rpbot_pipeline_graph_build_graph_pool_meta_from_pair_rescore_graph_in_place (imports)
- benches_scaling_rs → benches_scaling_rs_import_rpbot_pipeline_graph_cache_connectivity_fingerprint (imports)
- benches_scaling_rs → benches_scaling_rs_import_rpbot_pipeline_local_sim_simulate_route_minimal (imports)
- benches_scaling_rs → benches_scaling_rs_import_rpbot_pipeline_spot_price_rescore_cycles_by_spot_price (imports)
- benches_scaling_rs → benches_scaling_rs_import_rpbot_pipeline_types_cyclesearchpass (imports)
- benches_scaling_rs → benches_scaling_rs_import_rpbot_services_execution_gas_estimate_route_gas_from_hops (imports)
- benches_scaling_rs → benches_scaling_rs_import_ruint_aliases_u256 (imports)
- benches_scaling_rs → benches_scaling_rs_bench_gas_estimate (defines)

