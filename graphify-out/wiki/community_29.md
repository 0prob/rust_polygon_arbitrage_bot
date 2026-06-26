# Community 29: triangle_fixture()

**Members:** 18

## Nodes

- **cycle_finder** (`benches_cycle_finder_rs`, File, degree: 17)
- **bench_bellman_ford_triangle()** (`benches_cycle_finder_rs_bench_bellman_ford_triangle`, Function, degree: 2)
- **bench_dfs_dense_ring()** (`benches_cycle_finder_rs_bench_dfs_dense_ring`, Function, degree: 2)
- **bench_johnson_triangle()** (`benches_cycle_finder_rs_bench_johnson_triangle`, Function, degree: 2)
- **bench_parallel_dfs_hub()** (`benches_cycle_finder_rs_bench_parallel_dfs_hub`, Function, degree: 1)
- **dense_ring_fixture()** (`benches_cycle_finder_rs_dense_ring_fixture`, Function, degree: 2)
- **alloy::primitives::Address** (`benches_cycle_finder_rs_import_alloy_primitives_address`, Module, degree: 1)
- **criterion::{Criterion, criterion_group, criterion_main}** (`benches_cycle_finder_rs_import_criterion_criterion_criterion_group_criterion_main`, Module, degree: 1)
- **rpbot::core::types::{PoolState, ProtocolType, V2PoolState}** (`benches_cycle_finder_rs_import_rpbot_core_types_poolstate_protocoltype_v2poolstate`, Module, degree: 1)
- **rpbot::pipeline::arena::StateArena** (`benches_cycle_finder_rs_import_rpbot_pipeline_arena_statearena`, Module, degree: 1)
- **rpbot::pipeline::bellman_ford::find_cycles_bellman_ford** (`benches_cycle_finder_rs_import_rpbot_pipeline_bellman_ford_find_cycles_bellman_ford`, Module, degree: 1)
- **rpbot::pipeline::cycle_finder::{find_cycles, find_cycles_multi_pass}** (`benches_cycle_finder_rs_import_rpbot_pipeline_cycle_finder_find_cycles_find_cycles_multi_pass`, Module, degree: 1)
- **rpbot::pipeline::graph::{build_graph, pool_meta_from_pair}** (`benches_cycle_finder_rs_import_rpbot_pipeline_graph_build_graph_pool_meta_from_pair`, Module, degree: 1)
- **rpbot::pipeline::johnson::find_cycles_johnson_multi_pass** (`benches_cycle_finder_rs_import_rpbot_pipeline_johnson_find_cycles_johnson_multi_pass`, Module, degree: 1)
- **rpbot::pipeline::types::{CycleSearchPass, RoutingGraph}** (`benches_cycle_finder_rs_import_rpbot_pipeline_types_cyclesearchpass_routinggraph`, Module, degree: 1)
- **ruint::aliases::U256** (`benches_cycle_finder_rs_import_ruint_aliases_u256`, Module, degree: 1)
- **std::hint::black_box** (`benches_cycle_finder_rs_import_std_hint_black_box`, Module, degree: 1)
- **triangle_fixture()** (`benches_cycle_finder_rs_triangle_fixture`, Function, degree: 3)

## Relationships

- benches_cycle_finder_rs → benches_cycle_finder_rs_import_std_hint_black_box (imports)
- benches_cycle_finder_rs → benches_cycle_finder_rs_import_alloy_primitives_address (imports)
- benches_cycle_finder_rs → benches_cycle_finder_rs_import_rpbot_core_types_poolstate_protocoltype_v2poolstate (imports)
- benches_cycle_finder_rs → benches_cycle_finder_rs_import_rpbot_pipeline_arena_statearena (imports)
- benches_cycle_finder_rs → benches_cycle_finder_rs_import_rpbot_pipeline_bellman_ford_find_cycles_bellman_ford (imports)
- benches_cycle_finder_rs → benches_cycle_finder_rs_import_rpbot_pipeline_cycle_finder_find_cycles_find_cycles_multi_pass (imports)
- benches_cycle_finder_rs → benches_cycle_finder_rs_import_rpbot_pipeline_graph_build_graph_pool_meta_from_pair (imports)
- benches_cycle_finder_rs → benches_cycle_finder_rs_import_rpbot_pipeline_johnson_find_cycles_johnson_multi_pass (imports)
- benches_cycle_finder_rs → benches_cycle_finder_rs_import_rpbot_pipeline_types_cyclesearchpass_routinggraph (imports)
- benches_cycle_finder_rs → benches_cycle_finder_rs_import_criterion_criterion_criterion_group_criterion_main (imports)
- benches_cycle_finder_rs → benches_cycle_finder_rs_import_ruint_aliases_u256 (imports)
- benches_cycle_finder_rs → benches_cycle_finder_rs_triangle_fixture (defines)
- benches_cycle_finder_rs → benches_cycle_finder_rs_bench_bellman_ford_triangle (defines)
- benches_cycle_finder_rs → benches_cycle_finder_rs_bench_johnson_triangle (defines)
- benches_cycle_finder_rs → benches_cycle_finder_rs_bench_parallel_dfs_hub (defines)
- benches_cycle_finder_rs → benches_cycle_finder_rs_dense_ring_fixture (defines)
- benches_cycle_finder_rs → benches_cycle_finder_rs_bench_dfs_dense_ring (defines)
- benches_cycle_finder_rs_bench_bellman_ford_triangle → benches_cycle_finder_rs_triangle_fixture (calls)
- benches_cycle_finder_rs_bench_johnson_triangle → benches_cycle_finder_rs_triangle_fixture (calls)
- benches_cycle_finder_rs_bench_dfs_dense_ring → benches_cycle_finder_rs_dense_ring_fixture (calls)

