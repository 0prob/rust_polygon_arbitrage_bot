# Community 37: johnson_finds_triangle()

**Members:** 16

## Nodes

- **johnson** (`src_pipeline_johnson_rs`, File, degree: 15)
- **find_cycles_johnson_multi_pass()** (`src_pipeline_johnson_rs_find_cycles_johnson_multi_pass`, Function, degree: 3)
- **find_cycles_johnson_multi_pass_with_adj()** (`src_pipeline_johnson_rs_find_cycles_johnson_multi_pass_with_adj`, Function, degree: 2)
- **alloy::primitives::Address** (`src_pipeline_johnson_rs_import_alloy_primitives_address`, Module, degree: 1)
- **crate::core::types::FoundCycle** (`src_pipeline_johnson_rs_import_crate_core_types_foundcycle`, Module, degree: 1)
- **crate::core::types::{PoolState, ProtocolType, V2PoolState}** (`src_pipeline_johnson_rs_import_crate_core_types_poolstate_protocoltype_v2poolstate`, Module, degree: 1)
- **crate::pipeline::arena::StateArena** (`src_pipeline_johnson_rs_import_crate_pipeline_arena_statearena`, Module, degree: 1)
- **crate::pipeline::deadline::DeadlineGuard** (`src_pipeline_johnson_rs_import_crate_pipeline_deadline_deadlineguard`, Module, degree: 1)
- **crate::pipeline::graph::{build_graph, pool_meta_from_pair}** (`src_pipeline_johnson_rs_import_crate_pipeline_graph_build_graph_pool_meta_from_pair`, Module, degree: 1)
- **crate::pipeline::negative_cycle::collect_negative_cycles_from_source** (`src_pipeline_johnson_rs_import_crate_pipeline_negative_cycle_collect_negative_cycles_from_source`, Module, degree: 1)
- **crate::pipeline::types::{CycleSearchPass, RoutingGraph}** (`src_pipeline_johnson_rs_import_crate_pipeline_types_cyclesearchpass_routinggraph`, Module, degree: 1)
- **crate::pipeline::weighted_graph::{
    WeightedEdge, build_weighted_adjacency, compute_bf_potentials, reweight_adjacency,
    select_hub_tokens,
}** (`src_pipeline_johnson_rs_import_crate_pipeline_weighted_graph_weightededge_build_weighted_adjacency_compute_bf_potentials_reweight_adjacency_select_hub_tokens`, Module, degree: 1)
- **ruint::aliases::U256** (`src_pipeline_johnson_rs_import_ruint_aliases_u256`, Module, degree: 1)
- **std::time::Duration** (`src_pipeline_johnson_rs_import_std_time_duration`, Module, degree: 1)
- **super::*** (`src_pipeline_johnson_rs_import_super`, Module, degree: 1)
- **johnson_finds_triangle()** (`src_pipeline_johnson_rs_johnson_finds_triangle`, Function, degree: 2)

## Relationships

- src_pipeline_johnson_rs → src_pipeline_johnson_rs_import_std_time_duration (imports)
- src_pipeline_johnson_rs → src_pipeline_johnson_rs_import_crate_core_types_foundcycle (imports)
- src_pipeline_johnson_rs → src_pipeline_johnson_rs_import_crate_pipeline_arena_statearena (imports)
- src_pipeline_johnson_rs → src_pipeline_johnson_rs_import_crate_pipeline_deadline_deadlineguard (imports)
- src_pipeline_johnson_rs → src_pipeline_johnson_rs_import_crate_pipeline_negative_cycle_collect_negative_cycles_from_source (imports)
- src_pipeline_johnson_rs → src_pipeline_johnson_rs_import_crate_pipeline_types_cyclesearchpass_routinggraph (imports)
- src_pipeline_johnson_rs → src_pipeline_johnson_rs_import_crate_pipeline_weighted_graph_weightededge_build_weighted_adjacency_compute_bf_potentials_reweight_adjacency_select_hub_tokens (imports)
- src_pipeline_johnson_rs → src_pipeline_johnson_rs_find_cycles_johnson_multi_pass_with_adj (defines)
- src_pipeline_johnson_rs → src_pipeline_johnson_rs_find_cycles_johnson_multi_pass (defines)
- src_pipeline_johnson_rs → src_pipeline_johnson_rs_import_super (imports)
- src_pipeline_johnson_rs → src_pipeline_johnson_rs_import_crate_core_types_poolstate_protocoltype_v2poolstate (imports)
- src_pipeline_johnson_rs → src_pipeline_johnson_rs_import_crate_pipeline_graph_build_graph_pool_meta_from_pair (imports)
- src_pipeline_johnson_rs → src_pipeline_johnson_rs_import_alloy_primitives_address (imports)
- src_pipeline_johnson_rs → src_pipeline_johnson_rs_import_ruint_aliases_u256 (imports)
- src_pipeline_johnson_rs → src_pipeline_johnson_rs_johnson_finds_triangle (defines)
- src_pipeline_johnson_rs_find_cycles_johnson_multi_pass → src_pipeline_johnson_rs_find_cycles_johnson_multi_pass_with_adj (calls)
- src_pipeline_johnson_rs_johnson_finds_triangle → src_pipeline_johnson_rs_find_cycles_johnson_multi_pass (calls)

