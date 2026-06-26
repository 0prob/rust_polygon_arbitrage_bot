# Community 36: hybrid_finds_triangle()

**Members:** 17

## Nodes

- **cycle_search** (`src_pipeline_cycle_search_rs`, File, degree: 16)
- **find_cycles_hybrid_multi_pass()** (`src_pipeline_cycle_search_rs_find_cycles_hybrid_multi_pass`, Function, degree: 2)
- **hybrid_finds_triangle()** (`src_pipeline_cycle_search_rs_hybrid_finds_triangle`, Function, degree: 2)
- **alloy::primitives::Address** (`src_pipeline_cycle_search_rs_import_alloy_primitives_address`, Module, degree: 1)
- **crate::core::types::FoundCycle** (`src_pipeline_cycle_search_rs_import_crate_core_types_foundcycle`, Module, degree: 1)
- **crate::core::types::{PoolState, ProtocolType, V2PoolState}** (`src_pipeline_cycle_search_rs_import_crate_core_types_poolstate_protocoltype_v2poolstate`, Module, degree: 1)
- **crate::pipeline::arena::StateArena** (`src_pipeline_cycle_search_rs_import_crate_pipeline_arena_statearena`, Module, degree: 1)
- **crate::pipeline::bellman_ford::find_cycles_bellman_ford_multi_pass_with_adj** (`src_pipeline_cycle_search_rs_import_crate_pipeline_bellman_ford_find_cycles_bellman_ford_multi_pass_with_adj`, Module, degree: 1)
- **crate::pipeline::cycle_filter::{dedupe_cycles_by_fingerprint, prefilter_cycles_by_atomic_sim}** (`src_pipeline_cycle_search_rs_import_crate_pipeline_cycle_filter_dedupe_cycles_by_fingerprint_prefilter_cycles_by_atomic_sim`, Module, degree: 1)
- **crate::pipeline::cycle_finder::find_cycles_multi_pass** (`src_pipeline_cycle_search_rs_import_crate_pipeline_cycle_finder_find_cycles_multi_pass`, Module, degree: 1)
- **crate::pipeline::graph::{build_graph, pool_meta_from_pair}** (`src_pipeline_cycle_search_rs_import_crate_pipeline_graph_build_graph_pool_meta_from_pair`, Module, degree: 1)
- **crate::pipeline::johnson::find_cycles_johnson_multi_pass_with_adj** (`src_pipeline_cycle_search_rs_import_crate_pipeline_johnson_find_cycles_johnson_multi_pass_with_adj`, Module, degree: 1)
- **crate::pipeline::types::{CycleSearchPass, RoutingGraph}** (`src_pipeline_cycle_search_rs_import_crate_pipeline_types_cyclesearchpass_routinggraph`, Module, degree: 1)
- **crate::pipeline::weighted_graph::{
    build_weighted_adjacency, compute_bf_potentials, reweight_adjacency,
}** (`src_pipeline_cycle_search_rs_import_crate_pipeline_weighted_graph_build_weighted_adjacency_compute_bf_potentials_reweight_adjacency`, Module, degree: 1)
- **rayon::join** (`src_pipeline_cycle_search_rs_import_rayon_join`, Module, degree: 1)
- **ruint::aliases::U256** (`src_pipeline_cycle_search_rs_import_ruint_aliases_u256`, Module, degree: 1)
- **super::*** (`src_pipeline_cycle_search_rs_import_super`, Module, degree: 1)

## Relationships

- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_import_rayon_join (imports)
- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_import_crate_core_types_foundcycle (imports)
- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_import_crate_pipeline_arena_statearena (imports)
- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_import_crate_pipeline_bellman_ford_find_cycles_bellman_ford_multi_pass_with_adj (imports)
- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_import_crate_pipeline_cycle_filter_dedupe_cycles_by_fingerprint_prefilter_cycles_by_atomic_sim (imports)
- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_import_crate_pipeline_cycle_finder_find_cycles_multi_pass (imports)
- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_import_crate_pipeline_johnson_find_cycles_johnson_multi_pass_with_adj (imports)
- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_import_crate_pipeline_types_cyclesearchpass_routinggraph (imports)
- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_import_crate_pipeline_weighted_graph_build_weighted_adjacency_compute_bf_potentials_reweight_adjacency (imports)
- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_find_cycles_hybrid_multi_pass (defines)
- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_import_super (imports)
- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_import_crate_core_types_poolstate_protocoltype_v2poolstate (imports)
- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_import_crate_pipeline_graph_build_graph_pool_meta_from_pair (imports)
- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_import_alloy_primitives_address (imports)
- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_import_ruint_aliases_u256 (imports)
- src_pipeline_cycle_search_rs → src_pipeline_cycle_search_rs_hybrid_finds_triangle (defines)
- src_pipeline_cycle_search_rs_hybrid_finds_triangle → src_pipeline_cycle_search_rs_find_cycles_hybrid_multi_pass (calls)

