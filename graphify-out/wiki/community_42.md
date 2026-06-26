# Community 42: v2_pool()

**Members:** 16

## Nodes

- **cycle_filter** (`src_pipeline_cycle_filter_rs`, File, degree: 15)
- **atomic_prefilter_keeps_mispriced_triangle()** (`src_pipeline_cycle_filter_rs_atomic_prefilter_keeps_mispriced_triangle`, Function, degree: 3)
- **dedupe_cycles_by_fingerprint()** (`src_pipeline_cycle_filter_rs_dedupe_cycles_by_fingerprint`, Function, degree: 2)
- **dedupe_keeps_best_score()** (`src_pipeline_cycle_filter_rs_dedupe_keeps_best_score`, Function, degree: 2)
- **alloy::primitives::Address** (`src_pipeline_cycle_filter_rs_import_alloy_primitives_address`, Module, degree: 1)
- **crate::core::types::{Edge, FoundCycle}** (`src_pipeline_cycle_filter_rs_import_crate_core_types_edge_foundcycle`, Module, degree: 1)
- **crate::core::types::{PoolIndex, PoolState, ProtocolType, TokenIndex, V2PoolState}** (`src_pipeline_cycle_filter_rs_import_crate_core_types_poolindex_poolstate_protocoltype_tokenindex_v2poolstate`, Module, degree: 1)
- **crate::pipeline::arena::StateArena** (`src_pipeline_cycle_filter_rs_import_crate_pipeline_arena_statearena`, Module, degree: 1)
- **crate::pipeline::local_sim::simulate_route_minimal** (`src_pipeline_cycle_filter_rs_import_crate_pipeline_local_sim_simulate_route_minimal`, Module, degree: 1)
- **crate::pipeline::spot_price::SPOT_PROBE** (`src_pipeline_cycle_filter_rs_import_crate_pipeline_spot_price_spot_probe`, Module, degree: 1)
- **crate::pipeline::types::{compare_cycle_score, route_fingerprint}** (`src_pipeline_cycle_filter_rs_import_crate_pipeline_types_compare_cycle_score_route_fingerprint`, Module, degree: 1)
- **ruint::aliases::U256** (`src_pipeline_cycle_filter_rs_import_ruint_aliases_u256`, Module, degree: 1)
- **super::*** (`src_pipeline_cycle_filter_rs_import_super`, Module, degree: 1)
- **is_fully_simulable_route()** (`src_pipeline_cycle_filter_rs_is_fully_simulable_route`, Function, degree: 1)
- **prefilter_cycles_by_atomic_sim()** (`src_pipeline_cycle_filter_rs_prefilter_cycles_by_atomic_sim`, Function, degree: 2)
- **v2_pool()** (`src_pipeline_cycle_filter_rs_v2_pool`, Function, degree: 2)

## Relationships

- src_pipeline_cycle_filter_rs → src_pipeline_cycle_filter_rs_import_crate_core_types_edge_foundcycle (imports)
- src_pipeline_cycle_filter_rs → src_pipeline_cycle_filter_rs_import_crate_pipeline_arena_statearena (imports)
- src_pipeline_cycle_filter_rs → src_pipeline_cycle_filter_rs_import_crate_pipeline_local_sim_simulate_route_minimal (imports)
- src_pipeline_cycle_filter_rs → src_pipeline_cycle_filter_rs_import_crate_pipeline_spot_price_spot_probe (imports)
- src_pipeline_cycle_filter_rs → src_pipeline_cycle_filter_rs_import_crate_pipeline_types_compare_cycle_score_route_fingerprint (imports)
- src_pipeline_cycle_filter_rs → src_pipeline_cycle_filter_rs_prefilter_cycles_by_atomic_sim (defines)
- src_pipeline_cycle_filter_rs → src_pipeline_cycle_filter_rs_dedupe_cycles_by_fingerprint (defines)
- src_pipeline_cycle_filter_rs → src_pipeline_cycle_filter_rs_is_fully_simulable_route (defines)
- src_pipeline_cycle_filter_rs → src_pipeline_cycle_filter_rs_import_super (imports)
- src_pipeline_cycle_filter_rs → src_pipeline_cycle_filter_rs_import_crate_core_types_poolindex_poolstate_protocoltype_tokenindex_v2poolstate (imports)
- src_pipeline_cycle_filter_rs → src_pipeline_cycle_filter_rs_import_alloy_primitives_address (imports)
- src_pipeline_cycle_filter_rs → src_pipeline_cycle_filter_rs_import_ruint_aliases_u256 (imports)
- src_pipeline_cycle_filter_rs → src_pipeline_cycle_filter_rs_v2_pool (defines)
- src_pipeline_cycle_filter_rs → src_pipeline_cycle_filter_rs_atomic_prefilter_keeps_mispriced_triangle (defines)
- src_pipeline_cycle_filter_rs → src_pipeline_cycle_filter_rs_dedupe_keeps_best_score (defines)
- src_pipeline_cycle_filter_rs_atomic_prefilter_keeps_mispriced_triangle → src_pipeline_cycle_filter_rs_v2_pool (calls)
- src_pipeline_cycle_filter_rs_atomic_prefilter_keeps_mispriced_triangle → src_pipeline_cycle_filter_rs_prefilter_cycles_by_atomic_sim (calls)
- src_pipeline_cycle_filter_rs_dedupe_keeps_best_score → src_pipeline_cycle_filter_rs_dedupe_cycles_by_fingerprint (calls)

