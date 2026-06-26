# Community 103: route_call_count()

**Members:** 10

## Nodes

- **negative_cycle** (`src_pipeline_negative_cycle_rs`, File, degree: 9)
- **collect_negative_cycles_from_source()** (`src_pipeline_negative_cycle_rs_collect_negative_cycles_from_source`, Function, degree: 3)
- **crate::core::types::{CycleEdges, Edge, FoundCycle, TokenIndex}** (`src_pipeline_negative_cycle_rs_import_crate_core_types_cycleedges_edge_foundcycle_tokenindex`, Module, degree: 1)
- **crate::pipeline::cycle_finder::clamp_fee_bps** (`src_pipeline_negative_cycle_rs_import_crate_pipeline_cycle_finder_clamp_fee_bps`, Module, degree: 1)
- **crate::pipeline::spot_price::hop_penalty** (`src_pipeline_negative_cycle_rs_import_crate_pipeline_spot_price_hop_penalty`, Module, degree: 1)
- **crate::pipeline::types::route_fingerprint** (`src_pipeline_negative_cycle_rs_import_crate_pipeline_types_route_fingerprint`, Module, degree: 1)
- **crate::pipeline::weighted_graph::WeightedEdge** (`src_pipeline_negative_cycle_rs_import_crate_pipeline_weighted_graph_weightededge`, Module, degree: 1)
- **smallvec::SmallVec** (`src_pipeline_negative_cycle_rs_import_smallvec_smallvec`, Module, degree: 1)
- **is_simple_cycle()** (`src_pipeline_negative_cycle_rs_is_simple_cycle`, Function, degree: 2)
- **route_call_count()** (`src_pipeline_negative_cycle_rs_route_call_count`, Function, degree: 2)

## Relationships

- src_pipeline_negative_cycle_rs → src_pipeline_negative_cycle_rs_import_smallvec_smallvec (imports)
- src_pipeline_negative_cycle_rs → src_pipeline_negative_cycle_rs_import_crate_core_types_cycleedges_edge_foundcycle_tokenindex (imports)
- src_pipeline_negative_cycle_rs → src_pipeline_negative_cycle_rs_import_crate_pipeline_cycle_finder_clamp_fee_bps (imports)
- src_pipeline_negative_cycle_rs → src_pipeline_negative_cycle_rs_import_crate_pipeline_spot_price_hop_penalty (imports)
- src_pipeline_negative_cycle_rs → src_pipeline_negative_cycle_rs_import_crate_pipeline_types_route_fingerprint (imports)
- src_pipeline_negative_cycle_rs → src_pipeline_negative_cycle_rs_import_crate_pipeline_weighted_graph_weightededge (imports)
- src_pipeline_negative_cycle_rs → src_pipeline_negative_cycle_rs_route_call_count (defines)
- src_pipeline_negative_cycle_rs → src_pipeline_negative_cycle_rs_is_simple_cycle (defines)
- src_pipeline_negative_cycle_rs → src_pipeline_negative_cycle_rs_collect_negative_cycles_from_source (defines)
- src_pipeline_negative_cycle_rs_collect_negative_cycles_from_source → src_pipeline_negative_cycle_rs_is_simple_cycle (calls)
- src_pipeline_negative_cycle_rs_collect_negative_cycles_from_source → src_pipeline_negative_cycle_rs_route_call_count (calls)

