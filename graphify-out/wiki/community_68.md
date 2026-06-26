# Community 68: RoutingGraph

**Members:** 13

## Nodes

- **types** (`src_pipeline_types_rs`, File, degree: 10)
- **compare_cycle_score()** (`src_pipeline_types_rs_compare_cycle_score`, Function, degree: 1)
- **CycleSearchPass** (`src_pipeline_types_rs_cyclesearchpass`, Struct, degree: 1)
- **GraphEdge** (`src_pipeline_types_rs_graphedge`, Struct, degree: 1)
- **alloy::primitives::{Address, FixedBytes}** (`src_pipeline_types_rs_import_alloy_primitives_address_fixedbytes`, Module, degree: 1)
- **crate::core::types::{Edge, FoundCycle, PoolIndex, ProtocolType, TokenIndex}** (`src_pipeline_types_rs_import_crate_core_types_edge_foundcycle_poolindex_protocoltype_tokenindex`, Module, degree: 1)
- **MinimalSimResult** (`src_pipeline_types_rs_minimalsimresult`, Struct, degree: 1)
- **OptimizationResult** (`src_pipeline_types_rs_optimizationresult`, Struct, degree: 1)
- **PoolMeta** (`src_pipeline_types_rs_poolmeta`, Struct, degree: 1)
- **route_fingerprint()** (`src_pipeline_types_rs_route_fingerprint`, Function, degree: 2)
- **RoutingGraph** (`src_pipeline_types_rs_routinggraph`, Struct, degree: 3)
- **.add_edge()** (`src_pipeline_types_rs_routinggraph_add_edge`, Method, degree: 1)
- **.new()** (`src_pipeline_types_rs_routinggraph_new`, Method, degree: 2)

## Relationships

- src_pipeline_types_rs → src_pipeline_types_rs_import_alloy_primitives_address_fixedbytes (imports)
- src_pipeline_types_rs → src_pipeline_types_rs_import_crate_core_types_edge_foundcycle_poolindex_protocoltype_tokenindex (imports)
- src_pipeline_types_rs → src_pipeline_types_rs_graphedge (defines)
- src_pipeline_types_rs → src_pipeline_types_rs_poolmeta (defines)
- src_pipeline_types_rs → src_pipeline_types_rs_routinggraph (defines)
- src_pipeline_types_rs → src_pipeline_types_rs_cyclesearchpass (defines)
- src_pipeline_types_rs → src_pipeline_types_rs_minimalsimresult (defines)
- src_pipeline_types_rs → src_pipeline_types_rs_optimizationresult (defines)
- src_pipeline_types_rs_routinggraph → src_pipeline_types_rs_routinggraph_new (defines)
- src_pipeline_types_rs_routinggraph → src_pipeline_types_rs_routinggraph_add_edge (defines)
- src_pipeline_types_rs → src_pipeline_types_rs_route_fingerprint (defines)
- src_pipeline_types_rs → src_pipeline_types_rs_compare_cycle_score (defines)
- src_pipeline_types_rs_route_fingerprint → src_pipeline_types_rs_routinggraph_new (calls)

