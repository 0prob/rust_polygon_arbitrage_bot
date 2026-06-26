# Community 83: liquidity_risk_score()

**Members:** 12

## Nodes

- **route_viz** (`src_tui_route_viz_rs`, File, degree: 18)
- **cycle_has_long_tail()** (`src_tui_route_viz_rs_cycle_has_long_tail`, Function, degree: 3)
- **format_score_delta()** (`src_tui_route_viz_rs_format_score_delta`, Function, degree: 1)
- **alloy::primitives::Address** (`src_tui_route_viz_rs_import_alloy_primitives_address`, Module, degree: 1)
- **crate::core::types::{Edge, FoundCycle, TokenIndex}** (`src_tui_route_viz_rs_import_crate_core_types_edge_foundcycle_tokenindex`, Module, degree: 1)
- **crate::pipeline::arena::StateArena** (`src_tui_route_viz_rs_import_crate_pipeline_arena_statearena`, Module, degree: 1)
- **crate::pipeline::bellman_ford::route_call_count** (`src_tui_route_viz_rs_import_crate_pipeline_bellman_ford_route_call_count`, Module, degree: 1)
- **crate::pipeline::types::{PoolMeta, route_fingerprint}** (`src_tui_route_viz_rs_import_crate_pipeline_types_poolmeta_route_fingerprint`, Module, degree: 1)
- **crate::tui::update::{UiOpportunity, protocol_short_label}** (`src_tui_route_viz_rs_import_crate_tui_update_uiopportunity_protocol_short_label`, Module, degree: 1)
- **std::collections::HashSet** (`src_tui_route_viz_rs_import_std_collections_hashset`, Module, degree: 1)
- **is_major_token()** (`src_tui_route_viz_rs_is_major_token`, Function, degree: 2)
- **liquidity_risk_score()** (`src_tui_route_viz_rs_liquidity_risk_score`, Function, degree: 2)

## Relationships

- src_tui_route_viz_rs → src_tui_route_viz_rs_import_std_collections_hashset (imports)
- src_tui_route_viz_rs → src_tui_route_viz_rs_import_alloy_primitives_address (imports)
- src_tui_route_viz_rs → src_tui_route_viz_rs_import_crate_core_types_edge_foundcycle_tokenindex (imports)
- src_tui_route_viz_rs → src_tui_route_viz_rs_import_crate_pipeline_arena_statearena (imports)
- src_tui_route_viz_rs → src_tui_route_viz_rs_import_crate_pipeline_bellman_ford_route_call_count (imports)
- src_tui_route_viz_rs → src_tui_route_viz_rs_import_crate_pipeline_types_poolmeta_route_fingerprint (imports)
- src_tui_route_viz_rs → src_tui_route_viz_rs_import_crate_tui_update_uiopportunity_protocol_short_label (imports)
- src_tui_route_viz_rs → src_tui_route_viz_rs_is_major_token (defines)
- src_tui_route_viz_rs → src_tui_route_viz_rs_cycle_has_long_tail (defines)
- src_tui_route_viz_rs → src_tui_route_viz_rs_liquidity_risk_score (defines)
- src_tui_route_viz_rs → src_tui_route_viz_rs_format_score_delta (defines)
- src_tui_route_viz_rs_cycle_has_long_tail → src_tui_route_viz_rs_is_major_token (calls)

