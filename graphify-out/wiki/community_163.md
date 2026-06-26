# Community 163: enrich_v3_ticks()

**Members:** 6

## Nodes

- **tick_fetch** (`src_pipeline_tick_fetch_rs`, File, degree: 5)
- **collect_v3_pool_addresses()** (`src_pipeline_tick_fetch_rs_collect_v3_pool_addresses`, Function, degree: 1)
- **enrich_v3_ticks()** (`src_pipeline_tick_fetch_rs_enrich_v3_ticks`, Function, degree: 1)
- **alloy::primitives::Address** (`src_pipeline_tick_fetch_rs_import_alloy_primitives_address`, Module, degree: 1)
- **crate::core::types::{FoundCycle, ProtocolType}** (`src_pipeline_tick_fetch_rs_import_crate_core_types_foundcycle_protocoltype`, Module, degree: 1)
- **crate::pipeline::arena::StateArena** (`src_pipeline_tick_fetch_rs_import_crate_pipeline_arena_statearena`, Module, degree: 1)

## Relationships

- src_pipeline_tick_fetch_rs → src_pipeline_tick_fetch_rs_import_alloy_primitives_address (imports)
- src_pipeline_tick_fetch_rs → src_pipeline_tick_fetch_rs_import_crate_core_types_foundcycle_protocoltype (imports)
- src_pipeline_tick_fetch_rs → src_pipeline_tick_fetch_rs_import_crate_pipeline_arena_statearena (imports)
- src_pipeline_tick_fetch_rs → src_pipeline_tick_fetch_rs_collect_v3_pool_addresses (defines)
- src_pipeline_tick_fetch_rs → src_pipeline_tick_fetch_rs_enrich_v3_ticks (defines)

