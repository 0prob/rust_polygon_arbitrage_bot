# Community 32: fetch_woofi_pool()

**Members:** 17

## Nodes

- **mod** (`src_pipeline_pool_fetch_mod_rs`, File, degree: 16)
- **execute_plan_batch()** (`src_pipeline_pool_fetch_mod_rs_execute_plan_batch`, Function, degree: 2)
- **fetch_pools_batched()** (`src_pipeline_pool_fetch_mod_rs_fetch_pools_batched`, Function, degree: 3)
- **fetch_woofi_pool()** (`src_pipeline_pool_fetch_mod_rs_fetch_woofi_pool`, Function, degree: 2)
- **alloy::network::Ethereum** (`src_pipeline_pool_fetch_mod_rs_import_alloy_network_ethereum`, Module, degree: 1)
- **alloy::primitives::U256** (`src_pipeline_pool_fetch_mod_rs_import_alloy_primitives_u256`, Module, degree: 1)
- **alloy::providers::Provider** (`src_pipeline_pool_fetch_mod_rs_import_alloy_providers_provider`, Module, degree: 1)
- **crate::abis::{IWoofiPool, IWooracle}** (`src_pipeline_pool_fetch_mod_rs_import_crate_abis_iwoofipool_iwooracle`, Module, degree: 1)
- **crate::core::types::{PoolState, ProtocolType, WoofiBaseTokenState, WoofiPoolState}** (`src_pipeline_pool_fetch_mod_rs_import_crate_core_types_poolstate_protocoltype_woofibasetokenstate_woofipoolstate`, Module, degree: 1)
- **crate::pipeline::multicall::execute_multicall** (`src_pipeline_pool_fetch_mod_rs_import_crate_pipeline_multicall_execute_multicall`, Module, degree: 1)
- **crate::services::discovery::DiscoveredPool** (`src_pipeline_pool_fetch_mod_rs_import_crate_services_discovery_discoveredpool`, Module, degree: 1)
- **crate::services::state_cache::StateCache** (`src_pipeline_pool_fetch_mod_rs_import_crate_services_state_cache_statecache`, Module, degree: 1)
- **decode::decode_plan** (`src_pipeline_pool_fetch_mod_rs_import_decode_decode_plan`, Module, degree: 1)
- **plans::{PoolFetchPlan, build_plan}** (`src_pipeline_pool_fetch_mod_rs_import_plans_poolfetchplan_build_plan`, Module, degree: 1)
- **pub use decode::{decode_v2_reserves, decode_v3_slot0}** (`src_pipeline_pool_fetch_mod_rs_import_pub_use_decode_decode_v2_reserves_decode_v3_slot0`, Module, degree: 1)
- **std::sync::Arc** (`src_pipeline_pool_fetch_mod_rs_import_std_sync_arc`, Module, degree: 1)
- **tracing::debug** (`src_pipeline_pool_fetch_mod_rs_import_tracing_debug`, Module, degree: 1)

## Relationships

- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_import_std_sync_arc (imports)
- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_import_alloy_network_ethereum (imports)
- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_import_alloy_primitives_u256 (imports)
- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_import_alloy_providers_provider (imports)
- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_import_tracing_debug (imports)
- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_import_crate_abis_iwoofipool_iwooracle (imports)
- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_import_crate_core_types_poolstate_protocoltype_woofibasetokenstate_woofipoolstate (imports)
- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_import_crate_pipeline_multicall_execute_multicall (imports)
- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_import_crate_services_discovery_discoveredpool (imports)
- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_import_crate_services_state_cache_statecache (imports)
- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_import_pub_use_decode_decode_v2_reserves_decode_v3_slot0 (imports)
- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_import_decode_decode_plan (imports)
- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_import_plans_poolfetchplan_build_plan (imports)
- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_fetch_woofi_pool (defines)
- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_fetch_pools_batched (defines)
- src_pipeline_pool_fetch_mod_rs → src_pipeline_pool_fetch_mod_rs_execute_plan_batch (defines)
- src_pipeline_pool_fetch_mod_rs_fetch_pools_batched → src_pipeline_pool_fetch_mod_rs_execute_plan_batch (calls)
- src_pipeline_pool_fetch_mod_rs_fetch_pools_batched → src_pipeline_pool_fetch_mod_rs_fetch_woofi_pool (calls)

