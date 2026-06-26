# Community 65: evaluated_from_sim()

**Members:** 13

## Nodes

- **candidate** (`src_services_execution_candidate_rs`, File, degree: 12)
- **build_execution_candidate()** (`src_services_execution_candidate_rs_build_execution_candidate`, Function, degree: 1)
- **CandidateBuildConfig** (`src_services_execution_candidate_rs_candidatebuildconfig`, Struct, degree: 1)
- **CandidateExecution** (`src_services_execution_candidate_rs_candidateexecution`, Struct, degree: 1)
- **evaluated_from_sim()** (`src_services_execution_candidate_rs_evaluated_from_sim`, Function, degree: 1)
- **alloy::primitives::{Address, Bytes, FixedBytes, U256}** (`src_services_execution_candidate_rs_import_alloy_primitives_address_bytes_fixedbytes_u256`, Module, degree: 1)
- **crate::core::types::{EvaluatedRoute, FlashLoanSource, RouteSimulationResult}** (`src_services_execution_candidate_rs_import_crate_core_types_evaluatedroute_flashloansource_routesimulationresult`, Module, degree: 1)
- **crate::pipeline::arena::StateArena** (`src_services_execution_candidate_rs_import_crate_pipeline_arena_statearena`, Module, degree: 1)
- **crate::pipeline::types::PoolMeta** (`src_services_execution_candidate_rs_import_crate_pipeline_types_poolmeta`, Module, degree: 1)
- **crate::services::execution::calldata::{
    RouteEncodeConfig, build_arb_calldata, build_calldata_hops, encode_route,
}** (`src_services_execution_candidate_rs_import_crate_services_execution_calldata_routeencodeconfig_build_arb_calldata_build_calldata_hops_encode_route`, Module, degree: 1)
- **crate::services::execution::gas::buffer_gas_limit** (`src_services_execution_candidate_rs_import_crate_services_execution_gas_buffer_gas_limit`, Module, degree: 1)
- **crate::services::execution::profit::{on_chain_min_profit_for_route, slippage_adjusted}** (`src_services_execution_candidate_rs_import_crate_services_execution_profit_on_chain_min_profit_for_route_slippage_adjusted`, Module, degree: 1)
- **tracing::instrument** (`src_services_execution_candidate_rs_import_tracing_instrument`, Module, degree: 1)

## Relationships

- src_services_execution_candidate_rs → src_services_execution_candidate_rs_import_alloy_primitives_address_bytes_fixedbytes_u256 (imports)
- src_services_execution_candidate_rs → src_services_execution_candidate_rs_import_tracing_instrument (imports)
- src_services_execution_candidate_rs → src_services_execution_candidate_rs_import_crate_core_types_evaluatedroute_flashloansource_routesimulationresult (imports)
- src_services_execution_candidate_rs → src_services_execution_candidate_rs_import_crate_pipeline_arena_statearena (imports)
- src_services_execution_candidate_rs → src_services_execution_candidate_rs_import_crate_pipeline_types_poolmeta (imports)
- src_services_execution_candidate_rs → src_services_execution_candidate_rs_import_crate_services_execution_calldata_routeencodeconfig_build_arb_calldata_build_calldata_hops_encode_route (imports)
- src_services_execution_candidate_rs → src_services_execution_candidate_rs_import_crate_services_execution_gas_buffer_gas_limit (imports)
- src_services_execution_candidate_rs → src_services_execution_candidate_rs_import_crate_services_execution_profit_on_chain_min_profit_for_route_slippage_adjusted (imports)
- src_services_execution_candidate_rs → src_services_execution_candidate_rs_candidateexecution (defines)
- src_services_execution_candidate_rs → src_services_execution_candidate_rs_candidatebuildconfig (defines)
- src_services_execution_candidate_rs → src_services_execution_candidate_rs_build_execution_candidate (defines)
- src_services_execution_candidate_rs → src_services_execution_candidate_rs_evaluated_from_sim (defines)

