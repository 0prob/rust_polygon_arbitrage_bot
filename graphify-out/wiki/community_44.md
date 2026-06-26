# Community 44: dispatch_with_provider()

**Members:** 16

## Nodes

- **hf_execute** (`src_orchestrator_hf_execute_rs`, File, degree: 15)
- **dispatch_profitable_candidates()** (`src_orchestrator_hf_execute_rs_dispatch_profitable_candidates`, Function, degree: 2)
- **dispatch_with_provider()** (`src_orchestrator_hf_execute_rs_dispatch_with_provider`, Function, degree: 2)
- **alloy::network::Ethereum** (`src_orchestrator_hf_execute_rs_import_alloy_network_ethereum`, Module, degree: 1)
- **alloy::providers::Provider** (`src_orchestrator_hf_execute_rs_import_alloy_providers_provider`, Module, degree: 1)
- **crate::core::types::EvaluatedRoute** (`src_orchestrator_hf_execute_rs_import_crate_core_types_evaluatedroute`, Module, degree: 1)
- **crate::infra::tracing_util::{pool_addrs_csv, record_evaluated_route}** (`src_orchestrator_hf_execute_rs_import_crate_infra_tracing_util_pool_addrs_csv_record_evaluated_route`, Module, degree: 1)
- **crate::orchestrator::hf::HfContext** (`src_orchestrator_hf_execute_rs_import_crate_orchestrator_hf_hfcontext`, Module, degree: 1)
- **crate::pipeline::arena::StateArena** (`src_orchestrator_hf_execute_rs_import_crate_pipeline_arena_statearena`, Module, degree: 1)
- **crate::pipeline::types::route_fingerprint** (`src_orchestrator_hf_execute_rs_import_crate_pipeline_types_route_fingerprint`, Module, degree: 1)
- **crate::services::execution::{
    CandidateBuildConfig, ExecutionOutcome, PrepareDispatchInput, build_execution_candidate,
    collect_flash_tokens, prepare_evaluated_route,
}** (`src_orchestrator_hf_execute_rs_import_crate_services_execution_candidatebuildconfig_executionoutcome_preparedispatchinput_build_execution_candidate_collect_flash_tokens_prepare_evaluated_route`, Module, degree: 1)
- **crate::services::execution::flash_policy::parse_flash_policy** (`src_orchestrator_hf_execute_rs_import_crate_services_execution_flash_policy_parse_flash_policy`, Module, degree: 1)
- **crate::services::execution::impact_slippage::depth_impact_slippage_bps** (`src_orchestrator_hf_execute_rs_import_crate_services_execution_impact_slippage_depth_impact_slippage_bps`, Module, degree: 1)
- **crate::services::oracle::price_oracle::bootstrap_matic_rate_per_unit** (`src_orchestrator_hf_execute_rs_import_crate_services_oracle_price_oracle_bootstrap_matic_rate_per_unit`, Module, degree: 1)
- **ruint::aliases::U256 as RU256** (`src_orchestrator_hf_execute_rs_import_ruint_aliases_u256_as_ru256`, Module, degree: 1)
- **tracing::{Instrument, debug, info, info_span, instrument, warn}** (`src_orchestrator_hf_execute_rs_import_tracing_instrument_debug_info_info_span_instrument_warn`, Module, degree: 1)

## Relationships

- src_orchestrator_hf_execute_rs → src_orchestrator_hf_execute_rs_import_alloy_network_ethereum (imports)
- src_orchestrator_hf_execute_rs → src_orchestrator_hf_execute_rs_import_alloy_providers_provider (imports)
- src_orchestrator_hf_execute_rs → src_orchestrator_hf_execute_rs_import_ruint_aliases_u256_as_ru256 (imports)
- src_orchestrator_hf_execute_rs → src_orchestrator_hf_execute_rs_import_tracing_instrument_debug_info_info_span_instrument_warn (imports)
- src_orchestrator_hf_execute_rs → src_orchestrator_hf_execute_rs_import_crate_core_types_evaluatedroute (imports)
- src_orchestrator_hf_execute_rs → src_orchestrator_hf_execute_rs_import_crate_infra_tracing_util_pool_addrs_csv_record_evaluated_route (imports)
- src_orchestrator_hf_execute_rs → src_orchestrator_hf_execute_rs_import_crate_orchestrator_hf_hfcontext (imports)
- src_orchestrator_hf_execute_rs → src_orchestrator_hf_execute_rs_import_crate_pipeline_arena_statearena (imports)
- src_orchestrator_hf_execute_rs → src_orchestrator_hf_execute_rs_import_crate_pipeline_types_route_fingerprint (imports)
- src_orchestrator_hf_execute_rs → src_orchestrator_hf_execute_rs_import_crate_services_execution_candidatebuildconfig_executionoutcome_preparedispatchinput_build_execution_candidate_collect_flash_tokens_prepare_evaluated_route (imports)
- src_orchestrator_hf_execute_rs → src_orchestrator_hf_execute_rs_import_crate_services_oracle_price_oracle_bootstrap_matic_rate_per_unit (imports)
- src_orchestrator_hf_execute_rs → src_orchestrator_hf_execute_rs_import_crate_services_execution_flash_policy_parse_flash_policy (imports)
- src_orchestrator_hf_execute_rs → src_orchestrator_hf_execute_rs_import_crate_services_execution_impact_slippage_depth_impact_slippage_bps (imports)
- src_orchestrator_hf_execute_rs → src_orchestrator_hf_execute_rs_dispatch_profitable_candidates (defines)
- src_orchestrator_hf_execute_rs → src_orchestrator_hf_execute_rs_dispatch_with_provider (defines)
- src_orchestrator_hf_execute_rs_dispatch_profitable_candidates → src_orchestrator_hf_execute_rs_dispatch_with_provider (calls)

