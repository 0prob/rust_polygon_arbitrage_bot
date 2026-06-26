# Community 108: mod (108)

**Members:** 10

## Nodes

- **mod** (`src_services_execution_mod_rs`, File, degree: 9)
- **pub use candidate::{
    CandidateBuildConfig, CandidateExecution, build_execution_candidate, evaluated_from_sim,
}** (`src_services_execution_mod_rs_import_pub_use_candidate_candidatebuildconfig_candidateexecution_build_execution_candidate_evaluated_from_sim`, Module, degree: 1)
- **pub use circuit_breaker::CircuitBreaker** (`src_services_execution_mod_rs_import_pub_use_circuit_breaker_circuitbreaker`, Module, degree: 1)
- **pub use flash_liquidity::{
    FlashLiquidityCache, PreparedDispatch, PrepareDispatchInput, collect_flash_tokens,
    prepare_evaluated_route,
}** (`src_services_execution_mod_rs_import_pub_use_flash_liquidity_flashliquiditycache_prepareddispatch_preparedispatchinput_collect_flash_tokens_prepare_evaluated_route`, Module, degree: 1)
- **pub use flash_policy::{
    FlashLoanPolicy, hf_eval_flash_source, parse_flash_policy, parse_flash_source,
}** (`src_services_execution_mod_rs_import_pub_use_flash_policy_flashloanpolicy_hf_eval_flash_source_parse_flash_policy_parse_flash_source`, Module, degree: 1)
- **pub use gas::{FeeSnapshot, compute_conservative_gas_price, conservative_gas_price_wei}** (`src_services_execution_mod_rs_import_pub_use_gas_feesnapshot_compute_conservative_gas_price_conservative_gas_price_wei`, Module, degree: 1)
- **pub use gas_oracle::GasOracle** (`src_services_execution_mod_rs_import_pub_use_gas_oracle_gasoracle`, Module, degree: 1)
- **pub use opportunity_log::{OpportunityRecord, log_opportunity_evaluated, log_opportunity_outcome}** (`src_services_execution_mod_rs_import_pub_use_opportunity_log_opportunityrecord_log_opportunity_evaluated_log_opportunity_outcome`, Module, degree: 1)
- **pub use profit::{
    AssessProfitInput, ProfitEvalContext, ProfitThresholds, RouteProfitParams,
    assess_profit, build_assess_input, on_chain_min_profit_for_route,
    DEFAULT_PROFIT_SAFETY_MULTIPLIER_BPS,
}** (`src_services_execution_mod_rs_import_pub_use_profit_assessprofitinput_profitevalcontext_profitthresholds_routeprofitparams_assess_profit_build_assess_input_on_chain_min_profit_for_route_default_profit_safety_multiplier_bps`, Module, degree: 1)
- **pub use service::{ExecutionOutcome, ExecutionService}** (`src_services_execution_mod_rs_import_pub_use_service_executionoutcome_executionservice`, Module, degree: 1)

## Relationships

- src_services_execution_mod_rs → src_services_execution_mod_rs_import_pub_use_circuit_breaker_circuitbreaker (imports)
- src_services_execution_mod_rs → src_services_execution_mod_rs_import_pub_use_flash_liquidity_flashliquiditycache_prepareddispatch_preparedispatchinput_collect_flash_tokens_prepare_evaluated_route (imports)
- src_services_execution_mod_rs → src_services_execution_mod_rs_import_pub_use_flash_policy_flashloanpolicy_hf_eval_flash_source_parse_flash_policy_parse_flash_source (imports)
- src_services_execution_mod_rs → src_services_execution_mod_rs_import_pub_use_candidate_candidatebuildconfig_candidateexecution_build_execution_candidate_evaluated_from_sim (imports)
- src_services_execution_mod_rs → src_services_execution_mod_rs_import_pub_use_gas_feesnapshot_compute_conservative_gas_price_conservative_gas_price_wei (imports)
- src_services_execution_mod_rs → src_services_execution_mod_rs_import_pub_use_gas_oracle_gasoracle (imports)
- src_services_execution_mod_rs → src_services_execution_mod_rs_import_pub_use_opportunity_log_opportunityrecord_log_opportunity_evaluated_log_opportunity_outcome (imports)
- src_services_execution_mod_rs → src_services_execution_mod_rs_import_pub_use_profit_assessprofitinput_profitevalcontext_profitthresholds_routeprofitparams_assess_profit_build_assess_input_on_chain_min_profit_for_route_default_profit_safety_multiplier_bps (imports)
- src_services_execution_mod_rs → src_services_execution_mod_rs_import_pub_use_service_executionoutcome_executionservice (imports)

