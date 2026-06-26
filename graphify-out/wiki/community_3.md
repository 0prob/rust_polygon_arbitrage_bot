# Community 3: TokenFlashLiquidity

**Members:** 31

## Nodes

- **flash_liquidity** (`src_services_execution_flash_liquidity_rs`, File, degree: 43)
- **CachedLiquidity** (`src_services_execution_flash_liquidity_rs_cachedliquidity`, Struct, degree: 1)
- **FlashPlan** (`src_services_execution_flash_liquidity_rs_flashplan`, Struct, degree: 1)
- **FlashPlanAction** (`src_services_execution_flash_liquidity_rs_flashplanaction`, Enum, degree: 1)
- **alloy::network::Ethereum** (`src_services_execution_flash_liquidity_rs_import_alloy_network_ethereum`, Module, degree: 1)
- **alloy::primitives::Address** (`src_services_execution_flash_liquidity_rs_import_alloy_primitives_address`, Module, degree: 1)
- **alloy::providers::Provider** (`src_services_execution_flash_liquidity_rs_import_alloy_providers_provider`, Module, degree: 1)
- **alloy::sol_types::SolCall** (`src_services_execution_flash_liquidity_rs_import_alloy_sol_types_solcall`, Module, degree: 1)
- **crate::abis::{IAaveV3Pool, IERC20Metadata}** (`src_services_execution_flash_liquidity_rs_import_crate_abis_iaavev3pool_ierc20metadata`, Module, degree: 1)
- **crate::core::constants::{AAVE_V3_POOL, BALANCER_VAULT}** (`src_services_execution_flash_liquidity_rs_import_crate_core_constants_aave_v3_pool_balancer_vault`, Module, degree: 1)
- **crate::core::types::{
    EvaluatedRoute, FlashLoanSource, FoundCycle, PoolState, ProfitAssessment, ProtocolType,
    TokenIndex,
}** (`src_services_execution_flash_liquidity_rs_import_crate_core_types_evaluatedroute_flashloansource_foundcycle_poolstate_profitassessment_protocoltype_tokenindex`, Module, degree: 1)
- **crate::pipeline::arena::StateArena** (`src_services_execution_flash_liquidity_rs_import_crate_pipeline_arena_statearena`, Module, degree: 1)
- **crate::pipeline::local_sim::simulate_route_detailed** (`src_services_execution_flash_liquidity_rs_import_crate_pipeline_local_sim_simulate_route_detailed`, Module, degree: 1)
- **crate::pipeline::multicall::{MulticallItem, encode_call, execute_multicall}** (`src_services_execution_flash_liquidity_rs_import_crate_pipeline_multicall_multicallitem_encode_call_execute_multicall`, Module, degree: 1)
- **crate::pipeline::ternary::optimize_cycle** (`src_services_execution_flash_liquidity_rs_import_crate_pipeline_ternary_optimize_cycle`, Module, degree: 1)
- **crate::services::execution::flash_policy::FlashLoanPolicy** (`src_services_execution_flash_liquidity_rs_import_crate_services_execution_flash_policy_flashloanpolicy`, Module, degree: 1)
- **crate::services::execution::profit::{
    ProfitEvalContext, ProfitThresholds, RouteProfitParams, assess_profit, build_assess_input,
}** (`src_services_execution_flash_liquidity_rs_import_crate_services_execution_profit_profitevalcontext_profitthresholds_routeprofitparams_assess_profit_build_assess_input`, Module, degree: 1)
- **parking_lot::RwLock** (`src_services_execution_flash_liquidity_rs_import_parking_lot_rwlock`, Module, degree: 1)
- **ruint::aliases::U256 as RU256** (`src_services_execution_flash_liquidity_rs_import_ruint_aliases_u256_as_ru256`, Module, degree: 1)
- **rustc_hash::FxHashMap** (`src_services_execution_flash_liquidity_rs_import_rustc_hash_fxhashmap`, Module, degree: 1)
- **std::collections::HashMap** (`src_services_execution_flash_liquidity_rs_import_std_collections_hashmap`, Module, degree: 1)
- **std::time::{Duration, Instant}** (`src_services_execution_flash_liquidity_rs_import_std_time_duration_instant`, Module, degree: 1)
- **super::*** (`src_services_execution_flash_liquidity_rs_import_super`, Module, degree: 1)
- **tracing::debug** (`src_services_execution_flash_liquidity_rs_import_tracing_debug`, Module, degree: 1)
- **prepare_evaluated_route()** (`src_services_execution_flash_liquidity_rs_prepare_evaluated_route`, Function, degree: 5)
- **PreparedDispatch** (`src_services_execution_flash_liquidity_rs_prepareddispatch`, Struct, degree: 1)
- **PrepareDispatchInput** (`src_services_execution_flash_liquidity_rs_preparedispatchinput`, Struct, degree: 1)
- **reassess_route()** (`src_services_execution_flash_liquidity_rs_reassess_route`, Function, degree: 2)
- **reoptimize_capped()** (`src_services_execution_flash_liquidity_rs_reoptimize_capped`, Function, degree: 2)
- **route_balancer_flash_capacity()** (`src_services_execution_flash_liquidity_rs_route_balancer_flash_capacity`, Function, degree: 2)
- **TokenFlashLiquidity** (`src_services_execution_flash_liquidity_rs_tokenflashliquidity`, Struct, degree: 1)

## Relationships

- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_std_collections_hashmap (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_std_time_duration_instant (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_alloy_network_ethereum (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_alloy_primitives_address (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_alloy_providers_provider (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_alloy_sol_types_solcall (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_parking_lot_rwlock (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_ruint_aliases_u256_as_ru256 (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_rustc_hash_fxhashmap (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_tracing_debug (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_crate_abis_iaavev3pool_ierc20metadata (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_crate_core_constants_aave_v3_pool_balancer_vault (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_crate_core_types_evaluatedroute_flashloansource_foundcycle_poolstate_profitassessment_protocoltype_tokenindex (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_crate_pipeline_arena_statearena (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_crate_pipeline_local_sim_simulate_route_detailed (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_crate_pipeline_multicall_multicallitem_encode_call_execute_multicall (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_crate_pipeline_ternary_optimize_cycle (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_crate_services_execution_profit_profitevalcontext_profitthresholds_routeprofitparams_assess_profit_build_assess_input (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_crate_services_execution_flash_policy_flashloanpolicy (imports)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_flashplanaction (defines)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_flashplan (defines)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_tokenflashliquidity (defines)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_cachedliquidity (defines)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_route_balancer_flash_capacity (defines)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_preparedispatchinput (defines)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_prepareddispatch (defines)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_prepare_evaluated_route (defines)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_reoptimize_capped (defines)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_reassess_route (defines)
- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_import_super (imports)
- src_services_execution_flash_liquidity_rs_prepare_evaluated_route → src_services_execution_flash_liquidity_rs_route_balancer_flash_capacity (calls)
- src_services_execution_flash_liquidity_rs_prepare_evaluated_route → src_services_execution_flash_liquidity_rs_reassess_route (calls)
- src_services_execution_flash_liquidity_rs_prepare_evaluated_route → src_services_execution_flash_liquidity_rs_reoptimize_capped (calls)

