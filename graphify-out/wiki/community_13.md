# Community 13: HfEvalResult

**Members:** 23

## Nodes

- **hf_eval** (`src_orchestrator_hf_eval_rs`, File, degree: 21)
- **best_assessment_for_cycle()** (`src_orchestrator_hf_eval_rs_best_assessment_for_cycle`, Function, degree: 2)
- **build_hf_eval_pool()** (`src_orchestrator_hf_eval_rs_build_hf_eval_pool`, Function, degree: 1)
- **evaluate_cycles_parallel()** (`src_orchestrator_hf_eval_rs_evaluate_cycles_parallel`, Function, degree: 3)
- **evaluate_cycles_parallel_async()** (`src_orchestrator_hf_eval_rs_evaluate_cycles_parallel_async`, Function, degree: 2)
- **evaluate_one()** (`src_orchestrator_hf_eval_rs_evaluate_one`, Function, degree: 3)
- **HfEvalInput** (`src_orchestrator_hf_eval_rs_hfevalinput`, Struct, degree: 1)
- **HfEvalInputOwned** (`src_orchestrator_hf_eval_rs_hfevalinputowned`, Struct, degree: 2)
- **.from_input()** (`src_orchestrator_hf_eval_rs_hfevalinputowned_from_input`, Method, degree: 1)
- **HfEvalResult** (`src_orchestrator_hf_eval_rs_hfevalresult`, Struct, degree: 1)
- **alloy::primitives::Address** (`src_orchestrator_hf_eval_rs_import_alloy_primitives_address`, Module, degree: 1)
- **crate::core::types::{
    FlashLoanSource, FoundCycle, ProfitAssessment, RouteSimulationResult, TokenIndex,
}** (`src_orchestrator_hf_eval_rs_import_crate_core_types_flashloansource_foundcycle_profitassessment_routesimulationresult_tokenindex`, Module, degree: 1)
- **crate::pipeline::arena::StateArena** (`src_orchestrator_hf_eval_rs_import_crate_pipeline_arena_statearena`, Module, degree: 1)
- **crate::pipeline::local_sim::simulate_route_detailed** (`src_orchestrator_hf_eval_rs_import_crate_pipeline_local_sim_simulate_route_detailed`, Module, degree: 1)
- **crate::pipeline::ternary::optimize_cycle** (`src_orchestrator_hf_eval_rs_import_crate_pipeline_ternary_optimize_cycle`, Module, degree: 1)
- **crate::pipeline::types::OptimizationResult** (`src_orchestrator_hf_eval_rs_import_crate_pipeline_types_optimizationresult`, Module, degree: 1)
- **crate::services::execution::impact_slippage::{depth_impact_slippage_bps, effective_slippage_bps}** (`src_orchestrator_hf_eval_rs_import_crate_services_execution_impact_slippage_depth_impact_slippage_bps_effective_slippage_bps`, Module, degree: 1)
- **crate::services::execution::profit::{
    ProfitEvalContext, ProfitThresholds, RouteProfitParams, assess_profit, build_assess_input,
}** (`src_orchestrator_hf_eval_rs_import_crate_services_execution_profit_profitevalcontext_profitthresholds_routeprofitparams_assess_profit_build_assess_input`, Module, degree: 1)
- **rayon::prelude::*** (`src_orchestrator_hf_eval_rs_import_rayon_prelude`, Module, degree: 1)
- **ruint::aliases::U256** (`src_orchestrator_hf_eval_rs_import_ruint_aliases_u256`, Module, degree: 1)
- **rustc_hash::FxHashMap** (`src_orchestrator_hf_eval_rs_import_rustc_hash_fxhashmap`, Module, degree: 1)
- **std::collections::HashMap** (`src_orchestrator_hf_eval_rs_import_std_collections_hashmap`, Module, degree: 1)
- **std::sync::LazyLock** (`src_orchestrator_hf_eval_rs_import_std_sync_lazylock`, Module, degree: 1)

## Relationships

- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_import_std_collections_hashmap (imports)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_import_std_sync_lazylock (imports)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_import_alloy_primitives_address (imports)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_import_ruint_aliases_u256 (imports)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_import_rustc_hash_fxhashmap (imports)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_import_crate_core_types_flashloansource_foundcycle_profitassessment_routesimulationresult_tokenindex (imports)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_import_crate_pipeline_arena_statearena (imports)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_import_crate_pipeline_local_sim_simulate_route_detailed (imports)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_import_crate_pipeline_ternary_optimize_cycle (imports)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_import_crate_pipeline_types_optimizationresult (imports)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_import_crate_services_execution_impact_slippage_depth_impact_slippage_bps_effective_slippage_bps (imports)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_import_crate_services_execution_profit_profitevalcontext_profitthresholds_routeprofitparams_assess_profit_build_assess_input (imports)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_build_hf_eval_pool (defines)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_hfevalinput (defines)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_hfevalinputowned (defines)
- src_orchestrator_hf_eval_rs_hfevalinputowned → src_orchestrator_hf_eval_rs_hfevalinputowned_from_input (defines)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_hfevalresult (defines)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_evaluate_cycles_parallel (defines)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_evaluate_cycles_parallel_async (defines)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_evaluate_one (defines)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_best_assessment_for_cycle (defines)
- src_orchestrator_hf_eval_rs → src_orchestrator_hf_eval_rs_import_rayon_prelude (imports)
- src_orchestrator_hf_eval_rs_evaluate_cycles_parallel → src_orchestrator_hf_eval_rs_evaluate_one (calls)
- src_orchestrator_hf_eval_rs_evaluate_cycles_parallel_async → src_orchestrator_hf_eval_rs_evaluate_cycles_parallel (calls)
- src_orchestrator_hf_eval_rs_evaluate_one → src_orchestrator_hf_eval_rs_best_assessment_for_cycle (calls)

