# Community 5: run_hf_tick()

**Members:** 29

## Nodes

- **hf** (`src_orchestrator_hf_rs`, File, degree: 28)
- **HfContext** (`src_orchestrator_hf_rs_hfcontext`, Struct, degree: 1)
- **HfTickResult** (`src_orchestrator_hf_rs_hftickresult`, Struct, degree: 1)
- **crate::config::AppConfig** (`src_orchestrator_hf_rs_import_crate_config_appconfig`, Module, degree: 1)
- **crate::config::WalletSecrets** (`src_orchestrator_hf_rs_import_crate_config_walletsecrets`, Module, degree: 1)
- **crate::infra::hypersync::HyperSyncService** (`src_orchestrator_hf_rs_import_crate_infra_hypersync_hypersyncservice`, Module, degree: 1)
- **crate::infra::metrics::PipelineMetrics** (`src_orchestrator_hf_rs_import_crate_infra_metrics_pipelinemetrics`, Module, degree: 1)
- **crate::infra::rpc::RpcPool** (`src_orchestrator_hf_rs_import_crate_infra_rpc_rpcpool`, Module, degree: 1)
- **crate::infra::tracing_util::{pool_addrs_csv, start_token_addr}** (`src_orchestrator_hf_rs_import_crate_infra_tracing_util_pool_addrs_csv_start_token_addr`, Module, degree: 1)
- **crate::orchestrator::dispatch_queue::{
    PendingDispatch, queue_pending_dispatch, take_pending_dispatch,
}** (`src_orchestrator_hf_rs_import_crate_orchestrator_dispatch_queue_pendingdispatch_queue_pending_dispatch_take_pending_dispatch`, Module, degree: 1)
- **crate::orchestrator::hf_eval::{HfEvalInputOwned, evaluate_cycles_parallel_async}** (`src_orchestrator_hf_rs_import_crate_orchestrator_hf_eval_hfevalinputowned_evaluate_cycles_parallel_async`, Module, degree: 1)
- **crate::orchestrator::hf_execute::dispatch_profitable_candidates** (`src_orchestrator_hf_rs_import_crate_orchestrator_hf_execute_dispatch_profitable_candidates`, Module, degree: 1)
- **crate::orchestrator::ui_hook::SharedUiHook** (`src_orchestrator_hf_rs_import_crate_orchestrator_ui_hook_shareduihook`, Module, degree: 1)
- **crate::pipeline::spot_price::{SpotTable, rescore_cycles_with_table_and_gas}** (`src_orchestrator_hf_rs_import_crate_pipeline_spot_price_spottable_rescore_cycles_with_table_and_gas`, Module, degree: 1)
- **crate::pipeline::types::{compare_cycle_score, route_fingerprint as fp}** (`src_orchestrator_hf_rs_import_crate_pipeline_types_compare_cycle_score_route_fingerprint_as_fp`, Module, degree: 1)
- **crate::services::execution::{
    ExecutionService, GasOracle, OpportunityRecord, evaluated_from_sim,
    flash_policy::{hf_eval_flash_source, parse_flash_policy},
    log_opportunity_evaluated,
}** (`src_orchestrator_hf_rs_import_crate_services_execution_executionservice_gasoracle_opportunityrecord_evaluated_from_sim_flash_policy_hf_eval_flash_source_parse_flash_policy_log_opportunity_evaluated`, Module, degree: 1)
- **crate::services::hf_snapshot::SnapshotStore** (`src_orchestrator_hf_rs_import_crate_services_hf_snapshot_snapshotstore`, Module, degree: 1)
- **crate::services::partial_cache::PartialPoolCache** (`src_orchestrator_hf_rs_import_crate_services_partial_cache_partialpoolcache`, Module, degree: 1)
- **crate::services::state_cache::StateCache** (`src_orchestrator_hf_rs_import_crate_services_state_cache_statecache`, Module, degree: 1)
- **crate::services::state_refresh::StateRefreshService** (`src_orchestrator_hf_rs_import_crate_services_state_refresh_staterefreshservice`, Module, degree: 1)
- **crate::util::now_ms** (`src_orchestrator_hf_rs_import_crate_util_now_ms`, Module, degree: 1)
- **parking_lot::Mutex as ParkingMutex** (`src_orchestrator_hf_rs_import_parking_lot_mutex_as_parkingmutex`, Module, degree: 1)
- **ruint::aliases::U256** (`src_orchestrator_hf_rs_import_ruint_aliases_u256`, Module, degree: 1)
- **std::sync::Arc** (`src_orchestrator_hf_rs_import_std_sync_arc`, Module, degree: 1)
- **tokio::sync::{Mutex, watch}** (`src_orchestrator_hf_rs_import_tokio_sync_mutex_watch`, Module, degree: 1)
- **tracing::{debug, info, instrument, warn}** (`src_orchestrator_hf_rs_import_tracing_debug_info_instrument_warn`, Module, degree: 1)
- **parse_min_profit()** (`src_orchestrator_hf_rs_parse_min_profit`, Function, degree: 2)
- **run_dispatch_loop()** (`src_orchestrator_hf_rs_run_dispatch_loop`, Function, degree: 2)
- **run_hf_tick()** (`src_orchestrator_hf_rs_run_hf_tick`, Function, degree: 3)

## Relationships

- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_std_sync_arc (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_parking_lot_mutex_as_parkingmutex (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_ruint_aliases_u256 (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_tokio_sync_mutex_watch (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_tracing_debug_info_instrument_warn (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_config_appconfig (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_config_walletsecrets (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_infra_hypersync_hypersyncservice (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_infra_metrics_pipelinemetrics (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_infra_rpc_rpcpool (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_infra_tracing_util_pool_addrs_csv_start_token_addr (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_orchestrator_dispatch_queue_pendingdispatch_queue_pending_dispatch_take_pending_dispatch (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_orchestrator_hf_eval_hfevalinputowned_evaluate_cycles_parallel_async (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_orchestrator_hf_execute_dispatch_profitable_candidates (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_orchestrator_ui_hook_shareduihook (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_pipeline_spot_price_spottable_rescore_cycles_with_table_and_gas (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_pipeline_types_compare_cycle_score_route_fingerprint_as_fp (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_services_execution_executionservice_gasoracle_opportunityrecord_evaluated_from_sim_flash_policy_hf_eval_flash_source_parse_flash_policy_log_opportunity_evaluated (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_services_hf_snapshot_snapshotstore (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_services_partial_cache_partialpoolcache (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_services_state_cache_statecache (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_services_state_refresh_staterefreshservice (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_import_crate_util_now_ms (imports)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_hfcontext (defines)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_hftickresult (defines)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_run_hf_tick (defines)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_parse_min_profit (defines)
- src_orchestrator_hf_rs → src_orchestrator_hf_rs_run_dispatch_loop (defines)
- src_orchestrator_hf_rs_run_hf_tick → src_orchestrator_hf_rs_parse_min_profit (calls)
- src_orchestrator_hf_rs_run_hf_tick → src_orchestrator_hf_rs_run_dispatch_loop (calls)

