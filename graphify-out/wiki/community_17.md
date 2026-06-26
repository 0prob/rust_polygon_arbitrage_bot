# Community 17: pass_loop

**Members:** 22

## Nodes

- **pass_loop** (`src_orchestrator_pass_loop_rs`, File, degree: 25)
- **crate::config::{AppConfig, WalletSecrets}** (`src_orchestrator_pass_loop_rs_import_crate_config_appconfig_walletsecrets`, Module, degree: 1)
- **crate::error::ArbError** (`src_orchestrator_pass_loop_rs_import_crate_error_arberror`, Module, degree: 1)
- **crate::infra::hypersync::HyperSyncService** (`src_orchestrator_pass_loop_rs_import_crate_infra_hypersync_hypersyncservice`, Module, degree: 1)
- **crate::infra::metrics::PipelineMetrics** (`src_orchestrator_pass_loop_rs_import_crate_infra_metrics_pipelinemetrics`, Module, degree: 1)
- **crate::infra::rpc::RpcPool** (`src_orchestrator_pass_loop_rs_import_crate_infra_rpc_rpcpool`, Module, degree: 1)
- **crate::infra::wss_feed::spawn_pool_log_feed** (`src_orchestrator_pass_loop_rs_import_crate_infra_wss_feed_spawn_pool_log_feed`, Module, degree: 1)
- **crate::orchestrator::hf::{HfContext, run_hf_tick}** (`src_orchestrator_pass_loop_rs_import_crate_orchestrator_hf_hfcontext_run_hf_tick`, Module, degree: 1)
- **crate::orchestrator::lf::{LfContext, spawn_lf_background}** (`src_orchestrator_pass_loop_rs_import_crate_orchestrator_lf_lfcontext_spawn_lf_background`, Module, degree: 1)
- **crate::orchestrator::ui_hook::{SharedUiHook, noop_ui_hook}** (`src_orchestrator_pass_loop_rs_import_crate_orchestrator_ui_hook_shareduihook_noop_ui_hook`, Module, degree: 1)
- **crate::pipeline::graph_cache::{set_graph_rebuild_interval, GraphCache}** (`src_orchestrator_pass_loop_rs_import_crate_pipeline_graph_cache_set_graph_rebuild_interval_graphcache`, Module, degree: 1)
- **crate::services::execution::ExecutionService** (`src_orchestrator_pass_loop_rs_import_crate_services_execution_executionservice`, Module, degree: 1)
- **crate::services::execution::GasOracle** (`src_orchestrator_pass_loop_rs_import_crate_services_execution_gasoracle`, Module, degree: 1)
- **crate::services::hf_snapshot::SnapshotStore** (`src_orchestrator_pass_loop_rs_import_crate_services_hf_snapshot_snapshotstore`, Module, degree: 1)
- **crate::services::oracle::price_oracle::PriceOracle** (`src_orchestrator_pass_loop_rs_import_crate_services_oracle_price_oracle_priceoracle`, Module, degree: 1)
- **crate::services::partial_cache::{PartialPoolCache, StreamAddressSet}** (`src_orchestrator_pass_loop_rs_import_crate_services_partial_cache_partialpoolcache_streamaddressset`, Module, degree: 1)
- **crate::services::state_cache::StateCache** (`src_orchestrator_pass_loop_rs_import_crate_services_state_cache_statecache`, Module, degree: 1)
- **crate::services::state_refresh::StateRefreshService** (`src_orchestrator_pass_loop_rs_import_crate_services_state_refresh_staterefreshservice`, Module, degree: 1)
- **std::sync::Arc** (`src_orchestrator_pass_loop_rs_import_std_sync_arc`, Module, degree: 1)
- **tokio::sync::{Mutex, Semaphore, watch}** (`src_orchestrator_pass_loop_rs_import_tokio_sync_mutex_semaphore_watch`, Module, degree: 1)
- **tokio::time::{Duration, MissedTickBehavior, interval}** (`src_orchestrator_pass_loop_rs_import_tokio_time_duration_missedtickbehavior_interval`, Module, degree: 1)
- **tracing::{Instrument, debug, error, info, warn}** (`src_orchestrator_pass_loop_rs_import_tracing_instrument_debug_error_info_warn`, Module, degree: 1)

## Relationships

- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_std_sync_arc (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_tokio_sync_mutex_semaphore_watch (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_tokio_time_duration_missedtickbehavior_interval (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_tracing_instrument_debug_error_info_warn (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_config_appconfig_walletsecrets (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_infra_hypersync_hypersyncservice (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_error_arberror (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_infra_metrics_pipelinemetrics (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_infra_rpc_rpcpool (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_infra_wss_feed_spawn_pool_log_feed (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_orchestrator_hf_hfcontext_run_hf_tick (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_orchestrator_lf_lfcontext_spawn_lf_background (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_orchestrator_ui_hook_shareduihook_noop_ui_hook (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_pipeline_graph_cache_set_graph_rebuild_interval_graphcache (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_services_execution_executionservice (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_services_execution_gasoracle (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_services_hf_snapshot_snapshotstore (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_services_oracle_price_oracle_priceoracle (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_services_partial_cache_partialpoolcache_streamaddressset (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_services_state_cache_statecache (imports)
- src_orchestrator_pass_loop_rs → src_orchestrator_pass_loop_rs_import_crate_services_state_refresh_staterefreshservice (imports)

