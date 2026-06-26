# Community 53: tokio_console_enabled()

**Members:** 15

## Nodes

- **main** (`src_main_rs`, File, degree: 14)
- **anyhow::Context** (`src_main_rs_import_anyhow_context`, Module, degree: 1)
- **rpbot::config::{AppConfig, WalletSecrets}** (`src_main_rs_import_rpbot_config_appconfig_walletsecrets`, Module, degree: 1)
- **rpbot::core::constants::POLYGON_CHAIN_ID** (`src_main_rs_import_rpbot_core_constants_polygon_chain_id`, Module, degree: 1)
- **rpbot::orchestrator::{RuntimeContext, run_pass_loop}** (`src_main_rs_import_rpbot_orchestrator_runtimecontext_run_pass_loop`, Module, degree: 1)
- **std::sync::Arc** (`src_main_rs_import_std_sync_arc`, Module, degree: 1)
- **tokio::signal** (`src_main_rs_import_tokio_signal`, Module, degree: 1)
- **tokio::sync::watch** (`src_main_rs_import_tokio_sync_watch`, Module, degree: 1)
- **tracing::{info, warn}** (`src_main_rs_import_tracing_info_warn`, Module, degree: 1)
- **tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt}** (`src_main_rs_import_tracing_subscriber_envfilter_layer_subscriberext_util_subscriberinitext`, Module, degree: 1)
- **init_tracing()** (`src_main_rs_init_tracing`, Function, degree: 4)
- **json_logs_enabled()** (`src_main_rs_json_logs_enabled`, Function, degree: 2)
- **main()** (`src_main_rs_main`, Function, degree: 3)
- **shutdown_signal()** (`src_main_rs_shutdown_signal`, Function, degree: 2)
- **tokio_console_enabled()** (`src_main_rs_tokio_console_enabled`, Function, degree: 2)

## Relationships

- src_main_rs → src_main_rs_import_anyhow_context (imports)
- src_main_rs → src_main_rs_import_std_sync_arc (imports)
- src_main_rs → src_main_rs_import_tokio_signal (imports)
- src_main_rs → src_main_rs_import_tokio_sync_watch (imports)
- src_main_rs → src_main_rs_import_tracing_info_warn (imports)
- src_main_rs → src_main_rs_import_tracing_subscriber_envfilter_layer_subscriberext_util_subscriberinitext (imports)
- src_main_rs → src_main_rs_import_rpbot_config_appconfig_walletsecrets (imports)
- src_main_rs → src_main_rs_import_rpbot_core_constants_polygon_chain_id (imports)
- src_main_rs → src_main_rs_import_rpbot_orchestrator_runtimecontext_run_pass_loop (imports)
- src_main_rs → src_main_rs_tokio_console_enabled (defines)
- src_main_rs → src_main_rs_json_logs_enabled (defines)
- src_main_rs → src_main_rs_init_tracing (defines)
- src_main_rs → src_main_rs_main (defines)
- src_main_rs → src_main_rs_shutdown_signal (defines)
- src_main_rs_init_tracing → src_main_rs_json_logs_enabled (calls)
- src_main_rs_init_tracing → src_main_rs_tokio_console_enabled (calls)
- src_main_rs_main → src_main_rs_init_tracing (calls)
- src_main_rs_main → src_main_rs_shutdown_signal (calls)

