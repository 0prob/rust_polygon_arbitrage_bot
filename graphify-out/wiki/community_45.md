# Community 45: parse_args()

**Members:** 16

## Nodes

- **tui** (`src_bin_tui_rs`, File, degree: 15)
- **Args** (`src_bin_tui_rs_args`, Struct, degree: 1)
- **anyhow::Context** (`src_bin_tui_rs_import_anyhow_context`, Module, degree: 1)
- **rpbot::config::{AppConfig, WalletSecrets}** (`src_bin_tui_rs_import_rpbot_config_appconfig_walletsecrets`, Module, degree: 1)
- **rpbot::orchestrator::{RuntimeContext, run_pass_loop}** (`src_bin_tui_rs_import_rpbot_orchestrator_runtimecontext_run_pass_loop`, Module, degree: 1)
- **rpbot::tui::{App, UiBridge, run_tui}** (`src_bin_tui_rs_import_rpbot_tui_app_uibridge_run_tui`, Module, degree: 1)
- **rpbot::tui::mock::spawn_mock_updates** (`src_bin_tui_rs_import_rpbot_tui_mock_spawn_mock_updates`, Module, degree: 1)
- **rpbot::tui::run::spawn_snapshot_poller** (`src_bin_tui_rs_import_rpbot_tui_run_spawn_snapshot_poller`, Module, degree: 1)
- **rpbot::tui::update::UiUpdate** (`src_bin_tui_rs_import_rpbot_tui_update_uiupdate`, Module, degree: 1)
- **std::sync::Arc** (`src_bin_tui_rs_import_std_sync_arc`, Module, degree: 1)
- **tokio::sync::{mpsc, watch}** (`src_bin_tui_rs_import_tokio_sync_mpsc_watch`, Module, degree: 1)
- **tracing::info** (`src_bin_tui_rs_import_tracing_info`, Module, degree: 1)
- **tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt}** (`src_bin_tui_rs_import_tracing_subscriber_envfilter_layer_subscriberext_util_subscriberinitext`, Module, degree: 1)
- **init_tracing()** (`src_bin_tui_rs_init_tracing`, Function, degree: 2)
- **main()** (`src_bin_tui_rs_main`, Function, degree: 3)
- **parse_args()** (`src_bin_tui_rs_parse_args`, Function, degree: 2)

## Relationships

- src_bin_tui_rs → src_bin_tui_rs_import_std_sync_arc (imports)
- src_bin_tui_rs → src_bin_tui_rs_import_anyhow_context (imports)
- src_bin_tui_rs → src_bin_tui_rs_import_tokio_sync_mpsc_watch (imports)
- src_bin_tui_rs → src_bin_tui_rs_import_tracing_info (imports)
- src_bin_tui_rs → src_bin_tui_rs_import_tracing_subscriber_envfilter_layer_subscriberext_util_subscriberinitext (imports)
- src_bin_tui_rs → src_bin_tui_rs_import_rpbot_config_appconfig_walletsecrets (imports)
- src_bin_tui_rs → src_bin_tui_rs_import_rpbot_orchestrator_runtimecontext_run_pass_loop (imports)
- src_bin_tui_rs → src_bin_tui_rs_import_rpbot_tui_app_uibridge_run_tui (imports)
- src_bin_tui_rs → src_bin_tui_rs_import_rpbot_tui_mock_spawn_mock_updates (imports)
- src_bin_tui_rs → src_bin_tui_rs_import_rpbot_tui_run_spawn_snapshot_poller (imports)
- src_bin_tui_rs → src_bin_tui_rs_import_rpbot_tui_update_uiupdate (imports)
- src_bin_tui_rs → src_bin_tui_rs_args (defines)
- src_bin_tui_rs → src_bin_tui_rs_parse_args (defines)
- src_bin_tui_rs → src_bin_tui_rs_init_tracing (defines)
- src_bin_tui_rs → src_bin_tui_rs_main (defines)
- src_bin_tui_rs_main → src_bin_tui_rs_init_tracing (calls)
- src_bin_tui_rs_main → src_bin_tui_rs_parse_args (calls)

