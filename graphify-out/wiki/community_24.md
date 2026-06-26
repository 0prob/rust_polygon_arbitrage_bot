# Community 24: spawn_snapshot_poller()

**Members:** 19

## Nodes

- **run** (`src_tui_run_rs`, File, degree: 18)
- **apply_update()** (`src_tui_run_rs_apply_update`, Function, degree: 1)
- **anyhow::Context** (`src_tui_run_rs_import_anyhow_context`, Module, degree: 1)
- **crate::tui::app::{App, BotStatus}** (`src_tui_run_rs_import_crate_tui_app_app_botstatus`, Module, degree: 1)
- **crate::tui::bridge::UiBridge** (`src_tui_run_rs_import_crate_tui_bridge_uibridge`, Module, degree: 1)
- **crate::tui::events::handle_key** (`src_tui_run_rs_import_crate_tui_events_handle_key`, Module, degree: 1)
- **crate::tui::update::UiUpdate** (`src_tui_run_rs_import_crate_tui_update_uiupdate`, Module, degree: 1)
- **crate::tui::widgets** (`src_tui_run_rs_import_crate_tui_widgets`, Module, degree: 1)
- **crossterm::event::{self, Event, KeyEventKind}** (`src_tui_run_rs_import_crossterm_event_self_event_keyeventkind`, Module, degree: 1)
- **crossterm::execute** (`src_tui_run_rs_import_crossterm_execute`, Module, degree: 1)
- **crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
}** (`src_tui_run_rs_import_crossterm_terminal_enteralternatescreen_leavealternatescreen_disable_raw_mode_enable_raw_mode`, Module, degree: 1)
- **ratatui::backend::CrosstermBackend** (`src_tui_run_rs_import_ratatui_backend_crosstermbackend`, Module, degree: 1)
- **ratatui::Terminal** (`src_tui_run_rs_import_ratatui_terminal`, Module, degree: 1)
- **std::io** (`src_tui_run_rs_import_std_io`, Module, degree: 1)
- **std::time::Duration** (`src_tui_run_rs_import_std_time_duration`, Module, degree: 1)
- **tokio::sync::mpsc** (`src_tui_run_rs_import_tokio_sync_mpsc`, Module, degree: 1)
- **tokio::time** (`src_tui_run_rs_import_tokio_time`, Module, degree: 1)
- **run_tui()** (`src_tui_run_rs_run_tui`, Function, degree: 1)
- **spawn_snapshot_poller()** (`src_tui_run_rs_spawn_snapshot_poller`, Function, degree: 1)

## Relationships

- src_tui_run_rs → src_tui_run_rs_import_std_io (imports)
- src_tui_run_rs → src_tui_run_rs_import_std_time_duration (imports)
- src_tui_run_rs → src_tui_run_rs_import_anyhow_context (imports)
- src_tui_run_rs → src_tui_run_rs_import_crossterm_event_self_event_keyeventkind (imports)
- src_tui_run_rs → src_tui_run_rs_import_crossterm_execute (imports)
- src_tui_run_rs → src_tui_run_rs_import_crossterm_terminal_enteralternatescreen_leavealternatescreen_disable_raw_mode_enable_raw_mode (imports)
- src_tui_run_rs → src_tui_run_rs_import_ratatui_terminal (imports)
- src_tui_run_rs → src_tui_run_rs_import_ratatui_backend_crosstermbackend (imports)
- src_tui_run_rs → src_tui_run_rs_import_tokio_sync_mpsc (imports)
- src_tui_run_rs → src_tui_run_rs_import_tokio_time (imports)
- src_tui_run_rs → src_tui_run_rs_import_crate_tui_app_app_botstatus (imports)
- src_tui_run_rs → src_tui_run_rs_import_crate_tui_bridge_uibridge (imports)
- src_tui_run_rs → src_tui_run_rs_import_crate_tui_events_handle_key (imports)
- src_tui_run_rs → src_tui_run_rs_import_crate_tui_update_uiupdate (imports)
- src_tui_run_rs → src_tui_run_rs_import_crate_tui_widgets (imports)
- src_tui_run_rs → src_tui_run_rs_run_tui (defines)
- src_tui_run_rs → src_tui_run_rs_apply_update (defines)
- src_tui_run_rs → src_tui_run_rs_spawn_snapshot_poller (defines)

