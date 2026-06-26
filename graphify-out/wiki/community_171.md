# Community 171: handle_key()

**Members:** 5

## Nodes

- **events** (`src_tui_events_rs`, File, degree: 4)
- **handle_filter_input()** (`src_tui_events_rs_handle_filter_input`, Function, degree: 2)
- **handle_key()** (`src_tui_events_rs_handle_key`, Function, degree: 2)
- **crate::tui::app::{App, Tab}** (`src_tui_events_rs_import_crate_tui_app_app_tab`, Module, degree: 1)
- **crossterm::event::{KeyCode, KeyEvent, KeyModifiers}** (`src_tui_events_rs_import_crossterm_event_keycode_keyevent_keymodifiers`, Module, degree: 1)

## Relationships

- src_tui_events_rs → src_tui_events_rs_import_crossterm_event_keycode_keyevent_keymodifiers (imports)
- src_tui_events_rs → src_tui_events_rs_import_crate_tui_app_app_tab (imports)
- src_tui_events_rs → src_tui_events_rs_handle_key (defines)
- src_tui_events_rs → src_tui_events_rs_handle_filter_input (defines)
- src_tui_events_rs_handle_key → src_tui_events_rs_handle_filter_input (calls)

