# Community 50: UiSimulation

**Members:** 15

## Nodes

- **app** (`src_tui_app_rs`, File, degree: 11)
- **Alert** (`src_tui_app_rs_alert`, Struct, degree: 1)
- **AlertLevel** (`src_tui_app_rs_alertlevel`, Enum, degree: 1)
- **BotStatus** (`src_tui_app_rs_botstatus`, Enum, degree: 2)
- **.label()** (`src_tui_app_rs_botstatus_label`, Method, degree: 1)
- **crate::tui::update::{
    BalanceRow, ConfigSnapshot, GraphStatsSnapshot, ScannerMetrics, UiOpportunity, UiSimResult,
}** (`src_tui_app_rs_import_crate_tui_update_balancerow_configsnapshot_graphstatssnapshot_scannermetrics_uiopportunity_uisimresult`, Module, degree: 1)
- **std::time::Instant** (`src_tui_app_rs_import_std_time_instant`, Module, degree: 1)
- **Tab** (`src_tui_app_rs_tab`, Enum, degree: 4)
- **.from_index()** (`src_tui_app_rs_tab_from_index`, Method, degree: 1)
- **.index()** (`src_tui_app_rs_tab_index`, Method, degree: 1)
- **.title()** (`src_tui_app_rs_tab_title`, Method, degree: 1)
- **TradeRecord** (`src_tui_app_rs_traderecord`, Struct, degree: 1)
- **TradeStatus** (`src_tui_app_rs_tradestatus`, Enum, degree: 2)
- **.label()** (`src_tui_app_rs_tradestatus_label`, Method, degree: 1)
- **UiSimulation** (`src_tui_app_rs_uisimulation`, Struct, degree: 1)

## Relationships

- src_tui_app_rs → src_tui_app_rs_import_std_time_instant (imports)
- src_tui_app_rs → src_tui_app_rs_import_crate_tui_update_balancerow_configsnapshot_graphstatssnapshot_scannermetrics_uiopportunity_uisimresult (imports)
- src_tui_app_rs → src_tui_app_rs_tab (defines)
- src_tui_app_rs_tab → src_tui_app_rs_tab_title (defines)
- src_tui_app_rs_tab → src_tui_app_rs_tab_index (defines)
- src_tui_app_rs_tab → src_tui_app_rs_tab_from_index (defines)
- src_tui_app_rs → src_tui_app_rs_botstatus (defines)
- src_tui_app_rs_botstatus → src_tui_app_rs_botstatus_label (defines)
- src_tui_app_rs → src_tui_app_rs_alertlevel (defines)
- src_tui_app_rs → src_tui_app_rs_alert (defines)
- src_tui_app_rs → src_tui_app_rs_tradestatus (defines)
- src_tui_app_rs_tradestatus → src_tui_app_rs_tradestatus_label (defines)
- src_tui_app_rs → src_tui_app_rs_traderecord (defines)
- src_tui_app_rs → src_tui_app_rs_uisimulation (defines)

