# Community 107: spawn_mock_updates()

**Members:** 10

## Nodes

- **mock** (`src_tui_mock_rs`, File, degree: 9)
- **crate::core::types::{Edge, FoundCycle, PoolIndex, ProtocolType, TokenIndex}** (`src_tui_mock_rs_import_crate_core_types_edge_foundcycle_poolindex_protocoltype_tokenindex`, Module, degree: 1)
- **crate::tui::app::{BotStatus, TradeStatus}** (`src_tui_mock_rs_import_crate_tui_app_botstatus_tradestatus`, Module, degree: 1)
- **crate::tui::bridge::UiBridge** (`src_tui_mock_rs_import_crate_tui_bridge_uibridge`, Module, degree: 1)
- **crate::tui::update::{
    GraphStatsSnapshot, ScannerMetrics, UiOpportunity, UiUpdate, alert_info, alert_warn,
    trade_from_outcome,
}** (`src_tui_mock_rs_import_crate_tui_update_graphstatssnapshot_scannermetrics_uiopportunity_uiupdate_alert_info_alert_warn_trade_from_outcome`, Module, degree: 1)
- **std::collections::HashMap** (`src_tui_mock_rs_import_std_collections_hashmap`, Module, degree: 1)
- **mock_cycles()** (`src_tui_mock_rs_mock_cycles`, Function, degree: 3)
- **mock_graph_stats()** (`src_tui_mock_rs_mock_graph_stats`, Function, degree: 2)
- **mock_opportunity()** (`src_tui_mock_rs_mock_opportunity`, Function, degree: 2)
- **spawn_mock_updates()** (`src_tui_mock_rs_spawn_mock_updates`, Function, degree: 3)

## Relationships

- src_tui_mock_rs → src_tui_mock_rs_import_std_collections_hashmap (imports)
- src_tui_mock_rs → src_tui_mock_rs_import_crate_core_types_edge_foundcycle_poolindex_protocoltype_tokenindex (imports)
- src_tui_mock_rs → src_tui_mock_rs_import_crate_tui_app_botstatus_tradestatus (imports)
- src_tui_mock_rs → src_tui_mock_rs_import_crate_tui_bridge_uibridge (imports)
- src_tui_mock_rs → src_tui_mock_rs_import_crate_tui_update_graphstatssnapshot_scannermetrics_uiopportunity_uiupdate_alert_info_alert_warn_trade_from_outcome (imports)
- src_tui_mock_rs → src_tui_mock_rs_spawn_mock_updates (defines)
- src_tui_mock_rs → src_tui_mock_rs_mock_graph_stats (defines)
- src_tui_mock_rs → src_tui_mock_rs_mock_cycles (defines)
- src_tui_mock_rs → src_tui_mock_rs_mock_opportunity (defines)
- src_tui_mock_rs_spawn_mock_updates → src_tui_mock_rs_mock_graph_stats (calls)
- src_tui_mock_rs_spawn_mock_updates → src_tui_mock_rs_mock_cycles (calls)
- src_tui_mock_rs_mock_cycles → src_tui_mock_rs_mock_opportunity (calls)

