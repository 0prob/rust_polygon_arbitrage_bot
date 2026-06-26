# Community 101: UiBridge

**Members:** 10

## Nodes

- **build_graph_stats()** (`src_tui_bridge_rs_build_graph_stats`, Function, degree: 3)
- **UiBridge** (`src_tui_bridge_rs_uibridge`, Struct, degree: 9)
- **.new()** (`src_tui_bridge_rs_uibridge_new`, Method, degree: 3)
- **.notify_config()** (`src_tui_bridge_rs_uibridge_notify_config`, Method, degree: 3)
- **.notify_hf_tick()** (`src_tui_bridge_rs_uibridge_notify_hf_tick`, Method, degree: 2)
- **.notify_lf_complete()** (`src_tui_bridge_rs_uibridge_notify_lf_complete`, Method, degree: 3)
- **.notify_status()** (`src_tui_bridge_rs_uibridge_notify_status`, Method, degree: 2)
- **.notify_trade()** (`src_tui_bridge_rs_uibridge_notify_trade`, Method, degree: 2)
- **.sender()** (`src_tui_bridge_rs_uibridge_sender`, Method, degree: 1)
- **.try_send()** (`src_tui_bridge_rs_uibridge_try_send`, Method, degree: 6)

## Relationships

- src_tui_bridge_rs_uibridge → src_tui_bridge_rs_uibridge_new (defines)
- src_tui_bridge_rs_uibridge → src_tui_bridge_rs_uibridge_sender (defines)
- src_tui_bridge_rs_uibridge → src_tui_bridge_rs_uibridge_try_send (defines)
- src_tui_bridge_rs_uibridge → src_tui_bridge_rs_uibridge_notify_status (defines)
- src_tui_bridge_rs_uibridge → src_tui_bridge_rs_uibridge_notify_lf_complete (defines)
- src_tui_bridge_rs_uibridge → src_tui_bridge_rs_uibridge_notify_hf_tick (defines)
- src_tui_bridge_rs_uibridge → src_tui_bridge_rs_uibridge_notify_trade (defines)
- src_tui_bridge_rs_uibridge → src_tui_bridge_rs_uibridge_notify_config (defines)
- src_tui_bridge_rs_uibridge_notify_status → src_tui_bridge_rs_uibridge_try_send (calls)
- src_tui_bridge_rs_uibridge_notify_lf_complete → src_tui_bridge_rs_build_graph_stats (calls)
- src_tui_bridge_rs_uibridge_notify_lf_complete → src_tui_bridge_rs_uibridge_try_send (calls)
- src_tui_bridge_rs_uibridge_notify_hf_tick → src_tui_bridge_rs_uibridge_try_send (calls)
- src_tui_bridge_rs_uibridge_notify_trade → src_tui_bridge_rs_uibridge_try_send (calls)
- src_tui_bridge_rs_uibridge_notify_config → src_tui_bridge_rs_uibridge_try_send (calls)
- src_tui_bridge_rs_uibridge_notify_config → src_tui_bridge_rs_uibridge_new (calls)
- src_tui_bridge_rs_build_graph_stats → src_tui_bridge_rs_uibridge_new (calls)

