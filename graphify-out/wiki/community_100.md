# Community 100: FilterState

**Members:** 10

## Nodes

- **App** (`src_tui_app_rs_app`, Struct, degree: 8)
- **.clamp_selection()** (`src_tui_app_rs_app_clamp_selection`, Method, degree: 2)
- **.filtered_opportunities()** (`src_tui_app_rs_app_filtered_opportunities`, Method, degree: 4)
- **.new()** (`src_tui_app_rs_app_new`, Method, degree: 2)
- **.passes_filter()** (`src_tui_app_rs_app_passes_filter`, Method, degree: 2)
- **.push_alert()** (`src_tui_app_rs_app_push_alert`, Method, degree: 1)
- **.selected_opportunity()** (`src_tui_app_rs_app_selected_opportunity`, Method, degree: 2)
- **.uptime_secs()** (`src_tui_app_rs_app_uptime_secs`, Method, degree: 1)
- **FilterState** (`src_tui_app_rs_filterstate`, Struct, degree: 2)
- **.new_default()** (`src_tui_app_rs_filterstate_new_default`, Method, degree: 2)

## Relationships

- src_tui_app_rs_filterstate → src_tui_app_rs_filterstate_new_default (defines)
- src_tui_app_rs_app → src_tui_app_rs_app_new (defines)
- src_tui_app_rs_app → src_tui_app_rs_app_uptime_secs (defines)
- src_tui_app_rs_app → src_tui_app_rs_app_filtered_opportunities (defines)
- src_tui_app_rs_app → src_tui_app_rs_app_passes_filter (defines)
- src_tui_app_rs_app → src_tui_app_rs_app_clamp_selection (defines)
- src_tui_app_rs_app → src_tui_app_rs_app_selected_opportunity (defines)
- src_tui_app_rs_app → src_tui_app_rs_app_push_alert (defines)
- src_tui_app_rs_app_new → src_tui_app_rs_filterstate_new_default (calls)
- src_tui_app_rs_app_filtered_opportunities → src_tui_app_rs_app_passes_filter (calls)
- src_tui_app_rs_app_clamp_selection → src_tui_app_rs_app_filtered_opportunities (calls)
- src_tui_app_rs_app_selected_opportunity → src_tui_app_rs_app_filtered_opportunities (calls)

