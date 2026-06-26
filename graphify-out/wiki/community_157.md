# Community 157: token_label()

**Members:** 7

## Nodes

- **compact_route()** (`src_tui_route_viz_rs_compact_route`, Function, degree: 4)
- **cycle_to_ui_opportunity()** (`src_tui_route_viz_rs_cycle_to_ui_opportunity`, Function, degree: 7)
- **detail_route_tree()** (`src_tui_route_viz_rs_detail_route_tree`, Function, degree: 4)
- **protocol_label_for_edge()** (`src_tui_route_viz_rs_protocol_label_for_edge`, Function, degree: 4)
- **protocols_in_cycle()** (`src_tui_route_viz_rs_protocols_in_cycle`, Function, degree: 3)
- **short_addr()** (`src_tui_route_viz_rs_short_addr`, Function, degree: 3)
- **token_label()** (`src_tui_route_viz_rs_token_label`, Function, degree: 4)

## Relationships

- src_tui_route_viz_rs_token_label → src_tui_route_viz_rs_short_addr (calls)
- src_tui_route_viz_rs_compact_route → src_tui_route_viz_rs_token_label (calls)
- src_tui_route_viz_rs_compact_route → src_tui_route_viz_rs_protocol_label_for_edge (calls)
- src_tui_route_viz_rs_detail_route_tree → src_tui_route_viz_rs_protocol_label_for_edge (calls)
- src_tui_route_viz_rs_detail_route_tree → src_tui_route_viz_rs_short_addr (calls)
- src_tui_route_viz_rs_protocols_in_cycle → src_tui_route_viz_rs_protocol_label_for_edge (calls)
- src_tui_route_viz_rs_cycle_to_ui_opportunity → src_tui_route_viz_rs_compact_route (calls)
- src_tui_route_viz_rs_cycle_to_ui_opportunity → src_tui_route_viz_rs_detail_route_tree (calls)
- src_tui_route_viz_rs_cycle_to_ui_opportunity → src_tui_route_viz_rs_protocols_in_cycle (calls)
- src_tui_route_viz_rs_cycle_to_ui_opportunity → src_tui_route_viz_rs_token_label (calls)

