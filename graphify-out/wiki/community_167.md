# Community 167: render_tabs()

**Members:** 6

## Nodes

- **mod** (`src_tui_widgets_mod_rs`, File, degree: 5)
- **crate::tui::app::{App, Tab}** (`src_tui_widgets_mod_rs_import_crate_tui_app_app_tab`, Module, degree: 1)
- **ratatui::Frame** (`src_tui_widgets_mod_rs_import_ratatui_frame`, Module, degree: 1)
- **render_main()** (`src_tui_widgets_mod_rs_render_main`, Function, degree: 3)
- **render_tab_content()** (`src_tui_widgets_mod_rs_render_tab_content`, Function, degree: 2)
- **render_tabs()** (`src_tui_widgets_mod_rs_render_tabs`, Function, degree: 2)

## Relationships

- src_tui_widgets_mod_rs → src_tui_widgets_mod_rs_import_ratatui_frame (imports)
- src_tui_widgets_mod_rs → src_tui_widgets_mod_rs_import_crate_tui_app_app_tab (imports)
- src_tui_widgets_mod_rs → src_tui_widgets_mod_rs_render_main (defines)
- src_tui_widgets_mod_rs → src_tui_widgets_mod_rs_render_tabs (defines)
- src_tui_widgets_mod_rs → src_tui_widgets_mod_rs_render_tab_content (defines)
- src_tui_widgets_mod_rs_render_main → src_tui_widgets_mod_rs_render_tabs (calls)
- src_tui_widgets_mod_rs_render_main → src_tui_widgets_mod_rs_render_tab_content (calls)

