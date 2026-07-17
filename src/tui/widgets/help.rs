use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::Paragraph;

use crate::tui::app::App;
use crate::tui::theme;

pub fn render(frame: &mut Frame<'_>, area: Rect, _app: &App) {
    let lines = vec![
        Line::from("Navigation"),
        Line::from("  j/k or Up/Down   move selection"),
        Line::from("  h/l or Left/Right switch tabs"),
        Line::from("  g / G            top / bottom"),
        Line::from("  /                search filter"),
        Line::from("  f                cycle sort"),
        Line::from("  ?                toggle this help"),
        Line::from("  q                quit"),
        Line::from(""),
        Line::from("Panels"),
        Line::from("  Overview         KPIs, sparkline trends, recent activity"),
        Line::from("  Opportunities    LF soft scout (not execute gates)"),
        Line::from("  Graph            protocol distribution and hubs"),
        Line::from("  HF Pipeline      full-gate HF candidates + dry-run outcomes"),
        Line::from("  Trades           execution outcomes and history"),
        Line::from("  Portfolio        exposure and balances"),
        Line::from("  Diagnostics      latency, cache, gas, indexer health"),
        Line::from("  Config           live tunables"),
    ];
    frame.render_widget(
        Paragraph::new(lines).block(theme::panel_block("Help")),
        area,
    );
}
