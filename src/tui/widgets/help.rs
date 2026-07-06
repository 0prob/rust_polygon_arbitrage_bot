use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::tui::app::App;

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
        Line::from("  Opportunities    sortable route table + drill-down"),
        Line::from("  Graph            protocol distribution and hubs"),
        Line::from("  Simulations      route compare and local sim preview"),
        Line::from("  Trades           execution outcomes and history"),
        Line::from("  Portfolio        exposure and balances"),
        Line::from("  Diagnostics      latency, cache, gas, indexer health"),
        Line::from("  Config           live tunables"),
    ];
    frame.render_widget(
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title("Help")),
        area,
    );
}
