use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::widgets::{Block, Borders, Cell, Row, Table};

use crate::tui::app::App;
use crate::tui::layout::split_horizontal;
use crate::tui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let chunks = split_horizontal(area, 50);
    render_hubs(f, chunks[0], app);
    render_protocols(f, chunks[1], app);
}

fn render_hubs(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(
            " Top Hubs ({} pools / {} tokens / {} edges) ",
            app.graph_stats.pool_count,
            app.graph_stats.token_count,
            app.graph_stats.edge_count
        ))
        .border_style(Theme::block_border());

    let header = Row::new(vec!["Token", "Out-Degree"]).style(Theme::header());
    let rows: Vec<Row> = app
        .graph_stats
        .top_hubs
        .iter()
        .map(|(tok, deg)| Row::new(vec![Cell::from(tok.clone()), Cell::from(format!("{deg}"))]))
        .collect();

    let table = Table::new(
        rows,
        [Constraint::Min(20), Constraint::Length(12)],
    )
    .header(header)
    .block(block);
    f.render_widget(table, area);
}

fn render_protocols(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(
            " Protocol Distribution (+{} recent) ",
            app.graph_stats.recent_discoveries
        ))
        .border_style(Theme::block_border());

    let mut entries: Vec<_> = app.graph_stats.protocol_counts.iter().collect();
    entries.sort_by(|a, b| b.1.cmp(a.1));

    let max = entries.first().map(|(_, c)| **c).unwrap_or(1).max(1);
    let header = Row::new(vec!["Protocol", "Pools", "Bar"]).style(Theme::header());
    let rows: Vec<Row> = entries
        .iter()
        .take(20)
        .map(|(name, count)| {
            let bar_len = (*count * 20 / max).max(1);
            let bar = "█".repeat(bar_len);
            Row::new(vec![
                Cell::from((*name).clone()),
                Cell::from(format!("{count}")),
                Cell::from(bar).style(Theme::accent()),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Min(16),
            Constraint::Length(8),
            Constraint::Min(12),
        ],
    )
    .header(header)
    .block(block);
    f.render_widget(table, area);
}
