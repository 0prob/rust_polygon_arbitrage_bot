use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::tui::app::App;
use crate::tui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    if app.portfolio.is_empty() {
        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Portfolio / Exposure ")
            .border_style(Theme::block_border());
        let inner = block.inner(area);
        f.render_widget(block, area);
        f.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled("No wallet balances loaded.", Theme::muted())),
                Line::from(Span::raw(
                    "Connect execution wallet RPC to populate balances via oracle.",
                )),
            ]),
            inner,
        );
        return;
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Portfolio / Exposure ")
        .border_style(Theme::block_border());

    let header =
        Row::new(vec!["Symbol", "Balance", "USD", "%", "Long-tail"]).style(Theme::header());
    let rows: Vec<Row> = app
        .portfolio
        .iter()
        .map(|b| {
            Row::new(vec![
                Cell::from(b.symbol.clone()),
                Cell::from(b.balance.clone()),
                Cell::from(format!("${:.2}", b.usd_value)),
                Cell::from(format!("{:.1}%", b.pct_total)),
                Cell::from(if b.is_long_tail { "⚠" } else { "" }),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Min(16),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(10),
        ],
    )
    .header(header)
    .block(block);
    f.render_widget(table, area);
}
