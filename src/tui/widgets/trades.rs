use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::tui::app::{App, Severity};
use crate::tui::layout;
use crate::tui::theme;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(12), Constraint::Length(8)])
        .split(area);

    let len = app.trade_history.len();
    let table_block = Block::default()
        .borders(Borders::ALL)
        .title(Line::from(vec![
            Span::styled(" ", theme::muted()),
            Span::styled(format!("Trades [{}]", len), theme::title()),
        ]));
    let visible_rows = layout::table_body_rows(&table_block, chunks[0]);
    let (start, end) = layout::table_viewport(len, app.selected_index, visible_rows);

    let mut rows = Vec::with_capacity(end.saturating_sub(start));
    for (view_idx, trade) in app
        .trade_history
        .iter()
        .rev()
        .enumerate()
        .skip(start)
        .take(end.saturating_sub(start))
    {
        let selected = view_idx == app.selected_index;
        let style = if selected {
            theme::severity_style(Severity::Good)
        } else {
            ratatui::style::Style::default()
        };
        rows.push(
            Row::new(vec![
                Cell::from(format!("{:x}", trade.fingerprint)),
                Cell::from(trade.outcome.clone()),
                Cell::from(
                    trade
                        .gas_used
                        .map_or_else(|| "-".to_string(), |g| g.to_string()),
                ),
                Cell::from(
                    trade
                        .profit_wei
                        .map_or_else(|| "-".to_string(), |p| p.to_string()),
                ),
                Cell::from(trade.tx_hash.as_deref().unwrap_or("-")),
            ])
            .style(style),
        );
    }

    frame.render_widget(
        Table::new(
            rows,
            [
                Constraint::Length(12),
                Constraint::Length(22),
                Constraint::Length(10),
                Constraint::Length(20),
                Constraint::Percentage(40),
            ],
        )
        .header(Row::new(vec![
            Cell::from("fingerprint"),
            Cell::from("outcome"),
            Cell::from("gas"),
            Cell::from("profit"),
            Cell::from("tx hash"),
        ]))
        .block(table_block),
        chunks[0],
    );

    let detail = if let Some(row) = app.selected_trade() {
        vec![
            theme::label_value("latest", row.outcome.clone(), row.severity),
            theme::label_value(
                "fingerprint",
                format!("{:x}", row.fingerprint),
                Severity::Info,
            ),
            theme::label_value(
                "gas",
                row.gas_used
                    .map_or_else(|| "-".to_string(), |g| g.to_string()),
                Severity::Info,
            ),
            theme::label_value(
                "profit",
                row.profit_wei
                    .map_or_else(|| "-".to_string(), |p| p.to_string()),
                Severity::Info,
            ),
            theme::label_value(
                "tx",
                row.tx_hash.clone().unwrap_or_else(|| "-".to_string()),
                Severity::Info,
            ),
        ]
    } else {
        vec![Line::from("no trade history yet")]
    };
    frame.render_widget(
        Paragraph::new(detail).block(Block::default().borders(Borders::ALL).title(Line::from(
            vec![
                Span::styled(" ", theme::muted()),
                Span::styled("Latest", theme::title()),
            ],
        ))),
        chunks[1],
    );
}
