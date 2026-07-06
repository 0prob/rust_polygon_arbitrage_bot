use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::tui::app::{App, Severity};
use crate::tui::theme;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(12), Constraint::Length(8)])
        .split(area);

    let len = app.trade_history.len();
    let mut rows = Vec::with_capacity(len);
    for (view_idx, trade) in app.trade_history.iter().rev().enumerate() {
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
                Cell::from(trade.tx_hash.clone().unwrap_or_else(|| "-".to_string())),
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
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!("Trades [{}]", len)),
        ),
        chunks[0],
    );

    let detail = if let Some(row) = app.selected_trade() {
        vec![
            Line::from(format!("latest {}", row.outcome)),
            Line::from(format!("fingerprint {:x}", row.fingerprint)),
            Line::from(format!("gas {:?}", row.gas_used)),
            Line::from(format!("profit {:?}", row.profit_wei)),
            Line::from(format!("tx {:?}", row.tx_hash)),
        ]
    } else {
        vec![Line::from("no trade history yet")]
    };
    frame.render_widget(
        Paragraph::new(detail).block(Block::default().borders(Borders::ALL).title("Latest")),
        chunks[1],
    );
}
