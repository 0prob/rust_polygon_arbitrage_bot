use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell, Paragraph, Row, Table, TableState};

use crate::tui::app::{App, Severity};
use crate::tui::layout;
use crate::tui::theme;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let [table_area, detail_area] =
        Layout::vertical([Constraint::Fill(1), Constraint::Length(12)]).areas(area);

    let len = app.trade_history.len();
    let table_block = theme::table_block("Trades");
    let visible_rows = layout::table_body_rows(&table_block, table_area);
    let (start, end) = layout::table_viewport(len, app.selected_index, visible_rows);

    let mut rows = Vec::with_capacity(end.saturating_sub(start));
    for trade in app
        .trade_history
        .iter()
        .rev()
        .skip(start)
        .take(end.saturating_sub(start))
    {
        let tokens = if trade.tokens.is_empty() {
            trade.route.as_str()
        } else {
            trade.tokens.as_str()
        };
        let tokens = if tokens.is_empty() { "-" } else { tokens };
        rows.push(
            Row::new(vec![
                Cell::from(format!("{:x}", trade.fingerprint)),
                Cell::from(tokens.to_string()),
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
            .style(ratatui::style::Style::default().bg(theme::PANEL)),
        );
    }

    let mut table_state = TableState::default();
    if len > 0 {
        table_state.select(Some(app.selected_index.saturating_sub(start)));
    }
    frame.render_stateful_widget(
        Table::new(
            rows,
            [
                Constraint::Length(12),
                Constraint::Percentage(28),
                Constraint::Length(18),
                Constraint::Length(10),
                Constraint::Length(16),
                Constraint::Percentage(24),
            ],
        )
        .header(
            Row::new(vec![
                Cell::from("fingerprint"),
                Cell::from("tokens"),
                Cell::from("outcome"),
                Cell::from("gas"),
                Cell::from("profit"),
                Cell::from("tx hash"),
            ])
            .style(theme::table_header()),
        )
        .block(table_block)
        .row_highlight_style(theme::selected_row()),
        table_area,
        &mut table_state,
    );

    let detail = if let Some(row) = app.selected_trade() {
        let mut lines = vec![
            theme::label_value("latest", row.outcome.clone(), row.severity),
            theme::label_value(
                "fingerprint",
                format!("{:x}", row.fingerprint),
                Severity::Info,
            ),
            theme::label_value(
                "tokens",
                if row.tokens.is_empty() {
                    "-".to_string()
                } else {
                    row.tokens.clone()
                },
                Severity::Info,
            ),
            theme::label_value(
                "route",
                if row.route.is_empty() {
                    "-".to_string()
                } else {
                    row.route.clone()
                },
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
        ];
        if let Some(url) = row.explorer_tx.as_ref() {
            lines.push(theme::label_value("swap", url.clone(), Severity::Good));
        }
        if let Some(url) = row.explorer_contract.as_ref() {
            lines.push(theme::label_value("contract", url.clone(), Severity::Good));
        }
        lines
    } else {
        vec![Line::from("no trade history yet")]
    };
    frame.render_widget(
        Paragraph::new(detail).block(Block::bordered().title(Line::from(vec![
            Span::styled(" ", theme::muted()),
            Span::styled("Latest", theme::title()),
        ]))),
        detail_area,
    );
}
