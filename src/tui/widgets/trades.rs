use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::widgets::{Block, Borders, Cell, Row, Table};

use crate::tui::app::App;
use crate::tui::text::truncate_str;
use crate::tui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" Execution History ({}) ", app.trades.len()))
        .border_style(Theme::block_border());

    let header = Row::new(vec![
        "FP", "Route", "Hops", "Profit", "USD", "Gas", "Status", "Tx",
    ])
    .style(Theme::header());

    let rows: Vec<Row> =
        app.trades
            .iter()
            .rev()
            .take(area.height.saturating_sub(4) as usize)
            .map(|t| {
                let status_style = match t.status {
                    crate::tui::app::TradeStatus::Confirmed => Theme::profit(),
                    crate::tui::app::TradeStatus::Reverted
                    | crate::tui::app::TradeStatus::Failed => Theme::loss(),
                    _ => Theme::muted(),
                };
                Row::new(vec![
                    Cell::from(format!("{:08x}", t.fingerprint & 0xffff_ffff)),
                    Cell::from(truncate_str(&t.route_summary, 32)),
                    Cell::from(format!("{}", t.hops)),
                    Cell::from(t.profit_native.clone()),
                    Cell::from(format!("${:.2}", t.profit_usd)).style(Theme::profit()),
                    Cell::from(format!("{}", t.gas_used)),
                    Cell::from(t.status.label()).style(status_style),
                    Cell::from(t.tx_hash.clone().unwrap_or_else(|| "—".into())),
                ])
            })
            .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Min(24),
            Constraint::Length(5),
            Constraint::Length(12),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(7),
            Constraint::Length(18),
        ],
    )
    .header(header)
    .block(block);
    f.render_widget(table, area);
}
