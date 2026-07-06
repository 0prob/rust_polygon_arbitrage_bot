use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::tui::app::App;
use crate::tui::theme;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(12), Constraint::Length(9)])
        .split(area);

    let Some((snapshot, indices)) = app.route_view() else {
        frame.render_widget(
            Paragraph::new("waiting for opportunities...").block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Opportunities"),
            ),
            area,
        );
        return;
    };

    let total = indices.len();
    let start = app
        .selected_index
        .saturating_sub(6)
        .min(total.saturating_sub(1));
    let end = (start + 12).min(total);

    let mut table_rows: Vec<Row> = Vec::with_capacity(end.saturating_sub(start));
    for (idx, &route_idx) in indices[start..end].iter().enumerate() {
        let Some(route) = snapshot.opportunities.get(route_idx) else {
            continue;
        };
        let selected = start + idx == app.selected_index;
        let style = if selected {
            theme::severity_style(crate::tui::app::Severity::Good)
        } else if route.long_tail {
            theme::warn()
        } else {
            Style::default()
        };
        table_rows.push(
            Row::new(vec![
                Cell::from(format!("{:x}", route.fingerprint)),
                Cell::from(route.hops.to_string()),
                Cell::from(route.protocols.clone()),
                Cell::from(route.route.clone()),
                Cell::from(format!("{:.4}", route.rescored)),
                Cell::from(format!("{:.4} MATIC", route.profit_matic)),
                Cell::from(format!("{}/{}", route.risk_score, route.liquidity_score)),
            ])
            .style(style),
        );
    }

    let widths = [
        Constraint::Length(12),
        Constraint::Length(4),
        Constraint::Length(24),
        Constraint::Percentage(34),
        Constraint::Length(10),
        Constraint::Length(14),
        Constraint::Length(10),
    ];

    frame.render_widget(
        Table::new(table_rows, widths)
            .header(Row::new(vec![
                Cell::from("fingerprint"),
                Cell::from("hops"),
                Cell::from("protocols"),
                Cell::from("route"),
                Cell::from("score"),
                Cell::from("profit"),
                Cell::from("risk"),
            ]))
            .block(Block::default().borders(Borders::ALL).title(format!(
                "Opportunities [{} / {}] filter: {}",
                start + 1,
                total,
                if app.search.is_empty() {
                    "none"
                } else {
                    &app.search
                }
            )))
            .row_highlight_style(theme::accent()),
        chunks[0],
    );

    let detail = if let Some(route) = app.selected_route() {
        vec![
            Line::from(format!("fingerprint {:x}", route.fingerprint)),
            Line::from(format!("route {}", route.route)),
            Line::from(format!("detail {}", route.route_detail)),
            Line::from(format!(
                "amount in {} (~{:.4} MATIC)",
                route.amount_in_token, route.amount_in_matic
            )),
            Line::from(format!("amount out {}", route.amount_out_token)),
            Line::from(format!(
                "profit {:.4} MATIC / {:.2} USD",
                route.profit_matic, route.profit_usd
            )),
            Line::from(format!(
                "gas {}  long-tail {}",
                route.gas_estimate, route.long_tail
            )),
        ]
    } else {
        vec![Line::from("no route selected")]
    };

    frame.render_widget(
        Paragraph::new(detail).block(
            Block::default()
                .borders(Borders::ALL)
                .title("Selected route"),
        ),
        chunks[1],
    );
}
