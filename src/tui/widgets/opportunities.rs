use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table};

use crate::tui::app::App;
use crate::tui::layout;
use crate::tui::theme;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(14), Constraint::Length(11)])
        .split(area);

    let Some((snapshot, indices)) = app.route_view() else {
        frame.render_widget(
            Paragraph::new("waiting for opportunities...")
                .block(theme::panel_block("Opportunities")),
            area,
        );
        return;
    };

    let total = indices.len();
    let table_block = theme::table_block("Opportunities");
    let visible_rows = layout::table_body_rows(&table_block, chunks[0]);
    let (start, end) = layout::table_viewport(total, app.selected_index, visible_rows);
    let table_block = table_block.title(Line::from(format!(
        "Opportunities [{} / {}] filter: {} | sort: {:?}",
        if total == 0 { 0 } else { start + 1 },
        total,
        if app.search.is_empty() {
            "none"
        } else {
            &app.search
        },
        app.sort_mode,
    )));

    let mut table_rows: Vec<Row> = Vec::with_capacity(end.saturating_sub(start));
    for (idx, &route_idx) in indices[start..end].iter().enumerate() {
        let Some(route) = snapshot.opportunities.get(route_idx) else {
            continue;
        };
        let selected = start + idx == app.selected_index;
        let net_style = if route.net_profit_matic > 0.0 {
            theme::good()
        } else {
            theme::bad()
        };
        let style = if selected {
            theme::selected_row()
        } else if route.long_tail {
            theme::warn()
        } else {
            Style::default().bg(theme::PANEL)
        };
        table_rows.push(
            Row::new(vec![
                Cell::from(span_text(
                    format!("{:x}", route.fingerprint),
                    theme::muted(),
                )),
                Cell::from(span_text(route.hops.to_string(), theme::accent())),
                Cell::from(span_text(route.protocols.as_str(), theme::title())),
                Cell::from(span_text(route.route.as_str(), theme::muted())),
                Cell::from(span_text(format!("{:.4}", route.rescored), theme::accent())),
                Cell::from(span_text(
                    format!("{:+.4} MATIC", route.net_profit_matic),
                    net_style,
                )),
                Cell::from(span_text(
                    format!("{}/{}", route.risk_score, route.liquidity_score),
                    if route.risk_score >= 60 {
                        theme::warn()
                    } else {
                        theme::good()
                    },
                )),
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
            .header(
                Row::new(vec![
                    Cell::from("fp"),
                    Cell::from("hops"),
                    Cell::from("protocols"),
                    Cell::from("route"),
                    Cell::from("score"),
                    Cell::from("net"),
                    Cell::from("risk"),
                ])
                .style(theme::table_header()),
            )
            .block(table_block)
            .row_highlight_style(theme::selected_row()),
        chunks[0],
    );

    let detail = if let Some(route) = app.selected_route() {
        vec![
            theme::label_value(
                "fingerprint",
                format!("{:x}", route.fingerprint),
                crate::tui::app::Severity::Info,
            ),
            theme::label_value(
                "route",
                route.route.clone(),
                crate::tui::app::Severity::Info,
            ),
            theme::label_value(
                "detail",
                route.route_detail.clone(),
                crate::tui::app::Severity::Info,
            ),
            theme::label_value(
                "input",
                format!(
                    "{} (~{:.4} MATIC)",
                    route.amount_in_token, route.amount_in_matic
                ),
                crate::tui::app::Severity::Info,
            ),
            theme::label_value(
                "output",
                route.amount_out_token.clone(),
                if route.amount_out_token == "n/a" {
                    crate::tui::app::Severity::Warn
                } else {
                    crate::tui::app::Severity::Good
                },
            ),
            theme::label_value(
                "gross/net",
                format!(
                    "{:+.4} / {:+.4} MATIC  |  {:.2} USD",
                    route.profit_matic, route.net_profit_matic, route.profit_usd
                ),
                if route.net_profit_matic > 0.0 {
                    crate::tui::app::Severity::Good
                } else {
                    crate::tui::app::Severity::Warn
                },
            ),
            theme::label_value(
                "gas/risk",
                format!(
                    "{} gas  |  {} long-tail",
                    route.gas_estimate, route.long_tail
                ),
                if route.long_tail {
                    crate::tui::app::Severity::Warn
                } else {
                    crate::tui::app::Severity::Info
                },
            ),
        ]
    } else {
        vec![Line::from("no route selected")]
    };

    frame.render_widget(
        Paragraph::new(detail).block(theme::panel_block("Selected route")),
        chunks[1],
    );
}

fn span_text(text: impl Into<String>, style: ratatui::style::Style) -> Line<'static> {
    Line::from(vec![Span::styled(text.into(), style)])
}
