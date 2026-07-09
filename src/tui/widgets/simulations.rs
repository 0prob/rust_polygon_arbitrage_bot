use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::tui::app::App;
use crate::tui::layout;
use crate::tui::theme;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(snapshot) = app.snapshot.as_ref() else {
        frame.render_widget(
            Paragraph::new("waiting for simulation data...")
                .block(Block::default().borders(Borders::ALL).title("Simulations")),
            area,
        );
        return;
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(14), Constraint::Length(10)])
        .split(area);

    let total = snapshot.simulations.len();
    let table_block = Block::default().borders(Borders::ALL);
    let visible_rows = layout::table_body_rows(&table_block, chunks[0]);
    let selected = app.selected_row_index().unwrap_or(0);
    let (start, end) = layout::table_viewport(total, selected, visible_rows);
    let mut rows = Vec::with_capacity(end.saturating_sub(start));
    for sim in snapshot.simulations[start..end].iter() {
        rows.push(Row::new(vec![
            Cell::from(format!("{:x}", sim.fingerprint)),
            Cell::from(sim.route.clone()),
            Cell::from(sim.amount_in.clone()),
            Cell::from(sim.amount_out.clone()),
            Cell::from(sim.gross_profit.clone()),
            Cell::from(sim.net_profit.clone()),
            Cell::from(sim.gas.to_string()),
        ]));
    }

    frame.render_widget(
        Table::new(
            rows,
            [
                Constraint::Length(12),
                Constraint::Percentage(35),
                Constraint::Length(10),
                Constraint::Length(10),
                Constraint::Length(14),
                Constraint::Length(14),
                Constraint::Length(8),
            ],
        )
        .header(Row::new(vec![
            Cell::from("fp"),
            Cell::from("route"),
            Cell::from("in"),
            Cell::from("out"),
            Cell::from("gross"),
            Cell::from("net"),
            Cell::from("gas"),
        ]))
        .block(table_block.title(Line::from(vec![
            Span::styled(" ", theme::muted()),
            Span::styled(
                format!(
                    "Local sim [{} / {}]",
                    if total == 0 { 0 } else { start + 1 },
                    total
                ),
                theme::title(),
            ),
        ]))),
        chunks[0],
    );

    let selected = app.selected_route().map_or_else(
        || vec![Line::from("no selection")],
        |r| {
            vec![
                theme::label_value("route", r.route.clone(), crate::tui::app::Severity::Info),
                theme::label_value(
                    "raw score",
                    format!("{:.4}", r.raw_score),
                    crate::tui::app::Severity::Info,
                ),
                theme::label_value(
                    "rescored",
                    format!("{:.4}", r.rescored),
                    crate::tui::app::Severity::Info,
                ),
                theme::label_value(
                    "risk/liquidity",
                    format!("{}/{}", r.risk_score, r.liquidity_score),
                    if r.risk_score >= 60 {
                        crate::tui::app::Severity::Warn
                    } else {
                        crate::tui::app::Severity::Good
                    },
                ),
                theme::label_value(
                    "long-tail",
                    r.long_tail.to_string(),
                    if r.long_tail {
                        crate::tui::app::Severity::Warn
                    } else {
                        crate::tui::app::Severity::Good
                    },
                ),
            ]
        },
    );
    frame.render_widget(
        Paragraph::new(selected).block(Block::default().borders(Borders::ALL).title(Line::from(
            vec![
                Span::styled(" ", theme::muted()),
                Span::styled("Compare", theme::title()),
            ],
        ))),
        chunks[1],
    );
}
