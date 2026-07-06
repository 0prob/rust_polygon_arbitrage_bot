use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::tui::app::App;

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
        .constraints([Constraint::Min(12), Constraint::Length(8)])
        .split(area);

    let mut rows = Vec::with_capacity(snapshot.simulations.len());
    for sim in snapshot.simulations.as_ref() {
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
        .block(Block::default().borders(Borders::ALL).title("Local sim")),
        chunks[0],
    );

    let selected = app.selected_route().map_or_else(
        || vec![Line::from("no selection")],
        |r| {
            vec![
                Line::from(format!("route {}", r.route)),
                Line::from(format!("raw score {:.4}", r.raw_score)),
                Line::from(format!("rescored {:.4}", r.rescored)),
                Line::from(format!(
                    "risk {} liquidity {}",
                    r.risk_score, r.liquidity_score
                )),
                Line::from(format!("long-tail {}", r.long_tail)),
            ]
        },
    );
    frame.render_widget(
        Paragraph::new(selected).block(Block::default().borders(Borders::ALL).title("Compare")),
        chunks[1],
    );
}
