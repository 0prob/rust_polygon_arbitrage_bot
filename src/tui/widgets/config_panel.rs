use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::tui::app::App;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(snapshot) = app.snapshot.as_ref() else {
        frame.render_widget(
            Paragraph::new("waiting for config snapshot...")
                .block(Block::default().borders(Borders::ALL).title("Config")),
            area,
        );
        return;
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(12), Constraint::Length(8)])
        .split(area);

    let rows = snapshot
        .config
        .iter()
        .map(|row| {
            Row::new(vec![
                Cell::from(row.key.clone()),
                Cell::from(row.value.clone()),
            ])
        })
        .collect::<Vec<_>>();

    frame.render_widget(
        Table::new(rows, [Constraint::Length(24), Constraint::Percentage(76)])
            .header(Row::new(vec![Cell::from("key"), Cell::from("value")]))
            .block(Block::default().borders(Borders::ALL).title("Config")),
        chunks[0],
    );

    let help = vec![
        Line::from("Search uses route text, protocols, or fingerprint."),
        Line::from("j/k or arrows move, h/l tabs, / search, ? help."),
        Line::from("f cycles sort order."),
    ];
    frame.render_widget(
        Paragraph::new(help).block(Block::default().borders(Borders::ALL).title("Controls")),
        chunks[1],
    );
}
