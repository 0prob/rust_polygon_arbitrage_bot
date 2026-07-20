use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Cell, Paragraph, Row, Table};

use crate::tui::app::App;
use crate::tui::theme;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(snapshot) = app.snapshot.as_ref() else {
        frame.render_widget(
            Paragraph::new("waiting for config snapshot...").block(theme::panel_block("Config")),
            area,
        );
        return;
    };

    let [table_area, note_area] =
        Layout::vertical([Constraint::Fill(1), Constraint::Length(8)]).areas(area);

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
            .header(
                Row::new(vec![Cell::from("key"), Cell::from("value")]).style(theme::table_header()),
            )
            .block(theme::table_block("Config")),
        table_area,
    );

    let help = vec![
        Line::from("Search uses route text, protocols, or fingerprint."),
        Line::from("j/k or arrows move, h/l tabs, / search, ? help."),
        Line::from("f cycles sort order."),
    ];
    frame.render_widget(
        Paragraph::new(help).block(theme::panel_block("Controls")),
        note_area,
    );
}
