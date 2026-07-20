use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table, TableState};

use crate::tui::app::App;
use crate::tui::layout;
use crate::tui::theme;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(snapshot) = app.snapshot.as_ref() else {
        frame.render_widget(
            Paragraph::new("waiting for portfolio data...").block(theme::panel_block("Portfolio")),
            area,
        );
        return;
    };

    let [table_area, bottom_area] =
        Layout::vertical([Constraint::Fill(1), Constraint::Length(10)]).areas(area);

    let total = snapshot.portfolio.len();
    let table_block = theme::table_block("Portfolio");
    let visible_rows = layout::table_body_rows(&table_block, table_area);
    let selected = app.selected_row_index().unwrap_or(0);
    let (start, end) = layout::table_viewport(total, selected, visible_rows);

    let rows = snapshot.portfolio[start..end]
        .iter()
        .map(|row| {
            Row::new(vec![
                Cell::from(row.label.clone()),
                Cell::from(row.address.clone()),
                Cell::from(row.balance.clone()),
                Cell::from(row.usd.clone()),
                Cell::from(row.source.clone()),
            ])
            .style(theme::severity_style(row.severity))
        })
        .collect::<Vec<_>>();

    let mut table_state = TableState::default();
    if total > 0 {
        table_state.select(Some(selected.saturating_sub(start)));
    }
    frame.render_stateful_widget(
        Table::new(
            rows,
            [
                Constraint::Length(12),
                Constraint::Percentage(38),
                Constraint::Length(16),
                Constraint::Length(16),
                Constraint::Percentage(24),
            ],
        )
        .header(
            Row::new(vec![
                Cell::from("asset"),
                Cell::from("address"),
                Cell::from("balance"),
                Cell::from("USD"),
                Cell::from("source"),
            ])
            .style(theme::table_header()),
        )
        .block(table_block.title(Line::from(vec![
            Span::styled(" ", theme::muted()),
            Span::styled(
                format!(
                    "Exposure [{} / {}]",
                    if total == 0 { 0 } else { start + 1 },
                    total
                ),
                theme::title(),
            ),
        ])))
        .row_highlight_style(theme::selected_row()),
        table_area,
        &mut table_state,
    );

    let summary = vec![
        theme::label_value("assets", total.to_string(), crate::tui::app::Severity::Info),
        theme::label_value(
            "pools",
            snapshot.overview.discovered_pools.to_string(),
            crate::tui::app::Severity::Info,
        ),
        theme::label_value(
            "routes",
            snapshot.overview.cycle_count.to_string(),
            crate::tui::app::Severity::Info,
        ),
        theme::label_value(
            "win rate",
            format!("{:.2}%", snapshot.overview.win_rate * 100.0),
            crate::tui::app::Severity::Info,
        ),
    ];

    let selected = snapshot.portfolio.get(selected).map_or_else(
        || vec![Line::from("no selection")],
        |row| {
            vec![
                theme::label_value("asset", row.label.clone(), row.severity),
                theme::label_value(
                    "address",
                    row.address.clone(),
                    crate::tui::app::Severity::Info,
                ),
                theme::label_value("balance", row.balance.clone(), row.severity),
                theme::label_value("USD", row.usd.clone(), row.severity),
                theme::label_value(
                    "source",
                    row.source.clone(),
                    crate::tui::app::Severity::Info,
                ),
            ]
        },
    );

    let [summary_area, selected_area] =
        Layout::horizontal([Constraint::Percentage(46), Constraint::Percentage(54)])
            .areas(bottom_area);
    frame.render_widget(
        Paragraph::new(summary).block(theme::panel_block("Summary")),
        summary_area,
    );
    frame.render_widget(
        Paragraph::new(selected).block(theme::panel_block("Selected asset")),
        selected_area,
    );
}
