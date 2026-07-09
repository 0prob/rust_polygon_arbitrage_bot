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
            Paragraph::new("waiting for portfolio data...")
                .block(Block::default().borders(Borders::ALL).title("Portfolio")),
            area,
        );
        return;
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(14), Constraint::Length(10)])
        .split(area);

    let total = snapshot.portfolio.len();
    let table_block = Block::default().borders(Borders::ALL);
    let visible_rows = layout::table_body_rows(&table_block, chunks[0]);
    let selected = app.selected_row_index().unwrap_or(0);
    let (start, end) = layout::table_viewport(total, selected, visible_rows);

    let rows = snapshot.portfolio[start..end]
        .iter()
        .enumerate()
        .map(|row| {
            let is_selected = start + row.0 == selected;
            let style = if is_selected {
                theme::severity_style(crate::tui::app::Severity::Good)
            } else {
                theme::severity_style(row.1.severity)
            };
            Row::new(vec![
                Cell::from(row.1.label.clone()),
                Cell::from(row.1.address.clone()),
                Cell::from(row.1.balance.clone()),
                Cell::from(row.1.usd.clone()),
                Cell::from(row.1.source.clone()),
            ])
            .style(style)
        })
        .collect::<Vec<_>>();

    frame.render_widget(
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
        .header(Row::new(vec![
            Cell::from("asset"),
            Cell::from("address"),
            Cell::from("balance"),
            Cell::from("USD"),
            Cell::from("source"),
        ]))
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
        ]))),
        chunks[0],
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

    let bottom = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(46), Constraint::Percentage(54)])
        .split(chunks[1]);
    frame.render_widget(
        Paragraph::new(summary).block(Block::default().borders(Borders::ALL).title(Line::from(
            vec![
                Span::styled(" ", theme::muted()),
                Span::styled("Summary", theme::title()),
            ],
        ))),
        bottom[0],
    );
    frame.render_widget(
        Paragraph::new(selected).block(Block::default().borders(Borders::ALL).title(Line::from(
            vec![
                Span::styled(" ", theme::muted()),
                Span::styled("Selected asset", theme::title()),
            ],
        ))),
        bottom[1],
    );
}
