use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::tui::app::App;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(snapshot) = app.snapshot.as_ref() else {
        frame.render_widget(
            Paragraph::new("waiting for graph snapshot...")
                .block(Block::default().borders(Borders::ALL).title("Graph")),
            area,
        );
        return;
    };

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(36),
            Constraint::Percentage(32),
            Constraint::Percentage(32),
        ])
        .split(area);

    render_panel(
        frame,
        chunks[0],
        "Health",
        vec![
            Line::from(format!(
                "generation {}",
                snapshot.graph.health.graph_generation
            )),
            Line::from(format!("tokens {}", snapshot.graph.health.token_count)),
            Line::from(format!("pools {}", snapshot.graph.health.pool_count)),
            Line::from(format!(
                "top out-degree {}",
                snapshot.graph.health.top_out_degree
            )),
            Line::from(format!(
                "stale indexer {}",
                snapshot.graph.health.stale_indexer
            )),
        ],
    );

    render_panel(
        frame,
        chunks[1],
        "Protocols",
        snapshot
            .graph
            .protocol_counts
            .iter()
            .map(|row| Line::from(format!("{}  {}", row.key, row.value)))
            .collect(),
    );

    render_panel(
        frame,
        chunks[2],
        "Recent",
        snapshot
            .graph
            .recent_discoveries
            .iter()
            .map(|row| Line::from(format!("{}  {}", row.key, row.value)))
            .collect(),
    );
}

fn render_panel(frame: &mut Frame<'_>, area: Rect, title: &str, lines: Vec<Line>) {
    frame.render_widget(
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(title)),
        area,
    );
}
