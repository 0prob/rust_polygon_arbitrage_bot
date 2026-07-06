use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::tui::app::App;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(snapshot) = app.snapshot.as_ref() else {
        frame.render_widget(
            Paragraph::new("waiting for diagnostics...")
                .block(Block::default().borders(Borders::ALL).title("Diagnostics")),
            area,
        );
        return;
    };

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);

    let left = snapshot
        .diagnostics
        .iter()
        .map(|row| Line::from(format!("{}  {}", row.key, row.value)))
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(left).block(Block::default().borders(Borders::ALL).title("Health")),
        chunks[0],
    );

    let right = vec![
        Line::from(format!("search ms {}", snapshot.overview.search_ms)),
        Line::from(format!("hf ms {}", snapshot.overview.hf_ms)),
        Line::from(format!("cycles {}", snapshot.overview.cycle_count)),
        Line::from(format!(
            "trades {} / losses {}",
            snapshot.overview.total_trades, snapshot.overview.total_losses
        )),
        Line::from(format!(
            "pnl {:.4} MATIC",
            snapshot.overview.daily_pnl_wei as f64 / 1e18
        )),
    ];
    frame.render_widget(
        Paragraph::new(right).block(Block::default().borders(Borders::ALL).title("Runtime")),
        chunks[1],
    );
}
