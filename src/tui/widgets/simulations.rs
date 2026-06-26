use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::widgets::{Block, Borders, Cell, Row, Table};

use crate::tui::app::App;
use crate::tui::text::truncate_str;
use crate::tui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Simulations & Rescore ")
        .border_style(Theme::block_border());

    let header =
        Row::new(vec!["FP", "Route", "BF", "Live", "Slip", "Status"]).style(Theme::header());

    let rows: Vec<Row> = app
        .opportunities
        .iter()
        .take(40)
        .map(|o| {
            let live = o.live_score.unwrap_or(o.bf_score);
            Row::new(vec![
                Cell::from(format!("{:08x}", o.fingerprint & 0xffff_ffff)),
                Cell::from(truncate_str(&o.route_summary, 40)),
                Cell::from(format!("{:.4}", o.bf_score)).style(Theme::score_style(o.bf_score)),
                Cell::from(format!("{:.4}", live)).style(Theme::score_style(live)),
                Cell::from("—"),
                Cell::from(if live < o.bf_score {
                    "rescored↓"
                } else {
                    "ok"
                }),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Min(30),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(6),
            Constraint::Length(10),
        ],
    )
    .header(header)
    .block(block);
    f.render_widget(table, area);
}
