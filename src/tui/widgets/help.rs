use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

use crate::tui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect) {
    let popup_area = centered_rect(60, 70, area);
    f.render_widget(Clear, popup_area);

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Help ")
        .border_style(Theme::block_border());
    let inner = block.inner(popup_area);
    f.render_widget(block, popup_area);

    let lines = vec![
        Line::from(Span::styled("Navigation", Theme::title())),
        Line::from("  1-8        Switch tabs"),
        Line::from("  Tab        Next tab"),
        Line::from("  q / Ctrl-C Quit"),
        Line::from("  ?          Toggle this help"),
        Line::from(""),
        Line::from(Span::styled("Opportunities", Theme::title())),
        Line::from("  j/k ↑↓     Move selection"),
        Line::from("  Enter/d    Toggle route detail panel"),
        Line::from("  /          Filter routes"),
        Line::from("  f          Toggle 2-5 hop filter"),
        Line::from("  l          Toggle long-tail only"),
        Line::from("  Esc        Clear filter / close detail"),
        Line::from(""),
        Line::from(Span::styled("Colors", Theme::title())),
        Line::from(Span::styled("  Green", Theme::profit())),
        Line::from("    Profitable scores, confirmed trades"),
        Line::from(Span::styled("  Red", Theme::loss())),
        Line::from("    Reverts, unprofitable scores"),
        Line::from(Span::styled("  Orange", Theme::long_tail())),
        Line::from("    Long-tail token routes"),
        Line::from(""),
        Line::from(Span::styled("Press any key to close", Theme::muted())),
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    use ratatui::layout::{Constraint, Direction, Layout};
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
