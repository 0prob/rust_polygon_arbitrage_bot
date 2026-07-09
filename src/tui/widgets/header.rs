use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Tabs;

use crate::tui::app::{App, InputMode, Tab};
use crate::tui::theme;

pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let titles: Vec<Line> = Tab::ALL
        .iter()
        .map(|tab| Line::from(Span::styled(tab.title(), theme::tab_style(false))))
        .collect();

    let selected = app.tab.index();
    let title = match app.input_mode {
        InputMode::Search => format!("search: {}", app.search),
        InputMode::Normal => "j/k h/l tab switch  / search  ? help  q quit".to_string(),
    };

    let tabs = Tabs::new(titles)
        .block(theme::tab_block())
        .style(Style::default().bg(theme::TAB_BG))
        .highlight_style(theme::tab_style(true))
        .select(selected);

    frame.render_widget(tabs, area);

    let hint = Line::from(vec![
        Span::styled(
            title,
            match app.input_mode {
                InputMode::Search => theme::accent(),
                InputMode::Normal => theme::muted(),
            },
        ),
        Span::raw("  "),
        Span::styled(
            format!(
                "uptime {}s | routes {} | trades {} | profitable {}",
                app.snapshot
                    .as_ref()
                    .map_or(0, |s| s.overview.uptime.as_secs()),
                app.snapshot.as_ref().map_or(0, |s| s.opportunities.len()),
                app.trade_history.len(),
                app.snapshot
                    .as_ref()
                    .map_or(0, |s| s.overview.profitable_routes),
            ),
            theme::muted(),
        ),
    ]);
    let hint_area = Rect {
        x: area.x + 2,
        y: area.y + area.height.saturating_sub(1),
        width: area.width.saturating_sub(4),
        height: 1,
    };
    frame.render_widget(ratatui::widgets::Paragraph::new(hint), hint_area);
}
