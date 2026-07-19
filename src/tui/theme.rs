use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Block;

use crate::tui::app::Severity;

pub const PANEL: Color = Color::Rgb(18, 22, 34);
pub const PANEL_ALT: Color = Color::Rgb(24, 29, 44);
pub const TEXT: Color = Color::Rgb(226, 231, 240);
pub const MUTED: Color = Color::Rgb(140, 150, 170);
pub const ACCENT: Color = Color::Rgb(105, 171, 255);
pub const GOOD: Color = Color::Rgb(78, 201, 176);
pub const WARN: Color = Color::Rgb(255, 184, 108);
pub const BAD: Color = Color::Rgb(255, 96, 117);
pub const TAB_BG: Color = Color::Rgb(14, 17, 27);

#[must_use]
pub fn severity_color(severity: Severity) -> Color {
    match severity {
        Severity::Info => ACCENT,
        Severity::Warn => WARN,
        Severity::Error => BAD,
        Severity::Good => GOOD,
    }
}

#[must_use]
pub fn title() -> Style {
    Style::default().fg(TEXT).add_modifier(Modifier::BOLD)
}

#[must_use]
pub fn muted() -> Style {
    Style::default().fg(MUTED)
}

#[must_use]
pub fn good() -> Style {
    Style::default().fg(GOOD)
}

#[must_use]
pub fn warn() -> Style {
    Style::default().fg(WARN)
}

#[must_use]
pub fn bad() -> Style {
    Style::default().fg(BAD)
}

#[must_use]
pub fn accent() -> Style {
    Style::default().fg(ACCENT)
}

#[must_use]
pub fn panel_block(label: &'static str) -> Block<'static> {
    Block::default()
        .borders(ratatui::widgets::Borders::ALL)
        .border_style(muted())
        .style(Style::default().bg(PANEL))
        .title(Line::from(vec![
            Span::styled(" ", muted()),
            Span::styled(label, title()),
        ]))
}

#[must_use]
pub fn table_block(label: &'static str) -> Block<'static> {
    panel_block(label)
}

#[must_use]
pub fn selected_row() -> Style {
    Style::default()
        .bg(PANEL_ALT)
        .fg(TEXT)
        .add_modifier(Modifier::BOLD)
}

#[must_use]
pub fn table_header() -> Style {
    Style::default().fg(MUTED).add_modifier(Modifier::BOLD)
}

#[must_use]
pub fn tab_style(active: bool) -> Style {
    if active {
        Style::default()
            .fg(TEXT)
            .bg(ACCENT)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(MUTED).bg(TAB_BG)
    }
}

#[must_use]
pub fn tab_block() -> Block<'static> {
    Block::default()
        .borders(ratatui::widgets::Borders::ALL)
        .style(Style::default().bg(TAB_BG))
        .border_style(muted())
        .title(Line::from(vec![
            Span::styled(" arb cockpit ", title()),
            Span::styled("market intelligence and execution", muted()),
        ]))
}

#[must_use]
pub fn severity_style(severity: Severity) -> Style {
    Style::default()
        .fg(severity_color(severity))
        .add_modifier(Modifier::BOLD)
}

#[must_use]
pub fn label_value(
    label: impl Into<String>,
    value: impl Into<String>,
    severity: Severity,
) -> Line<'static> {
    Line::from(vec![
        Span::styled(label.into(), muted()),
        Span::raw("  "),
        Span::styled(value.into(), severity_style(severity)),
    ])
}
