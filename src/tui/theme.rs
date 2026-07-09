use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};

use crate::tui::app::Severity;

pub const BG: Color = Color::Rgb(10, 12, 20);
pub const PANEL: Color = Color::Rgb(18, 22, 34);
pub const PANEL_ALT: Color = Color::Rgb(24, 29, 44);
pub const TEXT: Color = Color::Rgb(226, 231, 240);
pub const MUTED: Color = Color::Rgb(140, 150, 170);
pub const ACCENT: Color = Color::Rgb(105, 171, 255);
pub const GOOD: Color = Color::Rgb(78, 201, 176);
pub const WARN: Color = Color::Rgb(255, 184, 108);
pub const BAD: Color = Color::Rgb(255, 96, 117);
pub const HIGHLIGHT: Color = Color::Rgb(247, 198, 68);

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
