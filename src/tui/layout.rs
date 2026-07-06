use ratatui::layout::{Constraint, Layout, Rect};

#[must_use]
pub fn root(area: Rect) -> [Rect; 3] {
    Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(6),
        Constraint::Length(8),
    ])
    .areas(area)
}
