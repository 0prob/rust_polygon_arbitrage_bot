use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::widgets::Block;

#[must_use]
pub fn root(area: Rect) -> [Rect; 3] {
    Layout::vertical([
        Constraint::Length(3),
        Constraint::Fill(1),
        Constraint::Length(8),
    ])
    .areas(area)
}

#[must_use]
pub fn inner_lines(block: &Block<'_>, area: Rect) -> usize {
    block.inner(area).height.max(1) as usize
}

#[must_use]
pub fn table_body_rows(block: &Block<'_>, area: Rect) -> usize {
    // Header row plus one line of bottom breathing room (matches the old `area.height - 4`).
    block.inner(area).height.saturating_sub(2).max(1) as usize
}

#[must_use]
pub fn table_viewport(total: usize, selected: usize, visible_rows: usize) -> (usize, usize) {
    if total == 0 {
        return (0, 0);
    }
    let start = selected
        .saturating_sub(visible_rows / 2)
        .min(total.saturating_sub(1));
    let end = (start + visible_rows).min(total);
    (start, end)
}
