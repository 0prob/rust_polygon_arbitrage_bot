use ratatui::layout::{Constraint, Direction, Layout, Rect};

pub fn root_layout(area: Rect) -> [Rect; 3] {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Min(0),
        ])
        .split(area);
    [chunks[0], chunks[1], chunks[2]]
}

pub fn split_horizontal(area: Rect, left_pct: u16) -> [Rect; 2] {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(left_pct), Constraint::Min(0)])
        .split(area);
    [chunks[0], chunks[1]]
}

pub fn split_vertical(area: Rect, top_len: u16) -> [Rect; 2] {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(top_len), Constraint::Min(0)])
        .split(area);
    [chunks[0], chunks[1]]
}

pub fn kpi_row(area: Rect, count: usize) -> Vec<Rect> {
    let n = count.max(1) as u32;
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints(vec![Constraint::Ratio(1, n); count])
        .split(area)
        .to_vec()
}

pub fn overview_layout(area: Rect) -> [Rect; 2] {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(7), Constraint::Min(0)])
        .split(area);
    [chunks[0], chunks[1]]
}

pub fn opp_layout(area: Rect, show_detail: bool) -> [Rect; 2] {
    if show_detail {
        split_horizontal(area, 62)
    } else {
        [area, Rect::default()]
    }
}
