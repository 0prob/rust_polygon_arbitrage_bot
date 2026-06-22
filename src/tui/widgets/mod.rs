pub mod config_panel;
pub mod diagnostics;
pub mod graph_stats;
pub mod header;
pub mod help;
pub mod opportunities;
pub mod overview;
pub mod portfolio;
pub mod simulations;
pub mod trades;

use ratatui::Frame;

use crate::tui::app::{App, Tab};

pub fn render_main(f: &mut Frame, app: &App) {
    if app.show_help {
        help::render(f, f.area());
        return;
    }

    let chunks = crate::tui::layout::root_layout(f.area());
    header::render(f, chunks[0], app);
    render_tabs(f, chunks[1], app);
    render_tab_content(f, chunks[2], app);
}

fn render_tabs(f: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    use ratatui::widgets::{Block, Tabs};
    let titles: Vec<_> = Tab::ALL
        .iter()
        .enumerate()
        .map(|(i, t)| format!(" {}:{} ", i + 1, t.title()))
        .collect();
    let tabs = Tabs::new(titles)
        .block(Block::default())
        .style(crate::tui::theme::Theme::tab_inactive())
        .highlight_style(crate::tui::theme::Theme::tab_active())
        .select(app.tab.index());
    f.render_widget(tabs, area);
}

fn render_tab_content(f: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    match app.tab {
        Tab::Overview => overview::render(f, area, app),
        Tab::Opportunities => opportunities::render(f, area, app),
        Tab::Graph => graph_stats::render(f, area, app),
        Tab::Simulations => simulations::render(f, area, app),
        Tab::Trades => trades::render(f, area, app),
        Tab::Portfolio => portfolio::render(f, area, app),
        Tab::Diagnostics => diagnostics::render(f, area, app),
        Tab::Config => config_panel::render(f, area, app),
    }
}
