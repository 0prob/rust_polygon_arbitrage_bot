use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::{Frame, text::Line};

use crate::tui::app::{App, Tab};
use crate::tui::layout::root;
use crate::tui::theme;

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

pub fn render(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    let [top, middle, bottom] = root(area);

    header::render(frame, top, app);

    if app.show_help || app.tab == Tab::Help {
        help::render(frame, middle, app);
    } else {
        match app.tab {
            Tab::Overview => overview::render(frame, middle, app),
            Tab::Opportunities => opportunities::render(frame, middle, app),
            Tab::Graph => graph_stats::render(frame, middle, app),
            Tab::Simulations => simulations::render(frame, middle, app),
            Tab::Trades => trades::render(frame, middle, app),
            Tab::Portfolio => portfolio::render(frame, middle, app),
            Tab::Diagnostics => diagnostics::render(frame, middle, app),
            Tab::Config => config_panel::render(frame, middle, app),
            Tab::Help => help::render(frame, middle, app),
        }
    }

    footer(frame, bottom, app);
}

fn footer(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let text = if let Some(route) = app.selected_route() {
        vec![Line::from(vec![
            ratatui::text::Span::styled(" selected ", theme::muted()),
            ratatui::text::Span::styled(
                format!("{:x}", route.fingerprint),
                theme::severity_style(crate::tui::app::Severity::Good),
            ),
            ratatui::text::Span::raw("  "),
            ratatui::text::Span::styled(route.route.clone(), theme::title()),
            ratatui::text::Span::raw("  "),
            ratatui::text::Span::styled(
                format!("net {:+.4} MATIC", route.net_profit_matic),
                if route.net_profit_matic > 0.0 {
                    theme::good()
                } else {
                    theme::bad()
                },
            ),
        ])]
    } else {
        vec![Line::from("no selection")]
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(theme::muted())
        .title(Line::from(vec![ratatui::text::Span::styled(
            " status ",
            theme::title(),
        )]));
    let paragraph = Paragraph::new(text).block(block);
    frame.render_widget(paragraph, area);
}
