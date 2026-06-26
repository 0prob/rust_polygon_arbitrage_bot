use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::tui::app::App;
use crate::tui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let status_style = Theme::status_style(app.status);
    let pnl = app.metrics.global_pnl_usd;
    let pnl_style = if pnl >= 0.0 {
        Theme::profit()
    } else {
        Theme::loss()
    };

    let line = Line::from(vec![
        Span::styled(" polarb ", Theme::title()),
        Span::raw("│ "),
        Span::styled(app.status.label(), status_style),
        Span::raw(format!(" │ up {}s ", app.uptime_secs())),
        Span::raw(format!("│ blk {} ", app.block)),
        Span::styled(format!("(+{}ms)", app.block_lag_ms), Theme::muted()),
        Span::raw(format!(" │ gas {:.1} gwei ", app.gas_gwei)),
        Span::raw("│ P&L "),
        Span::styled(format!("${pnl:.2}"), pnl_style),
        Span::raw(format!(
            " │ cycles {} │ last {}ms ",
            app.metrics.negative_cycles, app.metrics.last_search_ms
        )),
        Span::styled(" ? help  q quit ", Theme::muted()),
    ]);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Theme::block_border())
        .style(ratatui::style::Style::default().bg(Theme::bg()));
    let paragraph = Paragraph::new(line).block(block);
    f.render_widget(paragraph, area);
}
