use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::tui::app::App;
use crate::tui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let c = &app.config_view;
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Config / Controls (read-only view) ")
        .border_style(Theme::block_border());
    let inner = block.inner(area);
    f.render_widget(block, area);

    let mode = if c.dry_run { "DRY-RUN" } else { "LIVE" };
    let mode_style = if c.dry_run { Theme::warn() } else { Theme::loss() };

    let lines = vec![
        Line::from(vec![
            Span::raw("Mode: "),
            Span::styled(mode, mode_style),
        ]),
        Line::from(format!("Cycle finder: {}", c.cycle_finder)),
        Line::from(format!("Max hops: {}", c.max_hops)),
        Line::from(format!("Max cycles: {}", c.max_cycles)),
        Line::from(format!("Time budget: {} ms", c.time_budget_ms)),
        Line::from(format!("Slippage: {} bps", c.slippage_bps)),
        Line::from(format!("Min profit MATIC wei: {}", c.min_profit_matic_wei)),
        Line::from(""),
        Line::from(Span::styled(
            "Live tuning: edit config.toml and restart, or extend with mpsc control messages.",
            Theme::muted(),
        )),
        Line::from(Span::styled(
            "Protocol toggles / blacklists: wire ConfigSnapshot updates from orchestrator.",
            Theme::muted(),
        )),
    ];
    f.render_widget(Paragraph::new(lines), inner);
}
