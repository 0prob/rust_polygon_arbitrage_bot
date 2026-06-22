use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};

use crate::tui::app::App;
use crate::tui::layout::opp_layout;
use crate::tui::route_viz::format_score_delta;
use crate::tui::text::truncate_str;
use crate::tui::theme::Theme;

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let chunks = opp_layout(area, app.show_detail);
    render_table(f, chunks[0], app);
    if app.show_detail {
        render_detail(f, chunks[1], app);
    }
}

fn render_table(f: &mut Frame, area: Rect, app: &App) {
    let filtered = app.filtered_opportunities();
    let filter_hint = if app.filter.editing {
        format!("Filter: {}▌", app.filter.text)
    } else if !app.filter.text.is_empty() {
        format!("Filter: {} (/)", app.filter.text)
    } else {
        "j/k navigate  Enter detail  / filter  f 2-5 hops  l long-tail".into()
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(
            " Opportunities ({}) — {filter_hint} ",
            filtered.len()
        ))
        .border_style(Theme::block_border());

    let header = Row::new(vec![
        "FP", "Route", "Hops", "Mix", "Hub", "BF", "Live", "Δ", "Profit", "Risk", "Calls",
    ])
    .style(Theme::header())
    .height(1);

    let rows: Vec<Row> = filtered
        .iter()
        .enumerate()
        .skip(app.opp_scroll)
        .take(area.height.saturating_sub(4) as usize)
        .map(|(vis_i, o)| {
            let i = app.opp_scroll + vis_i;
            let selected = i == app.opp_selected;
            let fp = format!("{:08x}", o.fingerprint & 0xffff_ffff);
            let hops = format!("{}", o.cycle.hop_count);
            let mix = o.protocols.join(",");
            let bf = format!("{:.4}", o.bf_score);
            let live = o
                .live_score
                .map(|s| format!("{:.4}", s))
                .unwrap_or_else(|| "—".into());
            let delta = format_score_delta(o.bf_score, o.live_score);
            let profit = o
                .est_profit_usd
                .map(|p| format!("${p:.2}"))
                .unwrap_or_else(|| "—".into());
            let risk = format!("{:.0}%", o.liquidity_risk * 100.0);
            let calls = format!("{}", o.call_count);

            let route_style = if o.is_long_tail {
                Theme::long_tail()
            } else {
                ratatui::style::Style::default().fg(Theme::fg())
            };

            let mut row = Row::new(vec![
                Cell::from(fp),
                Cell::from(o.route_summary.clone()).style(route_style),
                Cell::from(hops),
                Cell::from(truncate_str(&mix, 18)),
                Cell::from(o.source_hub.clone()),
                Cell::from(bf).style(Theme::score_style(o.bf_score)),
                Cell::from(live).style(Theme::score_style(o.live_score.unwrap_or(0.0))),
                Cell::from(delta),
                Cell::from(profit).style(Theme::profit()),
                Cell::from(risk).style(if o.liquidity_risk > 0.4 {
                    Theme::warn()
                } else {
                    Theme::muted()
                }),
                Cell::from(calls),
            ])
            .height(1);

            if selected {
                row = row.style(Theme::selected_row());
            }
            if o.is_long_tail {
                row = row.style(Theme::long_tail());
            }

            row
        })
        .collect();

    let widths = [
        Constraint::Length(10),
        Constraint::Min(28),
        Constraint::Length(5),
        Constraint::Length(14),
        Constraint::Length(10),
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Length(7),
        Constraint::Length(8),
        Constraint::Length(6),
        Constraint::Length(6),
    ];

    let table = Table::new(rows, widths).header(header).block(block);
    f.render_widget(table, area);
}

fn render_detail(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Route Detail ")
        .border_style(Theme::block_border());
    let inner = block.inner(area);
    f.render_widget(block, area);

    let text = if let Some(o) = app.selected_opportunity() {
        let mut lines = vec![o.route_detail.clone()];
        if o.is_long_tail {
            lines.push("\n⚠ LONG-TAIL route — elevated revert risk".into());
        }
        lines.push(format!(
            "\nFingerprint: {:016x}\nCalls: {}  Freshness: {}ms",
            o.fingerprint, o.call_count, o.freshness_ms
        ));
        lines.join("\n")
    } else {
        "Select a route".into()
    };

    f.render_widget(Paragraph::new(text), inner);
}
