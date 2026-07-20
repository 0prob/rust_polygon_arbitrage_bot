use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, Table, TableState};

use crate::tui::app::{App, Severity};
use crate::tui::layout;
use crate::tui::theme;

/// HF Pipeline tab: post-verify assess/dispatch candidates with real gates.
/// Not a second soft-sim of Opportunities.
pub fn render(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let [table_area, detail_area] =
        Layout::vertical([Constraint::Fill(1), Constraint::Length(12)]).areas(area);

    let total = app.hf_candidates.len();
    let table_block = theme::table_block("HF Pipeline");
    let visible_rows = layout::table_body_rows(&table_block, table_area);
    let selected = if total == 0 {
        0
    } else {
        app.selected_index.min(total.saturating_sub(1))
    };
    let (start, end) = layout::table_viewport(total, selected, visible_rows);
    let mut rows = Vec::with_capacity(end.saturating_sub(start));
    for row in app.hf_candidates[start..end].iter() {
        let gate = if row.near_miss {
            "near-miss"
        } else if row.should_execute {
            "dispatch"
        } else {
            "reject"
        };
        let net_style = if row.should_execute && !row.near_miss {
            theme::good()
        } else if row.near_miss {
            theme::warn()
        } else {
            theme::bad()
        };
        rows.push(
            Row::new(vec![
                Cell::from(format!("{:x}", row.fingerprint)),
                Cell::from(row.hops.to_string()),
                Cell::from(row.flash.as_str()),
                Cell::from(row.route.clone()),
                Cell::from(format!("{} M", row.net_profit_matic)),
                Cell::from(gate),
                Cell::from(row.outcome.as_deref().unwrap_or(if row.near_miss {
                    "gate reject"
                } else {
                    "queued"
                })),
            ])
            .style(net_style),
        );
    }

    let mut table_state = TableState::default();
    if total > 0 {
        table_state.select(Some(selected.saturating_sub(start)));
    }
    frame.render_stateful_widget(
        Table::new(
            rows,
            [
                Constraint::Length(12),
                Constraint::Length(4),
                Constraint::Length(8),
                Constraint::Percentage(38),
                Constraint::Length(14),
                Constraint::Length(10),
                Constraint::Length(16),
            ],
        )
        .header(
            Row::new(vec![
                Cell::from("fp"),
                Cell::from("hops"),
                Cell::from("flash"),
                Cell::from("route"),
                Cell::from("net"),
                Cell::from("gate"),
                Cell::from("outcome"),
            ])
            .style(theme::table_header()),
        )
        .block(table_block.title(Line::from(format!(
            "HF Pipeline (full gates) [{} / {}] — not LF soft scout",
            if total == 0 { 0 } else { start + 1 },
            total
        ))))
        .row_highlight_style(theme::selected_row()),
        table_area,
        &mut table_state,
    );

    let detail = if let Some(row) = app.hf_candidates.get(selected).filter(|_| total > 0) {
        let reject = row
            .reject_reason
            .as_deref()
            .unwrap_or(if row.should_execute {
                "none (should_execute)"
            } else {
                "unknown"
            });
        vec![
            theme::label_value(
                "fingerprint",
                format!("{:x}", row.fingerprint),
                Severity::Info,
            ),
            theme::label_value("route", row.route.clone(), Severity::Info),
            theme::label_value(
                "amount in/out",
                format!("{} → {}", row.amount_in, row.amount_out),
                Severity::Info,
            ),
            theme::label_value(
                "gross tokens / net MATIC",
                format!("{} / {} MATIC", row.gross_profit, row.net_profit_matic),
                if row.should_execute {
                    Severity::Good
                } else {
                    Severity::Warn
                },
            ),
            theme::label_value(
                "flash / slip / gas",
                format!("{}  |  {} bps  |  {} gas", row.flash, row.slip_bps, row.gas),
                Severity::Info,
            ),
            theme::label_value(
                "should_execute",
                row.should_execute.to_string(),
                if row.should_execute {
                    Severity::Good
                } else {
                    Severity::Warn
                },
            ),
            theme::label_value(
                "reject",
                reject.to_string(),
                if row.reject_reason.is_some() {
                    Severity::Warn
                } else {
                    Severity::Good
                },
            ),
            theme::label_value(
                "outcome",
                row.outcome.clone().unwrap_or_else(|| {
                    if row.near_miss {
                        "not dispatched (near-miss)".into()
                    } else {
                        "awaiting dispatch/dry-run".into()
                    }
                }),
                row.outcome_severity,
            ),
            Line::from(Span::styled(
                "Source: HF assess after full min-profit/ROI/flash/slip gates (post-verify).",
                theme::muted(),
            )),
        ]
    } else {
        vec![
            Line::from("No HF candidates this tick."),
            Line::from(Span::styled(
                "Dispatch queue empty — Opportunities tab is LF soft scout only.",
                theme::muted(),
            )),
            Line::from(Span::styled(
                "Near-miss rows appear when net > 0 but a gate rejected should_execute.",
                theme::muted(),
            )),
        ]
    };
    frame.render_widget(
        Paragraph::new(detail).block(theme::panel_block("HF candidate detail")),
        detail_area,
    );
}
