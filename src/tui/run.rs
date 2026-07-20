use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::{mpsc, mpsc::Receiver, watch};

use crate::orchestrator::{RuntimeContext, run_pass_loop};
use crate::shutdown::{join_pass_loop_after_shutdown, wait_for_os_shutdown};

use super::app::App;
use super::bridge::TuiBridge;
use super::events::{UiEvent, spawn_input_thread};
use super::terminal::TerminalGuard;
use super::update::apply_event;

/// Caps terminal writes while keeping metric charts visibly responsive.
const REDRAW_INTERVAL: Duration = Duration::from_millis(250);

pub async fn run_tui<F>(
    bridge: TuiBridge,
    mut rx: Receiver<UiEvent>,
    mut snapshot_rx: watch::Receiver<Option<Arc<super::app::DashboardSnapshot>>>,
    bootstrap: F,
) -> anyhow::Result<()>
where
    F: Future<Output = anyhow::Result<Arc<RuntimeContext>>> + Send + 'static,
{
    // Bootstrap before taking the TTY: parallel terminal draw + spawn_blocking historically
    // stalled on "Loading configuration…" (SIGTTOU / blocking-pool contention). Logs go to
    // RPBot run dir while this runs (`RPBOT_LOG=debug` for phase timings).
    crate::info!("tui: loading configuration");
    let boot_started = std::time::Instant::now();
    let ctx = bootstrap.await?;
    crate::info!(
        "tui: configuration ready in {}ms",
        boot_started.elapsed().as_millis()
    );

    let mut terminal = TerminalGuard::enter().context("failed to initialize terminal")?;

    let mut app = App::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let pass_handle = tokio::spawn(run_pass_loop(Arc::clone(&ctx), shutdown_rx));
    // Let pass_loop (and the TUI snapshot publisher) start before the first blocking draw.
    tokio::task::yield_now().await;

    let tx = bridge.sender();
    let metrics_bridge = bridge.clone();
    let input_thread = spawn_input_thread(tx.clone()).context("spawn input thread")?;

    let (os_shutdown_tx, mut os_shutdown_rx) = mpsc::channel(1);
    tokio::spawn(async move {
        wait_for_os_shutdown().await;
        let _ = os_shutdown_tx.send(()).await;
    });

    draw_frame_blocking(&mut terminal, &app)?;

    let mut redraw = tokio::time::interval(REDRAW_INTERVAL);
    redraw.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut needs_redraw = true;

    loop {
        tokio::select! {
            biased;
            _ = os_shutdown_rx.recv() => break,
            maybe_event = rx.recv() => {
                let Some(event) = maybe_event else { break; };
                let mut immediate_draw = wants_immediate_draw(&event);
                apply_event(&mut app, event);
                if app.should_quit {
                    break;
                }
                needs_redraw = true;
                while let Ok(event) = rx.try_recv() {
                    immediate_draw |= wants_immediate_draw(&event);
                    apply_event(&mut app, event);
                    if app.should_quit {
                        break;
                    }
                    needs_redraw = true;
                }
                apply_coalesced(&metrics_bridge, &mut app, &mut needs_redraw);
                if app.should_quit {
                    break;
                }
                if immediate_draw && app.route_view_is_dirty() {
                    app.rebuild_route_view();
                }
                if immediate_draw {
                    draw_frame_blocking(&mut terminal, &app)?;
                    needs_redraw = false;
                }
            }
            changed = snapshot_rx.changed() => {
                if changed.is_ok()
                    && let Some(snapshot) = snapshot_rx.borrow().clone()
                {
                    app.set_snapshot(snapshot);
                    needs_redraw = true;
                }
                apply_coalesced(&metrics_bridge, &mut app, &mut needs_redraw);
            }
            _ = redraw.tick(), if !app.should_quit => {
                // Metrics can land only in coalesce slots while the live channel is quiet.
                apply_coalesced(&metrics_bridge, &mut app, &mut needs_redraw);
                if needs_redraw {
                    if app.route_view_is_dirty() {
                        app.rebuild_route_view();
                    }
                    draw_frame_blocking(&mut terminal, &app)?;
                    needs_redraw = false;
                }
            }
        }
    }

    let _ = shutdown_tx.send(true);
    // Let the pass loop observe shutdown before we tear down UI I/O.
    tokio::task::yield_now().await;

    drop(tx);
    // Never block the tokio runtime on join — that prevents pass_loop from exiting.
    drop(input_thread);
    terminal.restore().ok();

    join_pass_loop_after_shutdown(pass_handle).await
}

fn apply_coalesced(bridge: &TuiBridge, app: &mut App, needs_redraw: &mut bool) {
    for event in bridge.drain_coalesced_metrics() {
        apply_event(app, event);
        *needs_redraw = true;
    }
}

fn wants_immediate_draw(event: &UiEvent) -> bool {
    match event {
        // Input thread already drops release/repeat/mouse/focus; treat remaining as interactive.
        UiEvent::Input(ev) => ev.is_key_press() || ev.is_resize() || ev.is_paste(),
        _ => false,
    }
}

fn draw_frame(terminal: &mut TerminalGuard, app: &App) -> anyhow::Result<()> {
    terminal
        .terminal()
        .draw(|frame| super::widgets::render(frame, app))?;
    Ok(())
}

fn draw_frame_blocking(terminal: &mut TerminalGuard, app: &App) -> anyhow::Result<()> {
    tokio::task::block_in_place(|| draw_frame(terminal, app))
}
