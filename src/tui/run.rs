use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::{mpsc::Receiver, watch};

use crate::orchestrator::{RuntimeContext, run_pass_loop};

use super::app::App;
use super::bridge::TuiBridge;
use super::events::{UiEvent, spawn_input_thread};
use super::terminal::TerminalGuard;
use super::update::apply_event;

const PASS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
/// Caps terminal writes while keeping metric charts visibly responsive.
const REDRAW_INTERVAL: Duration = Duration::from_millis(250);

pub async fn run_tui(
    ctx: Arc<RuntimeContext>,
    bridge: TuiBridge,
    mut rx: Receiver<UiEvent>,
    mut snapshot_rx: watch::Receiver<Option<Arc<super::app::DashboardSnapshot>>>,
) -> anyhow::Result<()> {
    let mut terminal = TerminalGuard::enter().context("failed to initialize terminal")?;
    let mut app = App::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut pass_handle = tokio::spawn(run_pass_loop(Arc::clone(&ctx), shutdown_rx));

    let tx = bridge.sender();
    let input_thread = spawn_input_thread(tx.clone()).context("spawn input thread")?;

    draw_frame(&mut terminal, &app)?;

    let mut redraw = tokio::time::interval(REDRAW_INTERVAL);
    redraw.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut needs_redraw = true;

    loop {
        tokio::select! {
            maybe_event = rx.recv() => {
                let Some(event) = maybe_event else { break; };
                let mut input_event = matches!(event, UiEvent::Input(_));
                apply_event(&mut app, event);
                if app.should_quit {
                    break;
                }
                needs_redraw = true;
                while let Ok(event) = rx.try_recv() {
                    input_event |= matches!(event, UiEvent::Input(_));
                    apply_event(&mut app, event);
                    if app.should_quit {
                        break;
                    }
                    needs_redraw = true;
                }
                if app.should_quit {
                    break;
                }
                if input_event {
                    draw_frame(&mut terminal, &app)?;
                    needs_redraw = false;
                }
            }
            changed = snapshot_rx.changed() => {
                if changed.is_ok()
                    && let Some(snapshot) = snapshot_rx.borrow().as_ref()
                {
                    app.set_snapshot((**snapshot).clone());
                    needs_redraw = true;
                }
            }
            _ = redraw.tick(), if !app.should_quit => {
                // Periodic rendering keeps uptime/snapshot age moving. All
                // non-input events arriving in this window are coalesced into
                // this single terminal write. Only draw if needed.
                if needs_redraw {
                    draw_frame(&mut terminal, &app)?;
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

    match tokio::time::timeout(PASS_SHUTDOWN_TIMEOUT, &mut pass_handle).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => {
            crate::warn!("pass loop failed during shutdown: {e}");
        }
        Ok(Err(e)) => {
            crate::warn!("pass loop panicked during shutdown: {e}");
        }
        Err(_) => {
            pass_handle.abort();
            crate::warn!("pass loop shutdown timed out after {PASS_SHUTDOWN_TIMEOUT:?}");
        }
    }

    Ok(())
}

fn draw_frame(terminal: &mut TerminalGuard, app: &App) -> anyhow::Result<()> {
    terminal
        .terminal()
        .draw(|frame| super::widgets::render(frame, app))?;
    Ok(())
}
