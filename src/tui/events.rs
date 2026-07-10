use std::time::Duration;

use crossterm::event::{self, Event, KeyEventKind};
use tokio::sync::mpsc::Sender;

use crate::services::execution::service::ExecutionOutcome;

use super::app::Severity;

/// How long the input thread blocks in [`event::poll`] between channel-close checks.
const INPUT_POLL_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Debug, Clone)]
pub enum UiEvent {
    Input(Event),
    LfTick {
        search_ms: u64,
        discoveries: usize,
        cycles: usize,
    },
    HfTick {
        cycles_considered: usize,
        profitable_count: usize,
        best_profit_wei: String,
        elapsed_ms: u64,
    },
    GasUpdate {
        gwei: f64,
    },
    ExecutionOutcome {
        outcome: ExecutionOutcome,
        route_fingerprint: u64,
    },
    Message {
        severity: Severity,
        message: String,
    },
    Shutdown,
}

pub fn spawn_input_thread(tx: Sender<UiEvent>) -> anyhow::Result<std::thread::JoinHandle<()>> {
    use anyhow::Context;

    std::thread::Builder::new()
        .name("tui-input".into())
        .spawn(move || input_thread_main(tx))
        .context("failed to spawn tui input thread")
}

fn input_thread_main(tx: Sender<UiEvent>) {
    loop {
        if tx.is_closed() {
            break;
        }
        match event::poll(INPUT_POLL_TIMEOUT) {
            Ok(true) => {
                if !drain_available(&tx) {
                    break;
                }
            }
            Ok(false) => {}
            Err(_) => break,
        }
    }
}

/// Read every buffered event after [`event::poll`] returned true.
///
/// Crossterm may coalesce multiple events (e.g. resize); draining avoids stale input.
fn drain_available(tx: &Sender<UiEvent>) -> bool {
    loop {
        let Ok(ev) = event::read() else {
            return true;
        };
        if should_forward(&ev) && tx.try_send(UiEvent::Input(ev)).is_err() {
            return false;
        }
        if !event::poll(Duration::ZERO).unwrap_or(false) {
            return true;
        }
    }
}

fn should_forward(ev: &Event) -> bool {
    match ev {
        Event::Key(key) => key.kind == KeyEventKind::Press,
        Event::Resize(_, _) | Event::Paste(_) => true,
        Event::FocusGained | Event::FocusLost | Event::Mouse(_) => false,
    }
}
