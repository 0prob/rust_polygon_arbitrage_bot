use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{
    mpsc::{Receiver, Sender, channel},
    mpsc::error::TrySendError,
    watch,
};

/// Latest LF/HF/gas samples when the UI channel is saturated (metrics recover on next drain).
#[derive(Default)]
struct CoalescedUiMetrics {
    lf: Mutex<Option<UiEvent>>,
    hf: Mutex<Option<UiEvent>>,
    gas: Mutex<Option<UiEvent>>,
}

use crate::orchestrator::hf::HfTickResult;
use crate::orchestrator::ui_hook::PipelineUiHook;
use crate::services::execution::service::ExecutionOutcome;

use super::app::DashboardSnapshot;
use super::events::UiEvent;

#[derive(Clone)]
pub struct TuiBridge {
    tx: Sender<UiEvent>,
    snapshot_tx: watch::Sender<Option<Arc<DashboardSnapshot>>>,
    coalesce: Arc<CoalescedUiMetrics>,
}

impl TuiBridge {
    #[must_use]
    pub fn channel() -> (
        Self,
        Receiver<UiEvent>,
        watch::Receiver<Option<Arc<DashboardSnapshot>>>,
    ) {
        let (tx, rx) = channel(1024);
        let (snapshot_tx, snapshot_rx) = watch::channel(None);
        let coalesce = Arc::new(CoalescedUiMetrics::default());
        (
            Self {
                tx,
                snapshot_tx,
                coalesce,
            },
            rx,
            snapshot_rx,
        )
    }

    #[must_use]
    pub fn hook(&self) -> SharedTuiHook {
        Arc::new(TuiBridgeHook {
            tx: self.tx.clone(),
            coalesce: Arc::clone(&self.coalesce),
        })
    }

    #[must_use]
    pub fn sender(&self) -> Sender<UiEvent> {
        self.tx.clone()
    }

    #[must_use]
    pub fn snapshot_sender(&self) -> watch::Sender<Option<Arc<DashboardSnapshot>>> {
        self.snapshot_tx.clone()
    }

    /// Apply coalesced metric ticks after the UI drains the live channel.
    pub fn drain_coalesced_metrics(&self) -> impl Iterator<Item = UiEvent> + '_ {
        [
            self.coalesce.lf.lock().take(),
            self.coalesce.hf.lock().take(),
            self.coalesce.gas.lock().take(),
        ]
        .into_iter()
        .flatten()
    }
}

pub type SharedTuiHook = Arc<dyn PipelineUiHook>;

pub struct TuiBridgeHook {
    tx: Sender<UiEvent>,
    coalesce: Arc<CoalescedUiMetrics>,
}

impl TuiBridgeHook {
    fn stash_metric(&self, event: UiEvent) {
        let slot = match &event {
            UiEvent::LfTick { .. } => &self.coalesce.lf,
            UiEvent::HfTick { .. } => &self.coalesce.hf,
            UiEvent::GasUpdate { .. } => &self.coalesce.gas,
            _ => return,
        };
        *slot.lock() = Some(event);
    }

    fn send_metric(&self, event: UiEvent) {
        match self.tx.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Full(ev)) => self.stash_metric(ev),
            Err(TrySendError::Closed(_)) => {}
        }
    }

    fn clear_coalesced_metrics(&self) {
        self.coalesce.lf.lock().take();
        self.coalesce.hf.lock().take();
        self.coalesce.gas.lock().take();
    }
}

impl PipelineUiHook for TuiBridgeHook {
    fn on_lf_complete(&self, cycles: usize, search_ms: u64, discoveries: usize) {
        self.send_metric(UiEvent::LfTick {
            search_ms,
            discoveries,
            cycles,
        });
    }

    fn on_hf_tick(&self, result: &HfTickResult, cycles_considered: usize) {
        self.send_metric(UiEvent::HfTick {
            cycles_considered,
            profitable_count: result.profitable_count,
            best_profit_wei: result.best_profit.to_string(),
            elapsed_ms: result.elapsed_ms,
        });
    }

    fn on_gas_update(&self, gwei: f64) {
        self.send_metric(UiEvent::GasUpdate { gwei });
    }

    fn on_execution_outcome(&self, outcome: &ExecutionOutcome, route_fingerprint: u64) {
        let event = UiEvent::ExecutionOutcome {
            outcome: outcome.clone(),
            route_fingerprint,
        };
        if self.tx.try_send(event.clone()).is_ok() {
            return;
        }
        self.clear_coalesced_metrics();
        if self.tx.try_send(event.clone()).is_err()
            && self.tx.blocking_send(event).is_err()
        {
            crate::warn!("tui event channel closed — execution outcome not shown");
        }
    }
}

pub fn publish_snapshot(
    snapshot_tx: &watch::Sender<Option<Arc<DashboardSnapshot>>>,
    snapshot: DashboardSnapshot,
) {
    let _ = snapshot_tx.send(Some(Arc::new(snapshot)));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalesced_metrics_keep_latest_only() {
        let (bridge, mut rx, _) = TuiBridge::channel();
        let hook = bridge.hook();
        for i in 0..2048 {
            hook.on_lf_complete(i, i as u64, 1);
        }
        while rx.try_recv().is_ok() {}
        let _ = bridge.drain_coalesced_metrics().count();
        hook.on_lf_complete(9, 99, 2);
        let mut last_lf = None;
        while let Ok(ev) = rx.try_recv() {
            if matches!(ev, UiEvent::LfTick { .. }) {
                last_lf = Some(ev);
            }
        }
        if last_lf.is_none() {
            for ev in bridge.drain_coalesced_metrics() {
                if matches!(ev, UiEvent::LfTick { .. }) {
                    last_lf = Some(ev);
                }
            }
        } else {
            let _ = bridge.drain_coalesced_metrics().count();
        }
        assert!(matches!(
            last_lf,
            Some(UiEvent::LfTick {
                search_ms: 99,
                cycles: 9,
                ..
            })
        ));
    }

    #[test]
    fn execution_outcome_uses_blocking_when_channel_full() {
        let (bridge, mut rx, _) = TuiBridge::channel();
        let hook = bridge.hook();
        for _ in 0..2048 {
            hook.on_gas_update(1.0);
        }
        while rx.try_recv().is_ok() {}
        hook.on_execution_outcome(
            &crate::services::execution::service::ExecutionOutcome::SkippedShutdown,
            42,
        );
        let mut saw = false;
        while let Ok(ev) = rx.try_recv() {
            if matches!(ev, UiEvent::ExecutionOutcome { route_fingerprint: 42, .. }) {
                saw = true;
            }
        }
        assert!(saw);
    }
}
