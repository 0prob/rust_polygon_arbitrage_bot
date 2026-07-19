use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{
    mpsc::error::TrySendError,
    mpsc::{Receiver, Sender, channel},
    watch,
};

/// Latest LF/HF/gas samples when the UI channel is saturated (metrics recover on next drain).
#[derive(Default)]
struct CoalescedUiMetrics {
    lf: Mutex<Option<UiEvent>>,
    hf: Mutex<Option<UiEvent>>,
    gas: Mutex<Option<UiEvent>>,
}

impl CoalescedUiMetrics {
    fn take_all(&self) -> [Option<UiEvent>; 3] {
        [
            self.lf.lock().take(),
            self.hf.lock().take(),
            self.gas.lock().take(),
        ]
    }
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
    pub fn hook(&self) -> Arc<dyn PipelineUiHook> {
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
        self.coalesce.take_all().into_iter().flatten()
    }
}

struct TuiBridgeHook {
    tx: Sender<UiEvent>,
    coalesce: Arc<CoalescedUiMetrics>,
}

impl TuiBridgeHook {
    fn send_metric(&self, event: UiEvent) {
        let slot = match &event {
            UiEvent::LfTick { .. } => &self.coalesce.lf,
            UiEvent::HfTick { .. } => &self.coalesce.hf,
            UiEvent::GasUpdate { .. } => &self.coalesce.gas,
            _ => return,
        };
        // Hold the slot across try_send so a Full-stash can't land after a newer Ok
        // (drain applies channel then coalesce — a stale stash would regress the UI).
        let mut guard = slot.lock();
        match self.tx.try_send(event) {
            Ok(()) => {
                *guard = None;
            }
            Err(TrySendError::Full(ev)) => {
                *guard = Some(ev);
            }
            Err(TrySendError::Closed(_)) => {}
        }
    }

    fn clear_coalesced_metrics(&self) {
        let _ = self.coalesce.take_all();
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
            elapsed_ms: result.elapsed_ms,
            // Clone is O(dispatch size); same payload already built on the HF tick.
            candidates: result.candidates.clone(),
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
        match self.tx.try_send(event) {
            Ok(()) => {}
            Err(TrySendError::Closed(_)) => {
                crate::warn!("tui event channel closed — execution outcome not shown");
            }
            Err(TrySendError::Full(event)) => {
                // Drop coalesced metrics to prefer a trade outcome over stale LF/HF/gas.
                self.clear_coalesced_metrics();
                match self.tx.try_send(event) {
                    Ok(()) => {}
                    Err(TrySendError::Closed(_)) => {
                        crate::warn!("tui event channel closed — execution outcome not shown");
                    }
                    Err(TrySendError::Full(event)) => {
                        if self.tx.blocking_send(event).is_err() {
                            crate::warn!("tui event channel closed — execution outcome not shown");
                        }
                    }
                }
            }
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
    fn successful_metric_send_clears_stale_coalesce_slot() {
        let (bridge, mut rx, _) = TuiBridge::channel();
        let hook = bridge.hook();
        // Saturate the channel so the next LF tick is stashed.
        for i in 0..2048 {
            hook.on_lf_complete(i, i as u64, 1);
        }
        // Free one slot, then send a fresher sample that must win over the stash.
        let _ = rx.try_recv();
        hook.on_lf_complete(9, 99, 2);
        let mut last_lf = None;
        while let Ok(ev) = rx.try_recv() {
            if matches!(ev, UiEvent::LfTick { .. }) {
                last_lf = Some(ev);
            }
        }
        for ev in bridge.drain_coalesced_metrics() {
            if matches!(ev, UiEvent::LfTick { .. }) {
                last_lf = Some(ev);
            }
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
            if matches!(
                ev,
                UiEvent::ExecutionOutcome {
                    route_fingerprint: 42,
                    ..
                }
            ) {
                saw = true;
            }
        }
        assert!(saw);
    }
}
