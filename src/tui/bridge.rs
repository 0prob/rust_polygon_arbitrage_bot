use std::sync::Arc;

use tokio::sync::{
    mpsc::{Receiver, Sender, channel},
    watch,
};

use crate::orchestrator::hf::HfTickResult;
use crate::orchestrator::ui_hook::PipelineUiHook;
use crate::services::execution::service::ExecutionOutcome;

use super::app::DashboardSnapshot;
use super::events::UiEvent;

#[derive(Clone)]
pub struct TuiBridge {
    tx: Sender<UiEvent>,
    snapshot_tx: watch::Sender<Option<Arc<DashboardSnapshot>>>,
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
        (Self { tx, snapshot_tx }, rx, snapshot_rx)
    }

    #[must_use]
    pub fn hook(&self) -> SharedTuiHook {
        Arc::new(TuiBridgeHook {
            tx: self.tx.clone(),
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
}

pub type SharedTuiHook = Arc<dyn PipelineUiHook>;

pub struct TuiBridgeHook {
    tx: Sender<UiEvent>,
}

impl PipelineUiHook for TuiBridgeHook {
    fn on_lf_complete(&self, cycles: usize, search_ms: u64, discoveries: usize) {
        let _ = self.tx.try_send(UiEvent::LfTick {
            search_ms,
            discoveries,
            cycles,
        });
    }

    fn on_hf_tick(&self, result: &HfTickResult, cycles_considered: usize) {
        let _ = self.tx.try_send(UiEvent::HfTick {
            cycles_considered,
            profitable_count: result.profitable_count,
            best_profit_wei: result.best_profit.to_string(),
            elapsed_ms: result.elapsed_ms,
        });
    }

    fn on_gas_update(&self, gwei: f64) {
        let _ = self.tx.try_send(UiEvent::GasUpdate { gwei });
    }

    fn on_execution_outcome(&self, outcome: &ExecutionOutcome, route_fingerprint: u64) {
        let _ = self.tx.try_send(UiEvent::ExecutionOutcome {
            outcome: outcome.clone(),
            route_fingerprint,
        });
    }
}

pub fn publish_snapshot(
    snapshot_tx: &watch::Sender<Option<Arc<DashboardSnapshot>>>,
    snapshot: DashboardSnapshot,
) {
    let _ = snapshot_tx.send(Some(Arc::new(snapshot)));
}
