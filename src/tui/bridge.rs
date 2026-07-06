use std::sync::Arc;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::orchestrator::hf::HfTickResult;
use crate::orchestrator::ui_hook::PipelineUiHook;
use crate::services::execution::service::ExecutionOutcome;

use super::app::DashboardSnapshot;
use super::events::UiEvent;

#[derive(Clone)]
pub struct TuiBridge {
    tx: UnboundedSender<UiEvent>,
}

impl TuiBridge {
    #[must_use]
    pub fn channel() -> (Self, UnboundedReceiver<UiEvent>) {
        let (tx, rx) = unbounded_channel();
        (Self { tx }, rx)
    }

    #[must_use]
    pub fn hook(&self) -> SharedTuiHook {
        Arc::new(TuiBridgeHook {
            tx: self.tx.clone(),
        })
    }

    #[must_use]
    pub fn sender(&self) -> UnboundedSender<UiEvent> {
        self.tx.clone()
    }
}

pub type SharedTuiHook = Arc<dyn PipelineUiHook>;

pub struct TuiBridgeHook {
    tx: UnboundedSender<UiEvent>,
}

impl PipelineUiHook for TuiBridgeHook {
    fn on_lf_complete(&self, cycles: usize, search_ms: u64, discoveries: usize) {
        let _ = self.tx.send(UiEvent::LfTick {
            search_ms,
            discoveries,
            cycles,
        });
    }

    fn on_hf_tick(&self, result: &HfTickResult, cycles_considered: usize) {
        let _ = self.tx.send(UiEvent::HfTick {
            cycles_considered,
            profitable_count: result.profitable_count,
            best_profit_wei: result.best_profit.to_string(),
            elapsed_ms: result.elapsed_ms,
        });
    }

    fn on_gas_update(&self, gwei: f64) {
        let _ = self.tx.send(UiEvent::GasUpdate { gwei });
    }

    fn on_execution_outcome(&self, outcome: &ExecutionOutcome, route_fingerprint: u64) {
        let _ = self.tx.send(UiEvent::ExecutionOutcome {
            outcome: outcome.clone(),
            route_fingerprint,
        });
    }
}

pub fn publish_snapshot(tx: &UnboundedSender<UiEvent>, snapshot: DashboardSnapshot) {
    let _ = tx.send(UiEvent::Snapshot(Box::new(snapshot)));
}


