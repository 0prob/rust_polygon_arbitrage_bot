use std::sync::Arc;

use crate::orchestrator::hf::HfTickResult;
use crate::services::execution::service::ExecutionOutcome;

/// ponytail: trait+Arc<dyn> kept over callbacks — changing would touch all TUI code.
pub trait PipelineUiHook: Send + Sync {
    fn on_lf_complete(&self, _cycles: usize, _search_ms: u64, _discoveries: usize) {}

    fn on_hf_tick(&self, _result: &HfTickResult, _cycles_considered: usize) {}

    fn on_gas_update(&self, _gwei: f64) {}

    fn on_execution_outcome(&self, _outcome: &ExecutionOutcome, _route_fingerprint: u64) {}
}

impl PipelineUiHook for () {}

pub type SharedUiHook = Arc<dyn PipelineUiHook>;
