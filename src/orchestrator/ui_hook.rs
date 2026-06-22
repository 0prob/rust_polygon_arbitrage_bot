use std::sync::Arc;

use crate::core::types::FoundCycle;
use crate::orchestrator::hf::HfTickResult;
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::PoolMeta;

/// Optional UI notifications from LF/HF ticks (no-op when TUI is disabled).
pub trait PipelineUiHook: Send + Sync {
    fn on_lf_complete(
        &self,
        arena: &StateArena,
        cycles: &[FoundCycle],
        pool_metas: &[PoolMeta],
        search_ms: u64,
        discoveries: usize,
    );

    fn on_hf_tick(&self, result: &HfTickResult, cycles_considered: usize);

    fn on_gas_update(&self, gwei: f64);
}

#[derive(Debug, Default)]
pub struct NoopUiHook;

impl PipelineUiHook for NoopUiHook {
    fn on_lf_complete(
        &self,
        _arena: &StateArena,
        _cycles: &[FoundCycle],
        _pool_metas: &[PoolMeta],
        _search_ms: u64,
        _discoveries: usize,
    ) {
    }

    fn on_hf_tick(&self, _result: &HfTickResult, _cycles_considered: usize) {}

    fn on_gas_update(&self, _gwei: f64) {}
}

pub type SharedUiHook = Arc<dyn PipelineUiHook>;

pub fn noop_ui_hook() -> SharedUiHook {
    Arc::new(NoopUiHook)
}
