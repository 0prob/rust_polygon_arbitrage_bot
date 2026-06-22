use std::sync::Arc;

use crate::orchestrator::hf::HfTickResult;
use crate::orchestrator::ui_hook::PipelineUiHook;
use crate::tui::UiBridge;

pub struct TuiUiHook {
    bridge: UiBridge,
}

impl TuiUiHook {
    pub fn new(bridge: UiBridge) -> Self {
        Self { bridge }
    }
}

impl PipelineUiHook for TuiUiHook {
    fn on_lf_complete(
        &self,
        arena: &crate::pipeline::arena::StateArena,
        cycles: &[crate::core::types::FoundCycle],
        pool_metas: &[crate::pipeline::types::PoolMeta],
        search_ms: u64,
        discoveries: usize,
    ) {
        self.bridge
            .notify_lf_complete(arena, cycles, pool_metas, search_ms, discoveries);
    }

    fn on_hf_tick(&self, result: &HfTickResult, cycles_considered: usize) {
        self.bridge.notify_hf_tick(result, cycles_considered);
    }

    fn on_gas_update(&self, gwei: f64) {
        self.bridge
            .try_send(crate::tui::update::UiUpdate::GasUpdate { gwei });
    }
}
