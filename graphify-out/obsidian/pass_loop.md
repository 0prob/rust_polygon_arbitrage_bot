---
id: src_orchestrator_pass_loop_rs
type: File
source: ./src/orchestrator/pass_loop.rs
community: 17
community_label: pass_loop
---

## Connections

- [[std__sync__Arc_10]] (imports)
- [[tokio__sync___Mutex_ Semaphore_ watch_]] (imports)
- [[tokio__time___Duration_ MissedTickBehavior_ interval__1]] (imports)
- [[tracing___Instrument_ debug_ error_ info_ warn_]] (imports)
- [[crate__config___AppConfig_ WalletSecrets_]] (imports)
- [[crate__infra__hypersync__HyperSyncService_2]] (imports)
- [[crate__error__ArbError_2]] (imports)
- [[crate__infra__metrics__PipelineMetrics_2]] (imports)
- [[crate__infra__rpc__RpcPool_3]] (imports)
- [[crate__infra__wss_feed__spawn_pool_log_feed]] (imports)
- [[crate__orchestrator__hf___HfContext_ run_hf_tick_]] (imports)
- [[crate__orchestrator__lf___LfContext_ spawn_lf_background_]] (imports)
- [[crate__orchestrator__ui_hook___SharedUiHook_ noop_ui_hook_]] (imports)
- [[crate__pipeline__graph_cache___set_graph_rebuild_interval_ GraphCache_]] (imports)
- [[crate__services__execution__ExecutionService]] (imports)
- [[crate__services__execution__GasOracle]] (imports)
- [[crate__services__hf_snapshot__SnapshotStore_1]] (imports)
- [[crate__services__oracle__price_oracle__PriceOracle_1]] (imports)
- [[crate__services__partial_cache___PartialPoolCache_ StreamAddressSet_]] (imports)
- [[crate__services__state_cache__StateCache_3]] (imports)
- [[crate__services__state_refresh__StateRefreshService_1]] (imports)
- [[RuntimeContext]] (defines)
- [[run_hf_tick_logged__]] (defines)
- [[run_pass_loop__]] (defines)
- [[spawn_operator_balance_monitor__]] (defines)
