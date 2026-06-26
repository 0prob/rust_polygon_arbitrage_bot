---
id: src_orchestrator_hf_rs
type: File
source: ./src/orchestrator/hf.rs
community: 5
community_label: run_hf_tick()
---

## Connections

- [[std__sync__Arc_11]] (imports)
- [[parking_lot__Mutex as ParkingMutex]] (imports)
- [[ruint__aliases__U256_32]] (imports)
- [[tokio__sync___Mutex_ watch_]] (imports)
- [[tracing___debug_ info_ instrument_ warn_]] (imports)
- [[crate__config__AppConfig_4]] (imports)
- [[crate__config__WalletSecrets_1]] (imports)
- [[crate__infra__hypersync__HyperSyncService_3]] (imports)
- [[crate__infra__metrics__PipelineMetrics_3]] (imports)
- [[crate__infra__rpc__RpcPool_4]] (imports)
- [[crate__infra__tracing_util___pool_addrs_csv_ start_token_addr_]] (imports)
- [[crate__orchestrator__dispatch_queue____    PendingDispatch_ queue_pending_dispatch_ take_pending_dispatch___]] (imports)
- [[crate__orchestrator__hf_eval___HfEvalInputOwned_ evaluate_cycles_parallel_async_]] (imports)
- [[crate__orchestrator__hf_execute__dispatch_profitable_candidates]] (imports)
- [[crate__orchestrator__ui_hook__SharedUiHook_1]] (imports)
- [[crate__pipeline__spot_price___SpotTable_ rescore_cycles_with_table_and_gas_]] (imports)
- [[crate__pipeline__types___compare_cycle_score_ route_fingerprint as fp_]] (imports)
- [[crate__services__execution____    ExecutionService_ GasOracle_ OpportunityRecord_ evaluated_from_sim__    flash_policy___hf_eval_flash_source_ parse_flash_policy___    log_opportunity_evaluated___]] (imports)
- [[crate__services__hf_snapshot__SnapshotStore_2]] (imports)
- [[crate__services__partial_cache__PartialPoolCache]] (imports)
- [[crate__services__state_cache__StateCache_4]] (imports)
- [[crate__services__state_refresh__StateRefreshService_2]] (imports)
- [[crate__util__now_ms_1]] (imports)
- [[HfContext]] (defines)
- [[HfTickResult]] (defines)
- [[run_hf_tick__]] (defines)
- [[parse_min_profit__]] (defines)
- [[run_dispatch_loop__]] (defines)
