---
id: src_orchestrator_lf_rs
type: File
source: ./src/orchestrator/lf.rs
community: 0
community_label: spawn_lf_background()
---

## Connections

- [[std__sync__Arc_9]] (imports)
- [[std__sync__LazyLock_1]] (imports)
- [[alloy__primitives__Address_18]] (imports)
- [[rustc_hash__FxHashMap_8]] (imports)
- [[parking_lot__Mutex_2]] (imports)
- [[tokio__sync___Mutex as AsyncMutex_ watch_]] (imports)
- [[tokio__time___Duration_ MissedTickBehavior_ interval__0]] (imports)
- [[tracing___debug_ error_ info_ instrument_ warn_]] (imports)
- [[crate__config__AppConfig_3]] (imports)
- [[crate__config__CycleFinderKind]] (imports)
- [[crate__core__types__TokenIndex_2]] (imports)
- [[crate__infra__metrics__PipelineMetrics_1]] (imports)
- [[crate__infra__rpc__RpcPool_2]] (imports)
- [[crate__pipeline__arena__StateArena_19]] (imports)
- [[crate__pipeline__bellman_ford__find_cycles_bellman_ford_multi_pass]] (imports)
- [[crate__pipeline__cycle_finder__find_cycles_multi_pass_arc]] (imports)
- [[crate__pipeline__cycle_search__find_cycles_hybrid_multi_pass]] (imports)
- [[crate__pipeline__graph_cache___GraphCache_ connectivity_fingerprint_]] (imports)
- [[crate__pipeline__johnson__find_cycles_johnson_multi_pass]] (imports)
- [[crate__pipeline__spot_price___finalize_enumerated_cycles_ rescore_cycles_by_spot_price_]] (imports)
- [[crate__pipeline__tick_fetch___collect_v3_pool_addresses_ enrich_v3_ticks_]] (imports)
- [[crate__pipeline__types___CycleSearchPass_ compare_cycle_score_]] (imports)
- [[crate__services__hf_snapshot__SnapshotStore_0]] (imports)
- [[crate__services__oracle__enrich_token_to_matic_rates]] (imports)
- [[crate__services__oracle__price_oracle__PriceOracle_0]] (imports)
- [[crate__orchestrator__ui_hook__SharedUiHook_0]] (imports)
- [[crate__services__partial_cache___PartialPoolCache_ StreamAddressSet_ select_stream_targets_]] (imports)
- [[crate__services__state_cache__StateCache_2]] (imports)
- [[crate__services__state_refresh__StateRefreshService_0]] (imports)
- [[LfCpuWork]] (defines)
- [[LfCpuResult]] (defines)
- [[run_lf_cpu_work__]] (defines)
- [[run_lf_cpu_async__]] (defines)
- [[LfContext]] (defines)
- [[run_lf_tick__]] (defines)
- [[snap_oracle_rates__]] (defines)
- [[spawn_lf_background__]] (defines)
