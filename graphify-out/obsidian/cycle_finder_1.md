---
id: src_pipeline_cycle_finder_rs
type: File
source: ./src/pipeline/cycle_finder.rs
community: 8
community_label: prioritize_cycle_start_tokens_from_out_degrees()
---

## Connections

- [[std__collections__HashSet_1]] (imports)
- [[std__sync__Arc_14]] (imports)
- [[std__sync__atomic___AtomicBool_ AtomicU32_ Ordering__1]] (imports)
- [[std__time___Duration_ Instant__6]] (imports)
- [[rayon__prelude____1]] (imports)
- [[crate__core__types___CycleEdges_ Edge_ FoundCycle_ TokenIndex__0]] (imports)
- [[crate__pipeline__arena__StateArena_25]] (imports)
- [[crate__pipeline__cycle_filter__dedupe_cycles_by_fingerprint]] (imports)
- [[crate__pipeline__types____    CycleSearchPass_ GraphEdge_ RoutingGraph_ compare_cycle_score_ route_fingerprint___]] (imports)
- [[pub use crate__pipeline__spot_price__hop_penalty]] (imports)
- [[clamp_fee_bps__]] (defines)
- [[edges_from_path__]] (defines)
- [[prioritize_cycle_start_tokens__]] (defines)
- [[prioritize_cycle_start_tokens_from_out_degrees__]] (defines)
- [[ActiveGraph]] (defines)
- [[prepare_active_graph__]] (defines)
- [[Collector]] (defines)
- [[max_pool_index__]] (defines)
- [[collect_cycles_dfs__]] (defines)
- [[SharedCycleBudget]] (defines)
- [[collect_cycles_dfs_single_start__]] (defines)
- [[collect_cycles_dfs_parallel__]] (defines)
- [[CycleBudget]] (defines)
- [[find_cycles__]] (defines)
- [[find_cycles_multi_pass__]] (defines)
- [[find_cycles_multi_pass_arc__]] (defines)
- [[default_hop_quotas__]] (defines)
- [[apply_hop_stratified_cap__]] (defines)
- [[apply_hop_stratified_cap_with_quotas__]] (defines)
- [[super____38]] (imports)
- [[crate__core__types___PoolIndex_ PoolState_ ProtocolType_ V2PoolState_]] (imports)
- [[crate__pipeline__graph___build_graph_ pool_meta_from_pair__0]] (imports)
- [[alloy__primitives__Address_21]] (imports)
- [[ruint__aliases__U256_35]] (imports)
- [[finds_triangle_cycle__]] (defines)
- [[parallel_dfs_finds_cycles_on_hub_graph__]] (defines)
- [[hop_stratified_cap_limits_output__]] (defines)
