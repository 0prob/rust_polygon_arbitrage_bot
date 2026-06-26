---
id: src_pipeline_spot_price_rs
type: File
source: ./src/pipeline/spot_price.rs
community: 14
community_label: v2_marginal_spot()
---

## Connections

- [[crate__core__types____    ConcentratedLiquidityPoolState_ Edge_ FoundCycle_ PoolState_ ProtocolType_ TokenIndex___]] (imports)
- [[crate__pipeline__arena__StateArena_28]] (imports)
- [[crate__pipeline__cycle_finder__clamp_fee_bps_0]] (imports)
- [[crate__pipeline__local_sim__simulate_hop_amount_out_1]] (imports)
- [[crate__pipeline__types___GraphEdge_ RoutingGraph_]] (imports)
- [[crate__util__u256_to_f64]] (imports)
- [[ruint__aliases__U256_37]] (imports)
- [[rustc_hash__FxHashMap_10]] (imports)
- [[hop_penalty__]] (defines)
- [[fee_log_weight__]] (defines)
- [[compute_edge_log_weight__]] (defines)
- [[SpotTable]] (defines)
- [[v2_marginal_spot__]] (defines)
- [[cl_marginal_spot__]] (defines)
- [[spot_ratio__]] (defines)
- [[edge_log_weight_from_spot__]] (defines)
- [[compute_spot_price__]] (defines)
- [[compute_edge_log_weight_with_state__]] (defines)
- [[compute_edge_log_weight_with_table__]] (defines)
- [[rescore_cycles_by_spot_price__]] (defines)
- [[gas_log_penalty_for_cycle__]] (defines)
- [[rescore_cycles_with_table__]] (defines)
- [[rescore_cycles_with_table_and_gas__]] (defines)
- [[reweight_graph_with_spot__]] (defines)
- [[finalize_enumerated_cycles__]] (defines)
- [[super____41]] (imports)
- [[crate__core__types__V2PoolState_2]] (imports)
- [[crate__pipeline__graph___build_graph_ pool_meta_from_pair__1]] (imports)
- [[alloy__primitives__Address_25]] (imports)
- [[rescored_v2_cycle_has_negative_log_weight__]] (defines)
- [[spot_table_reuses_v2_entries__]] (defines)
