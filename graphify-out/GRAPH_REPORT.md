# 📊 Graph Analysis Report

**Root:** `.`

## Summary

| Metric | Value |
|--------|-------|
| Nodes | 2333 |
| Edges | 2949 |
| Communities | 201 |
| Hyperedges | 0 |

### Confidence Breakdown

| Level | Count | Percentage |
|-------|-------|------------|
| EXTRACTED | 2198 | 74.5% |
| INFERRED | 751 | 25.5% |
| AMBIGUOUS | 0 | 0.0% |

## 🌟 God Nodes (Most Connected)

| Node | Degree | Community |
|------|--------|-----------|
| .::mod | 60 | 2 |
| flash_liquidity | 43 | 3 |
| cycle_finder | 37 | 8 |
| lf | 37 | 0 |
| service | 32 | 4 |
| profit | 31 | 12 |
| spot_price | 31 | 14 |
| hf | 28 | 5 |
| ternary | 27 | 6 |
| price_oracle | 27 | 10 |

## 🔮 Surprising Connections

- **src_tui_route_viz_rs_cycle_to_ui_opportunity** → **src_tui_route_viz_rs_cycle_has_long_tail** (calls)
- **src_tui_route_viz_rs_cycle_to_ui_opportunity** → **src_tui_route_viz_rs_liquidity_risk_score** (calls)
- **src_core_math_uniswap_v2_rs_get_amount_out** → **src_core_math_uniswap_v2_rs_fits_u128** (calls)
- **src_core_math_uniswap_v2_rs_get_amount_out** → **src_core_math_uniswap_v2_rs_to_u128** (calls)
- **src_core_math_uniswap_v2_rs_simulate_v2_swap** → **src_core_math_uniswap_v2_rs_resolve_v2_fee_with_edge** (calls)

## 🏘️ Communities

### Community 0 — spawn_lf_background() (38 nodes, cohesion: 0.06)

- lf
- alloy::primitives::Address
- crate::config::AppConfig
- crate::config::CycleFinderKind
- crate::core::types::TokenIndex
- crate::infra::metrics::PipelineMetrics
- crate::infra::rpc::RpcPool
- crate::orchestrator::ui_hook::SharedUiHook
- crate::pipeline::arena::StateArena
- crate::pipeline::bellman_ford::find_cycles_bellman_ford_multi_pass
- crate::pipeline::cycle_finder::find_cycles_multi_pass_arc
- crate::pipeline::cycle_search::find_cycles_hybrid_multi_pass
- crate::pipeline::graph_cache::{GraphCache, connectivity_fingerprint}
- crate::pipeline::johnson::find_cycles_johnson_multi_pass
- crate::pipeline::spot_price::{finalize_enumerated_cycles, rescore_cycles_by_spot_price}
- crate::pipeline::tick_fetch::{collect_v3_pool_addresses, enrich_v3_ticks}
- crate::pipeline::types::{CycleSearchPass, compare_cycle_score}
- crate::services::hf_snapshot::SnapshotStore
- crate::services::oracle::enrich_token_to_matic_rates
- crate::services::oracle::price_oracle::PriceOracle
- _…and 18 more_

### Community 1 — WoofiEncoder (35 nodes, cohesion: 0.07)

- mod
- BalancerEncoder
- .encode_hop()
- CurveEncoder
- .encode_hop()
- DodoEncoder
- .encode_hop()
- encoder_for_protocol()
- alloy::primitives::Address
- crate::abis::ExecutorCall
- crate::core::types::ProtocolType
- crate::pipeline::arena::StateArena
- crate::services::execution::profit::slippage_adjusted
- crate::services::execution::quote::is_kyber_protocol
- pub use balancer::encode_balancer_hop
- pub use curve::encode_curve_hop
- pub use dodo::encode_dodo_hop
- pub use kyber::encode_kyber_hop
- pub use shared::{
    curve_uses_receiver, derive_balancer_pool_id, resolve_balancer_pool_id, to_v3_state,
}
- pub use v2::encode_v2_hop
- _…and 15 more_

### Community 2 — RpcConfig (32 nodes, cohesion: 0.09)

- mod
- .load()
- apply_flat_env_aliases()
- default_batch_size()
- default_batch_wait_ms()
- default_circuit_breaker_cooldown_secs()
- default_deadline_secs()
- default_execution_mode()
- default_flash_loan_source()
- default_max_flash_loan_usd()
- default_max_global_consecutive_failures()
- default_min_operator_matic_wei()
- default_min_profit_matic_wei()
- default_profit_priority_fee_alpha_bps()
- default_profit_safety_multiplier_bps()
- default_receipt_poll_ms()
- default_receipt_timeout_ms()
- default_request_timeout_ms()
- default_slippage_bps()
- env_var()
- _…and 12 more_

### Community 3 — TokenFlashLiquidity (31 nodes, cohesion: 0.07)

- flash_liquidity
- CachedLiquidity
- FlashPlan
- FlashPlanAction
- alloy::network::Ethereum
- alloy::primitives::Address
- alloy::providers::Provider
- alloy::sol_types::SolCall
- crate::abis::{IAaveV3Pool, IERC20Metadata}
- crate::core::constants::{AAVE_V3_POOL, BALANCER_VAULT}
- crate::core::types::{
    EvaluatedRoute, FlashLoanSource, FoundCycle, PoolState, ProfitAssessment, ProtocolType,
    TokenIndex,
}
- crate::pipeline::arena::StateArena
- crate::pipeline::local_sim::simulate_route_detailed
- crate::pipeline::multicall::{MulticallItem, encode_call, execute_multicall}
- crate::pipeline::ternary::optimize_cycle
- crate::services::execution::flash_policy::FlashLoanPolicy
- crate::services::execution::profit::{
    ProfitEvalContext, ProfitThresholds, RouteProfitParams, assess_profit, build_assess_input,
}
- parking_lot::RwLock
- ruint::aliases::U256 as RU256
- rustc_hash::FxHashMap
- _…and 11 more_

### Community 4 — ExecutionOutcome (31 nodes, cohesion: 0.06)

- service
- ExecutionOutcome
- alloy::network::Ethereum
- alloy::primitives::{Address, U256}
- alloy::providers::Provider
- crate::config::AppConfig
- crate::config::WalletSecrets
- crate::infra::hypersync::HyperSyncService
- crate::infra::metrics::PipelineMetrics
- crate::infra::rpc::RpcPool
- crate::infra::tracing_util::{record_candidate, record_gas_fees, record_receipt, record_tx}
- crate::services::execution::candidate::CandidateExecution
- crate::services::execution::circuit_breaker::CircuitBreaker
- crate::services::execution::dryrun::dry_run_candidate
- crate::services::execution::flash_liquidity::FlashLiquidityCache
- crate::services::execution::gas::{gas_drift_bps, pick_live_gas_limit}
- crate::services::execution::gas_oracle::GasOracle
- crate::services::execution::nonce::NonceManager
- crate::services::execution::opportunity_log::log_opportunity_outcome
- crate::services::execution::profit::{AssessProfitInput, assess_profit}
- _…and 11 more_

### Community 5 — run_hf_tick() (29 nodes, cohesion: 0.07)

- hf
- HfContext
- HfTickResult
- crate::config::AppConfig
- crate::config::WalletSecrets
- crate::infra::hypersync::HyperSyncService
- crate::infra::metrics::PipelineMetrics
- crate::infra::rpc::RpcPool
- crate::infra::tracing_util::{pool_addrs_csv, start_token_addr}
- crate::orchestrator::dispatch_queue::{
    PendingDispatch, queue_pending_dispatch, take_pending_dispatch,
}
- crate::orchestrator::hf_eval::{HfEvalInputOwned, evaluate_cycles_parallel_async}
- crate::orchestrator::hf_execute::dispatch_profitable_candidates
- crate::orchestrator::ui_hook::SharedUiHook
- crate::pipeline::spot_price::{SpotTable, rescore_cycles_with_table_and_gas}
- crate::pipeline::types::{compare_cycle_score, route_fingerprint as fp}
- crate::services::execution::{
    ExecutionService, GasOracle, OpportunityRecord, evaluated_from_sim,
    flash_policy::{hf_eval_flash_source, parse_flash_policy},
    log_opportunity_evaluated,
}
- crate::services::hf_snapshot::SnapshotStore
- crate::services::partial_cache::PartialPoolCache
- crate::services::state_cache::StateCache
- crate::services::state_refresh::StateRefreshService
- _…and 9 more_

### Community 6 — u256_to_i128() (28 nodes, cohesion: 0.10)

- ternary
- brent_finds_peak_on_quadratic()
- get_dynamic_search_bounds()
- hop_capacity()
- alloy::primitives::Address
- crate::core::math::dodo::estimate_dodo_hop_capacity
- crate::core::types::{Edge, FoundCycle, PoolState, ProtocolType, TokenIndex}
- crate::core::types::{PoolState, ProtocolType, V2PoolState}
- crate::pipeline::arena::StateArena
- crate::pipeline::{cycle_finder, local_sim}
- crate::pipeline::graph::{build_graph, pool_meta_from_pair}
- crate::pipeline::local_sim::simulate_route_minimal
- crate::pipeline::types::OptimizationResult
- crate::services::execution::profit::{ProfitEvalContext, net_profit_after_gas_from_sim}
- ruint::aliases::U256
- rustc_hash::FxHashMap
- std::collections::HashMap
- super::*
- tracing::instrument
- liquidity_cap_bounds_respect_cap_when_low_equals_cap()
- _…and 8 more_

### Community 7 — simulate_route_minimal() (26 nodes, cohesion: 0.10)

- local_sim
- estimate_hop_gas()
- estimate_route_gas()
- finalize_route_gas()
- HopResult
- alloy::primitives::Address
- crate::core::math::balancer::simulate_balancer_swap
- crate::core::math::curve::get_curve_stable_amount_out
- crate::core::math::dodo::simulate_dodo_swap
- crate::core::math::uniswap_v2::simulate_v2_swap
- crate::core::math::uniswap_v3::simulate_v3_swap
- crate::core::math::woofi::simulate_woofi_swap
- crate::core::types::{Edge, PoolState, ProtocolType, RouteSimulationResult}
- crate::core::types::V2PoolState
- crate::pipeline::arena::StateArena
- crate::pipeline::types::MinimalSimResult
- crate::services::execution::gas::estimate_route_gas_from_hops
- ruint::aliases::U256
- smallvec::SmallVec
- super::*
- _…and 6 more_

### Community 8 — prioritize_cycle_start_tokens_from_out_degrees() (25 nodes, cohesion: 0.10)

- cycle_finder
- ActiveGraph
- apply_hop_stratified_cap()
- apply_hop_stratified_cap_with_quotas()
- Collector
- default_hop_quotas()
- hop_stratified_cap_limits_output()
- alloy::primitives::Address
- crate::core::types::{CycleEdges, Edge, FoundCycle, TokenIndex}
- crate::core::types::{PoolIndex, PoolState, ProtocolType, V2PoolState}
- crate::pipeline::arena::StateArena
- crate::pipeline::cycle_filter::dedupe_cycles_by_fingerprint
- crate::pipeline::graph::{build_graph, pool_meta_from_pair}
- crate::pipeline::types::{
    CycleSearchPass, GraphEdge, RoutingGraph, compare_cycle_score, route_fingerprint,
}
- pub use crate::pipeline::spot_price::hop_penalty
- rayon::prelude::*
- ruint::aliases::U256
- std::collections::HashSet
- std::sync::Arc
- std::sync::atomic::{AtomicBool, AtomicU32, Ordering}
- _…and 5 more_

### Community 9 — WoofiPoolState (25 nodes, cohesion: 0.08)

- types
- BalancerPoolKind
- BalancerPoolState
- ConcentratedLiquidityPoolState
- CurvePoolState
- DodoPoolState
- Edge
- EvaluatedRoute
- FlashLoanSource
- FoundCycle
- crate::core::constants::MIN_HOP_TOKEN_BALANCE
- ruint::aliases::U256
- serde::{Deserialize, Serialize}
- smallvec::SmallVec
- PoolIndex
- PoolState
- .is_tradable()
- ProfitAssessment
- ProtocolType
- RouteSimulationResult
- _…and 5 more_

### Community 10 — TokenFeed (25 nodes, cohesion: 0.09)

- price_oracle
- bootstrap_matic_rate_per_unit()
- chainlink_integer_rate_matches_float_path()
- chainlink_usd_to_matic_rate_per_unit()
- alloy::network::Ethereum
- alloy::primitives::{Address, I256, address}
- alloy::primitives::I256
- alloy::providers::Provider
- alloy::sol_types::SolCall
- crate::abis::IChainlinkAggregator
- crate::core::constants::{RATE_PRECISION, WMATIC}
- crate::error::ArbError
- crate::pipeline::multicall::{MulticallItem, encode_call, execute_multicall}
- reqwest::Client
- ruint::aliases::U256
- rustc_hash::FxHashMap
- std::collections::HashMap
- std::time::{Duration, Instant}
- super::*
- pow10_f64()
- _…and 5 more_

### Community 11 — to_v3_state() (24 nodes, cohesion: 0.11)

- shared
- curve_uses_receiver()
- derive_balancer_pool_id()
- alloy::primitives::{Address, FixedBytes}
- alloy::primitives::U256
- crate::core::types::{PoolState, V3PoolState}
- crate::core::types::V4PoolState
- super::*
- resolve_balancer_pool_id()
- test_curve_uses_receiver_detects_stableswap_ng_lowercase()
- test_curve_uses_receiver_detects_stableswap_ng_mixed_case()
- test_curve_uses_receiver_detects_stableswap_ng_uppercase()
- test_curve_uses_receiver_rejects_none()
- test_curve_uses_receiver_rejects_other_labels()
- test_curve_uses_receiver_rejects_standard_stableswap()
- test_derive_balancer_pool_id_different_addresses()
- test_derive_balancer_pool_id_encodes_address()
- test_resolve_balancer_pool_id_derives_when_none()
- test_resolve_balancer_pool_id_prefers_explicit()
- test_to_v3_state_rejects_curve()
- _…and 4 more_

### Community 12 — slippage_adjusted() (24 nodes, cohesion: 0.09)

- profit
- AssessProfitInput
- build_assess_input()
- alloy::primitives::Address
- crate::core::constants::BPS_SCALE
- crate::core::types::{FlashLoanSource, ProfitAssessment, TokenIndex}
- crate::pipeline::arena::StateArena
- crate::pipeline::types::MinimalSimResult
- crate::services::oracle::resolve_token_to_matic_rate
- pub use crate::core::constants::{MIN_TOKEN_TO_MATIC_RATE, RATE_PRECISION}
- ruint::aliases::U256
- rustc_hash::FxHashMap
- std::collections::HashMap
- super::*
- tracing::instrument
- on_chain_min_profit()
- on_chain_min_profit_95_percent()
- on_chain_min_profit_for_route()
- ProfitEvalContext
- .for_cycle()
- _…and 4 more_

### Community 13 — HfEvalResult (23 nodes, cohesion: 0.10)

- hf_eval
- best_assessment_for_cycle()
- build_hf_eval_pool()
- evaluate_cycles_parallel()
- evaluate_cycles_parallel_async()
- evaluate_one()
- HfEvalInput
- HfEvalInputOwned
- .from_input()
- HfEvalResult
- alloy::primitives::Address
- crate::core::types::{
    FlashLoanSource, FoundCycle, ProfitAssessment, RouteSimulationResult, TokenIndex,
}
- crate::pipeline::arena::StateArena
- crate::pipeline::local_sim::simulate_route_detailed
- crate::pipeline::ternary::optimize_cycle
- crate::pipeline::types::OptimizationResult
- crate::services::execution::impact_slippage::{depth_impact_slippage_bps, effective_slippage_bps}
- crate::services::execution::profit::{
    ProfitEvalContext, ProfitThresholds, RouteProfitParams, assess_profit, build_assess_input,
}
- rayon::prelude::*
- ruint::aliases::U256
- _…and 3 more_

### Community 14 — v2_marginal_spot() (22 nodes, cohesion: 0.13)

- spot_price
- cl_marginal_spot()
- compute_edge_log_weight_with_state()
- compute_spot_price()
- finalize_enumerated_cycles()
- alloy::primitives::Address
- crate::core::types::{
    ConcentratedLiquidityPoolState, Edge, FoundCycle, PoolState, ProtocolType, TokenIndex,
}
- crate::core::types::V2PoolState
- crate::pipeline::arena::StateArena
- crate::pipeline::cycle_finder::clamp_fee_bps
- crate::pipeline::graph::{build_graph, pool_meta_from_pair}
- crate::pipeline::local_sim::simulate_hop_amount_out
- crate::pipeline::types::{GraphEdge, RoutingGraph}
- crate::util::u256_to_f64
- ruint::aliases::U256
- rustc_hash::FxHashMap
- super::*
- rescore_cycles_by_spot_price()
- rescore_cycles_with_table()
- rescored_v2_cycle_has_negative_log_weight()
- _…and 2 more_

### Community 15 — SubmitFees (22 nodes, cohesion: 0.12)

- submit
- build_transaction_request()
- bump_fees()
- alloy::network::Ethereum
- alloy::primitives::{B256, U256}
- alloy::providers::Provider
- alloy::rpc::types::TransactionRequest
- anyhow::{Result, anyhow}
- crate::services::execution::gas_oracle::GasOracle
- super::*
- super::candidate::CandidateExecution
- super::gas::{default_priority_fee_wei, u256_to_u128}
- super::gas_oracle::GasOracle
- super::nonce::NonceManager
- super::rpc_errors::{SubmitAction, classify_submit_error, extract_tx_hash_from_error}
- tracing::{info, instrument, warn}
- profit_boost_increases_priority_fee()
- resolve_submit_fees()
- resolve_submit_fees_with_profit()
- submit_live_candidate()
- _…and 2 more_

### Community 16 — v4_static_fields() (22 nodes, cohesion: 0.13)

- v4
- build_v4_pool_key()
- build_v4_pool_key_detects_zero_for_one()
- build_v4_pool_key_orders_currencies()
- create_test_hop()
- encode_v4_hop()
- encode_v4_hop_first_call_is_approval()
- encode_v4_hop_returns_two_calls()
- encode_v4_hop_second_call_is_lock()
- alloy::dyn_abi::DynSolValue
- alloy::primitives::{Address, I256, Signed, U256, Uint}
- alloy::sol_types::SolCall
- crate::abis::{ExecutorCall, IUniswapV4PoolManager, V4PoolKey}
- crate::core::constants::UNISWAP_V4_POOL_MANAGER
- crate::core::math::uniswap_v3::resolve_v3_fee_pips
- crate::core::types::{Edge, PoolIndex, TokenIndex}
- crate::core::types::PoolState
- crate::pipeline::arena::StateArena
- crate::services::execution::calldata::approvals::encode_approve_if_needed
- crate::services::execution::calldata::types::CalldataHop
- _…and 2 more_

### Community 17 — pass_loop (22 nodes, cohesion: 0.09)

- pass_loop
- crate::config::{AppConfig, WalletSecrets}
- crate::error::ArbError
- crate::infra::hypersync::HyperSyncService
- crate::infra::metrics::PipelineMetrics
- crate::infra::rpc::RpcPool
- crate::infra::wss_feed::spawn_pool_log_feed
- crate::orchestrator::hf::{HfContext, run_hf_tick}
- crate::orchestrator::lf::{LfContext, spawn_lf_background}
- crate::orchestrator::ui_hook::{SharedUiHook, noop_ui_hook}
- crate::pipeline::graph_cache::{set_graph_rebuild_interval, GraphCache}
- crate::services::execution::ExecutionService
- crate::services::execution::GasOracle
- crate::services::hf_snapshot::SnapshotStore
- crate::services::oracle::price_oracle::PriceOracle
- crate::services::partial_cache::{PartialPoolCache, StreamAddressSet}
- crate::services::state_cache::StateCache
- crate::services::state_refresh::StateRefreshService
- std::sync::Arc
- tokio::sync::{Mutex, Semaphore, watch}
- _…and 2 more_

### Community 18 — decode_v4() (21 nodes, cohesion: 0.16)

- decode
- decode_balancer()
- decode_curve_crypto()
- decode_curve_stable()
- decode_dodo()
- decode_plan()
- decode_u128_word()
- decode_u24_fee()
- decode_u256()
- decode_v2()
- decode_v2_reserves()
- decode_v3()
- decode_v3_slot0()
- decode_v4()
- alloy::primitives::{Bytes, U256}
- alloy::sol_types::SolCall
- crate::abis::{
    IBalancerPool, IBalancerVaultRead, ICurvePool, IDodoPoolState, IUniswapV4PoolManager,
}
- crate::core::math::balancer::balancer_swap_fee_from_pool_meta_fee
- crate::core::types::{
    BalancerPoolKind, BalancerPoolState, CurvePoolState, DodoPoolState, PoolState, ProtocolType,
    V2PoolState, V3PoolState, V4PoolState,
}
- crate::core::utils::v4_storage::{decode_v4_liquidity, decode_v4_slot0}
- _…and 1 more_

### Community 19 — tracks_stale_nonces() (21 nodes, cohesion: 0.11)

- nonce
- blocks_second_nonce_while_in_flight()
- concurrent_nonces_never_duplicate()
- alloy::eips::BlockNumberOrTag
- alloy::network::Ethereum
- alloy::primitives::Address
- alloy::providers::Provider
- parking_lot::Mutex
- std::collections::{BTreeSet, HashSet}
- std::sync::atomic::{AtomicBool, Ordering}
- super::*
- tracing::warn
- .is_initialized()
- .mark_stale()
- .next_nonce()
- .release()
- NonceManagerBuilder
- .new()
- .with_max_stale()
- releases_reserved_nonce()
- _…and 1 more_

### Community 20 — submit_via_provider() (20 nodes, cohesion: 0.11)

- private_submit
- alloy::network::Ethereum
- alloy::primitives::B256
- alloy::providers::Provider
- reqwest::Client
- serde::{Deserialize, Serialize}
- tracing::{info, warn}
- JsonRpcError
- JsonRpcRequest
- JsonRpcResponse
- log_probe_report()
- PrivateSubmitMode
- PrivateSubmitProbe
- probe_bloxroute_auth()
- probe_submit_endpoint()
- resolve_submit_mode()
- rpc_call()
- submit_bloxroute_private()
- submit_polygon_private_rpc()
- submit_via_provider()

### Community 21 — SlimPoolState (20 nodes, cohesion: 0.16)

- flush_updates_v2_reserves()
- PartialPoolCache
- .apply_log()
- .apply_patch()
- .flush_to_state_cache()
- .get()
- .is_empty()
- .len()
- .patch_count()
- .seed()
- .seed_from_pool_state()
- .seed_from_state_cache()
- .tracked_addresses()
- .trigger()
- select_stream_targets()
- SlimPoolState
- .from_v2()
- .from_v3()
- .default()
- .notify()

### Community 22 — Theme (20 nodes, cohesion: 0.22)

- theme
- ratatui::style::{Color, Modifier, Style}
- Theme
- .accent()
- .bg()
- .block_border()
- .fg()
- .header()
- .long_tail()
- .loss()
- .muted()
- .profit()
- .protocol_badge()
- .score_style()
- .selected_row()
- .status_style()
- .tab_active()
- .tab_inactive()
- .title()
- .warn()

### Community 23 — StateRefreshService (19 nodes, cohesion: 0.16)

- DiscoveryState
- .rebuild_discovered()
- StateRefreshService
- .cache_size()
- .discovered_pools()
- .discovered_pools_raw()
- .hot_addresses()
- .lf_refresh_batch()
- .lf_tick()
- .maybe_discover()
- .new()
- .prune_dead_pools()
- .refresh_pool_states()
- .refresh_token_metas()
- .refresh_token_metas_if_due()
- .routable_pool_count()
- .set_hot_addresses()
- .token_decimals_map()
- .token_metas()

### Community 24 — spawn_snapshot_poller() (19 nodes, cohesion: 0.11)

- run
- apply_update()
- anyhow::Context
- crate::tui::app::{App, BotStatus}
- crate::tui::bridge::UiBridge
- crate::tui::events::handle_key
- crate::tui::update::UiUpdate
- crate::tui::widgets
- crossterm::event::{self, Event, KeyEventKind}
- crossterm::execute
- crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
}
- ratatui::backend::CrosstermBackend
- ratatui::Terminal
- std::io
- std::time::Duration
- tokio::sync::mpsc
- tokio::time
- run_tui()
- spawn_snapshot_poller()

### Community 25 — finds_triangle_with_bellman_ford() (18 nodes, cohesion: 0.13)

- bellman_ford
- find_cycles_bellman_ford()
- find_cycles_bellman_ford_multi_pass()
- find_cycles_bellman_ford_multi_pass_with_adj()
- finds_triangle_with_bellman_ford()
- alloy::primitives::Address
- crate::core::types::FoundCycle
- crate::core::types::{PoolState, ProtocolType, V2PoolState}
- crate::pipeline::arena::StateArena
- crate::pipeline::deadline::DeadlineGuard
- crate::pipeline::graph::pool_meta_from_pair
- crate::pipeline::negative_cycle::collect_negative_cycles_from_source
- crate::pipeline::types::{CycleSearchPass, PoolMeta, RoutingGraph}
- crate::pipeline::weighted_graph::{WeightedEdge, build_weighted_adjacency, select_hub_tokens}
- pub use crate::pipeline::negative_cycle::{is_simple_cycle, route_call_count}
- ruint::aliases::U256
- std::time::Duration
- super::*

### Community 26 — v4_single_hop_matches_ts_fixture() (18 nodes, cohesion: 0.16)

- calldata_parity
- addr()
- execute_arb_wrapper_matches_ts_fixture()
- fixture_reserve()
- alloy::hex
- alloy::primitives::{Address, Bytes, FixedBytes, U256}
- rpbot::abis::ExecutorCall
- rpbot::core::types::{Edge, PoolState, ProtocolType, V3PoolState, V4PoolState}
- rpbot::pipeline::arena::StateArena
- rpbot::pipeline::types::PoolMeta
- rpbot::services::execution::calldata::{
    RouteEncodeConfig, build_arb_calldata, build_calldata_hops, compute_route_hash, encode_route,
}
- std::str::FromStr
- kyber_single_hop_matches_ts_fixture()
- multi_hop_v2_matches_ts_fixture()
- register_v2_pool()
- route_hash_matches_ts_simple_fixture()
- test_encode_cfg()
- v4_single_hop_matches_ts_fixture()

### Community 27 — solve_quadratic_function_for_trade() (18 nodes, cohesion: 0.17)

- dodo
- caps_above_one_base_sells()
- caps_below_one_quote_sells()
- div_ceil()
- div_floor()
- estimate_dodo_hop_capacity()
- general_integrate()
- get_dodo_amount_out()
- get_dodo_gross_amount_out()
- crate::core::types::DodoPoolState
- ruint::aliases::U256
- super::*
- super::fixed_point::{ONE, mul_down as mul_floor}
- super::int_sqrt::bigint_sqrt
- one2()
- reciprocal_floor()
- simulate_dodo_swap()
- solve_quadratic_function_for_trade()

### Community 28 — ring_fixture() (28) (18 nodes, cohesion: 0.12)

- hf_tick
- bench_arena_hot_patch()
- bench_graph_fingerprint()
- bench_hf_rescore_eval()
- alloy::primitives::Address
- criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main}
- rpbot::core::types::{PoolState, ProtocolType, V2PoolState}
- rpbot::orchestrator::hf_eval::{HfEvalInput, evaluate_cycles_parallel}
- rpbot::pipeline::arena::StateArena
- rpbot::pipeline::cycle_finder::find_cycles_multi_pass
- rpbot::pipeline::graph::{build_graph, pool_meta_from_pair}
- rpbot::pipeline::spot_price::{SpotTable, rescore_cycles_with_table_and_gas}
- rpbot::pipeline::types::{compare_cycle_score, CycleSearchPass}
- ruint::aliases::U256
- rustc_hash::FxHashMap
- std::hint::black_box
- std::sync::Arc
- ring_fixture()

### Community 29 — triangle_fixture() (18 nodes, cohesion: 0.13)

- cycle_finder
- bench_bellman_ford_triangle()
- bench_dfs_dense_ring()
- bench_johnson_triangle()
- bench_parallel_dfs_hub()
- dense_ring_fixture()
- alloy::primitives::Address
- criterion::{Criterion, criterion_group, criterion_main}
- rpbot::core::types::{PoolState, ProtocolType, V2PoolState}
- rpbot::pipeline::arena::StateArena
- rpbot::pipeline::bellman_ford::find_cycles_bellman_ford
- rpbot::pipeline::cycle_finder::{find_cycles, find_cycles_multi_pass}
- rpbot::pipeline::graph::{build_graph, pool_meta_from_pair}
- rpbot::pipeline::johnson::find_cycles_johnson_multi_pass
- rpbot::pipeline::types::{CycleSearchPass, RoutingGraph}
- ruint::aliases::U256
- std::hint::black_box
- triangle_fixture()

### Community 30 — TokenMeta (17 nodes, cohesion: 0.15)

- discovery
- discovered_to_pool_meta()
- discovered_to_pool_meta_propagates_hooks_and_tick_spacing()
- DiscoveredPool
- alloy::primitives::{Address, FixedBytes, keccak256}
- crate::core::protocol::{fee_to_bps, is_fetchable_protocol, normalize_protocol}
- crate::core::types::{PoolIndex, ProtocolType, TokenIndex}
- crate::pipeline::types::PoolMeta
- super::*
- is_hookless_v4_hooks()
- is_routable_pool()
- is_supported_v4_pool()
- is_valid_pool_key()
- parse_optional_bytes32()
- resolve_pool_identity()
- synthetic_cache_address()
- TokenMeta

### Community 31 — V3SwapResult (17 nodes, cohesion: 0.17)

- uniswap_v3
- default_no_tick_step()
- crate::core::constants::FEE_PIPS_SCALE
- crate::core::math::tick_math::get_sqrt_ratio_at_tick
- crate::core::types::{V3PoolState, V3Tick}
- ruint::aliases::U256
- super::*
- super::swap_math::compute_swap_step
- super::tick_math::{
    MAX_SQRT_RATIO, MAX_TICK, MIN_SQRT_RATIO, MIN_TICK, get_sqrt_ratio_at_tick,
    get_tick_at_sqrt_ratio_in_range,
}
- next_initialized_tick()
- next_initialized_tick_handles_boundaries()
- resolve_v3_fee_pips()
- simulate_v3_swap()
- single_tick_swap_produces_output()
- sorted_tick_indices()
- tick_liquidity_net()
- V3SwapResult

### Community 32 — fetch_woofi_pool() (17 nodes, cohesion: 0.13)

- mod
- execute_plan_batch()
- fetch_pools_batched()
- fetch_woofi_pool()
- alloy::network::Ethereum
- alloy::primitives::U256
- alloy::providers::Provider
- crate::abis::{IWoofiPool, IWooracle}
- crate::core::types::{PoolState, ProtocolType, WoofiBaseTokenState, WoofiPoolState}
- crate::pipeline::multicall::execute_multicall
- crate::services::discovery::DiscoveredPool
- crate::services::state_cache::StateCache
- decode::decode_plan
- plans::{PoolFetchPlan, build_plan}
- pub use decode::{decode_v2_reserves, decode_v3_slot0}
- std::sync::Arc
- tracing::debug

### Community 33 — UiSimResult (17 nodes, cohesion: 0.12)

- update
- alert_error()
- alert_info()
- alert_warn()
- BalanceRow
- ConfigSnapshot
- .default()
- GraphStatsSnapshot
- crate::core::types::{FoundCycle, ProtocolType}
- crate::tui::app::{Alert, AlertLevel, BotStatus, TradeRecord, TradeStatus}
- std::collections::HashMap
- protocol_short_label()
- ScannerMetrics
- trade_from_outcome()
- UiOpportunity
- UiSimResult
- UiUpdate

### Community 34 — PipelineMetrics (17 nodes, cohesion: 0.12)

- metrics
- std::sync::atomic::{AtomicU64, Ordering}
- MetricsSnapshot
- PipelineMetrics
- .record_block_triggered_hf()
- .record_dispatch_deferred()
- .record_dispatch_started()
- .record_dry_run_passed()
- .record_hf_skipped()
- .record_hf_tick()
- .record_lf_skipped()
- .record_lf_tick()
- .record_stream_log()
- .record_stream_triggered_hf()
- .record_tx_confirmed()
- .record_tx_reverted()
- .snapshot()

### Community 35 — weighted_50_50_zero_fee() (17 nodes, cohesion: 0.19)

- balancer
- abs_diff()
- balancer_swap_fee_from_pool_meta_fee()
- calculate_balancer_stable_invariant()
- get_balancer_stable_amount_out()
- get_balancer_weighted_amount_out()
- crate::core::types::{BalancerPoolKind, BalancerPoolState}
- ruint::aliases::U256
- super::*
- super::fixed_point::{ONE, complement, pow_down}
- super::full_math::div_rounding_up_or_zero
- resolve_swap_fee()
- simulate_balancer_swap()
- stable_invariant_positive()
- swap_fee_from_meta()
- token_balance_given_invariant()
- weighted_50_50_zero_fee()

### Community 36 — hybrid_finds_triangle() (17 nodes, cohesion: 0.13)

- cycle_search
- find_cycles_hybrid_multi_pass()
- hybrid_finds_triangle()
- alloy::primitives::Address
- crate::core::types::FoundCycle
- crate::core::types::{PoolState, ProtocolType, V2PoolState}
- crate::pipeline::arena::StateArena
- crate::pipeline::bellman_ford::find_cycles_bellman_ford_multi_pass_with_adj
- crate::pipeline::cycle_filter::{dedupe_cycles_by_fingerprint, prefilter_cycles_by_atomic_sim}
- crate::pipeline::cycle_finder::find_cycles_multi_pass
- crate::pipeline::graph::{build_graph, pool_meta_from_pair}
- crate::pipeline::johnson::find_cycles_johnson_multi_pass_with_adj
- crate::pipeline::types::{CycleSearchPass, RoutingGraph}
- crate::pipeline::weighted_graph::{
    build_weighted_adjacency, compute_bf_potentials, reweight_adjacency,
}
- rayon::join
- ruint::aliases::U256
- super::*

### Community 37 — johnson_finds_triangle() (16 nodes, cohesion: 0.14)

- johnson
- find_cycles_johnson_multi_pass()
- find_cycles_johnson_multi_pass_with_adj()
- alloy::primitives::Address
- crate::core::types::FoundCycle
- crate::core::types::{PoolState, ProtocolType, V2PoolState}
- crate::pipeline::arena::StateArena
- crate::pipeline::deadline::DeadlineGuard
- crate::pipeline::graph::{build_graph, pool_meta_from_pair}
- crate::pipeline::negative_cycle::collect_negative_cycles_from_source
- crate::pipeline::types::{CycleSearchPass, RoutingGraph}
- crate::pipeline::weighted_graph::{
    WeightedEdge, build_weighted_adjacency, compute_bf_potentials, reweight_adjacency,
    select_hub_tokens,
}
- ruint::aliases::U256
- std::time::Duration
- super::*
- johnson_finds_triangle()

### Community 38 — token_decimals_map() (16 nodes, cohesion: 0.13)

- mod
- enrich_token_to_matic_rates()
- alloy::network::Ethereum
- alloy::primitives::Address
- alloy::providers::Provider
- crate::core::constants::WMATIC
- crate::core::types::TokenIndex
- crate::pipeline::arena::StateArena
- crate::services::discovery::TokenMeta
- pub use rates::resolve_token_to_matic_rate
- ruint::aliases::U256
- rustc_hash::FxHashMap
- self::price_oracle::{
    PriceOracle, bootstrap_matic_rate_per_unit, token_usd_to_matic_rate_per_unit,
}
- std::collections::HashMap
- resolve_token_decimals()
- token_decimals_map()

### Community 39 — StateArena (16 nodes, cohesion: 0.19)

- StateArena
- .address_to_pool()
- .apply_hot_cache()
- .clone()
- .default()
- .new()
- .pool_address()
- .pool_count()
- .pool_state()
- .pool_state_mut()
- .refresh_pools_from_cache()
- .register_pool()
- .register_token()
- .sync_from_discovery()
- .token_address()
- .token_count()

### Community 40 — token_addrs_csv() (16 nodes, cohesion: 0.15)

- tracing_util
- alloy::primitives::Address
- crate::core::types::{Edge, EvaluatedRoute, FoundCycle}
- crate::pipeline::arena::StateArena
- crate::pipeline::types::route_fingerprint
- crate::services::execution::candidate::CandidateExecution
- tracing::Span
- pool_addrs_csv()
- record_candidate()
- record_cycle_route()
- record_evaluated_route()
- record_gas_fees()
- record_receipt()
- record_tx()
- start_token_addr()
- token_addrs_csv()

### Community 41 — pool_fingerprint() (16 nodes, cohesion: 0.13)

- graph_cache
- alloy::primitives::Address
- crate::core::types::FoundCycle
- crate::core::types::{PoolState, ProtocolType, V2PoolState}
- crate::pipeline::arena::StateArena
- crate::pipeline::graph::{build_graph, rescore_graph_in_place}
- crate::pipeline::graph::pool_meta_from_pair
- crate::pipeline::spot_price::rescore_cycles_by_spot_price
- crate::pipeline::types::{PoolMeta, RoutingGraph}
- crate::services::state_cache::StateCache
- ruint::aliases::U256
- std::hash::{Hash, Hasher}
- std::sync::Arc
- std::sync::atomic::{AtomicU64, Ordering}
- super::*
- pool_fingerprint()

### Community 42 — v2_pool() (16 nodes, cohesion: 0.15)

- cycle_filter
- atomic_prefilter_keeps_mispriced_triangle()
- dedupe_cycles_by_fingerprint()
- dedupe_keeps_best_score()
- alloy::primitives::Address
- crate::core::types::{Edge, FoundCycle}
- crate::core::types::{PoolIndex, PoolState, ProtocolType, TokenIndex, V2PoolState}
- crate::pipeline::arena::StateArena
- crate::pipeline::local_sim::simulate_route_minimal
- crate::pipeline::spot_price::SPOT_PROBE
- crate::pipeline::types::{compare_cycle_score, route_fingerprint}
- ruint::aliases::U256
- super::*
- is_fully_simulable_route()
- prefilter_cycles_by_atomic_sim()
- v2_pool()

### Community 43 — encode_kyber_hop_validates_recipient() (16 nodes, cohesion: 0.16)

- kyber
- create_test_hop()
- encode_kyber_hop()
- encode_kyber_hop_returns_single_call()
- encode_kyber_hop_uses_correct_protocol_constant()
- encode_kyber_hop_validates_recipient()
- alloy::dyn_abi::DynSolValue
- alloy::primitives::{Address, I256, U160, U256}
- alloy::sol_types::SolCall
- crate::abis::{ExecutorCall, IKyberElasticPool}
- crate::core::types::{Edge, PoolIndex, TokenIndex}
- crate::pipeline::arena::StateArena
- crate::services::execution::calldata::types::CalldataHop
- crate::services::execution::quote::{
    derive_tight_v3_price_limit_kyber, pool_tokens_from_hop, quote_hop_for_execution,
    resolve_kyber_fee_pips,
}
- super::*
- super::shared::to_v3_state

### Community 44 — dispatch_with_provider() (16 nodes, cohesion: 0.13)

- hf_execute
- dispatch_profitable_candidates()
- dispatch_with_provider()
- alloy::network::Ethereum
- alloy::providers::Provider
- crate::core::types::EvaluatedRoute
- crate::infra::tracing_util::{pool_addrs_csv, record_evaluated_route}
- crate::orchestrator::hf::HfContext
- crate::pipeline::arena::StateArena
- crate::pipeline::types::route_fingerprint
- crate::services::execution::{
    CandidateBuildConfig, ExecutionOutcome, PrepareDispatchInput, build_execution_candidate,
    collect_flash_tokens, prepare_evaluated_route,
}
- crate::services::execution::flash_policy::parse_flash_policy
- crate::services::execution::impact_slippage::depth_impact_slippage_bps
- crate::services::oracle::price_oracle::bootstrap_matic_rate_per_unit
- ruint::aliases::U256 as RU256
- tracing::{Instrument, debug, info, info_span, instrument, warn}

### Community 45 — parse_args() (16 nodes, cohesion: 0.14)

- tui
- Args
- anyhow::Context
- rpbot::config::{AppConfig, WalletSecrets}
- rpbot::orchestrator::{RuntimeContext, run_pass_loop}
- rpbot::tui::{App, UiBridge, run_tui}
- rpbot::tui::mock::spawn_mock_updates
- rpbot::tui::run::spawn_snapshot_poller
- rpbot::tui::update::UiUpdate
- std::sync::Arc
- tokio::sync::{mpsc, watch}
- tracing::info
- tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt}
- init_tracing()
- main()
- parse_args()

### Community 46 — bench_gas_estimate() (16 nodes, cohesion: 0.13)

- scaling
- bench_gas_estimate()
- alloy::primitives::Address
- criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main}
- rpbot::core::types::{PoolState, ProtocolType, V2PoolState}
- rpbot::pipeline::arena::StateArena
- rpbot::pipeline::cycle_finder::find_cycles_multi_pass
- rpbot::pipeline::graph::{build_graph, pool_meta_from_pair, rescore_graph_in_place}
- rpbot::pipeline::graph_cache::connectivity_fingerprint
- rpbot::pipeline::local_sim::simulate_route_minimal
- rpbot::pipeline::spot_price::rescore_cycles_by_spot_price
- rpbot::pipeline::types::CycleSearchPass
- rpbot::services::execution::gas::estimate_route_gas_from_hops
- ruint::aliases::U256
- std::hint::black_box
- std::sync::Arc

### Community 47 — u256_to_u128() (16 nodes, cohesion: 0.16)

- gas
- buffer_gas_limit()
- compute_conservative_gas_price()
- conservative_gas_price_wei()
- default_priority_fee_wei()
- estimate_route_gas_from_hops()
- FeeSnapshot
- gas_drift_bps()
- anyhow::{Result, anyhow}
- ruint::aliases::U256
- super::*
- pick_buffered_gas_limit()
- pick_live_gas_fails_on_zero()
- pick_live_gas_limit()
- pick_live_gas_uses_max_of_sim_and_dry_run()
- u256_to_u128()

### Community 48 — resolve_v3_fee_pips_for_hop() (15 nodes, cohesion: 0.17)

- quote
- derive_tight_v3_price_limit()
- derive_tight_v3_price_limit_inner()
- derive_tight_v3_price_limit_kyber()
- alloy::primitives::{Address, U256}
- crate::core::math::uniswap_v3::{resolve_v3_fee_pips, simulate_v3_swap}
- crate::core::types::{PoolState, V3PoolState}
- crate::pipeline::arena::StateArena
- crate::pipeline::local_sim::simulate_hop_amount_out
- crate::services::execution::calldata::CalldataHop
- is_kyber_protocol()
- pool_tokens_from_hop()
- quote_hop_for_execution()
- resolve_kyber_fee_pips()
- resolve_v3_fee_pips_for_hop()

### Community 49 — encode_v3_hop_validates_recipient() (15 nodes, cohesion: 0.17)

- v3
- create_test_hop()
- encode_v3_hop()
- encode_v3_hop_returns_single_call()
- encode_v3_hop_validates_recipient()
- alloy::dyn_abi::DynSolValue
- alloy::primitives::{Address, I256, U160, U256}
- alloy::sol_types::SolCall
- crate::abis::{ExecutorCall, IUniswapV3Pool}
- crate::core::types::{Edge, PoolIndex, TokenIndex}
- crate::pipeline::arena::StateArena
- crate::services::execution::calldata::types::CalldataHop
- crate::services::execution::quote::{
    derive_tight_v3_price_limit, pool_tokens_from_hop, quote_hop_for_execution,
    resolve_v3_fee_pips_for_hop,
}
- super::*
- super::shared::to_v3_state

### Community 50 — UiSimulation (15 nodes, cohesion: 0.13)

- app
- Alert
- AlertLevel
- BotStatus
- .label()
- crate::tui::update::{
    BalanceRow, ConfigSnapshot, GraphStatsSnapshot, ScannerMetrics, UiOpportunity, UiSimResult,
}
- std::time::Instant
- Tab
- .from_index()
- .index()
- .title()
- TradeRecord
- TradeStatus
- .label()
- UiSimulation

### Community 51 — ring_fixture() (15 nodes, cohesion: 0.16)

- cycle_filter
- bench_dedupe_fingerprint()
- bench_dedupe_high_duplicates()
- bench_prefilter_atomic_sim()
- alloy::primitives::Address
- criterion::{Criterion, criterion_group, criterion_main}
- rpbot::core::types::{FoundCycle, PoolState, ProtocolType, V2PoolState}
- rpbot::pipeline::arena::StateArena
- rpbot::pipeline::cycle_filter::{dedupe_cycles_by_fingerprint, prefilter_cycles_by_atomic_sim}
- rpbot::pipeline::cycle_finder::find_cycles_multi_pass
- rpbot::pipeline::graph::{build_graph, pool_meta_from_pair}
- rpbot::pipeline::types::CycleSearchPass
- ruint::aliases::U256
- std::hint::black_box
- ring_fixture()

### Community 52 — PipelineConfig (15 nodes, cohesion: 0.13)

- default_graph_rebuild_interval()
- default_hf_max_dispatch()
- default_hf_prefetch_count()
- default_hf_score_cap()
- default_hf_sim_cap()
- default_hf_skip_prefetch_on_stream()
- default_hf_trigger_on_block()
- default_hf_trigger_on_stream()
- default_lf_bootstrap_batch()
- default_lf_full_sweep_interval()
- default_lf_hot_batch()
- default_stream_enabled()
- default_stream_max_pools()
- PipelineConfig
- .default()

### Community 53 — tokio_console_enabled() (15 nodes, cohesion: 0.17)

- main
- anyhow::Context
- rpbot::config::{AppConfig, WalletSecrets}
- rpbot::core::constants::POLYGON_CHAIN_ID
- rpbot::orchestrator::{RuntimeContext, run_pass_loop}
- std::sync::Arc
- tokio::signal
- tokio::sync::watch
- tracing::{info, warn}
- tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt}
- init_tracing()
- json_logs_enabled()
- main()
- shutdown_signal()
- tokio_console_enabled()

### Community 54 — min_operator_balance_wei() (15 nodes, cohesion: 0.28)

- ExecutionService
- .any_quarantined()
- .clear_fail_count()
- .ensure_nonce_manager()
- .finalize_receipt()
- .is_route_quarantined()
- .new()
- .process_candidate()
- .quarantine_route()
- .quarantine_route_soft()
- .reassess_after_dry_run()
- .reassess_assessment()
- .shutdown_resync()
- .with_circuit_breaker_cooldown()
- min_operator_balance_wei()

### Community 55 — JournalEntry (14 nodes, cohesion: 0.16)

- opportunity_journal
- append()
- crate::core::types::ProfitAssessment
- crate::services::execution::opportunity_log::OpportunityRecord
- parking_lot::Mutex
- serde::Serialize
- std::fs::OpenOptions
- std::io::Write
- std::path::PathBuf
- std::sync::LazyLock
- init_from_env()
- journal_from_record()
- journal_outcome()
- JournalEntry

### Community 56 — curve (14 nodes, cohesion: 0.14)

- curve
- alloy::primitives::Address
- alloy::primitives::U256
- alloy::sol_types::SolCall
- crate::abis::{ExecutorCall, ICurveCryptoPool, ICurveStableNgPool, ICurveStablePool}
- crate::core::types::{Edge, PoolIndex, TokenIndex}
- crate::core::types::ProtocolType
- crate::pipeline::arena::StateArena
- crate::services::execution::calldata::approvals::encode_approve_if_needed
- crate::services::execution::calldata::types::CalldataHop
- crate::services::execution::profit::slippage_adjusted
- crate::services::execution::quote::quote_hop_for_execution
- super::*
- super::shared::curve_uses_receiver

### Community 57 — V2Fee (14 nodes, cohesion: 0.20)

- uniswap_v2
- fee_numerator_from_bps()
- fits_u128()
- get_amount_in()
- get_amount_in_u128()
- crate::core::constants::{BPS_SCALE, DEFAULT_FEE_NUMERATOR, FEE_DENOMINATOR}
- crate::core::types::V2PoolState
- ruint::aliases::U256
- super::*
- resolve_v2_fee()
- resolve_v2_fee_with_edge()
- to_u128()
- v2_fee_numerator_from_bps()
- V2Fee

### Community 58 — quantity_zero() (14 nodes, cohesion: 0.14)

- hypersync
- alloy::primitives::B256
- anyhow::{Context, Result}
- crate::config::RpcConfig
- crate::core::constants::POLYGON_CHAIN_ID
- hypersync_client::Client
- hypersync_client::format::TransactionStatus
- hypersync_client::net_types::{
    JoinMode, Query, TransactionFilter, TransactionSelection, transaction::TransactionField,
}
- super::*
- quantity_large_value()
- quantity_max_u64()
- quantity_small_value()
- quantity_to_u64()
- quantity_zero()

### Community 59 — encode_route() (14 nodes, cohesion: 0.14)

- mod
- build_arb_calldata()
- build_calldata_hops()
- encode_route()
- alloy::primitives::{Address, U256}
- alloy::sol_types::SolCall
- crate::abis::ExecutorCall
- crate::core::types::Edge
- crate::pipeline::arena::StateArena
- crate::pipeline::types::PoolMeta
- encoders::encoder_for_protocol
- pub use encoders::shared::{
    curve_uses_receiver, derive_balancer_pool_id, resolve_balancer_pool_id, to_v3_state,
}
- pub use hash::compute_route_hash
- pub use types::{BuiltArbTx, CalldataHop, RouteEncodeConfig}

### Community 60 — mask_wss_url() (14 nodes, cohesion: 0.14)

- wss_feed
- alloy::primitives::{Address, B256}
- alloy::providers::{Provider, ProviderBuilder, WsConnect}
- alloy::pubsub::Subscription
- alloy::rpc::types::Filter
- crate::config::AppConfig
- crate::infra::metrics::PipelineMetrics
- crate::services::partial_cache::{
    PartialPoolCache, StreamAddressSet, V2_SYNC_TOPIC, V3_SWAP_TOPIC,
}
- crate::util::now_ms
- futures::StreamExt
- std::sync::Arc
- tokio::sync::watch
- tracing::{debug, error, info, warn}
- mask_wss_url()

### Community 61 — state_refresh (14 nodes, cohesion: 0.14)

- state_refresh
- alloy::primitives::Address
- crate::config::AppConfig
- crate::error::ArbError
- crate::infra::hasura::{DiscoveryCursor, HasuraClient}
- crate::infra::rpc::RpcPool
- crate::pipeline::fetcher::fetch_missing_pool_states
- crate::services::discovery::{DiscoveredPool, TokenMeta}
- crate::services::state_cache::StateCache
- crate::util::now_ms
- std::collections::HashMap
- std::sync::Arc
- std::sync::atomic::{AtomicU64, Ordering}
- tracing::{info, warn}

### Community 62 — DryRunResult (13 nodes, cohesion: 0.17)

- dryrun
- build_tx()
- dry_run_candidate()
- DryRunResult
- alloy::network::Ethereum
- alloy::primitives::Address
- alloy::providers::Provider
- alloy::rpc::types::TransactionRequest
- crate::services::execution::candidate::CandidateExecution
- crate::services::execution::rpc_errors::classify_submit_error
- std::time::Duration
- tokio::time::timeout
- tracing::{instrument, warn}

### Community 63 — sort_desc() (13 nodes, cohesion: 0.23)

- curve_crypto
- compute_k0()
- curve_crypto_newton_d()
- curve_crypto_newton_y()
- geometric_mean()
- get_curve_crypto_amount_out()
- crate::core::types::CurvePoolState
- ruint::aliases::U256
- super::*
- super::fixed_point::ONE
- newton_d_converges_for_balanced_pool()
- NewtonResult
- sort_desc()

### Community 64 — TokenMetaRows (13 nodes, cohesion: 0.15)

- hasura
- DiscoveryCursor
- DiscoveryResult
- GraphQlResponse
- anyhow::Context
- crate::error::ArbError
- crate::services::discovery::{DiscoveredPool, TokenMeta, parse_pool_meta_row}
- serde::Deserialize
- super::*
- PoolMetaRow
- PoolMetaRows
- TokenMetaRow
- TokenMetaRows

### Community 65 — evaluated_from_sim() (13 nodes, cohesion: 0.15)

- candidate
- build_execution_candidate()
- CandidateBuildConfig
- CandidateExecution
- evaluated_from_sim()
- alloy::primitives::{Address, Bytes, FixedBytes, U256}
- crate::core::types::{EvaluatedRoute, FlashLoanSource, RouteSimulationResult}
- crate::pipeline::arena::StateArena
- crate::pipeline::types::PoolMeta
- crate::services::execution::calldata::{
    RouteEncodeConfig, build_arb_calldata, build_calldata_hops, encode_route,
}
- crate::services::execution::gas::buffer_gas_limit
- crate::services::execution::profit::{on_chain_min_profit_for_route, slippage_adjusted}
- tracing::instrument

### Community 66 — woofi (13 nodes, cohesion: 0.15)

- woofi
- alloy::primitives::Address
- alloy::primitives::U256
- alloy::sol_types::SolCall
- crate::abis::{ExecutorCall, IWoofiRouter}
- crate::core::constants::WOOFI_ROUTER_V2
- crate::core::types::{Edge, PoolIndex, TokenIndex}
- crate::pipeline::arena::StateArena
- crate::services::execution::calldata::approvals::encode_approve_if_needed
- crate::services::execution::calldata::types::CalldataHop
- crate::services::execution::profit::slippage_adjusted
- crate::services::execution::quote::quote_hop_for_execution
- super::*

### Community 67 — v3_swap_min_length() (13 nodes, cohesion: 0.21)

- decode
- decode_pool_log()
- decode_v2_sync()
- decode_v3_swap()
- alloy::primitives::{Address, B256, U256}
- alloy::sol_types::SolEvent
- crate::abis::{IUniswapV2Pair, IUniswapV3Pool}
- super::*
- is_streamable_protocol()
- LogPatch
- pool_address_from_log()
- v2_sync_min_length()
- v3_swap_min_length()

### Community 68 — RoutingGraph (13 nodes, cohesion: 0.17)

- types
- compare_cycle_score()
- CycleSearchPass
- GraphEdge
- alloy::primitives::{Address, FixedBytes}
- crate::core::types::{Edge, FoundCycle, PoolIndex, ProtocolType, TokenIndex}
- MinimalSimResult
- OptimizationResult
- PoolMeta
- route_fingerprint()
- RoutingGraph
- .add_edge()
- .new()

### Community 69 — resolve_v4_pool_id() (13 nodes, cohesion: 0.15)

- fetcher
- alloy::network::Ethereum
- alloy::primitives::Address
- alloy::primitives::{Address, FixedBytes}
- alloy::providers::Provider
- crate::core::protocol::is_fetchable_protocol
- crate::core::types::{PoolState, ProtocolType}
- crate::pipeline::pool_fetch::fetch_pools_batched
- crate::services::discovery::DiscoveredPool
- crate::services::state_cache::StateCache
- std::sync::Arc
- super::*
- resolve_v4_pool_id()

### Community 70 — apply_slim_to_pool_state() (13 nodes, cohesion: 0.15)

- mod
- apply_slim_to_pool_state()
- alloy::primitives::{Address, B256, U256}
- crate::core::types::{PoolState, ProtocolType}
- crate::core::types::V2PoolState
- crate::services::state_cache::StateCache
- dashmap::DashMap
- parking_lot::RwLock
- pub use decode::{LogPatch, V2_SYNC_TOPIC, V3_SWAP_TOPIC, decode_pool_log, is_streamable_protocol}
- std::sync::Arc
- std::sync::atomic::{AtomicU64, Ordering}
- super::*
- tokio::sync::watch

### Community 71 — flame_profile (13 nodes, cohesion: 0.15)

- flame_profile
- alloy::primitives::Address
- rpbot::core::types::{Edge, FoundCycle, PoolState, ProtocolType, V2PoolState}
- rpbot::pipeline::arena::StateArena
- rpbot::pipeline::bellman_ford::find_cycles_bellman_ford
- rpbot::pipeline::cycle_finder::find_cycles_multi_pass
- rpbot::pipeline::graph::{build_graph, pool_meta_from_pair}
- rpbot::pipeline::local_sim::simulate_route_minimal
- rpbot::pipeline::spot_price::{compute_spot_price, rescore_cycles_by_spot_price}
- rpbot::pipeline::types::CycleSearchPass
- ruint::aliases::U256
- std::env
- std::hint::black_box

### Community 72 — normalizes_v3_labels() (13 nodes, cohesion: 0.21)

- protocol
- all_polygon_dex_labels_normalize_and_fetch()
- converts_kyber_elastic_units()
- converts_v3_pips_to_bps()
- default_pool_fee_raw()
- fee_to_bps()
- fee_unit_divisor()
- crate::core::types::ProtocolType
- super::*
- is_fetchable_protocol()
- keeps_v2_bps_unchanged()
- normalize_protocol()
- normalizes_v3_labels()

### Community 73 — HfSnapshot (13 nodes, cohesion: 0.15)

- hf_snapshot
- HfSnapshot
- arc_swap::ArcSwap
- crate::core::types::{FoundCycle, TokenIndex}
- crate::pipeline::arena::StateArena
- crate::pipeline::types::PoolMeta
- crate::services::discovery::DiscoveredPool
- ruint::aliases::U256
- rustc_hash::FxHashMap
- std::collections::HashMap
- std::sync::Arc
- std::sync::atomic::{AtomicU64, Ordering}
- super::*

### Community 74 — balancer (13 nodes, cohesion: 0.15)

- balancer
- alloy::primitives::{Address, U256}
- alloy::sol_types::SolCall
- crate::abis::{BalancerFundManagement, BalancerSingleSwap, ExecutorCall, IBalancerVault}
- crate::core::constants::BALANCER_VAULT
- crate::core::types::{Edge, PoolIndex, TokenIndex}
- crate::pipeline::arena::StateArena
- crate::services::execution::calldata::approvals::encode_approve_if_needed
- crate::services::execution::calldata::types::CalldataHop
- crate::services::execution::profit::slippage_adjusted
- crate::services::execution::quote::quote_hop_for_execution
- super::*
- super::shared::resolve_balancer_pool_id

### Community 75 — RpcPool (12 nodes, cohesion: 0.26)

- RpcPool
- .connect_http()
- .connect_simulation()
- .connect_state()
- .connect_submit()
- .execution_url()
- .from_config()
- .private_url()
- .require_private_submit()
- .simulation_url()
- .state_url()
- .submit_url()

### Community 76 — render_sparkline() (12 nodes, cohesion: 0.21)

- overview
- crate::tui::app::App
- crate::tui::layout::{kpi_row, overview_layout, split_horizontal}
- crate::tui::theme::Theme
- ratatui::Frame
- ratatui::layout::Rect
- ratatui::text::{Line, Span}
- ratatui::widgets::{Block, Borders, Paragraph, Sparkline}
- render()
- render_alerts()
- render_kpis()
- render_sparkline()

### Community 77 — CircuitBreaker (12 nodes, cohesion: 0.29)

- auto_resets_after_cooldown()
- CircuitBreaker
- .check_operator_balance()
- .default()
- .is_paused()
- .new()
- .pause_reason()
- .record_failure()
- .record_success()
- .reset()
- .trip()
- .try_auto_reset()

### Community 78 — zero_on_symmetric_liquidity() (12 nodes, cohesion: 0.18)

- impact_slippage
- depth_impact_slippage_bps()
- effective_slippage_bps()
- alloy::primitives::Address
- crate::core::constants::BPS_SCALE
- crate::core::types::Edge
- crate::core::types::{Edge, PoolState, ProtocolType, V2PoolState}
- crate::pipeline::arena::StateArena
- crate::pipeline::local_sim::simulate_route_minimal
- ruint::aliases::U256
- super::*
- zero_on_symmetric_liquidity()

### Community 79 — render_table() (12 nodes, cohesion: 0.20)

- opportunities
- crate::tui::app::App
- crate::tui::layout::opp_layout
- crate::tui::route_viz::format_score_delta
- crate::tui::text::truncate_str
- crate::tui::theme::Theme
- ratatui::Frame
- ratatui::layout::{Constraint, Rect}
- ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table}
- render()
- render_detail()
- render_table()

### Community 80 — make_simple_chain() (12 nodes, cohesion: 0.20)

- negative_cycle
- bench_collect_negative_cycles_from_source()
- bench_is_simple_cycle()
- bench_is_simple_cycle_complex()
- bench_route_call_count()
- criterion::{Criterion, criterion_group, criterion_main}
- rpbot::core::types::{Edge, PoolIndex, ProtocolType, TokenIndex}
- rpbot::pipeline::negative_cycle::{collect_negative_cycles_from_source, is_simple_cycle, route_call_count}
- rpbot::pipeline::weighted_graph::WeightedEdge
- rustc_hash::FxHashSet
- std::hint::black_box
- make_simple_chain()

### Community 81 — recover_after_receipt_timeout() (12 nodes, cohesion: 0.17)

- recovery
- alloy::network::Ethereum
- alloy::primitives::{Address, B256, U256}
- alloy::providers::Provider
- alloy::rpc::types::TransactionRequest
- super::gas::u256_to_u128
- super::nonce::NonceManager
- super::receipt::ReceiptData
- super::submit::{SubmitFees, bump_fees, FEE_BUMP_BPS}
- tracing::{info, warn}
- NonceRecoveryOutcome
- recover_after_receipt_timeout()

### Community 82 — bridge (12 nodes, cohesion: 0.17)

- bridge
- crate::config::AppConfig
- crate::core::types::FoundCycle
- crate::orchestrator::hf::HfTickResult
- crate::pipeline::arena::StateArena
- crate::pipeline::cycle_finder::prioritize_cycle_start_tokens_from_out_degrees
- crate::pipeline::types::PoolMeta
- crate::tui::app::{BotStatus, TradeStatus}
- crate::tui::route_viz::{
    cycle_to_ui_opportunity, token_label,
}
- crate::tui::update::{
    GraphStatsSnapshot, ScannerMetrics, UiOpportunity, UiUpdate, alert_info, trade_from_outcome,
}
- std::collections::HashMap
- tokio::sync::mpsc

### Community 83 — liquidity_risk_score() (12 nodes, cohesion: 0.18)

- route_viz
- cycle_has_long_tail()
- format_score_delta()
- alloy::primitives::Address
- crate::core::types::{Edge, FoundCycle, TokenIndex}
- crate::pipeline::arena::StateArena
- crate::pipeline::bellman_ford::route_call_count
- crate::pipeline::types::{PoolMeta, route_fingerprint}
- crate::tui::update::{UiOpportunity, protocol_short_label}
- std::collections::HashSet
- is_major_token()
- liquidity_risk_score()

### Community 84 — gas_oracle (11 nodes, cohesion: 0.18)

- gas_oracle
- alloy::eips::BlockNumberOrTag
- alloy::network::Ethereum
- alloy::providers::{Provider, ProviderBuilder}
- arc_swap::ArcSwap
- ruint::aliases::U256
- std::sync::Arc
- std::time::Duration
- super::gas::{
    DEFAULT_CONSERVATIVE_GAS_PRICE_WEI, FeeSnapshot, compute_conservative_gas_price,
    conservative_gas_price_wei, default_priority_fee_wei,
}
- tokio::sync::watch
- tracing::{debug, warn}

### Community 85 — PoolFetchPlan (11 nodes, cohesion: 0.18)

- plans
- CallKind
- FetchPoolInfo
- alloy::primitives::{Address, Bytes, FixedBytes, U256}
- crate::abis::{
    IBalancerPool, IBalancerVaultRead, ICurvePool, IDodoPoolState, IUniswapV2Pair, IUniswapV3Pool,
    IUniswapV4PoolManager,
}
- crate::core::constants::{BALANCER_VAULT, UNISWAP_V4_POOL_MANAGER}
- crate::core::types::ProtocolType
- crate::core::utils::v4_storage::{V4_LIQUIDITY_OFFSET, compute_v4_pool_field_slot}
- crate::pipeline::multicall::{MulticallItem, encode_call}
- crate::services::discovery::DiscoveredPool
- PoolFetchPlan

### Community 86 — CachedEntry (11 nodes, cohesion: 0.18)

- state_cache
- CachedEntry
- alloy::primitives::Address
- crate::core::types::PoolState
- parking_lot::RwLock
- rustc_hash::FxHashMap
- std::sync::Arc
- std::sync::atomic::{AtomicU64, Ordering}
- std::thread
- std::time::{Duration, Instant}
- super::*

### Community 87 — v2 (11 nodes, cohesion: 0.18)

- v2
- alloy::primitives::{Address, Bytes, U256}
- alloy::sol_types::SolCall
- crate::abis::{ExecutorCall, IERC20, IUniswapV2Pair}
- crate::core::types::{Edge, PoolIndex, ProtocolType, TokenIndex}
- crate::pipeline::arena::StateArena
- crate::services::execution::calldata::approvals::encode_transfer_all
- crate::services::execution::profit::slippage_adjusted
- crate::services::execution::quote::quote_hop_for_execution
- super::*
- super::super::types::CalldataHop

### Community 88 — render_metrics() (11 nodes, cohesion: 0.22)

- diagnostics
- crate::tui::app::App
- crate::tui::layout::split_horizontal
- crate::tui::theme::Theme
- ratatui::Frame
- ratatui::layout::Rect
- ratatui::text::{Line, Span}
- ratatui::widgets::{Block, Borders, Paragraph}
- render()
- render_logs()
- render_metrics()

### Community 89 — WeightedEdge (11 nodes, cohesion: 0.18)

- weighted_graph
- build_weighted_adjacency()
- build_weighted_adjacency_rescored()
- compute_bf_potentials()
- crate::core::types::{Edge, TokenIndex}
- crate::pipeline::arena::StateArena
- crate::pipeline::spot_price::{SpotTable, compute_edge_log_weight_with_table}
- crate::pipeline::types::RoutingGraph
- reweight_adjacency()
- select_hub_tokens()
- WeightedEdge

### Community 90 — one_36() (11 nodes, cohesion: 0.35)

- log_exp_math
- ruint::aliases::U256
- super::fixed_point::ONE as ONE_18
- ln()
- ln_36()
- log_exp_exp()
- log_exp_ln()
- log_exp_pow()
- max_natural_exponent()
- one_20()
- one_36()

### Community 91 — read_chainlink_usd() (11 nodes, cohesion: 0.36)

- chainlink_answer_to_usd()
- chainlink_feed()
- PriceOracle
- .fetch_pyth()
- .fresh()
- .get_matic_usd()
- .is_enabled()
- .new()
- .prefetch_token_usd()
- .token_usd()
- read_chainlink_usd()

### Community 92 — spot_price (11 nodes, cohesion: 0.18)

- spot_price
- alloy::primitives::Address
- criterion::{BenchmarkId, Criterion, criterion_group, criterion_main}
- rpbot::core::types::{Edge, FoundCycle, PoolState, ProtocolType, V2PoolState}
- rpbot::pipeline::arena::StateArena
- rpbot::pipeline::cycle_finder::find_cycles_multi_pass
- rpbot::pipeline::graph::{build_graph, pool_meta_from_pair}
- rpbot::pipeline::spot_price::{compute_spot_price, rescore_cycles_by_spot_price}
- rpbot::pipeline::types::CycleSearchPass
- ruint::aliases::U256
- std::hint::black_box

### Community 93 — MulticallItem (11 nodes, cohesion: 0.18)

- multicall
- chunk_items()
- encode_call()
- execute_multicall()
- alloy::network::Ethereum
- alloy::primitives::{Address, Bytes}
- alloy::providers::Provider
- alloy::sol_types::SolCall
- crate::abis::IMulticall3
- crate::core::constants::MULTICALL3
- MulticallItem

### Community 94 — round_trips_various_ticks() (11 nodes, cohesion: 0.31)

- tick_math
- get_sqrt_ratio_at_tick()
- get_tick_at_sqrt_ratio()
- get_tick_at_sqrt_ratio_in_range()
- ruint::aliases::U256
- super::*
- in_range_respects_bounds()
- mul_shift()
- normalize_tick_search_bounds()
- round_trips_tick_zero()
- round_trips_various_ticks()

### Community 95 — ReceiptData (11 nodes, cohesion: 0.18)

- receipt
- alloy::network::Ethereum
- alloy::primitives::B256
- alloy::providers::Provider
- alloy::rpc::types::Log
- crate::infra::hypersync::HyperSyncService
- crate::services::execution::rpc_errors::is_transient_receipt_error
- std::time::{Duration, Instant}
- tokio::sync::watch
- tracing::{debug, instrument, warn}
- ReceiptData

### Community 96 — StateCache (11 nodes, cohesion: 0.31)

- StateCache
- .addresses()
- .classify_for_fetch()
- .contains()
- .generation()
- .get()
- .get_arc()
- .is_empty()
- .len()
- .lookup_pool_state()
- .with_ttls()

### Community 97 — load_key_material() (11 nodes, cohesion: 0.20)

- wallet
- env_var()
- alloy::primitives::Address
- alloy::signers::local::PrivateKeySigner
- crate::config::{ExecutionConfig, OracleConfig, RoutingConfig, RpcConfig}
- std::fs
- std::path::Path
- super::*
- super::AppConfig
- zeroize::Zeroizing
- load_key_material()

### Community 98 — transfer_log() (10 nodes, cohesion: 0.20)

- profit_logs
- alloy::primitives::{Address, U256}
- alloy::primitives::{Log as PrimitiveLog, address}
- alloy::rpc::types::Log
- alloy::sol_types::SolEvent
- crate::abis::IERC20
- super::*
- nets_inflows_and_outflows_to_executor()
- parse_transfer_profit()
- transfer_log()

### Community 99 — simulate_woofi_swap() (10 nodes, cohesion: 0.33)

- woofi
- apply_woofi_fee()
- calc_base_amount_sell_quote()
- calc_quote_amount_sell_base()
- get_woofi_amount_out()
- has_positive_swap_factor()
- crate::core::types::{WoofiBaseTokenState, WoofiPoolState}
- ruint::aliases::U256
- super::fixed_point::ONE
- simulate_woofi_swap()

### Community 100 — FilterState (10 nodes, cohesion: 0.27)

- App
- .clamp_selection()
- .filtered_opportunities()
- .new()
- .passes_filter()
- .push_alert()
- .selected_opportunity()
- .uptime_secs()
- FilterState
- .new_default()

### Community 101 — UiBridge (10 nodes, cohesion: 0.36)

- build_graph_stats()
- UiBridge
- .new()
- .notify_config()
- .notify_hf_tick()
- .notify_lf_complete()
- .notify_status()
- .notify_trade()
- .sender()
- .try_send()

### Community 102 — field_slots_are_deterministic() (10 nodes, cohesion: 0.27)

- v4_storage
- compute_v4_pool_field_slot()
- compute_v4_pool_state_slot()
- decode_v4_liquidity()
- decode_v4_slot0()
- DecodedV4Slot0
- decodes_v4_fixture_slot0_and_liquidity()
- field_slots_are_deterministic()
- alloy::primitives::{FixedBytes, U256, keccak256}
- super::*

### Community 103 — route_call_count() (10 nodes, cohesion: 0.24)

- negative_cycle
- collect_negative_cycles_from_source()
- crate::core::types::{CycleEdges, Edge, FoundCycle, TokenIndex}
- crate::pipeline::cycle_finder::clamp_fee_bps
- crate::pipeline::spot_price::hop_penalty
- crate::pipeline::types::route_fingerprint
- crate::pipeline::weighted_graph::WeightedEdge
- smallvec::SmallVec
- is_simple_cycle()
- route_call_count()

### Community 104 — pool_meta_from_pair() (10 nodes, cohesion: 0.20)

- graph
- alloy::primitives::Address
- crate::core::types::{Edge, PoolIndex, ProtocolType, TokenIndex}
- crate::core::types::{PoolState, V2PoolState}
- crate::pipeline::arena::StateArena
- crate::pipeline::spot_price::compute_edge_log_weight_with_state
- crate::pipeline::types::{GraphEdge, PoolMeta, RoutingGraph}
- ruint::aliases::U256
- super::*
- pool_meta_from_pair()

### Community 105 — GraphCache (10 nodes, cohesion: 0.24)

- full_rebuild_interval()
- GraphCache
- .can_rescore_cycles()
- .cycles()
- .graph()
- .lf_pass_count()
- .needs_connectivity_rebuild()
- .needs_cycle_refind()
- .rescore_cached_cycles()
- .state_generation()

### Community 106 — test_encode_transfer_all_encodes_correctly() (10 nodes, cohesion: 0.27)

- approvals
- encode_approve_if_needed()
- encode_transfer_all()
- alloy::primitives::{Address, U256}
- alloy::sol_types::SolCall
- crate::abis::{ExecutorCall, IArbExecutor}
- super::*
- test_encode_approve_if_needed_different_amounts()
- test_encode_approve_if_needed_encodes_correctly()
- test_encode_transfer_all_encodes_correctly()

### Community 107 — spawn_mock_updates() (10 nodes, cohesion: 0.27)

- mock
- crate::core::types::{Edge, FoundCycle, PoolIndex, ProtocolType, TokenIndex}
- crate::tui::app::{BotStatus, TradeStatus}
- crate::tui::bridge::UiBridge
- crate::tui::update::{
    GraphStatsSnapshot, ScannerMetrics, UiOpportunity, UiUpdate, alert_info, alert_warn,
    trade_from_outcome,
}
- std::collections::HashMap
- mock_cycles()
- mock_graph_stats()
- mock_opportunity()
- spawn_mock_updates()

### Community 108 — mod (108) (10 nodes, cohesion: 0.20)

- mod
- pub use candidate::{
    CandidateBuildConfig, CandidateExecution, build_execution_candidate, evaluated_from_sim,
}
- pub use circuit_breaker::CircuitBreaker
- pub use flash_liquidity::{
    FlashLiquidityCache, PreparedDispatch, PrepareDispatchInput, collect_flash_tokens,
    prepare_evaluated_route,
}
- pub use flash_policy::{
    FlashLoanPolicy, hf_eval_flash_source, parse_flash_policy, parse_flash_source,
}
- pub use gas::{FeeSnapshot, compute_conservative_gas_price, conservative_gas_price_wei}
- pub use gas_oracle::GasOracle
- pub use opportunity_log::{OpportunityRecord, log_opportunity_evaluated, log_opportunity_outcome}
- pub use profit::{
    AssessProfitInput, ProfitEvalContext, ProfitThresholds, RouteProfitParams,
    assess_profit, build_assess_input, on_chain_min_profit_for_route,
    DEFAULT_PROFIT_SAFETY_MULTIPLIER_BPS,
}
- pub use service::{ExecutionOutcome, ExecutionService}

### Community 109 — is_transient_receipt_error() (10 nodes, cohesion: 0.20)

- rpc_errors
- classifies_already_known()
- classifies_nonce_too_low()
- classifies_underpriced()
- classify_submit_error()
- extract_tx_hash_from_error()
- alloy::primitives::B256
- super::*
- is_transient_receipt_error()
- SubmitAction

### Community 110 — render_protocols() (10 nodes, cohesion: 0.24)

- graph_stats
- crate::tui::app::App
- crate::tui::layout::split_horizontal
- crate::tui::theme::Theme
- ratatui::Frame
- ratatui::layout::{Constraint, Rect}
- ratatui::widgets::{Block, Borders, Cell, Row, Table}
- render()
- render_hubs()
- render_protocols()

### Community 111 — test_compute_route_hash_different_calls() (9 nodes, cohesion: 0.28)

- hash
- compute_route_hash()
- alloy::dyn_abi::DynSolValue
- alloy::primitives::{Address, U256}
- alloy::primitives::{FixedBytes, keccak256}
- crate::abis::ExecutorCall
- super::*
- test_compute_route_hash_deterministic()
- test_compute_route_hash_different_calls()

### Community 112 — get_next_sqrt_price_from_output() (9 nodes, cohesion: 0.33)

- sqrt_price_math
- get_amount0_delta()
- get_amount1_delta()
- get_next_sqrt_price_from_amount0_rounding_up()
- get_next_sqrt_price_from_amount1_rounding_down()
- get_next_sqrt_price_from_input()
- get_next_sqrt_price_from_output()
- ruint::aliases::U256
- super::full_math::{div_rounding_up, mul_div, mul_div_rounding_up}

### Community 113 — spawn_operator_balance_monitor() (9 nodes, cohesion: 0.39)

- run_hf_tick_logged()
- run_pass_loop()
- RuntimeContext
- .hf_context()
- .lf_context()
- .new()
- .with_ui_bridge()
- .with_ui_hook()
- spawn_operator_balance_monitor()

### Community 114 — SharedCycleBudget (9 nodes, cohesion: 0.33)

- collect_cycles_dfs_parallel()
- find_cycles()
- find_cycles_multi_pass()
- find_cycles_multi_pass_arc()
- finds_triangle_cycle()
- parallel_dfs_finds_cycles_on_hub_graph()
- SharedCycleBudget
- .new()
- .tick()

### Community 115 — bench_v2_triangle_sim() (9 nodes, cohesion: 0.22)

- local_sim
- bench_v2_triangle_sim()
- alloy::primitives::Address
- criterion::{Criterion, criterion_group, criterion_main}
- rpbot::core::types::{Edge, PoolState, ProtocolType, V2PoolState}
- rpbot::pipeline::arena::StateArena
- rpbot::pipeline::local_sim::simulate_route_minimal
- ruint::aliases::U256
- std::hint::black_box

### Community 116 — push_call() (9 nodes, cohesion: 0.39)

- build_balancer_plan()
- build_curve_plan()
- build_dodo_plan()
- build_plan()
- build_v2_plan()
- build_v3_plan()
- build_v4_plan()
- .from()
- push_call()

### Community 117 — returns_error_when_not_initialized() (9 nodes, cohesion: 0.31)

- .confirm()
- .new()
- .build()
- NonceState
- .init()
- .next_available()
- .prune_stale()
- reserves_and_confirms_sequential_nonces()
- returns_error_when_not_initialized()

### Community 118 — TuiUiHook (9 nodes, cohesion: 0.22)

- hook
- crate::orchestrator::hf::HfTickResult
- crate::orchestrator::ui_hook::PipelineUiHook
- crate::tui::UiBridge
- TuiUiHook
- .new()
- .on_gas_update()
- .on_hf_tick()
- .on_lf_complete()

### Community 119 — parse_flash_source() (9 nodes, cohesion: 0.22)

- flash_policy
- FlashLoanPolicy
- hf_eval_flash_source()
- hf_eval_flash_source_pessimistic_in_auto()
- crate::core::types::FlashLoanSource
- super::*
- parse_flash_policy()
- parse_flash_policy_variants()
- parse_flash_source()

### Community 120 — resolve_woofi_router_uses_explicit() (9 nodes, cohesion: 0.39)

- create_test_hop()
- encode_woofi_hop()
- encode_woofi_hop_first_call_is_approval()
- encode_woofi_hop_returns_two_calls()
- encode_woofi_hop_second_call_is_swap()
- encode_woofi_hop_zero_slippage_fails()
- resolve_woofi_router()
- resolve_woofi_router_defaults_to_v2()
- resolve_woofi_router_uses_explicit()

### Community 121 — try_from_env() (8 nodes, cohesion: 0.32)

- HyperSyncService
- .chain_id()
- .from_config()
- .get_height()
- .get_transaction_receipt()
- .inner()
- .stream_height()
- try_from_env()

### Community 122 — take_pending_dispatch() (8 nodes, cohesion: 0.25)

- dispatch_queue
- crate::core::types::EvaluatedRoute
- crate::pipeline::arena::StateArena
- crate::pipeline::types::PoolMeta
- parking_lot::Mutex
- PendingDispatch
- queue_pending_dispatch()
- take_pending_dispatch()

### Community 123 — render() (123) (8 nodes, cohesion: 0.25)

- config_panel
- crate::tui::app::App
- crate::tui::theme::Theme
- ratatui::Frame
- ratatui::layout::Rect
- ratatui::text::{Line, Span}
- ratatui::widgets::{Block, Borders, Paragraph}
- render()

### Community 124 — rpc (8 nodes, cohesion: 0.25)

- rpc
- alloy::network::{Ethereum, EthereumWallet}
- alloy::providers::{Provider, ProviderBuilder}
- alloy::signers::local::PrivateKeySigner
- crate::config::AppConfig
- reqwest::Client
- std::time::Duration
- tracing::warn

### Community 125 — SpotTable (8 nodes, cohesion: 0.50)

- spot_table_reuses_v2_entries()
- SpotTable
- .build_for_graph()
- .ensure_edge()
- .get()
- .new()
- .set()
- .slot()

### Community 126 — render() (126) (8 nodes, cohesion: 0.25)

- trades
- crate::tui::app::App
- crate::tui::text::truncate_str
- crate::tui::theme::Theme
- ratatui::Frame
- ratatui::layout::{Constraint, Rect}
- ratatui::widgets::{Block, Borders, Cell, Row, Table}
- render()

### Community 127 — render() (8 nodes, cohesion: 0.25)

- portfolio
- crate::tui::app::App
- crate::tui::theme::Theme
- ratatui::Frame
- ratatui::layout::{Constraint, Rect}
- ratatui::text::{Line, Span}
- ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table}
- render()

### Community 128 — dodo (8 nodes, cohesion: 0.25)

- dodo
- alloy::primitives::{Address, U256}
- alloy::sol_types::SolCall
- crate::abis::{ExecutorCall, IDodoPool, IERC20}
- crate::core::types::{Edge, PoolIndex, TokenIndex}
- crate::services::execution::calldata::approvals::encode_transfer_all
- crate::services::execution::calldata::types::CalldataHop
- super::*

### Community 129 — u256_to_f64() (8 nodes, cohesion: 0.29)

- util
- ruint::aliases::U256
- std::time::{SystemTime, UNIX_EPOCH}
- super::*
- now_ms()
- test_now_ms_monotonic()
- test_u256_to_f64_zero()
- u256_to_f64()

### Community 130 — test_new_and_default_produce_identical_state() (8 nodes, cohesion: 0.54)

- .default()
- SnapshotStore
- .default()
- .init()
- .new()
- .publish()
- .read()
- test_new_and_default_produce_identical_state()

### Community 131 — circuit_breaker (8 nodes, cohesion: 0.25)

- circuit_breaker
- parking_lot::RwLock
- ruint::aliases::U256
- std::sync::atomic::{AtomicBool, AtomicU32, Ordering}
- std::time::Duration
- std::time::{Duration, Instant}
- super::*
- tracing::{info, warn}

### Community 132 — split_vertical() (8 nodes, cohesion: 0.29)

- layout
- ratatui::layout::{Constraint, Direction, Layout, Rect}
- kpi_row()
- opp_layout()
- overview_layout()
- root_layout()
- split_horizontal()
- split_vertical()

### Community 133 — resolve_token_to_matic_rate() (8 nodes, cohesion: 0.25)

- rates
- crate::core::constants::MIN_TOKEN_TO_MATIC_RATE
- crate::core::types::TokenIndex
- crate::pipeline::arena::StateArena
- crate::services::oracle::price_oracle::bootstrap_matic_rate_per_unit
- ruint::aliases::U256
- rustc_hash::FxHashMap
- resolve_token_to_matic_rate()

### Community 134 — ArenaInner (8 nodes, cohesion: 0.25)

- arena
- ArenaInner
- alloy::primitives::Address
- crate::core::types::{PoolIndex, PoolState, TokenIndex}
- crate::services::discovery::{DiscoveredPool, discovered_to_pool_meta}
- crate::services::state_cache::StateCache
- rustc_hash::FxHashMap
- std::sync::Arc

### Community 135 — FlashLiquidityCache (8 nodes, cohesion: 0.39)

- collect_flash_tokens()
- decode_balance()
- FlashLiquidityCache
- .default()
- .new()
- .refresh()
- .snapshot()
- .with_addresses()

### Community 136 — reweight_graph_with_spot() (8 nodes, cohesion: 0.32)

- compute_edge_log_weight()
- compute_edge_log_weight_with_table()
- edge_log_weight_from_spot()
- fee_log_weight()
- gas_log_penalty_for_cycle()
- hop_penalty()
- rescore_cycles_with_table_and_gas()
- reweight_graph_with_spot()

### Community 137 — encode_curve_hop_zero_slippage_fails() (8 nodes, cohesion: 0.43)

- create_test_hop()
- encode_curve_hop()
- encode_curve_hop_first_call_is_approval()
- encode_curve_hop_rejects_same_token_indices()
- encode_curve_hop_returns_two_calls()
- encode_curve_hop_second_call_is_exchange()
- encode_curve_hop_supports_crypto_pools()
- encode_curve_hop_zero_slippage_fails()

### Community 138 — render() (138) (8 nodes, cohesion: 0.25)

- simulations
- crate::tui::app::App
- crate::tui::text::truncate_str
- crate::tui::theme::Theme
- ratatui::Frame
- ratatui::layout::{Constraint, Rect}
- ratatui::widgets::{Block, Borders, Cell, Row, Table}
- render()

### Community 139 — render() (139) (8 nodes, cohesion: 0.29)

- help
- centered_rect()
- crate::tui::theme::Theme
- ratatui::Frame
- ratatui::layout::Rect
- ratatui::text::{Line, Span}
- ratatui::widgets::{Block, Borders, Clear, Paragraph}
- render()

### Community 140 — OpportunityRecord (8 nodes, cohesion: 0.25)

- opportunity_log
- crate::core::types::ProfitAssessment
- ruint::aliases::U256
- tracing::info
- log_opportunity_evaluated()
- log_opportunity_outcome()
- OpportunityRecord
- .from_assessment()

### Community 141 — HasuraClient (8 nodes, cohesion: 0.46)

- field_selector_degrades_when_hooks_missing()
- HasuraClient
- .block_cursor_where()
- .discover_pools()
- .fetch_token_metas()
- .new()
- .query()
- .query_pool_meta_page()

### Community 142 — render() (142) (8 nodes, cohesion: 0.25)

- header
- crate::tui::app::App
- crate::tui::theme::Theme
- ratatui::Frame
- ratatui::layout::Rect
- ratatui::text::{Line, Span}
- ratatui::widgets::{Block, Borders, Paragraph}
- render()

### Community 143 — encode_v2_hop_zero_slippage_fails() (7 nodes, cohesion: 0.48)

- create_test_hop()
- encode_v2_hop()
- encode_v2_hop_creates_two_calls()
- encode_v2_hop_first_call_is_transfer()
- encode_v2_hop_respects_zero_for_one()
- encode_v2_hop_second_call_is_swap()
- encode_v2_hop_zero_slippage_fails()

### Community 144 — max_pool_index() (7 nodes, cohesion: 0.38)

- clamp_fee_bps()
- collect_cycles_dfs()
- collect_cycles_dfs_single_start()
- CycleBudget
- .tick()
- edges_from_path()
- max_pool_index()

### Community 145 — flat_routing_and_oracle_aliases_apply() (7 nodes, cohesion: 0.48)

- default_discovery_interval_ms()
- default_hasura_url()
- default_hf_interval_ms()
- default_lf_interval_ms()
- default_max_multicall_calls()
- flat_execution_mode_overrides_default()
- flat_routing_and_oracle_aliases_apply()

### Community 146 — GasOracle (7 nodes, cohesion: 0.52)

- GasOracle
- .conservative_gas_price()
- .default()
- .new()
- .refresh_once()
- .snapshot()
- .start_background()

### Community 147 — encode_dodo_hop_with_transfer_all() (7 nodes, cohesion: 0.48)

- create_test_hop()
- encode_dodo_hop()
- encode_dodo_hop_generates_valid_calls()
- encode_dodo_hop_respects_zero_for_one()
- encode_dodo_hop_returns_two_calls()
- encode_dodo_hop_with_explicit_transfer()
- encode_dodo_hop_with_transfer_all()

### Community 148 — set_graph_rebuild_interval() (7 nodes, cohesion: 0.48)

- connectivity_fingerprint()
- .get_or_rescore_graph()
- .new()
- .store()
- rebuilds_when_pool_becomes_tradable()
- reuses_graph_when_only_reserves_change()
- set_graph_rebuild_interval()

### Community 149 — pending_nonce() (7 nodes, cohesion: 0.33)

- NonceManager
- .address()
- .in_flight_count()
- .initialize()
- .resync()
- .stale_count()
- pending_nonce()

### Community 150 — spawn_pool_log_feed() (7 nodes, cohesion: 0.43)

- http_to_wss()
- PoolLogFeed
- .handle_log()
- .new()
- .run()
- .run_subscriptions()
- spawn_pool_log_feed()

### Community 151 — noop_ui_hook() (7 nodes, cohesion: 0.29)

- ui_hook
- crate::core::types::FoundCycle
- crate::orchestrator::hf::HfTickResult
- crate::pipeline::arena::StateArena
- crate::pipeline::types::PoolMeta
- std::sync::Arc
- noop_ui_hook()

### Community 152 — ring_fixture() (152) (7 nodes, cohesion: 0.29)

- bench_connectivity_fingerprint()
- bench_cycle_find_scaling()
- bench_cycle_rescore()
- bench_graph_build_scaling()
- bench_graph_rescore_scaling()
- bench_sim_route()
- ring_fixture()

### Community 153 — StreamTrigger (7 nodes, cohesion: 0.33)

- .default()
- .new()
- .new()
- StreamTrigger
- .default()
- .new()
- .take_stream_triggered()

### Community 154 — SwapStepResult (7 nodes, cohesion: 0.29)

- swap_math
- compute_swap_step()
- crate::core::constants::FEE_PIPS_SCALE
- ruint::aliases::U256
- super::full_math::{mul_div, mul_div_rounding_up}
- super::sqrt_price_math::{
    get_amount0_delta, get_amount1_delta, get_next_sqrt_price_from_input,
    get_next_sqrt_price_from_output,
}
- SwapStepResult

### Community 155 — pow_down() (7 nodes, cohesion: 0.38)

- fixed_point
- complement()
- ruint::aliases::U256
- super::log_exp_math::log_exp_pow
- mul_down()
- mul_up()
- pow_down()

### Community 156 — RouteEncodeConfig (7 nodes, cohesion: 0.29)

- types
- BuiltArbTx
- CalldataHop
- alloy::primitives::{Address, Bytes, FixedBytes, U256}
- crate::abis::ExecutorCall
- crate::core::types::Edge
- RouteEncodeConfig

### Community 157 — token_label() (7 nodes, cohesion: 0.48)

- compact_route()
- cycle_to_ui_opportunity()
- detail_route_tree()
- protocol_label_for_edge()
- protocols_in_cycle()
- short_addr()
- token_label()

### Community 158 — .parse_str() (7 nodes, cohesion: 0.33)

- routing
- CycleFinderKind
- .deserialize()
- .fmt()
- .parse_str()
- serde::{Deserialize, Deserializer, Serialize}
- std::fmt

### Community 159 — mod (159) (6 nodes, cohesion: 0.33)

- mod
- pub use app::{App, Tab}
- pub use bridge::UiBridge
- pub use hook::TuiUiHook
- pub use run::run_tui
- pub use update::UiUpdate

### Community 160 — stable_swap_zero_a_returns_zero() (6 nodes, cohesion: 0.33)

- curve
- crate::core::types::CurvePoolState
- ruint::aliases::U256
- super::*
- super::fixed_point::ONE
- stable_swap_zero_a_returns_zero()

### Community 161 — RoutingConfig (6 nodes, cohesion: 0.33)

- default_cycle_finder()
- default_enumeration_max_paths()
- default_max_hops()
- default_ternary_search_iterations()
- RoutingConfig
- .default()

### Community 162 — v2_state() (6 nodes, cohesion: 0.33)

- bench_compute_spot_all_edges()
- bench_hf_rescore_cap()
- bench_rescore_cycles()
- build_fixture()
- reserve()
- v2_state()

### Community 163 — enrich_v3_ticks() (6 nodes, cohesion: 0.33)

- tick_fetch
- collect_v3_pool_addresses()
- enrich_v3_ticks()
- alloy::primitives::Address
- crate::core::types::{FoundCycle, ProtocolType}
- crate::pipeline::arena::StateArena

### Community 164 — encode_balancer_hop_zero_slippage_fails() (6 nodes, cohesion: 0.53)

- create_test_hop()
- encode_balancer_hop()
- encode_balancer_hop_first_call_is_approval()
- encode_balancer_hop_returns_two_calls()
- encode_balancer_hop_second_call_is_swap()
- encode_balancer_hop_zero_slippage_fails()

### Community 165 — parse_addr() (6 nodes, cohesion: 0.33)

- error
- ArbError
- .from()
- alloy::primitives::Address
- thiserror::Error
- parse_addr()

### Community 166 — ReceiptPoller (6 nodes, cohesion: 0.53)

- fetch_receipt_from_rpc()
- ReceiptPoller
- .default()
- .new()
- .wait()
- .wait_with_hypersync()

### Community 167 — render_tabs() (6 nodes, cohesion: 0.47)

- mod
- crate::tui::app::{App, Tab}
- ratatui::Frame
- render_main()
- render_tab_content()
- render_tabs()

### Community 168 — test_expired_entry_is_evicted() (6 nodes, cohesion: 0.53)

- generation_increments_on_insert()
- .default()
- .insert()
- .new()
- .patch_pool()
- test_expired_entry_is_evicted()

### Community 169 — mul_div_rounding_up() (6 nodes, cohesion: 0.40)

- full_math
- div_rounding_up()
- div_rounding_up_or_zero()
- ruint::aliases::U256
- mul_div()
- mul_div_rounding_up()

### Community 170 — WalletSecrets (5 nodes, cohesion: 0.40)

- WalletSecrets
- .dry_run()
- .has_signer()
- .operator_address()
- .signer()

### Community 171 — handle_key() (5 nodes, cohesion: 0.50)

- events
- handle_filter_input()
- handle_key()
- crate::tui::app::{App, Tab}
- crossterm::event::{KeyCode, KeyEvent, KeyModifiers}

### Community 172 — profile_routing() (5 nodes, cohesion: 0.60)

- build_ring()
- main()
- profile_math()
- profile_price()
- profile_routing()

### Community 173 — OracleConfig (5 nodes, cohesion: 0.40)

- default_oracle_enabled()
- default_pyth_hermes_url()
- default_tick_word_range()
- OracleConfig
- .default()

### Community 174 — AppConfig (5 nodes, cohesion: 0.60)

- AppConfig
- .is_dry_run()
- .min_profit_matic_wei()
- .state_rpc_url()
- .validate()

### Community 175 — to_xp() (5 nodes, cohesion: 0.40)

- get_curve_stable_amount_out()
- get_d()
- get_y()
- stable_swap_produces_output()
- to_xp()

### Community 176 — selects_unfetched_pools_in_order() (5 nodes, cohesion: 0.40)

- fetch_missing_pool_states()
- includes_curve_in_fetch_targets()
- prioritizes_never_fetched_over_invalid_retries()
- select_fetch_targets()
- selects_unfetched_pools_in_order()

### Community 177 — liq() (5 nodes, cohesion: 0.40)

- auto_caps_when_neither_sufficient()
- auto_falls_back_to_aave()
- auto_rejects_when_no_liquidity()
- balancer_only_caps_partial()
- liq()

### Community 178 — preserves_balancer_pool_id_from_row() (5 nodes, cohesion: 0.40)

- accepts_32_byte_v4_pool_id()
- accepts_v4_when_hooks_field_missing_assumes_hookless()
- drops_v4_hook_pools()
- parse_pool_meta_row()
- preserves_balancer_pool_id_from_row()

### Community 179 — rescore_graph_in_place() (5 nodes, cohesion: 0.40)

- build_graph()
- builds_two_pool_graph()
- edges_for_multi_token()
- edges_for_pair()
- rescore_graph_in_place()

### Community 180 — slippage_applied_to_gross_profit_not_amount_in() (5 nodes, cohesion: 0.40)

- assess_profit()
- flash_loan_fee_bps()
- net_profit_after_gas_from_sim()
- roi_threshold_rejects_low_margin()
- slippage_applied_to_gross_profit_not_amount_in()

### Community 181 — NoopUiHook (5 nodes, cohesion: 0.40)

- NoopUiHook
- .on_gas_update()
- .on_hf_tick()
- .on_lf_complete()
- PipelineUiHook

### Community 182 — live_mode_rejects_missing_key() (5 nodes, cohesion: 0.60)

- base_config()
- clears_private_key_from_config_after_load()
- dry_run_allows_missing_key()
- live_mode_rejects_missing_key()
- .load()

### Community 183 — zero_gas_means_no_revert_penalty() (5 nodes, cohesion: 0.40)

- default_input()
- non_18_decimal_token_gas_cost_is_accurate()
- rate_failure_rejects_trade()
- safety_multiplier_override_works()
- zero_gas_means_no_revert_penalty()

### Community 184 — PoolMetaFieldSelector (5 nodes, cohesion: 0.40)

- is_missing_graphql_field_error()
- PoolMetaFieldSelector
- .current()
- .degrade_for_error()
- .new()

### Community 185 — plan_single() (5 nodes, cohesion: 0.40)

- auto_prefers_balancer_when_sufficient()
- balancer_only_rejects_when_vault_and_route_cap_zero()
- plan_auto()
- plan_flash_loan()
- plan_single()

### Community 186 — StreamAddressSet (5 nodes, cohesion: 0.40)

- StreamAddressSet
- .read()
- .replace()
- .watch()
- .subscribe()

### Community 187 — u128_fast_path_matches_u256_for_realistic_reserves() (5 nodes, cohesion: 0.60)

- constant_product_swap()
- get_amount_out()
- get_amount_out_u128()
- simulate_v2_swap()
- u128_fast_path_matches_u256_for_realistic_reserves()

### Community 188 — DeadlineGuard (5 nodes, cohesion: 0.40)

- deadline
- DeadlineGuard
- .new()
- .tick()
- std::time::{Duration, Instant}

### Community 189 — truncate_str() (4 nodes, cohesion: 0.67)

- text
- super::*
- truncate_respects_char_boundaries()
- truncate_str()

### Community 190 — bigint_sqrt() (3 nodes, cohesion: 0.67)

- int_sqrt
- bigint_sqrt()
- ruint::aliases::U256

### Community 191 — constants (3 nodes, cohesion: 0.67)

- constants
- alloy::primitives::{Address, address}
- ruint::aliases::U256

### Community 192 — mod (2 nodes, cohesion: 1.00)

- mod
- pub use pass_loop::{RuntimeContext, run_pass_loop}

### Community 193 — abis (2 nodes, cohesion: 1.00)

- abis
- alloy::sol

### Community 194 — lib (1 nodes, cohesion: 1.00)

- lib

### Community 195 — mod (195) (1 nodes, cohesion: 1.00)

- mod

### Community 196 — mod (196) (1 nodes, cohesion: 1.00)

- mod

### Community 197 — mod (197) (1 nodes, cohesion: 1.00)

- mod

### Community 198 — mod (198) (1 nodes, cohesion: 1.00)

- mod

### Community 199 — mod (199) (1 nodes, cohesion: 1.00)

- mod

### Community 200 — mod (200) (1 nodes, cohesion: 1.00)

- mod

## 🕳️ Knowledge Gaps

**Isolated nodes** (7):
- mod
- mod
- mod
- mod
- mod
- lib
- mod

**Thin communities** (< 3 nodes): 9 communities

## 💰 Token Cost

| File | Tokens |
|------|--------|
| input | 0 |
| output | 0 |
| **Total** | **0** |

## ❓ Suggested Questions

1. How does 'src_services_execution_profit_rs_zero_gas_means_no_revert_penalty' relate to 3 different communities (slippage_applied_to_gross_profit_not_amount_in(), slippage_adjusted(), zero_gas_means_no_revert_penalty())?
1. How does 'src_services_execution_flash_liquidity_rs_liq' relate to 3 different communities (TokenFlashLiquidity, liq(), plan_single())?
1. How does 'src_pipeline_cycle_finder_rs_max_pool_index' relate to 3 different communities (prioritize_cycle_start_tokens_from_out_degrees(), SharedCycleBudget, max_pool_index())?
1. How does 'src_services_execution_flash_liquidity_rs_plan_flash_loan' relate to 3 different communities (plan_single(), TokenFlashLiquidity, liq())?
1. How does 'src_services_partial_cache_mod_rs_flush_updates_v2_reserves' relate to 3 different communities (StreamTrigger, apply_slim_to_pool_state(), SlimPoolState)?
1. How does 'src_pipeline_graph_cache_rs' relate to 3 different communities (pool_fingerprint(), set_graph_rebuild_interval(), GraphCache)?
1. How does 'src_services_partial_cache_mod_rs' relate to 4 different communities (StreamTrigger, StreamAddressSet, apply_slim_to_pool_state(), SlimPoolState)?

---
_Generated by graphify-rs_
