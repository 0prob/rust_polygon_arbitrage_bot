mod support;

pub mod aave;
pub mod balancer_fee;
pub mod balancer_verify;
pub mod calldata;
pub mod candidate;
pub mod dryrun;
pub mod flash_liquidity;
pub mod gas_oracle;
pub mod mempool;
pub mod private_submit;
pub mod profit;
pub mod quote;
pub mod receipt;
pub mod recovery;
pub mod revert_decoder;
pub mod service;
pub mod submit;

pub mod flash_policy {
    pub use super::support::{FlashLoanPolicy, hf_eval_flash_source, try_parse_flash_policy};
}
pub mod profit_logs {
    pub use super::support::parse_transfer_profit;
}
pub mod rpc_errors {
    pub use super::support::{
        SubmitAction, classify_submit_error, extract_tx_hash_from_error, is_rpc_rate_limited,
        is_transient_receipt_error,
    };
}
pub mod gas {
    pub use super::support::{
        FeeSnapshot, GAS_FALLBACK_BUFFER_BPS, GAS_LIMIT_BUFFER_BPS, MIN_PRIORITY_FEE_PER_GAS,
        PER_HOP_EXECUTOR_GAS_OVERHEAD, ROUTE_EXECUTION_GAS_OVERHEAD, buffer_gas_limit,
        compute_assessment_gas_price, compute_conservative_gas_price, estimate_route_gas_from_hops,
        estimate_route_gas_from_hops_evm, estimate_route_storage_gas, pick_buffered_gas_limit,
        pick_live_gas_limit, pick_live_gas_limit_with_buffer, profit_reassess_gas,
        scaled_simulated_gas, submit_gas_basis, u256_to_u128,
    };
}
pub mod impact_slippage {
    pub use super::support::{depth_impact_slippage_bps_with_base, effective_slippage_bps};
}
pub mod nonce;

pub use aave::{
    AaveRefreshStats, AaveReserveStatus, aave_flash_reserve_status_live,
    aave_reserve_flash_eligible, fetch_and_cache_aave_flash_loan_fee_bps, log_aave_gate_summary,
    record_aave_mark_inactive, record_aave_prepare_skip_inactive,
    refresh_aave_flash_fee_with_fallback,
};
pub use balancer_fee::{
    fetch_and_cache_balancer_flash_loan_fee_pct, refresh_balancer_flash_fee_with_fallback,
};
pub use balancer_verify::{
    BalancerBatchReject, BatchQueryOutcome, BatchQueryVerdict, balancer_batch_within_max_in_ratio,
    batch_profit_covers_min, evaluate_batch_query, log_balancer_batch_filter_summary,
    log_balancer_prepare_gate_summary, query_balancer_batch_profit, record_balancer_batch_reject,
    record_balancer_filter_accept, record_balancer_filter_window, record_balancer_prepare_skip,
};
pub use candidate::{
    CandidateBuildConfig, CandidateExecution, build_execution_candidate, evaluated_from_sim,
    hash_cycle_edges,
};
pub use flash_liquidity::{
    CycleFlashContext, FlashLiquidityCache, FlashLiquiditySnapshot, FlashLoanDiagnostics,
    FlashRejectReason, PrepareDispatchInput, PreparedDispatch, balancer_route_flash_feasible,
    build_cycle_flash_context, collect_flash_tokens_for_cycle, cycle_has_aave_listed_token,
    flash_reject_reason, prefer_aave_flash_start, prepare_evaluated_route,
    resolve_flash_source_for_cycle, resolve_flash_source_with_context, rotate_cycle_to_start,
    token_eligible_for_flash_borrow_graph, token_flash_borrow_proven_unviable,
    token_flash_liquidity_borrowable,
};
pub use flash_policy::{FlashLoanPolicy, hf_eval_flash_source, try_parse_flash_policy};
pub use gas::{FeeSnapshot, compute_assessment_gas_price, compute_conservative_gas_price};
pub use gas_oracle::{GasOracle, ROUTE_GAS_CACHE_MIN_ROUTES, RouteGasLookup};
pub use profit::{
    AssessProfitInput, AssessmentGas, DEFAULT_PROFIT_SAFETY_MULTIPLIER_BPS, ProfitEvalContext,
    ProfitThresholds, RouteAssessRequest, RouteProfitParams, assess_profit, assess_route_from_sim,
    assess_route_profit, assessment_gas_for_edges, assessment_gas_units, compound_slippage_bps,
    modeled_net_profit_tokens,
    on_chain_min_profit_for_route, on_chain_min_profit_from_assessment,
    profit_priority_tip_per_gas, profit_priority_uplift_wei, safety_floor_matic_wei,
};
pub use service::{ExecutionOutcome, ExecutionService};
