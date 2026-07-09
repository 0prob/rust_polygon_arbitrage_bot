mod support;

pub mod balancer_verify;
pub mod calldata;
pub mod candidate;
pub mod dryrun;
pub mod flash_liquidity;
pub mod gas_oracle;
pub mod private_submit;
pub mod profit;
pub mod quote;
pub mod receipt;
pub mod recovery;
pub mod revert_decoder;
pub mod service;
pub mod submit;

pub mod flash_policy {
    pub use super::support::{FlashLoanPolicy, hf_eval_flash_source, parse_flash_policy};
}
pub mod profit_logs {
    pub use super::support::parse_transfer_profit;
}
pub mod rpc_errors {
    pub use super::support::{
        SubmitAction, classify_submit_error, extract_tx_hash_from_error, is_transient_receipt_error,
    };
}
pub mod gas {
    pub use super::support::{
        FeeSnapshot, GAS_FALLBACK_BUFFER_BPS, GAS_LIMIT_BUFFER_BPS, PER_HOP_EXECUTOR_GAS_OVERHEAD,
        ROUTE_EXECUTION_GAS_OVERHEAD, buffer_gas_limit, compute_conservative_gas_price,
        estimate_route_gas_from_hops, estimate_route_gas_from_hops_evm, estimate_route_storage_gas,
        pick_buffered_gas_limit, pick_live_gas_limit, pick_live_gas_limit_with_buffer,
        profit_reassess_gas, scaled_simulated_gas, submit_gas_basis, u256_to_u128,
    };
}
pub mod impact_slippage {
    pub use super::support::{
        depth_impact_slippage_bps, depth_impact_slippage_bps_with_base, effective_slippage_bps,
    };
}
pub mod nonce;

pub use candidate::{
    CandidateBuildConfig, CandidateExecution, build_execution_candidate, evaluated_from_sim,
    hash_cycle_edges,
};
pub use flash_liquidity::{
    FlashLiquidityCache, PrepareDispatchInput, PreparedDispatch, balancer_route_flash_feasible,
    collect_flash_tokens_for_cycle, cycle_has_aave_listed_token, prefer_aave_flash_start,
    prepare_evaluated_route, resolve_flash_source_for_cycle, rotate_cycle_to_start,
};
pub use flash_policy::{FlashLoanPolicy, hf_eval_flash_source, parse_flash_policy};
pub use gas::{FeeSnapshot, compute_conservative_gas_price};
pub use gas_oracle::{GasOracle, ROUTE_GAS_CACHE_MIN_ROUTES, RouteGasLookup};
pub use profit::{
    AssessProfitInput, AssessmentGas, DEFAULT_PROFIT_SAFETY_MULTIPLIER_BPS, ProfitError,
    ProfitEvalContext, ProfitThresholds, RouteAssessRequest, RouteProfitParams, assess_profit,
    assess_route_from_sim, assess_route_profit, assessment_gas_units, modeled_net_profit_tokens,
    on_chain_min_profit_for_route, on_chain_min_profit_from_assessment, profit_priority_uplift_wei,
    safety_floor_matic_wei,
};
pub use service::{ExecutionOutcome, ExecutionService};
