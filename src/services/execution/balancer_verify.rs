use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::network::Ethereum;
use alloy::primitives::{Address, I256, U256};
use alloy::providers::Provider;
use alloy::sol_types::SolCall;
use tokio::time::timeout;

use crate::abis::IBalancerVault;
use crate::core::constants::BALANCER_VAULT;
use crate::core::math::balancer::exceeds_balancer_max_in_ratio;
use crate::core::types::{FlashLoanSource, PoolState};
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::encoders::balancer::{
    build_balancer_batch_swap_request, encode_balancer_batch_route,
};
use crate::services::execution::calldata::{
    CalldataHop, ExecutorEntrypoint, build_arb_calldata,
};
use crate::services::execution::profit::on_chain_min_profit_for_route;

const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
/// `queryBatchSwap` is a static vault view; cap avoids RPC default gas overflow on heavy batches.
const BALANCER_QUERY_BATCH_GAS: u64 = 2_000_000;
/// `executeArbDirect` eth_call confirmation after vault query (batch + profit asserts).
const BALANCER_DIRECT_CONFIRM_GAS: u64 = 3_000_000;
const BALANCER_GIVEN_IN: u8 = 0;
const DIRECT_CONFIRM_DEADLINE_SECS: u64 = 120;

fn query_block_id(block_number: Option<u64>) -> Option<BlockId> {
    block_number
        .filter(|&b| b > 0)
        .map(|block| BlockId::Number(BlockNumberOrTag::Number(block)))
}

/// Net vault delta for the profit token: negative means the vault sends tokens (profit).
fn profit_from_vault_delta(delta: I256) -> Option<U256> {
    if delta >= I256::ZERO {
        return None;
    }
    U256::try_from(-delta).ok()
}

/// Outcome of vault `queryBatchSwap` simulation for an `executeArbDirect` batch route.
pub enum BatchQueryOutcome {
    Profit(U256),
    NonPositiveDelta(I256),
    RpcError(String),
    Timeout,
    BuildFailed,
    DecodeFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BalancerBatchReject {
    MissingStartToken,
    CalldataBuildFailed,
    MaxInRatio,
    ProfitBelowMin,
    NonPositiveDelta,
    ReassessAfterOnChain,
    /// Vault `queryBatchSwap` was profitable but `executeArbDirect` eth_call realized 0.
    ZeroRealized,
    RpcError,
    Timeout,
    BuildDecodeFailed,
}

static BAL_FILTER_IN: AtomicU32 = AtomicU32::new(0);
static BAL_FILTER_ACCEPT: AtomicU32 = AtomicU32::new(0);
static BAL_FILTER_SKIP_VERIFIED: AtomicU32 = AtomicU32::new(0);
static BAL_FILTER_PASSTHROUGH: AtomicU32 = AtomicU32::new(0);
static BAL_PREPARE_SKIP: AtomicU32 = AtomicU32::new(0);
static BAL_REJECT_MAX_IN: AtomicU32 = AtomicU32::new(0);
static BAL_REJECT_BUILD: AtomicU32 = AtomicU32::new(0);
static BAL_REJECT_PROFIT_FLOOR: AtomicU32 = AtomicU32::new(0);
static BAL_REJECT_NON_POS: AtomicU32 = AtomicU32::new(0);
static BAL_REJECT_REASSESS: AtomicU32 = AtomicU32::new(0);
static BAL_REJECT_ZERO_REALIZED: AtomicU32 = AtomicU32::new(0);
static BAL_REJECT_RPC: AtomicU32 = AtomicU32::new(0);
static BAL_REJECT_TIMEOUT: AtomicU32 = AtomicU32::new(0);
static BAL_REJECT_DECODE: AtomicU32 = AtomicU32::new(0);

pub fn record_balancer_batch_reject(reason: BalancerBatchReject) {
    match reason {
        BalancerBatchReject::MissingStartToken => {}
        BalancerBatchReject::CalldataBuildFailed => {
            BAL_REJECT_BUILD.fetch_add(1, Ordering::Relaxed);
        }
        BalancerBatchReject::MaxInRatio => {
            BAL_REJECT_MAX_IN.fetch_add(1, Ordering::Relaxed);
        }
        BalancerBatchReject::ProfitBelowMin => {
            BAL_REJECT_PROFIT_FLOOR.fetch_add(1, Ordering::Relaxed);
        }
        BalancerBatchReject::NonPositiveDelta => {
            BAL_REJECT_NON_POS.fetch_add(1, Ordering::Relaxed);
        }
        BalancerBatchReject::ReassessAfterOnChain => {
            BAL_REJECT_REASSESS.fetch_add(1, Ordering::Relaxed);
        }
        BalancerBatchReject::ZeroRealized => {
            BAL_REJECT_ZERO_REALIZED.fetch_add(1, Ordering::Relaxed);
        }
        BalancerBatchReject::RpcError => {
            BAL_REJECT_RPC.fetch_add(1, Ordering::Relaxed);
        }
        BalancerBatchReject::Timeout => {
            BAL_REJECT_TIMEOUT.fetch_add(1, Ordering::Relaxed);
        }
        BalancerBatchReject::BuildDecodeFailed => {
            BAL_REJECT_DECODE.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn record_balancer_filter_accept() {
    BAL_FILTER_ACCEPT.fetch_add(1, Ordering::Relaxed);
}

pub fn record_balancer_prepare_skip() {
    BAL_PREPARE_SKIP.fetch_add(1, Ordering::Relaxed);
}

pub fn record_balancer_filter_window(
    query_jobs: u32,
    skip_already_verified: u32,
    passthrough_mixed: u32,
) {
    BAL_FILTER_IN.fetch_add(query_jobs, Ordering::Relaxed);
    BAL_FILTER_SKIP_VERIFIED.fetch_add(skip_already_verified, Ordering::Relaxed);
    BAL_FILTER_PASSTHROUGH.fetch_add(passthrough_mixed, Ordering::Relaxed);
}

pub fn log_balancer_batch_filter_summary() {
    let jobs = BAL_FILTER_IN.load(Ordering::Relaxed);
    let accept = BAL_FILTER_ACCEPT.load(Ordering::Relaxed);
    let skip_verified = BAL_FILTER_SKIP_VERIFIED.load(Ordering::Relaxed);
    let passthrough = BAL_FILTER_PASSTHROUGH.load(Ordering::Relaxed);
    let rejects = BAL_REJECT_MAX_IN.load(Ordering::Relaxed)
        + BAL_REJECT_BUILD.load(Ordering::Relaxed)
        + BAL_REJECT_PROFIT_FLOOR.load(Ordering::Relaxed)
        + BAL_REJECT_NON_POS.load(Ordering::Relaxed)
        + BAL_REJECT_REASSESS.load(Ordering::Relaxed)
        + BAL_REJECT_ZERO_REALIZED.load(Ordering::Relaxed)
        + BAL_REJECT_RPC.load(Ordering::Relaxed)
        + BAL_REJECT_TIMEOUT.load(Ordering::Relaxed)
        + BAL_REJECT_DECODE.load(Ordering::Relaxed);
    if jobs == 0 && skip_verified == 0 && passthrough == 0 && rejects == 0 && accept == 0 {
        return;
    }
    crate::info!(
        "balancer: batch_filter jobs={jobs} accept={accept} skip_verified={skip_verified} passthrough={passthrough} \
         reject_max_in={} reject_build={} reject_profit_floor={} reject_non_pos={} reject_reassess={} \
         reject_zero_realized={} reject_rpc={} reject_timeout={} reject_decode={}",
        BAL_REJECT_MAX_IN.load(Ordering::Relaxed),
        BAL_REJECT_BUILD.load(Ordering::Relaxed),
        BAL_REJECT_PROFIT_FLOOR.load(Ordering::Relaxed),
        BAL_REJECT_NON_POS.load(Ordering::Relaxed),
        BAL_REJECT_REASSESS.load(Ordering::Relaxed),
        BAL_REJECT_ZERO_REALIZED.load(Ordering::Relaxed),
        BAL_REJECT_RPC.load(Ordering::Relaxed),
        BAL_REJECT_TIMEOUT.load(Ordering::Relaxed),
        BAL_REJECT_DECODE.load(Ordering::Relaxed),
    );
}

pub fn log_balancer_prepare_gate_summary(candidates: u32) {
    if candidates == 0 {
        return;
    }
    let prepare = BAL_PREPARE_SKIP.load(Ordering::Relaxed);
    if prepare == 0 {
        return;
    }
    crate::info!(
        "balancer: prepare_gate candidates={candidates} prepare_skip={prepare} \
         (max_in={} profit_floor={} non_pos={} rpc={} timeout={} decode={})",
        BAL_REJECT_MAX_IN.load(Ordering::Relaxed),
        BAL_REJECT_PROFIT_FLOOR.load(Ordering::Relaxed),
        BAL_REJECT_NON_POS.load(Ordering::Relaxed),
        BAL_REJECT_RPC.load(Ordering::Relaxed),
        BAL_REJECT_TIMEOUT.load(Ordering::Relaxed),
        BAL_REJECT_DECODE.load(Ordering::Relaxed),
    );
}

/// True when every hop amount stays within the vault `MAX_IN_RATIO` (30%) limit.
#[must_use]
pub fn balancer_batch_within_max_in_ratio(arena: &StateArena, hops: &[CalldataHop]) -> bool {
    hops.iter().all(|hop| {
        let Some(PoolState::Balancer(state)) = arena.pool_state(hop.edge.pool_index) else {
            return false;
        };
        // Prefer vault address lookup — edge idxs may lag meta vs getPoolTokens.
        let in_idx = state
            .tokens
            .iter()
            .position(|&t| t == hop.token_in)
            .unwrap_or(hop.edge.token_in_idx as usize);
        state
            .balances
            .get(in_idx)
            .is_some_and(|bal| !exceeds_balancer_max_in_ratio(hop.amount_in, *bal))
    })
}

/// On-chain profit for an `executeArbDirect` batch route via vault `queryBatchSwap`.
pub async fn query_balancer_batch_profit<P: Provider<Ethereum>>(
    provider: &P,
    executor: Address,
    hops: &[CalldataHop],
    profit_token: Address,
    block_number: Option<u64>,
) -> BatchQueryOutcome {
    let Some(req) = build_balancer_batch_swap_request(hops, executor).ok() else {
        return BatchQueryOutcome::BuildFailed;
    };
    let Some(idx) = req.assets.iter().position(|a| *a == profit_token) else {
        return BatchQueryOutcome::BuildFailed;
    };
    let call = IBalancerVault::queryBatchSwapCall {
        kind: BALANCER_GIVEN_IN,
        swaps: req.swaps,
        assets: req.assets,
        funds: req.funds,
    };
    let tx = alloy::rpc::types::TransactionRequest::default()
        .to(BALANCER_VAULT)
        .input(call.abi_encode().into())
        .gas_limit(BALANCER_QUERY_BATCH_GAS);
    let mut eth_call = provider.call(tx);
    if let Some(block) = query_block_id(block_number) {
        eth_call = eth_call.block(block);
    }
    let output = match timeout(QUERY_TIMEOUT, eth_call).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(e)) => return BatchQueryOutcome::RpcError(format!("{e:#}")),
        Err(_) => return BatchQueryOutcome::Timeout,
    };
    let Ok(deltas) = IBalancerVault::queryBatchSwapCall::abi_decode_returns(&output) else {
        return BatchQueryOutcome::DecodeFailed;
    };
    let Some(delta) = deltas.get(idx).copied() else {
        return BatchQueryOutcome::DecodeFailed;
    };
    match profit_from_vault_delta(delta) {
        Some(profit) => BatchQueryOutcome::Profit(profit),
        None => BatchQueryOutcome::NonPositiveDelta(delta),
    }
}

fn decode_u256_return(output: &[u8]) -> Option<U256> {
    if output.len() == 32 {
        return Some(U256::from_be_slice(output));
    }
    if output.len() > 32 {
        return Some(U256::from_be_slice(&output[output.len() - 32..]));
    }
    None
}

fn direct_confirm_deadline() -> U256 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs().saturating_add(DIRECT_CONFIRM_DEADLINE_SECS))
        .unwrap_or(DIRECT_CONFIRM_DEADLINE_SECS);
    U256::from(secs)
}

/// Confirm vault-query profit with an `executeArbDirect` eth_call (same packing as dispatch).
///
/// `queryBatchSwap` can report a positive vault delta while the executor balance delta is
/// zero — those phantoms previously passed the batch filter and failed dry-run later.
pub async fn confirm_direct_batch_realized_profit<P: Provider<Ethereum>>(
    provider: &P,
    executor: Address,
    operator: Address,
    hops: &[CalldataHop],
    profit_token: Address,
    amount_in: U256,
    min_profit: U256,
    block_number: Option<u64>,
) -> Option<U256> {
    let deadline = direct_confirm_deadline();
    let calls = encode_balancer_batch_route(hops, executor, deadline).ok()?;
    let built = build_arb_calldata(
        executor,
        profit_token,
        profit_token,
        amount_in,
        min_profit,
        deadline,
        calls,
        ExecutorEntrypoint::Direct,
    )
    .ok()?;
    let tx = alloy::rpc::types::TransactionRequest::default()
        .to(built.to)
        .input(built.data.into())
        .value(built.value)
        .from(operator)
        .gas_limit(BALANCER_DIRECT_CONFIRM_GAS);
    let mut eth_call = provider.call(tx);
    if let Some(block) = query_block_id(block_number) {
        eth_call = eth_call.block(block);
    }
    let output = match timeout(QUERY_TIMEOUT, eth_call).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) | Err(_) => return None,
    };
    let realized = decode_u256_return(&output)?;
    (!realized.is_zero()).then_some(realized)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchQueryVerdict {
    Accepted(U256),
    Rejected(BalancerBatchReject),
}

/// Evaluates vault batch profit and on-chain min-profit floor (HF filter + dispatch).
#[must_use]
pub fn evaluate_batch_query(
    outcome: BatchQueryOutcome,
    amount_in: U256,
    slippage_bps: u64,
    hop_count: u32,
) -> BatchQueryVerdict {
    match outcome {
        BatchQueryOutcome::Profit(on_chain) => {
            if batch_profit_covers_min(on_chain, amount_in, slippage_bps, hop_count) {
                BatchQueryVerdict::Accepted(on_chain)
            } else {
                BatchQueryVerdict::Rejected(BalancerBatchReject::ProfitBelowMin)
            }
        }
        BatchQueryOutcome::NonPositiveDelta(_) => {
            BatchQueryVerdict::Rejected(BalancerBatchReject::NonPositiveDelta)
        }
        BatchQueryOutcome::RpcError(_) => {
            BatchQueryVerdict::Rejected(BalancerBatchReject::RpcError)
        }
        BatchQueryOutcome::Timeout => BatchQueryVerdict::Rejected(BalancerBatchReject::Timeout),
        BatchQueryOutcome::BuildFailed | BatchQueryOutcome::DecodeFailed => {
            BatchQueryVerdict::Rejected(BalancerBatchReject::BuildDecodeFailed)
        }
    }
}

/// Reject Direct routes when vault `queryBatchSwap` profit cannot satisfy calldata `minProfit`.
///
/// The floor must be derived from on-chain gross, not local sim — Balancer batch sim
/// routinely overstates profit vs `queryBatchSwap`, which caused viable Direct routes
/// to be filtered while only overstated mixed AaveFlash routes reached dispatch.
#[must_use]
pub fn batch_profit_covers_min(
    on_chain_profit: U256,
    amount_in: U256,
    slippage_bps: u64,
    hop_count: u32,
) -> bool {
    if on_chain_profit.is_zero() {
        return false;
    }
    let Some(min_profit) = on_chain_min_profit_for_route(
        on_chain_profit,
        amount_in,
        slippage_bps,
        hop_count,
        FlashLoanSource::Direct,
    ) else {
        return false;
    };
    on_chain_profit >= min_profit
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profit_from_vault_delta_matches_balancer_convention() {
        assert_eq!(profit_from_vault_delta(I256::ZERO), None);
        assert_eq!(profit_from_vault_delta(I256::ONE), None);
        assert_eq!(
            profit_from_vault_delta(I256::MINUS_ONE),
            Some(U256::from(1u8))
        );
        assert_eq!(
            profit_from_vault_delta(I256::unchecked_from(-58312062848374169i128)),
            Some(U256::from(58312062848374169u128)),
        );
    }

    #[test]
    fn batch_profit_covers_min_uses_on_chain_gross_not_modeled() {
        let on_chain = U256::from(111_906_298_841_187_462u128);
        let modeled = U256::from(296_685_017_513_143_239u128);
        let amount_in = U256::from(7_978_784_081_956_178u128);
        assert!(batch_profit_covers_min(on_chain, amount_in, 476, 2));
        let modeled_floor =
            on_chain_min_profit_for_route(modeled, amount_in, 476, 2, FlashLoanSource::Direct)
                .expect("modeled floor");
        assert!(on_chain < modeled_floor);
    }

    #[test]
    fn evaluate_batch_query_accepts_when_floor_met() {
        let on_chain = U256::from(111_906_298_841_187_462u128);
        let amount_in = U256::from(7_978_784_081_956_178u128);
        let verdict = evaluate_batch_query(BatchQueryOutcome::Profit(on_chain), amount_in, 476, 2);
        assert!(matches!(verdict, BatchQueryVerdict::Accepted(_)));
    }

    #[test]
    fn exceeds_max_in_ratio_constant_matches_vault() {
        use crate::core::math::balancer::exceeds_balancer_max_in_ratio;
        let bal = U256::from(1_000_000u64);
        assert!(!exceeds_balancer_max_in_ratio(U256::from(300_000u64), bal));
        assert!(exceeds_balancer_max_in_ratio(U256::from(300_001u64), bal));
    }
}
