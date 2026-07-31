use std::sync::Arc;

use anyhow::Context;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};
use rustc_hash::FxHashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};
#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::network::Ethereum;
use alloy::primitives::{Address, FixedBytes, U256, address};
use alloy::providers::Provider;
use tokio::sync::watch;

use crate::config::AppConfig;
use crate::config::WalletSecrets;
use crate::core::types::FlashLoanSource;
use crate::infra::rpc::RpcPool;
use crate::orchestrator::ui_hook::SharedUiHook;
use crate::services::execution::candidate::CandidateExecution;
use crate::services::execution::dryrun::dry_run_candidate;
use crate::services::execution::flash_liquidity::FlashLiquidityCache;
use crate::services::execution::gas::{
    GAS_FALLBACK_BUFFER_BPS, pick_live_gas_limit, pick_live_gas_limit_with_buffer,
    profit_reassess_gas, submit_gas_basis,
};
use crate::services::execution::gas_oracle::GasOracle;
use crate::services::execution::mempool::{
    MEMPOOL_NONCE_CACHE_TTL, MEMPOOL_STALL_TIMEOUT, decide_mempool_gate,
};
use crate::services::execution::nonce::NonceManager;
use crate::services::execution::private_submit::{
    PrivateSubmitConfig, private_submit_mode_requires_chain_id, resolve_submit_mode,
};
use crate::services::execution::profit::assess_profit;
use crate::services::execution::profit_logs::parse_transfer_profit;
use crate::services::execution::profit_sweep::sweep_profit_to_recipient;
use crate::services::execution::receipt::{ReceiptPollOutcome, ReceiptPoller};
use crate::services::execution::recovery::{NonceRecoveryOutcome, recover_after_receipt_timeout};
use crate::services::execution::revert_decoder::DecodedRevert;
use crate::services::execution::rpc_errors::{SubmitAction, classify_submit_error};
use crate::services::execution::submit::{
    FEE_BUMP_BPS, bump_fees, expected_submit_gas_price, resolve_submit_fees_with_profit,
    submit_with_recovery,
};
use crate::services::state_cache::StateCache;

mod pnl;
use pnl::{PnlState, parse_max_daily_loss_wei, token_profit_to_matic_wei};
mod route_stats;
use route_stats::{RouteFailureKind, RouteStats, RouteStatsWriter};
mod risk;

const ROUTE_COOLDOWN: Duration = Duration::from_secs(30);
const DRY_RUN_PASS_COOLDOWN: Duration = Duration::from_secs(120);
/// Exclusive Brent/assess window per edge-rotation set — concurrent HF ticks were
/// all ranking the same near_net DODO then hitting quarantine in evaluate_one
/// (live iter15: assess_q 73→693, 503 events <300ms apart).
const ROUTE_ASSESS_CLAIM_TTL: Duration = Duration::from_secs(3);
/// eth_blockNumber hang protection (matches state_refresh head budget).
const CHAIN_HEAD_RPC_TIMEOUT: Duration = Duration::from_millis(1_500);
// ponytail: prepare-skip only counts for logs — quarantine was starving selected=0.
/// Best-eval cover below this (bps of gas cost) is chronic underwater — soft-quarantine
/// so sticky routes stop crowding the HF window.
///
/// Live: fp 278927702089123978 (BAL+BAL+V3) won best-eval **940×** at cover≈50% /
/// net=0 / avail≈0.14 MATIC without cooling — sub-5% ceiling only caught deep dust.
/// 3-strike + 120s window still protects one-shot diversions; true near-misses that
/// cover gas fully (cover≥10_000, often positive-net path) stay out of this band.
const CHRONIC_UNDERWATER_COVER_BPS: u64 = 10_000;
/// Absolute MATIC available toward gas; below this, cover_bps≥1000 is wei-dust
/// (live: USDT input=8040 cover=1024 escaped uq while sticky V2 at 0.006 MATIC cooled).
const CHRONIC_UNDERWATER_MIN_AVAILABLE_MATIC_WEI: u128 = 10u128.pow(15); // 0.001 MATIC
/// Require repeated best-eval wins before soft quarantine. Live uqfix: unblocking
/// quarantine then one-shot-quarantined 74 distinct fps → selected=0 / kept max 1.
const CHRONIC_UNDERWATER_STRIKES: u32 = 3;
/// Reset strike count when best-eval gaps exceed this (one-shot diversions).
const CHRONIC_UNDERWATER_STRIKE_WINDOW: Duration = Duration::from_secs(120);
/// Ignore near-zero cover diversions (uqstrikes: cover=0/1/2 fps still cascaded
/// into selected=0). Live: cover=42/65 4-hop losers escaped the old 100 floor and
/// kept winning best-eval after sticky cool — cool from 25 bps with real MATIC.
const CHRONIC_UNDERWATER_MIN_COVER_BPS: u64 = 25;
const BATCH_QUERY_FAIL_QUARANTINE: Duration = Duration::from_secs(600);
/// Start-token cooldown when vault query profit does not appear in executor balance
/// (fee-on-transfer / reflective / nonstandard ERC20 such as STARV4).
const DIRECT_TOKEN_ZERO_REALIZED_QUARANTINE: Duration = Duration::from_secs(1800);
/// Live TransferFailed on flash input / mid-hop (FoT / nonstandard ERC-20) — seed so
/// restarts don't re-burn dry-run. Mid-hop hits must also be filtered via
/// [`ExecutionService::cycle_has_quarantined_token`] (start-token-only was insufficient).
const KNOWN_FOT_TOKENS: &[Address] = &[
    address!("0xeB51D9A39AD5EEF215dC0Bf39a8821ff804A0F01"), // LGNS
    address!("0xd93f7E271cB87c23AaA73edC008A79646d1F9912"), // Wrapped SOL (hop-2 TransferFailed)
];
// ponytail: process-lifetime stand-in; Instant has no forever.
const KNOWN_FOT_SEED_TTL: Duration = Duration::from_secs(7 * 24 * 3600);
const STRUCTURAL_DRY_RUN_QUARANTINE: Duration = Duration::from_secs(600);
/// Probe-only sizing stuck below the 1-token dispatch floor (live: 641 INFO lines
/// for 3 fps in 17m). Shorter than structural so liquidity recovery can retry.
const PROBE_BELOW_FLOOR_QUARANTINE: Duration = Duration::from_secs(300);
/// Sticky V3 dust (~355 bps) — same TTL as structural dry-run (was 30m).
const CHRONIC_UNDERWATER_QUARANTINE: Duration = STRUCTURAL_DRY_RUN_QUARANTINE;
/// Wei-dust underwater (available < 0.001 MATIC) — long cool. Weak-cover thin
/// (0.001–0.05) uses [`CHRONIC_UNDERWATER_QUARANTINE`] instead — live iter26
/// 1h-cooled cover~500–575 (+rotations) emptied HF select for the whole run.
const CHRONIC_THIN_LIQ_QUARANTINE: Duration = Duration::from_secs(3600);
/// Gas near-miss (cover≥[`CHRONIC_NEAR_MISS_COVER_BPS`], avail≥0.01): short sticky
/// cool so real edges retry when base fee moves. Live iter27: DODO cover~26xx
/// reappeared after 90s clusters but 3 rapid strikes burned the window on the
/// same gas snapshot — 30s/1-strike matches `ROUTE_COOLDOWN` and catches dips.
const CHRONIC_NEAR_MISS_QUARANTINE: Duration = Duration::from_secs(30);
/// High-cover near-miss (cover≥this, avail≥0.01): almost clears gas — do **not**
/// first-strike quarantine (live: DODO×2 cover=8672 killed 30s on strike-1 while
/// gas seed was the only gap). Needs [`CHRONIC_UNDERWATER_STRIKES`] so base-fee
/// dips / seed calibration can re-win best-eval.
const CHRONIC_HIGH_COVER_BPS: u64 = 7_500;
/// Mid-band near-miss: cover≥500 but avail in [0.001, 0.01). Live iter35: weak
/// sticky DODO cover~960 / avail~0.009 retried every 30s (17× best-eval) and
/// crowded BAL-start DODO cover~3850 (3×). 90s keeps mid-band alive without
/// monopolizing HF vs real ≥0.01 near-misses.
const CHRONIC_MID_BAND_QUARANTINE: Duration = Duration::from_secs(90);
/// Below this, high cover% is still clamped into the chronic band (USDT dust
/// avail≈0.035 / cover≈1770 crowded a cover≈20000 near-miss). First-strike cool
/// uses [`CHRONIC_DUST_AVAILABLE_MATIC_WEI`] + cover gate — not this alone
/// (live iter23: cover=2661 / avail≈0.037 got 1h cool and killed the best edge).
const CHRONIC_THIN_LIQ_AVAILABLE_MATIC_WEI: u128 = 5u128 * 10u128.pow(16); // 0.05 MATIC
/// Absolute dust: always first-strike cool. Between this and thin-liq ceiling,
/// first-strike only when cover is weak (<[`CHRONIC_NEAR_MISS_COVER_BPS`]).
const CHRONIC_DUST_AVAILABLE_MATIC_WEI: u128 = 10u128.pow(16); // 0.01 MATIC
/// Cover at/above this with ≥ dust avail (0.01) is a gas near-miss — 1-strike + 30s.
/// Cover≥this with avail in [0.001, 0.01) is mid-band — 1-strike + 90s (iter36).
const CHRONIC_NEAR_MISS_COVER_BPS: u64 = 500;
const PERMANENT_QUARANTINE: Duration = Duration::from_secs(3600);
const MAX_CONSECUTIVE_FAILURES: u32 = 5;
const ADAPTIVE_FLASH_CAP_START_DIVISOR: u64 = 4;

#[derive(Debug)]
pub struct ExecutionService {
    last_submit: RwLock<FxHashMap<u64, Instant>>,
    last_global_submit: Mutex<Option<Instant>>,
    /// Cached `(fetched_at, latest_nonce, pending_nonce)` for mempool gate RPC coalescing.
    mempool_nonce_cache: Mutex<Option<(Instant, u64, u64)>>,
    quarantine: RwLock<FxHashMap<u64, Instant>>,
    /// Short exclusive claim while a tick runs Brent/assess on a route (all rotations).
    assess_inflight: RwLock<FxHashMap<u64, Instant>>,
    route_hash_quarantine: RwLock<FxHashMap<FixedBytes<32>, Instant>>,
    /// Direct (`executeArbDirect`) start tokens that failed zero-realized confirm.
    direct_token_quarantine: RwLock<FxHashMap<Address, Instant>>,
    /// Underwater best-eval strike counts: fp → (strikes, last_strike_at).
    underwater_strikes: RwLock<FxHashMap<u64, (u32, Instant)>>,
    global_quarantine_until: Mutex<Option<Instant>>,
    fail_counts: RwLock<FxHashMap<u64, u32>>,
    nonce: RwLock<Option<(Address, Arc<NonceManager>)>>,
    pub flash_liquidity: Arc<FlashLiquidityCache>,
    pnl: Mutex<PnlState>,
    pub total_trades: AtomicU64,
    pub total_losses: AtomicU64,
    pub consecutive_fails: AtomicU32,
    /// When set, [`Self::record_realized`] trips a 1h global quarantine on breach.
    max_daily_loss_matic_wei: Option<U256>,
    route_stats: RwLock<FxHashMap<u64, RouteStats>>,
    _route_stats_path: PathBuf,
    last_near_miss_log: Mutex<Option<Instant>>,
    last_dispatch_log: Mutex<Option<(u64, U256)>>,
    last_prepare_skip_log: Mutex<Option<u64>>,
    prepare_skip_counts: RwLock<FxHashMap<u64, u32>>,
    pub route_sim_cache: Arc<crate::pipeline::route_sim_cache::RouteSimCache>,
    route_stats_writer: RouteStatsWriter,
    cached_chain_id: Mutex<Option<u64>>,
}

impl Default for ExecutionService {
    fn default() -> Self {
        // For tests / direct use: honor ROUTE_STATS_PATH env or built-in default.
        let path = std::env::var("ROUTE_STATS_PATH")
            .unwrap_or_else(|_| ".rpbot-route-stats.json".to_string());
        Self::with_route_stats_path(PathBuf::from(path))
    }
}

impl ExecutionService {
    /// Build using centralized config (prefers execution.route_stats_path over ROUTE_STATS_PATH env).
    pub fn from_config(config: &AppConfig) -> Self {
        let path = if !config.execution.route_stats_path.trim().is_empty() {
            config.execution.route_stats_path.clone()
        } else {
            std::env::var("ROUTE_STATS_PATH")
                .unwrap_or_else(|_| ".rpbot-route-stats.json".to_string())
        };
        let mut service = Self::with_route_stats_path(PathBuf::from(path));
        service.max_daily_loss_matic_wei =
            parse_max_daily_loss_wei(&config.execution.max_daily_loss_matic_wei);
        service
    }

    pub(crate) fn with_route_stats_path(route_stats_path: PathBuf) -> Self {
        let route_stats = Self::replay_route_stats(&route_stats_path);
        // ponytail: process-lifetime seed; TransferFailed path re-extends on hit.
        let fot_until = Instant::now() + KNOWN_FOT_SEED_TTL;
        let direct_token_quarantine = KNOWN_FOT_TOKENS
            .iter()
            .copied()
            .map(|t| (t, fot_until))
            .collect();
        Self {
            last_submit: RwLock::new(FxHashMap::default()),
            last_global_submit: Mutex::new(None),
            mempool_nonce_cache: Mutex::new(None),
            quarantine: RwLock::new(FxHashMap::default()),
            assess_inflight: RwLock::new(FxHashMap::default()),
            route_hash_quarantine: RwLock::new(FxHashMap::default()),
            direct_token_quarantine: RwLock::new(direct_token_quarantine),
            underwater_strikes: RwLock::new(FxHashMap::default()),
            global_quarantine_until: Mutex::new(None),
            fail_counts: RwLock::new(FxHashMap::default()),
            nonce: RwLock::new(None),
            flash_liquidity: Arc::new(FlashLiquidityCache::new()),
            pnl: Mutex::new(PnlState::new()),
            total_trades: AtomicU64::new(0),
            total_losses: AtomicU64::new(0),
            consecutive_fails: AtomicU32::new(0),
            max_daily_loss_matic_wei: None,
            route_stats: RwLock::new(route_stats),
            _route_stats_path: route_stats_path.clone(),
            last_near_miss_log: Mutex::new(None),
            last_dispatch_log: Mutex::new(None),
            last_prepare_skip_log: Mutex::new(None),
            prepare_skip_counts: RwLock::new(FxHashMap::default()),
            route_sim_cache: Arc::new(crate::pipeline::route_sim_cache::RouteSimCache::new()),
            route_stats_writer: RouteStatsWriter::spawn(route_stats_path),
            cached_chain_id: Mutex::new(None),
        }
    }
}

impl ExecutionService {
    /// True when the candidate is still eligible for dry-run given current cache gen.
    ///
    /// Exact equality was too strict: gen advances on every unrelated pool write
    /// (LF attach / stream), so multi-candidate batches died before eth_call.
    /// Monotonic gen never goes backward; block+hash provenance is the real pin.
    #[must_use]
    fn candidate_matches_state_generation(
        candidate: &CandidateExecution,
        state_cache: &StateCache,
    ) -> bool {
        state_cache.generation() >= candidate.state_generation
    }

    async fn cached_chain_id<P: Provider<Ethereum>>(&self, provider: &P) -> Option<u64> {
        if let Some(id) = *self.cached_chain_id.lock() {
            return Some(id);
        }
        let id = provider.get_chain_id().await.ok()?;
        *self.cached_chain_id.lock() = Some(id);
        Some(id)
    }
}

#[derive(Debug, Clone)]
pub enum ExecutionOutcome {
    DryRunPassed {
        gas_used: u64,
    },
    DryRunFailed {
        reason: String,
    },
    SkippedCircuitBreaker,
    /// Operator MATIC below gas+value requirement (not a consecutive-fail trip).
    SkippedInsufficientBalance,
    SkippedQuarantined,
    SkippedCooldown,
    SkippedNoWallet,
    SkippedNoPrivateRpc,
    SkippedUnprofitablePreDryRun,
    SkippedUnprofitableAfterDryRun,
    SkippedShutdown,
    Confirmed {
        tx_hash: String,
        gas_used: u64,
        profit_wei: U256,
    },
    Reverted {
        tx_hash: String,
        gas_used: u64,
    },
    ReceiptTimeout {
        tx_hash: String,
    },
    SubmitFailed {
        reason: String,
    },
}

impl ExecutionService {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    async fn operator_mempool_clear<P: Provider<Ethereum>>(
        &self,
        provider: &P,
        operator: Address,
    ) -> anyhow::Result<(bool, bool)> {
        let now = Instant::now();
        let last_global_submit = *self.last_global_submit.lock();

        let cached = {
            let guard = self.mempool_nonce_cache.lock();
            *guard
        };
        let (latest, pending) = if let Some((fetched_at, latest, pending)) = cached
            && fetched_at.elapsed() < MEMPOOL_NONCE_CACHE_TTL
        {
            (latest, pending)
        } else {
            let (latest_res, pending_res) = tokio::join!(
                provider.get_transaction_count(operator),
                provider
                    .get_transaction_count(operator)
                    .block_id(alloy::eips::BlockId::pending()),
            );
            let latest = latest_res.context("failed to read latest operator nonce")?;
            let pending = pending_res.context("failed to read pending operator nonce")?;
            *self.mempool_nonce_cache.lock() = Some((Instant::now(), latest, pending));
            (latest, pending)
        };

        let decision = decide_mempool_gate(
            latest,
            pending,
            last_global_submit,
            now,
            MEMPOOL_STALL_TIMEOUT,
        );

        if decision.pending_ahead && decision.allow_submit {
            crate::warn!(
                "mempool ahead of latest — allowing submit with nonce resync (latest={latest} pending={pending})"
            );
        } else if !decision.allow_submit {
            crate::debug!(
                "mempool gate: waiting on pending tx (latest={latest} pending={pending})"
            );
        }

        Ok((decision.allow_submit, decision.pending_ahead))
    }

    fn invalidate_mempool_nonce_cache(&self) {
        *self.mempool_nonce_cache.lock() = None;
    }

    pub async fn ensure_nonce_manager<P: Provider<Ethereum>>(
        &self,
        provider: &P,
        operator: Address,
    ) -> anyhow::Result<Arc<NonceManager>> {
        {
            let guard = self.nonce.read();
            if let Some((addr, mgr)) = guard.as_ref()
                && *addr == operator
            {
                return Ok(Arc::clone(mgr));
            }
        }

        let mgr = Arc::new(NonceManager::new(operator));
        mgr.initialize(provider).await?;
        *self.nonce.write() = Some((operator, Arc::clone(&mgr)));
        Ok(mgr)
    }

    pub async fn shutdown_resync<P: Provider<Ethereum>>(&self, provider: &P, operator: Address) {
        let mgr = {
            let guard = self.nonce.read();
            guard.as_ref().and_then(|(addr, mgr)| {
                if *addr == operator {
                    Some(Arc::clone(mgr))
                } else {
                    None
                }
            })
        };
        if let Some(mgr) = mgr
            && (mgr.in_flight_count() > 0 || mgr.stale_count() > 0)
            && let Err(e) = mgr.resync(provider).await
        {
            crate::warn!("shutdown nonce resync failed for {operator}: {e:#}");
        }
    }

    fn reassess_assessment(
        candidate: &CandidateExecution,
        profit_gas: u64,
        gas_price: U256,
        min_profit_matic_wei: U256,
        realized_profit: Option<U256>,
        profit_priority_alpha_bps: u64,
    ) -> Option<crate::core::types::ProfitAssessment> {
        // Dry-run gas is authoritative for profit reassessment; submit gas limit
        // (and simulated_gas heuristic) must not inflate the post-dry-run floor.
        let gas_units = u32::try_from(profit_gas).unwrap_or(u32::MAX);
        let (gross_profit, amount_in, slippage_bps, flash_source) =
            if let Some(realized) = realized_profit.filter(|p| !p.is_zero()) {
                // Dry-run return is post-repayment token profit; calldata minProfit
                // was set from modeled net at build time — reassess without re-fees.
                (realized, candidate.amount_in, 0, FlashLoanSource::Direct)
            } else if realized_profit.is_some() {
                return None;
            } else {
                (
                    candidate.gross_profit,
                    candidate.amount_in,
                    candidate.slippage_bps,
                    candidate.flash_loan_source,
                )
            };
        let mut input =
            candidate.profit_assessment_input(gas_units, gas_price, min_profit_matic_wei);
        input.gross_profit = gross_profit;
        input.amount_in = amount_in;
        input.slippage_bps = slippage_bps;
        input.flash_loan_source = flash_source;
        input.profit_priority_alpha_bps = profit_priority_alpha_bps;
        Some(assess_profit(&input))
    }

    fn release_failed_submit_nonce(nonce_mgr: &NonceManager, nonce: u64, action: SubmitAction) {
        if action == SubmitAction::AlreadyKnown {
            nonce_mgr.mark_stale(nonce);
        } else {
            nonce_mgr.release(nonce);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn process_candidate<P: Provider<Ethereum>>(
        &self,
        sim_provider: &P,
        rpc: &RpcPool,
        wallet: &WalletSecrets,
        config: &AppConfig,
        candidate: &CandidateExecution,
        operator: Address,
        gas_oracle: &GasOracle,
        state_cache: &StateCache,
        expected_state_block: u64,
        expected_state_hash: Option<alloy::primitives::B256>,
        ui_hook: Option<&SharedUiHook>,
        shutdown: Option<&watch::Receiver<bool>>,
        _metrics: Option<&()>,
        chain_head_hint: Option<u64>,
    ) -> ExecutionOutcome {
        let fp = candidate.route_fingerprint;
        if shutdown.is_some_and(|rx| *rx.borrow()) {
            let outcome = ExecutionOutcome::SkippedShutdown;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        if self.global_is_quarantined() {
            crate::info!("dispatch skip: fp={}, global circuit breaker active", fp);
            let outcome = ExecutionOutcome::SkippedCircuitBreaker;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        let now = Instant::now();

        // Generation may advance (unrelated pool writes); only reject if cache
        // regressed (should not happen). Block+hash below is the real pin.
        if !Self::candidate_matches_state_generation(candidate, state_cache) {
            crate::info!(
                "dispatch skip: fp={}, state generation regressed candidate={} current={}",
                fp,
                candidate.state_generation,
                state_cache.generation(),
            );
            let outcome = ExecutionOutcome::SubmitFailed {
                reason: "candidate state generation mismatch".to_string(),
            };
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        if candidate.state_block != expected_state_block
            || candidate.state_hash != expected_state_hash
        {
            crate::info!(
                "dispatch skip: fp={}, stale state provenance candidate(block={}, hash={:?}) != expected(block={}, hash={:?})",
                fp,
                candidate.state_block,
                candidate.state_hash,
                expected_state_block,
                expected_state_hash,
            );
            let outcome = ExecutionOutcome::SubmitFailed {
                reason: "candidate state provenance mismatch".to_string(),
            };
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        let risk_multiplier = self.route_risk_multiplier_bps(fp);
        let learned_floor = candidate
            .min_profit_matic_wei
            .saturating_mul(U256::from(risk_multiplier))
            / U256::from(10_000u64);
        if candidate.expected_profit_matic_wei < learned_floor {
            crate::info!(
                "dispatch skip: fp={}, profit {} below learned floor {} (risk_mult={})",
                fp,
                candidate.expected_profit_matic_wei,
                learned_floor,
                risk_multiplier,
            );
            let outcome = ExecutionOutcome::SkippedUnprofitablePreDryRun;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        let chain_head = match chain_head_hint {
            Some(head) => head,
            None => {
                match tokio::time::timeout(CHAIN_HEAD_RPC_TIMEOUT, sim_provider.get_block_number())
                    .await
                {
                    Ok(Ok(block)) => block,
                    Ok(Err(e)) => {
                        return ExecutionOutcome::SubmitFailed {
                            reason: format!("cannot establish simulation block: {e}"),
                        };
                    }
                    Err(_) => {
                        return ExecutionOutcome::SubmitFailed {
                            reason: format!(
                                "cannot establish simulation block: eth_blockNumber timed out after {}ms",
                                CHAIN_HEAD_RPC_TIMEOUT.as_millis()
                            ),
                        };
                    }
                }
            }
        };
        // Pin eth_call to the LF/HF state block so simulation matches routed pool state.
        let provenance_block = candidate.state_block.max(expected_state_block);
        let simulation_block = if provenance_block > 0 {
            provenance_block.min(chain_head)
        } else {
            chain_head
        };
        if provenance_block == 0 {
            crate::warn!(
                "dry-run unpinned: fp={fp} route={} (head={chain_head})",
                candidate.route_trace,
            );
        } else if simulation_block != chain_head {
            crate::info!(
                "dry-run pinned: fp={fp} block={simulation_block} (head={chain_head}) route={}",
                candidate.route_trace,
            );
        }

        if let Some(expiry) = self.quarantine.read().get(&fp)
            && now < *expiry
        {
            crate::info!(
                "dispatch skip: fp={}, route quarantined until {expiry:?}",
                fp
            );
            let outcome = ExecutionOutcome::SkippedQuarantined;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        if self.is_route_hash_quarantined(&candidate.route_hash) {
            crate::info!(
                "dispatch skip: fp={}, route_hash={} quarantined (structural dry-run failure)",
                fp,
                candidate.route_hash,
            );
            let outcome = ExecutionOutcome::SkippedQuarantined;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        if self.is_direct_token_quarantined(candidate.profit_token) {
            crate::info!(
                "dispatch skip: fp={fp} token={} quarantined (FoT/TransferFailed)",
                candidate.profit_token
            );
            let outcome = ExecutionOutcome::SkippedQuarantined;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        let route_cooldown = self.route_cooldown(config);
        if let Some(last) = self.last_submit.read().get(&fp)
            && now.saturating_duration_since(*last) < route_cooldown
        {
            crate::info!(
                "dispatch skip: fp={}, route cooldown active ({route_cooldown:?}, last={last:?})",
                fp
            );
            let outcome = ExecutionOutcome::SkippedCooldown;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        let dry =
            dry_run_candidate(sim_provider, candidate, operator, Some(simulation_block)).await;

        let realized_profit = dry.realized_profit.filter(|p| !p.is_zero());
        if !dry.success || !dry.semantic_success || realized_profit.is_none() {
            // Huff `transferAll` reverts empty when executor balance is 0 (see
            // ArbExecutor.huff transfer_all_zero). Target is the executor itself —
            // common after Curve/DODO→V2 when the prior hop left no intermediate.
            let transfer_all_funding_miss = matches!(
                &dry.decoded_revert,
                Some(DecodedRevert::ExternalCallFailed {
                    target,
                    reason,
                    ..
                }) if *target == candidate.target_address
                    && (reason.contains("empty nested revert")
                        || reason.contains("transferAll zero balance"))
            );
            // Mid-route TransferFailed (hop index ≥ 1): prior hop under-delivered vs
            // optimistic chain_in / local quote — not FoT. Live: V3 hop1 callback
            // TransferFailed on BRZ/CES after hop0 WPOL→intermediate. Soft-cool only;
            // do not token-quarantine the intermediate (was blackholing long-tail legs).
            let mid_hop_transfer_underfund = is_mid_hop_transfer_underfund(&dry.decoded_revert);
            let sim_fidelity_miss = transfer_all_funding_miss
                || mid_hop_transfer_underfund
                || matches!(
                    dry.decoded_revert,
                    Some(DecodedRevert::InsufficientProfit {
                        final_balance,
                        ..
                    }) if final_balance.is_zero()
                );
            if matches!(dry.decoded_revert, Some(DecodedRevert::AaveReserveInactive)) {
                self.flash_liquidity
                    .mark_aave_inactive(candidate.profit_token);
            }
            if candidate.flash_loan_source == FlashLoanSource::Direct
                && dry.realized_profit.is_some_and(|p| p.is_zero())
                && !crate::core::constants::is_polygon_hub_token(candidate.profit_token)
            {
                // Direct FoT/reflective start tokens only — never cool WMATIC/USDT hubs.
                self.quarantine_direct_token_zero_realized(candidate.profit_token);
            }
            if sim_fidelity_miss {
                self.quarantine_route_soft(fp, now);
            } else {
                self.quarantine_route(fp, now, RouteFailureKind::DryRun);
            }
            if matches!(
                dry.decoded_revert,
                Some(DecodedRevert::ExternalCallFailed { .. })
                    | Some(DecodedRevert::TransferFailed { .. })
            ) {
                if matches!(
                    dry.decoded_revert,
                    Some(DecodedRevert::ExternalCallFailed { .. })
                ) && !transfer_all_funding_miss
                    && !mid_hop_transfer_underfund
                {
                    self.quarantine_route_hash(candidate.route_hash, now);
                    // Structural vault/router rejects (BAL#327, etc.) re-burn every
                    // ROUTE_COOLDOWN (30s) with new route_hash from re-sizing
                    // (live: same 2 fps failed BAL#327 three times in ~3m).
                    // Upgrade fp cool to structural TTL so mixed Aave phantoms
                    // stop crowding Direct profitable candidates.
                    // transferAll-zero / mid-hop underfund are sim-fidelity (soft cool).
                    self.quarantine_insert(fp, now + STRUCTURAL_DRY_RUN_QUARANTINE);
                }
                // FoT / nonstandard ERC20 TransferFailed: cool the *failing* token.
                // Live bug: hop-2 TransferFailed on long-tail token quarantined
                // profit_token=WMATIC for 30m (hub arbs blackholed). Prefer nested
                // `token=0x…` from the executor error; only fall back to start token
                // for hop-0 UniV2 TRANSFER_FAILED strings without an address.
                // Skip mid-hop underfund (chain_in optimism) — not a token ban.
                if !mid_hop_transfer_underfund
                    && let Some(token) = transfer_failed_token_to_quarantine(
                        &dry.decoded_revert,
                        candidate.profit_token,
                    )
                {
                    self.quarantine_direct_token_zero_realized(token);
                    crate::info!(
                        "token quarantine after TransferFailed dry-run: token={token} fp={fp}"
                    );
                }
            }
            let mut reason = dry.failure_reason();
            if transfer_all_funding_miss {
                // Rephrase so ops logs don't look like a random nested vault/router fail.
                if let Some(DecodedRevert::ExternalCallFailed { index, .. }) = &dry.decoded_revert {
                    reason = format!(
                        "transferAll zero balance at packed call {index} (prior hop left no intermediate on executor; often Curve/DODO→V2 after under-delivery or index mismatch)"
                    );
                }
            } else if mid_hop_transfer_underfund {
                if let Some(DecodedRevert::ExternalCallFailed { index, reason: r, .. }) =
                    &dry.decoded_revert
                {
                    reason = format!(
                        "mid-hop transfer underfund at packed call {index} (prior hop delivered less than chain_in; {r})"
                    );
                }
            }
            // Adaptive USD flash cap was the binding constraint — size-fail demotes
            // so the next assess starts smaller instead of replaying BAL#528 at cap.
            if candidate.adaptive_flash_cap_bound
                && flash_size_failure_reason(&reason)
                && let Some((previous, next)) =
                    self.demote_adaptive_flash_loan_cap(fp, candidate.adaptive_flash_loan_usd_limit)
            {
                crate::info!(
                    "flash cap demoted: fp={fp} usd={previous}->{next} after size dry-run fail"
                );
            }
            crate::info!(
                "dry-run failed: fp={}, route_hash={}, flash={:?}, ain={}, profit_matic={}, hops={}, route={}, reason={}{}",
                fp,
                candidate.route_hash,
                candidate.flash_loan_source,
                candidate.amount_in,
                candidate.expected_profit_matic_wei,
                candidate.hop_count,
                candidate.route_trace,
                reason,
                if sim_fidelity_miss {
                    " (sim fidelity miss — soft cooldown)"
                } else {
                    ""
                }
            );
            let outcome = ExecutionOutcome::DryRunFailed { reason };
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        let prior_observed_gas = gas_oracle.observed_route_gas(fp);
        let gas_used = submit_gas_basis(
            prior_observed_gas,
            gas_oracle.sim_scale_bps(),
            candidate.simulated_gas,
            dry.gas_used,
        );
        if gas_used == 0 {
            self.quarantine_route(fp, now, RouteFailureKind::DryRun);
            let outcome = ExecutionOutcome::DryRunFailed {
                reason: "dry-run passed but gas estimate is zero".into(),
            };
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }
        if dry.gas_used.is_none() {
            crate::info!(
                "dry-run pass (gas fallback): fp={}, sim_gas={}",
                fp,
                candidate.simulated_gas
            );
        } else {
            let realized = dry.realized_profit.unwrap_or_default();
            // Executor returns profit-token units; sim is MATIC wei — convert before ratio.
            let realized_matic = token_profit_to_matic_wei(
                realized,
                candidate.token_to_matic_rate,
                candidate.token_decimals,
            )
            .unwrap_or(U256::ZERO);
            let sim_matic = candidate.expected_profit_matic_wei;
            let retain_bps = if sim_matic.is_zero() {
                0u64
            } else {
                realized_matic
                    .saturating_mul(U256::from(10_000u64))
                    .checked_div(sim_matic)
                    .unwrap_or_default()
                    .try_into()
                    .unwrap_or(u64::MAX)
            };
            crate::info!(
                "dry-run pass: fp={}, gas_used={}, sim_gas={}, ain={}, flash={:?}, hops={}, sim_profit_matic={}, realized_profit={}, realized_matic={}, retain_bps={}, route={}",
                fp,
                gas_used,
                candidate.simulated_gas,
                candidate.amount_in,
                candidate.flash_loan_source,
                candidate.hop_count,
                sim_matic,
                realized,
                realized_matic,
                retain_bps,
                candidate.route_trace,
            );
        }
        let gas_fallback = dry.gas_used.is_none();
        // Only RPC-measured gas may calibrate the oracle; scaled heuristics are not observations.
        if !gas_fallback {
            gas_oracle.record_sim_observed(candidate.simulated_gas, gas_used);
            if gas_used > 0 {
                gas_oracle.record_route_gas(
                    candidate.route_fingerprint,
                    u32::try_from(gas_used).unwrap_or(u32::MAX),
                );
            }
        }
        let final_gas = match if gas_fallback {
            pick_live_gas_limit_with_buffer(
                candidate.simulated_gas,
                gas_used,
                GAS_FALLBACK_BUFFER_BPS,
            )
        } else {
            pick_live_gas_limit(candidate.simulated_gas, gas_used)
        } {
            Ok(g) => g,
            Err(e) => {
                self.quarantine_route(fp, now, RouteFailureKind::DryRun);
                let outcome = ExecutionOutcome::DryRunFailed {
                    reason: format!("{e:#}"),
                };
                if let Some(ui_hook) = ui_hook {
                    ui_hook.on_execution_outcome(&outcome, fp);
                }
                return outcome;
            }
        };

        let tip_profit = realized_profit
            .and_then(|p| {
                token_profit_to_matic_wei(
                    p,
                    candidate.token_to_matic_rate,
                    candidate.token_decimals,
                )
            })
            .filter(|p| !p.is_zero())
            .or_else(|| {
                (!candidate.priority_bid_basis_matic_wei.is_zero())
                    .then_some(candidate.priority_bid_basis_matic_wei)
            })
            .unwrap_or(candidate.expected_profit_matic_wei);
        // Tip intensity uses execution gas (sim/dry-run), not buffered tx limit.
        let tip_gas_units = dry
            .gas_used
            .filter(|&g| g > 0)
            .or(prior_observed_gas.map(u64::from).filter(|&g| g > 0))
            .unwrap_or(u64::from(candidate.simulated_gas.max(1)))
            .max(1);
        let Some(fees) = resolve_submit_fees_with_profit(
            gas_oracle,
            tip_profit,
            config.execution.profit_priority_fee_alpha_bps,
            tip_gas_units,
        ) else {
            let outcome = ExecutionOutcome::SubmitFailed {
                reason: "gas oracle has no snapshot for fee resolution".into(),
            };
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        };
        let Some(fee_snap) = gas_oracle.loaded_snapshot() else {
            let outcome = ExecutionOutcome::SubmitFailed {
                reason: "gas oracle has no snapshot for fee reassess".into(),
            };
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        };
        // Expected pay = base + bid tip (not max_fee ceiling / 12.5% base buffer).
        let reassess_gas_price = expected_submit_gas_price(fee_snap.base_fee, &fees);
        crate::info!(
            "submit fees: fp={}, tip_profit_matic={}, base_gwei={:.3}, priority_gwei={:.3}, max_fee_gwei={:.3}, expected_gwei={:.3}, gas_limit={}",
            fp,
            tip_profit,
            crate::util::u256_to_f64(fee_snap.base_fee) / 1e9,
            crate::util::u256_to_f64(fees.max_priority_fee_per_gas) / 1e9,
            crate::util::u256_to_f64(fees.max_fee_per_gas) / 1e9,
            crate::util::u256_to_f64(reassess_gas_price) / 1e9,
            final_gas,
        );

        let profit_gas = profit_reassess_gas(
            prior_observed_gas,
            candidate.simulated_gas,
            dry.gas_used,
            gas_fallback,
            gas_oracle.sim_scale_bps(),
        );
        // Tip already in reassess_gas_price (max_priority) — alpha=0 avoids double-count.
        let reassess = Self::reassess_assessment(
            candidate,
            profit_gas,
            reassess_gas_price,
            learned_floor,
            realized_profit,
            0,
        );
        let dry_pass = reassess.as_ref().is_some_and(|a| a.should_execute);
        if !dry_pass {
            let reject = reassess
                .as_ref()
                .and_then(|a| a.reject_reason.as_deref())
                .unwrap_or("unknown");
            crate::info!(
                "dispatch skip: fp={}, unprofitable after dry-run (profit_matic={}, profit_gas={}, submit_gas={}, expected_gwei={:.3}, max_fee_gwei={:.3}, reject={reject})",
                fp,
                candidate.expected_profit_matic_wei,
                profit_gas,
                gas_used,
                crate::util::u256_to_f64(reassess_gas_price) / 1e9,
                crate::util::u256_to_f64(fees.max_fee_per_gas) / 1e9
            );
            let outcome = ExecutionOutcome::SkippedUnprofitableAfterDryRun;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        // Sim+profit gate passed — lower learned risk floor for this fingerprint.
        self.record_route_dry_run_pass(fp);

        if config.is_dry_run() {
            self.last_submit.write().insert(fp, now);
            let outcome = ExecutionOutcome::DryRunPassed { gas_used };
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        if gas_oracle
            .conservative_gas_price_for_live_submit()
            .is_none()
        {
            let age = gas_oracle.snapshot_age_ms();
            crate::warn!(
                "dispatch skip: fp={}, gas fee snapshot missing or stale for live submit (age_ms={age:?})",
                fp,
            );
            let outcome = ExecutionOutcome::SubmitFailed {
                reason: format!("gas fee snapshot not fresh for live submit (age_ms={age:?})"),
            };
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        crate::debug!(
            "dispatch live: fp={}, gas_used={}, live_mode=true",
            fp,
            gas_used
        );

        let Some(signer) = wallet.signer() else {
            crate::error!("dispatch skip: fp={}, wallet has no signer", fp);
            let outcome = ExecutionOutcome::SkippedNoWallet;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        };

        let submit_provider = match rpc.connect_submit_checked(signer).await {
            Ok(p) => p,
            Err(e) => {
                crate::warn!(
                    "dispatch skip: fp={}, submit provider unavailable: {e:#}",
                    fp
                );
                let outcome = ExecutionOutcome::SkippedNoPrivateRpc;
                if let Some(ui_hook) = ui_hook {
                    ui_hook.on_execution_outcome(&outcome, fp);
                }
                return outcome;
            }
        };

        let (mempool_clear, pending_ahead) = match self
            .operator_mempool_clear(&submit_provider, operator)
            .await
        {
            Ok(status) => status,
            Err(e) => {
                crate::warn!("dispatch skip: fp={}, mempool check failed: {e:#}", fp);
                return ExecutionOutcome::SubmitFailed {
                    reason: format!("{e:#}"),
                };
            }
        };
        if !mempool_clear {
            crate::info!("dispatch skip: fp={}, mempool not clear — waiting", fp);
            let outcome = ExecutionOutcome::SkippedCooldown;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        if shutdown.is_some_and(|rx| *rx.borrow()) {
            let outcome = ExecutionOutcome::SkippedShutdown;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        if self.consecutive_fails.load(Ordering::Relaxed)
            >= config.execution.max_global_consecutive_failures
        {
            self.quarantine_global(Duration::from_secs(60), now);
            self.consecutive_fails.store(0, Ordering::Relaxed);
            crate::warn!(
                "global circuit breaker tripped: {} consecutive failures — quarantined 60s",
                config.execution.max_global_consecutive_failures
            );
            let outcome = ExecutionOutcome::SkippedCircuitBreaker;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        let Some(required_balance) =
            required_operator_balance(config, candidate.value, final_gas, fees.max_fee_per_gas)
        else {
            return ExecutionOutcome::SubmitFailed {
                reason: "operator balance requirement overflow".into(),
            };
        };
        let balance = match sim_provider.get_balance(operator).await {
            Ok(balance) => U256::from(balance),
            Err(e) => {
                return ExecutionOutcome::SubmitFailed {
                    reason: format!("operator balance check failed: {e}"),
                };
            }
        };
        if balance < required_balance {
            crate::info!(
                "dispatch skip: fp={}, operator balance {} below required {} (gas_limit={} max_fee={})",
                fp,
                balance,
                required_balance,
                final_gas,
                fees.max_fee_per_gas,
            );
            let outcome = ExecutionOutcome::SkippedInsufficientBalance;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        // Gate on head lag since dry-run started (`chain_head`), not the pinned
        // `simulation_block` (LF/HF provenance can already be 2+ behind live head).
        match tokio::time::timeout(CHAIN_HEAD_RPC_TIMEOUT, sim_provider.get_block_number()).await {
            Ok(Ok(head)) if head <= chain_head.saturating_add(1) => {}
            Ok(Ok(head)) => {
                return ExecutionOutcome::SubmitFailed {
                    reason: format!(
                        "candidate stale: dry-run head {chain_head} (sim block {simulation_block}), now {head}"
                    ),
                };
            }
            Ok(Err(e)) => {
                return ExecutionOutcome::SubmitFailed {
                    reason: format!("pre-submit head check failed: {e}"),
                };
            }
            Err(_) => {
                return ExecutionOutcome::SubmitFailed {
                    reason: format!(
                        "pre-submit head check timed out after {}ms",
                        CHAIN_HEAD_RPC_TIMEOUT.as_millis()
                    ),
                };
            }
        }

        let nonce_mgr = match self.ensure_nonce_manager(&submit_provider, operator).await {
            Ok(mgr) => mgr,
            Err(e) => {
                let outcome = ExecutionOutcome::SubmitFailed {
                    reason: format!("nonce init failed: {e}"),
                };
                if let Some(ui_hook) = ui_hook {
                    ui_hook.on_execution_outcome(&outcome, fp);
                }
                return outcome;
            }
        };

        if pending_ahead {
            if let Err(e) = nonce_mgr.resync(&submit_provider).await {
                crate::warn!(
                    "dispatch: fp={}, nonce resync after mempool stall failed: {e:#}",
                    fp
                );
            }
        } else if let Err(e) = nonce_mgr.resync_if_dirty(&submit_provider).await {
            crate::warn!("dispatch: fp={}, nonce resync_if_dirty failed: {e:#}", fp);
        }

        let mut nonce = match nonce_mgr.next_nonce() {
            Ok(n) => n,
            Err(e) => {
                let outcome = ExecutionOutcome::SubmitFailed {
                    reason: format!("{e:#}"),
                };
                if let Some(ui_hook) = ui_hook {
                    ui_hook.on_execution_outcome(&outcome, fp);
                }
                return outcome;
            }
        };

        let chain_id = self.cached_chain_id(&submit_provider).await;
        let private_cfg = match build_private_config(rpc, signer, chain_id) {
            Ok(cfg) => cfg,
            Err(e) => {
                nonce_mgr.release(nonce);
                crate::error!("private submit misconfigured: {e:#}");
                let outcome = ExecutionOutcome::SubmitFailed {
                    reason: format!("private submit requires chain_id: {e:#}"),
                };
                if let Some(ui_hook) = ui_hook {
                    ui_hook.on_execution_outcome(&outcome, fp);
                }
                return outcome;
            }
        };

        let tx_hash = match submit_with_recovery(
            &submit_provider,
            &nonce_mgr,
            candidate,
            &mut nonce,
            fees,
            final_gas,
            private_cfg.as_ref(),
        )
        .await
        {
            Ok(hash) => {
                crate::info!(
                    "submit success: fp={}, nonce={}, tx_hash={}, gas_limit={}",
                    fp,
                    nonce,
                    hash,
                    final_gas,
                );
                hash
            }
            Err(e) => {
                crate::warn!("submit failed: fp={}, nonce={}, error={e:#}", fp, nonce,);
                let action = classify_submit_error(&e);
                Self::release_failed_submit_nonce(&nonce_mgr, nonce, action.clone());
                self.invalidate_mempool_nonce_cache();
                self.consecutive_fails.fetch_add(1, Ordering::Relaxed);
                match action {
                    SubmitAction::ResyncAndRetry => {
                        self.quarantine_route_soft(fp, now);
                    }
                    _ => {
                        self.quarantine_route(fp, now, RouteFailureKind::Submit);
                    }
                }
                let outcome = ExecutionOutcome::SubmitFailed {
                    reason: format!("{e:#}"),
                };
                if let Some(ui_hook) = ui_hook {
                    ui_hook.on_execution_outcome(&outcome, fp);
                }
                return outcome;
            }
        };
        *self.last_global_submit.lock() = Some(now);
        self.invalidate_mempool_nonce_cache();

        let poller = ReceiptPoller::new(
            Duration::from_millis(config.execution.receipt_timeout_ms),
            Duration::from_millis(config.execution.receipt_poll_ms),
        );

        let tx_hash_str = tx_hash.to_string();

        let poll_outcome = poller.wait(sim_provider, tx_hash, shutdown).await;
        let Some(receipt) = (match poll_outcome {
            ReceiptPollOutcome::Received(receipt) => Some(receipt),
            ReceiptPollOutcome::Shutdown => {
                // Tx already submitted — never release; reuse races the in-flight nonce.
                nonce_mgr.mark_stale(nonce);
                let outcome = ExecutionOutcome::SkippedShutdown;
                if let Some(ui_hook) = ui_hook {
                    ui_hook.on_execution_outcome(&outcome, fp);
                }
                return outcome;
            }
            ReceiptPollOutcome::RpcFailure(reason) => {
                nonce_mgr.mark_stale(nonce);
                self.quarantine_route_soft(fp, now);
                let outcome = ExecutionOutcome::SubmitFailed {
                    reason: format!("receipt RPC failed; transaction fate unknown: {reason}"),
                };
                if let Some(ui_hook) = ui_hook {
                    ui_hook.on_execution_outcome(&outcome, fp);
                }
                return outcome;
            }
            ReceiptPollOutcome::TimedOut => None,
        }) else {
            crate::info!(
                "receipt timeout: fp={}, tx_hash={}, nonce={}",
                fp,
                tx_hash,
                nonce,
            );
            if shutdown.is_some_and(|rx| *rx.borrow()) {
                nonce_mgr.mark_stale(nonce);
                let outcome = ExecutionOutcome::SkippedShutdown;
                if let Some(ui_hook) = ui_hook {
                    ui_hook.on_execution_outcome(&outcome, fp);
                }
                return outcome;
            }

            match recover_after_receipt_timeout(
                &submit_provider,
                &nonce_mgr,
                operator,
                tx_hash,
                nonce,
                &fees,
                final_gas,
                private_cfg.as_ref(),
            )
            .await
            {
                NonceRecoveryOutcome::Mined(receipt) => {
                    return self
                        .finalize_receipt_and_maybe_sweep(
                            fp,
                            now,
                            &nonce_mgr,
                            nonce,
                            &tx_hash_str,
                            &receipt,
                            candidate,
                            final_gas,
                            gas_oracle,
                            ui_hook,
                            &submit_provider,
                            sim_provider,
                            config,
                            operator,
                            private_cfg.as_ref(),
                            shutdown,
                        )
                        .await;
                }
                NonceRecoveryOutcome::Cancelled(cancel_hash) => {
                    // Cancel uses bumped fees (same as recovery); upper-bound book to daily PnL.
                    let cancel_fees = bump_fees(fees, FEE_BUMP_BPS);
                    let cancel_cost =
                        U256::from(21_000u64).saturating_mul(cancel_fees.max_fee_per_gas);
                    self.record_gas_cost_loss(cancel_cost);
                    crate::info!(
                        "receipt timeout: cancel tx submitted fp={}, original={}, cancel={cancel_hash} attributed_gas_wei={cancel_cost}",
                        fp,
                        tx_hash,
                    );
                }
                NonceRecoveryOutcome::Dropped => {
                    crate::info!(
                        "receipt timeout: original tx dropped from mempool fp={}, tx_hash={}",
                        fp,
                        tx_hash,
                    );
                }
                NonceRecoveryOutcome::StillPending => {
                    crate::warn!(
                        "receipt timeout: tx still pending after cancel attempt fp={}, tx_hash={}, nonce={nonce}",
                        fp,
                        tx_hash,
                    );
                }
            }

            self.quarantine_route(fp, now, RouteFailureKind::Timeout);
            let outcome = ExecutionOutcome::ReceiptTimeout {
                tx_hash: tx_hash_str.to_string(),
            };
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        };

        self.finalize_receipt_and_maybe_sweep(
            fp,
            now,
            &nonce_mgr,
            nonce,
            &tx_hash_str,
            &receipt,
            candidate,
            final_gas,
            gas_oracle,
            ui_hook,
            &submit_provider,
            sim_provider,
            config,
            operator,
            private_cfg.as_ref(),
            shutdown,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn finalize_receipt_and_maybe_sweep<P: Provider<Ethereum>, S: Provider<Ethereum>>(
        &self,
        fp: u64,
        now: Instant,
        nonce_mgr: &NonceManager,
        nonce: u64,
        tx_hash_str: &str,
        receipt: &crate::services::execution::receipt::ReceiptData,
        candidate: &CandidateExecution,
        submitted_gas_limit: u64,
        gas_oracle: &GasOracle,
        ui_hook: Option<&SharedUiHook>,
        submit_provider: &P,
        receipt_provider: &S,
        config: &AppConfig,
        operator: Address,
        private: Option<&PrivateSubmitConfig>,
        shutdown: Option<&watch::Receiver<bool>>,
    ) -> ExecutionOutcome {
        let outcome = self.finalize_receipt(
            fp,
            now,
            nonce_mgr,
            nonce,
            tx_hash_str,
            receipt,
            candidate,
            submitted_gas_limit,
            gas_oracle,
            ui_hook,
        );
        if let ExecutionOutcome::Confirmed { profit_wei, .. } = &outcome
            && !profit_wei.is_zero()
        {
            match sweep_profit_to_recipient(
                submit_provider,
                receipt_provider,
                nonce_mgr,
                gas_oracle,
                config,
                candidate,
                operator,
                private,
                shutdown,
            )
            .await
            {
                Ok(()) => self.invalidate_mempool_nonce_cache(),
                Err(e) => {
                    self.invalidate_mempool_nonce_cache();
                    // Arb profit is already on-chain; sweep failure must not rewrite Confirmed.
                    crate::warn!(
                        "profit sweep failed after confirmed arb: fp={fp} hash={tx_hash_str} err={e:#}"
                    );
                }
            }
        }
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_receipt(
        &self,
        fp: u64,
        now: Instant,
        nonce_mgr: &NonceManager,
        nonce: u64,
        tx_hash_str: &str,
        receipt: &crate::services::execution::receipt::ReceiptData,
        candidate: &CandidateExecution,
        submitted_gas_limit: u64,
        gas_oracle: &GasOracle,
        ui_hook: Option<&SharedUiHook>,
    ) -> ExecutionOutcome {
        nonce_mgr.confirm(nonce);
        self.last_submit.write().insert(fp, now);

        if !receipt.success {
            self.quarantine_route(fp, now, RouteFailureKind::Revert);
            let gas_cost = receipt
                .effective_gas_price
                .and_then(|price| U256::from(receipt.gas_used).checked_mul(U256::from(price)))
                .or_else(|| {
                    gas_oracle
                        .conservative_gas_price()
                        .and_then(|price| U256::from(receipt.gas_used).checked_mul(price))
                })
                .unwrap_or_else(|| {
                    crate::warn!(
                        "revert receipt missing gas price attribution fp={fp} gas_used={}",
                        receipt.gas_used
                    );
                    U256::ZERO
                });
            self.record_gas_cost_loss(gas_cost);
            // OOG at the gas ceiling reads as a revert but still teaches route gas.
            if receipt.gas_used > u64::from(candidate.simulated_gas) * 105 / 100 {
                let oog_at_limit = submitted_gas_limit > 0
                    && receipt.gas_used >= submitted_gas_limit.saturating_mul(98) / 100;
                // OOG at the submitted ceiling — actual need exceeds observed gas.
                let teach_gas = if oog_at_limit {
                    receipt.gas_used.saturating_mul(130) / 100
                } else {
                    receipt.gas_used.saturating_mul(110) / 100
                };
                gas_oracle.record_sim_observed(candidate.simulated_gas, teach_gas);
                gas_oracle.record_route_gas(fp, u32::try_from(teach_gas).unwrap_or(u32::MAX));
            }
            let revert_detail = receipt.logs.first().map(|l| format!("{:.?}", l.topics()));
            crate::info!(
                "tx reverted: fp={}, hash={}, gas={}, sim_gas={}, revert_topics={:?}",
                fp,
                tx_hash_str,
                receipt.gas_used,
                candidate.simulated_gas,
                revert_detail,
            );
            let outcome = ExecutionOutcome::Reverted {
                tx_hash: tx_hash_str.to_string(),
                gas_used: receipt.gas_used,
            };
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        // Only successful receipts represent full-route execution gas. Revert
        // gas can stop at any hop and would bias future limits downward.
        if receipt.gas_used > 0 {
            gas_oracle.record_sim_observed(candidate.simulated_gas, receipt.gas_used);
            gas_oracle.record_route_gas(fp, u32::try_from(receipt.gas_used).unwrap_or(u32::MAX));
        }

        let parsed_profit = parse_transfer_profit(
            &receipt.logs,
            candidate.target_address,
            Some(candidate.profit_token),
        );
        let Some(effective_gas_price) = receipt.effective_gas_price else {
            crate::error!(
                "confirmed transaction had no effective_gas_price; refusing PnL attribution: fp={fp}, hash={tx_hash_str}"
            );
            self.quarantine_route(fp, now, RouteFailureKind::RealizedLoss);
            return ExecutionOutcome::Confirmed {
                tx_hash: tx_hash_str.to_string(),
                gas_used: receipt.gas_used,
                profit_wei: parsed_profit.unwrap_or(U256::ZERO),
            };
        };
        let Some(gas_cost) =
            U256::from(receipt.gas_used).checked_mul(U256::from(effective_gas_price))
        else {
            crate::error!("receipt gas cost overflow; refusing PnL attribution: fp={fp}");
            self.quarantine_route(fp, now, RouteFailureKind::RealizedLoss);
            return ExecutionOutcome::Confirmed {
                tx_hash: tx_hash_str.to_string(),
                gas_used: receipt.gas_used,
                profit_wei: parsed_profit.unwrap_or(U256::ZERO),
            };
        };
        let profit_wei = parsed_profit.unwrap_or(U256::ZERO);
        let profit_matic_wei = parsed_profit.and_then(|profit| {
            token_profit_to_matic_wei(
                profit,
                candidate.token_to_matic_rate,
                candidate.token_decimals,
            )
        });
        if let Some(profit_matic_wei) = profit_matic_wei {
            self.record_realized(profit_matic_wei, gas_cost);
            if profit_matic_wei >= gas_cost {
                self.clear_fail_count(fp);
                if profit_matic_wei > gas_cost
                    && candidate.adaptive_flash_cap_bound
                    && let Some((previous, next)) = self.promote_adaptive_flash_loan_cap(
                        fp,
                        candidate.adaptive_flash_loan_usd_limit,
                    )
                {
                    crate::info!(
                        "flash cap promoted: fp={fp} usd={previous}->{next} after profitable confirmed receipt"
                    );
                }
            } else {
                self.quarantine_route(fp, now, RouteFailureKind::RealizedLoss);
            }
        } else {
            // Attribution unknown — do not book a phantom gas-only loss (trips breaker /
            // daily-loss on confirmed wins). Soft-cool the route for a retry window.
            self.quarantine_route_soft(fp, now);
        }
        if parsed_profit.is_none() {
            crate::error!(
                "confirmed transaction had no attributable profit transfer: fp={fp}, hash={tx_hash_str}"
            );
        } else if profit_matic_wei.is_none() {
            crate::error!(
                "confirmed transaction profit could not be converted to MATIC: fp={fp}, hash={tx_hash_str}"
            );
        }
        crate::info!(
            "tx confirmed: fp={}, hash={}, gas={}, profit_wei={}",
            fp,
            tx_hash_str,
            receipt.gas_used,
            profit_wei
        );

        let outcome = ExecutionOutcome::Confirmed {
            tx_hash: tx_hash_str.to_string(),
            gas_used: receipt.gas_used,
            profit_wei,
        };
        if let Some(ui_hook) = ui_hook {
            ui_hook.on_execution_outcome(&outcome, fp);
        }
        outcome
    }
}

fn build_private_config(
    rpc: &RpcPool,
    signer: &alloy::signers::local::PrivateKeySigner,
    chain_id: Option<u64>,
) -> anyhow::Result<Option<PrivateSubmitConfig>> {
    let private_url = rpc.private_url().map(str::to_string);
    let bloxroute_auth = std::env::var("BLOXROUTE_AUTH_HEADER")
        .ok()
        .filter(|s| !s.is_empty());
    let probe = rpc.private_submit_probe();
    let mode = resolve_submit_mode(
        private_url.as_deref(),
        bloxroute_auth.as_deref(),
        probe.as_ref(),
    );
    if rpc.require_private_submit()
        && !required_private_relay_capability_verified(
            bloxroute_auth.is_some(),
            rpc.bloxroute_auth_verified(),
            probe.is_some_and(|probe| probe.supports_private_rpc_method),
        )
    {
        anyhow::bail!("private submit capability is not verified for the configured relay");
    }
    if !private_submit_mode_requires_chain_id(mode) {
        return Ok(None);
    }
    let chain_id = chain_id.ok_or_else(|| {
        anyhow::anyhow!(
            "eth_chainId unavailable for private submit mode {mode:?} (refusing public mempool fallback)"
        )
    })?;
    Ok(Some(PrivateSubmitConfig {
        mode,
        signer: signer.clone(),
        chain_id,
        private_url,
        bloxroute_auth,
    }))
}

fn required_private_relay_capability_verified(
    has_bloxroute_auth: bool,
    bloxroute_auth_verified: Option<bool>,
    private_rpc_method_verified: bool,
) -> bool {
    if has_bloxroute_auth {
        bloxroute_auth_verified == Some(true)
    } else {
        private_rpc_method_verified
    }
}

fn min_operator_balance_wei(config: &AppConfig) -> Option<U256> {
    config
        .execution
        .min_operator_matic_wei
        .parse::<U256>()
        .ok()
        .filter(|v| !v.is_zero())
}

/// Packed-call index ≥ 1 TransferFailed / balance shortfall after a prior hop ran.
/// Treat as chain_in / local-quote optimism, not FoT (see encode_route min_out chain).
#[must_use]
fn is_mid_hop_transfer_underfund(decoded: &Option<DecodedRevert>) -> bool {
    match decoded {
        Some(DecodedRevert::ExternalCallFailed { index, reason, .. }) if *index >= 1 => {
            let r = reason.to_ascii_lowercase();
            r.contains("transferfailed")
                || r.contains("transfer_failed")
                || r.contains("transfer amount exceeds balance")
                || r.contains("insufficient balance")
                || r.contains("exceeds balance")
        }
        Some(DecodedRevert::TransferFailed { .. }) => {
            // Top-level TransferFailed has no hop index; only nest under ExternalCallFailed
            // carries the packed-call index for mid-route classification.
            false
        }
        _ => false,
    }
}

/// Resolve which token (if any) should enter the FoT/TransferFailed cool-down.
///
/// Live bug (tui-bolt-optimized run): hop-2
/// `TransferFailed: token=0xd93f…` on a long-tail leg cooled
/// `profit_token=WMATIC` for 30m because this path always used the flash start.
fn transfer_failed_token_to_quarantine(
    decoded: &Option<DecodedRevert>,
    profit_token: Address,
) -> Option<Address> {
    if is_mid_hop_transfer_underfund(decoded) {
        return None;
    }
    let token = match decoded {
        Some(DecodedRevert::TransferFailed { token, .. }) => Some(*token),
        Some(DecodedRevert::ExternalCallFailed { index, reason, .. }) => {
            if !(reason.contains("TransferFailed") || reason.contains("TRANSFER_FAILED")) {
                return None;
            }
            // Nested executor error carries the failing ERC-20; UniV2 string
            // "TRANSFER_FAILED" does not — only cool start token on hop 0.
            parse_nested_transfer_failed_token(reason)
                .or_else(|| (*index == 0).then_some(profit_token))
        }
        _ => None,
    }?;
    if crate::core::constants::is_polygon_hub_token(token) {
        return None;
    }
    Some(token)
}

/// Parse `token=0x…` from `TransferFailed: token=0x…, to=…, amount=…`.
fn parse_nested_transfer_failed_token(reason: &str) -> Option<Address> {
    let rest = reason.split("token=").nth(1)?;
    let addr = rest.split([',', ' ', '\n', '\t']).next()?.trim();
    if addr.is_empty() {
        return None;
    }
    addr.parse::<Address>().ok()
}

/// Dry-run failure text that indicates the flash borrow size was too large.
#[must_use]
fn flash_size_failure_reason(reason: &str) -> bool {
    let r = reason.to_ascii_lowercase();
    r.contains("bal#528")
        || r.contains("insufficient balancer flash")
        || r.contains("flash-loan balance")
        || r.contains("flash loan amount")
        || r.contains("borrow amount exceeds")
        || r.contains("amount exceeds available")
}

fn required_operator_balance(
    config: &AppConfig,
    transaction_value: U256,
    gas_limit: u64,
    max_fee_per_gas: U256,
) -> Option<U256> {
    let gas_budget = U256::from(gas_limit).checked_mul(max_fee_per_gas)?;
    gas_budget
        .checked_add(transaction_value)?
        .checked_add(min_operator_balance_wei(config).unwrap_or(U256::ZERO))
}

#[cfg(test)]
mod safety_tests {
    use super::*;
    use crate::services::execution::candidate::CandidateExecution;

    #[test]
    fn already_known_submit_error_keeps_nonce_stale() {
        let nonce_mgr = NonceManager::new(Address::ZERO);
        ExecutionService::release_failed_submit_nonce(&nonce_mgr, 7, SubmitAction::AlreadyKnown);
        assert_eq!(nonce_mgr.stale_count(), 1);
        assert_eq!(nonce_mgr.in_flight_count(), 0);
    }

    #[test]
    fn chronic_underwater_quarantine_needs_repeated_strikes() {
        let exec = ExecutionService::default();
        let fp = 0xdead_beef_u64;
        let thick = U256::from(10u128.pow(17)); // 0.1 MATIC — needs 3 strikes
        // Near-zero cover never strikes (cascade guard).
        assert!(
            exec.quarantine_chronic_gas_underwater(fp, 10, thick)
                .is_none()
        );
        assert!(!exec.is_route_quarantined(fp));
        assert!(
            exec.quarantine_chronic_gas_underwater(fp, 350, thick)
                .is_none()
        );
        assert!(
            exec.quarantine_chronic_gas_underwater(fp, 350, thick)
                .is_none()
        );
        assert!(!exec.is_route_quarantined(fp));
        assert!(
            exec.quarantine_chronic_gas_underwater(fp, 350, thick)
                .is_some()
        );
        assert!(exec.is_route_quarantined(fp));
        // Already quarantined — no re-apply signal.
        assert!(
            exec.quarantine_chronic_gas_underwater(fp, 350, thick)
                .is_none()
        );
        // One-shot diversion (different fp) stays selectable.
        assert!(
            exec.quarantine_chronic_gas_underwater(0xcafe_u64, 200, thick)
                .is_none()
        );
        assert!(!exec.is_route_quarantined(0xcafe_u64));
        // Sub-100 cover with real MATIC still chronic-cools (live: 42/65 best-evals).
        let low_fp = 0x1042_u64;
        assert!(
            exec.quarantine_chronic_gas_underwater(low_fp, 42, thick)
                .is_none()
        );
        assert!(
            exec.quarantine_chronic_gas_underwater(low_fp, 42, thick)
                .is_none()
        );
        assert!(
            exec.quarantine_chronic_gas_underwater(low_fp, 42, thick)
                .is_some()
        );
        // Wei-dust (<0.001 MATIC) cools on first strike with 1h.
        let dust_fp = 0xd057_u64;
        let dust_avail = U256::from(100u64);
        assert_eq!(
            exec.quarantine_chronic_gas_underwater(dust_fp, 1024, dust_avail),
            Some(std::time::Duration::from_secs(3600))
        );
        // Thin absolute + weak cover: first-strike but 600s (not 1h) — iter26 1h
        // rotation cools emptied the HF window (peak cover 575, no ge_1000).
        let thin_fp = 0x7e17_u64;
        let thin_avail = U256::from(35u128 * 10u128.pow(15)); // 0.035 MATIC
        assert_eq!(
            exec.quarantine_chronic_gas_underwater(thin_fp, 380, thin_avail),
            Some(std::time::Duration::from_secs(600))
        );
        // Near-miss cover (≥500) at same avail: 1-strike + 30s sticky cool.
        let miss_fp = 0x2661_u64;
        assert_eq!(
            exec.quarantine_chronic_gas_underwater(miss_fp, 2_661, thin_avail),
            Some(std::time::Duration::from_secs(30))
        );
        assert!(exec.is_route_quarantined(miss_fp));
        // Mid-cover with avail in [0.001, 0.01): 90s mid-band (iter36 — was 30s
        // and monopolized HF vs real ≥0.01 near-misses).
        let mid_fp = 0x0524_u64;
        let mid_avail = U256::from(7u128 * 10u128.pow(15)); // 0.007 MATIC
        assert_eq!(
            exec.quarantine_chronic_gas_underwater(mid_fp, 524, mid_avail),
            Some(std::time::Duration::from_secs(90))
        );
        assert!(exec.is_route_quarantined(mid_fp));
        // Cover≥break-even (10_000) with real MATIC escapes chronic.
        let near_fp = 0x9ea5_u64;
        let near_avail = U256::from(15u128 * 10u128.pow(16)); // 0.15 MATIC
        assert!(
            exec.quarantine_chronic_gas_underwater(near_fp, 20_000, near_avail)
                .is_none()
        );
        assert!(
            exec.quarantine_chronic_gas_underwater(near_fp, 20_000, near_avail)
                .is_none()
        );
        assert!(
            exec.quarantine_chronic_gas_underwater(near_fp, 20_000, near_avail)
                .is_none()
        );
        assert!(!exec.is_route_quarantined(near_fp));
        // Half-cover with real MATIC (≥dust) also 1-strike + 30s.
        let half_fp = 0xba1b_a1b0_u64;
        let half_avail = U256::from(14u128 * 10u128.pow(16)); // 0.14 MATIC
        assert_eq!(
            exec.quarantine_chronic_gas_underwater(half_fp, 5_068, half_avail),
            Some(std::time::Duration::from_secs(30))
        );
        assert!(exec.is_route_quarantined(half_fp));
        // High-cover near-miss (≥7500) needs 3 strikes — do not kill on first look.
        let hi_fp = 0xd0d0_8672_u64;
        let hi_avail = U256::from(8u128 * 10u128.pow(16)); // 0.08 MATIC
        assert!(
            exec.quarantine_chronic_gas_underwater(hi_fp, 8_672, hi_avail)
                .is_none()
        );
        assert!(
            exec.quarantine_chronic_gas_underwater(hi_fp, 8_672, hi_avail)
                .is_none()
        );
        assert!(!exec.is_route_quarantined(hi_fp));
        assert_eq!(
            exec.quarantine_chronic_gas_underwater(hi_fp, 8_672, hi_avail),
            Some(std::time::Duration::from_secs(30))
        );
        assert!(exec.is_route_quarantined(hi_fp));
    }

    #[test]
    fn required_private_submit_needs_verified_private_rpc_capability() {
        let mut config = AppConfig::default();
        config.execution.require_private_submit = true;
        config.rpc.private_rpc_url = Some("https://private.example".into());
        let rpc = RpcPool::from_config(&config);
        let signer = "0x0101010101010101010101010101010101010101010101010101010101010101"
            .parse::<alloy::signers::local::PrivateKeySigner>()
            .expect("test signer");

        assert!(build_private_config(&rpc, &signer, Some(137)).is_err());

        rpc.record_private_submit_probe(
            crate::services::execution::private_submit::PrivateSubmitProbe {
                url: "https://private.example".into(),
                chain_id_ok: true,
                supports_private_rpc_method: true,
                private_method_error: None,
                recommended_mode:
                    crate::services::execution::private_submit::PrivateSubmitMode::PolygonPrivateRpc,
            },
        );
        let config = build_private_config(&rpc, &signer, Some(137))
            .expect("verified private RPC config")
            .expect("private mode");
        assert_eq!(
            config.mode,
            crate::services::execution::private_submit::PrivateSubmitMode::PolygonPrivateRpc
        );
    }

    #[test]
    fn required_private_submit_requires_positive_bloxroute_auth_probe() {
        assert!(!required_private_relay_capability_verified(
            true, None, true
        ));
        assert!(!required_private_relay_capability_verified(
            true,
            Some(false),
            true
        ));
        assert!(required_private_relay_capability_verified(
            true,
            Some(true),
            false
        ));
    }

    #[test]
    fn operator_balance_covers_reserve_value_and_worst_case_gas() {
        let mut config = AppConfig::default();
        config.execution.min_operator_matic_wei = "500".into();

        assert_eq!(
            required_operator_balance(&config, U256::from(7u8), 100, U256::from(3u8)),
            Some(U256::from(807u64))
        );
    }

    #[test]
    fn operator_balance_requirement_fails_closed_on_overflow() {
        let config = AppConfig::default();
        assert_eq!(
            required_operator_balance(&config, U256::MAX, 1, U256::from(1u8)),
            None
        );
    }

    #[test]
    fn realized_loss_updates_pnl_and_breaker() {
        let service = ExecutionService::new();
        service.record_realized(U256::from(1u8), U256::from(10u8));
        assert_eq!(service.pnl_snapshot().1, -9);
        assert_eq!(service.total_losses.load(Ordering::Relaxed), 1);
        assert_eq!(service.consecutive_fails.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn realized_profit_is_net_of_gas_and_resets_breaker() {
        let service = ExecutionService::new();
        service.record_realized(U256::ZERO, U256::from(1u8));
        service.record_realized(U256::from(20u8), U256::from(5u8));
        assert_eq!(service.pnl_snapshot().1, 14);
        assert_eq!(service.total_trades.load(Ordering::Relaxed), 1);
        assert_eq!(service.consecutive_fails.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn break_even_realized_is_neutral_not_loss() {
        let service = ExecutionService::new();
        service.record_realized(U256::from(10u8), U256::from(10u8));
        assert_eq!(service.pnl_snapshot().1, 0);
        assert_eq!(service.total_trades.load(Ordering::Relaxed), 0);
        assert_eq!(service.total_losses.load(Ordering::Relaxed), 0);
        assert_eq!(service.consecutive_fails.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn daily_loss_limit_trips_on_record_realized() {
        let mut service = ExecutionService::new();
        service.max_daily_loss_matic_wei = Some(U256::from(5u8));
        service.record_realized(U256::ZERO, U256::from(5u8));
        assert!(service.global_is_quarantined());
    }

    #[test]
    fn daily_pnl_resets_on_utc_day_boundary() {
        let service = ExecutionService::new();
        service.record_realized(U256::from(20u8), U256::from(5u8));
        {
            let mut pnl = service.pnl.lock();
            pnl.daily_utc_day = pnl.daily_utc_day.saturating_sub(1);
        }
        service.record_realized(U256::from(10u8), U256::from(2u8));
        let (total, daily) = service.pnl_snapshot();
        assert_eq!(total, 23);
        assert_eq!(daily, 8);
    }

    #[test]
    fn token_profit_is_converted_to_matic_before_pnl_accounting() {
        // 2 USDC at 0.5 MATIC/USDC = 1 MATIC.
        assert_eq!(
            token_profit_to_matic_wei(
                U256::from(2_000_000u64),
                U256::from(500_000_000_000_000_000u64),
                6,
            ),
            Some(U256::from(1_000_000_000_000_000_000u64))
        );
    }

    #[test]
    fn token_profit_conversion_fails_closed_for_unusable_rate() {
        assert_eq!(
            token_profit_to_matic_wei(U256::from(1_000_000u64), U256::ZERO, 6),
            None
        );
    }

    #[test]
    fn realized_profit_zero_fails_reassess() {
        let candidate = CandidateExecution {
            route_fingerprint: 8,
            calldata: Default::default(),
            target_address: Address::repeat_byte(8),
            value: U256::ZERO,
            profit_token: Address::repeat_byte(9),
            expected_profit_matic_wei: U256::from(1u64),
            priority_bid_basis_matic_wei: U256::from(1u64),
            gas_limit: None,
            simulated_gas: 100,
            route_hash: Default::default(),
            gross_profit: U256::from(1_000u64),
            amount_in: U256::from(1_000u64),
            token_decimals: 18,
            token_to_matic_rate: U256::from(1_000_000_000_000_000_000u128),
            slippage_bps: 250,
            flash_loan_source: FlashLoanSource::AaveV3,
            min_profit_matic_wei: U256::ZERO,
            min_profit_roi_bps: 0,
            hop_count: 2,
            safety_multiplier_bps: 10_000,
            state_generation: 1,
            state_block: 1,
            state_hash: None,
            route_trace: String::new(),
            adaptive_flash_cap_bound: false,
            adaptive_flash_loan_usd_limit: 50_000,
        };
        assert!(
            ExecutionService::reassess_assessment(
                &candidate,
                100,
                U256::from(1u8),
                U256::ZERO,
                Some(U256::ZERO),
                0,
            )
            .is_none()
        );
    }

    #[test]
    fn realized_profit_short_circuits_modeled_slippage_and_flash_fee() {
        let candidate = CandidateExecution {
            route_fingerprint: 7,
            calldata: Default::default(),
            target_address: Address::repeat_byte(7),
            value: U256::ZERO,
            profit_token: Address::repeat_byte(8),
            expected_profit_matic_wei: U256::ZERO,
            priority_bid_basis_matic_wei: U256::ZERO,
            gas_limit: None,
            simulated_gas: 100,
            route_hash: Default::default(),
            gross_profit: U256::from(1_000u64),
            amount_in: U256::from(1_000u64),
            token_decimals: 18,
            token_to_matic_rate: U256::from(1_000_000_000_000_000_000u128),
            slippage_bps: 250,
            flash_loan_source: FlashLoanSource::AaveV3,
            min_profit_matic_wei: U256::ZERO,
            min_profit_roi_bps: 0,
            hop_count: 2,
            safety_multiplier_bps: 10_000,
            state_generation: 1,
            state_block: 1,
            state_hash: None,
            route_trace: String::new(),
            adaptive_flash_cap_bound: false,
            adaptive_flash_loan_usd_limit: 50_000,
        };

        let assessment = ExecutionService::reassess_assessment(
            &candidate,
            100,
            U256::from(1u8),
            U256::ZERO,
            Some(U256::from(900u64)),
            0,
        )
        .expect("realized profit should produce an assessment");

        assert_eq!(assessment.gross_profit, U256::from(900u64));
        assert_eq!(assessment.slippage_deduction, U256::ZERO);
        assert_eq!(assessment.flash_loan_fee, U256::ZERO);
    }

    #[test]
    fn dry_run_reassess_uses_observed_gas_not_submit_limit() {
        let candidate = CandidateExecution {
            route_fingerprint: 9,
            calldata: Default::default(),
            target_address: Address::repeat_byte(9),
            value: U256::ZERO,
            profit_token: Address::repeat_byte(10),
            expected_profit_matic_wei: U256::from(878_359_083_215_296_116u128),
            priority_bid_basis_matic_wei: U256::from(878_359_083_215_296_116u128),
            gas_limit: None,
            simulated_gas: 795_000,
            route_hash: Default::default(),
            gross_profit: U256::from(1_128_473_000_000_000_000u128),
            amount_in: U256::from(7_978_784_081_956_178u128),
            token_decimals: 18,
            token_to_matic_rate: U256::from(1_000_000_000_000_000_000u128),
            slippage_bps: 50,
            flash_loan_source: FlashLoanSource::Direct,
            min_profit_matic_wei: U256::from(10_000_000_000_000_000u128),
            min_profit_roi_bps: 0,
            hop_count: 2,
            safety_multiplier_bps: 10_000,
            state_generation: 1,
            state_block: 1,
            state_hash: None,
            route_trace: String::new(),
            adaptive_flash_cap_bound: false,
            adaptive_flash_loan_usd_limit: 50_000,
        };
        let gas_price = U256::from(314_608_528_420u64);
        let realized = U256::from(300_000_000_000_000_000u128);
        let dry_run_gas = 186_903u64;
        let submit_limit = 874_501u64;

        let with_observed = ExecutionService::reassess_assessment(
            &candidate,
            dry_run_gas,
            gas_price,
            candidate.min_profit_matic_wei,
            Some(realized),
            1_000,
        )
        .expect("dry-run reassess");
        let with_submit_limit = ExecutionService::reassess_assessment(
            &candidate,
            submit_limit,
            gas_price,
            candidate.min_profit_matic_wei,
            Some(realized),
            1_000,
        )
        .expect("submit-limit reassess");

        assert!(
            with_observed.should_execute,
            "observed dry-run gas should keep marginal routes executable: {:?}",
            with_observed.reject_reason
        );
        assert!(
            !with_submit_limit.should_execute,
            "inflated submit gas must not reject routes that dry-run validated"
        );
    }

    #[test]
    fn learned_failure_rate_raises_profit_floor() {
        let service = ExecutionService::new();
        service.route_stats.write().insert(
            7,
            RouteStats {
                successes: 1,
                failures: 3,
                dry_run_failures: 3,
                ..RouteStats::default()
            },
        );
        assert_eq!(service.route_risk_multiplier_bps(7), 25_000);
        assert_eq!(service.route_risk_multiplier_bps(8), 10_000);
    }

    #[test]
    fn odd_timeout_count_applies_half_weight() {
        let service = ExecutionService::new();
        service.route_stats.write().insert(
            9,
            RouteStats {
                successes: 2,
                failures: 1,
                receipt_timeouts: 1,
                ..RouteStats::default()
            },
        );
        // attempts=3, weighted_half=2*1 + 1 = 3 → 10_000 + 30_000/3 = 20_000
        // (old integer /2 truncated the +½ and yielded 16_666)
        assert_eq!(service.route_risk_multiplier_bps(9), 20_000);
        let (risk, flash) = service.route_learning_snapshot(9, 50_000);
        assert_eq!(risk, 20_000);
        assert_eq!(flash, 12_500);
    }

    #[test]
    fn dry_run_pass_does_not_reduce_risk_floor_without_mined_receipt() {
        let service = ExecutionService::new();
        service.route_stats.write().insert(
            11,
            RouteStats {
                successes: 0,
                failures: 4,
                dry_run_failures: 4,
                ..RouteStats::default()
            },
        );
        let elevated = service.route_risk_multiplier_bps(11);
        assert!(elevated > 10_000);
        for _ in 0..4 {
            service.record_route_dry_run_pass(11);
        }
        let cooled = service.route_risk_multiplier_bps(11);
        assert_eq!(cooled, elevated);
    }

    #[test]
    fn adaptive_flash_cap_starts_conservatively_and_promotes_to_configured_limit() {
        let path = std::env::temp_dir().join(format!(
            "rpbot-adaptive-flash-cap-{}-{}.log",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_nanos()
        ));
        let service = ExecutionService::with_route_stats_path(path.clone());
        assert_eq!(service.adaptive_flash_loan_usd(7, 50_000), 12_500);
        assert_eq!(
            service.promote_adaptive_flash_loan_cap(7, 50_000),
            Some((12_500, 25_000))
        );
        assert_eq!(service.adaptive_flash_loan_usd(7, 50_000), 25_000);
        assert_eq!(
            service.promote_adaptive_flash_loan_cap(7, 50_000),
            Some((25_000, 50_000))
        );
        assert_eq!(service.promote_adaptive_flash_loan_cap(7, 50_000), None);
        assert_eq!(service.adaptive_flash_loan_usd(7, 10_000), 10_000);
        drop(service);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn adaptive_flash_cap_demotes_on_size_failure_to_start_floor() {
        let path = std::env::temp_dir().join(format!(
            "rpbot-adaptive-flash-demote-{}-{}.log",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_nanos()
        ));
        let service = ExecutionService::with_route_stats_path(path.clone());
        assert_eq!(
            service.promote_adaptive_flash_loan_cap(9, 50_000),
            Some((12_500, 25_000))
        );
        assert_eq!(
            service.promote_adaptive_flash_loan_cap(9, 50_000),
            Some((25_000, 50_000))
        );
        assert_eq!(
            service.demote_adaptive_flash_loan_cap(9, 50_000),
            Some((50_000, 25_000))
        );
        assert_eq!(
            service.demote_adaptive_flash_loan_cap(9, 50_000),
            Some((25_000, 12_500))
        );
        // Floor at configured/4 — further demote is a no-op.
        assert_eq!(service.demote_adaptive_flash_loan_cap(9, 50_000), None);
        assert_eq!(service.adaptive_flash_loan_usd(9, 50_000), 12_500);
        assert!(flash_size_failure_reason(
            "execution reverted: BAL#528 (insufficient Balancer flash-loan balance)"
        ));
        assert!(!flash_size_failure_reason(
            "execution reverted: InsufficientProfit"
        ));
        drop(service);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn candidate_generation_tracks_state_cache_at_build_time() {
        let cache = StateCache::new(8, std::time::Duration::from_secs(60));
        let mut candidate = CandidateExecution {
            route_fingerprint: 1,
            calldata: Default::default(),
            target_address: Address::ZERO,
            value: U256::ZERO,
            profit_token: Address::ZERO,
            expected_profit_matic_wei: U256::ZERO,
            priority_bid_basis_matic_wei: U256::ZERO,
            gas_limit: None,
            simulated_gas: 1,
            route_hash: Default::default(),
            gross_profit: U256::ZERO,
            amount_in: U256::ZERO,
            token_decimals: 18,
            token_to_matic_rate: U256::from(1u8),
            slippage_bps: 0,
            flash_loan_source: FlashLoanSource::Balancer,
            min_profit_matic_wei: U256::ZERO,
            min_profit_roi_bps: 0,
            hop_count: 1,
            safety_multiplier_bps: 0,
            state_generation: cache.generation(),
            state_block: 0,
            state_hash: None,
            route_trace: String::new(),
            adaptive_flash_cap_bound: false,
            adaptive_flash_loan_usd_limit: 50_000,
        };

        assert!(ExecutionService::candidate_matches_state_generation(
            &candidate, &cache
        ));
        cache.insert(
            Address::repeat_byte(1),
            crate::core::types::PoolState::Invalid,
        );
        // Generation advanced on unrelated write — still eligible (block pin is the gate).
        assert!(ExecutionService::candidate_matches_state_generation(
            &candidate, &cache
        ));
        // Candidate from the future is rejected (cache regressed / inconsistent snapshot).
        candidate.state_generation = cache.generation() + 10;
        assert!(!ExecutionService::candidate_matches_state_generation(
            &candidate, &cache
        ));
    }

    #[test]
    fn route_stats_appends_events_and_replays() {
        let path = std::env::temp_dir().join(format!(
            "rpbot-route-stats-{}-{}.log",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_nanos()
        ));
        let service = ExecutionService::with_route_stats_path(path.clone());
        service.route_stats.write().clear();

        for _ in 0..100 {
            service.record_route_success(7);
        }
        service.record_route_dry_run_pass(7);
        service.record_route_failure(7, RouteFailureKind::DryRun);
        assert_eq!(
            service.promote_adaptive_flash_loan_cap(7, 50_000),
            Some((12_500, 25_000))
        );
        service.route_stats_writer.flush();

        let saved = ExecutionService::replay_route_stats(&path);
        let stats = saved.get(&7).expect("saved route");
        assert_eq!(stats.successes, 100);
        assert_eq!(stats.dry_run_successes, 1);
        assert_eq!(stats.failures, 1);
        assert_eq!(stats.dry_run_failures, 1);
        assert_eq!(stats.adaptive_flash_loan_usd, Some(25_000));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn transfer_failed_quarantines_nested_token_not_hub_profit_token() {
        let failing = address!("0xd93f7E271cB87c23AaA73edC008A79646d1F9912");
        let wmatic = crate::core::constants::WMATIC;
        // Mid-hop (index≥1) TransferFailed = chain_in underfund, not FoT.
        // Live: hop1 V3 callback TransferFailed on BRZ/CES after WPOL→intermediate.
        let mid_hop = Some(DecodedRevert::ExternalCallFailed {
            index: 1,
            target: address!("0x296b95DD0E8B726c4e358b0683ff0B6d675C35E9"),
            reason: format!(
                "TransferFailed: token={failing}, to=0x296b95DD0E8B726c4e358b0683ff0B6d675C35E9, amount=847905430719831828"
            ),
        });
        assert!(is_mid_hop_transfer_underfund(&mid_hop));
        assert_eq!(
            transfer_failed_token_to_quarantine(&mid_hop, wmatic),
            None,
            "must not FoT-cool intermediate on mid-hop underfund"
        );
        // Hop-0 nested TransferFailed still cools the failing (non-hub) token.
        let hop0_nested = Some(DecodedRevert::ExternalCallFailed {
            index: 0,
            target: address!("0x28056401Bb178061950b5Db21fEEED261b808E6C"),
            reason: format!(
                "TransferFailed: token={failing}, to=0x28056401Bb178061950b5Db21fEEED261b808E6C, amount=1648989"
            ),
        });
        assert!(!is_mid_hop_transfer_underfund(&hop0_nested));
        assert_eq!(
            transfer_failed_token_to_quarantine(&hop0_nested, wmatic),
            Some(failing)
        );
        // Never cool WMATIC even if nested token is missing and hop==0.
        let hop0_string = Some(DecodedRevert::ExternalCallFailed {
            index: 0,
            target: Address::ZERO,
            reason: "UniswapV2: TRANSFER_FAILED".into(),
        });
        assert_eq!(
            transfer_failed_token_to_quarantine(&hop0_string, wmatic),
            None
        );
        // Non-hub FoT start on hop-0 UniV2 string still cools start token.
        let lgns = address!("0xeB51D9A39AD5EEF215dC0Bf39a8821ff804A0F01");
        assert_eq!(
            transfer_failed_token_to_quarantine(&hop0_string, lgns),
            Some(lgns)
        );
        // Mid-hop UniV2 string without token address must not cool start.
        let hop1_string = Some(DecodedRevert::ExternalCallFailed {
            index: 1,
            target: Address::ZERO,
            reason: "UniswapV2: TRANSFER_FAILED".into(),
        });
        assert_eq!(
            transfer_failed_token_to_quarantine(&hop1_string, lgns),
            None
        );
        // Top-level TransferFailed on a hub token is ignored.
        let top_hub = Some(DecodedRevert::TransferFailed {
            token: wmatic,
            to: Address::ZERO,
            amount: U256::from(1u64),
        });
        assert_eq!(transfer_failed_token_to_quarantine(&top_hub, lgns), None);
        // Top-level TransferFailed on long-tail cools that token.
        let top_tail = Some(DecodedRevert::TransferFailed {
            token: failing,
            to: Address::ZERO,
            amount: U256::from(1u64),
        });
        assert_eq!(
            transfer_failed_token_to_quarantine(&top_tail, wmatic),
            Some(failing)
        );
    }

    #[test]
    fn quarantine_probe_below_dispatch_floor_extends_and_is_idempotent() {
        let path = PathBuf::from(format!(
            "/tmp/rpbot-probe-floor-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_nanos()
        ));
        let service = ExecutionService::with_route_stats_path(path.clone());
        let fp = 42u64;
        assert!(!service.is_route_quarantined(fp));
        assert!(service.quarantine_probe_below_dispatch_floor(fp));
        assert!(service.is_route_quarantined(fp));
        // Second call within TTL is a no-op (already cool until ≥300s).
        assert!(!service.quarantine_probe_below_dispatch_floor(fp));
        assert!(service.is_route_quarantined(fp));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn cycle_has_quarantined_token_matches_mid_hop_not_only_start() {
        use crate::core::types::{Edge, PoolIndex, ProtocolType};
        use crate::pipeline::arena::StateArena;

        let path = PathBuf::from(format!(
            "/tmp/rpbot-fot-cycle-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_nanos()
        ));
        let service = ExecutionService::with_route_stats_path(path.clone());
        // Seeded KNOWN_FOT includes Wrapped SOL.
        let sol = address!("0xd93f7E271cB87c23AaA73edC008A79646d1F9912");
        let wmatic = crate::core::constants::WMATIC;
        assert!(service.is_direct_token_quarantined(sol));
        assert!(!service.is_direct_token_quarantined(wmatic));

        let mut arena = StateArena::default();
        let t_wmatic = arena.register_token(wmatic);
        let t_sol = arena.register_token(sol);
        let t_usdc = arena.register_token(crate::core::constants::USDC_E);
        let edges = [
            Edge {
                pool_index: PoolIndex(0),
                token_in: t_wmatic,
                token_out: t_sol,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV3,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: PoolIndex(1),
                token_in: t_sol,
                token_out: t_usdc,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV3,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: PoolIndex(2),
                token_in: t_usdc,
                token_out: t_wmatic,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV3,
                fee_bps: 30,
                zero_for_one: true,
            },
        ];
        assert!(
            service.cycle_has_quarantined_token(&arena, &edges),
            "mid-hop Wrapped SOL must cool the whole cycle"
        );
        let hub_only = [Edge {
            pool_index: PoolIndex(0),
            token_in: t_wmatic,
            token_out: t_usdc,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: true,
        }];
        assert!(!service.cycle_has_quarantined_token(&arena, &hub_only));
        let _ = std::fs::remove_file(&path);
    }
}
