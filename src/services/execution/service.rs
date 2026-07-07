use std::io::{BufRead, BufReader, BufWriter, Write};
use std::sync::Arc;

use anyhow::Context;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};
use rustc_hash::FxHashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use alloy::network::Ethereum;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use tokio::sync::watch;

use crate::config::AppConfig;
use crate::config::WalletSecrets;
use crate::core::types::FlashLoanSource;
use crate::infra::hypersync::HyperSyncService;
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
use crate::services::execution::nonce::NonceManager;
use crate::services::execution::private_submit::{
    PrivateSubmitConfig, PrivateSubmitMode, resolve_submit_mode,
};
use crate::services::execution::profit::{AssessProfitInput, assess_profit};
use crate::services::execution::profit_logs::parse_transfer_profit;
use crate::services::execution::receipt::ReceiptPoller;
use crate::services::execution::recovery::{NonceRecoveryOutcome, recover_after_receipt_timeout};
use crate::services::execution::rpc_errors::{SubmitAction, classify_submit_error};
use crate::services::execution::submit::{resolve_submit_fees_with_profit, submit_with_recovery};
use crate::services::state_cache::StateCache;

const ROUTE_COOLDOWN: Duration = Duration::from_secs(30);
const PERMANENT_QUARANTINE: Duration = Duration::from_secs(3600);
const MAX_CONSECUTIVE_FAILURES: u32 = 5;

#[derive(Debug, Clone, Default)]
struct RouteStats {
    successes: u64,
    failures: u64,
    dry_run_failures: u64,
    submit_failures: u64,
    receipt_timeouts: u64,
    reverts: u64,
    realized_losses: u64,
}

#[derive(Debug, Clone, Copy)]
enum RouteFailureKind {
    DryRun,
    Submit,
    Timeout,
    Revert,
    RealizedLoss,
}

#[derive(Debug)]
pub struct ExecutionService {
    last_submit: RwLock<FxHashMap<u64, Instant>>,
    last_global_submit: Mutex<Option<Instant>>,
    quarantine: RwLock<FxHashMap<u64, Instant>>,
    global_quarantine_until: Mutex<Option<Instant>>,
    fail_counts: RwLock<FxHashMap<u64, u32>>,
    nonce: RwLock<Option<(Address, Arc<NonceManager>)>>,
    pub flash_liquidity: Arc<FlashLiquidityCache>,
    pnl: Mutex<(i128, i128)>,
    pub total_trades: AtomicU64,
    pub total_losses: AtomicU64,
    pub consecutive_fails: AtomicU32,
    route_stats: RwLock<FxHashMap<u64, RouteStats>>,
    route_stats_path: PathBuf,
    last_near_miss_log: Mutex<Option<(u64, U256)>>,
    last_dispatch_log: Mutex<Option<(u64, U256)>>,
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
        Self::with_route_stats_path(PathBuf::from(path))
    }

    fn with_route_stats_path(route_stats_path: PathBuf) -> Self {
        let route_stats = Self::replay_route_stats(&route_stats_path);
        Self {
            last_submit: RwLock::new(FxHashMap::default()),
            last_global_submit: parking_lot::Mutex::new(None),
            quarantine: RwLock::new(FxHashMap::default()),
            global_quarantine_until: parking_lot::Mutex::new(None),
            fail_counts: RwLock::new(FxHashMap::default()),
            nonce: RwLock::new(None),
            flash_liquidity: Arc::new(FlashLiquidityCache::new()),
            pnl: Mutex::new((0, 0)),
            total_trades: AtomicU64::new(0),
            total_losses: AtomicU64::new(0),
            consecutive_fails: AtomicU32::new(0),
            route_stats: RwLock::new(route_stats),
            route_stats_path,
            last_near_miss_log: Mutex::new(None),
            last_dispatch_log: Mutex::new(None),
        }
    }
}

impl ExecutionService {
    fn write_route_event(&self, line: &str) {
        let path = &self.route_stats_path;
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
        {
            // ponytail: BufWriter reduces syscall overhead when multiple route
            // events arrive in quick succession (HF ticks at ~200ms intervals).
            let mut writer = BufWriter::new(&mut file);
            let _ = writeln!(writer, "{}", line);
            let _ = writer.flush();
        }
    }

    fn replay_route_stats(path: &std::path::Path) -> FxHashMap<u64, RouteStats> {
        let Ok(file) = std::fs::File::open(path) else {
            return FxHashMap::default();
        };
        let mut stats = FxHashMap::default();
        for line in BufReader::new(file).lines() {
            let Ok(line) = line else { continue };
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 {
                continue;
            }
            let Ok(fp) = parts[0].parse::<u64>() else { continue };
            let entry: &mut RouteStats = stats.entry(fp).or_default();
            match parts[1] {
                "s" => entry.successes += 1,
                "f" => {
                    entry.failures += 1;
                    match parts.get(2).copied() {
                        Some("DryRun") => entry.dry_run_failures += 1,
                        Some("Submit") => entry.submit_failures += 1,
                        Some("Timeout") => entry.receipt_timeouts += 1,
                        Some("Revert") => entry.reverts += 1,
                        Some("RealizedLoss") => entry.realized_losses += 1,
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        stats
    }

    fn record_route_failure(&self, fp: u64, kind: RouteFailureKind) {
        let mut all = self.route_stats.write();
        let stats = all.entry(fp).or_default();
        stats.failures += 1;
        match kind {
            RouteFailureKind::DryRun => stats.dry_run_failures += 1,
            RouteFailureKind::Submit => stats.submit_failures += 1,
            RouteFailureKind::Timeout => stats.receipt_timeouts += 1,
            RouteFailureKind::Revert => stats.reverts += 1,
            RouteFailureKind::RealizedLoss => stats.realized_losses += 1,
        }
        drop(all);
        self.write_route_event(&format!("{} f {:?}", fp, kind));
    }

    fn record_route_success(&self, fp: u64) {
        self.route_stats.write().entry(fp).or_default().successes += 1;
        self.write_route_event(&format!("{} s", fp));
    }

    /// Learned minimum-profit uplift. With fewer than three outcomes there is
    /// no penalty; afterwards failure probability can raise the floor to 3x.
    pub fn route_risk_multiplier_bps(&self, fp: u64) -> u64 {
        let stats = self.route_stats.read();
        let Some(stats) = stats.get(&fp) else {
            return 10_000;
        };
        let attempts = stats.successes.saturating_add(stats.failures);
        if attempts < 3 {
            return 10_000;
        }
        let weighted_failures = stats
            .failures
            .saturating_add(stats.reverts)
            .saturating_add(stats.receipt_timeouts / 2);
        10_000u64
            .saturating_add(weighted_failures.saturating_mul(20_000) / attempts)
            .min(30_000)
    }
    pub fn pnl_snapshot(&self) -> (i128, i128) {
        *self.pnl.lock()
    }

    fn record_realized(&self, profit_wei: U256, gas_cost_wei: U256) {
        if profit_wei > gas_cost_wei {
            self.consecutive_fails.store(0, Ordering::Relaxed);
            let p = profit_wei
                .saturating_sub(gas_cost_wei)
                .min(U256::from(i128::MAX as u128))
                .to::<u128>() as i128;
            let mut pnl = self.pnl.lock();
            pnl.0 = pnl.0.saturating_add(p);
            pnl.1 = pnl.1.saturating_add(p);
            self.total_trades.fetch_add(1, Ordering::Relaxed);
        } else {
            let loss = gas_cost_wei
                .saturating_sub(profit_wei)
                .min(U256::from(i128::MAX as u128))
                .to::<u128>() as i128;
            let mut pnl = self.pnl.lock();
            pnl.0 = pnl.0.saturating_sub(loss);
            pnl.1 = pnl.1.saturating_sub(loss);
            self.total_losses.fetch_add(1, Ordering::Relaxed);
            self.consecutive_fails.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn token_profit_to_matic_wei(
    profit_token_units: U256,
    token_to_matic_rate: U256,
    token_decimals: u8,
) -> Option<U256> {
    if token_to_matic_rate < crate::services::execution::profit::MIN_TOKEN_TO_MATIC_RATE {
        return None;
    }
    profit_token_units
        .checked_mul(token_to_matic_rate)?
        .checked_div(crate::util::ten_pow_u256(token_decimals))
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
    SkippedQuarantined,
    SkippedCooldown,
    SkippedNoWallet,
    SkippedNoPrivateRpc,
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

    pub fn any_quarantined(&self, fingerprints: &[u64]) -> bool {
        let q = self.quarantine.read();
        let now = Instant::now();
        fingerprints
            .iter()
            .any(|fp| q.get(fp).is_some_and(|expiry| now < *expiry))
    }

    pub fn is_route_quarantined(&self, fingerprint: u64) -> bool {
        self.any_quarantined(&[fingerprint])
    }

    /// Suppress duplicate near-miss spam when fp and net MATIC are unchanged.
    pub fn should_log_near_miss(&self, fingerprint: u64, net_matic: U256) -> bool {
        let mut last = self.last_near_miss_log.lock();
        if *last == Some((fingerprint, net_matic)) {
            return false;
        }
        *last = Some((fingerprint, net_matic));
        true
    }

    /// Suppress duplicate dispatch logs when the same route is re-evaluated every HF tick.
    pub fn should_log_dispatch(&self, fingerprint: u64, profit_matic: U256) -> bool {
        let mut last = self.last_dispatch_log.lock();
        if *last == Some((fingerprint, profit_matic)) {
            return false;
        }
        *last = Some((fingerprint, profit_matic));
        true
    }

    fn quarantine_route(&self, fp: u64, now: Instant, kind: RouteFailureKind) {
        self.record_route_failure(fp, kind);
        // Lock order: fail_counts → quarantine (always acquire in this order).
        let count = {
            let mut fc = self.fail_counts.write();
            let count = fc.entry(fp).or_insert(0);
            *count += 1;
            *count
        };
        let cooldown = if count >= MAX_CONSECUTIVE_FAILURES {
            PERMANENT_QUARANTINE
        } else {
            ROUTE_COOLDOWN
        };
        self.quarantine.write().insert(fp, now + cooldown);
    }

    fn quarantine_route_soft(&self, fp: u64, now: Instant) {
        let mut q = self.quarantine.write();
        q.insert(fp, now + ROUTE_COOLDOWN);
    }

    pub fn quarantine_global(&self, duration: Duration, now: Instant) {
        *self.global_quarantine_until.lock() = Some(now + duration);
    }

    pub fn global_is_quarantined(&self) -> bool {
        self.global_quarantine_until
            .lock()
            .is_some_and(|expiry| Instant::now() < expiry)
    }

    fn clear_fail_count(&self, fp: u64) {
        self.fail_counts.write().remove(&fp);
        self.record_route_success(fp);
    }

    async fn operator_mempool_clear<P: Provider<Ethereum>>(
        &self,
        provider: &P,
        operator: Address,
    ) -> anyhow::Result<bool> {
        const MEMPOOL_STALL_TIMEOUT: Duration = Duration::from_secs(20);
        let (latest_res, pending_res) = tokio::join!(
            provider.get_transaction_count(operator),
            provider
                .get_transaction_count(operator)
                .block_id(alloy::eips::BlockId::pending()),
        );
        let latest = latest_res.context("failed to read latest operator nonce")?;
        let pending = pending_res.context("failed to read pending operator nonce")?;
        if pending == latest {
            return Ok(true);
        }
        if let Some(last) = *self.last_global_submit.lock()
            && last.elapsed() > MEMPOOL_STALL_TIMEOUT
        {
            crate::warn!(
                "mempool not clear for {:.0}s — resyncing nonce",
                last.elapsed().as_secs_f64()
            );
            return Ok(true);
        }
        Ok(false)
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
            && mgr.in_flight_count() > 0
        {
            let _ = mgr.resync(provider).await;
        }
    }

    fn reassess_assessment(
        candidate: &CandidateExecution,
        dry_run_gas: u64,
        gas_price: U256,
        min_profit_matic_wei: U256,
        realized_profit: Option<U256>,
    ) -> Option<crate::core::types::ProfitAssessment> {
        let gas_units = candidate
            .simulated_gas
            .max(u32::try_from(dry_run_gas).unwrap_or(candidate.simulated_gas));
        Some(assess_profit(&AssessProfitInput {
            // Executor return data is post-repayment realized profit, so do
            // not apply modeled slippage or flash-loan fees a second time.
            gross_profit: realized_profit.unwrap_or(candidate.gross_profit),
            amount_in: candidate.amount_in,
            gas_units,
            gas_price_wei: gas_price,
            token_to_matic_rate: candidate.token_to_matic_rate,
            token_decimals: candidate.token_decimals,
            hop_count: candidate.hop_count,
            min_profit_matic_wei,
            min_profit_roi_bps: candidate.min_profit_roi_bps,
            slippage_bps: realized_profit.map_or(candidate.slippage_bps, |_| 0),
            flash_loan_source: realized_profit
                .map_or(candidate.flash_loan_source, |_| FlashLoanSource::Direct),
            safety_multiplier_bps: candidate.safety_multiplier_bps,
        }))
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
        hypersync: Option<&HyperSyncService>,
        ui_hook: Option<&SharedUiHook>,
        shutdown: Option<&watch::Receiver<bool>>,
        _metrics: Option<&()>,
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
            let outcome = ExecutionOutcome::SkippedCircuitBreaker;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        let now = Instant::now();

        let risk_multiplier = self.route_risk_multiplier_bps(fp);
        let learned_floor = candidate
            .min_profit_matic_wei
            .saturating_mul(U256::from(risk_multiplier))
            / U256::from(10_000u64);
        if candidate.expected_profit_matic_wei < learned_floor {
            let outcome = ExecutionOutcome::SkippedUnprofitableAfterDryRun;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        if state_cache.generation() != candidate.state_generation {
            crate::info!(
                "dispatch skip: fp={}, stale state (candidate_gen={}, cache_gen={})",
                fp,
                candidate.state_generation,
                state_cache.generation()
            );
            return ExecutionOutcome::SkippedCooldown;
        }
        let simulation_block = match sim_provider.get_block_number().await {
            Ok(block) => block,
            Err(e) => {
                return ExecutionOutcome::SubmitFailed {
                    reason: format!("cannot establish simulation block: {e}"),
                };
            }
        };

        if let Some(expiry) = self.quarantine.read().get(&fp)
            && now < *expiry
        {
            let outcome = ExecutionOutcome::SkippedQuarantined;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        if let Some(last) = self.last_submit.read().get(&fp)
            && now.saturating_duration_since(*last) < ROUTE_COOLDOWN
        {
            let outcome = ExecutionOutcome::SkippedCooldown;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        let dry = dry_run_candidate(sim_provider, candidate, operator).await;

        if !dry.success {
            self.quarantine_route(fp, now, RouteFailureKind::DryRun);
            crate::info!(
                "dry-run failed: fp={}, reason={}",
                fp,
                dry.error.as_deref().unwrap_or("unknown")
            );
            let outcome = ExecutionOutcome::DryRunFailed {
                reason: dry.error.unwrap_or_else(|| "unknown".into()),
            };
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
            crate::info!(
                "dry-run pass: fp={}, gas_used={}, sim_gas={}",
                fp,
                gas_used,
                candidate.simulated_gas
            );
        }
        gas_oracle.record_sim_observed(candidate.simulated_gas, gas_used);
        if gas_used > 0 {
            gas_oracle.record_route_gas(
                candidate.route_fingerprint,
                u32::try_from(gas_used).unwrap_or(u32::MAX),
            );
        }
        let gas_fallback = dry.gas_used.is_none();
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
                    reason: e.to_string(),
                };
                if let Some(ui_hook) = ui_hook {
                    ui_hook.on_execution_outcome(&outcome, fp);
                }
                return outcome;
            }
        };

        let Some(fees) = resolve_submit_fees_with_profit(
            gas_oracle,
            candidate.expected_profit_matic_wei,
            config.execution.profit_priority_fee_alpha_bps,
            final_gas,
        ) else {
            let outcome = ExecutionOutcome::SubmitFailed {
                reason: "gas oracle has no snapshot for fee resolution".into(),
            };
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        };

        // Gas-overflow fallback has no RPC gas observation — reassessing at
        // submit max_fee × sim_gas inflates the safety floor above HF eval.
        let reassess_gas_price = if dry.gas_used.is_none() {
            gas_oracle
                .conservative_gas_price()
                .unwrap_or(fees.max_fee_per_gas)
        } else {
            fees.max_fee_per_gas
        };
        let profit_gas = profit_reassess_gas(
            prior_observed_gas,
            candidate.simulated_gas,
            dry.gas_used,
            gas_fallback,
        );
        let dry_pass = Self::reassess_assessment(
            candidate,
            profit_gas,
            reassess_gas_price,
            learned_floor,
            dry.realized_profit,
        )
        .is_some_and(|a| a.should_execute);
        if !dry_pass {
            crate::info!(
                "dispatch skip: fp={}, unprofitable after dry-run (profit_matic={}, profit_gas={}, submit_gas={}, reassess_fee_gwei={}, submit_max_fee_gwei={})",
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

        if config.is_dry_run() {
            let outcome = ExecutionOutcome::DryRunPassed { gas_used };
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        let mempool_clear = match self.operator_mempool_clear(sim_provider, operator).await {
            Ok(clear) => clear,
            Err(e) => {
                return ExecutionOutcome::SubmitFailed {
                    reason: e.to_string(),
                };
            }
        };
        if !mempool_clear {
            let outcome = ExecutionOutcome::SkippedCooldown;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        let Some(signer) = wallet.signer() else {
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
            let outcome = ExecutionOutcome::SkippedCircuitBreaker;
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        }

        // Dry-run eth_call already validated against live chain state; a newer
        // local cache generation must not block submit after a passing simulation.
        match sim_provider.get_block_number().await {
            Ok(head) if head <= simulation_block.saturating_add(1) => {}
            Ok(head) => {
                return ExecutionOutcome::SubmitFailed {
                    reason: format!(
                        "candidate stale: simulated at block {simulation_block}, head is {head}"
                    ),
                };
            }
            Err(e) => {
                return ExecutionOutcome::SubmitFailed {
                    reason: format!("pre-submit head check failed: {e}"),
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

        let nonce = match nonce_mgr.next_nonce() {
            Ok(n) => n,
            Err(e) => {
                let outcome = ExecutionOutcome::SubmitFailed {
                    reason: e.to_string(),
                };
                if let Some(ui_hook) = ui_hook {
                    ui_hook.on_execution_outcome(&outcome, fp);
                }
                return outcome;
            }
        };

        let private_cfg = build_private_config(rpc, signer, &submit_provider).await;

        let tx_hash = match submit_with_recovery(
            &submit_provider,
            &nonce_mgr,
            candidate,
            nonce,
            fees,
            final_gas,
            private_cfg.as_ref(),
        )
        .await
        {
            Ok(hash) => hash,
            Err(e) => {
                nonce_mgr.release(nonce);
                self.consecutive_fails.fetch_add(1, Ordering::Relaxed);
                match classify_submit_error(&e) {
                    SubmitAction::ResyncAndRetry => {
                        self.quarantine_route_soft(fp, now);
                    }
                    _ => {
                        self.quarantine_route(fp, now, RouteFailureKind::Submit);
                    }
                }
                let outcome = ExecutionOutcome::SubmitFailed {
                    reason: e.to_string(),
                };
                if let Some(ui_hook) = ui_hook {
                    ui_hook.on_execution_outcome(&outcome, fp);
                }
                return outcome;
            }
        };
        *self.last_global_submit.lock() = Some(now);

        let poller = ReceiptPoller::new(
            Duration::from_millis(config.execution.receipt_timeout_ms),
            Duration::from_millis(config.execution.receipt_poll_ms),
        );

        let tx_hash_str = tx_hash.to_string();

        let Some(receipt) = poller
            .wait_with_hypersync(sim_provider, tx_hash, hypersync, shutdown)
            .await
        else {
            if shutdown.is_some_and(|rx| *rx.borrow()) {
                nonce_mgr.release(nonce);
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
            )
            .await
            {
                NonceRecoveryOutcome::Mined(receipt) => {
                    return self.finalize_receipt(
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
                    );
                }
                NonceRecoveryOutcome::Cancelled(_cancel_hash) => {}
                NonceRecoveryOutcome::Dropped | NonceRecoveryOutcome::StillPending => {}
            }

            self.quarantine_route(fp, now, RouteFailureKind::Timeout);
            let outcome = ExecutionOutcome::ReceiptTimeout {
                tx_hash: tx_hash_str,
            };
            if let Some(ui_hook) = ui_hook {
                ui_hook.on_execution_outcome(&outcome, fp);
            }
            return outcome;
        };

        self.finalize_receipt(
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
        )
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
                .unwrap_or_else(|| {
                    crate::warn!("no effective_gas_price in revert receipt fp={fp}");
                    U256::ZERO
                });
            self.record_realized(U256::ZERO, gas_cost);
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
        let gas_cost = receipt
            .effective_gas_price
            .and_then(|price| U256::from(receipt.gas_used).checked_mul(U256::from(price)))
            .unwrap_or_else(|| {
                crate::warn!("no effective_gas_price in success receipt fp={fp}");
                U256::ZERO
            });
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
            if profit_matic_wei > gas_cost {
                self.clear_fail_count(fp);
            } else {
                self.quarantine_route(fp, now, RouteFailureKind::RealizedLoss);
            }
        } else {
            self.quarantine_route(fp, now, RouteFailureKind::RealizedLoss);
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

async fn build_private_config(
    rpc: &RpcPool,
    signer: &alloy::signers::local::PrivateKeySigner,
    submit_provider: &alloy::providers::DynProvider,
) -> Option<PrivateSubmitConfig> {
    let private_url = rpc.private_url().map(str::to_string);
    let bloxroute_auth = std::env::var("BLOXROUTE_AUTH_HEADER")
        .ok()
        .filter(|s| !s.is_empty());
    let mode = resolve_submit_mode(private_url.as_deref(), bloxroute_auth.as_deref(), None);
    if mode == PrivateSubmitMode::Standard {
        return None;
    }
    let chain_id = submit_provider.get_chain_id().await.ok()?;
    Some(PrivateSubmitConfig {
        mode,
        signer: signer.clone(),
        chain_id,
        private_url,
        bloxroute_auth,
    })
}

fn min_operator_balance_wei(config: &AppConfig) -> Option<U256> {
    config
        .execution
        .min_operator_matic_wei
        .parse::<U256>()
        .ok()
        .filter(|v| !v.is_zero())
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
    fn realized_profit_short_circuits_modeled_slippage_and_flash_fee() {
        let candidate = CandidateExecution {
            route_fingerprint: 7,
            calldata: Default::default(),
            target_address: Address::repeat_byte(7),
            value: U256::ZERO,
            profit_token: Address::repeat_byte(8),
            expected_profit_matic_wei: U256::ZERO,
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
        };

        let assessment = ExecutionService::reassess_assessment(
            &candidate,
            100,
            U256::from(1u8),
            U256::ZERO,
            Some(U256::from(900u64)),
        )
        .expect("realized profit should produce an assessment");

        assert_eq!(assessment.gross_profit, U256::from(900u64));
        assert_eq!(assessment.slippage_deduction, U256::ZERO);
        assert_eq!(assessment.flash_loan_fee, U256::ZERO);
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
    fn route_stats_appends_events_and_replays() {
        let mut service = ExecutionService::new();
        service.route_stats.write().clear();
        service.route_stats_path = std::env::temp_dir().join(format!(
            "rpbot-route-stats-{}-{}.log",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_nanos()
        ));

        for _ in 0..100 {
            service.record_route_success(7);
        }
        service.record_route_failure(7, RouteFailureKind::DryRun);

        let saved = ExecutionService::replay_route_stats(&service.route_stats_path);
        let stats = saved.get(&7).expect("saved route");
        assert_eq!(stats.successes, 100);
        assert_eq!(stats.failures, 1);
        assert_eq!(stats.dry_run_failures, 1);

        let _ = std::fs::remove_file(&service.route_stats_path);
    }
}
