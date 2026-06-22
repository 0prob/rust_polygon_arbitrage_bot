use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use std::time::{Duration, Instant};

use alloy::network::Ethereum;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use tokio::sync::watch;
use tracing::{Instrument, info, info_span, instrument, warn};

use crate::config::AppConfig;
use crate::config::WalletSecrets;
use crate::infra::hypersync::HyperSyncService;
use crate::infra::metrics::PipelineMetrics;
use crate::infra::rpc::RpcPool;
use crate::infra::tracing_util::{record_candidate, record_gas_fees, record_receipt, record_tx};
use crate::services::execution::candidate::CandidateExecution;
use crate::services::execution::circuit_breaker::CircuitBreaker;
use crate::services::execution::dryrun::dry_run_candidate;
use crate::services::execution::flash_liquidity::FlashLiquidityCache;
use crate::services::execution::gas::{gas_drift_bps, pick_live_gas_limit};
use crate::services::execution::gas_oracle::GasOracle;
use crate::services::execution::nonce::NonceManager;
use crate::services::execution::opportunity_log::log_opportunity_outcome;
use crate::services::execution::profit::{AssessProfitInput, assess_profit};
use crate::services::execution::profit_logs::parse_transfer_profit;
use crate::services::execution::receipt::ReceiptPoller;
use crate::services::execution::recovery::{NonceRecoveryOutcome, recover_after_receipt_timeout};
use crate::services::execution::rpc_errors::{SubmitAction, classify_submit_error};
use crate::services::execution::submit::{
    resolve_submit_fees_with_profit, submit_with_recovery,
};

const ROUTE_COOLDOWN: Duration = Duration::from_secs(30);
const PERMANENT_QUARANTINE: Duration = Duration::from_secs(3600);
const MAX_CONSECUTIVE_FAILURES: u32 = 5;

#[derive(Debug, Default)]
pub struct ExecutionService {
    last_submit: RwLock<HashMap<u64, Instant>>,
    quarantine: RwLock<HashMap<u64, Instant>>,
    fail_counts: RwLock<HashMap<u64, u32>>,
    nonce: RwLock<Option<(Address, Arc<NonceManager>)>>,
    pub flash_liquidity: FlashLiquidityCache,
    pub circuit_breaker: CircuitBreaker,
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
    SkippedDryRunOnly,
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
    pub fn new() -> Self {
        Self::with_circuit_breaker_cooldown(std::time::Duration::from_secs(300))
    }

    pub fn with_circuit_breaker_cooldown(cooldown: std::time::Duration) -> Self {
        Self {
            flash_liquidity: FlashLiquidityCache::new(),
            circuit_breaker: CircuitBreaker::new(cooldown),
            ..Self::default()
        }
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

    fn quarantine_route(&self, fp: u64, now: Instant) {
        let mut q = self.quarantine.write();
        let mut fc = self.fail_counts.write();
        let count = fc.entry(fp).or_insert(0);
        *count += 1;
        let cooldown = if *count >= MAX_CONSECUTIVE_FAILURES {
            info!(
                fingerprint = fp,
                failures = *count,
                "route permanently quarantined"
            );
            PERMANENT_QUARANTINE
        } else {
            ROUTE_COOLDOWN
        };
        q.insert(fp, now + cooldown);
    }

    fn quarantine_route_soft(&self, fp: u64, now: Instant) {
        let mut q = self.quarantine.write();
        q.insert(fp, now + ROUTE_COOLDOWN);
    }

    fn clear_fail_count(&self, fp: u64) {
        self.fail_counts.write().remove(&fp);
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
            info!(
                in_flight = mgr.in_flight_count(),
                "resyncing nonce manager on shutdown"
            );
            if let Err(e) = mgr.resync(provider).await {
                warn!(error = %e, "shutdown nonce resync failed");
            }
        }
    }

    fn reassess_after_dry_run(
        candidate: &CandidateExecution,
        dry_run_gas: u64,
        gas_price: U256,
    ) -> bool {
        Self::reassess_assessment(candidate, dry_run_gas, gas_price)
            .map(|a| a.should_execute)
            .unwrap_or(false)
    }

    fn reassess_assessment(
        candidate: &CandidateExecution,
        dry_run_gas: u64,
        gas_price: U256,
    ) -> Option<crate::core::types::ProfitAssessment> {
        let gas_units = candidate
            .simulated_gas
            .max(u32::try_from(dry_run_gas).unwrap_or(u32::MAX));
        Some(assess_profit(AssessProfitInput {
            gross_profit: candidate.gross_profit,
            amount_in: candidate.amount_in,
            gas_units,
            gas_price_wei: gas_price,
            token_to_matic_rate: candidate.token_to_matic_rate,
            token_decimals: candidate.token_decimals,
            hop_count: candidate.hop_count,
            min_profit_matic_wei: candidate.min_profit_matic_wei,
            min_profit_roi_bps: candidate.min_profit_roi_bps,
            slippage_bps: candidate.slippage_bps,
            flash_loan_source: candidate.flash_loan_source,
            safety_multiplier_bps: candidate.safety_multiplier_bps,
        }))
    }

    #[instrument(
        skip(self, sim_provider, config, candidate, gas_oracle, hypersync, shutdown, rpc, wallet),
        fields(
            route_fingerprint = candidate.route_fingerprint,
            expected_profit_matic_wei = %candidate.expected_profit_matic_wei,
            simulated_gas = candidate.simulated_gas,
            outcome = tracing::field::Empty,
        )
    )]
    pub async fn process_candidate<P: Provider<Ethereum>>(
        &self,
        sim_provider: &P,
        rpc: &RpcPool,
        wallet: &WalletSecrets,
        config: &AppConfig,
        candidate: &CandidateExecution,
        operator: Address,
        gas_oracle: &GasOracle,
        hypersync: Option<&HyperSyncService>,
        shutdown: Option<&watch::Receiver<bool>>,
        _metrics: Option<&PipelineMetrics>,
    ) -> ExecutionOutcome {
        if shutdown.is_some_and(|rx| *rx.borrow()) {
            tracing::Span::current().record("outcome", "skipped_shutdown");
            return ExecutionOutcome::SkippedShutdown;
        }

        if self.circuit_breaker.is_paused() {
            self.circuit_breaker.try_auto_reset();
        }
        if self.circuit_breaker.is_paused() {
            tracing::Span::current().record("outcome", "skipped_circuit_breaker");
            return ExecutionOutcome::SkippedCircuitBreaker;
        }

        let fp = candidate.route_fingerprint;
        let now = Instant::now();

        if let Some(expiry) = self.quarantine.read().get(&fp)
            && now < *expiry
        {
            tracing::Span::current().record("outcome", "skipped_quarantined");
            return ExecutionOutcome::SkippedQuarantined;
        }

        if let Some(last) = self.last_submit.read().get(&fp)
            && now.duration_since(*last) < ROUTE_COOLDOWN
        {
            tracing::Span::current().record("outcome", "skipped_cooldown");
            return ExecutionOutcome::SkippedCooldown;
        }

        let dry_span = info_span!("arb.dry_run", route_fingerprint = fp);
        let dry = dry_run_candidate(sim_provider, candidate, operator)
            .instrument(dry_span)
            .await;
        if !dry.success {
            self.quarantine_route(fp, now);
            self.circuit_breaker.record_failure(
                config.execution.max_global_consecutive_failures,
                "dry_run_failed",
            );
            tracing::Span::current().record("outcome", "dry_run_failed");
            return ExecutionOutcome::DryRunFailed {
                reason: dry.error.unwrap_or_else(|| "unknown".into()),
            };
        }

        let gas_used = dry.gas_used.unwrap_or(0);
        let gas_price = gas_oracle.conservative_gas_price();

        if !Self::reassess_after_dry_run(candidate, gas_used, gas_price) {
            info!(
                route_fingerprint = fp,
                dry_run_gas = gas_used,
                simulated_gas = candidate.simulated_gas,
                "unprofitable after dry-run gas — skipping"
            );
            if let Some(a) = Self::reassess_assessment(candidate, gas_used, gas_price) {
                log_opportunity_outcome(
                    fp,
                    &a,
                    candidate.slippage_bps,
                    Some(gas_used),
                    "skipped_unprofitable_after_dry_run",
                );
            }
            tracing::Span::current().record("outcome", "skipped_unprofitable_after_dry_run");
            return ExecutionOutcome::SkippedUnprofitableAfterDryRun;
        }

        let final_gas = match pick_live_gas_limit(candidate.simulated_gas, gas_used) {
            Ok(g) => g,
            Err(e) => {
                self.quarantine_route(fp, now);
                tracing::Span::current().record("outcome", "dry_run_failed");
                return ExecutionOutcome::DryRunFailed {
                    reason: e.to_string(),
                };
            }
        };

        info!(
            route_fingerprint = fp,
            dry_run_gas = gas_used,
            final_gas_limit = final_gas,
            simulated_gas = candidate.simulated_gas,
            gas_drift_bps = gas_drift_bps(candidate.simulated_gas as u64, gas_used),
            "simulation gas estimate"
        );

        if config.is_dry_run() {
            info!(
                route = fp,
                gas_used,
                profit_matic = %candidate.expected_profit_matic_wei,
                "dry-run passed — execution suppressed (dry-run mode)"
            );
            tracing::Span::current().record("outcome", "dry_run_passed");
            return ExecutionOutcome::DryRunPassed { gas_used };
        }

        if !wallet.has_signer() {
            warn!(route = fp, "live mode requires PRIVATE_KEY or PRIVATE_KEY_FILE");
            tracing::Span::current().record("outcome", "skipped_no_wallet");
            return ExecutionOutcome::SkippedNoWallet;
        }

        let Some(signer) = wallet.signer() else {
            warn!(route = fp, "live mode requires loaded signer");
            tracing::Span::current().record("outcome", "skipped_no_wallet");
            return ExecutionOutcome::SkippedNoWallet;
        };

        let submit_provider = match rpc.connect_submit(signer) {
            Ok(p) => p,
            Err(e) => {
                warn!(route = fp, error = %e, "private submit RPC unavailable");
                tracing::Span::current().record("outcome", "skipped_no_private_rpc");
                return ExecutionOutcome::SkippedNoPrivateRpc;
            }
        };

        if shutdown.is_some_and(|rx| *rx.borrow()) {
            tracing::Span::current().record("outcome", "skipped_shutdown");
            return ExecutionOutcome::SkippedShutdown;
        }

        if let Some(min_wei) = min_operator_balance_wei(config)
            && let Ok(balance) = sim_provider.get_balance(operator).await
        {
            let balance_u256 = U256::from(balance);
            if !self
                .circuit_breaker
                .check_operator_balance(balance_u256, min_wei)
            {
                tracing::Span::current().record("outcome", "skipped_circuit_breaker");
                return ExecutionOutcome::SkippedCircuitBreaker;
            }
        }

        let nonce_mgr = match self.ensure_nonce_manager(&submit_provider, operator).await {
            Ok(mgr) => mgr,
            Err(e) => {
                tracing::Span::current().record("outcome", "submit_failed");
                return ExecutionOutcome::SubmitFailed {
                    reason: format!("nonce init failed: {e}"),
                };
            }
        };

        let nonce = match nonce_mgr.next_nonce() {
            Ok(n) => n,
            Err(e) => {
                tracing::Span::current().record("outcome", "submit_failed");
                return ExecutionOutcome::SubmitFailed {
                    reason: e.to_string(),
                };
            }
        };

        let fees = resolve_submit_fees_with_profit(
            gas_oracle,
            candidate.expected_profit_matic_wei,
            config.execution.profit_priority_fee_alpha_bps,
            final_gas,
        );
        record_gas_fees(
            &tracing::Span::current(),
            fees.max_fee_per_gas,
            fees.max_priority_fee_per_gas,
        );

        let submit_span = info_span!(
            "arb.submit_tx",
            route_fingerprint = fp,
            nonce,
            gas_limit = final_gas,
        );
        record_candidate(&submit_span, candidate);

        let tx_hash =
            match submit_with_recovery(&submit_provider, &nonce_mgr, candidate, nonce, fees, final_gas)
                .instrument(submit_span)
                .await
            {
                Ok(hash) => hash,
                Err(e) => {
                    match classify_submit_error(&e) {
                        SubmitAction::InsufficientFunds => {
                            nonce_mgr.release(nonce);
                            self.quarantine_route(fp, now);
                        }
                        SubmitAction::ResyncAndRetry => {
                            nonce_mgr.release(nonce);
                            self.quarantine_route_soft(fp, now);
                        }
                        _ => {
                            nonce_mgr.release(nonce);
                            self.quarantine_route(fp, now);
                        }
                    }
                    tracing::Span::current().record("outcome", "submit_failed");
                    return ExecutionOutcome::SubmitFailed {
                        reason: e.to_string(),
                    };
                }
            };

        let poller = ReceiptPoller::new(
            Duration::from_millis(config.execution.receipt_timeout_ms),
            Duration::from_millis(config.execution.receipt_poll_ms),
        );

        let tx_hash_str = tx_hash.to_string();
        record_tx(&tracing::Span::current(), &tx_hash_str, nonce, final_gas);

        let receipt_span = info_span!("arb.await_receipt", tx_hash = tx_hash_str.as_str());
        let Some(receipt) = poller
            .wait_with_hypersync(sim_provider, tx_hash, hypersync, shutdown)
            .instrument(receipt_span)
            .await
        else {
            if shutdown.is_some_and(|rx| *rx.borrow()) {
                nonce_mgr.release(nonce);
                tracing::Span::current().record("outcome", "skipped_shutdown");
                return ExecutionOutcome::SkippedShutdown;
            }

            match recover_after_receipt_timeout(
                &submit_provider, &nonce_mgr, operator, tx_hash, nonce, &fees, final_gas,
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
                        receipt,
                        candidate,
                        final_gas,
                        gas_used,
                        gas_price,
                        config.execution.max_global_consecutive_failures,
                    );
                }
                NonceRecoveryOutcome::Cancelled(cancel_hash) => {
                    warn!(
                        route = fp,
                        tx_hash = tx_hash_str,
                        cancel_hash = %cancel_hash,
                        "sent cancel after receipt timeout"
                    );
                }
                NonceRecoveryOutcome::Dropped => {}
                NonceRecoveryOutcome::StillPending => {}
            }

            self.quarantine_route(fp, now);
            warn!(
                route = fp,
                tx_hash = tx_hash_str,
                "no receipt within timeout"
            );
            tracing::Span::current().record("outcome", "receipt_timeout");
            return ExecutionOutcome::ReceiptTimeout {
                tx_hash: tx_hash_str,
            };
        };

        self.finalize_receipt(
            fp,
            now,
            &nonce_mgr,
            nonce,
            &tx_hash_str,
            receipt,
            candidate,
            final_gas,
            gas_used,
            gas_price,
            config.execution.max_global_consecutive_failures,
        )
    }

    fn finalize_receipt(
        &self,
        fp: u64,
        now: Instant,
        nonce_mgr: &NonceManager,
        nonce: u64,
        tx_hash_str: &str,
        receipt: crate::services::execution::receipt::ReceiptData,
        candidate: &CandidateExecution,
        final_gas: u64,
        dry_run_gas: u64,
        gas_price: U256,
        max_global_failures: u32,
    ) -> ExecutionOutcome {
        nonce_mgr.confirm(nonce);
        self.last_submit.write().insert(fp, now);

        let drift = gas_drift_bps(dry_run_gas.max(1), receipt.gas_used);
        if receipt.gas_used > final_gas {
            warn!(
                route = fp,
                tx_hash = tx_hash_str,
                gas_used = receipt.gas_used,
                gas_limit = final_gas,
                "on-chain gas exceeded submitted limit"
            );
        } else if drift > 12_000 {
            warn!(
                route = fp,
                tx_hash = tx_hash_str,
                dry_run_gas,
                gas_used = receipt.gas_used,
                drift_bps = drift,
                "material gas drift vs dry-run estimate"
            );
        }

        record_receipt(&tracing::Span::current(), receipt.success, receipt.gas_used);

        if !receipt.success {
            self.quarantine_route(fp, now);
            self.circuit_breaker.record_failure(max_global_failures, "reverted");
            if let Some(a) = Self::reassess_assessment(candidate, receipt.gas_used, gas_price) {
                log_opportunity_outcome(
                    fp,
                    &a,
                    candidate.slippage_bps,
                    Some(receipt.gas_used),
                    "reverted",
                );
            }
            warn!(
                route = fp,
                tx_hash = tx_hash_str,
                gas_used = receipt.gas_used,
                "transaction reverted on-chain"
            );
            tracing::Span::current().record("outcome", "reverted");
            return ExecutionOutcome::Reverted {
                tx_hash: tx_hash_str.to_string(),
                gas_used: receipt.gas_used,
            };
        }

        self.clear_fail_count(fp);
        self.circuit_breaker.record_success();

        let profit_wei = parse_transfer_profit(
            &receipt.logs,
            candidate.target_address,
            Some(candidate.profit_token),
        );

        if let Some(a) = Self::reassess_assessment(candidate, receipt.gas_used, gas_price) {
            log_opportunity_outcome(
                fp,
                &a,
                candidate.slippage_bps,
                Some(receipt.gas_used),
                "confirmed",
            );
        }

        info!(
            route = fp,
            tx_hash = tx_hash_str,
            gas_used = receipt.gas_used,
            profit_wei = %profit_wei,
            expected_matic = %candidate.expected_profit_matic_wei,
            "transaction confirmed"
        );

        tracing::Span::current().record("outcome", "confirmed");
        ExecutionOutcome::Confirmed {
            tx_hash: tx_hash_str.to_string(),
            gas_used: receipt.gas_used,
            profit_wei,
        }
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
