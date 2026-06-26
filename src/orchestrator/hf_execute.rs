use alloy::network::Ethereum;
use alloy::providers::Provider;
use ruint::aliases::U256 as RU256;
use tracing::{Instrument, debug, info, info_span, instrument, warn};

use crate::core::types::EvaluatedRoute;
use crate::infra::tracing_util::{pool_addrs_csv, record_evaluated_route};
use crate::orchestrator::hf::HfContext;
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::route_fingerprint;
use crate::services::execution::flash_policy::parse_flash_policy;
use crate::services::execution::impact_slippage::depth_impact_slippage_bps;
use crate::services::execution::{
    CandidateBuildConfig, ExecutionOutcome, PrepareDispatchInput, build_execution_candidate,
    collect_flash_tokens, prepare_evaluated_route,
};
use crate::services::execution::dryrun::estimate_candidate_gas;
use crate::services::oracle::resolve_token_to_matic_rate_or_bootstrap;

#[instrument(skip(ctx, arena, profitable, pool_metas), fields(dispatch_count = profitable.len()))]
pub async fn dispatch_profitable_candidates(
    ctx: &HfContext,
    arena: &StateArena,
    profitable: Vec<EvaluatedRoute>,
    pool_metas: &[crate::pipeline::types::PoolMeta],
) {
    if profitable.is_empty() {
        return;
    }

    if *ctx.shutdown.borrow() {
        info!("shutdown signalled — skipping execution dispatch");
        return;
    }

    let Some(executor) = ctx.config.execution.executor_address else {
        warn!("EXECUTOR_ADDRESS not set — skipping execution dispatch");
        return;
    };

    let sim_provider = match ctx.rpc.connect_simulation() {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "no execution RPC configured");
            return;
        }
    };

    let operator = ctx.wallet.operator_address(executor);
    let min_profit_matic = ctx
        .config
        .execution
        .min_profit_matic_wei
        .parse::<RU256>()
        .unwrap_or(RU256::ZERO);

    dispatch_with_provider(
        ctx,
        arena,
        profitable,
        &sim_provider,
        operator,
        executor,
        min_profit_matic,
        pool_metas,
    )
    .await;

    ctx.execution.shutdown_resync(&sim_provider, operator).await;
}

#[instrument(skip(ctx, arena, profitable, sim_provider, pool_metas))]
async fn dispatch_with_provider<P: Provider<Ethereum>>(
    ctx: &HfContext,
    arena: &StateArena,
    profitable: Vec<EvaluatedRoute>,
    sim_provider: &P,
    operator: alloy::primitives::Address,
    executor: alloy::primitives::Address,
    min_profit_matic: RU256,
    pool_metas: &[crate::pipeline::types::PoolMeta],
) {
    let fps: Vec<u64> = profitable
        .iter()
        .map(|r| route_fingerprint(&r.cycle.edges))
        .collect();
    if fps.iter().all(|fp| ctx.execution.any_quarantined(&[*fp])) {
        info!("all profitable routes quarantined — skipping dispatch");
        // #region agent log
        crate::debug_agent::log(
            "H-B",
            "hf_execute.rs:dispatch_with_provider",
            "dispatch_skipped_all_quarantined",
            serde_json::json!({ "route_fingerprints": fps }),
        );
        // #endregion
        return;
    }

    // #region agent log
    crate::debug_agent::log(
        "H-B",
        "hf_execute.rs:dispatch_with_provider",
        "dispatch_enter",
        serde_json::json!({
            "route_count": profitable.len(),
            "dry_run": ctx.config.is_dry_run(),
            "route_fingerprints": fps,
        }),
    );
    // #endregion

    let snap = ctx.snapshots.read();
    let flash_policy = parse_flash_policy(&ctx.config.execution.flash_loan_source);
    let gas_price = ctx.gas_oracle.conservative_gas_price();
    let brent_iters = ctx.config.routing.ternary_search_iterations;
    let base_slippage_bps = ctx.config.execution.slippage_bps;
    let min_profit_roi_bps = ctx.config.execution.min_profit_roi_bps;
    let max_flash_loan_usd = ctx.config.execution.max_flash_loan_usd;
    let deadline_secs = ctx.config.execution.deadline_secs;

    let flash_tokens = collect_flash_tokens(arena, &profitable);
    if !flash_tokens.is_empty()
        && let Err(e) = ctx
            .execution
            .flash_liquidity
            .refresh(sim_provider, &flash_tokens)
            .await
    {
        warn!(error = %e, "flash liquidity refresh failed — dispatch may skip routes");
    }

    let top_n_gas = ctx.config.pipeline.hf_gas_estimate_top_n;
    let mut prepared_count = 0u32;
    let mut skipped_prepare = 0u32;
    let mut skipped_quarantined = 0u32;

    for (route_index, evaluated) in profitable.into_iter().enumerate() {
        if *ctx.shutdown.borrow() {
            info!("shutdown signalled — stopping dispatch loop");
            break;
        }

        let route_fp = route_fingerprint(&evaluated.cycle.edges);
        let exec_span = info_span!(
            "arb.execute_route",
            route_fingerprint = route_fp,
            hop_count = evaluated.cycle.hop_count,
            pool_addrs = pool_addrs_csv(arena, &evaluated.cycle.edges),
            amount_in = tracing::field::Empty,
            gross_profit = tracing::field::Empty,
            net_profit_matic_wei = tracing::field::Empty,
        );
        record_evaluated_route(&exec_span, arena, &evaluated);

        if ctx.execution.is_route_quarantined(route_fp) {
            skipped_quarantined += 1;
            debug!(route_fingerprint = route_fp, "route skipped — quarantined");
            continue;
        }

        let Some(start_token_addr) = arena.token_address(evaluated.cycle.start_token) else {
            warn!(route_fingerprint = route_fp, "missing start token address");
            continue;
        };
        let token_decimals = snap
            .token_decimals
            .get(&start_token_addr)
            .copied()
            .unwrap_or(18);
        let Some(token_to_matic_rate) = resolve_token_to_matic_rate_or_bootstrap(
            evaluated.cycle.start_token,
            arena,
            &snap.token_to_matic_rates,
        ) else {
            skipped_prepare += 1;
            debug!(
                route_fingerprint = route_fp,
                "route skipped — no reliable MATIC rate"
            );
            continue;
        };

        let liquidity = ctx.execution.flash_liquidity.snapshot(start_token_addr);
        let slippage_bps = if evaluated.effective_slippage_bps > 0 {
            evaluated.effective_slippage_bps
        } else {
            let depth_bps = depth_impact_slippage_bps(
                arena,
                &evaluated.cycle.edges,
                evaluated.result.amount_in,
            );
            crate::services::execution::impact_slippage::effective_slippage_bps(
                base_slippage_bps,
                depth_bps,
            )
        };
        let Some(prepared) = prepare_evaluated_route(&PrepareDispatchInput {
            evaluated: &evaluated,
            arena,
            liquidity,
            policy: flash_policy,
            token_to_matic_rates: &snap.token_to_matic_rates,
            token_decimals: &snap.token_decimals,
            brent_iters,
            min_profit_matic,
            min_profit_roi_bps,
            gas_price,
            slippage_bps,
            max_flash_loan_usd,
            safety_multiplier_bps: ctx.config.execution.profit_safety_multiplier_bps,
        }) else {
            skipped_prepare += 1;
            debug!(
                route_fingerprint = route_fp,
                amount_in = %evaluated.result.amount_in,
                balancer = %liquidity.balancer,
                aave = %liquidity.aave,
                flash_policy = ?flash_policy,
                "route skipped after flash liquidity resolution"
            );
            continue;
        };

        prepared_count += 1;

        if prepared.liquidity_cap_applied {
            info!(
                route_fingerprint = route_fp,
                amount_in = %prepared.evaluated.result.amount_in,
                source = ?prepared.flash_source,
                "flash loan size reduced to on-chain liquidity"
            );
        }

        let build_cfg = CandidateBuildConfig {
            executor_address: executor,
            slippage_bps,
            flash_loan_source: prepared.flash_source,
            deadline_secs_from_now: deadline_secs,
            min_profit_matic_wei: min_profit_matic,
            min_profit_roi_bps,
            token_decimals,
            token_to_matic_rate,
            safety_multiplier_bps: ctx.config.execution.profit_safety_multiplier_bps,
        };

        async {
            let mut candidate =
                match build_execution_candidate(arena, &prepared.evaluated, &build_cfg, pool_metas)
                {
                    Ok(c) => c,
                    Err(e) => {
                        warn!(route_fingerprint = route_fp, error = %e, "candidate build failed");
                        return;
                    }
                };

            if route_index < top_n_gas {
                if let Some(gas) =
                    estimate_candidate_gas(sim_provider, &candidate, operator).await
                {
                    ctx.gas_oracle
                        .record_sim_observed(candidate.simulated_gas, gas);
                    candidate.simulated_gas = gas.min(u32::MAX as u64) as u32;
                    if let Some(limit) =
                        crate::services::execution::gas::buffer_gas_limit(candidate.simulated_gas)
                    {
                        candidate.gas_limit = Some(limit);
                    }
                    debug!(
                        route_fingerprint = route_fp,
                        estimate_gas = gas,
                        simulated_gas = candidate.simulated_gas,
                        "top-N estimate_gas applied"
                    );
                }
            }

            let outcome = ctx
                .execution
                .process_candidate(
                    sim_provider,
                    ctx.rpc.as_ref(),
                    ctx.wallet.as_ref(),
                    &ctx.config,
                    &candidate,
                    operator,
                    &ctx.gas_oracle,
                    ctx.hypersync.as_deref(),
                    Some(&ctx.shutdown),
                    Some(ctx.metrics.as_ref()),
                )
                .await;
            // #region agent log
            crate::debug_agent::log(
                "H-D",
                "hf_execute.rs:dispatch_with_provider",
                "execution_outcome",
                serde_json::json!({
                    "route_fingerprint": candidate.route_fingerprint,
                    "outcome": format!("{outcome:?}"),
                }),
            );
            // #endregion
            match outcome {
                ExecutionOutcome::DryRunPassed { gas_used } => {
                    ctx.metrics.record_dry_run_passed();
                    info!(
                        route_fingerprint = candidate.route_fingerprint,
                        gas_used,
                        profit = %candidate.expected_profit_matic_wei,
                        "execution dry-run ok"
                    );
                }
                ExecutionOutcome::DryRunFailed { reason } => {
                    warn!(
                        route_fingerprint = candidate.route_fingerprint,
                        reason, "execution dry-run failed"
                    );
                }
                ExecutionOutcome::SkippedUnprofitableAfterDryRun => {
                    info!(
                        route_fingerprint = candidate.route_fingerprint,
                        "skipped — unprofitable after dry-run gas"
                    );
                }
                ExecutionOutcome::SkippedShutdown => {
                    info!(
                        route_fingerprint = candidate.route_fingerprint,
                        "execution cancelled — shutdown"
                    );
                }
                ExecutionOutcome::SkippedCircuitBreaker => {
                    warn!(
                        route_fingerprint = candidate.route_fingerprint,
                        reason = ctx
                            .execution
                            .circuit_breaker
                            .pause_reason()
                            .unwrap_or_default(),
                        "skipped — circuit breaker tripped"
                    );
                }
                ExecutionOutcome::SkippedNoPrivateRpc => {
                    warn!(
                        route_fingerprint = candidate.route_fingerprint,
                        "skipped — private submit RPC required but unavailable"
                    );
                }
                ExecutionOutcome::Confirmed {
                    tx_hash,
                    gas_used,
                    profit_wei,
                } => {
                    ctx.metrics.record_tx_confirmed();
                    info!(
                        route_fingerprint = candidate.route_fingerprint,
                        tx_hash,
                        gas_used,
                        profit = %profit_wei,
                        "transaction confirmed"
                    );
                }
                ExecutionOutcome::Reverted { tx_hash, gas_used } => {
                    ctx.metrics.record_tx_reverted();
                    warn!(
                        route_fingerprint = candidate.route_fingerprint,
                        tx_hash, gas_used, "transaction reverted"
                    );
                }
                ExecutionOutcome::ReceiptTimeout { tx_hash } => {
                    warn!(
                        route_fingerprint = candidate.route_fingerprint,
                        tx_hash, "receipt poll timed out"
                    );
                }
                ExecutionOutcome::SubmitFailed { reason } => {
                    warn!(
                        route_fingerprint = candidate.route_fingerprint,
                        reason, "transaction submit failed"
                    );
                }
                other => {
                    info!(
                        route_fingerprint = candidate.route_fingerprint,
                        ?other,
                        "execution skipped"
                    );
                }
            }
        }
        .instrument(exec_span)
        .await;
    }

    if prepared_count == 0 && skipped_prepare > 0 && skipped_quarantined == 0 {
        info!(
            skipped_prepare,
            flash_policy = ?flash_policy,
            "dispatch complete — all routes skipped before execution"
        );
    } else if skipped_quarantined > 0 && prepared_count == 0 {
        info!(
            skipped_quarantined,
            skipped_prepare,
            flash_policy = ?flash_policy,
            "dispatch complete — routes quarantined or skipped at prepare"
        );
    } else if prepared_count > 0 {
        debug!(
            prepared = prepared_count,
            skipped_prepare,
            skipped_quarantined,
            "dispatch complete"
        );
    }
}
