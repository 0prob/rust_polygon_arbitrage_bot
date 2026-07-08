use alloy::network::Ethereum;
use alloy::primitives::U256;
use alloy::providers::Provider;
use rustc_hash::FxHashMap;

use crate::core::types::FoundCycle;
use crate::orchestrator::hf::HfContext;
use crate::orchestrator::hf_eval::HfEvalResult;
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::{self, simulate_route_detailed};
use crate::pipeline::spot_price::spot_probe_for_token;
use crate::pipeline::tick_fetch::{
    collect_v3_pool_addresses, collect_v4_tick_targets, enrich_v3_ticks, enrich_v4_ticks,
};
use crate::services::execution::flash_liquidity::collect_flash_tokens_for_cycle;

use crate::services::execution::impact_slippage::depth_impact_slippage_bps;
use crate::services::execution::{
    CandidateBuildConfig, PrepareDispatchInput, build_execution_candidate, prepare_evaluated_route,
};
use crate::services::oracle::resolve_token_to_matic_rate_or_bootstrap;

pub async fn dispatch_profitable_candidates(
    ctx: &HfContext,
    arena: &StateArena,
    profitable: Vec<HfEvalResult>,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    token_to_matic_rates: &rustc_hash::FxHashMap<crate::core::types::TokenIndex, U256>,
    token_decimals: &rustc_hash::FxHashMap<alloy::primitives::Address, u8>,
    state_generation: u64,
) {
    if profitable.is_empty() || *ctx.shutdown.borrow() {
        return;
    }

    let Some(executor) = ctx.config.execution.executor_address else {
        crate::warn!("dispatch skipped: EXECUTOR_ADDRESS not configured");
        return;
    };

    // ponytail: skip executor bytecode check in dry-run mode — no on-chain txs to sign
    let sim_provider = if ctx.config.is_dry_run() {
        match ctx.rpc.connect_simulation() {
            Ok(p) => p,
            Err(e) => {
                crate::warn!("dispatch skipped: simulation RPC unavailable: {e:#}");
                return;
            }
        }
    } else {
        match ctx.rpc.connect_simulation_checked(executor).await {
            Ok(p) => p,
            Err(e) => {
                crate::warn!("dispatch skipped: simulation RPC/executor check failed: {e:#}");
                return;
            }
        }
    };
    let pool_metas_by_pool: FxHashMap<
        crate::core::types::PoolIndex,
        &crate::pipeline::types::PoolMeta,
    > = pool_metas
        .iter()
        .map(|meta| (meta.pool_index, meta))
        .collect();

    let operator = ctx.wallet.operator_address(executor);
    let min_profit_matic = ctx.config.min_profit_matic;

    dispatch_with_provider(
        ctx,
        arena,
        profitable,
        &sim_provider,
        operator,
        executor,
        min_profit_matic,
        pool_metas,
        &pool_metas_by_pool,
        token_to_matic_rates,
        token_decimals,
        state_generation,
    )
    .await;

    ctx.execution.shutdown_resync(&sim_provider, operator).await;
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_with_provider<P: Provider<Ethereum> + Clone + Send + 'static>(
    ctx: &HfContext,
    arena: &StateArena,
    profitable: Vec<HfEvalResult>,
    sim_provider: &P,
    operator: alloy::primitives::Address,
    executor: alloy::primitives::Address,
    min_profit_matic: U256,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    pool_metas_by_pool: &FxHashMap<
        crate::core::types::PoolIndex,
        &crate::pipeline::types::PoolMeta,
    >,
    token_to_matic_rates: &rustc_hash::FxHashMap<crate::core::types::TokenIndex, U256>,
    token_decimals: &rustc_hash::FxHashMap<alloy::primitives::Address, u8>,
    state_generation: u64,
) {
    if ctx.execution.global_is_quarantined() {
        crate::warn!("dispatch skipped: execution circuit breaker active");
        return;
    }
    let flash_policy = ctx.config.flash_policy;
    let Some(gas_price) = ctx.gas_oracle.conservative_gas_price() else {
        crate::warn!("dispatch skipped: gas oracle has no fee snapshot yet");
        return;
    };
    let brent_iters = ctx.config.routing.ternary_search_iterations;
    let base_slippage_bps = ctx.config.execution.slippage_bps;
    let min_profit_roi_bps = ctx.config.execution.min_profit_roi_bps;
    let max_flash_loan_usd = ctx.config.execution.max_flash_loan_usd;
    let deadline_secs = ctx.config.execution.deadline_secs;

    let flash_tokens = collect_flash_tokens_for_evals(arena, &profitable);
    let flash_stale = flash_tokens
        .iter()
        .any(|token| !ctx.execution.flash_liquidity.has_fresh_entry(*token));
    if flash_stale
        && !flash_tokens.is_empty()
        && let Err(_e) = ctx
            .execution
            .flash_liquidity
            .refresh(sim_provider, &flash_tokens)
            .await
    {
        crate::warn!("flash liquidity refresh failed: {_e}");
    }

    let dispatch_pools = collect_route_pool_addresses(arena, &profitable);
    let dispatch_cycles: Vec<&FoundCycle> = profitable.iter().map(|r| &r.cycle).collect();
    let mut dispatch_arena = arena.clone();
    let mut dispatch_state_generation = state_generation;
    if !dispatch_pools.is_empty() {
        if let Err(e) = ctx
            .refresh
            .refresh_pool_states_for(&dispatch_pools, dispatch_pools.len())
            .await
        {
            crate::debug!("dispatch pool refresh failed: {e:#}");
        } else {
            dispatch_state_generation =
                dispatch_arena.apply_hot_cache(&ctx.cache, &dispatch_pools);
            enrich_dispatch_cl_ticks(
                sim_provider,
                &mut dispatch_arena,
                &dispatch_cycles,
                pool_metas,
                ctx.config.oracle.tick_word_range,
            )
            .await;
        }
    }

    let mut skipped: FxHashMap<&'static str, u32> = FxHashMap::default();

    for mut evaluated in profitable {
        if *ctx.shutdown.borrow() {
            break;
        }

        let fp = evaluated.route_fingerprint;
        if ctx.execution.is_route_quarantined(fp) {
            *skipped.entry("quarantine").or_default() += 1;
            continue;
        }

        let Some(start_token_addr) = dispatch_arena.token_address(evaluated.cycle.start_token)
        else {
            *skipped.entry("prepare").or_default() += 1;
            continue;
        };
        let resolved_token_decimals = token_decimals.get(&start_token_addr).copied().unwrap_or(18);
        let Some(token_to_matic_rate) = resolve_token_to_matic_rate_or_bootstrap(
            evaluated.cycle.start_token,
            &dispatch_arena,
            token_to_matic_rates,
        ) else {
            *skipped.entry("prepare").or_default() += 1;
            continue;
        };

        let amount_in = evaluated.sim.amount_in;
        let Some(refreshed) =
            simulate_route_detailed(&dispatch_arena, &evaluated.cycle.edges, amount_in)
        else {
            *skipped.entry("fidelity").or_default() += 1;
            crate::debug!("dispatch skip: fp={fp} resim failed after pool refresh");
            continue;
        };
        if let Some(reason) = local_sim::route_resim_fidelity_reject(&evaluated.sim, &refreshed) {
            *skipped.entry("fidelity").or_default() += 1;
            crate::debug!(
                "dispatch skip: fp={fp} resim fidelity gate failed: {reason} baseline_profit={} refreshed_profit={}",
                evaluated.sim.profit,
                refreshed.profit,
            );
            continue;
        }
        if let Some(reject) = local_sim::route_hop_fidelity_reject(
            &dispatch_arena,
            &evaluated.cycle.edges,
            &refreshed.hop_amounts,
            spot_probe_for_token(&dispatch_arena, evaluated.cycle.start_token),
        ) {
            *skipped.entry("fidelity").or_default() += 1;
            let hop = match reject {
                local_sim::HopFidelityReject::MissingPool(i)
                | local_sim::HopFidelityReject::PoolLocked(i)
                | local_sim::HopFidelityReject::ShallowCl(i)
                | local_sim::HopFidelityReject::V2ReserveExhausted(i) => i,
            };
            let hop_amount = refreshed
                .hop_amounts
                .get(hop)
                .copied()
                .unwrap_or(U256::ZERO);
            crate::debug!(
                "dispatch skip: fp={fp} hop fidelity gate failed: {reject:?} hop={hop} amount={hop_amount}"
            );
            continue;
        }
        evaluated.sim = refreshed;

        let liquidity = ctx.execution.flash_liquidity.snapshot(start_token_addr);
        let slippage_bps = if evaluated.effective_slippage_bps > 0 {
            evaluated.effective_slippage_bps
        } else {
            let depth_bps = depth_impact_slippage_bps(
                &dispatch_arena,
                &evaluated.cycle.edges,
                evaluated.sim.amount_in,
            );
            crate::services::execution::impact_slippage::effective_slippage_bps(
                base_slippage_bps,
                depth_bps,
            )
        };
        let evaluated = crate::services::execution::candidate::evaluated_from_sim(
            evaluated.cycle,
            evaluated.sim,
            evaluated.assessment,
            slippage_bps,
        );
        let Some(prepared) = prepare_evaluated_route(&PrepareDispatchInput {
            evaluated: &evaluated,
            arena: &dispatch_arena,
            liquidity,
            policy: flash_policy,
            token_to_matic_rates,
            token_decimals,
            brent_iters,
            min_profit_matic,
            min_profit_roi_bps,
            gas_price,
            slippage_bps,
            max_flash_loan_usd,
            safety_multiplier_bps: ctx.config.execution.profit_safety_multiplier_bps,
            gas_scale_bps: ctx.gas_oracle.sim_scale_bps(),
        }) else {
            *skipped.entry("prepare").or_default() += 1;
            continue;
        };

        let build_cfg = CandidateBuildConfig {
            executor_address: executor,
            slippage_bps,
            flash_loan_source: prepared.flash_source,
            deadline_secs_from_now: deadline_secs,
            min_profit_matic_wei: min_profit_matic,
            min_profit_roi_bps,
            token_decimals: resolved_token_decimals,
            token_to_matic_rate,
            safety_multiplier_bps: ctx.config.execution.profit_safety_multiplier_bps,
            state_generation: dispatch_state_generation,
            route_fingerprint: fp,
        };

        let candidate = match build_execution_candidate(
            &dispatch_arena,
            &prepared.evaluated,
            &build_cfg,
            pool_metas_by_pool,
        ) {
            Ok(c) => c,
            Err(e) => {
                crate::debug!("prepare skip: build failed fp={fp}: {e:#}");
                *skipped.entry("build").or_default() += 1;
                continue;
            }
        };

        if ctx
            .execution
            .should_log_dispatch(fp, candidate.expected_profit_matic_wei)
        {
            crate::info!(
                "dispatch candidate: fp={}, hops={}, sim_gas={}, profit_matic={}",
                fp,
                candidate.hop_count,
                candidate.simulated_gas,
                candidate.expected_profit_matic_wei
            );
        }

        let _ = ctx
            .execution
            .process_candidate(
                sim_provider,
                ctx.rpc.as_ref(),
                ctx.wallet.as_ref(),
                &ctx.config,
                &candidate,
                operator,
                &ctx.gas_oracle,
                &ctx.cache,
                ctx.hypersync.as_deref(),
                Some(&ctx.ui_hook),
                Some(&ctx.shutdown),
                None,
            )
            .await;
    }

    if skipped.values().any(|&v| v > 0) {
        crate::info!(
            "dispatch summary: quarantine={}, fidelity={}, prepare={}, build={}",
            skipped.get("quarantine").copied().unwrap_or(0),
            skipped.get("fidelity").copied().unwrap_or(0),
            skipped.get("prepare").copied().unwrap_or(0),
            skipped.get("build").copied().unwrap_or(0),
        );
    }
}

async fn enrich_dispatch_cl_ticks<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: &P,
    arena: &mut StateArena,
    cycles: &[&FoundCycle],
    pool_metas: &[crate::pipeline::types::PoolMeta],
    word_range: i16,
) {
    if cycles.is_empty() {
        return;
    }
    let tick_pools = collect_v3_pool_addresses(arena, cycles);
    let v4_targets = collect_v4_tick_targets(cycles, pool_metas);
    if tick_pools.is_empty() && v4_targets.is_empty() {
        return;
    }
    let (algebra_pools, algebra_integral_pools) =
        crate::pipeline::tick_fetch::collect_algebra_pools(arena, pool_metas);
    let v3_loaded = enrich_v3_ticks(
        provider,
        arena,
        &tick_pools,
        word_range,
        &algebra_pools,
        &algebra_integral_pools,
        None,
    )
    .await;
    let v4_loaded = enrich_v4_ticks(provider, arena, &v4_targets, word_range, None).await;
    crate::debug!(
        "dispatch tick enrich: v3_pools={} v3_loaded={v3_loaded} v4_targets={} v4_loaded={v4_loaded}",
        tick_pools.len(),
        v4_targets.len(),
    );
}

fn collect_route_pool_addresses(
    arena: &StateArena,
    routes: &[HfEvalResult],
) -> Vec<alloy::primitives::Address> {
    let mut seen = rustc_hash::FxHashSet::default();
    let mut out = Vec::new();
    for route in routes {
        for edge in &route.cycle.edges {
            if let Some(addr) = arena.pool_address(edge.pool_index)
                && seen.insert(addr)
            {
                out.push(addr);
            }
        }
    }
    out
}

fn collect_flash_tokens_for_evals(
    arena: &StateArena,
    routes: &[HfEvalResult],
) -> Vec<alloy::primitives::Address> {
    let mut seen = rustc_hash::FxHashSet::default();
    let mut out = Vec::new();
    for route in routes {
        collect_flash_tokens_for_cycle(arena, &route.cycle, &mut seen, &mut out);
    }
    out
}
