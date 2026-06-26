use std::collections::HashMap;
use std::time::{Duration, Instant};

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::sol_types::SolCall;
use parking_lot::RwLock;
use ruint::aliases::U256 as RU256;
use rustc_hash::FxHashMap;
use tracing::debug;

use crate::abis::{IAaveV3Pool, IERC20Metadata};
use crate::core::constants::{AAVE_V3_POOL, BALANCER_VAULT};
use crate::core::types::{
    EvaluatedRoute, FlashLoanSource, FoundCycle, PoolState, ProfitAssessment, ProtocolType,
    TokenIndex,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::simulate_route_detailed;
use crate::pipeline::multicall::{MulticallItem, encode_call, execute_multicall};
use crate::pipeline::sim_sanity::{SimSanityInput, check_sim_sanity};
use crate::pipeline::ternary::optimize_cycle;
use crate::services::execution::flash_policy::FlashLoanPolicy;
use crate::services::execution::profit::{
    ProfitEvalContext, ProfitThresholds, RouteProfitParams, assess_route_profit,
};
use crate::services::oracle::{resolve_token_decimals_for_index, resolve_token_to_matic_rate};

const CACHE_TTL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlashPlanAction {
    /// Use `amount_in` unchanged with the chosen provider.
    Direct,
    /// Re-optimize and simulate with an upper bound of `cap`.
    CapAndReoptimize,
    /// No provider can fund this route.
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlashPlan {
    pub source: FlashLoanSource,
    pub action: FlashPlanAction,
    pub cap: RU256,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TokenFlashLiquidity {
    pub balancer: RU256,
    pub aave: RU256,
    pub aave_listed: bool,
}

#[derive(Debug, Clone)]
struct CachedLiquidity {
    snapshot: TokenFlashLiquidity,
    fetched_at: Instant,
}

#[derive(Debug)]
pub struct FlashLiquidityCache {
    entries: RwLock<HashMap<Address, CachedLiquidity>>,
    ttl: Duration,
    balancer_vault: Address,
    aave_pool: Address,
}

impl Default for FlashLiquidityCache {
    fn default() -> Self {
        Self::new()
    }
}

impl FlashLiquidityCache {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            ttl: CACHE_TTL,
            balancer_vault: BALANCER_VAULT,
            aave_pool: AAVE_V3_POOL,
        }
    }

    pub fn with_addresses(balancer_vault: Address, aave_pool: Address) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            ttl: CACHE_TTL,
            balancer_vault,
            aave_pool,
        }
    }

    pub fn snapshot(&self, token: Address) -> TokenFlashLiquidity {
        let guard = self.entries.read();
        guard
            .get(&token)
            .filter(|e| e.fetched_at.elapsed() < self.ttl)
            .map(|e| e.snapshot)
            .unwrap_or_default()
    }

    pub async fn refresh<P: Provider<Ethereum>>(
        &self,
        provider: &P,
        tokens: &[Address],
    ) -> anyhow::Result<()> {
        let mut to_fetch = Vec::new();
        let now = Instant::now();
        {
            let guard = self.entries.read();
            for token in tokens {
                let stale = guard
                    .get(token)
                    .is_none_or(|e| now.duration_since(e.fetched_at) >= self.ttl);
                if stale {
                    to_fetch.push(*token);
                }
            }
        }
        if to_fetch.is_empty() {
            return Ok(());
        }

        let mut items = Vec::with_capacity(to_fetch.len() * 3);
        for token in &to_fetch {
            items.push(MulticallItem {
                target: *token,
                data: encode_call(&IERC20Metadata::balanceOfCall {
                    account: self.balancer_vault,
                }),
            });
            items.push(MulticallItem {
                target: self.aave_pool,
                data: encode_call(&IAaveV3Pool::getReserveDataCall { asset: *token }),
            });
            items.push(MulticallItem {
                target: *token,
                data: encode_call(&IERC20Metadata::balanceOfCall {
                    account: self.aave_pool,
                }),
            });
        }

        let results = execute_multicall(provider, &items).await?;
        let mut guard = self.entries.write();
        for (i, token) in to_fetch.iter().enumerate() {
            let base = i * 3;
            let balancer = decode_balance(results.get(base));
            let reserve = results.get(base + 1).and_then(|b| b.as_ref());
            let aave_listed = reserve
                .and_then(|b| IAaveV3Pool::getReserveDataCall::abi_decode_returns(b).ok())
                .is_some_and(|d| d.id > 0);
            let mut aave = decode_balance(results.get(base + 2));
            if !aave_listed {
                aave = RU256::ZERO;
            }
            guard.insert(
                *token,
                CachedLiquidity {
                    snapshot: TokenFlashLiquidity {
                        balancer,
                        aave,
                        aave_listed,
                    },
                    fetched_at: now,
                },
            );
        }
        debug!(tokens = to_fetch.len(), "flash liquidity cache refreshed");
        Ok(())
    }
}

fn decode_balance(bytes: Option<&Option<alloy::primitives::Bytes>>) -> RU256 {
    bytes
        .and_then(|b| b.as_ref())
        .and_then(|b| IERC20Metadata::balanceOfCall::abi_decode_returns(b).ok())
        .map(RU256::from)
        .unwrap_or(RU256::ZERO)
}

/// True when the route swaps through the Balancer vault (not just pool flash liquidity).
pub fn route_uses_balancer_vault_swap(cycle: &FoundCycle) -> bool {
    cycle
        .edges
        .iter()
        .any(|e| e.protocol == ProtocolType::BalancerV2)
}

/// Rotate a closed cycle so the first hop borrows `new_start`.
pub fn rotate_cycle_to_start(cycle: &FoundCycle, new_start: TokenIndex) -> Option<FoundCycle> {
    let n = cycle.edges.len();
    if n == 0 {
        return None;
    }
    let k = cycle.edges.iter().position(|e| e.token_in == new_start)?;
    let mut edges = cycle.edges.clone();
    edges.rotate_left(k);
    if edges.last().is_some_and(|e| e.token_out == new_start) {
        Some(FoundCycle {
            start_token: new_start,
            edges,
            ..cycle.clone()
        })
    } else {
        None
    }
}

/// True when any hop token is listed on Aave V3 (flash borrow candidate).
pub fn cycle_has_aave_listed_token(
    cycle: &FoundCycle,
    arena: &StateArena,
    flash_liquidity: &FlashLiquidityCache,
) -> bool {
    for edge in &cycle.edges {
        let Some(addr) = arena.token_address(edge.token_in) else {
            continue;
        };
        if flash_liquidity.snapshot(addr).aave_listed {
            return true;
        }
    }
    false
}

/// Balancer vault swaps require an Aave-listed token somewhere in the cycle for flash borrow.
pub fn balancer_route_flash_feasible(
    cycle: &FoundCycle,
    arena: &StateArena,
    flash_liquidity: &FlashLiquidityCache,
) -> bool {
    if !route_uses_balancer_vault_swap(cycle) {
        return true;
    }
    cycle_has_aave_listed_token(cycle, arena, flash_liquidity)
}
/// Balancer vault swaps forbid Balancer flash loans (BAL#400). Prefer an Aave-listed token
/// already present in the cycle as the flash borrow asset.
pub fn prefer_aave_flash_start(
    cycle: &FoundCycle,
    arena: &StateArena,
    flash_liquidity: &FlashLiquidityCache,
) -> FoundCycle {
    if !route_uses_balancer_vault_swap(cycle) {
        return cycle.clone();
    }

    let mut candidates: Vec<(RU256, TokenIndex)> = Vec::new();
    for edge in &cycle.edges {
        let token = edge.token_in;
        if candidates.iter().any(|(_, t)| *t == token) {
            continue;
        }
        let Some(addr) = arena.token_address(token) else {
            continue;
        };
        let liq = flash_liquidity.snapshot(addr);
        if liq.aave_listed {
            candidates.push((liq.aave, token));
        }
    }

    let Some((_, best)) = candidates.into_iter().max_by_key(|(aave, _)| *aave) else {
        return cycle.clone();
    };

    if best == cycle.start_token {
        return cycle.clone();
    }

    let rotated = rotate_cycle_to_start(cycle, best).unwrap_or_else(|| {
        debug!(
            from = cycle.start_token.0,
            to = best.0,
            "cycle rotation failed — keeping original start token"
        );
        cycle.clone()
    });
    // #region agent log
    crate::debug_agent::log(
        "H-E",
        "flash_liquidity.rs:prefer_aave_flash_start",
        "rotated_flash_start_for_balancer_route",
        serde_json::json!({
            "route_fingerprint": crate::pipeline::types::route_fingerprint(&cycle.edges),
            "original_start": cycle.start_token.0,
            "new_start": best.0,
        }),
    );
    // #endregion
    rotated
}

pub fn plan_flash_loan(
    policy: FlashLoanPolicy,
    amount_in: RU256,
    liquidity: TokenFlashLiquidity,
    forbid_balancer_flash: bool,
) -> FlashPlan {
    if amount_in.is_zero() {
        return FlashPlan {
            source: FlashLoanSource::Balancer,
            action: FlashPlanAction::Reject,
            cap: RU256::ZERO,
        };
    }

    if forbid_balancer_flash {
        return match policy {
            FlashLoanPolicy::BalancerOnly => FlashPlan {
                source: FlashLoanSource::Balancer,
                action: FlashPlanAction::Reject,
                cap: RU256::ZERO,
            },
            FlashLoanPolicy::Auto | FlashLoanPolicy::AaveOnly => {
                // Balancer vault flash + vault.swap reverts with BAL#400 (reentrancy).
                // Use Aave flash and let dry-run validate reserve support for the token.
                FlashPlan {
                    source: FlashLoanSource::AaveV3,
                    action: FlashPlanAction::Direct,
                    cap: amount_in,
                }
            }
        };
    }

    match policy {
        FlashLoanPolicy::Auto => plan_auto(amount_in, liquidity, true),
        FlashLoanPolicy::BalancerOnly => plan_single(
            FlashLoanSource::Balancer,
            amount_in,
            liquidity.balancer,
            true,
        ),
        FlashLoanPolicy::AaveOnly => {
            if !liquidity.aave_listed {
                return FlashPlan {
                    source: FlashLoanSource::AaveV3,
                    action: FlashPlanAction::Reject,
                    cap: RU256::ZERO,
                };
            }
            plan_single(FlashLoanSource::AaveV3, amount_in, liquidity.aave, false)
        }
    }
}

fn plan_auto(
    amount_in: RU256,
    liquidity: TokenFlashLiquidity,
    allow_balancer: bool,
) -> FlashPlan {
    if allow_balancer && liquidity.balancer >= amount_in {
        return FlashPlan {
            source: FlashLoanSource::Balancer,
            action: FlashPlanAction::Direct,
            cap: amount_in,
        };
    }
    if liquidity.aave_listed && liquidity.aave >= amount_in {
        return FlashPlan {
            source: FlashLoanSource::AaveV3,
            action: FlashPlanAction::Direct,
            cap: amount_in,
        };
    }
    let cap = if allow_balancer {
        liquidity.balancer.max(liquidity.aave)
    } else {
        liquidity.aave
    };
    if cap.is_zero() {
        return FlashPlan {
            source: FlashLoanSource::Balancer,
            action: FlashPlanAction::Reject,
            cap: RU256::ZERO,
        };
    }
    let source = if allow_balancer && liquidity.balancer >= liquidity.aave {
        FlashLoanSource::Balancer
    } else {
        FlashLoanSource::AaveV3
    };
    FlashPlan {
        source,
        action: FlashPlanAction::CapAndReoptimize,
        cap,
    }
}

fn plan_single(
    source: FlashLoanSource,
    amount_in: RU256,
    available: RU256,
    defer_zero: bool,
) -> FlashPlan {
    if available >= amount_in {
        FlashPlan {
            source,
            action: FlashPlanAction::Direct,
            cap: amount_in,
        }
    } else if available.is_zero() {
        if defer_zero && matches!(source, FlashLoanSource::Balancer) {
            // Vault ERC20 balance is often zero even when pool flash loans work.
            FlashPlan {
                source,
                action: FlashPlanAction::Direct,
                cap: amount_in,
            }
        } else {
            FlashPlan {
                source,
                action: FlashPlanAction::Reject,
                cap: RU256::ZERO,
            }
        }
    } else {
        FlashPlan {
            source,
            action: FlashPlanAction::CapAndReoptimize,
            cap: available,
        }
    }
}

/// Max start-token balance in Balancer pools along the route (vault balanceOf is a poor proxy).
pub fn route_balancer_flash_capacity(arena: &StateArena, cycle: &FoundCycle) -> RU256 {
    let mut max = RU256::ZERO;
    for edge in &cycle.edges {
        if edge.protocol != ProtocolType::BalancerV2 || edge.token_in != cycle.start_token {
            continue;
        }
        let Some(PoolState::Balancer(state)) = arena.pool_state(edge.pool_index) else {
            continue;
        };
        let idx = edge.token_in_idx as usize;
        if let Some(bal) = state.balances.get(idx) {
            max = max.max(RU256::from(*bal));
        }
    }
    max
}

pub struct PrepareDispatchInput<'a> {
    pub evaluated: &'a EvaluatedRoute,
    pub arena: &'a StateArena,
    pub liquidity: TokenFlashLiquidity,
    pub policy: FlashLoanPolicy,
    pub token_to_matic_rates: &'a FxHashMap<TokenIndex, RU256>,
    pub token_decimals: &'a HashMap<Address, u8>,
    pub brent_iters: u32,
    pub min_profit_matic: RU256,
    pub min_profit_roi_bps: u64,
    pub gas_price: RU256,
    pub slippage_bps: u64,
    pub max_flash_loan_usd: u64,
    pub safety_multiplier_bps: u64,
}

pub struct PreparedDispatch {
    pub evaluated: EvaluatedRoute,
    pub flash_source: FlashLoanSource,
    pub liquidity_cap_applied: bool,
}

pub fn prepare_evaluated_route(input: &PrepareDispatchInput<'_>) -> Option<PreparedDispatch> {
    let flash_token = input
        .arena
        .token_address(input.evaluated.cycle.start_token)?;
    let amount_in = input.evaluated.result.amount_in;
    let mut liquidity = input.liquidity;
    if liquidity.balancer.is_zero() {
        let route_cap = route_balancer_flash_capacity(input.arena, &input.evaluated.cycle);
        if !route_cap.is_zero() {
            debug!(
                token = %flash_token,
                route_cap = %route_cap,
                "balancer vault balance zero — using route pool liquidity"
            );
            liquidity.balancer = route_cap;
        }
    }
    let forbid_balancer_flash = route_uses_balancer_vault_swap(&input.evaluated.cycle);
    let plan = plan_flash_loan(input.policy, amount_in, liquidity, forbid_balancer_flash);

    match plan.action {
        FlashPlanAction::Reject => {
            debug!(
                token = %flash_token,
                amount_in = %amount_in,
                balancer = %input.liquidity.balancer,
                aave = %input.liquidity.aave,
                "flash loan plan rejected — insufficient liquidity"
            );
            None
        }
        FlashPlanAction::Direct => {
            let assessment = reassess_route(
                input.evaluated,
                plan.source,
                input.min_profit_matic,
                input.min_profit_roi_bps,
                input.gas_price,
                input.slippage_bps,
                input.safety_multiplier_bps,
                input.token_to_matic_rates,
                input.token_decimals,
                input.arena,
            )?;
            if !assessment.should_execute {
                return None;
            }
            Some(PreparedDispatch {
                evaluated: EvaluatedRoute {
                    cycle: input.evaluated.cycle.clone(),
                    result: input.evaluated.result.clone(),
                    assessment: Some(assessment),
                    effective_slippage_bps: input.slippage_bps,
                },
                flash_source: plan.source,
                liquidity_cap_applied: false,
            })
        }
        FlashPlanAction::CapAndReoptimize => {
            let capped = reoptimize_capped(input, plan.source, plan.cap)?;
            if !capped
                .evaluated
                .assessment
                .as_ref()
                .is_some_and(|a| a.should_execute)
            {
                return None;
            }
            Some(capped)
        }
    }
}

fn reoptimize_capped(
    input: &PrepareDispatchInput<'_>,
    source: FlashLoanSource,
    cap: RU256,
) -> Option<PreparedDispatch> {
    let profit_ctx = ProfitEvalContext::with_safety_multiplier(
        input.evaluated.cycle.start_token,
        input.arena,
        input.token_to_matic_rates,
        input.token_decimals,
        input.gas_price,
        input.slippage_bps,
        source,
        input.safety_multiplier_bps,
    );
    let opt = optimize_cycle(
        input.arena,
        &input.evaluated.cycle,
        input.token_to_matic_rates,
        input.token_decimals,
        Some(input.max_flash_loan_usd),
        Some(input.brent_iters),
        Some(cap),
        &profit_ctx,
        None,
    )?;
    let optimal_input = opt.optimal_input.min(cap);
    let sim = simulate_route_detailed(input.arena, &input.evaluated.cycle.edges, optimal_input)?;
    if sim.profit.is_zero() {
        return None;
    }
    if !capped_sim_passes_sanity(input, &sim, cap, opt.search_low) {
        return None;
    }

    let route = RouteProfitParams {
        gross_profit: sim.profit,
        amount_in: sim.amount_in,
        gas_units: sim.total_gas,
        hop_count: input.evaluated.cycle.hop_count,
        slippage_bps: input.slippage_bps,
        flash_loan_source: source,
    };
    let thresholds = ProfitThresholds {
        min_profit_matic_wei: input.min_profit_matic,
        min_profit_roi_bps: input.min_profit_roi_bps,
        safety_multiplier_bps: input.safety_multiplier_bps,
    };
    let assessment = assess_route_profit(
        input.evaluated.cycle.start_token,
        input.arena,
        &route,
        input.token_to_matic_rates,
        input.token_decimals,
        input.gas_price,
        &thresholds,
    );

    debug!(
        route_fingerprint = crate::pipeline::types::route_fingerprint(&input.evaluated.cycle.edges),
        original_amount_in = %input.evaluated.result.amount_in,
        capped_amount_in = %sim.amount_in,
        cap = %cap,
        source = ?source,
        net_profit_matic = %assessment.net_profit_after_gas_matic_wei,
        "flash loan capped and re-optimized"
    );

    Some(PreparedDispatch {
        evaluated: EvaluatedRoute {
            cycle: input.evaluated.cycle.clone(),
            result: sim,
            assessment: Some(assessment),
            effective_slippage_bps: input.slippage_bps,
        },
        flash_source: source,
        liquidity_cap_applied: true,
    })
}

fn capped_sim_passes_sanity(
    input: &PrepareDispatchInput<'_>,
    sim: &crate::core::types::RouteSimulationResult,
    cap: RU256,
    search_low: RU256,
) -> bool {
    let token_to_matic_rate = resolve_token_to_matic_rate(
        input.evaluated.cycle.start_token,
        input.arena,
        input.token_to_matic_rates,
    );
    if token_to_matic_rate < crate::core::constants::MIN_TOKEN_TO_MATIC_RATE {
        return false;
    }
    let token_decimals = resolve_token_decimals_for_index(
        input.evaluated.cycle.start_token,
        input.arena,
        input.token_decimals,
    );
    match check_sim_sanity(SimSanityInput {
        amount_in: sim.amount_in,
        gross_profit: sim.profit,
        search_low,
        token_decimals,
        token_to_matic_rate,
    }) {
        Ok(()) => true,
        Err(reason) => {
            debug!(
                route_fingerprint =
                    crate::pipeline::types::route_fingerprint(&input.evaluated.cycle.edges),
                amount_in = %sim.amount_in,
                gross_profit = %sim.profit,
                cap = %cap,
                ?reason,
                "capped re-optimize rejected — sanity check"
            );
            false
        }
    }
}

fn reassess_route(
    evaluated: &EvaluatedRoute,
    source: FlashLoanSource,
    min_profit_matic: RU256,
    min_profit_roi_bps: u64,
    gas_price: RU256,
    slippage_bps: u64,
    safety_multiplier_bps: u64,
    token_to_matic_rates: &FxHashMap<TokenIndex, RU256>,
    token_decimals: &HashMap<Address, u8>,
    arena: &StateArena,
) -> Option<ProfitAssessment> {
    let route = RouteProfitParams {
        gross_profit: evaluated.result.profit,
        amount_in: evaluated.result.amount_in,
        gas_units: evaluated.result.total_gas,
        hop_count: evaluated.cycle.hop_count,
        slippage_bps,
        flash_loan_source: source,
    };
    let thresholds = ProfitThresholds {
        min_profit_matic_wei: min_profit_matic,
        min_profit_roi_bps,
        safety_multiplier_bps,
    };
    Some(assess_route_profit(
        evaluated.cycle.start_token,
        arena,
        &route,
        token_to_matic_rates,
        token_decimals,
        gas_price,
        &thresholds,
    ))
}

fn push_flash_token(
    arena: &StateArena,
    token: TokenIndex,
    seen: &mut rustc_hash::FxHashSet<Address>,
    out: &mut Vec<Address>,
) {
    if let Some(addr) = arena.token_address(token)
        && seen.insert(addr)
    {
        out.push(addr);
    }
}

/// Tokens whose flash liquidity must be cached before eval/dispatch.
pub fn collect_flash_tokens_for_cycle(
    arena: &StateArena,
    cycle: &FoundCycle,
    seen: &mut rustc_hash::FxHashSet<Address>,
    out: &mut Vec<Address>,
) {
    if route_uses_balancer_vault_swap(cycle) {
        for edge in &cycle.edges {
            push_flash_token(arena, edge.token_in, seen, out);
        }
    } else {
        push_flash_token(arena, cycle.start_token, seen, out);
    }
}

pub fn collect_flash_tokens(arena: &StateArena, routes: &[EvaluatedRoute]) -> Vec<Address> {
    let mut seen = rustc_hash::FxHashSet::default();
    let mut out = Vec::new();
    for route in routes {
        collect_flash_tokens_for_cycle(arena, &route.cycle, &mut seen, &mut out);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn liq(balancer: u128, aave: u128, aave_listed: bool) -> TokenFlashLiquidity {
        TokenFlashLiquidity {
            balancer: RU256::from(balancer),
            aave: RU256::from(aave),
            aave_listed,
        }
    }

    #[test]
    fn auto_prefers_balancer_when_sufficient() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::Auto,
            RU256::from(1_000u64),
            liq(2_000, 5_000, true),
            false,
        );
        assert_eq!(plan.source, FlashLoanSource::Balancer);
        assert_eq!(plan.action, FlashPlanAction::Direct);
    }

    #[test]
    fn auto_falls_back_to_aave() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::Auto,
            RU256::from(4_000u64),
            liq(1_000, 5_000, true),
            false,
        );
        assert_eq!(plan.source, FlashLoanSource::AaveV3);
        assert_eq!(plan.action, FlashPlanAction::Direct);
    }

    #[test]
    fn auto_skips_balancer_flash_when_vault_swap_in_route() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::Auto,
            RU256::from(1_000u64),
            liq(2_000, 0, false),
            true,
        );
        assert_eq!(plan.source, FlashLoanSource::AaveV3);
        assert_eq!(plan.action, FlashPlanAction::Direct);
    }

    #[test]
    fn auto_caps_when_neither_sufficient() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::Auto,
            RU256::from(10_000u64),
            liq(3_000, 7_000, true),
            false,
        );
        assert_eq!(plan.source, FlashLoanSource::AaveV3);
        assert_eq!(plan.action, FlashPlanAction::CapAndReoptimize);
        assert_eq!(plan.cap, RU256::from(7_000u64));
    }

    #[test]
    fn auto_rejects_when_no_liquidity() {
        let plan = plan_flash_loan(FlashLoanPolicy::Auto, RU256::from(100u64), liq(0, 0, true), false);
        assert_eq!(plan.action, FlashPlanAction::Reject);
    }

    #[test]
    fn balancer_only_caps_partial() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::BalancerOnly,
            RU256::from(2_000u64),
            liq(500, 9_000, true),
            false,
        );
        assert_eq!(plan.source, FlashLoanSource::Balancer);
        assert_eq!(plan.action, FlashPlanAction::CapAndReoptimize);
        assert_eq!(plan.cap, RU256::from(500u64));
    }

    #[test]
    fn balancer_only_defers_when_vault_balance_zero() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::BalancerOnly,
            RU256::from(6_735_261_273_796_695_416u64),
            liq(0, 0, false),
            false,
        );
        assert_eq!(plan.source, FlashLoanSource::Balancer);
        assert_eq!(plan.action, FlashPlanAction::Direct);
    }

    #[test]
    fn rotate_cycle_reorders_edges_to_new_start() {
        use crate::core::types::Edge;
        use smallvec::smallvec;

        let t0 = TokenIndex(0);
        let t1 = TokenIndex(1);
        let t2 = TokenIndex(2);
        let mk = |tin, tout| Edge {
            pool_index: crate::core::types::PoolIndex(1),
            token_in: tin,
            token_out: tout,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let cycle = FoundCycle {
            start_token: t0,
            edges: smallvec![mk(t0, t1), mk(t1, t2), mk(t2, t0)],
            hop_count: 3,
            log_weight: -0.1,
            cumulative_fee_bps: 90,
            score: -0.1,
        };
        let rotated = rotate_cycle_to_start(&cycle, t1).expect("rotation");
        assert_eq!(rotated.start_token, t1);
        assert_eq!(rotated.edges[0].token_in, t1);
        assert_eq!(rotated.edges[2].token_out, t1);
    }
}
