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
use crate::pipeline::ternary::optimize_cycle;
use crate::services::execution::profit::{
    ProfitEvalContext, ProfitThresholds, RouteProfitParams, assess_profit, build_assess_input,
};
use crate::services::execution::flash_policy::FlashLoanPolicy;

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

pub fn plan_flash_loan(
    policy: FlashLoanPolicy,
    amount_in: RU256,
    liquidity: TokenFlashLiquidity,
) -> FlashPlan {
    if amount_in.is_zero() {
        return FlashPlan {
            source: FlashLoanSource::Balancer,
            action: FlashPlanAction::Reject,
            cap: RU256::ZERO,
        };
    }

    match policy {
        FlashLoanPolicy::Auto => plan_auto(amount_in, liquidity),
        FlashLoanPolicy::BalancerOnly => plan_single(
            FlashLoanSource::Balancer,
            amount_in,
            liquidity.balancer,
        ),
        FlashLoanPolicy::AaveOnly => {
            if !liquidity.aave_listed {
                return FlashPlan {
                    source: FlashLoanSource::AaveV3,
                    action: FlashPlanAction::Reject,
                    cap: RU256::ZERO,
                };
            }
            plan_single(FlashLoanSource::AaveV3, amount_in, liquidity.aave)
        }
    }
}

fn plan_auto(amount_in: RU256, liquidity: TokenFlashLiquidity) -> FlashPlan {
    if liquidity.balancer >= amount_in {
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
    let cap = liquidity.balancer.max(liquidity.aave);
    if cap.is_zero() {
        return FlashPlan {
            source: FlashLoanSource::Balancer,
            action: FlashPlanAction::Reject,
            cap: RU256::ZERO,
        };
    }
    let source = if liquidity.balancer >= liquidity.aave {
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

fn plan_single(source: FlashLoanSource, amount_in: RU256, available: RU256) -> FlashPlan {
    if available >= amount_in {
        FlashPlan {
            source,
            action: FlashPlanAction::Direct,
            cap: amount_in,
        }
    } else if available.is_zero() {
        FlashPlan {
            source,
            action: FlashPlanAction::Reject,
            cap: RU256::ZERO,
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

pub fn prepare_evaluated_route(input: PrepareDispatchInput<'_>) -> Option<PreparedDispatch> {
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
    let plan = plan_flash_loan(input.policy, amount_in, liquidity);

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
    input: PrepareDispatchInput<'_>,
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
        Some(&profit_ctx),
    )?;
    let optimal_input = opt.optimal_input.min(cap);
    let sim = simulate_route_detailed(
        input.arena,
        &input.evaluated.cycle.edges,
        optimal_input,
    )?;
    if sim.profit.is_zero() {
        return None;
    }

    let assessment = assess_profit(build_assess_input(
        input.evaluated.cycle.start_token,
        input.arena,
        RouteProfitParams {
            gross_profit: sim.profit,
            amount_in: sim.amount_in,
            gas_units: sim.total_gas,
            hop_count: input.evaluated.cycle.hop_count,
            slippage_bps: input.slippage_bps,
            flash_loan_source: source,
        },
        input.token_to_matic_rates,
        input.token_decimals,
        input.gas_price,
        ProfitThresholds {
            min_profit_matic_wei: input.min_profit_matic,
            min_profit_roi_bps: input.min_profit_roi_bps,
            safety_multiplier_bps: input.safety_multiplier_bps,
        },
    ));

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
    Some(assess_profit(build_assess_input(
        evaluated.cycle.start_token,
        arena,
        RouteProfitParams {
            gross_profit: evaluated.result.profit,
            amount_in: evaluated.result.amount_in,
            gas_units: evaluated.result.total_gas,
            hop_count: evaluated.cycle.hop_count,
            slippage_bps,
            flash_loan_source: source,
        },
        token_to_matic_rates,
        token_decimals,
        gas_price,
        ProfitThresholds {
            min_profit_matic_wei: min_profit_matic,
            min_profit_roi_bps,
            safety_multiplier_bps,
        },
    )))
}

pub fn collect_flash_tokens(arena: &StateArena, routes: &[EvaluatedRoute]) -> Vec<Address> {
    let mut seen = rustc_hash::FxHashSet::default();
    let mut out = Vec::new();
    for route in routes {
        if let Some(addr) = arena.token_address(route.cycle.start_token)
            && seen.insert(addr)
        {
            out.push(addr);
        }
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
        );
        assert_eq!(plan.source, FlashLoanSource::AaveV3);
        assert_eq!(plan.action, FlashPlanAction::CapAndReoptimize);
        assert_eq!(plan.cap, RU256::from(7_000u64));
    }

    #[test]
    fn auto_rejects_when_no_liquidity() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::Auto,
            RU256::from(100u64),
            liq(0, 0, true),
        );
        assert_eq!(plan.action, FlashPlanAction::Reject);
    }

    #[test]
    fn balancer_only_caps_partial() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::BalancerOnly,
            RU256::from(2_000u64),
            liq(500, 9_000, true),
        );
        assert_eq!(plan.source, FlashLoanSource::Balancer);
        assert_eq!(plan.action, FlashPlanAction::CapAndReoptimize);
        assert_eq!(plan.cap, RU256::from(500u64));
    }

    #[test]
    fn balancer_only_rejects_when_vault_and_route_cap_zero() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::BalancerOnly,
            RU256::from(6_735_261_273_796_695_416u64),
            liq(0, 0, false),
        );
        assert_eq!(plan.source, FlashLoanSource::Balancer);
        assert_eq!(plan.action, FlashPlanAction::Reject);
    }
}
