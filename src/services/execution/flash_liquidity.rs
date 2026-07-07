use std::borrow::Cow;
use std::time::{Duration, Instant};

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::primitives::U256;
use alloy::providers::Provider;
use alloy::sol_types::SolCall;
use anyhow::Context;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;

use crate::abis::{IAaveV3Pool, IERC20Metadata};
use crate::core::constants::{AAVE_V3_POOL, BALANCER_VAULT};
use crate::core::types::{
    EvaluatedRoute, FlashLoanSource, FoundCycle, PoolState, ProfitAssessment, ProtocolType,
    TokenIndex,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::simulate_route_detailed;
use crate::pipeline::multicall::{MulticallItem, encode_call, execute_multicall};
use crate::pipeline::sim_sanity::{
    SimSanityInput, check_sim_sanity, max_flash_borrow_wei, min_economic_amount_in,
};
use crate::pipeline::ternary::optimize_cycle;
use crate::services::execution::flash_policy::FlashLoanPolicy;
use crate::services::execution::profit::{
    ProfitEvalContext, ProfitThresholds, RouteProfitParams, assess_route_profit,
};
use crate::services::oracle::{resolve_token_decimals_for_index, resolve_token_to_matic_rate};

const CACHE_TTL: Duration = Duration::from_secs(30);

pub async fn fetch_and_cache_aave_flash_loan_fee_bps<P: Provider<Ethereum>>(
    provider: &P,
) -> anyhow::Result<u64> {
    let pool = IAaveV3Pool::new(crate::core::constants::AAVE_V3_POOL, provider);
    let fee = pool.FLASHLOAN_PREMIUM_TOTAL().call().await?;
    let bps = u64::try_from(fee)
        .with_context(|| format!("Aave flash loan fee {fee} does not fit u64"))?;
    if bps == 0 {
        anyhow::bail!("Aave FLASHLOAN_PREMIUM_TOTAL returned zero — on-chain data unreliable");
    }
    crate::services::execution::profit::set_aave_flash_loan_fee_bps(bps);
    Ok(bps)
}

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
    pub cap: U256,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TokenFlashLiquidity {
    pub balancer: U256,
    pub aave: U256,
    pub aave_listed: bool,
    /// DODO V2 pool liquidity — max of all DODO pools for this token.
    pub dodo: U256,
}

#[derive(Debug, Clone)]
struct CachedLiquidity {
    snapshot: TokenFlashLiquidity,
    fetched_at: Instant,
}

#[derive(Debug)]
pub struct FlashLiquidityCache {
    entries: RwLock<FxHashMap<Address, CachedLiquidity>>,
    ttl: Duration,
    balancer_vault: Address,
    aave_pool: Address,
}

impl FlashLiquidityCache {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(FxHashMap::default()),
            ttl: CACHE_TTL,
            balancer_vault: BALANCER_VAULT,
            aave_pool: AAVE_V3_POOL,
        }
    }
}

impl Default for FlashLiquidityCache {
    fn default() -> Self {
        Self::new()
    }
}

impl FlashLiquidityCache {
    #[must_use]
    pub fn with_addresses(balancer_vault: Address, aave_pool: Address) -> Self {
        Self {
            entries: RwLock::new(FxHashMap::default()),
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

    #[must_use]
    pub fn has_fresh_entry(&self, token: Address) -> bool {
        let guard = self.entries.read();
        guard
            .get(&token)
            .is_some_and(|e| e.fetched_at.elapsed() < self.ttl)
    }

    pub async fn refresh<P: Provider<Ethereum> + Clone + Send + 'static>(
        &self,
        provider: &P,
        tokens: &[Address],
    ) -> anyhow::Result<()> {
        let mut to_fetch = Vec::with_capacity(tokens.len());
        let now = Instant::now();
        {
            let guard = self.entries.read();
            for token in tokens {
                let stale = guard
                    .get(token)
                    .is_none_or(|e| now.saturating_duration_since(e.fetched_at) >= self.ttl);
                if stale {
                    to_fetch.push(*token);
                }
            }
        }
        if to_fetch.is_empty() {
            return Ok(());
        }

        let mut items = Vec::with_capacity(to_fetch.len() * 2);
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
        }

        let results = execute_multicall(provider, &items).await?;
        let reserves: Vec<Option<Address>> = (0..to_fetch.len())
            .map(|i| {
                results
                    .get(i * 2 + 1)
                    .and_then(|bytes| bytes.as_ref())
                    .and_then(|bytes| {
                        IAaveV3Pool::getReserveDataCall::abi_decode_returns(bytes).ok()
                    })
                    .filter(|reserve| {
                        !reserve.aTokenAddress.is_zero()
                            && aave_reserve_flash_eligible(reserve.configuration)
                    })
                    .map(|reserve| reserve.aTokenAddress)
            })
            .collect();
        let aave_items: Vec<MulticallItem> = to_fetch
            .iter()
            .zip(&reserves)
            .filter_map(|(token, reserve)| {
                reserve.map(|a_token| MulticallItem {
                    target: *token,
                    data: encode_call(&IERC20Metadata::balanceOfCall { account: a_token }),
                })
            })
            .collect();
        let aave_results = if aave_items.is_empty() {
            Vec::new()
        } else {
            execute_multicall(provider, &aave_items).await?
        };

        let mut guard = self.entries.write();
        let mut aave_index = 0usize;
        for (i, token) in to_fetch.iter().enumerate() {
            let base = i * 2;
            let balancer = decode_balance(results.get(base));
            let aave_listed = reserves[i].is_some();
            let aave = if aave_listed {
                let balance = decode_balance(aave_results.get(aave_index));
                aave_index += 1;
                balance
            } else {
                U256::ZERO
            };
            guard.insert(
                *token,
                CachedLiquidity {
                    snapshot: TokenFlashLiquidity {
                        balancer,
                        aave,
                        aave_listed,
                        dodo: U256::MAX,
                    },
                    fetched_at: now,
                },
            );
        }
        Ok(())
    }
}

fn decode_balance(bytes: Option<&Option<alloy::primitives::Bytes>>) -> U256 {
    bytes
        .and_then(|b| b.as_ref())
        .and_then(|b| IERC20Metadata::balanceOfCall::abi_decode_returns(b).ok())
        .map_or(U256::ZERO, U256::from)
}

/// Aave V3 `ReserveConfiguration` flags — inactive/frozen/paused reserves revert with
/// `ReserveInactive()` (selector `0x90cd6f24`) on flash loan.
#[inline]
#[must_use]
fn aave_reserve_flash_eligible(configuration: U256) -> bool {
    const ACTIVE: u128 = 1;
    const FROZEN: u128 = 2;
    const PAUSED: u128 = 16;
    const FLASHLOAN_ENABLED: u128 = 128;
    (configuration & U256::from(ACTIVE)) != U256::ZERO
        && (configuration & U256::from(FROZEN)) == U256::ZERO
        && (configuration & U256::from(PAUSED)) == U256::ZERO
        && (configuration & U256::from(FLASHLOAN_ENABLED)) != U256::ZERO
}

/// True when the route swaps through the Balancer vault (not just pool flash liquidity).
fn route_uses_balancer_vault_swap(cycle: &FoundCycle) -> bool {
    cycle
        .edges
        .iter()
        .any(|e| e.protocol == ProtocolType::BalancerV2)
}

/// True when every hop is a Balancer V2 vault swap (eligible for `executeArbDirect` + `batchSwap`).
#[must_use]
pub fn route_is_balancer_only(cycle: &FoundCycle) -> bool {
    !cycle.edges.is_empty()
        && cycle
            .edges
            .iter()
            .all(|e| e.protocol == ProtocolType::BalancerV2)
}

/// Rotate a closed cycle so the first hop borrows `new_start`.
#[must_use]
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
            hop_count: cycle.hop_count,
            log_weight: cycle.log_weight,
            cumulative_fee_bps: cycle.cumulative_fee_bps,
            score: cycle.score,
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
        let liquidity = flash_liquidity.snapshot(addr);
        if liquidity.aave_listed && !liquidity.aave.is_zero() {
            return true;
        }
    }
    false
}

/// Mixed Balancer routes need an Aave-listed token for flash borrow. Pure Balancer routes
/// use `executeArbDirect` + `batchSwap` and do not require Aave liquidity.
pub fn balancer_route_flash_feasible(
    cycle: &FoundCycle,
    arena: &StateArena,
    flash_liquidity: &FlashLiquidityCache,
) -> bool {
    if !route_uses_balancer_vault_swap(cycle) {
        return true;
    }
    if route_is_balancer_only(cycle) {
        return true;
    }
    cycle_has_aave_listed_token(cycle, arena, flash_liquidity)
}
/// Mixed Balancer routes forbid Balancer flash loans (`BalancerVaultReentrancy`). Prefer an Aave-listed token
/// already present in the cycle as the flash borrow asset.
pub fn prefer_aave_flash_start<'a>(
    cycle: &'a FoundCycle,
    arena: &StateArena,
    flash_liquidity: &FlashLiquidityCache,
) -> Cow<'a, FoundCycle> {
    if !route_uses_balancer_vault_swap(cycle) {
        return Cow::Borrowed(cycle);
    }

    // ponytail: FxHashSet avoids O(n²) linear scan per edge for small cycles.
    let mut seen: rustc_hash::FxHashSet<TokenIndex> = rustc_hash::FxHashSet::default();
    let mut candidates: Vec<(U256, TokenIndex)> = Vec::new();
    for edge in &cycle.edges {
        let token = edge.token_in;
        if !seen.insert(token) {
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
        return Cow::Borrowed(cycle);
    };

    if best == cycle.start_token {
        return Cow::Borrowed(cycle);
    }

    rotate_cycle_to_start(cycle, best).map_or(Cow::Borrowed(cycle), Cow::Owned)
}

#[must_use]
pub fn plan_flash_loan(
    policy: FlashLoanPolicy,
    amount_in: U256,
    liquidity: TokenFlashLiquidity,
    forbid_balancer_flash: bool,
    balancer_only: bool,
) -> FlashPlan {
    if amount_in.is_zero() {
        return FlashPlan {
            source: FlashLoanSource::Balancer,
            action: FlashPlanAction::Reject,
            cap: U256::ZERO,
        };
    }

    if forbid_balancer_flash {
        // Balancer `flashLoan` cannot call the vault again in its callback
        // (`BalancerVaultReentrancy`). Pure Balancer routes use `executeArbDirect`
        // + vault `batchSwap` flash swaps; mixed routes borrow from Aave instead.
        if balancer_only {
            return FlashPlan {
                source: FlashLoanSource::Direct,
                action: FlashPlanAction::Direct,
                cap: amount_in,
            };
        }
        if !liquidity.aave_listed {
            return FlashPlan {
                source: FlashLoanSource::AaveV3,
                action: FlashPlanAction::Reject,
                cap: U256::ZERO,
            };
        }
        return plan_single(FlashLoanSource::AaveV3, amount_in, liquidity.aave, false);
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
                    cap: U256::ZERO,
                };
            }
            plan_single(FlashLoanSource::AaveV3, amount_in, liquidity.aave, false)
        }
    }
}

fn plan_auto(amount_in: U256, liquidity: TokenFlashLiquidity, allow_balancer: bool) -> FlashPlan {
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
    // DODO fallback — pool transfers tokens upfront, no pre-check needed.
    if liquidity.dodo >= amount_in {
        return FlashPlan {
            source: FlashLoanSource::Dodo,
            action: FlashPlanAction::Direct,
            cap: amount_in,
        };
    }
    let cap = if allow_balancer {
        liquidity.balancer.max(liquidity.aave).max(liquidity.dodo)
    } else {
        liquidity.aave.max(liquidity.dodo)
    };
    if cap.is_zero() {
        return FlashPlan {
            source: FlashLoanSource::Balancer,
            action: FlashPlanAction::Reject,
            cap: U256::ZERO,
        };
    }
    let source = if allow_balancer && liquidity.balancer >= liquidity.aave {
        FlashLoanSource::Balancer
    } else if liquidity.aave_listed && liquidity.aave >= liquidity.dodo {
        FlashLoanSource::AaveV3
    } else {
        FlashLoanSource::Dodo
    };
    FlashPlan {
        source,
        action: FlashPlanAction::CapAndReoptimize,
        cap,
    }
}

fn plan_single(
    source: FlashLoanSource,
    amount_in: U256,
    available: U256,
    _defer_zero: bool,
) -> FlashPlan {
    if available.is_zero() {
        return FlashPlan {
            source,
            action: FlashPlanAction::Reject,
            cap: U256::ZERO,
        };
    }
    if available >= amount_in {
        FlashPlan {
            source,
            action: FlashPlanAction::Direct,
            cap: amount_in,
        }
    } else {
        FlashPlan {
            source,
            action: FlashPlanAction::CapAndReoptimize,
            cap: available,
        }
    }
}

fn flash_liquidity_for_cycle(
    cycle: &FoundCycle,
    arena: &StateArena,
    flash_liquidity: &FlashLiquidityCache,
) -> Option<TokenFlashLiquidity> {
    let start_addr = arena.token_address(cycle.start_token)?;
    let snapshot = flash_liquidity.snapshot(start_addr);
    let route_cap = route_balancer_flash_capacity(arena, cycle);
    Some(TokenFlashLiquidity {
        balancer: effective_balancer_liquidity(snapshot.balancer, route_cap),
        aave: snapshot.aave,
        aave_listed: snapshot.aave_listed,
        // DODO pools transfer tokens upfront — liquidity is bounded by pool reserves.
        // Set to U256::MAX so DODO is always tried as fallback when Balancer/Aave fail.
        dodo: U256::MAX,
    })
}

fn policy_fallback_flash_source(
    policy: FlashLoanPolicy,
    forbid_balancer: bool,
    balancer_only: bool,
) -> FlashLoanSource {
    if forbid_balancer {
        if balancer_only {
            FlashLoanSource::Direct
        } else {
            FlashLoanSource::AaveV3
        }
    } else {
        match policy {
            FlashLoanPolicy::AaveOnly => FlashLoanSource::AaveV3,
            FlashLoanPolicy::Auto | FlashLoanPolicy::BalancerOnly => FlashLoanSource::Balancer,
        }
    }
}

/// Flash loan source for eval/ranking. Uses cached liquidity when present; defers
/// strict sizing checks to `prepare_evaluated_route` when the cache has not warmed yet.
#[must_use]
pub fn resolve_flash_source_for_cycle(
    cycle: &FoundCycle,
    arena: &StateArena,
    flash_liquidity: &FlashLiquidityCache,
    policy: FlashLoanPolicy,
) -> Option<FlashLoanSource> {
    let start_addr = arena.token_address(cycle.start_token)?;
    let liquidity = flash_liquidity_for_cycle(cycle, arena, flash_liquidity)?;
    let forbid = route_uses_balancer_vault_swap(cycle);
    let balancer_only = route_is_balancer_only(cycle);
    let plan = plan_flash_loan(policy, U256::from(1u64), liquidity, forbid, balancer_only);
    match plan.action {
        FlashPlanAction::Reject => {
            if flash_liquidity.has_fresh_entry(start_addr) {
                crate::debug!(
                    "flash source reject: token={start_addr} policy={policy:?} forbid_balancer={forbid} balancer_only={balancer_only} balancer={} aave={} aave_listed={} dodo={}",
                    liquidity.balancer,
                    liquidity.aave,
                    liquidity.aave_listed,
                    liquidity.dodo,
                );
                None
            } else {
                Some(policy_fallback_flash_source(policy, forbid, balancer_only))
            }
        }
        _ => Some(plan.source),
    }
}

/// Max start-token cash in Balancer pools along the route (vault ERC20 balanceOf is a poor
/// proxy for flash-loan availability and causes BAL#528 when over-estimated).
#[must_use]
pub fn route_balancer_flash_capacity(arena: &StateArena, cycle: &FoundCycle) -> U256 {
    let Some(start_addr) = arena.token_address(cycle.start_token) else {
        return U256::ZERO;
    };
    let mut max = U256::ZERO;
    for edge in &cycle.edges {
        if edge.protocol != ProtocolType::BalancerV2 {
            continue;
        }
        let Some(PoolState::Balancer(state)) = arena.pool_state(edge.pool_index) else {
            continue;
        };
        let Some(idx) = state.tokens.iter().position(|t| *t == start_addr) else {
            continue;
        };
        if let Some(bal) = state.balances.get(idx) {
            max = max.max(*bal);
        }
    }
    max
}

/// Conservative Balancer flash ceiling: prefer on-route pool cash over vault ERC20 balance.
#[must_use]
fn effective_balancer_liquidity(snapshot: U256, route_cap: U256) -> U256 {
    match (snapshot.is_zero(), route_cap.is_zero()) {
        (true, true) => U256::ZERO,
        (true, false) => route_cap,
        (false, true) => snapshot,
        (false, false) => snapshot.min(route_cap),
    }
}

pub struct PrepareDispatchInput<'a> {
    pub evaluated: &'a EvaluatedRoute,
    pub arena: &'a StateArena,
    pub liquidity: TokenFlashLiquidity,
    pub policy: FlashLoanPolicy,
    pub token_to_matic_rates: &'a FxHashMap<TokenIndex, U256>,
    pub token_decimals: &'a FxHashMap<Address, u8>,
    pub brent_iters: u32,
    pub min_profit_matic: U256,
    pub min_profit_roi_bps: u64,
    pub gas_price: U256,
    pub slippage_bps: u64,
    pub max_flash_loan_usd: u64,
    pub safety_multiplier_bps: u64,
    /// Gas-oracle scale in bps (10_000 = 1.0×) applied to simulated gas.
    pub gas_scale_bps: u64,
}

pub struct PreparedDispatch {
    pub evaluated: EvaluatedRoute,
    pub flash_source: FlashLoanSource,
    pub liquidity_cap_applied: bool,
}

#[must_use]
pub fn prepare_evaluated_route(input: &PrepareDispatchInput<'_>) -> Option<PreparedDispatch> {
    // ponytail: ensure flash token address is resolvable
    let _flash_token = input
        .arena
        .token_address(input.evaluated.cycle.start_token)?;
    let amount_in = input.evaluated.result.amount_in;
    let route_cap = route_balancer_flash_capacity(input.arena, &input.evaluated.cycle);
    let liquidity = TokenFlashLiquidity {
        balancer: effective_balancer_liquidity(input.liquidity.balancer, route_cap),
        aave: input.liquidity.aave,
        aave_listed: input.liquidity.aave_listed,
        dodo: U256::MAX,
    };
    let forbid_balancer_flash = route_uses_balancer_vault_swap(&input.evaluated.cycle);
    let balancer_only = route_is_balancer_only(&input.evaluated.cycle);
    let plan = plan_flash_loan(
        input.policy,
        amount_in,
        liquidity,
        forbid_balancer_flash,
        balancer_only,
    );

    let token_decimals = resolve_token_decimals_for_index(
        input.evaluated.cycle.start_token,
        input.arena,
        input.token_decimals,
    );
    let token_to_matic_rate = resolve_token_to_matic_rate(
        input.evaluated.cycle.start_token,
        input.arena,
        input.token_to_matic_rates,
    );
    let flash_borrow_cap = max_flash_borrow_wei(
        input.max_flash_loan_usd,
        token_decimals,
        token_to_matic_rate,
    );
    if plan.action != FlashPlanAction::Reject
        && let Some(cap) = flash_borrow_cap
        && amount_in > cap
    {
        crate::debug!(
            "prepare cap: amount_in={amount_in} exceeds flash borrow cap {cap}, reoptimizing"
        );
        return reoptimize_capped(input, plan.source, cap);
    }

    match plan.action {
        FlashPlanAction::Reject => {
            crate::debug!(
                "prepare skip: flash plan rejected (policy={:?}, forbid_balancer={forbid_balancer_flash}, amount_in={amount_in})",
                input.policy
            );
            None
        }
        FlashPlanAction::Direct => {
            if !dispatch_sim_passes_sanity(
                input,
                &input.evaluated.result,
                min_economic_amount_in(token_decimals, token_to_matic_rate),
                token_decimals,
                token_to_matic_rate,
            ) {
                crate::debug!("prepare skip: dispatch sim sanity rejected");
                return None;
            }
            if plan.source == FlashLoanSource::Balancer
                && amount_in > liquidity.balancer
                && !liquidity.balancer.is_zero()
            {
                crate::debug!(
                    "prepare cap: Balancer liquidity {} < amount_in={amount_in}, reoptimizing",
                    liquidity.balancer
                );
                return reoptimize_capped(input, plan.source, liquidity.balancer);
            }
            let assessment = reassess_route(
                input.evaluated,
                plan.source,
                input.min_profit_matic,
                input.min_profit_roi_bps,
                input.gas_price,
                input.slippage_bps,
                input.safety_multiplier_bps,
                input.gas_scale_bps,
                input.token_to_matic_rates,
                input.token_decimals,
                input.arena,
            )?;
            if !assessment.should_execute {
                crate::debug!(
                    "prepare skip: reassess rejected ({})",
                    assessment.reject_reason.as_deref().unwrap_or("unknown")
                );
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
                let reason = capped
                    .evaluated
                    .assessment
                    .as_ref()
                    .and_then(|a| a.reject_reason.clone())
                    .unwrap_or_else(|| "capped reassess rejected".into());
                crate::debug!("prepare skip: {reason}");
                return None;
            }
            Some(capped)
        }
    }
}

fn reoptimize_capped(
    input: &PrepareDispatchInput<'_>,
    source: FlashLoanSource,
    cap: U256,
) -> Option<PreparedDispatch> {
    let mut profit_ctx = ProfitEvalContext::with_safety_multiplier(
        input.evaluated.cycle.start_token,
        input.arena,
        input.token_to_matic_rates,
        input.token_decimals,
        input.gas_price,
        input.slippage_bps,
        source,
        input.safety_multiplier_bps,
    );
    profit_ctx.gas_scale_bps = input.gas_scale_bps;
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
        None,
    )?;
    let optimal_input = opt.optimal_input.min(cap);
    let sim = simulate_route_detailed(input.arena, &input.evaluated.cycle.edges, optimal_input)?;
    if sim.profit.is_zero() {
        return None;
    }
    if !capped_sim_passes_sanity(input, &sim, opt.search_low) {
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

fn dispatch_sim_passes_sanity(
    input: &PrepareDispatchInput<'_>,
    result: &crate::core::types::RouteSimulationResult,
    search_low: U256,
    token_decimals: u8,
    token_to_matic_rate: U256,
) -> bool {
    if token_to_matic_rate < crate::core::constants::MIN_TOKEN_TO_MATIC_RATE {
        return false;
    }
    if let Some(cap) = max_flash_borrow_wei(
        input.max_flash_loan_usd,
        token_decimals,
        token_to_matic_rate,
    ) && result.amount_in > cap
    {
        return false;
    }
    check_sim_sanity(SimSanityInput {
        amount_in: result.amount_in,
        gross_profit: result.profit,
        search_low,
        token_decimals,
        token_to_matic_rate,
    })
    .is_ok()
}

fn capped_sim_passes_sanity(
    input: &PrepareDispatchInput<'_>,
    result: &crate::core::types::RouteSimulationResult,
    search_low: U256,
) -> bool {
    let token_to_matic_rate = resolve_token_to_matic_rate(
        input.evaluated.cycle.start_token,
        input.arena,
        input.token_to_matic_rates,
    );
    let token_decimals = resolve_token_decimals_for_index(
        input.evaluated.cycle.start_token,
        input.arena,
        input.token_decimals,
    );
    dispatch_sim_passes_sanity(
        input,
        result,
        search_low,
        token_decimals,
        token_to_matic_rate,
    )
}

// ponytail: 11 params, bundle into a struct when a 3rd call site appears.
#[allow(clippy::too_many_arguments)]
fn reassess_route(
    evaluated: &EvaluatedRoute,
    source: FlashLoanSource,
    min_profit_matic: U256,
    min_profit_roi_bps: u64,
    gas_price: U256,
    slippage_bps: u64,
    safety_multiplier_bps: u64,
    gas_scale_bps: u64,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
    token_decimals: &FxHashMap<Address, u8>,
    arena: &StateArena,
) -> Option<ProfitAssessment> {
    let gas_units = if gas_scale_bps == 10_000 {
        evaluated.result.total_gas
    } else {
        crate::services::execution::support::scaled_simulated_gas(
            evaluated.result.total_gas,
            gas_scale_bps,
        )
    };
    let route = RouteProfitParams {
        gross_profit: evaluated.result.profit,
        amount_in: evaluated.result.amount_in,
        gas_units,
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

#[must_use]
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

    #[test]
    fn mixed_balancer_route_caps_to_known_aave_liquidity() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::Auto,
            U256::from(1_000u64),
            TokenFlashLiquidity {
                balancer: U256::from(10_000u64),
                aave: U256::from(400u64),
                aave_listed: true,
                dodo: U256::ZERO,
            },
            true,
            false,
        );

        assert_eq!(plan.source, FlashLoanSource::AaveV3);
        assert_eq!(plan.action, FlashPlanAction::CapAndReoptimize);
        assert_eq!(plan.cap, U256::from(400u64));
    }

    #[test]
    fn mixed_balancer_route_uses_aave_under_balancer_only_policy() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::BalancerOnly,
            U256::from(1_000u64),
            TokenFlashLiquidity {
                balancer: U256::from(10_000u64),
                aave: U256::from(500u64),
                aave_listed: true,
                dodo: U256::ZERO,
            },
            true,
            false,
        );

        assert_eq!(plan.source, FlashLoanSource::AaveV3);
        assert_eq!(plan.action, FlashPlanAction::CapAndReoptimize);
        assert_eq!(plan.cap, U256::from(500u64));
    }

    #[test]
    fn balancer_only_route_uses_direct_entrypoint() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::Auto,
            U256::from(1_000u64),
            TokenFlashLiquidity {
                balancer: U256::from(10_000u64),
                aave: U256::ZERO,
                aave_listed: false,
                dodo: U256::ZERO,
            },
            true,
            true,
        );

        assert_eq!(plan.source, FlashLoanSource::Direct);
        assert_eq!(plan.action, FlashPlanAction::Direct);
        assert_eq!(plan.cap, U256::from(1_000u64));
    }

    #[test]
    fn zero_balancer_liquidity_rejects_instead_of_deferring() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::BalancerOnly,
            U256::from(1_000u64),
            TokenFlashLiquidity {
                balancer: U256::ZERO,
                aave: U256::ZERO,
                aave_listed: false,
                dodo: U256::ZERO,
            },
            false,
            false,
        );
        assert_eq!(plan.action, FlashPlanAction::Reject);
    }

    #[test]
    fn effective_balancer_prefers_route_cap_when_vault_balance_is_higher() {
        assert_eq!(
            effective_balancer_liquidity(U256::from(10_000u64), U256::from(400u64)),
            U256::from(400u64)
        );
        assert_eq!(
            effective_balancer_liquidity(U256::ZERO, U256::from(400u64)),
            U256::from(400u64)
        );
        assert_eq!(
            effective_balancer_liquidity(U256::from(10_000u64), U256::ZERO),
            U256::from(10_000u64)
        );
    }

    #[test]
    fn aave_reserve_flash_eligible_requires_active_and_flash_enabled() {
        assert!(aave_reserve_flash_eligible(U256::from(0x81))); // active + flash
        assert!(!aave_reserve_flash_eligible(U256::ZERO)); // inactive
        assert!(!aave_reserve_flash_eligible(U256::from(0x80))); // flash but inactive
        assert!(!aave_reserve_flash_eligible(U256::from(0x83))); // active+flash but frozen
    }

    #[test]
    fn mixed_balancer_route_rejects_zero_aave_liquidity() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::Auto,
            U256::from(1_000u64),
            TokenFlashLiquidity {
                balancer: U256::from(10_000u64),
                aave: U256::ZERO,
                aave_listed: true,
                dodo: U256::ZERO,
            },
            true,
            false,
        );

        assert_eq!(plan.action, FlashPlanAction::Reject);
    }
}
