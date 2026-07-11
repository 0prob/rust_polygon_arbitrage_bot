use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::primitives::U256;
use alloy::providers::Provider;
use alloy::sol_types::SolCall;
use anyhow::Context;
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use crate::abis::{IAaveV3Pool, IERC20Metadata};
use crate::core::constants::{AAVE_V3_POOL, BALANCER_VAULT};
use crate::infra::rpc::{RpcPool, rpc_host_label};
use crate::services::execution::rpc_errors::is_rpc_rate_limited;
use crate::core::types::{
    EvaluatedRoute, FlashLoanSource, FoundCycle, PoolState, ProfitAssessment, ProtocolType,
    TokenIndex,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::simulate_route_detailed;
use crate::pipeline::multicall::{MulticallItem, encode_call, execute_multicall};
use crate::pipeline::sim_sanity::{SimSanityInput, max_flash_borrow_wei};
use crate::pipeline::ternary::RouteGasCosting;
use crate::pipeline::ternary::optimize_cycle;
use crate::services::execution::flash_policy::FlashLoanPolicy;
use crate::services::execution::gas_oracle::GasOracle;
use crate::services::execution::profit::{
    AssessmentGas, ProfitEvalContext, RouteAssessRequest, assess_route_from_sim,
    route_profit_thresholds,
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

/// Immutable, atomically published flash-liquidity map (LF/HF readers hold `Arc` snapshots).
#[derive(Debug, Clone, Default)]
pub struct FlashLiquiditySnapshot {
    generation: u64,
    entries: FxHashMap<Address, CachedLiquidity>,
}

impl FlashLiquiditySnapshot {
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn token_liquidity(&self, token: Address, ttl: Duration) -> TokenFlashLiquidity {
        self.entries
            .get(&token)
            .filter(|e| e.fetched_at.elapsed() < ttl)
            .map(|e| e.snapshot)
            .unwrap_or_default()
    }

    #[must_use]
    pub fn tokens_liquidity(&self, tokens: &[Address], ttl: Duration) -> Vec<TokenFlashLiquidity> {
        tokens
            .iter()
            .map(|token| self.token_liquidity(*token, ttl))
            .collect()
    }

    #[must_use]
    pub fn has_fresh(&self, token: Address, ttl: Duration) -> bool {
        self.entries
            .get(&token)
            .is_some_and(|e| e.fetched_at.elapsed() < ttl)
    }
}

/// Lock-free flash liquidity handoff: background refresh builds a new map, then `store`s it.
#[derive(Debug)]
pub struct FlashLiquidityCache {
    inner: ArcSwap<FlashLiquiditySnapshot>,
    ttl: Duration,
    balancer_vault: Address,
    aave_pool: Address,
    /// Tokens merged from HF/LF ticks for the background refresher.
    hot_tokens: Mutex<FxHashSet<Address>>,
    /// Dry-run `ReserveInactive` pins — refresh must not resurrect Aave for these tokens.
    aave_inactive_pins: Mutex<FxHashSet<Address>>,
    /// Prevents HF tick + background + dispatch from hammering the same multicall batch.
    refresh_inflight: AtomicBool,
}

impl FlashLiquidityCache {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(FlashLiquiditySnapshot::default()),
            ttl: CACHE_TTL,
            balancer_vault: BALANCER_VAULT,
            aave_pool: AAVE_V3_POOL,
            hot_tokens: Mutex::new(FxHashSet::default()),
            aave_inactive_pins: Mutex::new(FxHashSet::default()),
            refresh_inflight: AtomicBool::new(false),
        }
    }

    #[must_use]
    pub fn with_addresses(balancer_vault: Address, aave_pool: Address) -> Self {
        Self {
            inner: ArcSwap::from_pointee(FlashLiquiditySnapshot::default()),
            ttl: CACHE_TTL,
            balancer_vault,
            aave_pool,
            hot_tokens: Mutex::new(FxHashSet::default()),
            aave_inactive_pins: Mutex::new(FxHashSet::default()),
            refresh_inflight: AtomicBool::new(false),
        }
    }

    #[must_use]
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        self.inner.load().generation
    }

    /// Latest published snapshot — lock-free; safe to call on every route evaluation.
    pub fn load(&self) -> Arc<FlashLiquiditySnapshot> {
        self.inner.load_full()
    }

    fn publish_updates(&self, updates: FxHashMap<Address, CachedLiquidity>) {
        self.inner.rcu(|current| {
            let mut entries = current.entries.clone();
            entries.extend(updates.iter().map(|(token, value)| (*token, value.clone())));
            Arc::new(FlashLiquiditySnapshot {
                generation: current.generation.saturating_add(1),
                entries,
            })
        });
    }

    /// Merge tokens into the background refresher's hot set.
    pub fn track_hot_tokens(&self, tokens: &[Address]) {
        let mut hot = self.hot_tokens.lock();
        for token in tokens {
            hot.insert(*token);
        }
    }

    fn hot_token_list(&self) -> Vec<Address> {
        self.hot_tokens.lock().iter().copied().collect()
    }

    pub fn snapshot(&self, token: Address) -> TokenFlashLiquidity {
        let snap = self.inner.load();
        snap.token_liquidity(token, self.ttl)
    }

    /// Batch-read flash liquidity from the current published snapshot.
    #[must_use]
    pub fn snapshots_for(&self, tokens: &[Address]) -> Vec<TokenFlashLiquidity> {
        let snap = self.inner.load();
        snap.tokens_liquidity(tokens, self.ttl)
    }

    #[must_use]
    pub fn has_fresh_entry(&self, token: Address) -> bool {
        let snap = self.inner.load();
        snap.has_fresh(token, self.ttl)
    }

    /// Drop cached liquidity so the next refresh re-fetches (e.g. after ReserveInactive).
    pub fn invalidate(&self, token: Address) {
        self.invalidate_batch(std::slice::from_ref(&token));
    }

    /// Drop stale entries for a refresh batch that failed or timed out.
    pub fn invalidate_batch(&self, tokens: &[Address]) {
        self.inner.rcu(|current| {
            let mut entries = current.entries.clone();
            for token in tokens {
                entries.remove(token);
            }
            Arc::new(FlashLiquiditySnapshot {
                generation: current.generation.saturating_add(1),
                entries,
            })
        });
    }

    /// Pin Aave as inactive after on-chain `ReserveInactive` so HF eval stops routing through it.
    pub fn mark_aave_inactive(&self, token: Address) {
        self.aave_inactive_pins.lock().insert(token);
        self.inner.rcu(|current| {
            let mut entries = current.entries.clone();
            let prior = entries.get(&token).map(|e| e.snapshot).unwrap_or_default();
            entries.insert(
                token,
                CachedLiquidity {
                    snapshot: TokenFlashLiquidity {
                        balancer: prior.balancer,
                        aave: U256::ZERO,
                        aave_listed: false,
                        dodo: prior.dodo,
                    },
                    fetched_at: Instant::now(),
                },
            );
            Arc::new(FlashLiquiditySnapshot {
                generation: current.generation.saturating_add(1),
                entries,
            })
        });
    }

    pub fn start_background(
        self: Arc<Self>,
        rpc: Arc<RpcPool>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let poll = self.ttl;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(poll);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        let tokens = self.hot_token_list();
                        if tokens.is_empty() {
                            continue;
                        }
                        if let Err(e) = self.refresh_with_fallback(&rpc, &tokens).await {
                            crate::debug!("background flash liquidity refresh failed: {e:#}");
                        }
                    }
                }
            }
        });
    }

    /// Fire-and-forget refresh for the HF tick path — never blocks the tick on RPC IO.
    pub fn spawn_refresh_if_stale(self: &Arc<Self>, rpc: Arc<RpcPool>, tokens: &[Address]) {
        if tokens.is_empty() {
            return;
        }
        self.track_hot_tokens(tokens);
        if !tokens.iter().any(|token| !self.has_fresh_entry(*token)) {
            return;
        }
        if self
            .refresh_inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let cache = Arc::clone(self);
        let tokens = tokens.to_vec();
        tokio::spawn(async move {
            struct InflightGuard<'a>(&'a AtomicBool);
            impl Drop for InflightGuard<'_> {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::Release);
                }
            }
            let _guard = InflightGuard(&cache.refresh_inflight);
            if let Err(e) = cache.refresh_with_fallback(&rpc, &tokens).await
                && !is_rpc_rate_limited(&e)
            {
                crate::debug!("hf flash liquidity refresh failed: {e:#}");
            }
        });
    }

    pub async fn refresh<P: Provider<Ethereum> + Clone + Send + 'static>(
        &self,
        provider: &P,
        tokens: &[Address],
    ) -> anyhow::Result<u64> {
        self.track_hot_tokens(tokens);
        let to_fetch = self.stale_tokens(tokens);
        if to_fetch.is_empty() {
            return Ok(self.generation());
        }
        self.fetch_and_publish(provider, &to_fetch).await?;
        Ok(self.generation())
    }

    /// Walk state RPC candidates on failure/rate-limit instead of pinning to one endpoint.
    pub async fn refresh_with_fallback(
        &self,
        rpc: &RpcPool,
        tokens: &[Address],
    ) -> anyhow::Result<u64> {
        self.track_hot_tokens(tokens);
        let to_fetch = self.stale_tokens(tokens);
        if to_fetch.is_empty() {
            return Ok(self.generation());
        }
        let candidates = rpc.state_url_candidates();
        anyhow::ensure!(!candidates.is_empty(), "no state RPC configured");
        let mut last_err: Option<anyhow::Error> = None;
        for (idx, url) in candidates.iter().enumerate() {
            let provider = match rpc.connect_state_at(url) {
                Ok(p) => p,
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            };
            match self.fetch_and_publish(&provider, &to_fetch).await {
                Ok(()) => return Ok(self.generation()),
                Err(e) => {
                    if is_rpc_rate_limited(&e) {
                        rpc.deprioritize_state_url(url);
                        crate::debug!(
                            "flash liquidity refresh rate-limited on {}",
                            rpc_host_label(url)
                        );
                    } else if idx + 1 < candidates.len() {
                        rpc.deprioritize_state_url(url);
                    }
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            anyhow::anyhow!("flash liquidity refresh failed on all state RPCs")
        }))
    }

    fn stale_tokens(&self, tokens: &[Address]) -> Vec<Address> {
        let current = self.inner.load_full();
        let now = Instant::now();
        tokens
            .iter()
            .copied()
            .filter(|token| {
                current
                    .entries
                    .get(token)
                    .is_none_or(|e| now.saturating_duration_since(e.fetched_at) >= self.ttl)
            })
            .collect()
    }

    async fn fetch_and_publish<P: Provider<Ethereum> + Clone + Send + 'static>(
        &self,
        provider: &P,
        to_fetch: &[Address],
    ) -> anyhow::Result<()> {
        if to_fetch.is_empty() {
            return Ok(());
        }
        let now = Instant::now();
        let mut items = Vec::with_capacity(to_fetch.len() * 2);
        for token in to_fetch {
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

        let mut updates =
            FxHashMap::with_capacity_and_hasher(to_fetch.len(), rustc_hash::FxBuildHasher);
        let mut aave_index = 0usize;
        let inactive_pins = self.aave_inactive_pins.lock();
        for (i, token) in to_fetch.iter().enumerate() {
            let base = i * 2;
            let balancer = decode_balance(results.get(base));
            let aave_pinned = inactive_pins.contains(token);
            let aave_listed = !aave_pinned && reserves[i].is_some();
            let aave = if aave_listed {
                let balance = decode_balance(aave_results.get(aave_index));
                aave_index += 1;
                balance
            } else {
                U256::ZERO
            };
            updates.insert(
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
        self.publish_updates(updates);
        Ok(())
    }
}

impl Default for FlashLiquidityCache {
    fn default() -> Self {
        Self::new()
    }
}

/// On-chain Aave reserve check — mirrors `ValidationLogic.validateFlashloanSimple`.
pub async fn aave_flash_reserve_viable<P: Provider<Ethereum>>(
    provider: &P,
    aave_pool: Address,
    token: Address,
) -> bool {
    let pool = IAaveV3Pool::new(aave_pool, provider);
    let Ok(reserve) = pool.getReserveData(token).call().await else {
        return false;
    };
    !reserve.aTokenAddress.is_zero() && aave_reserve_flash_eligible(reserve.configuration)
}

fn decode_balance(bytes: Option<&Option<alloy::primitives::Bytes>>) -> U256 {
    bytes
        .and_then(|b| b.as_ref())
        .and_then(|b| IERC20Metadata::balanceOfCall::abi_decode_returns(b).ok())
        .map_or(U256::ZERO, U256::from)
}

/// Aave V3 `ReserveConfigurationMap` bit positions (see Pool.sol / ReserveConfiguration.sol).
const AAVE_CFG_ACTIVE_BIT: u32 = 56;
const AAVE_CFG_FROZEN_BIT: u32 = 57;
const AAVE_CFG_PAUSED_BIT: u32 = 60;
const AAVE_CFG_FLASH_BIT: u32 = 63;

#[inline]
fn aave_cfg_bit_set(configuration: U256, bit: u32) -> bool {
    (configuration >> bit) & U256::from(1) != U256::ZERO
}

/// Active, unfrozen, unpaused, flash-loan-enabled — else Aave reverts `ReserveInactive()` / `FLASHLOAN_DISABLED`.
#[inline]
#[must_use]
pub fn aave_reserve_flash_eligible(configuration: U256) -> bool {
    aave_cfg_bit_set(configuration, AAVE_CFG_ACTIVE_BIT)
        && !aave_cfg_bit_set(configuration, AAVE_CFG_FROZEN_BIT)
        && !aave_cfg_bit_set(configuration, AAVE_CFG_PAUSED_BIT)
        && aave_cfg_bit_set(configuration, AAVE_CFG_FLASH_BIT)
}

/// True when the route swaps through the Balancer vault (not just pool flash liquidity).
fn route_uses_balancer_vault_swap(cycle: &FoundCycle) -> bool {
    cycle
        .edges
        .iter()
        .any(|e| e.protocol == ProtocolType::BalancerV2)
}

#[must_use]
pub fn cycle_has_dodo_pool(arena: &StateArena, cycle: &FoundCycle) -> bool {
    cycle
        .edges
        .iter()
        .any(|edge| matches!(arena.pool_state(edge.pool_index), Some(PoolState::Dodo(_))))
}

/// Map eval-time flash plans onto sources the executor can actually dispatch.
/// Mixed routes cannot use Balancer flash; when Aave is unavailable, fall back to Dodo.
#[must_use]
pub fn align_flash_source_for_dispatch(
    source: FlashLoanSource,
    liquidity: &TokenFlashLiquidity,
    balancer_only: bool,
    has_dodo: bool,
) -> Option<FlashLoanSource> {
    let aave_viable = liquidity.aave_listed && !liquidity.aave.is_zero();
    if !balancer_only && matches!(source, FlashLoanSource::Balancer | FlashLoanSource::Direct) {
        if aave_viable {
            return Some(FlashLoanSource::AaveV3);
        }
        if has_dodo {
            return Some(FlashLoanSource::Dodo);
        }
        return None;
    }
    if source == FlashLoanSource::AaveV3 && !aave_viable {
        if has_dodo {
            return Some(FlashLoanSource::Dodo);
        }
        return None;
    }
    Some(source)
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
            cycle_ratio: cycle.cycle_ratio,
        })
    } else {
        None
    }
}

/// True when any hop token is listed on Aave V3 (flash borrow candidate).
pub fn cycle_has_aave_listed_token(
    cycle: &FoundCycle,
    arena: &StateArena,
    flash: &FlashLiquiditySnapshot,
    ttl: Duration,
) -> bool {
    let mut addrs = Vec::with_capacity(cycle.edges.len());
    for edge in &cycle.edges {
        if let Some(addr) = arena.token_address(edge.token_in) {
            addrs.push(addr);
        }
    }
    flash
        .tokens_liquidity(&addrs, ttl)
        .into_iter()
        .any(|liquidity| liquidity.aave_listed && !liquidity.aave.is_zero())
}

fn cycle_flash_cache_warm(
    cycle: &FoundCycle,
    arena: &StateArena,
    flash: &FlashLiquiditySnapshot,
    ttl: Duration,
) -> bool {
    let mut seen = rustc_hash::FxHashSet::default();
    for edge in &cycle.edges {
        let token = edge.token_in;
        if !seen.insert(token) {
            continue;
        }
        let Some(addr) = arena.token_address(token) else {
            continue;
        };
        if flash.has_fresh(addr, ttl) {
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
    flash: &FlashLiquiditySnapshot,
    ttl: Duration,
) -> bool {
    if !route_uses_balancer_vault_swap(cycle) {
        return true;
    }
    if route_is_balancer_only(cycle) {
        return true;
    }
    // ponytail: cold cache must not reject every mixed Balancer route before refresh lands.
    if !cycle_flash_cache_warm(cycle, arena, flash, ttl) {
        return true;
    }
    cycle_has_aave_listed_token(cycle, arena, flash, ttl)
}
/// Mixed Balancer routes forbid Balancer flash loans (`BalancerVaultReentrancy`). Prefer an Aave-listed token
/// already present in the cycle as the flash borrow asset.
pub fn prefer_aave_flash_start<'a>(
    cycle: &'a FoundCycle,
    arena: &StateArena,
    flash: &FlashLiquiditySnapshot,
    ttl: Duration,
) -> Cow<'a, FoundCycle> {
    if !route_uses_balancer_vault_swap(cycle) {
        return Cow::Borrowed(cycle);
    }

    let mut seen: rustc_hash::FxHashSet<TokenIndex> = rustc_hash::FxHashSet::default();
    let mut token_addrs: Vec<Address> = Vec::new();
    let mut token_indices: Vec<TokenIndex> = Vec::new();
    for edge in &cycle.edges {
        let token = edge.token_in;
        if !seen.insert(token) {
            continue;
        }
        let Some(addr) = arena.token_address(token) else {
            continue;
        };
        token_addrs.push(addr);
        token_indices.push(token);
    }
    let snapshots = flash.tokens_liquidity(&token_addrs, ttl);
    let mut candidates: Vec<(U256, TokenIndex)> = Vec::new();
    for (liq, token) in snapshots.into_iter().zip(token_indices) {
        if liq.aave_listed && !liq.aave.is_zero() {
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
    flash: &FlashLiquiditySnapshot,
    ttl: Duration,
) -> Option<TokenFlashLiquidity> {
    let start_addr = arena.token_address(cycle.start_token)?;
    let snapshot = flash.token_liquidity(start_addr, ttl);
    let route_cap = route_balancer_flash_capacity(arena, cycle);
    let has_dodo = cycle_has_dodo_pool(arena, cycle);
    Some(TokenFlashLiquidity {
        balancer: effective_balancer_liquidity(snapshot.balancer, route_cap),
        aave: snapshot.aave,
        aave_listed: snapshot.aave_listed,
        // Only offer DODO when the route actually swaps through a DODO pool.
        dodo: if has_dodo { U256::MAX } else { U256::ZERO },
    })
}

/// Flash loan source for eval/ranking at a concrete borrow size (probe or optimal `amount_in`).
#[must_use]
pub fn resolve_flash_source_for_cycle(
    cycle: &FoundCycle,
    arena: &StateArena,
    flash: &FlashLiquiditySnapshot,
    ttl: Duration,
    policy: FlashLoanPolicy,
    amount_in: U256,
) -> Option<FlashLoanSource> {
    if amount_in.is_zero() {
        return None;
    }
    let start_addr = arena.token_address(cycle.start_token)?;
    let liquidity = flash_liquidity_for_cycle(cycle, arena, flash, ttl)?;
    let forbid = route_uses_balancer_vault_swap(cycle);
    let balancer_only = route_is_balancer_only(cycle);
    let plan = plan_flash_loan(policy, amount_in, liquidity, forbid, balancer_only);
    let has_dodo = cycle_has_dodo_pool(arena, cycle);
    match plan.action {
        FlashPlanAction::Reject => {
            // ponytail: no optimistic Aave/Balancer fallback when cache is cold — caused
            // AaveReserveInactive dry-runs after failed flash refresh (RPC rate limits).
            if balancer_only && forbid {
                Some(FlashLoanSource::Direct)
            } else {
                if flash.has_fresh(start_addr, ttl) {
                    crate::debug!(
                        "flash source reject: token={start_addr} policy={policy:?} forbid_balancer={forbid} balancer_only={balancer_only} balancer={} aave={} aave_listed={} dodo={}",
                        liquidity.balancer,
                        liquidity.aave,
                        liquidity.aave_listed,
                        liquidity.dodo,
                    );
                }
                None
            }
        }
        _ => align_flash_source_for_dispatch(plan.source, &liquidity, balancer_only, has_dodo),
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
    pub profit_priority_alpha_bps: u64,
    pub route_fingerprint: u64,
    pub gas_oracle: &'a GasOracle,
    /// Brent/probe search_low from HF eval — must match validate_optimized_sim.
    pub search_low: U256,
    /// Learned route risk multiplier applied to min-profit thresholds (matches HF eval).
    pub risk_multiplier_bps: u64,
    /// Reuse HF assessment when dispatch state matches eval (skip reassess).
    pub existing_assessment: Option<crate::core::types::ProfitAssessment>,
    /// Emit prepare reject reason at INFO (rate-limited by caller).
    pub log_skips: bool,
}

#[inline]
fn prepare_skip_log(enabled: bool, msg: &str) {
    if enabled {
        crate::info!("{msg}");
    } else {
        crate::debug!("{msg}");
    }
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
    let has_dodo = cycle_has_dodo_pool(input.arena, &input.evaluated.cycle);
    let liquidity = TokenFlashLiquidity {
        balancer: effective_balancer_liquidity(input.liquidity.balancer, route_cap),
        aave: input.liquidity.aave,
        aave_listed: input.liquidity.aave_listed,
        dodo: if has_dodo { U256::MAX } else { U256::ZERO },
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
            prepare_skip_log(
                input.log_skips,
                &format!(
                    "prepare skip: flash plan rejected (policy={:?}, forbid_balancer={forbid_balancer_flash}, amount_in={amount_in})",
                    input.policy
                ),
            );
            None
        }
        FlashPlanAction::Direct => {
            if !dispatch_sim_passes_sanity(
                input,
                &input.evaluated.result,
                input.search_low,
                token_decimals,
                token_to_matic_rate,
            ) {
                prepare_skip_log(
                    input.log_skips,
                    &format!(
                        "prepare skip: dispatch sim sanity rejected (search_low={} amount_in={amount_in} profit={})",
                        input.search_low, input.evaluated.result.profit,
                    ),
                );
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
            let assessment = input
                .existing_assessment
                .clone()
                .filter(|a| a.should_execute)
                .or_else(|| reassess_route(input, plan.source))?;
            if !assessment.should_execute {
                prepare_skip_log(
                    input.log_skips,
                    &format!(
                        "prepare skip: reassess rejected ({})",
                        assessment.reject_reason.as_deref().unwrap_or("unknown")
                    ),
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
                prepare_skip_log(input.log_skips, &format!("prepare skip: {reason}"));
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
    profit_ctx.gas_scale_bps = 10_000;
    profit_ctx.hop_count = input.evaluated.cycle.hop_count;
    profit_ctx.profit_priority_alpha_bps = input.profit_priority_alpha_bps;
    let route_gas = crate::services::execution::gas_oracle::RouteGasLookup::for_fingerprints(
        input.gas_oracle,
        [input.route_fingerprint],
    );
    let route_gas_costing = RouteGasCosting {
        lookup: &route_gas,
        oracle: input.gas_oracle,
        fingerprint: input.route_fingerprint,
    };
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
        Some(route_gas_costing),
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

    let assessment = assess_route_from_sim(&RouteAssessRequest {
        cycle_start: input.evaluated.cycle.start_token,
        arena: input.arena,
        gross_profit: sim.profit,
        amount_in: sim.amount_in,
        simulated_gas: sim.total_gas,
        hop_count: input.evaluated.cycle.hop_count,
        slippage_bps: input.slippage_bps,
        flash_source: source,
        gas: AssessmentGas::Route {
            oracle: input.gas_oracle,
            route_fp: input.route_fingerprint,
        },
        thresholds: prepare_profit_thresholds(input),
        token_to_matic_rates: input.token_to_matic_rates,
        token_decimals: input.token_decimals,
        gas_price: input.gas_price,
    });

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

fn prepare_profit_thresholds(
    input: &PrepareDispatchInput<'_>,
) -> crate::services::execution::profit::ProfitThresholds {
    route_profit_thresholds(
        input.min_profit_matic,
        input.min_profit_roi_bps,
        input.safety_multiplier_bps,
        input.profit_priority_alpha_bps,
        input.risk_multiplier_bps,
    )
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
    crate::pipeline::sim_sanity::check_sim_sanity_for_dispatch(SimSanityInput {
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

fn reassess_route(
    input: &PrepareDispatchInput<'_>,
    source: FlashLoanSource,
) -> Option<ProfitAssessment> {
    Some(assess_route_from_sim(&RouteAssessRequest {
        cycle_start: input.evaluated.cycle.start_token,
        arena: input.arena,
        gross_profit: input.evaluated.result.profit,
        amount_in: input.evaluated.result.amount_in,
        simulated_gas: input.evaluated.result.total_gas,
        hop_count: input.evaluated.cycle.hop_count,
        slippage_bps: input.slippage_bps,
        flash_source: source,
        gas: AssessmentGas::Route {
            oracle: input.gas_oracle,
            route_fp: input.route_fingerprint,
        },
        thresholds: prepare_profit_thresholds(input),
        token_to_matic_rates: input.token_to_matic_rates,
        token_decimals: input.token_decimals,
        gas_price: input.gas_price,
    }))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_dispatch_rejects_aave_when_unlisted_on_mixed_route() {
        let liquidity = TokenFlashLiquidity {
            balancer: U256::from(10_000u64),
            aave: U256::ZERO,
            aave_listed: false,
            dodo: U256::MAX,
        };
        assert_eq!(
            align_flash_source_for_dispatch(FlashLoanSource::Balancer, &liquidity, false, false,),
            None
        );
    }

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
        let active_flash =
            U256::from(1u128 << AAVE_CFG_ACTIVE_BIT) | U256::from(1u128 << AAVE_CFG_FLASH_BIT);
        assert!(aave_reserve_flash_eligible(active_flash));
        assert!(!aave_reserve_flash_eligible(U256::ZERO));
        assert!(!aave_reserve_flash_eligible(U256::from(
            1u128 << AAVE_CFG_FLASH_BIT
        )));
        let frozen = active_flash | U256::from(1u128 << AAVE_CFG_FROZEN_BIT);
        assert!(!aave_reserve_flash_eligible(frozen));
        let paused = active_flash | U256::from(1u128 << AAVE_CFG_PAUSED_BIT);
        assert!(!aave_reserve_flash_eligible(paused));
    }

    #[test]
    fn flash_source_at_economic_probe_differs_from_unit_probe() {
        let liquidity = TokenFlashLiquidity {
            balancer: U256::from(500u64),
            aave: U256::from(10_000u64),
            aave_listed: true,
            dodo: U256::ZERO,
        };
        let unit = plan_flash_loan(
            FlashLoanPolicy::Auto,
            U256::from(1u64),
            liquidity,
            false,
            false,
        );
        let economic = plan_flash_loan(
            FlashLoanPolicy::Auto,
            U256::from(1_000u64),
            liquidity,
            false,
            false,
        );
        assert_eq!(unit.source, FlashLoanSource::Balancer);
        assert_eq!(unit.action, FlashPlanAction::Direct);
        assert_eq!(economic.source, FlashLoanSource::AaveV3);
        assert_eq!(economic.action, FlashPlanAction::Direct);
    }

    #[test]
    fn prepare_thresholds_apply_route_risk_multiplier() {
        let thresholds =
            route_profit_thresholds(U256::from(10_000_000_000_000_000u64), 0, 10_000, 0, 30_000);
        assert_eq!(
            thresholds.min_profit_matic_wei,
            U256::from(30_000_000_000_000_000u64)
        );
    }

    impl FlashLiquidityCache {
        fn seed_token(&self, token: Address, liquidity: TokenFlashLiquidity) {
            let current = self.load();
            let mut next = current.entries.clone();
            next.insert(
                token,
                CachedLiquidity {
                    snapshot: liquidity,
                    fetched_at: Instant::now(),
                },
            );
            let generation = current.generation.saturating_add(1);
            self.inner.store(Arc::new(FlashLiquiditySnapshot {
                generation,
                entries: next,
            }));
        }
    }

    #[test]
    fn mark_aave_inactive_pins_negative_snapshot() {
        let cache = FlashLiquidityCache::new();
        let token = Address::repeat_byte(0xcd);
        cache.seed_token(
            token,
            TokenFlashLiquidity {
                balancer: U256::from(1_000u64),
                aave: U256::from(5_000u64),
                aave_listed: true,
                dodo: U256::MAX,
            },
        );
        cache.mark_aave_inactive(token);
        let liquidity = cache.snapshot(token);
        assert!(!liquidity.aave_listed);
        assert!(liquidity.aave.is_zero());
        assert_eq!(liquidity.balancer, U256::from(1_000u64));
        assert!(cache.has_fresh_entry(token));
    }

    #[test]
    fn reject_without_fresh_cache_returns_none_for_aave_routes() {
        let cache = FlashLiquidityCache::new();
        let token = Address::repeat_byte(0xab);
        let liquidity = cache.snapshot(token);
        assert!(!liquidity.aave_listed);
        let plan = plan_flash_loan(
            FlashLoanPolicy::Auto,
            U256::from(1_000u64),
            liquidity,
            false,
            false,
        );
        assert_eq!(plan.action, FlashPlanAction::Reject);
        assert!(!cache.has_fresh_entry(token));
    }

    #[test]
    fn plan_auto_skips_dodo_without_dodo_pool_liquidity() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::Auto,
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
