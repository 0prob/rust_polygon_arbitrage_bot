use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::primitives::U256;
use alloy::providers::Provider;
use alloy::sol_types::SolCall;

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use crate::abis::{IAaveV3Pool, IERC20Metadata};
use crate::core::constants::{AAVE_V3_POOL, BALANCER_VAULT, is_polygon_hub_token};
use crate::core::types::{
    EvaluatedRoute, FlashLoanSource, FoundCycle, PoolState, ProfitAssessment, ProtocolType,
    TokenIndex,
};
use crate::infra::rpc::{RpcPool, rpc_host_label};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::simulate_route_detailed;
use crate::pipeline::multicall::{MulticallItem, encode_call, execute_multicall};
use crate::pipeline::sim_sanity::{FlashBorrowCapParams, SimSanityInput};
use crate::pipeline::ternary::RouteGasCosting;
use crate::pipeline::ternary::optimize_cycle;
use crate::services::execution::aave::{
    AaveRefreshStats, AaveReserveStatus, reserve_status_from_config,
};
use crate::services::execution::flash_policy::FlashLoanPolicy;
use crate::services::execution::gas_oracle::GasOracle;
use crate::services::execution::profit::{
    AssessmentGas, ProfitEvalContext, RouteAssessRequest, assess_route_from_sim,
    route_profit_thresholds,
};
use crate::services::execution::rpc_errors::is_rpc_rate_limited;
use crate::services::oracle::{
    has_reliable_matic_rate, resolve_token_decimals_for_index, resolve_token_to_matic_rate,
};

const CACHE_TTL: Duration = Duration::from_secs(30);
/// Cap hot-token tracking so the 30s background loop does not refresh unbounded history.
const MAX_HOT_FLASH_TOKENS: usize = 384;

/// Clears [`FlashLiquidityCache::refresh_inflight`] on drop so only one multicall batch runs at a time.
pub(crate) struct RefreshInflightGuard<'a>(&'a AtomicBool);

impl Drop for RefreshInflightGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

pub use crate::services::execution::aave::fetch_and_cache_aave_flash_loan_fee_bps;

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

/// Why a flash plan or dispatch alignment failed (HF funnel diagnostics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlashRejectReason {
    ZeroAmount,
    ColdCache,
    ZeroLiquidity,
    AaveUnlisted,
    MixedBalancerNoAave,
    AlignDispatch,
}

#[derive(Debug, Clone, Default)]
pub struct FlashLoanDiagnostics {
    pub mixed_no_aave: u32,
    pub reject_cold_cache: u32,
    pub reject_zero_liquidity: u32,
    pub reject_aave_unlisted: u32,
    pub reject_zero_amount: u32,
    pub reject_align: u32,
    pub refresh_tokens: u32,
    pub cache_generation: u64,
}

impl FlashLoanDiagnostics {
    pub fn record_reject(&mut self, reason: FlashRejectReason) {
        match reason {
            FlashRejectReason::ZeroAmount => self.reject_zero_amount += 1,
            FlashRejectReason::ColdCache => self.reject_cold_cache += 1,
            FlashRejectReason::ZeroLiquidity => self.reject_zero_liquidity += 1,
            FlashRejectReason::AaveUnlisted => self.reject_aave_unlisted += 1,
            FlashRejectReason::MixedBalancerNoAave => self.mixed_no_aave += 1,
            FlashRejectReason::AlignDispatch => self.reject_align += 1,
        }
    }

    pub fn merge(&mut self, other: Self) {
        self.mixed_no_aave += other.mixed_no_aave;
        self.reject_cold_cache += other.reject_cold_cache;
        self.reject_zero_liquidity += other.reject_zero_liquidity;
        self.reject_aave_unlisted += other.reject_aave_unlisted;
        self.reject_zero_amount += other.reject_zero_amount;
        self.reject_align += other.reject_align;
        self.refresh_tokens += other.refresh_tokens;
        if other.cache_generation > self.cache_generation {
            self.cache_generation = other.cache_generation;
        }
    }

    pub fn log_summary(&self, label: &str) {
        let rejects = self.reject_cold_cache
            + self.reject_zero_liquidity
            + self.reject_aave_unlisted
            + self.reject_zero_amount
            + self.reject_align
            + self.mixed_no_aave;
        if rejects == 0 && self.refresh_tokens == 0 {
            return;
        }
        crate::debug!(
            "flash loan: {label} mixed_no_aave={} cold={} zero_liq={} aave_unlisted={} zero_amt={} align={} refresh_tokens={} gen={}",
            self.mixed_no_aave,
            self.reject_cold_cache,
            self.reject_zero_liquidity,
            self.reject_aave_unlisted,
            self.reject_zero_amount,
            self.reject_align,
            self.refresh_tokens,
            self.cache_generation,
        );
    }
}

/// Per-cycle flash borrow context — build once, resolve at multiple probe sizes.
#[derive(Debug, Clone, Copy)]
pub struct CycleFlashContext {
    pub liquidity: TokenFlashLiquidity,
    pub forbid_balancer_flash: bool,
    pub balancer_only: bool,
    pub has_dodo: bool,
    pub start_addr: Address,
    pub start_fresh: bool,
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
        if hot.len() <= MAX_HOT_FLASH_TOKENS {
            return;
        }
        let current: FxHashSet<Address> = tokens.iter().copied().collect();
        hot.retain(|t| current.contains(t));
        if hot.len() > MAX_HOT_FLASH_TOKENS {
            hot.clear();
            for token in tokens.iter().take(MAX_HOT_FLASH_TOKENS) {
                hot.insert(*token);
            }
        }
    }

    /// One in-flight flash liquidity multicall (HF prefetch, background tick, dispatch spawn).
    #[must_use]
    pub(crate) fn try_acquire_refresh_inflight(&self) -> Option<RefreshInflightGuard<'_>> {
        if self
            .refresh_inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }
        Some(RefreshInflightGuard(&self.refresh_inflight))
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

    /// Fresh snapshot with listed Aave reserve and non-zero aToken liquidity — skips dispatch RPC.
    #[must_use]
    pub fn aave_viable_for_dispatch(&self, token: Address) -> bool {
        if !self.has_fresh_entry(token) {
            return false;
        }
        let snap = self.snapshot(token);
        snap.aave_listed && !snap.aave.is_zero()
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
        crate::services::execution::aave::record_aave_mark_inactive();
        self.aave_inactive_pins.lock().insert(token);
        crate::info!("aave: mark_inactive token={token}");
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
                        if let Err(e) =
                            crate::services::execution::aave::refresh_aave_flash_fee_with_fallback(
                                &rpc,
                            )
                            .await
                        {
                            crate::warn!("background aave fee refresh failed: {e:#}");
                        }
                        let tokens = self.hot_token_list();
                        if tokens.is_empty() {
                            continue;
                        }
                        let stale = self.stale_tokens(&tokens);
                        if stale.is_empty() {
                            continue;
                        }
                        let Some(_guard) = self.try_acquire_refresh_inflight() else {
                            continue;
                        };
                        if let Err(e) = self.refresh_with_fallback(&rpc, &stale).await {
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
        if self.refresh_inflight.load(Ordering::Acquire) {
            return;
        }
        let cache = Arc::clone(self);
        let tokens = tokens.to_vec();
        tokio::spawn(async move {
            let Some(_guard) = cache.try_acquire_refresh_inflight() else {
                return;
            };
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
        Err(last_err
            .unwrap_or_else(|| anyhow::anyhow!("flash liquidity refresh failed on all state RPCs")))
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
        let pinned_tokens: FxHashSet<Address> =
            self.aave_inactive_pins.lock().iter().copied().collect();
        let mut aave_stats = AaveRefreshStats::default();
        let reserves: Vec<Option<Address>> = (0..to_fetch.len())
            .map(|i| {
                let token = to_fetch[i];
                let pinned = pinned_tokens.contains(&token);
                let decoded = results
                    .get(i * 2 + 1)
                    .and_then(|bytes| bytes.as_ref())
                    .and_then(|bytes| {
                        IAaveV3Pool::getReserveDataCall::abi_decode_returns(bytes).ok()
                    });
                let status = match decoded.as_ref() {
                    Some(r) => {
                        let has_a_token = !r.aTokenAddress.is_zero();
                        reserve_status_from_config(r.configuration, has_a_token)
                    }
                    None => AaveReserveStatus::RpcError,
                };
                aave_stats.record(status, pinned);
                if pinned || status != AaveReserveStatus::Viable {
                    None
                } else {
                    decoded.map(|r| r.aTokenAddress)
                }
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
                        // DODO borrow size is route-local (see `flash_liquidity_for_cycle`).
                        dodo: U256::ZERO,
                    },
                    fetched_at: now,
                },
            );
        }
        let n = to_fetch.len();
        let generation = self.generation();
        self.publish_updates(updates);
        crate::info!("flash liquidity refresh: tokens={n} generation={generation}",);
        aave_stats.log_refresh_summary(n, self.generation());
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
    crate::services::execution::aave::aave_flash_reserve_status_live(provider, aave_pool, token)
        .await
        == AaveReserveStatus::Viable
}

fn decode_balance(bytes: Option<&Option<alloy::primitives::Bytes>>) -> U256 {
    bytes
        .and_then(|b| b.as_ref())
        .and_then(|b| IERC20Metadata::balanceOfCall::abi_decode_returns(b).ok())
        .map_or(U256::ZERO, U256::from)
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

/// Return a DODO pool that can lend the cycle start token through the executor's
/// base-asset-only flash entrypoint.
///
/// DODO V2 `flashLoan` is `preventReentrant`. Calling `sellBase` / `sellQuote` on
/// the **same** pool inside `DPPFlashLoanCall` / `DVMFlashLoanCall` always reverts.
/// Cycle edges are swap hops, so a cycle-local DODO pool is never a viable flash
/// source. Returning `None` fails closed at prepare instead of dry-run
/// `transfer amount exceeds balance` / reentrancy after a false-positive HF pass.
///
/// External (non-route) DODO flash liquidity is not wired yet; when it is, return a
/// base-compatible pool that is **not** a route swap hop ([`dodo_pool_is_route_swap_hop`]).
#[must_use]
pub fn dodo_base_flash_pool_for_cycle(arena: &StateArena, cycle: &FoundCycle) -> Option<Address> {
    // Keep start-token resolution so callers still fail closed on unknown tokens
    // the same way they did before reentrancy gating.
    let _start_token = arena.token_address(cycle.start_token)?;
    let _ = cycle;
    None
}

/// True when a DODO pool address is also a swap hop in the cycle (flash-incompatible).
#[must_use]
pub fn dodo_pool_is_route_swap_hop(arena: &StateArena, cycle: &FoundCycle, pool: Address) -> bool {
    cycle.edges.iter().any(|edge| {
        matches!(arena.pool_state(edge.pool_index), Some(PoolState::Dodo(_)))
            && arena.pool_address(edge.pool_index) == Some(pool)
    })
}

/// Map eval-time flash plans onto sources the executor can actually dispatch.
/// Mixed routes cannot use Balancer flash; DODO is only used when a non-swap-hop
/// flash pool is available (`has_dodo` from [`dodo_base_flash_pool_for_cycle`]).
#[must_use]
pub fn align_flash_source_for_dispatch(
    source: FlashLoanSource,
    liquidity: &TokenFlashLiquidity,
    balancer_only: bool,
    has_dodo: bool,
    route_uses_balancer_vault: bool,
) -> Option<FlashLoanSource> {
    let aave_viable = liquidity.aave_listed && !liquidity.aave.is_zero();
    let mixed_balancer_route = route_uses_balancer_vault && !balancer_only;
    if !balancer_only && matches!(source, FlashLoanSource::Balancer | FlashLoanSource::Direct) {
        if aave_viable {
            return Some(FlashLoanSource::AaveV3);
        }
        // Cycle-local DODO pools are reentrancy-incompatible; has_dodo is false for them.
        if has_dodo {
            return Some(FlashLoanSource::Dodo);
        }
        // Vault flash is safe for pure V2/V3/… routes; mixed vault hops need Aave/DODO instead.
        if !liquidity.balancer.is_zero() && !mixed_balancer_route {
            return Some(FlashLoanSource::Balancer);
        }
        return None;
    }
    if source == FlashLoanSource::AaveV3 && !aave_viable {
        if has_dodo {
            return Some(FlashLoanSource::Dodo);
        }
        return None;
    }
    if source == FlashLoanSource::Dodo && !has_dodo {
        if aave_viable {
            return Some(FlashLoanSource::AaveV3);
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
            hop_count: cycle.edge_hops(),
            log_weight: cycle.log_weight,
            cumulative_fee_bps: cycle.cumulative_fee_bps,
            score: cycle.score,
            cycle_ratio: cycle.cycle_ratio,
        })
    } else {
        None
    }
}

/// Non-zero flash liquidity on at least one provider (Aave listed + balance, Balancer vault, or DODO).
#[must_use]
pub fn token_flash_liquidity_borrowable(liquidity: &TokenFlashLiquidity) -> bool {
    !liquidity.balancer.is_zero()
        || !liquidity.dodo.is_zero()
        || (liquidity.aave_listed && !liquidity.aave.is_zero())
}

/// Fresh cache shows the token cannot fund any flash borrow size.
#[must_use]
pub fn token_flash_borrow_proven_unviable(
    token: Address,
    flash: &FlashLiquiditySnapshot,
    ttl: Duration,
) -> bool {
    flash.has_fresh(token, ttl)
        && !token_flash_liquidity_borrowable(&flash.token_liquidity(token, ttl))
}

/// Graph/cycle gates: borrow may exist, or hub allowlist while flash cache is still cold.
#[must_use]
pub fn token_eligible_for_flash_borrow_graph(
    token: Address,
    flash: &FlashLiquiditySnapshot,
    ttl: Duration,
) -> bool {
    if flash.has_fresh(token, ttl) {
        return token_flash_liquidity_borrowable(&flash.token_liquidity(token, ttl));
    }
    is_polygon_hub_token(token)
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

/// Every hop borrow candidate must have fresh flash liquidity before we fail-close mixed routes.
fn cycle_flash_tokens_all_fresh(
    cycle: &FoundCycle,
    arena: &StateArena,
    flash: &FlashLiquiditySnapshot,
    ttl: Duration,
) -> bool {
    let mut seen = rustc_hash::FxHashSet::default();
    let mut checked = 0u32;
    for edge in &cycle.edges {
        let token = edge.token_in;
        if !seen.insert(token) {
            continue;
        }
        let Some(addr) = arena.token_address(token) else {
            continue;
        };
        checked += 1;
        if !flash.has_fresh(addr, ttl) {
            return false;
        }
    }
    checked > 0
}

/// Mixed Balancer routes need Aave liquidity or a DODO pool that lends the cycle's
/// start token as base. Pure Balancer routes use `executeArbDirect` + `batchSwap`.
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
    if !cycle_flash_tokens_all_fresh(cycle, arena, flash, ttl) {
        // HF flash prefetch may still be in flight; partial cache must not false-reject.
        return true;
    }
    cycle_has_aave_listed_token(cycle, arena, flash, ttl)
        || dodo_base_flash_pool_for_cycle(arena, cycle).is_some()
}
/// Mixed Balancer routes forbid Balancer flash loans (`BalancerVaultReentrancy`). Prefer an Aave-listed token
/// already present in the cycle as the flash borrow asset.
pub fn prefer_aave_flash_start<'a>(
    cycle: &'a FoundCycle,
    arena: &StateArena,
    flash: &FlashLiquiditySnapshot,
    ttl: Duration,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
) -> Cow<'a, FoundCycle> {
    if !route_uses_balancer_vault_swap(cycle) || route_is_balancer_only(cycle) {
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
        if liq.aave_listed
            && !liq.aave.is_zero()
            && has_reliable_matic_rate(token, token_to_matic_rates)
        {
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
        if !liquidity.aave_listed || liquidity.aave.is_zero() {
            if !liquidity.dodo.is_zero() {
                return plan_single(FlashLoanSource::Dodo, amount_in, liquidity.dodo, false);
            }
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
    let source = if allow_balancer
        && !liquidity.balancer.is_zero()
        && liquidity.balancer >= liquidity.aave
    {
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

#[must_use]
pub fn build_cycle_flash_context(
    cycle: &FoundCycle,
    arena: &StateArena,
    flash: &FlashLiquiditySnapshot,
    ttl: Duration,
) -> Option<CycleFlashContext> {
    let start_addr = arena.token_address(cycle.start_token)?;
    let snapshot = flash.token_liquidity(start_addr, ttl);
    let route_cap = route_balancer_flash_capacity(arena, cycle);
    let has_dodo = dodo_base_flash_pool_for_cycle(arena, cycle).is_some();
    Some(CycleFlashContext {
        liquidity: TokenFlashLiquidity {
            balancer: effective_balancer_liquidity(snapshot.balancer, route_cap),
            aave: snapshot.aave,
            aave_listed: snapshot.aave_listed,
            dodo: if has_dodo { U256::MAX } else { U256::ZERO },
        },
        forbid_balancer_flash: route_uses_balancer_vault_swap(cycle),
        balancer_only: route_is_balancer_only(cycle),
        has_dodo,
        start_addr,
        start_fresh: flash.has_fresh(start_addr, ttl),
    })
}

#[must_use]
pub fn flash_reject_reason(
    ctx: &CycleFlashContext,
    policy: FlashLoanPolicy,
    amount_in: U256,
) -> Option<FlashRejectReason> {
    if amount_in.is_zero() {
        return Some(FlashRejectReason::ZeroAmount);
    }
    if ctx.balancer_only && ctx.forbid_balancer_flash {
        return None;
    }
    let plan = plan_flash_loan(
        policy,
        amount_in,
        ctx.liquidity,
        ctx.forbid_balancer_flash,
        ctx.balancer_only,
    );
    match plan.action {
        FlashPlanAction::Reject => {
            if !ctx.start_fresh {
                return Some(FlashRejectReason::ColdCache);
            }
            if ctx.forbid_balancer_flash
                && !ctx.balancer_only
                && (!ctx.liquidity.aave_listed || ctx.liquidity.aave.is_zero())
            {
                return Some(FlashRejectReason::MixedBalancerNoAave);
            }
            if matches!(policy, FlashLoanPolicy::AaveOnly) && !ctx.liquidity.aave_listed {
                return Some(FlashRejectReason::AaveUnlisted);
            }
            Some(FlashRejectReason::ZeroLiquidity)
        }
        _ => align_flash_source_for_dispatch(
            plan.source,
            &ctx.liquidity,
            ctx.balancer_only,
            ctx.has_dodo,
            ctx.forbid_balancer_flash,
        )
        .is_none()
        .then_some(FlashRejectReason::AlignDispatch),
    }
}

/// Flash loan source at a concrete borrow size using a pre-built [`CycleFlashContext`].
#[must_use]
pub fn resolve_flash_source_with_context(
    ctx: &CycleFlashContext,
    policy: FlashLoanPolicy,
    amount_in: U256,
) -> Option<FlashLoanSource> {
    if amount_in.is_zero() {
        return None;
    }
    let plan = plan_flash_loan(
        policy,
        amount_in,
        ctx.liquidity,
        ctx.forbid_balancer_flash,
        ctx.balancer_only,
    );
    match plan.action {
        FlashPlanAction::Reject => {
            if ctx.balancer_only && ctx.forbid_balancer_flash {
                Some(FlashLoanSource::Direct)
            } else if !ctx.start_fresh {
                None
            } else {
                if ctx.start_fresh {
                    crate::debug!(
                        "flash source reject: token={} policy={policy:?} forbid_balancer={} balancer_only={} balancer={} aave={} aave_listed={} dodo={}",
                        ctx.start_addr,
                        ctx.forbid_balancer_flash,
                        ctx.balancer_only,
                        ctx.liquidity.balancer,
                        ctx.liquidity.aave,
                        ctx.liquidity.aave_listed,
                        ctx.liquidity.dodo,
                    );
                }
                None
            }
        }
        _ => align_flash_source_for_dispatch(
            plan.source,
            &ctx.liquidity,
            ctx.balancer_only,
            ctx.has_dodo,
            ctx.forbid_balancer_flash,
        ),
    }
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
    let ctx = build_cycle_flash_context(cycle, arena, flash, ttl)?;
    resolve_flash_source_with_context(&ctx, policy, amount_in)
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
    pub matic_usd: f64,
    pub matic_usd_chainlink: Option<alloy::primitives::I256>,
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
    let has_dodo = dodo_base_flash_pool_for_cycle(input.arena, &input.evaluated.cycle).is_some();
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
    let flash_cap = flash_cap_for_prepare(input, token_decimals, token_to_matic_rate);
    if plan.action != FlashPlanAction::Reject
        && flash_cap.cap_enforced_but_unresolved()
        && !amount_in.is_zero()
    {
        prepare_skip_log(
            input.log_skips,
            "prepare skip: flash borrow cap unavailable (missing token/MATIC rate or MATIC/USD)",
        );
        return None;
    }
    if plan.action != FlashPlanAction::Reject
        && let Some(cap) = flash_cap.cap_wei()
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
                false,
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
            let assessment = reuse_or_reassess(input, plan.source)?;
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
    profit_ctx.hop_count = input.evaluated.cycle.edge_hops();
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
        input.matic_usd,
        input.matic_usd_chainlink,
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
        hop_count: input.evaluated.cycle.edge_hops(),
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

#[inline]
fn flash_cap_for_prepare(
    input: &PrepareDispatchInput<'_>,
    token_decimals: u8,
    token_to_matic_rate: U256,
) -> FlashBorrowCapParams {
    FlashBorrowCapParams {
        max_flash_loan_usd: input.max_flash_loan_usd,
        token_decimals,
        token_to_matic_rate,
        matic_usd: input.matic_usd,
        matic_usd_chainlink: input.matic_usd_chainlink,
    }
}

fn dispatch_sim_passes_sanity(
    input: &PrepareDispatchInput<'_>,
    result: &crate::core::types::RouteSimulationResult,
    search_low: U256,
    token_decimals: u8,
    token_to_matic_rate: U256,
    check_flash_cap: bool,
) -> bool {
    if token_to_matic_rate < crate::core::constants::MIN_TOKEN_TO_MATIC_RATE {
        return false;
    }
    if check_flash_cap {
        let flash_cap = flash_cap_for_prepare(input, token_decimals, token_to_matic_rate);
        if !flash_cap.amount_within_cap(result.amount_in) {
            return false;
        }
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
        true,
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
        hop_count: input.evaluated.cycle.edge_hops(),
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

/// Reuse HF assessment when sim profit + flash fee + gas model still match the planned source.
fn reuse_or_reassess(
    input: &PrepareDispatchInput<'_>,
    source: FlashLoanSource,
) -> Option<ProfitAssessment> {
    // Risk haircut is applied only at reassess; never reuse under elevated risk.
    if input.risk_multiplier_bps > 10_000 {
        return reassess_route(input, source);
    }
    if let Some(existing) = input.existing_assessment.as_ref()
        && existing.should_execute
        && existing.gross_profit == input.evaluated.result.profit
        && assessment_flash_fee_matches(existing, source, input.evaluated.result.amount_in)
        // Direct vs Balancer both have 0 flash fee but different gas seeds — require gas match.
        && assessment_gas_matches(existing, input.evaluated.result.total_gas, input.gas_price)
    {
        return Some(existing.clone());
    }
    reassess_route(input, source)
}

#[inline]
fn assessment_flash_fee_matches(
    assessment: &ProfitAssessment,
    source: FlashLoanSource,
    amount_in: U256,
) -> bool {
    let fee_bps = crate::services::execution::profit::flash_loan_fee_bps(source);
    let expected = amount_in
        .checked_mul(U256::from(fee_bps))
        .map(|v| v / crate::core::constants::BPS_SCALE)
        .unwrap_or(U256::MAX);
    assessment.flash_loan_fee == expected
}

#[inline]
fn assessment_gas_matches(assessment: &ProfitAssessment, simulated_gas: u32, gas_price: U256) -> bool {
    // ponytail: gas_cost_wei = units × price; mismatch ⇒ flash/gas model drifted since HF eval.
    match U256::from(simulated_gas).checked_mul(gas_price) {
        Some(expected) => assessment.gas_cost_wei == expected,
        None => false,
    }
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

    fn dodo_cycle(
        start_token: TokenIndex,
        pool_index: crate::core::types::PoolIndex,
    ) -> FoundCycle {
        FoundCycle {
            start_token,
            edges: smallvec::smallvec![crate::core::types::Edge {
                pool_index,
                token_in: start_token,
                token_out: start_token,
                token_in_idx: 0,
                token_out_idx: 0,
                protocol: ProtocolType::Dodo,
                fee_bps: 0,
                zero_for_one: true,
            }],
            hop_count: 1,
            log_weight: 0.0,
            cumulative_fee_bps: 0,
            score: 0.0,
            cycle_ratio: U256::ZERO,
        }
    }

    #[test]
    fn dodo_flash_pool_rejects_cycle_local_swap_hop() {
        let base = Address::repeat_byte(0x01);
        let quote = Address::repeat_byte(0x02);
        let pool_address = Address::repeat_byte(0x03);
        let mut arena = StateArena::default();
        let base_index = arena.register_token(base);
        let quote_index = arena.register_token(quote);
        let pool_index = arena.register_pool(
            pool_address,
            Arc::new(PoolState::Dodo(crate::core::types::DodoPoolState {
                base_reserve: U256::from(1_000u64),
                quote_reserve: U256::from(1_000u64),
                base_token: base,
                quote_token: quote,
                base_target: U256::from(1_000u64),
                quote_target: U256::from(1_000u64),
                r_state: crate::core::types::DodoRState::One,
                i: U256::from(1u64),
                k: U256::from(1u64),
                lp_fee_rate: U256::ZERO,
                mt_fee_rate: U256::ZERO,
            })),
        );

        // Cycle-local DODO pools are swap hops; flashLoan reentrancy forbids
        // sellBase/sellQuote on the same pool during the callback.
        assert_eq!(
            dodo_base_flash_pool_for_cycle(&arena, &dodo_cycle(base_index, pool_index)),
            None
        );
        assert_eq!(
            dodo_base_flash_pool_for_cycle(&arena, &dodo_cycle(quote_index, pool_index)),
            None
        );
        assert!(dodo_pool_is_route_swap_hop(
            &arena,
            &dodo_cycle(base_index, pool_index),
            pool_address
        ));
    }

    #[test]
    fn align_dispatch_rejects_dodo_when_cycle_has_no_external_flash_pool() {
        let liquidity = TokenFlashLiquidity {
            balancer: U256::from(10_000u64),
            aave: U256::ZERO,
            aave_listed: false,
            dodo: U256::MAX,
        };
        // has_dodo=false: cycle-local only (reentrancy-incompatible).
        assert_eq!(
            align_flash_source_for_dispatch(FlashLoanSource::Dodo, &liquidity, false, false, true),
            None
        );
        assert_eq!(
            align_flash_source_for_dispatch(
                FlashLoanSource::Balancer,
                &liquidity,
                false,
                false,
                true
            ),
            None
        );
    }

    #[test]
    fn align_dispatch_rejects_aave_when_unlisted_on_mixed_route() {
        let liquidity = TokenFlashLiquidity {
            balancer: U256::from(10_000u64),
            aave: U256::ZERO,
            aave_listed: false,
            dodo: U256::MAX,
        };
        assert_eq!(
            align_flash_source_for_dispatch(
                FlashLoanSource::Balancer,
                &liquidity,
                false,
                false,
                true
            ),
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
    fn mixed_balancer_route_uses_compatible_dodo_fallback_without_aave() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::Auto,
            U256::from(1_000u64),
            TokenFlashLiquidity {
                balancer: U256::from(10_000u64),
                aave: U256::ZERO,
                aave_listed: false,
                dodo: U256::from(2_000u64),
            },
            true,
            false,
        );

        assert_eq!(plan.source, FlashLoanSource::Dodo);
        assert_eq!(plan.action, FlashPlanAction::Direct);
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
        fn hot_tokens_for_test(&self) -> Vec<Address> {
            self.hot_tokens.lock().iter().copied().collect()
        }

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
    fn track_hot_tokens_overflow_retains_current_tick() {
        let cache = FlashLiquidityCache::new();
        let mut old = Vec::with_capacity(400);
        for i in 0..400u32 {
            let mut bytes = [0u8; 20];
            bytes[..4].copy_from_slice(&i.to_be_bytes());
            old.push(Address::from(bytes));
        }
        cache.track_hot_tokens(&old);
        let new_batch: Vec<Address> = (0u8..6)
            .map(|i| {
                Address::from([
                    0xfe, i, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                ])
            })
            .collect();
        cache.track_hot_tokens(&new_batch);
        let hot = cache.hot_tokens_for_test();
        assert!(hot.len() <= MAX_HOT_FLASH_TOKENS);
        for addr in &new_batch {
            assert!(hot.contains(addr), "missing current-tick token {addr}");
        }
        let mut evicted = [0u8; 20];
        evicted[..4].copy_from_slice(&1u32.to_be_bytes());
        assert!(
            !hot.contains(&Address::from(evicted)),
            "evicted token from prior tick should not remain"
        );
    }

    #[test]
    fn refresh_inflight_single_flight() {
        let cache = FlashLiquidityCache::new();
        let g1 = cache.try_acquire_refresh_inflight();
        assert!(g1.is_some());
        assert!(cache.try_acquire_refresh_inflight().is_none());
        drop(g1);
        assert!(cache.try_acquire_refresh_inflight().is_some());
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
    fn plan_auto_selects_dodo_when_balancer_and_aave_are_empty() {
        let plan = plan_flash_loan(
            FlashLoanPolicy::Auto,
            U256::from(1_000u64),
            TokenFlashLiquidity {
                balancer: U256::ZERO,
                aave: U256::ZERO,
                aave_listed: false,
                dodo: U256::from(2_000u64),
            },
            false,
            false,
        );
        assert_eq!(plan.source, FlashLoanSource::Dodo);
        assert_eq!(plan.action, FlashPlanAction::Direct);
    }

    #[test]
    fn cycle_flash_context_reuses_liquidity_across_probe_sizes() {
        let liquidity = TokenFlashLiquidity {
            balancer: U256::from(500u64),
            aave: U256::from(10_000u64),
            aave_listed: true,
            dodo: U256::ZERO,
        };
        let ctx = CycleFlashContext {
            liquidity,
            forbid_balancer_flash: false,
            balancer_only: false,
            has_dodo: false,
            start_addr: Address::repeat_byte(0x01),
            start_fresh: true,
        };
        let economic =
            resolve_flash_source_with_context(&ctx, FlashLoanPolicy::Auto, U256::from(1_000u64));
        let again =
            resolve_flash_source_with_context(&ctx, FlashLoanPolicy::Auto, U256::from(1_000u64));
        assert_eq!(economic, Some(FlashLoanSource::AaveV3));
        assert_eq!(economic, again);
    }

    #[test]
    fn flash_reject_reason_cold_cache_when_stale() {
        let ctx = CycleFlashContext {
            liquidity: TokenFlashLiquidity::default(),
            forbid_balancer_flash: false,
            balancer_only: false,
            has_dodo: false,
            start_addr: Address::repeat_byte(0x02),
            start_fresh: false,
        };
        assert_eq!(
            flash_reject_reason(&ctx, FlashLoanPolicy::Auto, U256::from(1_000u64)),
            Some(FlashRejectReason::ColdCache)
        );
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
