use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use alloy::eips::BlockNumberOrTag;
use alloy::network::Ethereum;
use alloy::primitives::U256;
use alloy::providers::Provider;
use arc_swap::ArcSwapOption;
use parking_lot::Mutex;
use rustc_hash::{FxBuildHasher, FxHashMap};
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use crate::infra::rpc::RpcPool;

use super::gas::{
    FeeSnapshot, MIN_PRIORITY_FEE_PER_GAS, compute_conservative_gas_price, scaled_simulated_gas,
};

const ROUTE_GAS_HISTORY: usize = 256;
/// Cap sim→observed uplift for cold heuristics (unseen fingerprints).
/// 3.0× let BAL/mixed underestimates (live 1.87M vs 720k) train a global scale that
/// re-inflated every V2/V3 near-miss (~2× gas phantom). Per-route `record_route_gas`
/// still learns outliers; 1.5× is enough for mild hop-seed underestimates.
const MAX_SIM_SCALE_BPS: u32 = 15_000;
/// Stored global scale never goes below 1.0× (heuristic is a lower bound).
const MIN_SIM_SCALE_BPS: u32 = 10_000;
/// Floor for the *observation* blended into the EMA. Allow sub-1.0 ratios so
/// accurate dry-runs can pull a poisoned scale back toward 1.0× (prior clamp at
/// 10k left scale sticky at 1.4–1.5× forever).
const MIN_OBS_RATIO_BPS: u32 = 8_000;
/// Ratios above this are protocol/route outliers — teach fingerprint only, not global EMA.
const GLOBAL_SCALE_OUTLIER_BPS: u32 = 18_000;
const SNAPSHOT_LOG_CHANGE_BPS: u64 = 500;

/// Prefetch observed route gas for an HF tick's fingerprints (always — was gated at 48,
/// so small ticks paid a mutex per route on the hot path).
pub const ROUTE_GAS_CACHE_MIN_ROUTES: usize = 1;

/// Per-HF-tick snapshot of route gas data for lock-free lookups in `hf_eval`.
#[derive(Clone, Debug)]
pub struct RouteGasLookup {
    scale_bps: u64,
    observed: FxHashMap<u64, u32>,
    preloaded: bool,
}

impl RouteGasLookup {
    /// Build a tick-local observed-gas map for `fingerprints` (one mutex pass).
    pub fn for_fingerprints(
        oracle: &GasOracle,
        fingerprints: impl IntoIterator<Item = u64>,
    ) -> Self {
        let fps: Vec<u64> = fingerprints.into_iter().collect();
        let scale_bps = oracle.sim_scale_bps().max(10_000);
        if fps.is_empty() {
            return Self {
                scale_bps,
                observed: FxHashMap::default(),
                preloaded: true,
            };
        }
        let mut observed =
            FxHashMap::with_capacity_and_hasher(fps.len().min(ROUTE_GAS_HISTORY), FxBuildHasher);
        for fp in fps {
            if let Some(gas) = oracle.observed_route_gas(fp) {
                observed.insert(fp, gas);
            }
        }
        Self {
            scale_bps,
            observed,
            preloaded: true,
        }
    }

    #[must_use]
    pub fn scale_bps(&self) -> u64 {
        self.scale_bps
    }

    #[cfg(test)]
    pub(crate) fn preloaded(&self) -> bool {
        self.preloaded
    }

    #[cfg(test)]
    pub(crate) fn observed_gas(&self, route_fp: u64) -> Option<u32> {
        self.observed.get(&route_fp).copied()
    }

    /// Prefer prefetched observed gas; otherwise scaled heuristic (lock-free after build).
    pub fn route_gas_or_heuristic(&self, oracle: &GasOracle, route_fp: u64, heuristic: u32) -> u32 {
        if let Some(&gas) = self.observed.get(&route_fp) {
            return gas;
        }
        // Snapshot miss: fall back to live oracle map (handles empty tick fps / races).
        if !self.preloaded {
            return oracle.route_gas_or_heuristic(route_fp, heuristic);
        }
        if let Some(gas) = oracle.observed_route_gas(route_fp) {
            return gas;
        }
        scaled_simulated_gas(heuristic, self.scale_bps)
    }

    /// Observed gas if known; otherwise the seed **without** global sim_scale.
    ///
    /// Use for all-in calibrated seeds ([`crate::core::constants::balancer_direct_batch_gas`])
    /// so mixed-route underestimates cannot re-inflate Direct near-miss gas.
    #[must_use]
    pub fn route_gas_observed_or_seed(
        &self,
        oracle: &GasOracle,
        route_fp: u64,
        seed: u32,
    ) -> u32 {
        if let Some(&gas) = self.observed.get(&route_fp) {
            return gas;
        }
        if !self.preloaded {
            return oracle.route_gas_observed_or_seed(route_fp, seed);
        }
        oracle
            .observed_route_gas(route_fp)
            .unwrap_or(seed)
    }
}

/// Bounded route-fp → observed gas with FIFO eviction (≤[`ROUTE_GAS_HISTORY`]).
/// One mutex — 256 entries do not need DashMap sharding (docs warn on `iter`/`len`).
#[derive(Debug)]
struct RouteGasHistory {
    map: FxHashMap<u64, u32>,
    order: VecDeque<u64>,
}

#[derive(Debug)]
pub struct GasOracle {
    snapshot: ArcSwapOption<FeeSnapshot>,
    /// Millis from [`crate::util::now_ms`] at last successful [`GasOracle::refresh_once`].
    snapshot_updated_at_ms: AtomicU64,
    poll_interval: Duration,
    route_gas: Mutex<RouteGasHistory>,
    /// Latest observed/simulated ratio in bps (10_000 = 1.0×) for heuristic uplift.
    sim_scale_bps: AtomicU32,
}

impl Default for GasOracle {
    fn default() -> Self {
        Self::new(Duration::from_secs(1))
    }
}

impl GasOracle {
    #[must_use]
    pub fn new(poll_interval: Duration) -> Self {
        Self {
            snapshot: ArcSwapOption::empty(),
            snapshot_updated_at_ms: AtomicU64::new(0),
            poll_interval,
            route_gas: Mutex::new(RouteGasHistory {
                map: FxHashMap::with_capacity_and_hasher(ROUTE_GAS_HISTORY, FxBuildHasher),
                order: VecDeque::with_capacity(ROUTE_GAS_HISTORY),
            }),
            sim_scale_bps: AtomicU32::new(10_000),
        }
    }

    pub fn observed_route_gas(&self, route_fp: u64) -> Option<u32> {
        self.route_gas.lock().map.get(&route_fp).copied()
    }

    /// Prefer dry-run / on-chain gas for this route fingerprint, else scaled heuristic.
    pub fn route_gas_or_heuristic(&self, route_fp: u64, heuristic: u32) -> u32 {
        if let Some(observed) = self.observed_route_gas(route_fp) {
            return observed;
        }
        let scale = self.sim_scale_bps.load(Ordering::Relaxed).max(10_000) as u64;
        scaled_simulated_gas(heuristic, scale)
    }

    /// Prefer dry-run / on-chain gas; else return `seed` without global sim_scale.
    /// For live-calibrated Direct batch seeds — see [`RouteGasLookup::route_gas_observed_or_seed`].
    #[must_use]
    pub fn route_gas_observed_or_seed(&self, route_fp: u64, seed: u32) -> u32 {
        self.observed_route_gas(route_fp).unwrap_or(seed)
    }

    /// Current global sim→observed gas scale in bps (10_000 = 1.0×).
    pub fn sim_scale_bps(&self) -> u64 {
        self.sim_scale_bps.load(Ordering::Relaxed).max(10_000) as u64
    }

    /// Record on-chain or dry-run gas for a route fingerprint.
    ///
    /// Updates use mild EMA + LRU touch so hot routes are not FIFO-evicted while
    /// cold fingerprints sit at the tail (live: re-hit fps lost after 256 inserts).
    pub fn record_route_gas(&self, route_fp: u64, gas: u32) {
        if gas == 0 {
            return;
        }
        let mut hist = self.route_gas.lock();
        if let Some(entry) = hist.map.get_mut(&route_fp) {
            // 75% prior + 25% observation — dampens estimate_gas thrash.
            let blended = (u64::from(*entry).saturating_mul(3) + u64::from(gas)) / 4;
            *entry = u32::try_from(blended).unwrap_or(u32::MAX).max(1);
            // LRU: move to back so active fps survive the 256-cap.
            if let Some(pos) = hist.order.iter().position(|&fp| fp == route_fp) {
                hist.order.remove(pos);
                hist.order.push_back(route_fp);
            }
            return;
        }
        while hist.order.len() >= ROUTE_GAS_HISTORY {
            let Some(old) = hist.order.pop_front() else {
                break;
            };
            hist.map.remove(&old);
        }
        hist.map.insert(route_fp, gas);
        hist.order.push_back(route_fp);
    }

    #[cfg(test)]
    fn route_gas_tracked(&self) -> usize {
        self.route_gas.lock().order.len()
    }

    /// Calibrate heuristic gas from estimate_gas / dry-run observations.
    ///
    /// Extreme ratios (BAL/callback underestimates) are ignored for the **global**
    /// scale — those routes already get an accurate fingerprint via
    /// [`Self::record_route_gas`]. Blending them here starved V2/V3 cold edges.
    ///
    /// Sub-1.0× observations are blended (floor [`MIN_OBS_RATIO_BPS`]) so scale can
    /// decay back toward 1.0×; the stored scale still never goes below 1.0×.
    pub fn record_sim_observed(&self, simulated: u32, observed: u64) {
        if simulated == 0 || observed == 0 {
            return;
        }
        let raw_ratio_bps =
            ((observed.saturating_mul(10_000)) / u64::from(simulated)).min(u64::from(u32::MAX))
                as u32;
        if raw_ratio_bps > GLOBAL_SCALE_OUTLIER_BPS {
            return;
        }
        let ratio_bps = raw_ratio_bps.clamp(MIN_OBS_RATIO_BPS, MAX_SIM_SCALE_BPS);
        // CAS loop so concurrent dry-runs do not drop intermediate blends.
        let _ = self
            .sim_scale_bps
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |prev| {
                let prev = prev.max(MIN_SIM_SCALE_BPS);
                let blended = ((u64::from(prev) * 3 + u64::from(ratio_bps)) / 4) as u32;
                Some(blended.clamp(MIN_SIM_SCALE_BPS, MAX_SIM_SCALE_BPS))
            });
    }

    /// Single arc-swap read; prefer this over separate `snapshot` +
    /// `conservative_gas_price` in the same scope.
    pub fn loaded_snapshot(&self) -> Option<FeeSnapshot> {
        self.snapshot.load().as_deref().copied()
    }

    #[inline]
    pub fn snapshot(&self) -> Option<FeeSnapshot> {
        self.loaded_snapshot()
    }

    pub fn conservative_gas_price(&self) -> Option<U256> {
        self.loaded_snapshot().map(compute_conservative_gas_price)
    }

    /// Conservative gas price only when the fee snapshot is fresh enough for live submit.
    #[must_use]
    pub fn conservative_gas_price_for_live_submit(&self) -> Option<U256> {
        // One ArcSwap load — age check is a separate atomic, not a second snapshot read.
        let fees = self.loaded_snapshot()?;
        let age_ok = self
            .snapshot_age_ms()
            .is_some_and(|age| age <= self.live_snapshot_max_age_ms());
        age_ok.then(|| compute_conservative_gas_price(fees))
    }

    #[must_use]
    pub fn snapshot_age_ms(&self) -> Option<u64> {
        let updated = self.snapshot_updated_at_ms.load(Ordering::Relaxed);
        if updated == 0 {
            return None;
        }
        Some(crate::util::now_ms().saturating_sub(updated))
    }

    fn live_snapshot_max_age_ms(&self) -> u64 {
        self.poll_interval.as_millis().saturating_mul(5).max(15_000) as u64
    }

    /// Live submit requires a fee snapshot refreshed within [`Self::live_snapshot_max_age_ms`].
    #[must_use]
    pub fn fees_ready_for_live_submit(&self) -> bool {
        self.loaded_snapshot().is_some()
            && self
                .snapshot_age_ms()
                .is_some_and(|age| age <= self.live_snapshot_max_age_ms())
    }

    #[cfg(test)]
    pub(crate) fn set_fee_snapshot_for_test(&self, fees: FeeSnapshot) {
        self.snapshot.store(Some(Arc::new(fees)));
        self.snapshot_updated_at_ms
            .store(crate::util::now_ms(), Ordering::Relaxed);
    }

    pub async fn refresh_once<P: Provider<Ethereum>>(&self, provider: &P) -> anyhow::Result<()> {
        // One RTT instead of two sequential — tip fetch does not depend on the block body.
        let (block_res, tip_res) = tokio::join!(
            provider.get_block_by_number(BlockNumberOrTag::Latest),
            provider.get_max_priority_fee_per_gas(),
        );

        let block = block_res?.ok_or_else(|| anyhow::anyhow!("latest block unavailable"))?;

        let base_fee = block
            .header
            .base_fee_per_gas
            .map(U256::from)
            .ok_or_else(|| anyhow::anyhow!("block header missing base_fee_per_gas"))?;

        // Single load for tip fallback + change detection (consistent snapshot pattern).
        let previous = self.loaded_snapshot();
        let (mut priority_fee, mut priority_fee_source) = match tip_res {
            Ok(v) => (U256::from(v), "rpc"),
            Err(_e) => (
                previous.map_or(U256::ZERO, |snap| snap.priority_fee),
                "previous_snapshot",
            ),
        };
        if priority_fee.is_zero() {
            priority_fee = MIN_PRIORITY_FEE_PER_GAS;
            priority_fee_source = "fallback_min_priority";
        }
        // Clamp so assess/rank and submit share the same tip floor.
        priority_fee = priority_fee.max(MIN_PRIORITY_FEE_PER_GAS);

        let snapshot = FeeSnapshot {
            base_fee,
            priority_fee,
        };
        let is_initial_snapshot = previous.is_none();
        self.snapshot.store(Some(Arc::new(snapshot)));
        // ponytail: stale tip must not refresh the live-submit age clock.
        if priority_fee_source != "previous_snapshot" || is_initial_snapshot {
            self.snapshot_updated_at_ms
                .store(crate::util::now_ms(), Ordering::Relaxed);
        }
        if is_initial_snapshot {
            crate::info!(
                "gas oracle initialized: base_fee_wei={} priority_fee_wei={} priority_fee_source={} conservative_gas_price_wei={}",
                snapshot.base_fee,
                snapshot.priority_fee,
                priority_fee_source,
                compute_conservative_gas_price(snapshot),
            );
        } else if priority_fee_source != "rpc"
            || previous.is_some_and(|prior| snapshot_changed_materially(prior, snapshot))
        {
            crate::info!(
                "gas oracle update: base_fee_wei={} priority_fee_wei={} priority_fee_source={} conservative_gas_price_wei={}",
                snapshot.base_fee,
                snapshot.priority_fee,
                priority_fee_source,
                compute_conservative_gas_price(snapshot),
            );
        }
        Ok(())
    }

    async fn refresh_from_state_pool(&self, rpc: &RpcPool) -> anyhow::Result<()> {
        let candidates = rpc.state_url_candidates();
        anyhow::ensure!(
            !candidates.is_empty(),
            "no state RPC configured for gas oracle"
        );
        let mut last_error = None;

        for (idx, url) in candidates.iter().enumerate() {
            let provider = match rpc.connect_state_at(url) {
                Ok(provider) => provider,
                Err(error) => {
                    rpc.deprioritize_state_url(url);
                    last_error = Some(error);
                    continue;
                }
            };
            match self.refresh_once(&provider).await {
                Ok(()) => {
                    if idx > 0 {
                        crate::info!("gas oracle fallback refresh succeeded (url_index={idx})");
                    }
                    return Ok(());
                }
                Err(error) => {
                    rpc.deprioritize_state_url(url);
                    last_error = Some(error);
                }
            }
        }

        match last_error {
            Some(error) => Err(error),
            None => Err(anyhow::anyhow!("gas oracle exhausted state RPC candidates")),
        }
    }

    pub fn start_background(
        self: Arc<Self>,
        rpc: Arc<RpcPool>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let poll = self.poll_interval;
        tokio::spawn(async move {
            static REFRESH_FAILS: AtomicU32 = AtomicU32::new(0);
            if let Err(e) = self.refresh_from_state_pool(&rpc).await {
                let n = REFRESH_FAILS.fetch_add(1, Ordering::Relaxed) + 1;
                crate::warn!("gas oracle initial refresh failed ({n}): {e:#}");
            } else {
                REFRESH_FAILS.store(0, Ordering::Relaxed);
            }
            let mut ticker = tokio::time::interval(poll);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            // interval fires immediately — discard so we don't double-hit after initial refresh.
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        if let Err(e) = self.refresh_from_state_pool(&rpc).await {
                            let n = REFRESH_FAILS.fetch_add(1, Ordering::Relaxed) + 1;
                            if n == 1 || n.is_multiple_of(20) {
                                crate::warn!(
                                    "gas oracle refresh failed ({n} consecutive): {e:#}"
                                );
                            }
                        } else {
                            REFRESH_FAILS.store(0, Ordering::Relaxed);
                        }
                    }
                }
            }
        });
    }
}

fn snapshot_changed_materially(previous: FeeSnapshot, current: FeeSnapshot) -> bool {
    fee_changed_by_at_least(previous.base_fee, current.base_fee)
        || fee_changed_by_at_least(previous.priority_fee, current.priority_fee)
}

fn fee_changed_by_at_least(previous: U256, current: U256) -> bool {
    if previous.is_zero() {
        return !current.is_zero();
    }
    let delta = if current >= previous {
        current - previous
    } else {
        previous - current
    };
    delta >= previous.saturating_mul(U256::from(SNAPSHOT_LOG_CHANGE_BPS)) / U256::from(10_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_gas_lookup_prefetches_observed_gas() {
        let oracle = GasOracle::new(Duration::from_secs(1));
        oracle.record_route_gas(7, 500_000);
        let lookup = RouteGasLookup::for_fingerprints(&oracle, [7u64, 8, 9]);
        assert!(lookup.preloaded());
        assert_eq!(lookup.observed_gas(7), Some(500_000));
        assert_eq!(lookup.route_gas_or_heuristic(&oracle, 7, 100_000), 500_000);
        assert_eq!(lookup.route_gas_or_heuristic(&oracle, 99, 100_000), 100_000);
    }

    #[test]
    fn gas_scaling_saturates_instead_of_wrapping() {
        assert_eq!(scaled_simulated_gas(100_000, 25_000), 250_000);
        assert_eq!(scaled_simulated_gas(u32::MAX, u64::MAX), u32::MAX);
    }

    #[test]
    fn sim_scale_caps_mild_uplift_and_ignores_outliers() {
        let oracle = GasOracle::new(Duration::from_secs(1));
        // 10× outlier must not train global scale (fingerprint-only).
        for _ in 0..32 {
            oracle.record_sim_observed(100_000, 1_000_000);
        }
        assert_eq!(oracle.sim_scale_bps(), 10_000);
        // Mild 1.4× observations blend toward the 1.5× cap.
        for _ in 0..32 {
            oracle.record_sim_observed(100_000, 140_000);
        }
        let scale = oracle.sim_scale_bps();
        assert!(scale <= u64::from(MAX_SIM_SCALE_BPS));
        assert!(
            scale >= 13_500,
            "expected mild uplift toward 1.4×, got {scale}"
        );
    }

    #[test]
    fn calibrated_seed_ignores_global_sim_scale() {
        let oracle = GasOracle::new(Duration::from_secs(1));
        // Mild underestimates train global scale (1.5× obs → 1.125× blend).
        oracle.record_sim_observed(100_000, 150_000);
        assert_eq!(oracle.sim_scale_bps(), 11_250);
        let direct_seed = 220_000u32; // balancer_direct_batch_gas(2)
        // Scaled path re-inflates; calibrated path must not.
        assert_eq!(oracle.route_gas_or_heuristic(99, direct_seed), 247_500);
        assert_eq!(
            oracle.route_gas_observed_or_seed(99, direct_seed),
            direct_seed
        );
        let lookup = RouteGasLookup::for_fingerprints(&oracle, [1u64]);
        assert_eq!(
            lookup.route_gas_observed_or_seed(&oracle, 99, direct_seed),
            direct_seed
        );
        // Observed still wins over seed.
        oracle.record_route_gas(7, 210_000);
        assert_eq!(oracle.route_gas_observed_or_seed(7, direct_seed), 210_000);
    }

    #[test]
    fn mined_gas_calibrates_heuristic_uplift() {
        let oracle = GasOracle::new(Duration::from_secs(1));
        oracle.record_sim_observed(100_000, 150_000);

        // 75% prior (1.0x) + 25% observation (1.5x) → 1.125×.
        assert_eq!(oracle.sim_scale_bps(), 11_250);
        assert_eq!(oracle.route_gas_or_heuristic(99, 100_000), 112_500);
    }

    #[test]
    fn route_gas_lookup_prefetches_even_small_batches() {
        let oracle = GasOracle::new(Duration::from_secs(1));
        oracle.record_route_gas(1, 250_000);
        let lookup = RouteGasLookup::for_fingerprints(&oracle, [1u64, 2]);
        assert!(lookup.preloaded());
        assert_eq!(lookup.observed_gas(1), Some(250_000));
        assert_eq!(lookup.route_gas_or_heuristic(&oracle, 1, 100_000), 250_000);
    }

    #[test]
    fn storage_gas_first_touch_premium_not_double_count() {
        use crate::services::execution::support::estimate_route_storage_gas;
        // 2 hops, both cold: 2*4*100 warm base + 2*(2100-100) premium = 800 + 4000 = 4800
        assert_eq!(estimate_route_storage_gas(2, 2), 4_800);
        // 2 hops, 0 cold: pure warm
        assert_eq!(estimate_route_storage_gas(2, 0), 800);
    }

    #[test]
    fn route_gas_history_is_bounded_and_updates_existing_fingerprint() {
        let oracle = GasOracle::new(Duration::from_secs(1));
        for fp in 0..ROUTE_GAS_HISTORY as u64 {
            oracle.record_route_gas(fp, 100 + fp as u32);
        }
        assert_eq!(oracle.route_gas_tracked(), ROUTE_GAS_HISTORY);
        // Touch fp=0: EMA + LRU refresh (not last-write only, not FIFO-sticky).
        oracle.record_route_gas(0, 9_999);
        let after_touch = oracle.observed_route_gas(0).expect("hot fp");
        assert!(
            after_touch > 100 && after_touch < 9_999,
            "EMA blend prior+obs, got {after_touch}"
        );
        oracle.record_route_gas(ROUTE_GAS_HISTORY as u64, 42);
        assert!(oracle.route_gas_tracked() <= ROUTE_GAS_HISTORY);
        assert_eq!(
            oracle.observed_route_gas(ROUTE_GAS_HISTORY as u64),
            Some(42)
        );
        // Hot fp=0 must survive eviction of the FIFO head (was: update left order[0]=0).
        assert_eq!(
            oracle.observed_route_gas(0),
            Some(after_touch),
            "LRU touch must keep active fingerprint under cap pressure"
        );
        // Oldest untouched (fp=1) is the eviction victim.
        assert!(oracle.observed_route_gas(1).is_none());
    }

    #[test]
    fn sim_scale_decays_toward_one_on_accurate_observations() {
        let oracle = GasOracle::new(Duration::from_secs(1));
        // Train up with mild underestimates.
        for _ in 0..16 {
            oracle.record_sim_observed(100_000, 140_000);
        }
        let elevated = oracle.sim_scale_bps();
        assert!(elevated > 12_000, "precondition elevated scale, got {elevated}");
        // Accurate dry-runs (obs ≈ sim) must pull scale down (not sticky at 1.4×).
        for _ in 0..32 {
            oracle.record_sim_observed(100_000, 100_000);
        }
        let cooled = oracle.sim_scale_bps();
        assert!(
            cooled < elevated,
            "scale should decay: elevated={elevated} cooled={cooled}"
        );
        assert!(cooled >= 10_000);
    }

    #[test]
    fn arc_swap_option_stores_and_loads_fee_snapshot() {
        let oracle = GasOracle::new(Duration::from_secs(1));
        assert!(oracle.snapshot().is_none());

        let fees = FeeSnapshot {
            base_fee: U256::from(100u64),
            priority_fee: U256::from(2u64),
        };
        oracle.set_fee_snapshot_for_test(fees);

        let loaded = oracle.loaded_snapshot().expect("fees stored");
        assert_eq!(loaded.base_fee, fees.base_fee);
        assert_eq!(loaded.priority_fee, fees.priority_fee);
        assert_eq!(
            oracle.conservative_gas_price(),
            Some(compute_conservative_gas_price(fees))
        );
    }

    #[test]
    fn fees_ready_for_live_submit_requires_fresh_timestamp() {
        let oracle = GasOracle::new(Duration::from_secs(1));
        assert!(!oracle.fees_ready_for_live_submit());

        oracle.set_fee_snapshot_for_test(FeeSnapshot {
            base_fee: U256::from(100u64),
            priority_fee: U256::from(2u64),
        });
        assert!(oracle.fees_ready_for_live_submit());
        assert!(oracle.conservative_gas_price_for_live_submit().is_some());

        oracle.snapshot_updated_at_ms.store(
            crate::util::now_ms().saturating_sub(60_000),
            Ordering::Relaxed,
        );
        assert!(!oracle.fees_ready_for_live_submit());
        assert!(oracle.conservative_gas_price_for_live_submit().is_none());
    }

    #[test]
    fn stale_priority_reuse_does_not_refresh_live_submit_age() {
        let oracle = GasOracle::new(Duration::from_secs(1));
        oracle.set_fee_snapshot_for_test(FeeSnapshot {
            base_fee: U256::from(100u64),
            priority_fee: MIN_PRIORITY_FEE_PER_GAS,
        });
        let aged = crate::util::now_ms().saturating_sub(60_000);
        oracle.snapshot_updated_at_ms.store(aged, Ordering::Relaxed);
        assert!(!oracle.fees_ready_for_live_submit());

        // Simulate refresh_once path that keeps previous tip (RPC tip miss).
        let previous = oracle.loaded_snapshot().expect("snapshot");
        let snapshot = FeeSnapshot {
            base_fee: U256::from(110u64),
            priority_fee: previous.priority_fee.max(MIN_PRIORITY_FEE_PER_GAS),
        };
        oracle.snapshot.store(Some(Arc::new(snapshot)));
        // Age clock intentionally not bumped when tip source is previous_snapshot.
        assert_eq!(oracle.snapshot_updated_at_ms.load(Ordering::Relaxed), aged);
        assert!(!oracle.fees_ready_for_live_submit());
        let loaded = oracle.loaded_snapshot().expect("updated base");
        assert_eq!(loaded.base_fee, U256::from(110u64));
    }

    #[test]
    fn snapshot_logging_ignores_small_fee_moves() {
        let previous = FeeSnapshot {
            base_fee: U256::from(200u64),
            priority_fee: U256::from(30u64),
        };
        assert!(!snapshot_changed_materially(
            previous,
            FeeSnapshot {
                base_fee: U256::from(209u64),
                priority_fee: U256::from(30u64),
            }
        ));
        assert!(snapshot_changed_materially(
            previous,
            FeeSnapshot {
                base_fee: U256::from(210u64),
                priority_fee: U256::from(30u64),
            }
        ));
    }
}
