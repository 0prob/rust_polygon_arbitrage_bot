use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use alloy::primitives::{Address, FixedBytes, U256};

use super::ExecutionService;
use super::route_stats::{RouteFailureKind, RouteStatsEvent};
use super::{
    ADAPTIVE_FLASH_CAP_START_DIVISOR, BATCH_QUERY_FAIL_QUARANTINE,
    CHRONIC_DUST_AVAILABLE_MATIC_WEI, CHRONIC_HIGH_COVER_BPS, CHRONIC_MID_BAND_QUARANTINE,
    CHRONIC_NEAR_MISS_COVER_BPS, CHRONIC_NEAR_MISS_QUARANTINE,
    CHRONIC_THIN_LIQ_AVAILABLE_MATIC_WEI, CHRONIC_THIN_LIQ_QUARANTINE,
    CHRONIC_UNDERWATER_COVER_BPS, CHRONIC_UNDERWATER_MIN_AVAILABLE_MATIC_WEI,
    CHRONIC_UNDERWATER_MIN_COVER_BPS, CHRONIC_UNDERWATER_QUARANTINE,
    CHRONIC_UNDERWATER_STRIKE_WINDOW, CHRONIC_UNDERWATER_STRIKES,
    DIRECT_TOKEN_ZERO_REALIZED_QUARANTINE, DRY_RUN_PASS_COOLDOWN, MAX_CONSECUTIVE_FAILURES,
    PERMANENT_QUARANTINE, PROBE_BELOW_FLOOR_QUARANTINE, ROUTE_ASSESS_CLAIM_TTL, ROUTE_COOLDOWN,
    STRUCTURAL_DRY_RUN_QUARANTINE,
};
use crate::config::AppConfig;
use crate::core::types::{CycleEdges, Edge};

impl ExecutionService {
    pub fn any_quarantined(&self, routes: &[CycleEdges]) -> bool {
        let q = self.quarantine.read();
        let now = Instant::now();
        routes
            .iter()
            .any(|edges| q.get(edges).is_some_and(|expiry| now < *expiry))
    }

    pub fn is_route_quarantined(&self, edges: &[Edge]) -> bool {
        self.any_quarantined(&[CycleEdges::from_slice(edges)])
    }

    #[inline]
    pub(crate) fn for_each_edge_rotation(edges: &[Edge], mut visit: impl FnMut(&[Edge])) {
        let _ = Self::any_edge_rotation(edges, |rotation| {
            visit(rotation);
            false
        });
    }

    #[inline]
    fn any_edge_rotation(edges: &[Edge], mut predicate: impl FnMut(&[Edge]) -> bool) -> bool {
        if edges.is_empty() {
            return false;
        }
        let mut rotated = CycleEdges::from_slice(edges);
        for _ in 0..edges.len() {
            if predicate(&rotated) {
                return true;
            }
            rotated.rotate_left(1);
        }
        false
    }

    /// True when any start-rotation of `edges` is quarantined.
    /// Underwater / stale cools call `quarantine_all_edge_rotations`; checking only the
    /// current fp lets Aave-rotated starts leak back into probe_kept (live iter13:
    /// assess in=1 quarantine=1 ×224 after single-fp re-check).
    #[must_use]
    pub fn cycle_edges_quarantined(&self, edges: &[Edge]) -> bool {
        let q = self.quarantine.read();
        let now = Instant::now();
        Self::any_edge_rotation(edges, |rotation| {
            q.get(rotation).is_some_and(|expiry| now < *expiry)
        })
    }

    /// Claim exclusive assess/Brent for this edge set (all rotations). Fails when
    /// quarantined or another tick already claimed — stops concurrent assess_q spam.
    #[must_use]
    pub fn try_claim_route_assess(&self, edges: &[Edge]) -> bool {
        if edges.is_empty() || self.cycle_edges_quarantined(edges) {
            return false;
        }
        let mut map = self.assess_inflight.write();
        let now = Instant::now();
        if Self::any_edge_rotation(edges, |rotation| {
            map.get(rotation).is_some_and(|exp| *exp > now)
        }) {
            return false;
        }
        static CLAIMS: AtomicU32 = AtomicU32::new(0);
        if CLAIMS.fetch_add(1, Ordering::Relaxed).is_multiple_of(32) {
            map.retain(|_, exp| *exp > now);
        }
        let until = now + ROUTE_ASSESS_CLAIM_TTL;
        Self::for_each_edge_rotation(edges, |rotation| {
            map.insert(CycleEdges::from_slice(rotation), until);
        });
        true
    }

    pub fn is_route_hash_quarantined(&self, route_hash: &FixedBytes<32>) -> bool {
        let q = self.route_hash_quarantine.read();
        let now = Instant::now();
        q.get(route_hash).is_some_and(|expiry| now < *expiry)
    }

    /// Insert route quarantine and amortize expired-entry prune (live: 700+ cools/run
    /// left stale fps forever → larger maps + slower `any_quarantined` on every select).
    pub(super) fn quarantine_insert(&self, edges: &[Edge], until: Instant) {
        let mut q = self.quarantine.write();
        static INSERTS: AtomicU32 = AtomicU32::new(0);
        if INSERTS.fetch_add(1, Ordering::Relaxed).is_multiple_of(32) {
            let now = Instant::now();
            q.retain(|_, exp| *exp > now);
        }
        q.insert(CycleEdges::from_slice(edges), until);
    }

    pub(super) fn route_cooldown(&self, config: &AppConfig) -> Duration {
        if config.is_dry_run() {
            DRY_RUN_PASS_COOLDOWN
        } else {
            ROUTE_COOLDOWN
        }
    }

    pub fn is_route_on_cooldown(&self, edges: &[Edge], config: &AppConfig) -> bool {
        let cooldown = self.route_cooldown(config);
        let last = self.last_submit.read();
        let now = Instant::now();
        last.get(edges)
            .is_some_and(|t| now.saturating_duration_since(*t) < cooldown)
    }

    pub(super) fn quarantine_route_hash(&self, route_hash: FixedBytes<32>, now: Instant) {
        self.route_hash_quarantine
            .write()
            .insert(route_hash, now + STRUCTURAL_DRY_RUN_QUARANTINE);
    }

    /// Suppress near-miss spam: HF ticks every ~200ms but pool-index fingerprints drift.
    pub fn should_log_near_miss(&self, _fingerprint: u64, _net_matic: U256) -> bool {
        const COOLDOWN: Duration = Duration::from_secs(30);
        let now = Instant::now();
        let mut last = self.last_near_miss_log.lock();
        if let Some(prev) = *last
            && now.duration_since(prev) < COOLDOWN
        {
            return false;
        }
        *last = Some(now);
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

    /// Log prepare skip once per fingerprint until it changes.
    pub fn should_log_prepare_skip(&self, fingerprint: u64) -> bool {
        let mut last = self.last_prepare_skip_log.lock();
        if *last == Some(fingerprint) {
            return false;
        }
        *last = Some(fingerprint);
        true
    }

    /// Longer cooldown when vault `queryBatchSwap` disagrees with local sim.
    pub fn quarantine_batch_query_failure(&self, edges: &[Edge], fingerprint: u64) {
        self.quarantine_insert(edges, Instant::now() + BATCH_QUERY_FAIL_QUARANTINE);
        self.prepare_skip_counts.write().remove(&fingerprint);
    }

    /// Soft cooldown for structurally dead routes (e.g. Balancer tokens ∉ vault).
    /// Uses `ROUTE_COOLDOWN` — batch-query's 600s emptied the HF window (live: selected=0).
    /// Never shortens an existing longer cooldown (underwater 600s was getting
    /// clobbered to 30s by rotation cools).
    pub fn quarantine_stale_route(&self, edges: &[Edge]) {
        let until = Instant::now() + ROUTE_COOLDOWN;
        let mut q = self.quarantine.write();
        if q.get(edges).is_none_or(|exp| *exp < until) {
            q.insert(CycleEdges::from_slice(edges), until);
        }
    }

    /// Cool probe-only routes that never clear the ≥1 start-token dispatch floor.
    /// Returns true when a new cool-down was applied (no refresh of an active cool).
    pub fn quarantine_probe_below_dispatch_floor(&self, edges: &[Edge]) -> bool {
        let now = Instant::now();
        let mut q = self.quarantine.write();
        if q.get(edges).is_some_and(|exp| *exp > now) {
            return false;
        }
        q.insert(
            CycleEdges::from_slice(edges),
            now + PROBE_BELOW_FLOOR_QUARANTINE,
        );
        true
    }

    /// Cool down a Direct start token after `executeArbDirect` realizes zero vs vault query.
    pub fn quarantine_direct_token_zero_realized(&self, token: Address) {
        self.direct_token_quarantine.write().insert(
            token,
            Instant::now() + DIRECT_TOKEN_ZERO_REALIZED_QUARANTINE,
        );
    }

    #[must_use]
    pub fn is_direct_token_quarantined(&self, token: Address) -> bool {
        let q = self.direct_token_quarantine.read();
        let now = Instant::now();
        q.get(&token).is_some_and(|expiry| now < *expiry)
    }

    /// True if any hop token (in or out) is in the FoT / TransferFailed cool-down.
    ///
    /// Live: hop-2 TransferFailed on Wrapped SOL quarantined the token, but probe
    /// rank only checked `start_token` (WMATIC hub) so the same routes kept dry-running.
    #[must_use]
    pub fn cycle_has_quarantined_token(
        &self,
        arena: &crate::pipeline::arena::StateArena,
        edges: &[Edge],
    ) -> bool {
        let q = self.direct_token_quarantine.read();
        if q.is_empty() {
            return false;
        }
        let now = Instant::now();
        edges.iter().any(|edge| {
            [edge.token_in, edge.token_out].into_iter().any(|ti| {
                arena
                    .token_address(ti)
                    .is_some_and(|addr| q.get(&addr).is_some_and(|expiry| now < *expiry))
            })
        })
    }

    /// Count prepare skips for diagnostics (does not quarantine — soft quarantine
    /// after 2 skips starved the HF window).
    pub fn record_prepare_skip(&self, fingerprint: u64) {
        let mut counts = self.prepare_skip_counts.write();
        *counts.entry(fingerprint).or_insert(0) += 1;
    }

    pub(super) fn quarantine_route(
        &self,
        edges: &[Edge],
        fp: u64,
        now: Instant,
        kind: RouteFailureKind,
    ) {
        self.record_route_failure(edges, fp, kind);
        // Lock order: fail_counts → quarantine (always acquire in this order).
        let count = {
            let mut fc = self.fail_counts.write();
            let count = fc.entry(CycleEdges::from_slice(edges)).or_insert(0);
            *count += 1;
            *count
        };
        let cooldown = if count >= MAX_CONSECUTIVE_FAILURES {
            PERMANENT_QUARANTINE
        } else {
            ROUTE_COOLDOWN
        };
        self.quarantine_insert(edges, now + cooldown);
    }

    pub(super) fn quarantine_route_soft(&self, edges: &[Edge], now: Instant) {
        self.quarantine_insert(edges, now + ROUTE_COOLDOWN);
    }

    /// Soft-quarantine routes that win best-eval while covering ≪ gas (dust arbs).
    /// Returns the applied TTL when a *new* cooldown was set (not on refresh / strike).
    pub fn quarantine_chronic_gas_underwater(
        &self,
        edges: &[Edge],
        gas_cover_bps: u64,
        available_matic_wei: U256,
    ) -> Option<Duration> {
        // Clamp inflated cover% into the chronic band when absolute MATIC is thin.
        // Live: USDT-start dust posted cover≈1768 with gross≈0 while real WMATIC
        // edges at ~0.017 MATIC / ~490 cover never won best-eval long enough.
        let cover_bps = if available_matic_wei
            < U256::from(CHRONIC_UNDERWATER_MIN_AVAILABLE_MATIC_WEI)
            || (available_matic_wei < U256::from(CHRONIC_THIN_LIQ_AVAILABLE_MATIC_WEI)
                && gas_cover_bps >= CHRONIC_UNDERWATER_COVER_BPS)
        {
            gas_cover_bps.min(CHRONIC_UNDERWATER_COVER_BPS.saturating_sub(1))
        } else {
            gas_cover_bps
        };
        if !(CHRONIC_UNDERWATER_MIN_COVER_BPS..CHRONIC_UNDERWATER_COVER_BPS).contains(&cover_bps) {
            return None;
        }
        let now = Instant::now();
        if self
            .quarantine
            .read()
            .get(edges)
            .is_some_and(|expiry| now < *expiry)
        {
            return None;
        }
        let strikes = {
            let mut map = self.underwater_strikes.write();
            let entry = map.entry(CycleEdges::from_slice(edges)).or_insert((0, now));
            if now.saturating_duration_since(entry.1) > CHRONIC_UNDERWATER_STRIKE_WINDOW {
                *entry = (0, now);
            }
            entry.0 = entry.0.saturating_add(1);
            entry.1 = now;
            entry.0
        };
        // First-strike: true dust / weak-cover thin, mid-band, OR moderate near-miss.
        // Near-miss (≥500 + ≥0.01): 30s sticky. Mid-band (≥500 + [0.001,0.01)): 90s
        // — live iter35 weak sticky cover~960/avail~0.009 monopolized best-eval 17×
        // vs BAL-DODO cover~3850 only 3×.
        // High-cover (≥7500 + ≥0.01): 3 strikes — almost-profitable edges (live
        // DODO×2 cover=8672) must survive base-fee noise, not die on strike-1.
        let near_miss = gas_cover_bps >= CHRONIC_NEAR_MISS_COVER_BPS
            && available_matic_wei >= U256::from(CHRONIC_DUST_AVAILABLE_MATIC_WEI);
        let high_cover_near_miss = gas_cover_bps >= CHRONIC_HIGH_COVER_BPS
            && available_matic_wei >= U256::from(CHRONIC_DUST_AVAILABLE_MATIC_WEI);
        let mid_band = gas_cover_bps >= CHRONIC_NEAR_MISS_COVER_BPS
            && available_matic_wei >= U256::from(CHRONIC_UNDERWATER_MIN_AVAILABLE_MATIC_WEI)
            && available_matic_wei < U256::from(CHRONIC_DUST_AVAILABLE_MATIC_WEI);
        let thin_first_strike = available_matic_wei < U256::from(CHRONIC_DUST_AVAILABLE_MATIC_WEI)
            || (available_matic_wei < U256::from(CHRONIC_THIN_LIQ_AVAILABLE_MATIC_WEI)
                && cover_bps < CHRONIC_NEAR_MISS_COVER_BPS);
        let strikes_needed =
            if thin_first_strike || mid_band || (near_miss && !high_cover_near_miss) {
                1
            } else {
                CHRONIC_UNDERWATER_STRIKES
            };
        if strikes < strikes_needed {
            return None;
        }
        self.underwater_strikes.write().remove(edges);
        // TTL tiers:
        //   wei-dust (<0.001 MATIC)                    → 1h
        //   near-miss (≥500 bps + ≥0.01)               → 30s
        //   mid-band (≥500 bps + [0.001, 0.01))        → 90s
        //   thin weak-cover (<500 bps / other)         → 600s
        let ttl = if available_matic_wei < U256::from(CHRONIC_UNDERWATER_MIN_AVAILABLE_MATIC_WEI) {
            CHRONIC_THIN_LIQ_QUARANTINE
        } else if near_miss {
            CHRONIC_NEAR_MISS_QUARANTINE
        } else if mid_band {
            CHRONIC_MID_BAND_QUARANTINE
        } else {
            CHRONIC_UNDERWATER_QUARANTINE
        };
        self.quarantine_insert(edges, now + ttl);
        Some(ttl)
    }

    /// Extend quarantine to `until` without shortening an existing longer cool.
    pub fn quarantine_extend_until(&self, edges: &[Edge], until: Instant) {
        let mut q = self.quarantine.write();
        if q.get(edges).is_none_or(|exp| *exp < until) {
            q.insert(CycleEdges::from_slice(edges), until);
        }
    }

    pub fn quarantine_global(&self, duration: Duration, now: Instant) {
        *self.global_quarantine_until.lock() = Some(now + duration);
    }

    pub fn global_is_quarantined(&self) -> bool {
        self.global_quarantine_until
            .lock()
            .is_some_and(|expiry| Instant::now() < expiry)
    }

    pub(super) fn clear_fail_count(&self, edges: &[Edge], fp: u64) {
        self.fail_counts.write().remove(edges);
        self.record_route_success(edges, fp);
    }

    /// Learned minimum-profit uplift. With fewer than three outcomes there is
    /// no penalty; afterwards failure probability can raise the floor to 3x.
    ///
    /// `failures` already counts every bad outcome once. Hard kinds (revert /
    /// realized loss) get **+1 weight** (2× total); timeouts get **+½**.
    pub fn route_risk_multiplier_bps(&self, edges: &[Edge]) -> u64 {
        let stats = self.route_stats.read();
        Self::risk_multiplier_bps_from_stats(stats.get(edges))
    }

    /// Single read-lock snapshot of learned risk + adaptive flash USD for the HF prepare path.
    #[must_use]
    pub fn route_learning_snapshot(&self, edges: &[Edge], configured_max_usd: u64) -> (u64, u64) {
        let stats = self.route_stats.read();
        let entry = stats.get(edges);
        let risk_bps = Self::risk_multiplier_bps_from_stats(entry);
        let flash_usd = Self::adaptive_flash_loan_usd_from_stats(entry, configured_max_usd);
        (risk_bps, flash_usd)
    }

    #[inline]
    fn risk_multiplier_bps_from_stats(stats: Option<&super::RouteStats>) -> u64 {
        let Some(stats) = stats else {
            return 10_000;
        };
        let attempts = stats.successes.saturating_add(stats.failures);
        if attempts < 3 {
            return 10_000;
        }
        let hard_extra = stats.reverts.saturating_add(stats.realized_losses);
        // Half-units: base failure = 2, hard extra = +2, timeout = +1 (+½ weight).
        let weighted_half = stats
            .failures
            .saturating_mul(2)
            .saturating_add(hard_extra.saturating_mul(2))
            .saturating_add(stats.receipt_timeouts);
        10_000u64
            .saturating_add(weighted_half.saturating_mul(10_000) / attempts)
            .min(30_000)
    }

    #[inline]
    fn adaptive_flash_cap_initial(configured_max_usd: u64) -> u64 {
        configured_max_usd.saturating_add(ADAPTIVE_FLASH_CAP_START_DIVISOR - 1)
            / ADAPTIVE_FLASH_CAP_START_DIVISOR
    }

    #[inline]
    fn adaptive_flash_loan_usd_from_stats(
        stats: Option<&super::RouteStats>,
        configured_max_usd: u64,
    ) -> u64 {
        let initial = Self::adaptive_flash_cap_initial(configured_max_usd);
        stats
            .and_then(|s| s.adaptive_flash_loan_usd)
            .unwrap_or(initial)
            .min(configured_max_usd)
    }

    #[must_use]
    pub fn adaptive_flash_loan_usd(&self, edges: &[Edge], configured_max_usd: u64) -> u64 {
        let stats = self.route_stats.read();
        Self::adaptive_flash_loan_usd_from_stats(stats.get(edges), configured_max_usd)
    }

    pub(super) fn promote_adaptive_flash_loan_cap(
        &self,
        edges: &[Edge],
        fp: u64,
        configured_max_usd: u64,
    ) -> Option<(u64, u64)> {
        let mut stats = self.route_stats.write();
        let initial = Self::adaptive_flash_cap_initial(configured_max_usd);
        let current = stats
            .get(edges)
            .and_then(|stats| stats.adaptive_flash_loan_usd)
            .unwrap_or(initial)
            .min(configured_max_usd);
        let next = current.saturating_mul(2).min(configured_max_usd);
        if next == current {
            return None;
        }
        stats
            .entry(CycleEdges::from_slice(edges))
            .or_default()
            .adaptive_flash_loan_usd = Some(next);
        drop(stats);
        self.write_route_event(edges, fp, RouteStatsEvent::AdaptiveFlashCap(next));
        Some((current, next))
    }

    /// Halve learned flash USD cap after size-bound dry-run failures (BAL#528 / flash cash).
    /// Floor is the conservative start (`configured/4`) so we do not collapse to zero.
    pub(super) fn demote_adaptive_flash_loan_cap(
        &self,
        edges: &[Edge],
        fp: u64,
        configured_max_usd: u64,
    ) -> Option<(u64, u64)> {
        let mut stats = self.route_stats.write();
        let initial = Self::adaptive_flash_cap_initial(configured_max_usd);
        let current = stats
            .get(edges)
            .and_then(|s| s.adaptive_flash_loan_usd)
            .unwrap_or(initial)
            .min(configured_max_usd);
        let next = (current / 2).max(initial).min(configured_max_usd);
        if next >= current {
            return None;
        }
        stats
            .entry(CycleEdges::from_slice(edges))
            .or_default()
            .adaptive_flash_loan_usd = Some(next);
        drop(stats);
        self.write_route_event(edges, fp, RouteStatsEvent::AdaptiveFlashCap(next));
        Some((current, next))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolIndex, ProtocolType, TokenIndex};

    fn edge(pool: u32, token_in: u32, token_out: u32) -> Edge {
        Edge {
            pool_index: PoolIndex(pool),
            token_in: TokenIndex(token_in),
            token_out: TokenIndex(token_out),
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        }
    }

    #[test]
    fn edge_rotation_visitor_preserves_order_and_empty_behavior() {
        let edges = CycleEdges::from_slice(&[edge(1, 1, 2), edge(2, 2, 3), edge(3, 3, 1)]);
        let mut rotations = Vec::new();
        ExecutionService::for_each_edge_rotation(&edges, |rotation| {
            rotations.push(CycleEdges::from_slice(rotation));
        });

        assert_eq!(rotations.len(), edges.len());
        for (offset, rotation) in rotations.iter().enumerate() {
            for (index, edge) in rotation.iter().enumerate() {
                assert_eq!(*edge, edges[(offset + index) % edges.len()]);
            }
        }

        let mut empty_visits = 0;
        ExecutionService::for_each_edge_rotation(&[], |_| empty_visits += 1);
        assert_eq!(empty_visits, 0);
    }
}
