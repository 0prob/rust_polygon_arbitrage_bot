use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use alloy::primitives::{Address, FixedBytes, U256};

use super::ExecutionService;
use super::route_stats::RouteFailureKind;
use super::{
    ADAPTIVE_FLASH_CAP_START_DIVISOR, BATCH_QUERY_FAIL_QUARANTINE,
    CHRONIC_THIN_LIQ_AVAILABLE_MATIC_WEI, CHRONIC_THIN_LIQ_QUARANTINE,
    CHRONIC_UNDERWATER_COVER_BPS, CHRONIC_UNDERWATER_MIN_AVAILABLE_MATIC_WEI,
    CHRONIC_UNDERWATER_MIN_COVER_BPS, CHRONIC_UNDERWATER_QUARANTINE, CHRONIC_UNDERWATER_STRIKES,
    CHRONIC_UNDERWATER_STRIKE_WINDOW, DIRECT_TOKEN_ZERO_REALIZED_QUARANTINE,
    DRY_RUN_PASS_COOLDOWN, MAX_CONSECUTIVE_FAILURES, PERMANENT_QUARANTINE,
    PROBE_BELOW_FLOOR_QUARANTINE, ROUTE_ASSESS_CLAIM_TTL, ROUTE_COOLDOWN,
    STRUCTURAL_DRY_RUN_QUARANTINE,
};
use crate::config::AppConfig;

impl ExecutionService {
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

    /// True when any start-rotation of `edges` is quarantined.
    /// Underwater / stale cools call `quarantine_all_edge_rotations`; checking only the
    /// current fp lets Aave-rotated starts leak back into probe_kept (live iter13:
    /// assess in=1 quarantine=1 ×224 after single-fp re-check).
    #[must_use]
    pub fn cycle_edges_quarantined(&self, edges: &[crate::core::types::Edge]) -> bool {
        let fps = Self::edge_rotation_fps(edges);
        !fps.is_empty() && self.any_quarantined(&fps)
    }

    fn edge_rotation_fps(
        edges: &[crate::core::types::Edge],
    ) -> smallvec::SmallVec<[u64; crate::core::constants::HOP_CAP_USIZE]> {
        let n = edges.len();
        let mut fps =
            smallvec::SmallVec::<[u64; crate::core::constants::HOP_CAP_USIZE]>::with_capacity(n);
        if n == 0 {
            return fps;
        }
        let mut rotated = crate::core::types::CycleEdges::from_slice(edges);
        for _ in 0..n {
            fps.push(super::super::candidate::hash_cycle_edges(&rotated));
            rotated.rotate_left(1);
        }
        fps
    }

    /// Claim exclusive assess/Brent for this edge set (all rotations). Fails when
    /// quarantined or another tick already claimed — stops concurrent assess_q spam.
    #[must_use]
    pub fn try_claim_route_assess(&self, edges: &[crate::core::types::Edge]) -> bool {
        let fps = Self::edge_rotation_fps(edges);
        if fps.is_empty() || self.any_quarantined(&fps) {
            return false;
        }
        let mut map = self.assess_inflight.write();
        let now = Instant::now();
        if fps
            .iter()
            .any(|fp| map.get(fp).is_some_and(|exp| *exp > now))
        {
            return false;
        }
        static CLAIMS: AtomicU32 = AtomicU32::new(0);
        if CLAIMS.fetch_add(1, Ordering::Relaxed).is_multiple_of(32) {
            map.retain(|_, exp| *exp > now);
        }
        let until = now + ROUTE_ASSESS_CLAIM_TTL;
        for fp in fps {
            map.insert(fp, until);
        }
        true
    }

    pub fn is_route_hash_quarantined(&self, route_hash: &FixedBytes<32>) -> bool {
        let q = self.route_hash_quarantine.read();
        let now = Instant::now();
        q.get(route_hash).is_some_and(|expiry| now < *expiry)
    }

    /// Insert route quarantine and amortize expired-entry prune (live: 700+ cools/run
    /// left stale fps forever → larger maps + slower `any_quarantined` on every select).
    pub(super) fn quarantine_insert(&self, fingerprint: u64, until: Instant) {
        let mut q = self.quarantine.write();
        static INSERTS: AtomicU32 = AtomicU32::new(0);
        if INSERTS.fetch_add(1, Ordering::Relaxed).is_multiple_of(32) {
            let now = Instant::now();
            q.retain(|_, exp| *exp > now);
        }
        q.insert(fingerprint, until);
    }

    pub(super) fn route_cooldown(&self, config: &AppConfig) -> Duration {
        if config.is_dry_run() {
            DRY_RUN_PASS_COOLDOWN
        } else {
            ROUTE_COOLDOWN
        }
    }

    pub fn is_route_on_cooldown(&self, fingerprint: u64, config: &AppConfig) -> bool {
        let cooldown = self.route_cooldown(config);
        let last = self.last_submit.read();
        let now = Instant::now();
        last.get(&fingerprint)
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
    pub fn quarantine_batch_query_failure(&self, fingerprint: u64) {
        self.quarantine_insert(fingerprint, Instant::now() + BATCH_QUERY_FAIL_QUARANTINE);
        self.prepare_skip_counts.write().remove(&fingerprint);
    }

    /// Soft cooldown for structurally dead routes (e.g. Balancer tokens ∉ vault).
    /// Uses `ROUTE_COOLDOWN` — batch-query's 600s emptied the HF window (live: selected=0).
    /// Never shortens an existing longer cooldown (underwater 600s was getting
    /// clobbered to 30s by rotation cools).
    pub fn quarantine_stale_route(&self, fingerprint: u64) {
        let until = Instant::now() + ROUTE_COOLDOWN;
        let mut q = self.quarantine.write();
        if q.get(&fingerprint).is_none_or(|exp| *exp < until) {
            q.insert(fingerprint, until);
        }
    }

    /// Cool probe-only routes that never clear the ≥1 start-token dispatch floor.
    /// Returns true when a new cool-down was applied (no refresh of an active cool).
    pub fn quarantine_probe_below_dispatch_floor(&self, fingerprint: u64) -> bool {
        let now = Instant::now();
        let mut q = self.quarantine.write();
        if q.get(&fingerprint).is_some_and(|exp| *exp > now) {
            return false;
        }
        q.insert(fingerprint, now + PROBE_BELOW_FLOOR_QUARANTINE);
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
        edges: &[crate::core::types::Edge],
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

    pub(super) fn quarantine_route(&self, fp: u64, now: Instant, kind: RouteFailureKind) {
        self.record_route_failure(fp, kind);
        // Lock order: fail_counts → quarantine (always acquire in this order).
        let count = {
            let mut fc = self.fail_counts.write();
            let count = fc.entry(fp).or_insert(0);
            *count += 1;
            *count
        };
        let cooldown = if count >= MAX_CONSECUTIVE_FAILURES {
            PERMANENT_QUARANTINE
        } else {
            ROUTE_COOLDOWN
        };
        self.quarantine_insert(fp, now + cooldown);
    }

    pub(super) fn quarantine_route_soft(&self, fp: u64, now: Instant) {
        self.quarantine_insert(fp, now + ROUTE_COOLDOWN);
    }

    /// Soft-quarantine routes that win best-eval while covering ≪ gas (dust arbs).
    /// Returns true only when a *new* cooldown was applied (not on refresh / first strike).
    pub fn quarantine_chronic_gas_underwater(
        &self,
        fingerprint: u64,
        gas_cover_bps: u64,
        available_matic_wei: U256,
    ) -> bool {
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
            return false;
        }
        let now = Instant::now();
        if self
            .quarantine
            .read()
            .get(&fingerprint)
            .is_some_and(|expiry| now < *expiry)
        {
            return false;
        }
        let strikes = {
            let mut map = self.underwater_strikes.write();
            let entry = map.entry(fingerprint).or_insert((0, now));
            if now.saturating_duration_since(entry.1) > CHRONIC_UNDERWATER_STRIKE_WINDOW {
                *entry = (0, now);
            }
            entry.0 = entry.0.saturating_add(1);
            entry.1 = now;
            entry.0
        };
        // ponytail: thin-liq (<0.05 MATIC) cools on first strike — sticky V2 dust
        // burned 3 best-eval ticks every cold start before the 1h cool applied.
        let strikes_needed =
            if available_matic_wei < U256::from(CHRONIC_THIN_LIQ_AVAILABLE_MATIC_WEI) {
                1
            } else {
                CHRONIC_UNDERWATER_STRIKES
            };
        if strikes < strikes_needed {
            return false;
        }
        self.underwater_strikes.write().remove(&fingerprint);
        let ttl = if available_matic_wei < U256::from(CHRONIC_THIN_LIQ_AVAILABLE_MATIC_WEI) {
            CHRONIC_THIN_LIQ_QUARANTINE
        } else {
            CHRONIC_UNDERWATER_QUARANTINE
        };
        self.quarantine_insert(fingerprint, now + ttl);
        true
    }

    pub fn quarantine_global(&self, duration: Duration, now: Instant) {
        *self.global_quarantine_until.lock() = Some(now + duration);
    }

    pub fn global_is_quarantined(&self) -> bool {
        self.global_quarantine_until
            .lock()
            .is_some_and(|expiry| Instant::now() < expiry)
    }

    pub(super) fn clear_fail_count(&self, fp: u64) {
        self.fail_counts.write().remove(&fp);
        self.record_route_success(fp);
    }

    /// Learned minimum-profit uplift. With fewer than three outcomes there is
    /// no penalty; afterwards failure probability can raise the floor to 3x.
    ///
    /// `failures` already counts every bad outcome once. Hard kinds (revert /
    /// realized loss) get **+1 weight** (2× total); timeouts get **+½**.
    pub fn route_risk_multiplier_bps(&self, fp: u64) -> u64 {
        let stats = self.route_stats.read();
        let Some(stats) = stats.get(&fp) else {
            return 10_000;
        };
        let attempts = stats.successes.saturating_add(stats.failures);
        if attempts < 3 {
            return 10_000;
        }
        let hard_extra = stats.reverts.saturating_add(stats.realized_losses);
        let weighted_failures = stats
            .failures
            .saturating_add(hard_extra)
            .saturating_add(stats.receipt_timeouts / 2);
        10_000u64
            .saturating_add(weighted_failures.saturating_mul(20_000) / attempts)
            .min(30_000)
    }

    #[inline]
    fn adaptive_flash_cap_initial(configured_max_usd: u64) -> u64 {
        configured_max_usd.saturating_add(ADAPTIVE_FLASH_CAP_START_DIVISOR - 1)
            / ADAPTIVE_FLASH_CAP_START_DIVISOR
    }

    #[must_use]
    pub fn adaptive_flash_loan_usd(&self, fp: u64, configured_max_usd: u64) -> u64 {
        let initial = Self::adaptive_flash_cap_initial(configured_max_usd);
        self.route_stats
            .read()
            .get(&fp)
            .and_then(|stats| stats.adaptive_flash_loan_usd)
            .unwrap_or(initial)
            .min(configured_max_usd)
    }

    pub(super) fn promote_adaptive_flash_loan_cap(
        &self,
        fp: u64,
        configured_max_usd: u64,
    ) -> Option<(u64, u64)> {
        let mut stats = self.route_stats.write();
        let initial = Self::adaptive_flash_cap_initial(configured_max_usd);
        let current = stats
            .get(&fp)
            .and_then(|stats| stats.adaptive_flash_loan_usd)
            .unwrap_or(initial)
            .min(configured_max_usd);
        let next = current.saturating_mul(2).min(configured_max_usd);
        if next == current {
            return None;
        }
        stats.entry(fp).or_default().adaptive_flash_loan_usd = Some(next);
        drop(stats);
        self.write_route_event(format!("{fp} c {next}"));
        Some((current, next))
    }

    /// Halve learned flash USD cap after size-bound dry-run failures (BAL#528 / flash cash).
    /// Floor is the conservative start (`configured/4`) so we do not collapse to zero.
    pub(super) fn demote_adaptive_flash_loan_cap(
        &self,
        fp: u64,
        configured_max_usd: u64,
    ) -> Option<(u64, u64)> {
        let mut stats = self.route_stats.write();
        let initial = Self::adaptive_flash_cap_initial(configured_max_usd);
        let current = stats
            .get(&fp)
            .and_then(|s| s.adaptive_flash_loan_usd)
            .unwrap_or(initial)
            .min(configured_max_usd);
        let next = (current / 2).max(initial).min(configured_max_usd);
        if next >= current {
            return None;
        }
        stats.entry(fp).or_default().adaptive_flash_loan_usd = Some(next);
        drop(stats);
        self.write_route_event(format!("{fp} c {next}"));
        Some((current, next))
    }
}
