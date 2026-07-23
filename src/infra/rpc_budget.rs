//! Per-host HTTP request budgets so free-tier RPCs stay under published caps.
//!
//! Live: PublicNode/Allnodes `1200rqs/60s` and dRPC free "error code 15" when the
//! latency-ranked primary was hammered with multicall fan-out. Token buckets pace
//! admits before each RPC round-trip; 429s temporarily tighten the refill rate.

use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use tokio::task_local;

task_local! {
    /// Active state/execution URL for the current async task (and scoped children).
    static RPC_BUDGET_URL: Arc<str>;
}

static HOST_BUDGETS: LazyLock<Mutex<FxHashMap<String, Arc<Mutex<TokenBucket>>>>> =
    LazyLock::new(|| Mutex::new(FxHashMap::default()));

/// Run `fut` with `url` as the admit key for nested multicall chunks.
pub async fn scope_rpc_budget<F>(url: &str, fut: F) -> F::Output
where
    F: std::future::Future,
{
    scope_rpc_budget_arc(Arc::<str>::from(url), fut).await
}

/// Like [`scope_rpc_budget`] but reuses an existing `Arc<str>` (JoinSet re-scope).
pub async fn scope_rpc_budget_arc<F>(url: Arc<str>, fut: F) -> F::Output
where
    F: std::future::Future,
{
    RPC_BUDGET_URL.scope(url, fut).await
}

#[must_use]
pub fn current_budget_url() -> Option<Arc<str>> {
    RPC_BUDGET_URL.try_with(Arc::clone).ok()
}

/// Block until this task's scoped URL (if any) has a request token.
pub async fn admit_rpc_request() {
    let Some(url) = current_budget_url() else {
        return;
    };
    admit_url(&url).await;
}

pub async fn admit_url(url: &str) {
    let bucket = bucket_for_url(url);
    loop {
        let wait = {
            let mut b = bucket.lock();
            b.try_take()
        };
        match wait {
            None => return,
            Some(delay) => tokio::time::sleep(delay).await,
        }
    }
}

/// After a 429 / free-plan limit: cool the host and cut refill for a while.
pub fn note_rate_limited(url: &str) {
    let bucket = bucket_for_url(url);
    let mut b = bucket.lock();
    b.punish();
    crate::debug!(
        "rpc budget punished ({}) refill={:.1}/s",
        crate::infra::rpc::rpc_host_label(url),
        b.refill_per_sec
    );
}

/// Prefer hosts that still have tokens when choosing among healthy endpoints.
#[must_use]
pub fn approx_tokens(url: &str) -> f64 {
    let bucket = bucket_for_url(url);
    let mut b = bucket.lock();
    b.refill();
    b.tokens
}

/// Floor for `RPC_BATCH_PACE_MS` so free tiers cannot burst below their RPS cap.
#[must_use]
pub fn min_batch_pace_ms(url: &str) -> u64 {
    match host_tier(url) {
        HostTier::FreePublic => 55, // ≤~18 rps headroom under Allnodes 20 rps
        HostTier::Drpc => 45,
        HostTier::Paid => 0,
    }
}

#[must_use]
pub fn effective_batch_pace_ms(url: &str, configured: u64) -> u64 {
    configured.max(min_batch_pace_ms(url))
}

/// Sort key: paid first, then dRPC, then free public (stable with latency sort).
#[must_use]
pub fn host_rank_class(url: &str) -> u8 {
    match host_tier(url) {
        HostTier::Paid => 0,
        HostTier::Drpc => 1,
        HostTier::FreePublic => 2,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HostTier {
    Paid,
    Drpc,
    FreePublic,
}

fn host_tier(url: &str) -> HostTier {
    let host = crate::infra::rpc::rpc_host_label(url).to_ascii_lowercase();
    if host.contains("publicnode") || host.contains("allnodes") {
        HostTier::FreePublic
    } else if host.contains("drpc") {
        HostTier::Drpc
    } else if host.contains("alchemy")
        || host.contains("ankr.com")
        || host.contains("chainstack")
        || host.contains("quicknode")
        || host.contains("infura")
    {
        HostTier::Paid
    } else {
        // Unknown public endpoints: treat like free.
        HostTier::FreePublic
    }
}

fn default_rps(url: &str) -> f64 {
    match host_tier(url) {
        // Allnodes free: 1200/60s = 20 rps — stay under with headroom for WSS/other.
        HostTier::FreePublic => 16.0,
        // dRPC free plan trips "error code 15" under LF+HF fan-out.
        HostTier::Drpc => 20.0,
        HostTier::Paid => 40.0,
    }
}

fn bucket_for_url(url: &str) -> Arc<Mutex<TokenBucket>> {
    let host = crate::infra::rpc::rpc_host_label(url);
    let mut map = HOST_BUDGETS.lock();
    map.entry(host)
        .or_insert_with(|| {
            let rps = default_rps(url);
            Arc::new(Mutex::new(TokenBucket::new(rps)))
        })
        .clone()
}

struct TokenBucket {
    tokens: f64,
    capacity: f64,
    refill_per_sec: f64,
    base_refill_per_sec: f64,
    last: Instant,
    /// Until this instant, refill stays at the punished rate.
    punish_until: Option<Instant>,
}

impl TokenBucket {
    fn new(rps: f64) -> Self {
        let rps = rps.max(1.0);
        Self {
            tokens: rps,
            capacity: rps,
            refill_per_sec: rps,
            base_refill_per_sec: rps,
            last: Instant::now(),
            punish_until: None,
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        if self
            .punish_until
            .is_some_and(|until| now >= until && self.refill_per_sec < self.base_refill_per_sec)
        {
            self.refill_per_sec = self.base_refill_per_sec;
            self.punish_until = None;
        }
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
            self.last = now;
        }
    }

    /// `None` = token taken; `Some(delay)` = wait then retry.
    fn try_take(&mut self) -> Option<Duration> {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return None;
        }
        let need = 1.0 - self.tokens;
        let secs = (need / self.refill_per_sec.max(0.1)).clamp(0.005, 2.0);
        Some(Duration::from_secs_f64(secs))
    }

    fn punish(&mut self) {
        self.refill();
        // Cut to half of base (floor 4 rps) for 60s — stops stampeding after 429.
        self.refill_per_sec = (self.base_refill_per_sec * 0.5).max(4.0);
        self.tokens = self.tokens.min(2.0);
        self.punish_until = Some(Instant::now() + Duration::from_secs(60));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publicnode_is_free_tier_slower_pace() {
        let url = "https://polygon-bor-rpc.publicnode.com";
        assert_eq!(host_rank_class(url), 2);
        assert!(min_batch_pace_ms(url) >= 50);
        assert!(effective_batch_pace_ms(url, 8) >= 50);
    }

    #[test]
    fn alchemy_is_paid_no_pace_floor() {
        let url = "https://polygon-mainnet.g.alchemy.com/v2/key";
        assert_eq!(host_rank_class(url), 0);
        assert_eq!(min_batch_pace_ms(url), 0);
        assert_eq!(effective_batch_pace_ms(url, 8), 8);
    }

    #[test]
    fn rank_prefers_paid_over_public() {
        assert!(host_rank_class("https://rpc.ankr.com/polygon/x") < host_rank_class(
            "https://polygon-bor-rpc.publicnode.com"
        ));
    }

    #[tokio::test]
    async fn admit_consumes_token_under_scope() {
        let url = "https://polygon-bor-rpc.publicnode.com";
        // Isolate bucket by using unique host via punish/refill on shared map —
        // just ensure scope + admit does not hang.
        let ok = tokio::time::timeout(Duration::from_secs(2), async {
            scope_rpc_budget(url, async {
                admit_rpc_request().await;
                true
            })
            .await
        })
        .await
        .expect("admit timed out");
        assert!(ok);
    }

    #[test]
    fn punish_reduces_refill() {
        let mut b = TokenBucket::new(16.0);
        b.punish();
        assert!(b.refill_per_sec < 16.0);
        assert!(b.refill_per_sec >= 4.0);
    }
}
