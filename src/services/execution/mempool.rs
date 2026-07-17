use std::time::{Duration, Instant};

/// How long to wait after a local submit while `pending > latest` before allowing another.
pub const MEMPOOL_STALL_TIMEOUT: Duration = Duration::from_secs(20);

/// Reuse latest/pending nonce reads briefly; stall timer is re-evaluated from wall clock.
pub const MEMPOOL_NONCE_CACHE_TTL: Duration = Duration::from_millis(400);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MempoolGateDecision {
    pub allow_submit: bool,
    pub pending_ahead: bool,
}

/// Whether the operator account may accept another live submit.
#[must_use]
pub fn decide_mempool_gate(
    latest: u64,
    pending: u64,
    last_global_submit: Option<Instant>,
    now: Instant,
    stall_timeout: Duration,
) -> MempoolGateDecision {
    if pending == latest {
        return MempoolGateDecision {
            allow_submit: true,
            pending_ahead: false,
        };
    }
    let waiting_on_recent_local_submit =
        last_global_submit.is_some_and(|t| now.saturating_duration_since(t) <= stall_timeout);
    if waiting_on_recent_local_submit {
        return MempoolGateDecision {
            allow_submit: false,
            pending_ahead: false,
        };
    }
    MempoolGateDecision {
        allow_submit: true,
        pending_ahead: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> Instant {
        Instant::now()
    }

    #[test]
    fn clear_when_pending_equals_latest() {
        let d = decide_mempool_gate(5, 5, None, now(), MEMPOOL_STALL_TIMEOUT);
        assert!(d.allow_submit);
        assert!(!d.pending_ahead);
    }

    #[test]
    fn blocks_while_recent_local_submit_pending() {
        let t0 = now();
        let d = decide_mempool_gate(
            5,
            7,
            Some(t0),
            t0 + Duration::from_secs(5),
            MEMPOOL_STALL_TIMEOUT,
        );
        assert!(!d.allow_submit);
        assert!(!d.pending_ahead);
    }

    #[test]
    fn allows_after_stall_timeout() {
        let t0 = now();
        let d = decide_mempool_gate(
            5,
            7,
            Some(t0),
            t0 + MEMPOOL_STALL_TIMEOUT + Duration::from_secs(1),
            MEMPOOL_STALL_TIMEOUT,
        );
        assert!(d.allow_submit);
        assert!(d.pending_ahead);
    }

    #[test]
    fn external_pending_without_local_submit_allows_with_resync() {
        let d = decide_mempool_gate(3, 5, None, now(), MEMPOOL_STALL_TIMEOUT);
        assert!(d.allow_submit);
        assert!(d.pending_ahead);
    }
}
