use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use ruint::aliases::U256;
use tracing::{info, warn};

/// Global execution killswitch — pauses live submits after repeated failures or low wallet balance.
#[derive(Debug)]
pub struct CircuitBreaker {
    consecutive_failures: AtomicU32,
    paused: AtomicBool,
    pause_reason: RwLock<Option<String>>,
    paused_at: RwLock<Option<Instant>>,
    cooldown: Duration,
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(Duration::from_secs(300))
    }
}

impl CircuitBreaker {
    pub fn new(cooldown: Duration) -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            paused: AtomicBool::new(false),
            pause_reason: RwLock::new(None),
            paused_at: RwLock::new(None),
            cooldown,
        }
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    pub fn pause_reason(&self) -> Option<String> {
        self.pause_reason.read().clone()
    }

    pub fn trip(&self, reason: impl Into<String>) {
        let reason = reason.into();
        self.paused.store(true, Ordering::Relaxed);
        *self.pause_reason.write() = Some(reason.clone());
        *self.paused_at.write() = Some(Instant::now());
        warn!(reason = %reason, "circuit breaker tripped — execution paused");
    }

    pub fn reset(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.paused.store(false, Ordering::Relaxed);
        *self.pause_reason.write() = None;
        *self.paused_at.write() = None;
        info!("circuit breaker reset — execution enabled");
    }

    /// Resume after cooldown elapsed (e.g. before each candidate or on balance recovery).
    pub fn try_auto_reset(&self) -> bool {
        if !self.is_paused() {
            return false;
        }
        let should = self
            .paused_at
            .read()
            .is_some_and(|at| at.elapsed() >= self.cooldown);
        if should {
            self.reset();
            true
        } else {
            false
        }
    }

    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
    }

    /// Returns `true` if the breaker tripped on this failure.
    pub fn record_failure(&self, max_consecutive: u32, context: &str) -> bool {
        let count = self
            .consecutive_failures
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if count >= max_consecutive {
            self.trip(format!("{count} consecutive failures (last: {context})"));
            true
        } else {
            false
        }
    }

    /// Trip when operator MATIC balance drops below configured floor (gas runway).
    pub fn check_operator_balance(&self, balance_wei: U256, min_wei: U256) -> bool {
        if min_wei.is_zero() {
            return true;
        }
        if balance_wei < min_wei {
            self.trip(format!(
                "operator balance {balance_wei} MATIC wei below floor {min_wei}"
            ));
            false
        } else {
            if self.is_paused()
                && self
                    .pause_reason
                    .read()
                    .as_deref()
                    .is_some_and(|r| r.contains("operator balance"))
            {
                self.try_auto_reset();
            }
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn auto_resets_after_cooldown() {
        let cb = CircuitBreaker::new(Duration::from_millis(1));
        cb.trip("test");
        assert!(cb.is_paused());
        std::thread::sleep(Duration::from_millis(2));
        assert!(cb.try_auto_reset());
        assert!(!cb.is_paused());
    }
}
