use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const DEADLINE_CHECK_INTERVAL: u32 = 256;
const DEADLINE_CHECK_MASK: u32 = DEADLINE_CHECK_INTERVAL - 1;

pub struct DeadlineGuard {
    deadline: Instant,
    ops: u32,
    expired: bool,
}

impl DeadlineGuard {
    #[must_use]
    pub fn new(budget: Duration) -> Self {
        Self {
            deadline: Instant::now() + budget,
            ops: 0,
            expired: false,
        }
    }

    #[inline]
    pub fn tick(&mut self) -> bool {
        if self.expired {
            return true;
        }
        self.ops += 1;
        if self.ops & DEADLINE_CHECK_MASK == 0 && Instant::now() > self.deadline {
            self.expired = true;
        }
        self.expired
    }
}

/// Thread-safe deadline for parallel cycle enumeration.
pub struct SharedDeadlineGuard {
    deadline: Instant,
    expired: AtomicBool,
}

thread_local! {
    static LOCAL_OPS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

impl SharedDeadlineGuard {
    #[must_use]
    pub fn new(budget: Duration) -> Arc<Self> {
        Arc::new(Self {
            deadline: Instant::now() + budget,
            expired: AtomicBool::new(false),
        })
    }

    #[inline]
    pub fn tick(&self) -> bool {
        if self.expired.load(Ordering::Relaxed) {
            return true;
        }
        let ops = LOCAL_OPS.with(|cell| {
            let val = cell.get();
            cell.set(val.wrapping_add(1));
            val
        });
        if ops & DEADLINE_CHECK_MASK == 0 && Instant::now() > self.deadline {
            self.expired.store(true, Ordering::Relaxed);
            // Return immediately — we know we just expired.
            return true;
        }
        // Re-check: another thread may have set expired since our first load.
        self.expired.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_deadline_guard_expiration() {
        let mut guard = DeadlineGuard::new(Duration::from_millis(10));
        assert!(!guard.tick());

        // Wait for deadline to pass
        thread::sleep(Duration::from_millis(15));

        // It shouldn't check every tick, so tick multiple times to trigger the mask
        let mut expired = false;
        for _ in 0..1000 {
            if guard.tick() {
                expired = true;
                break;
            }
        }
        assert!(expired, "DeadlineGuard should expire after deadline passes");
    }

    #[test]
    fn test_deadline_guard_not_expired_initially() {
        let mut guard = DeadlineGuard::new(Duration::from_secs(10));
        for _ in 0..1000 {
            assert!(!guard.tick(), "DeadlineGuard should not expire early");
        }
    }

    #[test]
    fn test_shared_deadline_guard_expiration() {
        let guard = SharedDeadlineGuard::new(Duration::from_millis(10));
        assert!(!guard.tick());

        thread::sleep(Duration::from_millis(15));

        let mut expired = false;
        for _ in 0..1000 {
            if guard.tick() {
                expired = true;
                break;
            }
        }
        assert!(
            expired,
            "SharedDeadlineGuard should expire after deadline passes"
        );
    }

    #[test]
    fn test_shared_deadline_guard_parallel() {
        let guard = SharedDeadlineGuard::new(Duration::from_millis(10));
        let guard_clone = guard.clone();

        let handle1 = thread::spawn(move || {
            let mut expired = false;
            for _ in 0..10000 {
                if guard_clone.tick() {
                    expired = true;
                    break;
                }
            }
            expired
        });

        let guard_clone2 = guard.clone();
        let handle2 = thread::spawn(move || {
            let mut expired = false;
            for _ in 0..10000 {
                if guard_clone2.tick() {
                    expired = true;
                    break;
                }
            }
            expired
        });

        thread::sleep(Duration::from_millis(15));

        let mut expired = false;
        for _ in 0..10000 {
            if guard.tick() {
                expired = true;
                break;
            }
        }

        let expired1 = handle1.join().expect("thread 1 panicked");
        let expired2 = handle2.join().expect("thread 2 panicked");

        assert!(
            expired || expired1 || expired2,
            "At least one thread should see the expired state"
        );
    }
}
