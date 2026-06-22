use std::time::{Duration, Instant};

const DEADLINE_CHECK_INTERVAL: u32 = 256;

pub struct DeadlineGuard {
    deadline: Instant,
    ops: u32,
    expired: bool,
}

impl DeadlineGuard {
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
        if self.ops.is_multiple_of(DEADLINE_CHECK_INTERVAL) && Instant::now() > self.deadline {
            self.expired = true;
        }
        self.expired
    }
}