use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrentOptimizeReject {
    BoundsEmpty,
    ClCapZero,
    ClCapBoundsEmpty,
    BelowEconomicFloor,
    ZeroProfit,
    SanityDispatch,
}

static ATTEMPTS: AtomicU32 = AtomicU32::new(0);
static OK: AtomicU32 = AtomicU32::new(0);
static BOUNDS_FAIL: AtomicU32 = AtomicU32::new(0);
static CL_CAP_FAIL: AtomicU32 = AtomicU32::new(0);
static FLOOR_FAIL: AtomicU32 = AtomicU32::new(0);
static ZERO_PROFIT: AtomicU32 = AtomicU32::new(0);
static SANITY_FAIL: AtomicU32 = AtomicU32::new(0);
static EVAL_SIM: AtomicU32 = AtomicU32::new(0);
static EVAL_REJECT: AtomicU32 = AtomicU32::new(0);
static CACHE_LOCAL: AtomicU32 = AtomicU32::new(0);
static CACHE_ROUTE: AtomicU32 = AtomicU32::new(0);
static WARM_SEED: AtomicU32 = AtomicU32::new(0);

pub fn record_brent_attempt() {
    ATTEMPTS.fetch_add(1, Ordering::Relaxed);
}

pub fn record_brent_ok() {
    OK.fetch_add(1, Ordering::Relaxed);
}

pub fn record_brent_reject(reason: BrentOptimizeReject) {
    match reason {
        BrentOptimizeReject::BoundsEmpty => {
            BOUNDS_FAIL.fetch_add(1, Ordering::Relaxed);
        }
        BrentOptimizeReject::ClCapZero | BrentOptimizeReject::ClCapBoundsEmpty => {
            CL_CAP_FAIL.fetch_add(1, Ordering::Relaxed);
        }
        BrentOptimizeReject::BelowEconomicFloor => {
            FLOOR_FAIL.fetch_add(1, Ordering::Relaxed);
        }
        BrentOptimizeReject::ZeroProfit => {
            ZERO_PROFIT.fetch_add(1, Ordering::Relaxed);
        }
        BrentOptimizeReject::SanityDispatch => {
            SANITY_FAIL.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn record_brent_eval_sim() {
    EVAL_SIM.fetch_add(1, Ordering::Relaxed);
}

pub fn record_brent_eval_reject() {
    EVAL_REJECT.fetch_add(1, Ordering::Relaxed);
}

pub fn record_brent_cache_local() {
    CACHE_LOCAL.fetch_add(1, Ordering::Relaxed);
}

pub fn record_brent_cache_route() {
    CACHE_ROUTE.fetch_add(1, Ordering::Relaxed);
}

pub fn record_brent_warm_seed() {
    WARM_SEED.fetch_add(1, Ordering::Relaxed);
}

pub fn log_brent_summary() {
    let attempts = ATTEMPTS.load(Ordering::Relaxed);
    let ok = OK.load(Ordering::Relaxed);
    if attempts == 0 {
        return;
    }
    crate::info!(
        "brent: attempts={attempts} ok={ok} bounds_fail={} cl_cap_fail={} floor_fail={} zero_profit={} sanity_fail={} \
         eval_sim={} eval_reject={} cache_local={} cache_route={} warm_seed={}",
        BOUNDS_FAIL.load(Ordering::Relaxed),
        CL_CAP_FAIL.load(Ordering::Relaxed),
        FLOOR_FAIL.load(Ordering::Relaxed),
        ZERO_PROFIT.load(Ordering::Relaxed),
        SANITY_FAIL.load(Ordering::Relaxed),
        EVAL_SIM.load(Ordering::Relaxed),
        EVAL_REJECT.load(Ordering::Relaxed),
        CACHE_LOCAL.load(Ordering::Relaxed),
        CACHE_ROUTE.load(Ordering::Relaxed),
        WARM_SEED.load(Ordering::Relaxed),
    );
}