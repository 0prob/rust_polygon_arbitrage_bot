use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TernaryBoundsReject {
    HopCapacity,
    FlashCapUnavailable,
    InvalidRange,
}

static BOUNDS_CALLS: AtomicU32 = AtomicU32::new(0);
static BOUNDS_OK: AtomicU32 = AtomicU32::new(0);
static HOP_CAPACITY_FAIL: AtomicU32 = AtomicU32::new(0);
static FLASH_CAP_UNAVAILABLE: AtomicU32 = AtomicU32::new(0);
static INVALID_RANGE: AtomicU32 = AtomicU32::new(0);
static RATE_FALLBACK: AtomicU32 = AtomicU32::new(0);
static LIQUIDITY_CAP_CLAMP: AtomicU32 = AtomicU32::new(0);
static FLASH_CAP_CLAMP: AtomicU32 = AtomicU32::new(0);
static ECONOMIC_HIGH_RAISE: AtomicU32 = AtomicU32::new(0);
static GOLDEN_ZERO_EXIT: AtomicU32 = AtomicU32::new(0);
static SEED_HIGH_CLAMP: AtomicU32 = AtomicU32::new(0);
static BAL_HIGH_CLAMP: AtomicU32 = AtomicU32::new(0);
static CL_DEPTH_CLAMP: AtomicU32 = AtomicU32::new(0);

pub fn record_ternary_bounds_call() {
    BOUNDS_CALLS.fetch_add(1, Ordering::Relaxed);
}

pub fn record_ternary_bounds_ok() {
    BOUNDS_OK.fetch_add(1, Ordering::Relaxed);
}

pub fn record_ternary_bounds_reject(reason: TernaryBoundsReject) {
    match reason {
        TernaryBoundsReject::HopCapacity => {
            HOP_CAPACITY_FAIL.fetch_add(1, Ordering::Relaxed);
        }
        TernaryBoundsReject::FlashCapUnavailable => {
            FLASH_CAP_UNAVAILABLE.fetch_add(1, Ordering::Relaxed);
        }
        TernaryBoundsReject::InvalidRange => {
            INVALID_RANGE.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn record_ternary_rate_fallback() {
    RATE_FALLBACK.fetch_add(1, Ordering::Relaxed);
}

pub fn record_ternary_liquidity_cap_clamp() {
    LIQUIDITY_CAP_CLAMP.fetch_add(1, Ordering::Relaxed);
}

pub fn record_ternary_flash_cap_clamp() {
    FLASH_CAP_CLAMP.fetch_add(1, Ordering::Relaxed);
}

pub fn record_ternary_economic_high_raise() {
    ECONOMIC_HIGH_RAISE.fetch_add(1, Ordering::Relaxed);
}

pub fn record_ternary_golden_zero_exit() {
    GOLDEN_ZERO_EXIT.fetch_add(1, Ordering::Relaxed);
}

pub fn record_ternary_seed_high_clamp() {
    SEED_HIGH_CLAMP.fetch_add(1, Ordering::Relaxed);
}

pub fn record_ternary_bal_high_clamp() {
    BAL_HIGH_CLAMP.fetch_add(1, Ordering::Relaxed);
}

pub fn record_ternary_cl_depth_clamp() {
    CL_DEPTH_CLAMP.fetch_add(1, Ordering::Relaxed);
}

pub fn log_ternary_summary() {
    let calls = BOUNDS_CALLS.load(Ordering::Relaxed);
    if calls == 0 {
        return;
    }
    crate::info!(
        "ternary: bounds_calls={calls} bounds_ok={} hop_fail={} flash_cap_unavail={} invalid_range={} rate_fallback={} \
         liq_cap_clamp={} flash_cap_clamp={} economic_high_raise={} golden_zero_exit={} seed_high_clamp={} bal_high_clamp={} cl_depth_clamp={}",
        BOUNDS_OK.load(Ordering::Relaxed),
        HOP_CAPACITY_FAIL.load(Ordering::Relaxed),
        FLASH_CAP_UNAVAILABLE.load(Ordering::Relaxed),
        INVALID_RANGE.load(Ordering::Relaxed),
        RATE_FALLBACK.load(Ordering::Relaxed),
        LIQUIDITY_CAP_CLAMP.load(Ordering::Relaxed),
        FLASH_CAP_CLAMP.load(Ordering::Relaxed),
        ECONOMIC_HIGH_RAISE.load(Ordering::Relaxed),
        GOLDEN_ZERO_EXIT.load(Ordering::Relaxed),
        SEED_HIGH_CLAMP.load(Ordering::Relaxed),
        BAL_HIGH_CLAMP.load(Ordering::Relaxed),
        CL_DEPTH_CLAMP.load(Ordering::Relaxed),
    );
}
