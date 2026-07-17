use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrentOptimizeReject {
    BoundsEmpty,
    /// Balancer MAX_IN window infeasible (subset of former BoundsEmpty).
    BalancerBoundsEmpty,
    ClCapZero,
    ClCapBoundsEmpty,
    BelowEconomicFloor,
    ZeroProfit,
    SanityDispatch,
}

static ATTEMPTS: AtomicU32 = AtomicU32::new(0);
static OK: AtomicU32 = AtomicU32::new(0);
static BOUNDS_FAIL: AtomicU32 = AtomicU32::new(0);
static BAL_BOUNDS_FAIL: AtomicU32 = AtomicU32::new(0);
static CL_CAP_FAIL: AtomicU32 = AtomicU32::new(0);
static FLOOR_FAIL: AtomicU32 = AtomicU32::new(0);
static ZERO_PROFIT: AtomicU32 = AtomicU32::new(0);
static SANITY_FAIL: AtomicU32 = AtomicU32::new(0);
static EVAL_SIM: AtomicU32 = AtomicU32::new(0);
static EVAL_REJECT: AtomicU32 = AtomicU32::new(0);
static EVAL_REJECT_SIM_NONE: AtomicU32 = AtomicU32::new(0);
static EVAL_REJECT_ZERO: AtomicU32 = AtomicU32::new(0);
static EVAL_REJECT_SANITY: AtomicU32 = AtomicU32::new(0);
static SIM_NONE_SAMPLE: AtomicU32 = AtomicU32::new(0);
static SIM_NONE_V2: AtomicU32 = AtomicU32::new(0);
static SIM_NONE_SHALLOW: AtomicU32 = AtomicU32::new(0);
static SIM_NONE_TICKLESS: AtomicU32 = AtomicU32::new(0);
static SIM_NONE_ZERO_OUT: AtomicU32 = AtomicU32::new(0);
static SIM_NONE_UNSUPPORTED: AtomicU32 = AtomicU32::new(0);
static SIM_NONE_TOKEN_MISMATCH: AtomicU32 = AtomicU32::new(0);
static SIM_NONE_OTHER: AtomicU32 = AtomicU32::new(0);
static ZO_PROTO_V2: AtomicU32 = AtomicU32::new(0);
static ZO_PROTO_V3: AtomicU32 = AtomicU32::new(0);
static ZO_PROTO_V4: AtomicU32 = AtomicU32::new(0);
static ZO_PROTO_BAL: AtomicU32 = AtomicU32::new(0);
static ZO_PROTO_OTHER: AtomicU32 = AtomicU32::new(0);
/// Sampled Balancer ZeroOutput split: vault MAX_IN_RATIO vs other zero-out.
static BAL_ZO_MAX_IN: AtomicU32 = AtomicU32::new(0);
static BAL_ZO_OTHER: AtomicU32 = AtomicU32::new(0);
/// Sampled UnsupportedState by edge protocol.
static UNSUP_PROTO_V2: AtomicU32 = AtomicU32::new(0);
static UNSUP_PROTO_V3: AtomicU32 = AtomicU32::new(0);
static UNSUP_PROTO_V4: AtomicU32 = AtomicU32::new(0);
static UNSUP_PROTO_BAL: AtomicU32 = AtomicU32::new(0);
static UNSUP_PROTO_CRV: AtomicU32 = AtomicU32::new(0);
static UNSUP_PROTO_DODO: AtomicU32 = AtomicU32::new(0);
static UNSUP_PROTO_WOOFI: AtomicU32 = AtomicU32::new(0);
static CACHE_LOCAL: AtomicU32 = AtomicU32::new(0);
static CACHE_ROUTE: AtomicU32 = AtomicU32::new(0);
static WARM_SEED: AtomicU32 = AtomicU32::new(0);
static SEED_HIGH_CLAMP: AtomicU32 = AtomicU32::new(0);
static CL_DEPTH_CLAMP: AtomicU32 = AtomicU32::new(0);
/// Sampled ShallowCl / ClCapExceeded hop index buckets.
static SHALLOW_HOP_0: AtomicU32 = AtomicU32::new(0);
static SHALLOW_HOP_1: AtomicU32 = AtomicU32::new(0);
static SHALLOW_HOP_2P: AtomicU32 = AtomicU32::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrentEvalReject {
    SimNone,
    ZeroProfit,
    Sanity,
}

/// Coarse buckets for sampled Brent `SimNone` diagnoses (every 16th reject).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrentSimNoneKind {
    V2Reserve,
    ShallowCl,
    ClTickless,
    ZeroOutput,
    /// Balancer hop amount exceeds vault `MAX_IN_RATIO` (subset of former ZeroOutput).
    BalancerMaxIn,
    Unsupported,
    TokenMismatch,
    Other,
}

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
        BrentOptimizeReject::BalancerBoundsEmpty => {
            BOUNDS_FAIL.fetch_add(1, Ordering::Relaxed);
            BAL_BOUNDS_FAIL.fetch_add(1, Ordering::Relaxed);
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

pub fn record_brent_eval_reject(reason: BrentEvalReject) {
    EVAL_REJECT.fetch_add(1, Ordering::Relaxed);
    match reason {
        BrentEvalReject::SimNone => {
            EVAL_REJECT_SIM_NONE.fetch_add(1, Ordering::Relaxed);
        }
        BrentEvalReject::ZeroProfit => {
            EVAL_REJECT_ZERO.fetch_add(1, Ordering::Relaxed);
        }
        BrentEvalReject::Sanity => {
            EVAL_REJECT_SANITY.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Sample whether this SimNone should run `minimal_sim_failure` (every 16th).
#[must_use]
pub fn should_sample_brent_sim_none() -> bool {
    SIM_NONE_SAMPLE
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(16)
}

pub fn record_brent_sim_none_kind(kind: BrentSimNoneKind) {
    match kind {
        BrentSimNoneKind::V2Reserve => {
            SIM_NONE_V2.fetch_add(1, Ordering::Relaxed);
        }
        BrentSimNoneKind::ShallowCl => {
            SIM_NONE_SHALLOW.fetch_add(1, Ordering::Relaxed);
        }
        BrentSimNoneKind::ClTickless => {
            SIM_NONE_TICKLESS.fetch_add(1, Ordering::Relaxed);
        }
        BrentSimNoneKind::ZeroOutput => {
            SIM_NONE_ZERO_OUT.fetch_add(1, Ordering::Relaxed);
        }
        BrentSimNoneKind::BalancerMaxIn => {
            // Counted under zero_out_proto(bal) + bal_zo(max_in); keep sample bucket
            // adjacent to zero_out so existing % math still reads "liquidity reject".
            SIM_NONE_ZERO_OUT.fetch_add(1, Ordering::Relaxed);
            BAL_ZO_MAX_IN.fetch_add(1, Ordering::Relaxed);
        }
        BrentSimNoneKind::Unsupported => {
            SIM_NONE_UNSUPPORTED.fetch_add(1, Ordering::Relaxed);
        }
        BrentSimNoneKind::TokenMismatch => {
            SIM_NONE_TOKEN_MISMATCH.fetch_add(1, Ordering::Relaxed);
        }
        BrentSimNoneKind::Other => {
            SIM_NONE_OTHER.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Protocol of the hop that produced a sampled Brent `ZeroOutput` SimNone.
pub fn record_brent_zero_out_protocol(protocol: crate::core::types::ProtocolType) {
    use crate::core::types::ProtocolType;
    match protocol {
        ProtocolType::UniswapV2 => {
            ZO_PROTO_V2.fetch_add(1, Ordering::Relaxed);
        }
        ProtocolType::UniswapV3 => {
            ZO_PROTO_V3.fetch_add(1, Ordering::Relaxed);
        }
        ProtocolType::UniswapV4 => {
            ZO_PROTO_V4.fetch_add(1, Ordering::Relaxed);
        }
        ProtocolType::BalancerV2 => {
            ZO_PROTO_BAL.fetch_add(1, Ordering::Relaxed);
        }
        _ => {
            ZO_PROTO_OTHER.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Balancer ZeroOutput that is *not* `MAX_IN_RATIO` (weights/fee/math/paused).
pub fn record_brent_bal_zero_other() {
    BAL_ZO_OTHER.fetch_add(1, Ordering::Relaxed);
}

/// Protocol of the hop that produced a sampled Brent `UnsupportedState` SimNone.
pub fn record_brent_unsupported_protocol(protocol: crate::core::types::ProtocolType) {
    use crate::core::types::ProtocolType;
    match protocol {
        ProtocolType::UniswapV2 => {
            UNSUP_PROTO_V2.fetch_add(1, Ordering::Relaxed);
        }
        ProtocolType::UniswapV3 => {
            UNSUP_PROTO_V3.fetch_add(1, Ordering::Relaxed);
        }
        ProtocolType::UniswapV4 => {
            UNSUP_PROTO_V4.fetch_add(1, Ordering::Relaxed);
        }
        ProtocolType::BalancerV2 => {
            UNSUP_PROTO_BAL.fetch_add(1, Ordering::Relaxed);
        }
        ProtocolType::CurveStable | ProtocolType::CurveCrypto => {
            UNSUP_PROTO_CRV.fetch_add(1, Ordering::Relaxed);
        }
        ProtocolType::Dodo => {
            UNSUP_PROTO_DODO.fetch_add(1, Ordering::Relaxed);
        }
        ProtocolType::Woofi => {
            UNSUP_PROTO_WOOFI.fetch_add(1, Ordering::Relaxed);
        }
    }
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

pub fn record_brent_seed_high_clamp() {
    SEED_HIGH_CLAMP.fetch_add(1, Ordering::Relaxed);
}

pub fn record_brent_cl_depth_clamp() {
    CL_DEPTH_CLAMP.fetch_add(1, Ordering::Relaxed);
}

/// Hop index of a sampled Brent shallow / CL-cap SimNone.
pub fn record_brent_shallow_hop(hop: usize) {
    match hop {
        0 => {
            SHALLOW_HOP_0.fetch_add(1, Ordering::Relaxed);
        }
        1 => {
            SHALLOW_HOP_1.fetch_add(1, Ordering::Relaxed);
        }
        _ => {
            SHALLOW_HOP_2P.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn log_brent_summary() {
    let attempts = ATTEMPTS.load(Ordering::Relaxed);
    let ok = OK.load(Ordering::Relaxed);
    if attempts == 0 {
        return;
    }
    crate::info!(
        "brent: attempts={attempts} ok={ok} bounds_fail={} bal_bounds_fail={} cl_cap_fail={} floor_fail={} zero_profit={} sanity_fail={} \
         eval_sim={} eval_reject={} (sim_none={} zero={} sanity={}) \
         sim_none_sample(v2={} shallow={} tickless={} zero_out={} unsupported={} token_mismatch={} other={}) \
         shallow_hop(0={} 1={} 2p={}) \
         zero_out_proto(v2={} v3={} v4={} bal={} other={}) \
         bal_zo(max_in={} other={}) \
         unsup_proto(v2={} v3={} v4={} bal={} crv={} dodo={} woofi={}) \
         cache_local={} cache_route={} warm_seed={} seed_high_clamp={} cl_depth_clamp={}",
        BOUNDS_FAIL.load(Ordering::Relaxed),
        BAL_BOUNDS_FAIL.load(Ordering::Relaxed),
        CL_CAP_FAIL.load(Ordering::Relaxed),
        FLOOR_FAIL.load(Ordering::Relaxed),
        ZERO_PROFIT.load(Ordering::Relaxed),
        SANITY_FAIL.load(Ordering::Relaxed),
        EVAL_SIM.load(Ordering::Relaxed),
        EVAL_REJECT.load(Ordering::Relaxed),
        EVAL_REJECT_SIM_NONE.load(Ordering::Relaxed),
        EVAL_REJECT_ZERO.load(Ordering::Relaxed),
        EVAL_REJECT_SANITY.load(Ordering::Relaxed),
        SIM_NONE_V2.load(Ordering::Relaxed),
        SIM_NONE_SHALLOW.load(Ordering::Relaxed),
        SIM_NONE_TICKLESS.load(Ordering::Relaxed),
        SIM_NONE_ZERO_OUT.load(Ordering::Relaxed),
        SIM_NONE_UNSUPPORTED.load(Ordering::Relaxed),
        SIM_NONE_TOKEN_MISMATCH.load(Ordering::Relaxed),
        SIM_NONE_OTHER.load(Ordering::Relaxed),
        SHALLOW_HOP_0.load(Ordering::Relaxed),
        SHALLOW_HOP_1.load(Ordering::Relaxed),
        SHALLOW_HOP_2P.load(Ordering::Relaxed),
        ZO_PROTO_V2.load(Ordering::Relaxed),
        ZO_PROTO_V3.load(Ordering::Relaxed),
        ZO_PROTO_V4.load(Ordering::Relaxed),
        ZO_PROTO_BAL.load(Ordering::Relaxed),
        ZO_PROTO_OTHER.load(Ordering::Relaxed),
        BAL_ZO_MAX_IN.load(Ordering::Relaxed),
        BAL_ZO_OTHER.load(Ordering::Relaxed),
        UNSUP_PROTO_V2.load(Ordering::Relaxed),
        UNSUP_PROTO_V3.load(Ordering::Relaxed),
        UNSUP_PROTO_V4.load(Ordering::Relaxed),
        UNSUP_PROTO_BAL.load(Ordering::Relaxed),
        UNSUP_PROTO_CRV.load(Ordering::Relaxed),
        UNSUP_PROTO_DODO.load(Ordering::Relaxed),
        UNSUP_PROTO_WOOFI.load(Ordering::Relaxed),
        CACHE_LOCAL.load(Ordering::Relaxed),
        CACHE_ROUTE.load(Ordering::Relaxed),
        WARM_SEED.load(Ordering::Relaxed),
        SEED_HIGH_CLAMP.load(Ordering::Relaxed),
        CL_DEPTH_CLAMP.load(Ordering::Relaxed),
    );
}
