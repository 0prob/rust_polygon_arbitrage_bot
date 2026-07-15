use std::sync::atomic::{AtomicU32, Ordering};

use alloy::primitives::U256;

use crate::core::math::curve::{try_curve_stable_amount_out, CurveStableReject};
use crate::core::math::curve_crypto::{try_curve_crypto_amount_out, CurveCryptoReject};
use crate::core::types::{CurvePoolState, ProtocolType};

static STABLE_OK: AtomicU32 = AtomicU32::new(0);
static STABLE_ZERO_OUT: AtomicU32 = AtomicU32::new(0);
static STABLE_D: AtomicU32 = AtomicU32::new(0);
static STABLE_Y: AtomicU32 = AtomicU32::new(0);
static STABLE_OTHER: AtomicU32 = AtomicU32::new(0);
static CRYPTO_OK: AtomicU32 = AtomicU32::new(0);
static CRYPTO_ZERO_OUT: AtomicU32 = AtomicU32::new(0);
static CRYPTO_NEWTON_D: AtomicU32 = AtomicU32::new(0);
static CRYPTO_NEWTON_Y: AtomicU32 = AtomicU32::new(0);
static CRYPTO_OTHER: AtomicU32 = AtomicU32::new(0);

fn record_stable_reject(reason: CurveStableReject) {
    match reason {
        CurveStableReject::ZeroOut => {
            STABLE_ZERO_OUT.fetch_add(1, Ordering::Relaxed);
        }
        CurveStableReject::DInvariant => {
            STABLE_D.fetch_add(1, Ordering::Relaxed);
        }
        CurveStableReject::Y => {
            STABLE_Y.fetch_add(1, Ordering::Relaxed);
        }
        CurveStableReject::ZeroAmount
        | CurveStableReject::InvalidIndices
        | CurveStableReject::FeeTooHigh
        | CurveStableReject::Xp => {
            STABLE_OTHER.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn record_crypto_reject(reason: CurveCryptoReject) {
    match reason {
        CurveCryptoReject::ZeroOut => {
            CRYPTO_ZERO_OUT.fetch_add(1, Ordering::Relaxed);
        }
        CurveCryptoReject::NewtonD => {
            CRYPTO_NEWTON_D.fetch_add(1, Ordering::Relaxed);
        }
        CurveCryptoReject::NewtonY => {
            CRYPTO_NEWTON_Y.fetch_add(1, Ordering::Relaxed);
        }
        CurveCryptoReject::ZeroAmount
        | CurveCryptoReject::InvalidIndices
        | CurveCryptoReject::MissingGamma => {
            CRYPTO_OTHER.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Stable or crypto Curve hop — `None` when swap is unusable (zero out or math failure).
#[must_use]
pub fn curve_hop_amount_out(
    state: &CurvePoolState,
    protocol: ProtocolType,
    amount_in: U256,
    token_in_idx: usize,
    token_out_idx: usize,
) -> Option<U256> {
    match protocol {
        ProtocolType::CurveStable => match try_curve_stable_amount_out(
            state,
            amount_in,
            token_in_idx,
            token_out_idx,
        ) {
            Ok(out) if !out.is_zero() => {
                STABLE_OK.fetch_add(1, Ordering::Relaxed);
                Some(out)
            }
            Ok(_) => {
                record_stable_reject(CurveStableReject::ZeroOut);
                None
            }
            Err(reason) => {
                record_stable_reject(reason);
                None
            }
        },
        ProtocolType::CurveCrypto => match try_curve_crypto_amount_out(
            state,
            amount_in,
            token_in_idx,
            token_out_idx,
        ) {
            Ok(out) if !out.is_zero() => {
                CRYPTO_OK.fetch_add(1, Ordering::Relaxed);
                Some(out)
            }
            Ok(_) => {
                record_crypto_reject(CurveCryptoReject::ZeroOut);
                None
            }
            Err(reason) => {
                record_crypto_reject(reason);
                None
            }
        },
        _ => None,
    }
}

pub fn log_curve_sim_summary() {
    let stable_ok = STABLE_OK.load(Ordering::Relaxed);
    let crypto_ok = CRYPTO_OK.load(Ordering::Relaxed);
    let stable_fail = STABLE_ZERO_OUT.load(Ordering::Relaxed)
        + STABLE_D.load(Ordering::Relaxed)
        + STABLE_Y.load(Ordering::Relaxed)
        + STABLE_OTHER.load(Ordering::Relaxed);
    let crypto_fail = CRYPTO_ZERO_OUT.load(Ordering::Relaxed)
        + CRYPTO_NEWTON_D.load(Ordering::Relaxed)
        + CRYPTO_NEWTON_Y.load(Ordering::Relaxed)
        + CRYPTO_OTHER.load(Ordering::Relaxed);
    if stable_ok == 0 && crypto_ok == 0 && stable_fail == 0 && crypto_fail == 0 {
        return;
    }
    crate::info!(
        "curve: sim stable_ok={stable_ok} stable_fail={stable_fail} (zero_out={} d={} y={} other={}) \
         crypto_ok={crypto_ok} crypto_fail={crypto_fail} (zero_out={} newton_d={} newton_y={} other={})",
        STABLE_ZERO_OUT.load(Ordering::Relaxed),
        STABLE_D.load(Ordering::Relaxed),
        STABLE_Y.load(Ordering::Relaxed),
        STABLE_OTHER.load(Ordering::Relaxed),
        CRYPTO_ZERO_OUT.load(Ordering::Relaxed),
        CRYPTO_NEWTON_D.load(Ordering::Relaxed),
        CRYPTO_NEWTON_Y.load(Ordering::Relaxed),
        CRYPTO_OTHER.load(Ordering::Relaxed),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::math::fixed_point::ONE;

    #[test]
    fn curve_hop_rejects_zero_stable_output() {
        let state = CurvePoolState {
            balances: vec![U256::ONE, U256::ONE],
            a: U256::ZERO,
            fee: U256::ZERO,
            rates: vec![ONE, ONE],
            n_coins: 2,
            gamma: None,
            d: None,
        };
        assert!(curve_hop_amount_out(
            &state,
            ProtocolType::CurveStable,
            U256::from(1_000u64),
            0,
            1,
        )
        .is_none());
    }
}