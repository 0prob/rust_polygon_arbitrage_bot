//! Shared Proptest strategies for math property tests (cfg(test) only).
//!
//! Prefer bounded ranges over `u128::MAX` so cases stay in on-chain-like
//! magnitudes and rejects shrink cleanly (Proptest book / 1.11 tips).

use alloy::primitives::U256;
use proptest::prelude::*;

/// ~1e36 — covers 1e18-decimal amounts with headroom without full u128 space.
pub const U256_AMT_MAX: u128 = 10u128.pow(36);

/// Fixed-point ONE (1e18) used by Balancer/DODO/WOOFi weights and coeffs.
pub const FP18_ONE: u128 = 1_000_000_000_000_000_000;

pub fn u256_nonzero() -> impl Strategy<Value = U256> {
    (1u128..=U256_AMT_MAX).prop_map(U256::from)
}

/// Values in `(0, 1e18]` — fixed-point weights / k / spread coeffs.
pub fn u256_fp18() -> impl Strategy<Value = U256> {
    (1u128..=FP18_ONE).prop_map(U256::from)
}
