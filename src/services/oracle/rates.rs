use alloy::primitives::U256;
use rustc_hash::FxHashMap;

use crate::core::constants::MIN_TOKEN_TO_MATIC_RATE;
use crate::core::types::TokenIndex;
use crate::services::oracle::price_oracle::bootstrap_matic_rate_per_unit;

/// True when the token has an oracle rate above the dust floor (not bootstrap-only).
#[must_use]
pub fn has_reliable_matic_rate(token: TokenIndex, rates: &FxHashMap<TokenIndex, U256>) -> bool {
    rates
        .get(&token)
        .copied()
        .is_some_and(|r| r >= MIN_TOKEN_TO_MATIC_RATE)
}

/// Returns a rate only when oracle data is present; dispatch paths skip on `None`.
#[must_use]
pub fn resolve_token_to_matic_rate_or_bootstrap(
    token: TokenIndex,
    rates: &FxHashMap<TokenIndex, U256>,
) -> Option<U256> {
    rates
        .get(&token)
        .copied()
        .filter(|r| *r >= MIN_TOKEN_TO_MATIC_RATE)
}

/// Single policy for token/MATIC conversion used in eval, dispatch, and sizing.
pub fn resolve_token_to_matic_rate(token: TokenIndex, rates: &FxHashMap<TokenIndex, U256>) -> U256 {
    rates
        .get(&token)
        .copied()
        .filter(|r| *r >= MIN_TOKEN_TO_MATIC_RATE)
        .unwrap_or_else(bootstrap_matic_rate_per_unit)
}
