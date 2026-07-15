use alloy::primitives::U256;
use rustc_hash::FxHashMap;

use crate::core::constants::MIN_TOKEN_TO_MATIC_RATE;
use crate::core::types::TokenIndex;
use crate::services::oracle::price_oracle::bootstrap_matic_rate_per_unit;

#[inline]
fn lookup_matic_rate(token: TokenIndex, rates: &FxHashMap<TokenIndex, U256>) -> Option<U256> {
    rates
        .get(&token)
        .copied()
        .filter(|r| *r >= MIN_TOKEN_TO_MATIC_RATE)
}

/// True when the token has an oracle rate above the dust floor (not bootstrap-only).
#[must_use]
pub fn has_reliable_matic_rate(token: TokenIndex, rates: &FxHashMap<TokenIndex, U256>) -> bool {
    lookup_matic_rate(token, rates).is_some()
}

/// Returns a rate only when oracle data is present; dispatch paths skip on `None`.
#[must_use]
pub fn resolve_token_to_matic_rate_or_bootstrap(
    token: TokenIndex,
    rates: &FxHashMap<TokenIndex, U256>,
) -> Option<U256> {
    lookup_matic_rate(token, rates)
}

/// Single policy for token/MATIC conversion used in eval, dispatch, and sizing.
pub fn resolve_token_to_matic_rate(token: TokenIndex, rates: &FxHashMap<TokenIndex, U256>) -> U256 {
    lookup_matic_rate(token, rates).unwrap_or_else(bootstrap_matic_rate_per_unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_respects_min_rate_floor() {
        let mut rates = FxHashMap::default();
        rates.insert(TokenIndex(0), U256::from(1u64));
        assert!(!has_reliable_matic_rate(TokenIndex(0), &rates));
        rates.insert(TokenIndex(0), MIN_TOKEN_TO_MATIC_RATE);
        assert!(has_reliable_matic_rate(TokenIndex(0), &rates));
    }
}
