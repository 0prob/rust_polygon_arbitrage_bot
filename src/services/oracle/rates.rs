use ruint::aliases::U256;
use rustc_hash::FxHashMap;

use crate::core::constants::MIN_TOKEN_TO_MATIC_RATE;
use crate::core::types::TokenIndex;
use crate::pipeline::arena::StateArena;
use crate::services::oracle::price_oracle::bootstrap_matic_rate_per_unit;

/// Single policy for token/MATIC conversion used in eval, dispatch, and sizing.
pub fn resolve_token_to_matic_rate(
    token: TokenIndex,
    _arena: &StateArena,
    rates: &FxHashMap<TokenIndex, U256>,
) -> U256 {
    rates
        .get(&token)
        .copied()
        .filter(|r| *r >= MIN_TOKEN_TO_MATIC_RATE)
        .unwrap_or_else(bootstrap_matic_rate_per_unit)
}
