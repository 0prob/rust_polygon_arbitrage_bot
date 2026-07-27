//! On-chain LST exchange rates for base-price (gas / profit thresholds).
//!
//! Routing still uses DEX pool state; these rates are for token→MATIC conversion
//! when Chainlink/Pyth USD spot is missing. See Lido `convertStMaticToMatic` and
//! Stader Child Pool `convertSharesToTokens`.

use alloy::network::Ethereum;
use alloy::primitives::{Address, U256, address};
use alloy::providers::Provider;
use alloy::sol;
use alloy::sol_types::SolCall;
use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicU8, Ordering};

use crate::core::constants::{MATIC_X, MIN_TOKEN_TO_MATIC_RATE, RATE_PRECISION, ST_MATIC};
use crate::core::types::TokenIndex;
use crate::pipeline::arena::StateArena;
use crate::pipeline::multicall::{MulticallItem, encode_call, execute_multicall};
use crate::util::ten_pow_u256_cached;

sol! {
    /// Lido stMATIC — returns (maticOut, totalStMatic, totalPooledMatic).
    function convertStMaticToMatic(uint256 amountInStMatic) external view returns (
        uint256 amountInMatic,
        uint256 totalStMaticAmount,
        uint256 totalPooledMatic
    );
    /// Stader Child Pool — shares → underlying tokens (POL).
    function convertSharesToTokens(uint256 shares) external view returns (uint256);
}

/// Alternate stMATIC / MaticX deployment addresses on Polygon mainnet.
const ST_MATIC_ALT: Address = address!("0x3A58a54C066FdC0f2D55FC9C89F0415C92eBf3C4");
const MATIC_X_ALT: Address = address!("0xfa68FB4628DFF1028CFEc22b4162FCcd0d45efb6");

/// Stader Labs Child Pool (`convertSharesToTokens`).
const STADER_CHILD_POOL: Address = address!("0xfd225c9e6601c9d38d8f98d8731bf59efcf8c0e3");

const ONE_18: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);

static UNSUPPORTED_LST_VIEWS: AtomicU8 = AtomicU8::new(0);

#[inline]
#[must_use]
pub fn is_lst_priced_token(addr: Address) -> bool {
    addr == ST_MATIC || addr == ST_MATIC_ALT || addr == MATIC_X || addr == MATIC_X_ALT
}

/// Fetch LST→MATIC rates for tokens present in `tokens` (keyed by TokenIndex).
pub async fn fetch_lst_matic_rates<P>(
    arena: &StateArena,
    tokens: &[TokenIndex],
    provider: &P,
) -> FxHashMap<TokenIndex, U256>
where
    P: Provider<Ethereum> + Clone + Send + 'static,
{
    let mut want_stmatic = Vec::new();
    let mut want_maticx = Vec::new();
    for &idx in tokens {
        let Some(addr) = arena.token_address(idx) else {
            continue;
        };
        if addr == ST_MATIC || addr == ST_MATIC_ALT {
            want_stmatic.push(idx);
        } else if addr == MATIC_X || addr == MATIC_X_ALT {
            want_maticx.push(idx);
        }
    }
    if want_stmatic.is_empty() && want_maticx.is_empty() {
        return FxHashMap::default();
    }

    let mut items = Vec::with_capacity(2);
    let mut tags = Vec::with_capacity(2);
    let unsupported = UNSUPPORTED_LST_VIEWS.load(Ordering::Relaxed);
    if !want_stmatic.is_empty() && unsupported & LstKind::StMatic.bit() == 0 {
        items.push(MulticallItem {
            target: ST_MATIC,
            data: encode_call(&convertStMaticToMaticCall {
                amountInStMatic: ONE_18,
            }),
        });
        tags.push(LstKind::StMatic);
    }
    if !want_maticx.is_empty() && unsupported & LstKind::MaticX.bit() == 0 {
        items.push(MulticallItem {
            target: STADER_CHILD_POOL,
            data: encode_call(&convertSharesToTokensCall { shares: ONE_18 }),
        });
        tags.push(LstKind::MaticX);
    }
    if items.is_empty() {
        return FxHashMap::default();
    }

    let Ok(results) = execute_multicall(provider, &items).await else {
        crate::debug!("lst rates: multicall failed");
        return FxHashMap::default();
    };

    let mut out = FxHashMap::default();
    for (tag, raw) in tags.into_iter().zip(results) {
        let Some(bytes) = raw else {
            tag.disable_after_failed_view();
            continue;
        };
        let rate = match tag {
            LstKind::StMatic => decode_stmatic_rate(&bytes),
            LstKind::MaticX => decode_shares_rate(&bytes),
        };
        let Some(rate) = rate.filter(|r| *r >= MIN_TOKEN_TO_MATIC_RATE) else {
            tag.disable_after_failed_view();
            continue;
        };
        let targets = match tag {
            LstKind::StMatic => &want_stmatic,
            LstKind::MaticX => &want_maticx,
        };
        for &idx in targets {
            out.insert(idx, rate);
        }
    }
    if !out.is_empty() {
        crate::debug!("lst rates: priced={}", out.len());
    }
    out
}

#[derive(Clone, Copy)]
enum LstKind {
    StMatic,
    MaticX,
}

impl LstKind {
    const fn bit(self) -> u8 {
        match self {
            Self::StMatic => 1,
            Self::MaticX => 2,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::StMatic => "stMATIC",
            Self::MaticX => "MaticX",
        }
    }

    fn disable_after_failed_view(self) {
        let prior = UNSUPPORTED_LST_VIEWS.fetch_or(self.bit(), Ordering::Relaxed);
        if prior & self.bit() == 0 {
            crate::info!(
                "lst rates: {} conversion view unavailable; suppressing further calls",
                self.label()
            );
        }
    }
}

fn decode_stmatic_rate(bytes: &[u8]) -> Option<U256> {
    let decoded = convertStMaticToMaticCall::abi_decode_returns(bytes).ok()?;
    // amountInMatic for 1 whole stMATIC (18 dec) → MATIC wei / whole token.
    // RATE_PRECISION == 1e18 ⇒ rate == amountInMatic.
    matic_wei_to_rate(decoded.amountInMatic, 18)
}

fn decode_shares_rate(bytes: &[u8]) -> Option<U256> {
    let decoded = convertSharesToTokensCall::abi_decode_returns(bytes).ok()?;
    matic_wei_to_rate(decoded, 18)
}

/// Convert "MATIC wei for 1 whole token" into RATE_PRECISION units.
fn matic_wei_to_rate(matic_wei: U256, decimals: u8) -> Option<U256> {
    if matic_wei.is_zero() {
        return None;
    }
    let scale = ten_pow_u256_cached(decimals);
    matic_wei
        .checked_mul(RATE_PRECISION)?
        .checked_div(scale)
        .filter(|r| *r >= MIN_TOKEN_TO_MATIC_RATE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_to_one_lst_rate_is_rate_precision() {
        let rate = matic_wei_to_rate(ONE_18, 18).expect("rate");
        assert_eq!(rate, RATE_PRECISION);
    }

    #[test]
    fn lst_token_recognition() {
        assert!(is_lst_priced_token(ST_MATIC));
        assert!(is_lst_priced_token(MATIC_X));
        assert!(!is_lst_priced_token(crate::core::constants::WMATIC));
    }

    #[test]
    fn lst_kind_bits_are_disjoint() {
        assert_eq!(LstKind::StMatic.bit() & LstKind::MaticX.bit(), 0);
    }
}
