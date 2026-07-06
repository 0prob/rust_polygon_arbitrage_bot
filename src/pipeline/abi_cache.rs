//! Pre-encoded view calldata and zero-copy ABI word decoders for the pool-fetch hot path.

use std::sync::LazyLock;

use alloy::primitives::{Bytes, FixedBytes, U256};
use alloy::sol_types::SolCall;

use crate::abis::{
    IAlgebraPool, IBalancerLinearPool, IBalancerPool, IBalancerVaultRead, ICurvePool,
    IDodoPoolState, IERC20Metadata, IUniswapV2Pair, IUniswapV3Pool, IUniswapV4PoolManager,
    IWoofiPool,
};

/// Decode the last 32-byte ABI word (or an exact 32-byte return).
#[inline]
#[must_use]
pub fn decode_abi_word(bytes: &[u8]) -> Option<U256> {
    match bytes.len() {
        0..=31 => None,
        32 => Some(U256::from_be_slice(bytes)),
        _ => Some(U256::from_be_slice(&bytes[bytes.len() - 32..])),
    }
}

#[inline]
#[must_use]
pub fn decode_abi_u128(bytes: &[u8]) -> Option<u128> {
    decode_abi_word(bytes).map(|v| v.as_limbs()[0] as u128)
}

/// `extsload(bytes32)` — selector + slot without full `abi_encode` allocation.
#[inline]
#[must_use]
pub fn encode_extsload(slot: FixedBytes<32>) -> Bytes {
    const SELECTOR: [u8; 4] = IUniswapV4PoolManager::extsloadCall::SELECTOR;
    let mut buf = [0u8; 36];
    buf[..4].copy_from_slice(&SELECTOR);
    buf[4..36].copy_from_slice(slot.as_slice());
    Bytes::copy_from_slice(&buf)
}

/// `getPoolTokens(bytes32)` — selector + pool id (36-byte calldata).
#[inline]
#[must_use]
pub fn encode_balancer_pool_tokens(pool_id: FixedBytes<32>) -> Bytes {
    const SELECTOR: [u8; 4] = IBalancerVaultRead::getPoolTokensCall::SELECTOR;
    let mut buf = [0u8; 36];
    buf[..4].copy_from_slice(&SELECTOR);
    buf[4..36].copy_from_slice(pool_id.as_slice());
    Bytes::copy_from_slice(&buf)
}

/// Algebra `globalState()` — six ABI words (price, tick, lastFee, pluginConfig, communityFee, unlocked).
#[inline]
#[must_use]
pub fn decode_algebra_global_state(bytes: &[u8]) -> Option<(U256, i32, bool, U256)> {
    if bytes.len() < 192 {
        return None;
    }
    let price = U256::from_be_slice(&bytes[0..32]);
    let tick = crate::util::sign_extend_tick24(U256::from_be_slice(&bytes[32..64]));
    let last_fee = U256::from_be_slice(&bytes[64..96]).as_limbs()[0] as u32;
    let unlocked = U256::from_be_slice(&bytes[160..192]).as_limbs()[0] != 0;
    Some((price, tick, unlocked, U256::from(last_fee)))
}

macro_rules! cached_view_call {
    ($name:ident, $call:expr) => {
        pub static $name: LazyLock<Bytes> =
            LazyLock::new(|| crate::pipeline::multicall::encode_call(&$call));
    };
}

cached_view_call!(V2_GET_RESERVES, IUniswapV2Pair::getReservesCall {});
cached_view_call!(V3_SLOT0, IUniswapV3Pool::slot0Call {});
cached_view_call!(V3_LIQUIDITY, IUniswapV3Pool::liquidityCall {});
cached_view_call!(V3_FEE, IUniswapV3Pool::feeCall {});
cached_view_call!(ALGEBRA_GLOBAL_STATE, IAlgebraPool::globalStateCall {});
cached_view_call!(DODO_BASE_RESERVE, IDodoPoolState::_BASE_RESERVE_Call {});
cached_view_call!(DODO_QUOTE_RESERVE, IDodoPoolState::_QUOTE_RESERVE_Call {});
cached_view_call!(DODO_BASE_TOKEN, IDodoPoolState::_BASE_TOKEN_Call {});
cached_view_call!(DODO_QUOTE_TOKEN, IDodoPoolState::_QUOTE_TOKEN_Call {});
cached_view_call!(DODO_I, IDodoPoolState::_I_Call {});
cached_view_call!(DODO_K, IDodoPoolState::_K_Call {});
cached_view_call!(DODO_LP_FEE, IDodoPoolState::_LP_FEE_RATE_Call {});
cached_view_call!(CURVE_A, ICurvePool::ACall {});
cached_view_call!(CURVE_FEE, ICurvePool::feeCall {});
cached_view_call!(CURVE_STORED_RATES, ICurvePool::stored_ratesCall {});
cached_view_call!(CURVE_GAMMA, ICurvePool::gammaCall {});
cached_view_call!(
    BALANCER_SWAP_FEE,
    IBalancerPool::getSwapFeePercentageCall {}
);
cached_view_call!(BALANCER_WEIGHTS, IBalancerPool::getNormalizedWeightsCall {});
cached_view_call!(
    BALANCER_AMP,
    IBalancerPool::getAmplificationParameterCall {}
);
cached_view_call!(BALANCER_SCALING, IBalancerPool::getScalingFactorsCall {});
cached_view_call!(
    BALANCER_LINEAR_MAIN,
    IBalancerLinearPool::getMainTokenCall {}
);
cached_view_call!(
    BALANCER_LINEAR_WRAPPED,
    IBalancerLinearPool::getWrappedTokenCall {}
);
cached_view_call!(
    BALANCER_LINEAR_TARGETS,
    IBalancerLinearPool::getTargetsCall {}
);
cached_view_call!(
    BALANCER_LINEAR_RATE,
    IBalancerLinearPool::getWrappedTokenRateCall {}
);
cached_view_call!(BALANCER_POOL_ID, IBalancerPool::getPoolIdCall {});
cached_view_call!(WOOFI_QUOTE_TOKEN, IWoofiPool::quoteTokenCall {});
cached_view_call!(WOOFI_WOORACLE, IWoofiPool::wooracleCall {});
cached_view_call!(ERC20_DECIMALS, IERC20Metadata::decimalsCall {});

/// Curve `balances(uint256)` for indices 0..7 (max stable-pool balance slots).
pub static CURVE_BALANCES: LazyLock<[Bytes; 8]> = LazyLock::new(|| {
    std::array::from_fn(|i| {
        crate::pipeline::multicall::encode_call(&ICurvePool::balancesCall { i: U256::from(i) })
    })
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_calls_match_fresh_abi_encode() {
        assert_eq!(
            *V2_GET_RESERVES,
            crate::pipeline::multicall::encode_call(&IUniswapV2Pair::getReservesCall {})
        );
        assert_eq!(
            *ERC20_DECIMALS,
            crate::pipeline::multicall::encode_call(&IERC20Metadata::decimalsCall {})
        );
    }

    #[test]
    fn encode_extsload_matches_abi_encode() {
        let slot = FixedBytes::<32>::from([7u8; 32]);
        assert_eq!(
            encode_extsload(slot),
            crate::pipeline::multicall::encode_call(&IUniswapV4PoolManager::extsloadCall { slot })
        );
    }

    #[test]
    fn encode_balancer_pool_tokens_matches_abi_encode() {
        let pool_id = FixedBytes::<32>::from([3u8; 32]);
        assert_eq!(
            encode_balancer_pool_tokens(pool_id),
            crate::pipeline::multicall::encode_call(&IBalancerVaultRead::getPoolTokensCall {
                poolId: pool_id
            })
        );
    }

    #[test]
    fn curve_balances_match_fresh_abi_encode() {
        for i in 0..8 {
            assert_eq!(
                CURVE_BALANCES[i],
                crate::pipeline::multicall::encode_call(&ICurvePool::balancesCall {
                    i: U256::from(i),
                })
            );
        }
    }

    #[test]
    fn algebra_global_state_zero_copy_matches_layout() {
        let mut bytes = vec![0u8; 192];
        bytes[31] = 9; // price = 9
        bytes[32..64].fill(0xFF); // tick = -1 (int24 sign-extended)
        bytes[95] = 5; // lastFee = 5
        bytes[191] = 1; // unlocked
        let (price, tick, unlocked, fee) = decode_algebra_global_state(&bytes).expect("decode");
        assert_eq!(price, U256::from(9u64));
        assert_eq!(tick, -1);
        assert!(unlocked);
        assert_eq!(fee, U256::from(5u64));
    }
}
