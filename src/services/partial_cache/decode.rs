use alloy::primitives::{B256, U256};
use alloy::sol_types::SolEvent;

use crate::abis::{IUniswapV2Pair, IUniswapV3Pool};

/// `Sync(uint112,uint112)` — Uniswap V2 pair reserve update.
pub const V2_SYNC_TOPIC: B256 = IUniswapV2Pair::Sync::SIGNATURE_HASH;

/// `Swap(address,address,int256,int256,uint160,uint128,int24)` — Uniswap V3 pool swap.
pub const V3_SWAP_TOPIC: B256 = IUniswapV3Pool::Swap::SIGNATURE_HASH;
/// Algebra V1.9 and Integral deliberately retain the same Swap event ABI.
pub const ALGEBRA_SWAP_TOPIC: B256 = V3_SWAP_TOPIC;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogPatch {
    V2Reserves {
        reserve0: U256,
        reserve1: U256,
    },
    V3Slot {
        sqrt_price_x96: U256,
        liquidity: u128,
        tick: i32,
    },
}

/// Zero-copy decode of a filtered pool log (topic0 already matched by subscription).
#[must_use]
pub fn decode_pool_log(topic0: B256, data: &[u8]) -> Option<LogPatch> {
    if topic0 == V2_SYNC_TOPIC {
        decode_v2_sync(data)
    } else if topic0 == V3_SWAP_TOPIC {
        decode_v3_swap(data)
    } else {
        None
    }
}

/// V2 `Sync`: two ABI words (reserve0, reserve1).
pub fn decode_v2_sync(data: &[u8]) -> Option<LogPatch> {
    if data.len() < 64 {
        return None;
    }
    Some(LogPatch::V2Reserves {
        reserve0: U256::from_be_slice(&data[0..32]),
        reserve1: U256::from_be_slice(&data[32..64]),
    })
}

/// V3 `Swap` non-indexed fields: amount0, amount1, sqrtPriceX96, liquidity, tick.
pub fn decode_v3_swap(data: &[u8]) -> Option<LogPatch> {
    if data.len() < 160 {
        return None;
    }
    let sqrt_price_x96 = U256::from_be_slice(&data[64..96]);
    let liquidity = U256::from_be_slice(&data[96..128]).as_limbs()[0] as u128;
    let tick = crate::util::sign_extend_tick24(U256::from_be_slice(&data[128..160]));
    Some(LogPatch::V3Slot {
        sqrt_price_x96,
        liquidity,
        tick,
    })
}

#[must_use]
pub fn is_streamable_protocol(protocol: crate::core::types::ProtocolType) -> bool {
    matches!(
        protocol,
        crate::core::types::ProtocolType::UniswapV2 | crate::core::types::ProtocolType::UniswapV3
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn algebra_and_uniswap_swap_topics_match() {
        assert_eq!(ALGEBRA_SWAP_TOPIC, V3_SWAP_TOPIC);
    }
}
