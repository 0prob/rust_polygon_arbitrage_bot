use alloy::primitives::{Address, B256, U256};
use alloy::sol_types::SolEvent;

use crate::abis::{IUniswapV2Pair, IUniswapV3Pool};

/// `Sync(uint112,uint112)` — Uniswap V2 pair reserve update.
pub const V2_SYNC_TOPIC: B256 = IUniswapV2Pair::Sync::SIGNATURE_HASH;

/// `Swap(address,address,int256,int256,uint160,uint128,int24)` — Uniswap V3 pool swap.
pub const V3_SWAP_TOPIC: B256 = IUniswapV3Pool::Swap::SIGNATURE_HASH;

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
    let tick_word = U256::from_be_slice(&data[128..160]);
    let tick_raw = (tick_word & U256::from(0xFF_FFFFu64)).as_limbs()[0] as u32;
    let tick = if tick_raw & 0x80_0000 != 0 {
        (tick_raw | !0xFF_FFFF) as i32
    } else {
        tick_raw as i32
    };
    Some(LogPatch::V3Slot {
        sqrt_price_x96,
        liquidity,
        tick,
    })
}

pub fn is_streamable_protocol(protocol: crate::core::types::ProtocolType) -> bool {
    matches!(
        protocol,
        crate::core::types::ProtocolType::UniswapV2 | crate::core::types::ProtocolType::UniswapV3
    )
}

#[allow(dead_code)]
pub fn pool_address_from_log(address: Address) -> Address {
    address
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_sync_min_length() {
        let mut data = [0u8; 64];
        data[31] = 1;
        data[63] = 2;
        let patch = decode_v2_sync(&data).unwrap();
        assert!(matches!(patch, LogPatch::V2Reserves { .. }));
    }

    #[test]
    fn v3_swap_min_length() {
        let mut data = [0u8; 160];
        data[95] = 0x42;
        data[127] = 0x01;
        let patch = decode_v3_swap(&data).unwrap();
        match patch {
            LogPatch::V3Slot {
                sqrt_price_x96,
                liquidity,
                ..
            } => {
                assert_eq!(sqrt_price_x96, U256::from(0x42u64));
                assert_eq!(liquidity, 1);
            }
            _ => panic!("expected v3"),
        }
    }
}
