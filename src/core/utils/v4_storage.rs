use alloy::primitives::{FixedBytes, U256, keccak256};

/// `_pools` mapping slot in PoolManager (StateLibrary.POOLS_SLOT).
pub const V4_POOLS_MAPPING_SLOT: u64 = 6;
/// Offset of `Pool.State.liquidity` from the pool state base slot.
pub const V4_LIQUIDITY_OFFSET: u64 = 3;

const SLOT0_MASK: U256 = U256::from_limbs([u64::MAX, u64::MAX, 0xffff_ffff, 0]);
const INT24_MASK: U256 = U256::from_limbs([0xffffff, 0, 0, 0]);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedV4Slot0 {
    pub sqrt_price_x96: U256,
    pub tick: i32,
    pub protocol_fee: u32,
    pub lp_fee: u32,
}

pub fn compute_v4_pool_state_slot(pool_id: &FixedBytes<32>) -> FixedBytes<32> {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(pool_id.as_slice());
    buf[60..64].copy_from_slice(&(V4_POOLS_MAPPING_SLOT as u32).to_be_bytes());
    keccak256(buf)
}

pub fn compute_v4_pool_field_slot(pool_id: &FixedBytes<32>, offset: u64) -> FixedBytes<32> {
    let base = U256::from_be_bytes(compute_v4_pool_state_slot(pool_id).0);
    let sum = (base + U256::from(offset)) & U256::MAX;
    FixedBytes::from(sum)
}

pub fn decode_v4_slot0(raw: U256) -> DecodedV4Slot0 {
    let sqrt_price_x96: U256 = raw & SLOT0_MASK;
    let tick_word: U256 = (raw >> U256::from(160u32)) & INT24_MASK;
    let tick_u = tick_word.to::<u32>() & 0xffffff;
    let tick = if tick_u >= 0x800000 {
        (tick_u as i32) - 0x1_000_000
    } else {
        tick_u as i32
    };
    let protocol_fee: u32 = ((raw >> U256::from(184u32)) & INT24_MASK).to();
    let lp_fee: u32 = ((raw >> U256::from(208u32)) & INT24_MASK).to();
    DecodedV4Slot0 {
        sqrt_price_x96,
        tick,
        protocol_fee,
        lp_fee,
    }
}

pub fn decode_v4_liquidity(raw: U256) -> u128 {
    (raw & U256::from(u128::MAX)).to::<u128>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_v4_fixture_slot0_and_liquidity() {
        let sqrt = U256::from(22_069_811_848_594_097_156u128);
        let tick = -301_888i32;
        let tick_word = if tick < 0 {
            (tick as i64 + 0x1_000_000) as u64
        } else {
            tick as u64
        };
        let slot0_packed = (U256::from(tick_word) << 160) | sqrt;
        let decoded = decode_v4_slot0(slot0_packed);
        assert_eq!(decoded.sqrt_price_x96, sqrt);
        assert_eq!(decoded.tick, tick);
        assert_eq!(decoded.lp_fee, 0);

        let liquidity = U256::from(13_286_789_630_483_468u128);
        assert_eq!(decode_v4_liquidity(liquidity), 13_286_789_630_483_468);
    }

    #[test]
    fn field_slots_are_deterministic() {
        let pool_id: FixedBytes<32> = FixedBytes::from_slice(&[0xabu8; 32]);
        let slot0 = compute_v4_pool_field_slot(&pool_id, 0);
        let liq = compute_v4_pool_field_slot(&pool_id, V4_LIQUIDITY_OFFSET);
        assert_ne!(slot0, liq);
    }
}
