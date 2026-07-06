use alloy::primitives::{FixedBytes, U256, keccak256};

/// Offset of `Pool.State.liquidity` from the pool state base slot.
pub const V4_LIQUIDITY_OFFSET: u64 = 3;
/// Offset of `Pool.State.ticks` mapping from the pool state base slot.
pub const V4_TICKS_OFFSET: u64 = 4;
/// Offset of `Pool.State.tickBitmap` mapping from the pool state base slot.
pub const V4_TICK_BITMAP_OFFSET: u64 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedV4Slot0 {
    pub sqrt_price_x96: U256,
    pub tick: i32,
    pub protocol_fee: u32,
    pub lp_fee: u32,
}

#[must_use]
pub fn compute_v4_pool_field_slot(pool_id: &FixedBytes<32>, offset: u64) -> FixedBytes<32> {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(pool_id.as_slice());
    buf[60..64].copy_from_slice(&6u32.to_be_bytes());
    let base = U256::from_be_bytes(keccak256(buf).0);
    FixedBytes::from((base + U256::from(offset)) & U256::MAX)
}

#[must_use]
pub fn decode_v4_slot0(raw: U256) -> DecodedV4Slot0 {
    let m = U256::from_limbs([0xffffff, 0, 0, 0]);
    let sqrt_price_x96 = raw & U256::from_limbs([u64::MAX, u64::MAX, 0xffff_ffff, 0]);
    let tick_u: u32 = ((raw >> U256::from(160)) & m).to();
    let tick = tick_u as i32 - if tick_u >= 0x800000 { 0x1_000_000 } else { 0 };
    let protocol_fee: u32 = ((raw >> U256::from(184)) & m).to();
    let lp_fee: u32 = ((raw >> U256::from(208)) & m).to();
    DecodedV4Slot0 {
        sqrt_price_x96,
        tick,
        protocol_fee,
        lp_fee,
    }
}

#[must_use]
pub fn decode_v4_liquidity(raw: U256) -> u128 {
    (raw & U256::from(u128::MAX)).to::<u128>()
}

#[must_use]
pub fn decode_v4_tick_liquidity(raw: U256) -> (u128, i128) {
    let liquidity_gross = (raw & U256::from(u128::MAX)).to::<u128>();
    let net_bytes = raw.to_be_bytes::<32>();
    // net_bytes is always exactly 32 bytes; first 16 are the signed net in BE.
    let mut net = [0u8; 16];
    net.copy_from_slice(&net_bytes[..16]);
    let liquidity_net = i128::from_be_bytes(net);
    (liquidity_gross, liquidity_net)
}

#[must_use]
pub fn compute_v4_tick_bitmap_slot(pool_id: &FixedBytes<32>, word: i16) -> FixedBytes<32> {
    let mapping_base = compute_v4_pool_field_slot(pool_id, V4_TICK_BITMAP_OFFSET);
    mapping_slot(&sign_extend_i16(word), mapping_base)
}

#[must_use]
pub fn compute_v4_tick_info_slot(pool_id: &FixedBytes<32>, tick: i32) -> FixedBytes<32> {
    let mapping_base = compute_v4_pool_field_slot(pool_id, V4_TICKS_OFFSET);
    mapping_slot(&sign_extend_i24(tick), mapping_base)
}

fn mapping_slot(key: &[u8; 32], mapping_base: FixedBytes<32>) -> FixedBytes<32> {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(key);
    buf[32..64].copy_from_slice(mapping_base.as_slice());
    keccak256(buf)
}

fn sign_extend_i16(value: i16) -> [u8; 32] {
    let mut out = [0u8; 32];
    if value < 0 {
        out.fill(0xff);
    }
    out[30..32].copy_from_slice(&value.to_be_bytes());
    out
}

fn sign_extend_i24(value: i32) -> [u8; 32] {
    let mut out = [0u8; 32];
    if value < 0 {
        out.fill(0xff);
    }
    out[29..32].copy_from_slice(&value.to_be_bytes()[1..4]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_v4_liquidity_zero() {
        assert_eq!(decode_v4_liquidity(U256::ZERO), 0);
    }

    #[test]
    fn tick_info_slots_differ_by_tick() {
        let pool_id = FixedBytes::<32>::ZERO;
        let a = compute_v4_tick_info_slot(&pool_id, -60);
        let b = compute_v4_tick_info_slot(&pool_id, 60);
        assert_ne!(a, b);
    }
}
