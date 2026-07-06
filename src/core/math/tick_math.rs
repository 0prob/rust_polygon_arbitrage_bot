use alloy::primitives::U256;

pub const MIN_TICK: i32 = -887_272;
pub const MAX_TICK: i32 = 887_272;
pub const MIN_SQRT_RATIO: U256 = U256::from_limbs([4_295_128_739, 0, 0, 0]);
pub const MAX_SQRT_RATIO: U256 =
    U256::from_limbs([3_402_823_669, 2_147_483_647, 2_147_483_647, 146_144_670]);
/// `MAX_SQRT_RATIO - 1` — the exclusion bound for non-zero-for-one swaps.
pub const MAX_SQRT_RATIO_EXCLUSIVE: U256 =
    U256::from_limbs([3_402_823_668, 2_147_483_647, 2_147_483_647, 146_144_670]);

const LOW_32_MASK: U256 = U256::from_limbs([u32::MAX as u64, 0, 0, 0]);

#[must_use]
pub fn normalize_tick_search_bounds(min_tick: i32, max_tick: i32) -> (i32, i32) {
    let lo = MIN_TICK.max(min_tick.min(max_tick));
    let hi = MAX_TICK.min(max_tick.max(min_tick));
    (lo, hi)
}

fn mul_shift(r: U256, m: U256) -> U256 {
    (r * m) >> 128
}

/// Calculates sqrt(1.0001^tick) * 2^96.
#[must_use]
pub fn get_sqrt_ratio_at_tick(tick: i32) -> Option<U256> {
    if !(MIN_TICK..=MAX_TICK).contains(&tick) {
        return None;
    }

    let abs_tick = tick.unsigned_abs();

    let mut ratio = if abs_tick & 1 != 0 {
        TICK_FFFCB933BD
    } else {
        TICK_1000000000
    };

    if abs_tick & 2 != 0 {
        ratio = mul_shift(ratio, TICK_FFF9727237);
    }
    if abs_tick & 4 != 0 {
        ratio = mul_shift(ratio, TICK_FFF2E50F5F);
    }
    if abs_tick & 8 != 0 {
        ratio = mul_shift(ratio, TICK_FFE5CACA7E);
    }
    if abs_tick & 16 != 0 {
        ratio = mul_shift(ratio, TICK_FFCB9843D6);
    }
    if abs_tick & 32 != 0 {
        ratio = mul_shift(ratio, TICK_FF973B41FA);
    }
    if abs_tick & 64 != 0 {
        ratio = mul_shift(ratio, TICK_FF2EA16466);
    }
    if abs_tick & 128 != 0 {
        ratio = mul_shift(ratio, TICK_FE5DEE046A);
    }
    if abs_tick & 256 != 0 {
        ratio = mul_shift(ratio, TICK_FCBE86C790);
    }
    if abs_tick & 512 != 0 {
        ratio = mul_shift(ratio, TICK_F987A7253A);
    }
    if abs_tick & 1024 != 0 {
        ratio = mul_shift(ratio, TICK_F3392B0822);
    }
    if abs_tick & 2048 != 0 {
        ratio = mul_shift(ratio, TICK_E7159475A2);
    }
    if abs_tick & 4096 != 0 {
        ratio = mul_shift(ratio, TICK_D097F3BDFD);
    }
    if abs_tick & 8192 != 0 {
        ratio = mul_shift(ratio, TICK_A9F746462D);
    }
    if abs_tick & 16384 != 0 {
        ratio = mul_shift(ratio, TICK_70D869A156);
    }
    if abs_tick & 32768 != 0 {
        ratio = mul_shift(ratio, TICK_31BE135F97);
    }
    if abs_tick & 65536 != 0 {
        ratio = mul_shift(ratio, TICK_9AA508B5B7);
    }
    if abs_tick & 131072 != 0 {
        ratio = mul_shift(ratio, TICK_5D6AF8DEDB);
    }
    if abs_tick & 262144 != 0 {
        ratio = mul_shift(ratio, TICK_2216E584F5);
    }
    if abs_tick & 524288 != 0 {
        ratio = mul_shift(ratio, TICK_48A170391F);
    }

    if tick > 0 {
        ratio = U256::MAX / ratio;
    }

    let shifted = ratio >> 32;
    Some(if ratio & LOW_32_MASK == U256::ZERO {
        shifted
    } else {
        shifted + U256::ONE
    })
}

#[must_use]
pub fn get_tick_at_sqrt_ratio_in_range(
    sqrt_price_x96: U256,
    min_tick: i32,
    max_tick: i32,
) -> Option<i32> {
    if sqrt_price_x96 < MIN_SQRT_RATIO || sqrt_price_x96 >= MAX_SQRT_RATIO {
        return None;
    }

    let (lo, hi) = normalize_tick_search_bounds(min_tick, max_tick);
    let mut low = lo;
    let mut high = hi;
    let mut answer = lo;

    while low <= high {
        let mid = i32::midpoint(low, high);
        if get_sqrt_ratio_at_tick(mid)? <= sqrt_price_x96 {
            answer = mid;
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }

    Some(answer)
}

// Pre-computed U256 values for Uniswap V3 sqrt ratio tick constants.
// Verified against the original hex string literals.

const TICK_FFFCB933BD: U256 = U256::from_limbs([12262481743371124737, 18445821805675392311, 0, 0]);
const TICK_1000000000: U256 = U256::from_limbs([0, 0, 1, 0]);
const TICK_FFF9727237: U256 = U256::from_limbs([6459403834229662010, 18444899583751176498, 0, 0]);
const TICK_FFF2E50F5F: U256 = U256::from_limbs([17226890335427755468, 18443055278223354162, 0, 0]);
const TICK_FFE5CACA7E: U256 = U256::from_limbs([2032852871939366096, 18439367220385604838, 0, 0]);
const TICK_FFCB9843D6: U256 = U256::from_limbs([14545316742740207172, 18431993317065449817, 0, 0]);
const TICK_FF973B41FA: U256 = U256::from_limbs([5129152022828963008, 18417254355718160513, 0, 0]);
const TICK_FF2EA16466: U256 = U256::from_limbs([4894419605888772193, 18387811781193591352, 0, 0]);
const TICK_FE5DEE046A: U256 = U256::from_limbs([1280255884321894483, 18329067761203520168, 0, 0]);
const TICK_FCBE86C790: U256 = U256::from_limbs([15924666964335305636, 18212142134806087854, 0, 0]);
const TICK_F987A7253A: U256 = U256::from_limbs([8010504389359918676, 17980523815641551639, 0, 0]);
const TICK_F3392B0822: U256 = U256::from_limbs([10668036004952895731, 17526086738831147013, 0, 0]);
const TICK_E7159475A2: U256 = U256::from_limbs([4878133418470705625, 16651378430235024244, 0, 0]);
const TICK_D097F3BDFD: U256 = U256::from_limbs([9537173718739605541, 15030750278693429944, 0, 0]);
const TICK_A9F746462D: U256 = U256::from_limbs([9972618978014552549, 12247334978882834399, 0, 0]);
const TICK_70D869A156: U256 = U256::from_limbs([10428997489610666743, 8131365268884726200, 0, 0]);
const TICK_31BE135F97: U256 = U256::from_limbs([9305304367709015974, 3584323654723342297, 0, 0]);
const TICK_9AA508B5B7: U256 = U256::from_limbs([14301143598189091785, 696457651847595233, 0, 0]);
const TICK_5D6AF8DEDB: U256 = U256::from_limbs([7393154844743099908, 26294789957452057, 0, 0]);
const TICK_2216E584F5: U256 = U256::from_limbs([2209338891292245656, 37481735321082, 0, 0]);
const TICK_48A170391F: U256 = U256::from_limbs([10518117631919034274, 76158723, 0, 0]);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_sqrt_ratio_at_tick_zero() {
        assert!(get_sqrt_ratio_at_tick(0).is_some());
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used)]
mod proptests {
    use proptest::prelude::*;
    use super::*;

    proptest! {
        #[test]
        fn tick_sqrt_price_round_trip(
            tick in (MIN_TICK + 1)..=MAX_TICK,
        ) {
            if let Some(sqrt_price) = get_sqrt_ratio_at_tick(tick) {
                let round_trip = get_tick_at_sqrt_ratio_in_range(
                    sqrt_price,
                    MIN_TICK,
                    MAX_TICK,
                );
                prop_assert!(round_trip.is_some(),
                    "round-trip None for tick={}", tick);
                let rt = round_trip.unwrap();
                prop_assert!(rt.abs_diff(tick) <= 1,
                    "round-trip error: tick={}, got={}", tick, rt);
            }
        }

        #[test]
        fn sqrt_price_monotonic(
            tick_a in (MIN_TICK + 1)..=MAX_TICK,
            tick_b in (MIN_TICK + 1)..=MAX_TICK,
        ) {
            if tick_a >= tick_b { return Ok(()); }
            let px_a = get_sqrt_ratio_at_tick(tick_a);
            let px_b = get_sqrt_ratio_at_tick(tick_b);
            if let (Some(a), Some(b)) = (px_a, px_b) {
                prop_assert!(b > a,
                    "higher tick {} has lower price than lower tick {}: {:?} <= {:?}",
                    tick_b, tick_a, b, a);
            }
        }
    }
}
