use std::time::{SystemTime, UNIX_EPOCH};

use ruint::aliases::U256;

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn u256_to_f64(v: U256) -> f64 {
    let limbs = v.as_limbs();
    if limbs[1] == 0 && limbs[2] == 0 && limbs[3] == 0 {
        limbs[0] as f64
    } else {
        let hi = limbs[3] as f64;
        let mid_hi = limbs[2] as f64;
        let mid_lo = limbs[1] as f64;
        let lo = limbs[0] as f64;
        hi.mul_add(
            2f64.powi(192),
            mid_hi.mul_add(2f64.powi(128), mid_lo.mul_add(2f64.powi(64), lo)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_ms_monotonic() {
        let a = now_ms();
        let b = now_ms();
        assert!(b >= a);
    }

    #[test]
    fn test_u256_to_f64_zero() {
        assert_eq!(u256_to_f64(U256::ZERO), 0.0);
    }
}
