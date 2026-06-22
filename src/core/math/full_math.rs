use ruint::aliases::U256;

#[inline]
pub fn mul_div(a: U256, b: U256, denominator: U256) -> Option<U256> {
    if denominator.is_zero() {
        return None;
    }
    Some(a * b / denominator)
}

#[inline]
pub fn mul_div_rounding_up(a: U256, b: U256, denominator: U256) -> Option<U256> {
    if denominator.is_zero() {
        return None;
    }
    let product = a * b;
    let result = product / denominator;
    if product % denominator > U256::ZERO {
        Some(result + U256::from(1))
    } else {
        Some(result)
    }
}

#[inline]
pub fn div_rounding_up(a: U256, b: U256) -> Option<U256> {
    if b.is_zero() {
        return None;
    }
    let result = a / b;
    if a % b > U256::ZERO {
        Some(result + U256::from(1))
    } else {
        Some(result)
    }
}

/// Wrapper returning U256::ZERO on zero denominator (avoids unwrap at call sites).
#[inline]
pub fn div_rounding_up_or_zero(a: U256, b: U256) -> U256 {
    div_rounding_up(a, b).unwrap_or(U256::ZERO)
}
