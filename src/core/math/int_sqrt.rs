use ruint::aliases::U256;

pub fn bigint_sqrt(value: U256) -> U256 {
    if value < U256::from(2) {
        return value;
    }
    let bit_len = value.bit_len();
    let mut x = U256::from(1) << ((bit_len / 2) + 1);
    let mut y = (x + value / x) / U256::from(2);
    while y < x {
        x = y;
        y = (x + value / x) / U256::from(2);
    }
    x
}
