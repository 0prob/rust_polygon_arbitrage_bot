use ruint::aliases::U256;

pub fn bigint_sqrt(value: U256) -> U256 {
    value.root(2)
}
