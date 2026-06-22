use ruint::aliases::U256;

use crate::core::types::DodoPoolState;

use super::fixed_point::{ONE, mul_down as mul_floor};
use super::int_sqrt::bigint_sqrt;

fn one2() -> U256 {
    ONE * ONE
}

pub const DODO_RSTATE_ONE: u8 = 0;
pub const DODO_RSTATE_ABOVE_ONE: u8 = 1;
pub const DODO_RSTATE_BELOW_ONE: u8 = 2;

fn div_floor(a: U256, b: U256) -> U256 {
    if b.is_zero() {
        return U256::ZERO;
    }
    (a * ONE) / b
}

fn div_ceil(a: U256, b: U256) -> U256 {
    if b.is_zero() {
        return U256::ZERO;
    }
    (a * ONE + b - U256::from(1)) / b
}

fn reciprocal_floor(target: U256) -> U256 {
    if target.is_zero() {
        return U256::ZERO;
    }
    one2() / target
}

fn general_integrate(v0: U256, v1: U256, v2: U256, i: U256, k: U256) -> U256 {
    if v0.is_zero() || v1.is_zero() || v2.is_zero() || v1 < v2 || i.is_zero() || k > ONE {
        return U256::ZERO;
    }

    let fair_amount = i * (v1 - v2);
    if k.is_zero() {
        return fair_amount / ONE;
    }

    let v0v0v1v2 = div_floor(v0 * v0, v1 * v2);
    let penalty = mul_floor(k, v0v0v1v2);
    ((ONE - k + penalty) * fair_amount) / one2()
}

fn solve_quadratic_function_for_trade(v0: U256, v1: U256, delta: U256, i: U256, k: U256) -> U256 {
    if v0.is_zero() || v1.is_zero() || delta.is_zero() || i.is_zero() || k > ONE {
        return U256::ZERO;
    }

    if k.is_zero() {
        let linear = mul_floor(i, delta);
        return if linear > v1 { v1 } else { linear };
    }

    if k == ONE {
        let idelta = i * delta;
        let temp = if idelta.is_zero() {
            U256::ZERO
        } else {
            (idelta * v1) / (v0 * v0)
        };
        return if temp.is_zero() {
            U256::ZERO
        } else {
            (v1 * temp) / (temp + ONE)
        };
    }

    let part2 = (k * v0 * v0) / v1 + (i * delta) / ONE;
    let mut b_abs = (ONE - k) * v1;
    let mut b_sig = false;
    if b_abs >= part2 {
        b_abs -= part2;
    } else {
        b_abs = part2 - b_abs;
        b_sig = true;
    }
    b_abs /= ONE;

    let mut square_root = mul_floor((ONE - k) * U256::from(4), mul_floor(k, v0) * v0);
    square_root = bigint_sqrt(b_abs * b_abs + square_root);

    let denominator = (ONE - k) * U256::from(2);
    if denominator.is_zero() {
        return U256::ZERO;
    }

    let numerator = if b_sig {
        if square_root <= b_abs {
            return U256::ZERO;
        }
        square_root - b_abs
    } else {
        b_abs + square_root
    };

    let v2 = div_ceil(numerator, denominator);
    if v2 > v1 { U256::ZERO } else { v1 - v2 }
}

pub fn get_dodo_gross_amount_out(
    state: &DodoPoolState,
    amount_in: U256,
    base_to_quote: bool,
) -> U256 {
    if amount_in.is_zero() {
        return U256::ZERO;
    }

    let b = state.base_reserve;
    let q = state.quote_reserve;
    let b0 = if state.base_target.is_zero() {
        b
    } else {
        state.base_target
    };
    let q0 = if state.quote_target.is_zero() {
        q
    } else {
        state.quote_target
    };
    let r = state.r_status;
    let i = state.i;
    let k = state.k;

    if i.is_zero() || k > ONE || b.is_zero() || q.is_zero() || b0.is_zero() || q0.is_zero() {
        return U256::ZERO;
    }

    if base_to_quote {
        return match r {
            DODO_RSTATE_ONE => solve_quadratic_function_for_trade(q0, q0, amount_in, i, k),
            DODO_RSTATE_ABOVE_ONE => {
                if b0 < b || q < q0 {
                    return U256::ZERO;
                }
                let back_to_one_pay_base = b0 - b;
                let back_to_one_receive_quote = q - q0;
                if amount_in < back_to_one_pay_base {
                    let receive_quote = general_integrate(b0, b + amount_in, b, i, k);
                    return receive_quote.min(back_to_one_receive_quote);
                }
                if amount_in == back_to_one_pay_base {
                    return back_to_one_receive_quote;
                }
                back_to_one_receive_quote
                    + solve_quadratic_function_for_trade(
                        q0,
                        q0,
                        amount_in - back_to_one_pay_base,
                        i,
                        k,
                    )
            }
            _ => solve_quadratic_function_for_trade(q0, q, amount_in, i, k),
        };
    }

    let inverse_i = reciprocal_floor(i);
    if inverse_i.is_zero() {
        return U256::ZERO;
    }

    match r {
        DODO_RSTATE_ONE => solve_quadratic_function_for_trade(b0, b0, amount_in, inverse_i, k),
        DODO_RSTATE_ABOVE_ONE => solve_quadratic_function_for_trade(b0, b, amount_in, inverse_i, k),
        DODO_RSTATE_BELOW_ONE => {
            if q0 < q || b < b0 {
                return U256::ZERO;
            }
            let back_to_one_pay_quote = q0 - q;
            let back_to_one_receive_base = b - b0;
            if amount_in < back_to_one_pay_quote {
                let receive_base = general_integrate(q0, q + amount_in, q, inverse_i, k);
                return receive_base.min(back_to_one_receive_base);
            }
            if amount_in == back_to_one_pay_quote {
                return back_to_one_receive_base;
            }
            back_to_one_receive_base
                + solve_quadratic_function_for_trade(
                    b0,
                    b0,
                    amount_in - back_to_one_pay_quote,
                    inverse_i,
                    k,
                )
        }
        _ => U256::ZERO,
    }
}

pub fn get_dodo_amount_out(state: &DodoPoolState, amount_in: U256, base_to_quote: bool) -> U256 {
    let gross = get_dodo_gross_amount_out(state, amount_in, base_to_quote);
    if gross.is_zero() {
        return U256::ZERO;
    }

    let lp = state.lp_fee_rate;
    let mt = state.mt_fee_rate;
    if lp + mt >= ONE {
        return U256::ZERO;
    }

    gross - mul_floor(gross, lp) - mul_floor(gross, mt)
}

pub fn simulate_dodo_swap(state: &DodoPoolState, amount_in: U256, base_to_quote: bool) -> U256 {
    get_dodo_amount_out(state, amount_in, base_to_quote)
}

pub fn estimate_dodo_hop_capacity(state: &DodoPoolState, base_to_quote: bool) -> U256 {
    let b = state.base_reserve;
    let q = state.quote_reserve;
    let b0 = if state.base_target.is_zero() {
        b
    } else {
        state.base_target
    };
    let q0 = if state.quote_target.is_zero() {
        q
    } else {
        state.quote_target
    };
    let r = state.r_status;

    if b.is_zero() || q.is_zero() || b0.is_zero() || q0.is_zero() {
        return U256::ZERO;
    }

    let reserve_fraction = |reserve: U256| {
        let tenth = reserve / U256::from(10);
        if tenth > U256::ZERO { tenth } else { reserve }
    };

    if base_to_quote {
        return match r {
            DODO_RSTATE_ABOVE_ONE => {
                if b0 < b || q < q0 {
                    U256::ZERO
                } else {
                    b0 - b
                }
            }
            DODO_RSTATE_BELOW_ONE => reserve_fraction(b),
            _ => reserve_fraction(if b0 > b { b0 - b } else { b }),
        };
    }

    match r {
        DODO_RSTATE_BELOW_ONE => {
            if q0 < q || b < b0 {
                U256::ZERO
            } else {
                q0 - q
            }
        }
        DODO_RSTATE_ABOVE_ONE => reserve_fraction(q),
        _ => reserve_fraction(if q0 > q { q0 - q } else { q }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caps_above_one_base_sells() {
        let state = DodoPoolState {
            base_reserve: U256::from(90u128) * ONE,
            quote_reserve: U256::from(105u128) * ONE,
            base_target: U256::from(100u128) * ONE,
            quote_target: U256::from(100u128) * ONE,
            i: ONE,
            k: ONE / U256::from(10),
            r_status: DODO_RSTATE_ABOVE_ONE,
            lp_fee_rate: U256::ZERO,
            mt_fee_rate: U256::ZERO,
        };
        assert_eq!(
            estimate_dodo_hop_capacity(&state, true),
            U256::from(10u128) * ONE
        );
    }

    #[test]
    fn caps_below_one_quote_sells() {
        let state = DodoPoolState {
            base_reserve: U256::from(105u128) * ONE,
            quote_reserve: U256::from(90u128) * ONE,
            base_target: U256::from(100u128) * ONE,
            quote_target: U256::from(100u128) * ONE,
            i: ONE,
            k: ONE / U256::from(10),
            r_status: DODO_RSTATE_BELOW_ONE,
            lp_fee_rate: U256::ZERO,
            mt_fee_rate: U256::ZERO,
        };
        assert_eq!(
            estimate_dodo_hop_capacity(&state, false),
            U256::from(10u128) * ONE
        );
    }
}
