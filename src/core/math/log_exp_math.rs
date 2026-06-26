use ruint::aliases::U256;

use super::fixed_point::ONE as ONE_18;

fn one_20() -> U256 {
    U256::from_limbs([7766279631452241920, 5, 0, 0])
}

fn one_36() -> U256 {
    ONE_18 * ONE_18
}

fn max_natural_exponent() -> U256 {
    U256::from_limbs([15145670503932362752, 7047314121155778, 0, 0])
}

const LN_36_LOWER_BOUND: U256 = U256::from_limbs([900_000_000_000_000_000, 0, 0, 0]);
const LN_36_UPPER_BOUND: U256 = U256::from_limbs([1_100_000_000_000_000_000, 0, 0, 0]);

fn ln_36(x: U256) -> U256 {
    let o36 = one_36();
    let x = x * ONE_18;
    let z = ((x - o36) * o36) / (x + o36);
    let z_squared = (z * z) / o36;
    let mut num = z;
    let mut series_sum = num;
    for div in [3u64, 5, 7, 9, 11, 13, 15] {
        num = (num * z_squared) / o36;
        series_sum += num / U256::from(div);
    }
    series_sum * U256::from(2)
}

fn ln(a: U256) -> U256 {
    if a < ONE_18 {
        return -ln((ONE_18 * ONE_18) / a);
    }

    let mut sum = U256::ZERO;
    let mut av = a;
    if av >= LN_A0 {
        av /= LN_A0;
        sum += LN_X0;
    }
    if av >= LN_A1 {
        av /= LN_A1;
        sum += LN_X1;
    }

    sum *= U256::from(100);
    av *= U256::from(100);

    let o20 = one_20();
    for &(threshold, factor) in &LN_STEPS {
        if av >= threshold {
            av = (av * o20) / factor;
            sum += threshold;
        }
    }

    let z = ((av - o20) * o20) / (av + o20);
    let z_squared = (z * z) / o20;
    let mut num = z;
    let mut series_sum = num;
    for div in [3u64, 5, 7, 9, 11] {
        num = (num * z_squared) / o20;
        series_sum += num / U256::from(div);
    }
    (sum + series_sum * U256::from(2)) / U256::from(100)
}

pub fn log_exp_ln(a: U256) -> U256 {
    if a.is_zero() {
        return U256::ZERO;
    }
    if a > LN_36_LOWER_BOUND && a < LN_36_UPPER_BOUND {
        return ln_36(a) / ONE_18;
    }
    ln(a)
}

pub fn log_exp_exp(x: U256) -> U256 {
    if x > max_natural_exponent() {
        return U256::MAX;
    }

    let (mut xv, first_an) = if x >= EXP_X0 {
        (x - EXP_X0, EXP_A0)
    } else if x >= EXP_X1 {
        (x - EXP_X1, EXP_A1)
    } else {
        (x, U256::from(1))
    };

    xv *= U256::from(100);
    let o20 = one_20();
    let mut product = o20;

    for &(threshold, factor) in &EXP_STEPS {
        if xv >= threshold {
            xv -= threshold;
            product = (product * factor) / o20;
        }
    }

    let mut series_sum = o20;
    let mut term = xv;
    series_sum += term;
    for div in 2u64..=12 {
        term = ((term * xv) / o20) / U256::from(div);
        series_sum += term;
    }

    (((product * series_sum) / o20) * first_an) / U256::from(100)
}

pub fn log_exp_pow(x: U256, y: U256) -> U256 {
    if y.is_zero() {
        return ONE_18;
    }
    if x.is_zero() {
        return U256::ZERO;
    }
    let logx_times_y = log_exp_ln(x) * y / ONE_18;
    if logx_times_y > max_natural_exponent() {
        return U256::MAX;
    }
    log_exp_exp(logx_times_y)
}

// Pre-computed U256::from_limbs representations of Balancer V2 LogExpMath literals.
// Verified against the original Solidity string literals.

const LN_X0: U256 = U256::from_limbs([0, 0, 0, 128]);
const LN_X1: U256 = U256::from_limbs([0, 0, 0, 64]);

const LN_A0: U256 = U256::from_limbs([
    171843153341448192,
    17670479068478958691,
    114249481722274167,
    0,
]);
const LN_A1: U256 = U256::from_limbs([17696838799657497472, 338008108, 0, 0]);

const LN_STEPS: [(U256, U256); 10] = [
    (
        U256::from_limbs([8713275248247570432, 173, 0, 0]),
        U256::from_limbs([17871857890508685312, 428059064879743, 0, 0]),
    ),
    (
        U256::from_limbs([13580009660978561024, 86, 0, 0]),
        U256::from_limbs([12108528782385981184, 48171701, 0, 0]),
    ),
    (
        U256::from_limbs([6790004830489280512, 43, 0, 0]),
        U256::from_limbs([14861217100182911056, 16159, 0, 0]),
    ),
    (
        U256::from_limbs([12618374452099416064, 21, 0, 0]),
        U256::from_limbs([18025501570106181090, 295, 0, 0]),
    ),
    (
        U256::from_limbs([15532559262904483840, 10, 0, 0]),
        U256::from_limbs([1035846944682958083, 40, 0, 0]),
    ),
    (
        U256::from_limbs([7766279631452241920, 5, 0, 0]),
        U256::from_limbs([13573765813970800912, 14, 0, 0]),
    ),
    (
        U256::from_limbs([13106511852580896768, 2, 0, 0]),
        U256::from_limbs([17298174480336401757, 8, 0, 0]),
    ),
    (
        U256::from_limbs([6553255926290448384, 1, 0, 0]),
        U256::from_limbs([17722077226516838711, 6, 0, 0]),
    ),
    (
        U256::from_limbs([12500000000000000000, 0, 0, 0]),
        U256::from_limbs([2634380864425321987, 6, 0, 0]),
    ),
    (
        U256::from_limbs([6250000000000000000, 0, 0, 0]),
        U256::from_limbs([14215725523238184876, 5, 0, 0]),
    ),
];

const EXP_X0: U256 = U256::from_limbs([0, 0, 0, 128]);
const EXP_X1: U256 = U256::from_limbs([0, 0, 0, 64]);

const EXP_A0: U256 = U256::from_limbs([
    171843153341448192,
    17670479068478958691,
    114249481722274167,
    0,
]);
const EXP_A1: U256 = U256::from_limbs([17696838799657497472, 338008108, 0, 0]);

const EXP_STEPS: [(U256, U256); 8] = [
    (
        U256::from_limbs([8713275248247570432, 173, 0, 0]),
        U256::from_limbs([17871857890508685312, 428059064879743, 0, 0]),
    ),
    (
        U256::from_limbs([13580009660978561024, 86, 0, 0]),
        U256::from_limbs([12108528782385981184, 48171701, 0, 0]),
    ),
    (
        U256::from_limbs([6790004830489280512, 43, 0, 0]),
        U256::from_limbs([14861217100182911056, 16159, 0, 0]),
    ),
    (
        U256::from_limbs([12618374452099416064, 21, 0, 0]),
        U256::from_limbs([18025501570106181090, 295, 0, 0]),
    ),
    (
        U256::from_limbs([15532559262904483840, 10, 0, 0]),
        U256::from_limbs([1035846944682958083, 40, 0, 0]),
    ),
    (
        U256::from_limbs([7766279631452241920, 5, 0, 0]),
        U256::from_limbs([13573765813970800912, 14, 0, 0]),
    ),
    (
        U256::from_limbs([13106511852580896768, 2, 0, 0]),
        U256::from_limbs([17298174480336401757, 8, 0, 0]),
    ),
    (
        U256::from_limbs([6553255926290448384, 1, 0, 0]),
        U256::from_limbs([17722077226516838711, 6, 0, 0]),
    ),
];
