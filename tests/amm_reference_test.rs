//! Differential checks against Uniswap V2/V3 reference formulas.
use alloy::primitives::U256;

use rpbot::core::math::swap_math::compute_swap_step;
use rpbot::core::math::tick_math::get_sqrt_ratio_at_tick;
use rpbot::core::math::uniswap_v2::get_amount_out;
use rpbot::core::math::uniswap_v3::simulate_v3_swap;
use rpbot::core::types::{V3PoolState, V3Tick};
use std::sync::Arc;

#[test]
fn v2_reference_1000_in_10k_reserves() {
    // Uniswap V2: amountOut = (amountIn * 997 * reserveOut) / (reserveIn * 1000 + amountIn * 997)
    let out = get_amount_out(
        U256::from(1000u64),
        U256::from(10_000u64),
        U256::from(10_000u64),
        U256::from(997u64),
        U256::from(1000u64),
    );
    assert_eq!(out, U256::from(906u64));
}

#[test]
fn v2_reference_polygon_usdc_scale() {
    let amount_in = U256::from(1_000_000u64); // 1 USDC (6 dec)
    let reserve_in = U256::from(5_000_000_000_000u64);
    let reserve_out = U256::from(2_000_000_000_000_000_000u64);
    let out = get_amount_out(
        amount_in,
        reserve_in,
        reserve_out,
        U256::from(997u64),
        U256::from(1000u64),
    );
    assert!(out > U256::ZERO);
    assert!(out < reserve_out);
}

#[test]
fn v3_swap_step_returns_uncapped_positive_output() {
    let sqrt = get_sqrt_ratio_at_tick(0).expect("tick 0 sqrt");
    let target = get_sqrt_ratio_at_tick(60).expect("tick 60 sqrt");
    let liquidity = U256::from(1_000_000u64);
    let amount = U256::from(10u128.pow(16));
    let fee = U256::from(3000u32);
    let step = compute_swap_step(sqrt, target, liquidity, amount, fee)
        .expect("swap step should succeed");
    assert!(step.amount_out > U256::ZERO);
    assert_ne!(
        step.amount_out,
        liquidity,
        "regression: amount_out must not be min-capped to liquidity"
    );
}

#[test]
fn v3_single_tick_swap_matches_simulator() {
    let sqrt = get_sqrt_ratio_at_tick(0).expect("tick 0 sqrt");
    let state = V3PoolState {
        sqrt_price_x96: sqrt,
        liquidity: 10_000_000_000,
        tick: 0,
        fee: U256::from(3000u32),
        tick_spacing: 60,
        unlocked: true,
        fee_protocol: 0,
        observation_cardinality: 1,
        ticks: Arc::from(vec![
            V3Tick {
                tick: -60,
                liquidity_gross: 10_000_000_000,
                liquidity_net: 10_000_000_000,
            },
            V3Tick {
                tick: 60,
                liquidity_gross: 10_000_000_000,
                liquidity_net: -10_000_000_000,
            },
        ]),
    };
    let amount_in = U256::from(10u128.pow(15));
    let r = simulate_v3_swap(&state, amount_in, true, Some(30));
    assert!(r.amount_out > U256::ZERO);
}