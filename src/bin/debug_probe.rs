//! Offline probe: exercises math/sim/eval pipeline with synthetic V2 triangle.
//! Run: `DEBUG_AGENT=1 cargo run --bin debug_probe`

use std::collections::HashMap;

use alloy::primitives::Address;
use ruint::aliases::U256;
use rustc_hash::FxHashMap;

use rpbot::core::types::{FlashLoanSource, PoolState, ProtocolType, V2PoolState};
use rpbot::orchestrator::hf_eval::{HfEvalInput, evaluate_cycles_parallel};
use rpbot::pipeline::arena::StateArena;
use rpbot::pipeline::graph::{build_graph, pool_meta_from_pair};
use rpbot::pipeline::ternary::optimize_cycle;
use rpbot::pipeline::cycle_finder;
use rpbot::pipeline::local_sim;
use rpbot::services::execution::profit::ProfitEvalContext;
use rpbot::services::execution::GasOracle;
use rpbot::services::execution::flash_liquidity::FlashLiquidityCache;

fn main() {
    println!("debug_probe: building synthetic V2 triangle…");

    let mut arena = StateArena::new();
    let t0 = arena.register_token(Address::repeat_byte(1));
    let t1 = arena.register_token(Address::repeat_byte(2));
    let t2 = arena.register_token(Address::repeat_byte(3));
    let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));

    let v2 = |r0: U256, r1: U256| {
        PoolState::V2(V2PoolState {
            reserve0: r0,
            reserve1: r1,
            fee: U256::ZERO,
            fee_denominator: U256::ZERO,
        })
    };

    let p01 = arena.register_pool(
        Address::repeat_byte(0x10),
        v2(reserve, reserve * U256::from(2u8)),
    );
    let p12 = arena.register_pool(
        Address::repeat_byte(0x11),
        v2(reserve, reserve * U256::from(2u8)),
    );
    let p20 = arena.register_pool(
        Address::repeat_byte(0x12),
        v2(reserve * U256::from(2u8), reserve),
    );

    let pools = vec![
        pool_meta_from_pair(p01, ProtocolType::UniswapV2, t0, t1, Some(30)),
        pool_meta_from_pair(p12, ProtocolType::UniswapV2, t1, t2, Some(30)),
        pool_meta_from_pair(p20, ProtocolType::UniswapV2, t2, t0, Some(30)),
    ];
    let graph = build_graph(&arena, &pools);
    let cycles = cycle_finder::find_cycles(&arena, &graph, 4, 50);
    let cycle = cycles
        .into_iter()
        .find(|c| c.hop_count == 3)
        .expect("expected 3-hop cycle");

    let mut rates = FxHashMap::default();
    rates.insert(t0, U256::from(10u128).pow(U256::from(18)));

    let gas_price = U256::from(30_000_000_000u64);
    let profit_ctx = ProfitEvalContext::for_cycle(
        t0,
        &arena,
        &rates,
        &HashMap::new(),
        gas_price,
        50,
        FlashLoanSource::Balancer,
    );
    let opt = optimize_cycle(
        &arena,
        &cycle,
        &rates,
        &HashMap::new(),
        None,
        Some(12),
        None,
        &profit_ctx,
        None,
    )
    .expect("optimize");

    let sim = local_sim::simulate_route_minimal(&arena, &cycle.edges, opt.optimal_input)
        .expect("sim");
    println!(
        "optimize: input={} profit={} gas={}",
        opt.optimal_input, sim.profit, sim.total_gas
    );

    let gas_oracle = GasOracle::default();
    let flash_liquidity = FlashLiquidityCache::new();
    let input = HfEvalInput {
        arena: &arena,
        token_to_matic_rates: &rates,
        token_decimals: &HashMap::new(),
        gas_oracle: &gas_oracle,
        brent_iters: 12,
        min_profit_matic: U256::ZERO,
        min_profit_roi_bps: 0,
        gas_price,
        slippage_bps: 50,
        flash_source: FlashLoanSource::Balancer,
        max_flash_loan_usd: 50_000,
        safety_multiplier_bps: 30_000,
        flash_liquidity: &flash_liquidity,
    };

    let results = evaluate_cycles_parallel(&[cycle], &input, &FxHashMap::default());
    for r in &results {
        println!(
            "eval: should_execute={} net_matic={} slippage_bps={}",
            r.assessment.should_execute,
            r.assessment.net_profit_after_gas_matic_wei,
            r.effective_slippage_bps
        );
    }

    println!("debug_probe done — check .cursor/debug-5a93f5.log");
}
