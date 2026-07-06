//! Live oracle smoke tests — require network.
//!
//! Run: `cargo test --test oracle_live_test -- --ignored --nocapture`

use alloy::primitives::{Address, address};
use rpbot::core::constants::{MIN_TOKEN_TO_MATIC_RATE, WMATIC};
use rpbot::services::oracle::price_oracle::PriceOracle;

fn hermes_oracle() -> PriceOracle {
    PriceOracle::new(
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("http client"),
        "https://hermes.pyth.network".to_string(),
    )
}

#[tokio::test]
#[ignore = "live network — run: cargo test --test oracle_live_test -- --ignored"]
async fn pyth_matic_usd_feed_resolves() {
    let oracle = hermes_oracle();
    let usd = oracle.get_matic_usd_offline().await;
    assert!(
        usd > 0.01 && usd < 100.0,
        "MATIC/USD out of sane range: {usd}"
    );
    assert!(
        oracle.cached_matic_usd().is_some(),
        "MATIC/USD should be cached after fetch"
    );
    let rate = oracle
        .token_matic_rate_per_unit_integer(&WMATIC)
        .expect("WMATIC integer rate after Pyth fetch");
    assert!(rate >= MIN_TOKEN_TO_MATIC_RATE);
}

#[tokio::test]
#[ignore = "live network — run: cargo test --test oracle_live_test -- --ignored"]
async fn pyth_usdc_prefetch_enables_integer_matic_rate() {
    let oracle = hermes_oracle();
    let usdc: Address = address!("0x2791bca1f2de4661ed88a30c99a7a9489c09eb3f");
    let _ = oracle.get_matic_usd_offline().await;
    oracle.prefetch_token_usd_offline(&[usdc]).await;
    let usd = oracle.token_usd(&usdc).expect("USDC/USD from Pyth");
    assert!(usd > 0.5 && usd < 2.0, "USDC/USD out of sane range: {usd}");
    let rate = oracle
        .token_matic_rate_per_unit_integer(&usdc)
        .expect("USDC/MATIC integer rate");
    assert!(rate >= MIN_TOKEN_TO_MATIC_RATE);
}
