//! Live oracle smoke tests — require network.
//!
//! Run: `cargo test --test oracle_live_test -- --ignored --nocapture`

use alloy::primitives::{Address, address};
use rpbot::core::constants::{MIN_TOKEN_TO_MATIC_RATE, WMATIC};
use rpbot::services::oracle::price_oracle::PriceOracle;
use serde::Deserialize;

fn hermes_oracle() -> PriceOracle {
    PriceOracle::new(
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("http client"),
        "https://hermes.pyth.network".to_string(),
        10_000,
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

#[tokio::test]
#[ignore = "live network — run: cargo test --test oracle_live_test -- --ignored"]
async fn pyth_extended_polygon_tokens_prefetch_enable_integer_matic_rates() {
    let oracle = hermes_oracle();
    let feeds: [(Address, &str, &str); 7] = [
        (
            address!("0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39"),
            "8ac0c70fff57e9aefdf5edf44b51d62c2d433653cbb2cf5cc06bb115af04d221",
            "Crypto.LINK/USD",
        ),
        (
            address!("0xd6df932a45c0f255f85145f286ea0b292b21c90b"),
            "2b9ab1e972a281585084148ba1389800799bd4be63b957507db1349314e47445",
            "Crypto.AAVE/USD",
        ),
        (
            address!("0x172370d5cd63279efa6d502dab29171933a610af"),
            "a19d04ac696c7a6616d291c7e5d1377cc8be437c327b75adb5dc1bad745fcae8",
            "Crypto.CRV/USD",
        ),
        (
            address!("0x0b3f868e0be5597d5db7feb59e1cadbb0fdda50a"),
            "26e4f737fde0263a9eea10ae63ac36dcedab2aaf629261a994e1eeb6ee0afe53",
            "Crypto.SUSHI/USD",
        ),
        (
            address!("0x9a71012b13ca4d3d0cdc72a177df3ef03b0e76a3"),
            "07ad7b4a7662d19a6bc675f6b467172d2f3947fa653ca97555a9b20236406628",
            "Crypto.BAL/USD",
        ),
        (
            address!("0xbbba073c31bf03b8acf7c28ef0738decf2b0bcee"),
            "cb7a1d45139117f8d3da0a4b67264579aa905e3b124efede272634f094e1e9d1",
            "Crypto.SAND/USD",
        ),
        (
            address!("0xa1c57f48f0deb89f569dfbe6e2b7f46d33606fd4"),
            "1dfffdcbc958d732750f53ff7f06d24bb01364b3f62abea511a390c74b8d16a5",
            "Crypto.MANA/USD",
        ),
    ];
    for (_, feed_id, expected_symbol) in feeds {
        assert_eq!(
            pyth_feed_id(expected_symbol).await.as_deref(),
            Some(feed_id),
            "wrong Pyth feed id for {expected_symbol}"
        );
    }
    let tokens: Vec<Address> = feeds.into_iter().map(|(token, _, _)| token).collect();
    let _ = oracle.get_matic_usd_offline().await;
    oracle.prefetch_token_usd_offline(&tokens).await;
    for token in tokens {
        assert!(
            oracle.token_usd(&token).is_some(),
            "missing token/USD price for {token}"
        );
        let rate = oracle
            .token_matic_rate_per_unit_integer(&token)
            .unwrap_or_else(|| panic!("missing token/MATIC integer rate for {token}"));
        assert!(rate >= MIN_TOKEN_TO_MATIC_RATE, "dust rate for {token}");
    }
}

#[derive(Deserialize)]
struct PythFeedMeta {
    id: String,
    attributes: PythFeedAttributes,
}

#[derive(Deserialize)]
struct PythFeedAttributes {
    symbol: String,
}

async fn pyth_feed_id(symbol: &str) -> Option<String> {
    let url = format!("https://hermes.pyth.network/v2/price_feeds?query={symbol}");
    let feeds: Vec<PythFeedMeta> = reqwest::get(url).await.ok()?.json().await.ok()?;
    feeds
        .into_iter()
        .find(|feed| feed.attributes.symbol == symbol)
        .map(|feed| feed.id)
}
