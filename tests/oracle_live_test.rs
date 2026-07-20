//! Live oracle smoke tests — require network.
//!
//! Run: `cargo test --test oracle_live_test -- --ignored --nocapture`

use alloy::primitives::{Address, address};
use reqwest::Client;
use rpbot::core::constants::{MIN_TOKEN_TO_MATIC_RATE, WMATIC};
use rpbot::infra::http::{HttpClientOpts, build};
use rpbot::services::oracle::price_oracle::PriceOracle;
use serde::Deserialize;

fn hermes_http() -> Client {
    build(HttpClientOpts {
        timeout: std::time::Duration::from_secs(10),
        pool_max_idle_per_host: 4,
        max_redirects: 5,
    })
    .expect("http client")
}

fn hermes_oracle() -> PriceOracle {
    PriceOracle::new(
        hermes_http(),
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
    let usdc: Address = address!("0x2791bca1f2de4661ed88a30c99a7a9449aa84174");
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
    let feeds: [(Address, &str, &str); 13] = [
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
        (
            address!("0xb33eaad8d922b1083446dc23f610c2567fb5180f"),
            "78d185a741d07edb3412b09008b7c5cfb9bbbd7d568bf00ba737b456ba171501",
            "Crypto.UNI/USD",
        ),
        (
            address!("0x5fe2b58a29225b59dadf811f5c49472a056ebff0"),
            "4d1f8dae0d96236fb98e8f47471a366ec3b1732b47041781934ca3a9bb2f35e7",
            "Crypto.GRT/USD",
        ),
        (
            address!("0x1b02da8cb0d097eb8d57a175b88c7d8b47997506"),
            "4a8e42861cabc5ecb50996f92e7cfa2bce3fd0a2423b0c44c9b423fb2bd25478",
            "Crypto.COMP/USD",
        ),
        (
            address!("0x9c2c5fd7b9e403564dc385c89d647e8bd6566614"),
            "63f341689d98a12ef60a5cff1d7f85c70a9e17bf1575f0e7c0b2512d48b1c8b3",
            "Crypto.1INCH/USD",
        ),
        (
            address!("0x53a0b3a00de21b8cf755f75ed53af39ecd158171"),
            "c63e2a7f37a04e5e614c07238bedb25dcc38927fba8fe890597a593c0b2fa4ad",
            "Crypto.LDO/USD",
        ),
        (
            address!("0xc9e3f325b6e02f3ca7e3ae0f329aee1014537c14"),
            "9a4df90b25497f66b1afb012467e316e801ca3d839456db028892fe8c70c8016",
            "Crypto.PENDLE/USD",
        ),
    ];
    let http = hermes_http();
    for (_, feed_id, expected_symbol) in feeds {
        assert_eq!(
            pyth_feed_id(&http, expected_symbol).await.as_deref(),
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
            .expect("missing token/MATIC integer rate");
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

#[tokio::test]
#[ignore = "live network — run: cargo test --test oracle_live_test curated_extensions -- --ignored"]
async fn pyth_curated_polygon_extensions_prefetch_enable_rates() {
    let oracle = hermes_oracle();
    let feeds: [(Address, &str, &str); 2] = [
        (
            address!("0x03b54A0eF8042C0f6A77B15e637c9f5d7c6790D0"),
            "6df640f3b8963d8f8358f791f352b8364513f6ab1cca5ed3f1f7b5448980e784",
            "Crypto.WSTETH/USD",
        ),
        (
            address!("0x45c32fA6DF82ead1e2EF74d32b0366496F5fDe09"),
            "735f591e4fed988cd38df74d8fcedecf2fe8d9111664e0fd500db9aa78b316b1",
            "Crypto.FRAX/USD",
        ),
    ];
    let http = hermes_http();
    for (_, feed_id, expected_symbol) in feeds {
        assert_eq!(
            pyth_feed_id(&http, expected_symbol).await.as_deref(),
            Some(feed_id),
            "wrong Pyth feed id for {expected_symbol}"
        );
    }
    let tokens: Vec<Address> = feeds.into_iter().map(|(token, _, _)| token).collect();
    let bridged_wsteth = address!("0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD");
    let _ = oracle.get_matic_usd_offline().await;
    oracle.prefetch_token_usd_offline(&tokens).await;
    oracle.prefetch_token_usd_offline(&[bridged_wsteth]).await;
    for token in tokens {
        assert!(oracle.token_usd(&token).is_some());
        let rate = oracle
            .token_matic_rate_per_unit_integer(&token)
            .expect("missing token/MATIC integer rate");
        assert!(rate >= MIN_TOKEN_TO_MATIC_RATE);
    }
    assert!(oracle.token_usd(&bridged_wsteth).is_some());
    let bridged_rate = oracle
        .token_matic_rate_per_unit_integer(&bridged_wsteth)
        .expect("bridged wstETH/MATIC rate");
    assert!(bridged_rate >= MIN_TOKEN_TO_MATIC_RATE);
}

async fn pyth_feed_id(http: &Client, symbol: &str) -> Option<String> {
    let url = format!("https://hermes.pyth.network/v2/price_feeds?query={symbol}");
    let feeds: Vec<PythFeedMeta> = http.get(url).send().await.ok()?.json().await.ok()?;
    feeds
        .into_iter()
        .find(|feed| feed.attributes.symbol == symbol)
        .map(|feed| feed.id)
}
