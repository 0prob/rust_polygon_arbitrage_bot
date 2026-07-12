use std::collections::HashMap;
use std::time::{Duration, Instant};

use alloy::network::Ethereum;
use alloy::primitives::U256;
use alloy::primitives::{Address, I256, address};
use alloy::providers::Provider;
use alloy::sol_types::SolCall;
use reqwest::{Client, Url};
use rustc_hash::FxHashMap;
use serde::Deserialize;

const ORACLE_HTTP_TIMEOUT: Duration = Duration::from_secs(8);
/// Hermes URL length stays bounded when many distinct feed ids are requested.
const PYTH_FETCH_CHUNK: usize = 24;

use crate::abis::IChainlinkAggregator;
use crate::core::constants::{RATE_PRECISION, WMATIC};

use crate::pipeline::multicall::{MulticallItem, encode_call, execute_multicall};

const CHAINLINK_USD_DECIMALS: u32 = 8;

const fn pow10_f64(exp: u32) -> f64 {
    let mut scale = 1.0;
    let mut i = 0;
    while i < exp {
        scale *= 10.0;
        i += 1;
    }
    scale
}

const CHAINLINK_SCALE: f64 = pow10_f64(CHAINLINK_USD_DECIMALS);
/// Pyth Hermes rejects symbol aliases (e.g. `Crypto.MATIC/USD`); use the hex feed id.
const PYTH_MATIC_USD_ID: &str = "ffd11c5a1cfd42f80afb2df4d9f264c15f956d68153335374ec10722edd70472";
/// ponytail: 0.0 when both Chainlink and Pyth are unavailable and no cached value exists.
/// Zero rates are rejected downstream by MIN_TOKEN_TO_MATIC_RATE — no trade
/// executes without a real price. The real MATIC/USD is fetched every tick.
const DEFAULT_MATIC_USD: f64 = 0.0;
#[allow(dead_code)]
const DEFAULT_CACHE_TTL_MS: u64 = 10_000;

#[derive(Clone, Copy)]
struct TokenFeed {
    token: Address,
    chainlink: Option<Address>,
    pyth_id: Option<&'static str>,
}

const TOKEN_FEEDS: &[TokenFeed] = &[
    TokenFeed {
        token: address!("0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"),
        chainlink: Some(address!("0xAB594600376Ec9fD91F8e885dADF0CE036862dE0")),
        pyth_id: Some("ffd11c5a1cfd42f80afb2df4d9f264c15f956d68153335374ec10722edd70472"),
    },
    TokenFeed {
        token: address!("0x2791bca1f2de4661ed88a30c99a7a9449aa84174"),
        chainlink: Some(address!("0xfE4A8cc5b5B2366C1B58Bea3858e81843581b2F7")),
        pyth_id: Some("eaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a"),
    },
    TokenFeed {
        token: address!("0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"),
        chainlink: Some(address!("0xfE4A8cc5b5B2366C1B58Bea3858e81843581b2F7")),
        pyth_id: Some("eaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a"),
    },
    TokenFeed {
        token: address!("0xc2132d05d31c914a87c6611c10748aeb04b58e8f"),
        chainlink: Some(address!("0x0A6513e40db6EB1b165753AD52E80663aeA50545")),
        pyth_id: Some("2b89b9dc8fdf9f34709a5b106b472f0f39bb6ca9ce04b0fd7f2e971688e2e53b"),
    },
    TokenFeed {
        token: address!("0x7ceb23fd6bc0add59e62ac25578270cff1b9f619"),
        chainlink: Some(address!("0xF9680D99D6C9589e2a93a78A04A279e509205945")),
        pyth_id: Some("ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace"),
    },
    TokenFeed {
        token: address!("0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6"),
        chainlink: Some(address!("0xDE31F8bFBD8c84b5360CFACCa3539B938dd78ae6")),
        pyth_id: Some("e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"),
    },
    TokenFeed {
        token: address!("0x8f3cf7ad23cd3cadbd9735aff958023239c6a063"),
        chainlink: None,
        pyth_id: Some("b0948a5e5313200c632b51bb5ca32f6de0d36e9950a942d19751e833f70dabfd"),
    },
    TokenFeed {
        token: address!("0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39"),
        chainlink: None,
        pyth_id: Some("8ac0c70fff57e9aefdf5edf44b51d62c2d433653cbb2cf5cc06bb115af04d221"),
    },
    TokenFeed {
        token: address!("0xd6df932a45c0f255f85145f286ea0b292b21c90b"),
        chainlink: None,
        pyth_id: Some("2b9ab1e972a281585084148ba1389800799bd4be63b957507db1349314e47445"),
    },
    TokenFeed {
        token: address!("0x172370d5cd63279efa6d502dab29171933a610af"),
        chainlink: None,
        pyth_id: Some("a19d04ac696c7a6616d291c7e5d1377cc8be437c327b75adb5dc1bad745fcae8"),
    },
    TokenFeed {
        token: address!("0x0b3f868e0be5597d5db7feb59e1cadbb0fdda50a"),
        chainlink: None,
        pyth_id: Some("26e4f737fde0263a9eea10ae63ac36dcedab2aaf629261a994e1eeb6ee0afe53"),
    },
    TokenFeed {
        token: address!("0x9a71012b13ca4d3d0cdc72a177df3ef03b0e76a3"),
        chainlink: None,
        pyth_id: Some("07ad7b4a7662d19a6bc675f6b467172d2f3947fa653ca97555a9b20236406628"),
    },
    TokenFeed {
        token: address!("0xbbba073c31bf03b8acf7c28ef0738decf2b0bcee"),
        chainlink: None,
        pyth_id: Some("cb7a1d45139117f8d3da0a4b67264579aa905e3b124efede272634f094e1e9d1"),
    },
    TokenFeed {
        token: address!("0xa1c57f48f0deb89f569dfbe6e2b7f46d33606fd4"),
        chainlink: None,
        pyth_id: Some("1dfffdcbc958d732750f53ff7f06d24bb01364b3f62abea511a390c74b8d16a5"),
    },
    TokenFeed {
        token: address!("0xb33eaad8d922b1083446dc23f610c2567fb5180f"),
        chainlink: Some(address!("0xdf0Fb4e4F928d2dCB76f438575fDD868eE6b11a9")),
        pyth_id: Some("78d185a741d07edb3412b09008b7c5cfb9bbbd7d568bf00ba737b456ba171501"),
    },
    TokenFeed {
        token: address!("0x5fe2b58a29225b59dadf811f5c49472a056ebff0"),
        chainlink: None,
        pyth_id: Some("4d1f8dae0d96236fb98e8f47471a366ec3b1732b47041781934ca3a9bb2f35e7"),
    },
    TokenFeed {
        token: address!("0x1b02da8cb0d097eb8d57a175b88c7d8b47997506"),
        chainlink: None,
        pyth_id: Some("4a8e42861cabc5ecb50996f92e7cfa2bce3fd0a2423b0c44c9b423fb2bd25478"),
    },
    TokenFeed {
        token: address!("0x9c2c5fd7b9e403564dc385c89d647e8bd6566614"),
        chainlink: None,
        pyth_id: Some("63f341689d98a12ef60a5cff1d7f85c70a9e17bf1575f0e7c0b2512d48b1c8b3"),
    },
    TokenFeed {
        token: address!("0x53a0b3a00de21b8cf755f75ed53af39ecd158171"),
        chainlink: None,
        pyth_id: Some("c63e2a7f37a04e5e614c07238bedb25dcc38927fba8fe890597a593c0b2fa4ad"),
    },
    TokenFeed {
        token: address!("0xc9e3f325b6e02f3ca7e3ae0f329aee1014537c14"),
        chainlink: None,
        pyth_id: Some("9a4df90b25497f66b1afb012467e316e801ca3d839456db028892fe8c70c8016"),
    },
];

#[derive(Clone)]
struct PriceEntry {
    value: f64,
    updated_at: Instant,
}

pub struct PriceOracle {
    http: Client,
    pyth_hermes_url: String,
    matic_usd: parking_lot::RwLock<Option<PriceEntry>>,
    token_usd: parking_lot::RwLock<FxHashMap<Address, PriceEntry>>,
    /// Raw Chainlink USD answers (8 decimals) for integer profit conversion.
    chainlink_usd_raw: parking_lot::RwLock<FxHashMap<Address, I256>>,
    /// Config-driven Pyth feed overrides (token -> Pyth price ID).
    custom_pyth: parking_lot::RwLock<FxHashMap<Address, String>>,
    /// Config-driven Chainlink feed overrides (token -> aggregator address).
    custom_chainlink: parking_lot::RwLock<FxHashMap<Address, Address>>,
    /// Configurable cache TTL — overrides DEFAULT_CACHE_TTL_MS.
    cache_ttl: Duration,
    /// Monotonic clock of the last successful rate-build across all tokens.
    pub rates_updated_at: parking_lot::RwLock<Option<Instant>>,
}

impl PriceOracle {
    #[must_use]
    pub fn new(http: Client, pyth_hermes_url: String, cache_ttl_ms: u64) -> Self {
        Self {
            http,
            pyth_hermes_url,
            matic_usd: parking_lot::RwLock::new(None),
            token_usd: parking_lot::RwLock::new(FxHashMap::default()),
            chainlink_usd_raw: parking_lot::RwLock::new(FxHashMap::default()),
            custom_pyth: parking_lot::RwLock::new(FxHashMap::default()),
            custom_chainlink: parking_lot::RwLock::new(FxHashMap::default()),
            cache_ttl: Duration::from_millis(cache_ttl_ms),
            rates_updated_at: parking_lot::RwLock::new(None),
        }
    }

    pub fn register_pyth_feed(&self, token: Address, feed_id: String) {
        self.custom_pyth.write().insert(token, feed_id);
    }

    pub fn register_chainlink_feed(&self, token: Address, feed: Address) {
        self.custom_chainlink.write().insert(token, feed);
    }

    fn fresh(&self, entry: &PriceEntry) -> bool {
        entry.updated_at.elapsed() < self.cache_ttl
    }

    pub fn cached_matic_usd(&self) -> Option<f64> {
        self.matic_usd
            .read()
            .as_ref()
            .filter(|entry| self.fresh(entry))
            .map(|entry| entry.value)
    }

    pub async fn get_matic_usd<P: Provider<Ethereum>>(&self, provider: Option<&P>) -> f64 {
        {
            let cache = self.matic_usd.read();
            if let Some(entry) = cache.as_ref()
                && self.fresh(entry)
            {
                return entry.value;
            }
        }
        let wmatic = WMATIC;
        if let Some(feed) = self.chainlink_feed_dyn(&wmatic)
            && let Some(p) = provider
        {
            let contract = IChainlinkAggregator::new(feed, p);
            if let Ok(data) = contract.latestRoundData().call().await
                && let Some(usd) = chainlink_answer_to_usd(data.answer)
            {
                self.chainlink_usd_raw.write().insert(wmatic, data.answer);
                self.matic_usd.write().replace(PriceEntry {
                    value: usd,
                    updated_at: Instant::now(),
                });
                return usd;
            }
            crate::debug!("Chainlink MATIC/USD read failed — trying Pyth");
        } else if provider.is_none() {
            crate::debug!("no state RPC for Chainlink MATIC/USD — trying Pyth");
        }
        if let Some(usd) = self.fetch_pyth_matic_usd().await {
            self.store_matic_usd(usd);
            return usd;
        }
        if let Some(stale) = self.matic_usd.read().as_ref().map(|e| e.value) {
            crate::warn!("oracle using stale MATIC/USD — Chainlink and Pyth unavailable");
            return stale;
        }
        crate::warn!(
            "oracle has no MATIC/USD price — Chainlink and Pyth unavailable, no cached value"
        );
        DEFAULT_MATIC_USD
    }

    /// Pyth + cache only — no Chainlink RPC required.
    pub async fn get_matic_usd_offline(&self) -> f64 {
        {
            let cache = self.matic_usd.read();
            if let Some(entry) = cache.as_ref()
                && self.fresh(entry)
            {
                return entry.value;
            }
        }
        if let Some(usd) = self.fetch_pyth_matic_usd().await {
            self.store_matic_usd(usd);
            return usd;
        }
        if let Some(stale) = self.matic_usd.read().as_ref().map(|e| e.value) {
            crate::warn!("oracle using stale MATIC/USD — Pyth unavailable");
            return stale;
        }
        crate::warn!("oracle has no MATIC/USD price — Pyth unavailable, no cached value");
        DEFAULT_MATIC_USD
    }

    pub async fn prefetch_token_usd_offline(&self, tokens: &[Address]) {
        let mut need = Vec::new();
        {
            let cache = self.token_usd.read();
            for token in tokens {
                if let Some(entry) = cache.get(token)
                    && self.fresh(entry)
                {
                    continue;
                }
                if self.has_pyth_feed(token) {
                    need.push(*token);
                }
            }
        }
        if need.is_empty() {
            return;
        }
        let mut pyth_ids: FxHashMap<String, Vec<Address>> = FxHashMap::default();
        for token in &need {
            if let Some(id) = self.pyth_feed_dyn(token) {
                pyth_ids.entry(id).or_default().push(*token);
            }
        }
        if pyth_ids.is_empty() {
            return;
        }
        let ids: Vec<&str> = pyth_ids.keys().map(String::as_str).collect();
        if let Ok(prices) = self.fetch_pyth(&ids).await {
            let now = Instant::now();
            let mut cache = self.token_usd.write();
            let mut chainlink_raw = self.chainlink_usd_raw.write();
            for (id, tokens) in pyth_ids {
                let Some(quote) = prices.get(&id) else {
                    continue;
                };
                if quote.usd <= 0.0 {
                    continue;
                }
                for token in tokens {
                    cache.insert(
                        token,
                        PriceEntry {
                            value: quote.usd,
                            updated_at: now,
                        },
                    );
                    chainlink_raw.insert(token, quote.chainlink_raw);
                }
            }
        }
    }

    pub async fn prefetch_token_usd<P: Provider<Ethereum> + Clone + Send + 'static>(
        &self,
        tokens: &[Address],
        provider: Option<&P>,
    ) {
        let mut need = Vec::new();
        {
            let cache = self.token_usd.read();
            for token in tokens {
                if let Some(entry) = cache.get(token)
                    && self.fresh(entry)
                {
                    continue;
                }
                if self.has_chainlink_feed(token) || self.has_pyth_feed(token) {
                    need.push(*token);
                }
            }
        }
        if need.is_empty() {
            return;
        }

        if let Some(p) = provider {
            let mut feed_map: FxHashMap<Address, Vec<Address>> = FxHashMap::default();
            for token in &need {
                if let Some(feed) = self.chainlink_feed_dyn(token) {
                    feed_map.entry(feed).or_default().push(*token);
                }
            }
            if !feed_map.is_empty() {
                let feeds: Vec<Address> = feed_map.keys().copied().collect();
                let items: Vec<MulticallItem> = feeds
                    .iter()
                    .map(|feed| MulticallItem {
                        target: *feed,
                        data: encode_call(&IChainlinkAggregator::latestRoundDataCall {}),
                    })
                    .collect();
                if let Ok(results) = execute_multicall(p, &items).await {
                    let now = Instant::now();
                    let mut cache = self.token_usd.write();
                    let mut chainlink_raw = self.chainlink_usd_raw.write();
                    for (feed, bytes) in feeds.iter().zip(results) {
                        let Some(bytes) = bytes else { continue };
                        let Ok(data) =
                            IChainlinkAggregator::latestRoundDataCall::abi_decode_returns(&bytes)
                        else {
                            continue;
                        };
                        let Some(usd) = chainlink_answer_to_usd(data.answer) else {
                            continue;
                        };
                        if let Some(tokens) = feed_map.get(feed) {
                            for token in tokens {
                                chainlink_raw.insert(*token, data.answer);
                                cache.insert(
                                    *token,
                                    PriceEntry {
                                        value: usd,
                                        updated_at: now,
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }

        let mut pyth_ids: FxHashMap<String, Vec<Address>> = FxHashMap::default();
        {
            let cache = self.token_usd.read();
            for token in &need {
                if cache.get(token).is_some_and(|e| self.fresh(e)) {
                    continue;
                }
                if let Some(id) = self.pyth_feed_dyn(token) {
                    pyth_ids.entry(id).or_default().push(*token);
                }
            }
        }
        if !pyth_ids.is_empty() {
            let ids: Vec<&str> = pyth_ids.keys().map(String::as_str).collect();
            if let Ok(prices) = self.fetch_pyth(&ids).await {
                let now = Instant::now();
                let mut cache = self.token_usd.write();
                let mut chainlink_raw = self.chainlink_usd_raw.write();
                for (id, tokens) in pyth_ids {
                    let Some(quote) = prices.get(&id) else {
                        continue;
                    };
                    if quote.usd <= 0.0 {
                        continue;
                    }
                    for token in tokens {
                        cache.insert(
                            token,
                            PriceEntry {
                                value: quote.usd,
                                updated_at: now,
                            },
                        );
                        chainlink_raw.insert(token, quote.chainlink_raw);
                    }
                }
            }
        }
    }

    fn store_matic_usd(&self, usd: f64) {
        self.matic_usd.write().replace(PriceEntry {
            value: usd,
            updated_at: Instant::now(),
        });
        if let Some(raw) = usd_to_chainlink_raw(usd) {
            self.chainlink_usd_raw.write().insert(WMATIC, raw);
        }
    }

    async fn fetch_pyth_matic_usd(&self) -> Option<f64> {
        let prices = self.fetch_pyth(&[PYTH_MATIC_USD_ID]).await.ok()?;
        let quote = prices.get(PYTH_MATIC_USD_ID)?;
        self.chainlink_usd_raw
            .write()
            .insert(WMATIC, quote.chainlink_raw);
        (quote.usd > 0.0).then_some(quote.usd)
    }

    async fn fetch_pyth(&self, ids: &[&str]) -> anyhow::Result<HashMap<String, PythQuote>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut out = HashMap::with_capacity(ids.len());
        for chunk in ids.chunks(PYTH_FETCH_CHUNK) {
            let batch = self.fetch_pyth_chunk(chunk).await?;
            out.extend(batch);
        }
        Ok(out)
    }

    async fn fetch_pyth_chunk(&self, ids: &[&str]) -> anyhow::Result<HashMap<String, PythQuote>> {
        match self.fetch_pyth_once(ids).await {
            Ok(prices) if !prices.is_empty() => Ok(prices),
            first => {
                if let Err(_e) = &first {
                    crate::debug!("Pyth Hermes request failed — retrying once: {_e}");
                }
                self.fetch_pyth_once(ids).await
            }
        }
    }

    async fn fetch_pyth_once(&self, ids: &[&str]) -> anyhow::Result<HashMap<String, PythQuote>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let url = pyth_updates_url(&self.pyth_hermes_url, ids)?;
        let resp = self
            .http
            .get(url)
            .timeout(ORACLE_HTTP_TIMEOUT)
            .send()
            .await?
            .error_for_status()?;
        let body: PythHermesResponse = resp.json().await?;
        let mut out = HashMap::with_capacity(body.parsed.len());
        for item in body.parsed {
            let Some(mantissa) = item.price.mantissa.as_i128() else {
                continue;
            };
            let Some(quote) = pyth_fields_to_quote(mantissa, item.price.expo) else {
                continue;
            };
            out.insert(item.id, quote);
        }
        Ok(out)
    }

    pub fn token_usd(&self, token: &Address) -> Option<f64> {
        self.token_usd.read().get(token).map(|e| e.value)
    }

    /// Integer-only token/MATIC rate when both feeds have Chainlink answers cached.
    pub fn token_matic_rate_per_unit_integer(&self, token: &Address) -> Option<U256> {
        let raw = self.chainlink_usd_raw.read();
        let token_raw = raw.get(token).copied()?;
        let matic_raw = raw.get(&WMATIC).copied()?;
        drop(raw);
        let rate = chainlink_usd_to_matic_rate_per_unit(token_raw, matic_raw);
        if rate.is_zero() { None } else { Some(rate) }
    }
}

fn pyth_updates_url(base_url: &str, ids: &[&str]) -> anyhow::Result<Url> {
    let mut url = Url::parse(&format!(
        "{}/v2/updates/price/latest",
        base_url.trim_end_matches('/')
    ))?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("encoding", "hex");
        pairs.append_pair("parsed", "true");
        for id in ids {
            pairs.append_pair("ids[]", id);
        }
    }
    Ok(url)
}

#[inline]
fn usd_to_chainlink_raw(usd: f64) -> Option<I256> {
    if usd.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
        return None;
    }
    let raw = (usd * CHAINLINK_SCALE).round();
    if raw <= 0.0 || raw > i128::MAX as f64 {
        return None;
    }
    let raw_u = raw as u128;
    if raw_u == 0 {
        return None;
    }
    Some(I256::from(U256::from(raw_u)))
}

#[inline]
fn chainlink_answer_to_usd(answer: I256) -> Option<f64> {
    let raw = i128::try_from(answer).ok()? as f64;
    let usd = raw / CHAINLINK_SCALE;
    if usd > 0.0 { Some(usd) } else { None }
}

#[inline]
fn chainlink_feed(token: &Address) -> Option<Address> {
    TOKEN_FEEDS
        .iter()
        .find(|entry| entry.token == *token)
        .and_then(|entry| entry.chainlink)
}

#[inline]
fn pyth_feed(token: &Address) -> Option<&'static str> {
    TOKEN_FEEDS
        .iter()
        .find(|entry| entry.token == *token)
        .and_then(|entry| entry.pyth_id)
}

impl PriceOracle {
    /// Chainlink aggregator address for `token`: checks built-in feeds first,
    /// then config-driven overrides.
    #[inline]
    fn chainlink_feed_dyn(&self, token: &Address) -> Option<Address> {
        chainlink_feed(token).or_else(|| self.custom_chainlink.read().get(token).copied())
    }

    /// Pyth price ID for `token`: checks built-in feeds first,
    /// then config-driven overrides.
    #[inline]
    fn pyth_feed_dyn(&self, token: &Address) -> Option<String> {
        pyth_feed(token)
            .map(ToString::to_string)
            .or_else(|| self.custom_pyth.read().get(token).cloned())
    }

    /// True when `token` has any Pyth feed (static or custom).
    #[inline]
    fn has_pyth_feed(&self, token: &Address) -> bool {
        pyth_feed(token).is_some() || self.custom_pyth.read().contains_key(token)
    }

    /// True when `token` has any Chainlink feed (static or custom).
    #[inline]
    fn has_chainlink_feed(&self, token: &Address) -> bool {
        chainlink_feed(token).is_some() || self.custom_chainlink.read().contains_key(token)
    }
}

#[derive(Debug, Deserialize)]
struct PythHermesResponse {
    #[serde(default)]
    parsed: Vec<PythParsedItem>,
}

#[derive(Debug, Deserialize)]
struct PythParsedItem {
    id: String,
    price: PythPriceFields,
}

#[derive(Debug, Deserialize)]
struct PythPriceFields {
    #[serde(rename = "price")]
    mantissa: PythMantissa,
    expo: i32,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PythMantissa {
    Str(String),
    Num(f64),
}

#[derive(Debug, Clone, Copy)]
struct PythQuote {
    usd: f64,
    chainlink_raw: I256,
}

impl PythMantissa {
    fn as_i128(&self) -> Option<i128> {
        match self {
            Self::Str(s) => s.parse().ok(),
            Self::Num(n) => {
                if *n > i128::MAX as f64 || *n < i128::MIN as f64 {
                    None
                } else {
                    Some(*n as i128)
                }
            }
        }
    }
}

/// Pyth mantissa×10^expo → Chainlink-style 8-decimal USD answer (integer only).
#[must_use]
fn pyth_to_chainlink_raw(mantissa: i128, expo: i32) -> Option<I256> {
    if mantissa <= 0 {
        return None;
    }
    let shift = expo + CHAINLINK_USD_DECIMALS as i32;
    let raw_i128 = if shift >= 0 {
        let factor = 10i128.checked_pow(shift as u32)?;
        mantissa.checked_mul(factor)?
    } else {
        let divisor = 10i128.checked_pow((-shift) as u32)?;
        mantissa.checked_div(divisor)?
    };
    if raw_i128 <= 0 {
        return None;
    }
    I256::try_from(raw_i128).ok()
}

fn pyth_fields_to_quote(mantissa: i128, expo: i32) -> Option<PythQuote> {
    let chainlink_raw = pyth_to_chainlink_raw(mantissa, expo)?;
    let usd = chainlink_answer_to_usd(chainlink_raw)?;
    if usd > 0.0 {
        Some(PythQuote { usd, chainlink_raw })
    } else {
        None
    }
}

#[must_use]
pub fn token_usd_to_matic_rate_per_unit(token_usd: f64, matic_usd: f64) -> U256 {
    if !(token_usd > 0.0 && matic_usd > 0.0) {
        return U256::ZERO;
    }
    // Use u128 intermediates to reduce f64 rounding loss before integer division.
    let token_micro = (token_usd * 1e18).round() as u128;
    let matic_micro = (matic_usd * 1e18).round() as u128;
    if matic_micro == 0 {
        return U256::ZERO;
    }
    let whole_matic_wei = (U256::from(token_micro) * RATE_PRECISION) / U256::from(matic_micro);
    if whole_matic_wei.is_zero() {
        return U256::ZERO;
    }
    whole_matic_wei
}

/// Integer-only MATIC wei per whole token unit from Chainlink USD answers
/// (`CHAINLINK_USD_DECIMALS` = 8 on each feed).
#[must_use]
pub fn chainlink_usd_to_matic_rate_per_unit(
    token_usd_answer: I256,
    matic_usd_answer: I256,
) -> U256 {
    let Ok(token) = i128::try_from(token_usd_answer) else {
        return U256::ZERO;
    };
    let Ok(matic) = i128::try_from(matic_usd_answer) else {
        return U256::ZERO;
    };
    if token <= 0 || matic <= 0 {
        return U256::ZERO;
    }
    (U256::from(token as u128) * RATE_PRECISION) / U256::from(matic as u128)
}

/// ponytail: returns U256::ZERO when MATIC/USD is not available.
/// This is a safe no-op — downstream rate checks (MIN_TOKEN_TO_MATIC_RATE)
/// filter out zero rates, preventing any trade execution without real prices.
/// The real MATIC/USD comes from Chainlink multi-call or Pyth Hermes HTTP.
#[must_use]
pub fn bootstrap_matic_rate_per_unit() -> U256 {
    token_usd_to_matic_rate_per_unit(1.0, DEFAULT_MATIC_USD)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;

    #[test]
    fn pyth_to_chainlink_raw_matches_eight_decimal_usd() {
        let raw = pyth_to_chainlink_raw(73_717_820, -8).expect("raw");
        assert_eq!(raw, I256::from(U256::from(73_717_820u64)));
        let quote = pyth_fields_to_quote(100_000_000, -8).expect("quote");
        assert_eq!(quote.usd, 1.0);
        assert_eq!(quote.chainlink_raw, I256::from(U256::from(100_000_000u64)));
    }

    #[test]
    fn usd_to_chainlink_raw_rounds_to_eight_decimals() {
        let raw = usd_to_chainlink_raw(0.7371782).expect("raw");
        assert_eq!(raw, I256::from(U256::from(73_717_820u64)));
    }

    #[test]
    fn pyth_updates_url_requests_parsed_prices() {
        let url =
            pyth_updates_url("https://hermes.pyth.network/", &["feed-a", "feed-b"]).expect("url");
        let query = url.query().expect("query");
        assert!(query.contains("encoding=hex"));
        assert!(query.contains("parsed=true"));
        assert!(query.contains("ids%5B%5D=feed-a"));
        assert!(query.contains("ids%5B%5D=feed-b"));
    }

    #[test]
    fn built_in_pyth_feeds_cover_extended_polygon_tokens() {
        let tokens = [
            address!("0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39"),
            address!("0xd6df932a45c0f255f85145f286ea0b292b21c90b"),
            address!("0x172370d5cd63279efa6d502dab29171933a610af"),
            address!("0x0b3f868e0be5597d5db7feb59e1cadbb0fdda50a"),
            address!("0x9a71012b13ca4d3d0cdc72a177df3ef03b0e76a3"),
            address!("0xbbba073c31bf03b8acf7c28ef0738decf2b0bcee"),
            address!("0xa1c57f48f0deb89f569dfbe6e2b7f46d33606fd4"),
            address!("0xb33eaad8d922b1083446dc23f610c2567fb5180f"),
            address!("0x5fe2b58a29225b59dadf811f5c49472a056ebff0"),
            address!("0x1b02da8cb0d097eb8d57a175b88c7d8b47997506"),
            address!("0x9c2c5fd7b9e403564dc385c89d647e8bd6566614"),
            address!("0x53a0b3a00de21b8cf755f75ed53af39ecd158171"),
            address!("0xc9e3f325b6e02f3ca7e3ae0f329aee1014537c14"),
        ];
        assert!(tokens.iter().all(|token| pyth_feed(token).is_some()));
    }

    #[test]
    fn polygon_hub_tokens_have_chainlink_or_pyth_feed() {
        for token in crate::core::constants::POLYGON_HUB_TOKENS {
            assert!(
                chainlink_feed(&token).is_some() || pyth_feed(&token).is_some(),
                "hub token missing oracle feed: {token}"
            );
        }
    }

    #[tokio::test]
    #[ignore = "live network — run: cargo test pyth_matic_live -- --ignored"]
    async fn pyth_matic_live() {
        let oracle = PriceOracle::new(
            reqwest::Client::new(),
            "https://hermes.pyth.network".to_string(),
            DEFAULT_CACHE_TTL_MS,
        );
        let usd = oracle.get_matic_usd_offline().await;
        assert!(usd > 0.01 && usd < 100.0, "MATIC/USD: {usd}");
        assert!(oracle.cached_matic_usd().is_some());
        let rate = oracle
            .token_matic_rate_per_unit_integer(&WMATIC)
            .expect("WMATIC rate");
        assert!(rate >= crate::core::constants::MIN_TOKEN_TO_MATIC_RATE);
    }

    #[test]
    fn token_usd_to_matic_rate_uses_integer_path() {
        let wmatic = WMATIC;
        let usdc = address!("0x2791bca1f2de4661ed88a30c99a7a9449aa84174");
        let oracle = PriceOracle::new(
            reqwest::Client::new(),
            "https://hermes.pyth.network".to_string(),
            DEFAULT_CACHE_TTL_MS,
        );
        oracle
            .chainlink_usd_raw
            .write()
            .insert(wmatic, I256::from(U256::from(50_000_000u64)));
        oracle
            .chainlink_usd_raw
            .write()
            .insert(usdc, I256::from(U256::from(100_000_000u64)));
        let rate = oracle
            .token_matic_rate_per_unit_integer(&usdc)
            .expect("rate");
        assert_eq!(rate, RATE_PRECISION * U256::from(2u64));
    }

    #[test]
    fn oracle_feeds_include_canonical_usdc_e() {
        let usdc_e = address!("0x2791bca1f2de4661ed88a30c99a7a9449aa84174");
        assert!(chainlink_feed(&usdc_e).is_some());
        assert!(pyth_feed(&usdc_e).is_some());
    }

    #[test]
    fn oracle_feeds_include_polygon_wbtc() {
        // Given: Polygon's canonical WBTC token address.
        let wbtc = address!("0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6");

        // When: built-in oracle mappings are resolved.
        let feeds = (chainlink_feed(&wbtc), pyth_feed(&wbtc));

        // Then: both independent price sources must cover WBTC.
        assert!(feeds.0.is_some());
        assert!(feeds.1.is_some());
    }

    #[test]
    fn polygon_chainlink_feeds_match_live_registry() {
        let usdc_e = address!("0x2791bca1f2de4661ed88a30c99a7a9449aa84174");
        let weth = address!("0x7ceb23fd6bc0add59e62ac25578270cff1b9f619");
        let wbtc = address!("0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6");

        assert_eq!(
            chainlink_feed(&usdc_e),
            Some(address!("0xfE4A8cc5b5B2366C1B58Bea3858e81843581b2F7"))
        );
        assert_eq!(
            chainlink_feed(&weth),
            Some(address!("0xF9680D99D6C9589e2a93a78A04A279e509205945"))
        );
        assert_eq!(
            chainlink_feed(&wbtc),
            Some(address!("0xDE31F8bFBD8c84b5360CFACCa3539B938dd78ae6"))
        );
    }

    #[test]
    fn stale_cache_entries_do_not_count_as_fresh() {
        let oracle = PriceOracle::new(
            reqwest::Client::new(),
            "https://hermes.pyth.network".to_string(),
            0,
        );
        oracle.matic_usd.write().replace(PriceEntry {
            value: 1.0,
            updated_at: Instant::now() - Duration::from_millis(1),
        });
        assert!(oracle.cached_matic_usd().is_none());
    }
}
