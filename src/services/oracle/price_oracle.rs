use std::collections::HashMap;
use std::time::{Duration, Instant};

use alloy::network::Ethereum;
use alloy::primitives::U256;
use alloy::primitives::{Address, I256, address};
use alloy::providers::Provider;
use alloy::sol_types::SolCall;
use reqwest::Client;
use rustc_hash::FxHashMap;
use serde::Deserialize;

const ORACLE_HTTP_TIMEOUT: Duration = Duration::from_secs(8);

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
const CACHE_TTL: Duration = Duration::from_secs(10);

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
        token: address!("0x2791bca1f2de4661ed88a30c99a7a9489c09eb3f"),
        chainlink: Some(address!("0xfE4A8cc5b5B2369C1C1948aBaC52816A1C139901")),
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
        chainlink: Some(address!("0xF9680D99D6C9589e2C4124a0F8594FB8B7D415EB")),
        pyth_id: Some("ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace"),
    },
    TokenFeed {
        token: address!("0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd5"),
        chainlink: Some(address!("0xDE31F8bF1478eBF7631D4642793642e358407879")),
        pyth_id: Some("e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"),
    },
    TokenFeed {
        token: address!("0x8f3cf7ad23cd3cadbd9735aff958023239c6a063"),
        chainlink: None,
        pyth_id: Some("b0948a5e5313200c632b51bb5ca32f6de0d36e9950a942d19751e833f70dabfd"),
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
}

impl PriceOracle {
    #[must_use]
    pub fn new(http: Client, pyth_hermes_url: String) -> Self {
        Self {
            http,
            pyth_hermes_url,
            matic_usd: parking_lot::RwLock::new(None),
            token_usd: parking_lot::RwLock::new(FxHashMap::default()),
            chainlink_usd_raw: parking_lot::RwLock::new(FxHashMap::default()),
            custom_pyth: parking_lot::RwLock::new(FxHashMap::default()),
            custom_chainlink: parking_lot::RwLock::new(FxHashMap::default()),
        }
    }

    pub fn register_pyth_feed(&self, token: Address, feed_id: String) {
        self.custom_pyth.write().insert(token, feed_id);
    }

    pub fn register_chainlink_feed(&self, token: Address, feed: Address) {
        self.custom_chainlink.write().insert(token, feed);
    }

    fn fresh(entry: &PriceEntry) -> bool {
        entry.updated_at.elapsed() < CACHE_TTL
    }

    pub fn cached_matic_usd(&self) -> Option<f64> {
        self.matic_usd
            .read()
            .as_ref()
            .filter(|entry| Self::fresh(entry))
            .map(|entry| entry.value)
    }

    pub async fn get_matic_usd<P: Provider<Ethereum>>(&self, provider: Option<&P>) -> f64 {
        {
            let cache = self.matic_usd.read();
            if let Some(entry) = cache.as_ref()
                && Self::fresh(entry)
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
                && Self::fresh(entry)
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
                    && Self::fresh(entry)
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
                let Some(usd) = prices.get(&id).copied() else {
                    continue;
                };
                if usd <= 0.0 {
                    continue;
                }
                let raw = usd_to_chainlink_raw(usd);
                for token in tokens {
                    cache.insert(
                        token,
                        PriceEntry {
                            value: usd,
                            updated_at: now,
                        },
                    );
                    if let Some(raw) = raw {
                        chainlink_raw.insert(token, raw);
                    }
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
                    && Self::fresh(entry)
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
                if cache.get(token).is_some_and(Self::fresh) {
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
                    let Some(usd) = prices.get(&id).copied() else {
                        continue;
                    };
                    if usd <= 0.0 {
                        continue;
                    }
                    let raw = usd_to_chainlink_raw(usd);
                    for token in tokens {
                        cache.insert(
                            token,
                            PriceEntry {
                                value: usd,
                                updated_at: now,
                            },
                        );
                        if let Some(raw) = raw {
                            chainlink_raw.insert(token, raw);
                        }
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
        let usd = prices.get(PYTH_MATIC_USD_ID).copied()?;
        (usd > 0.0).then_some(usd)
    }

    async fn fetch_pyth(&self, ids: &[&str]) -> anyhow::Result<HashMap<String, f64>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
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

    async fn fetch_pyth_once(&self, ids: &[&str]) -> anyhow::Result<HashMap<String, f64>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut url = reqwest::Url::parse(&format!(
            "{}/v2/updates/price/latest",
            self.pyth_hermes_url.trim_end_matches('/')
        ))?;
        {
            let mut pairs = url.query_pairs_mut();
            for id in ids {
                pairs.append_pair("ids[]", id);
            }
        }
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
            let Some(raw) = item.price.mantissa.as_f64() else {
                continue;
            };
            let usd = raw * 10f64.powi(item.price.expo);
            if usd > 0.0 {
                out.insert(item.id, usd);
            }
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

impl PythMantissa {
    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Str(s) => s.parse().ok(),
            Self::Num(n) => Some(*n),
        }
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

    #[test]
    fn usd_to_chainlink_raw_rounds_to_eight_decimals() {
        let raw = usd_to_chainlink_raw(0.7371782).expect("raw");
        assert_eq!(raw, I256::from(U256::from(73_717_820u64)));
    }

    #[tokio::test]
    #[ignore = "live network — run: cargo test pyth_matic_live -- --ignored"]
    async fn pyth_matic_live() {
        let oracle = PriceOracle::new(
            reqwest::Client::new(),
            "https://hermes.pyth.network".to_string(),
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
        let usdc = address!("0x2791bca1f2de4661ed88a30c99a7a9489c09eb3f");
        let oracle = PriceOracle::new(
            reqwest::Client::new(),
            "https://hermes.pyth.network".to_string(),
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
}
