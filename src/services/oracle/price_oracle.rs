use std::collections::HashMap;
use std::time::{Duration, Instant};

use alloy::network::Ethereum;
use alloy::primitives::{Address, I256, address};
use alloy::providers::Provider;
use alloy::sol_types::SolCall;
use reqwest::Client;
use ruint::aliases::U256;
use rustc_hash::FxHashMap;

use crate::abis::IChainlinkAggregator;
use crate::core::constants::{RATE_PRECISION, WMATIC};
use crate::pipeline::multicall::{MulticallItem, encode_call, execute_multicall};
use crate::error::ArbError;

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
const DEFAULT_MATIC_USD: f64 = 0.7;
#[expect(dead_code)]
pub(crate) const DEFAULT_MATIC_USD_F64: f64 = DEFAULT_MATIC_USD;
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
        pyth_id: Some("Crypto.MATIC/USD"),
    },
    TokenFeed {
        token: address!("0x2791bca1f2de4661ed88a30c99a7a9489c09eb3f"),
        chainlink: Some(address!("0xfE4A8cc5b5B2369C1C1948aBaC52816A1C139901")),
        pyth_id: Some("Crypto.USDC/USD"),
    },
    TokenFeed {
        token: address!("0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"),
        chainlink: Some(address!("0xfE4A8cc5b5B2366C1B58Bea3858e81843581b2F7")),
        pyth_id: Some("Crypto.USDC/USD"),
    },
    TokenFeed {
        token: address!("0xc2132d05d31c914a87c6611c10748aeb04b58e8f"),
        chainlink: Some(address!("0x0A6513e40db6EB1b165753AD52E80663aeA50545")),
        pyth_id: Some("Crypto.USDT/USD"),
    },
    TokenFeed {
        token: address!("0x7ceb23fd6bc0add59e62ac25578270cff1b9f619"),
        chainlink: Some(address!("0xF9680D99D6C9589e2C4124a0F8594FB8B7D415EB")),
        pyth_id: Some("Crypto.ETH/USD"),
    },
    TokenFeed {
        token: address!("0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd5"),
        chainlink: Some(address!("0xDE31F8bF1478eBF7631D4642793642e358407879")),
        pyth_id: Some("Crypto.BTC/USD"),
    },
    TokenFeed {
        token: address!("0x8f3cf7ad23cd3cadbd9735aff958023239c6a063"),
        chainlink: None,
        pyth_id: Some("Crypto.DAI/USD"),
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
    enabled: bool,
    matic_usd: parking_lot::RwLock<Option<PriceEntry>>,
    token_usd: parking_lot::RwLock<FxHashMap<Address, PriceEntry>>,
    /// Raw Chainlink USD answers (8 decimals) for integer profit conversion.
    chainlink_usd_raw: parking_lot::RwLock<FxHashMap<Address, I256>>,
}

impl PriceOracle {
    pub fn new(enabled: bool, pyth_hermes_url: String) -> Result<Self, ArbError> {
        let http = Client::builder()
            .timeout(Duration::from_secs(8))
            .build()
            .map_err(|e| ArbError::HttpClient(format!("oracle reqwest: {e}")))?;
        Ok(Self {
            http,
            pyth_hermes_url,
            enabled,
            matic_usd: parking_lot::RwLock::new(None),
            token_usd: parking_lot::RwLock::new(FxHashMap::default()),
            chainlink_usd_raw: parking_lot::RwLock::new(FxHashMap::default()),
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn fresh(entry: &PriceEntry) -> bool {
        entry.updated_at.elapsed() < CACHE_TTL
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
        if !self.enabled {
            return DEFAULT_MATIC_USD;
        }
        let wmatic = WMATIC;
        if let Some(feed) = chainlink_feed(&wmatic)
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
        }
        if let Ok(prices) = self.fetch_pyth(&["Crypto.MATIC/USD"]).await
            && let Some(usd) = prices.get("Crypto.MATIC/USD").copied()
            && usd > 0.0
        {
            self.matic_usd.write().replace(PriceEntry {
                value: usd,
                updated_at: Instant::now(),
            });
            return usd;
        }
        self.matic_usd
            .read()
            .as_ref()
            .map(|e| e.value)
            .unwrap_or(DEFAULT_MATIC_USD)
    }

    pub async fn prefetch_token_usd<P: Provider<Ethereum>>(
        &self,
        tokens: &[Address],
        provider: Option<&P>,
    ) {
        if !self.enabled {
            return;
        }
        let mut need = Vec::new();
        {
            let cache = self.token_usd.read();
            for token in tokens {
                if let Some(entry) = cache.get(token)
                    && Self::fresh(entry)
                {
                    continue;
                }
                if chainlink_feed(token).is_some() || pyth_feed(token).is_some() {
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
                if let Some(feed) = chainlink_feed(token) {
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
                        for token in feed_map.get(feed).into_iter().flatten() {
                            self.chainlink_usd_raw.write().insert(*token, data.answer);
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

        let mut pyth_ids: FxHashMap<&'static str, Vec<Address>> = FxHashMap::default();
        {
            let cache = self.token_usd.read();
            for token in &need {
                if cache.get(token).is_some_and(Self::fresh) {
                    continue;
                }
                if let Some(id) = pyth_feed(token) {
                    pyth_ids.entry(id).or_default().push(*token);
                }
            }
        }
        if !pyth_ids.is_empty() {
            let ids: Vec<&str> = pyth_ids.keys().copied().collect();
            if let Ok(prices) = self.fetch_pyth(&ids).await {
                let now = Instant::now();
                let mut cache = self.token_usd.write();
                for (id, tokens) in pyth_ids {
                    let Some(usd) = prices.get(id).copied() else {
                        continue;
                    };
                    if usd <= 0.0 {
                        continue;
                    }
                    for token in tokens {
                        cache.insert(
                            token,
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

    async fn fetch_pyth(&self, ids: &[&str]) -> anyhow::Result<HashMap<String, f64>> {
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
        let resp = self.http.get(url).send().await?;
        let body: serde_json::Value = resp.json().await?;
        let mut out = HashMap::new();
        let Some(parsed) = body.get("parsed").and_then(|v| v.as_array()) else {
            return Ok(out);
        };
        for item in parsed {
            let Some(id) = item.get("id").and_then(|v| v.as_str()) else {
                continue;
            };
            let price = item
                .pointer("/price/price")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| item.pointer("/price/price").and_then(|v| v.as_f64()));
            let expo = match item.pointer("/price/expo").and_then(|v| v.as_i64()) {
                Some(e) => e as i32,
                None => continue,
            };
            if let Some(raw) = price {
                let usd = raw * 10f64.powi(expo);
                if usd > 0.0 {
                    out.insert(id.to_string(), usd);
                }
            }
        }
        Ok(out)
    }

    pub fn token_usd(&self, token: &Address) -> Option<f64> {
        self.token_usd.read().get(token).map(|e| e.value)
    }

    /// Integer-only token/MATIC rate when both feeds have Chainlink answers cached.
    pub fn token_matic_rate_per_unit_integer(&self, token: &Address) -> Option<U256> {
        let token_raw = self.chainlink_usd_raw.read().get(token).copied()?;
        let matic_raw = self.chainlink_usd_raw.read().get(&WMATIC).copied()?;
        let rate = chainlink_usd_to_matic_rate_per_unit(token_raw, matic_raw);
        if rate.is_zero() {
            None
        } else {
            Some(rate)
        }
    }
}

#[expect(dead_code)]
async fn read_chainlink_usd<P: Provider<Ethereum>>(provider: &P, feed: Address) -> Option<f64> {
    let contract = IChainlinkAggregator::new(feed, provider);
    let data = contract.latestRoundData().call().await.ok()?;
    chainlink_answer_to_usd(data.answer)
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

pub fn bootstrap_matic_rate_per_unit() -> U256 {
    token_usd_to_matic_rate_per_unit(1.0, DEFAULT_MATIC_USD)
}

#[cfg(test)]
mod rate_tests {
    use super::*;
    use alloy::primitives::I256;

    #[test]
    fn chainlink_integer_rate_matches_float_path() {
        let token = I256::try_from(100_000_000i128).unwrap(); // $1
        let matic = I256::try_from(70_000_000i128).unwrap(); // $0.70
        let int_rate = chainlink_usd_to_matic_rate_per_unit(token, matic);
        let float_rate = token_usd_to_matic_rate_per_unit(1.0, 0.7);
        assert!(int_rate > U256::ZERO);
        assert_eq!(int_rate, float_rate);
    }
}
