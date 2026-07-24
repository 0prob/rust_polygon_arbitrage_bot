use std::sync::LazyLock;
use std::time::{Duration, Instant};

use alloy::network::Ethereum;
use alloy::primitives::U256;
use alloy::primitives::{Address, I256, address};
use alloy::providers::Provider;
use alloy::sol_types::SolCall;
use parking_lot::RwLock;
use reqwest::{Client, Url};
use rustc_hash::{FxBuildHasher, FxHashMap};
use serde::Deserialize;

const ORACLE_HTTP_TIMEOUT: Duration = Duration::from_secs(8);
/// Hermes URL length stays bounded when many distinct feed ids are requested.
const PYTH_FETCH_CHUNK: usize = 24;
/// Reject Pyth quotes older than this (plan: ≤60s skew vs wall clock).
const PYTH_MAX_PUBLISH_AGE_SECS: i64 = 60;
/// Reject when `conf / price` exceeds 1% (plan: confidence ratio cap).
const PYTH_MAX_CONF_BPS: i128 = 100;

use crate::abis::IChainlinkAggregator;
use crate::core::constants::{MIN_TOKEN_TO_MATIC_RATE, RATE_PRECISION, WMATIC};

use crate::pipeline::multicall::{MulticallItem, encode_call, execute_multicall};

const CHAINLINK_USD_DECIMALS: u32 = 8;
/// Reject on-chain Chainlink rounds older than this (Polygon majors heartbeat ≤1h).
const CHAINLINK_MAX_STALENESS_SECS: u64 = 7_200;

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
/// Default oracle quote cache TTL (also `OracleConfig::cache_ttl_ms` serde default).
pub const DEFAULT_CACHE_TTL_MS: u64 = 10_000;

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
        token: address!("0x8f3cf7ad23cd3cadbd9735aff958023239c6a063"), // DAI
        // Polygon DAI/USD proxy (docs.chain.link data-feeds polygon).
        chainlink: Some(address!("0x4746DeC9e833A82EC7C2C1356372CcF2cfcD2F3D")),
        pyth_id: Some("b0948a5e5313200c632b51bb5ca32f6de0d36e9950a942d19751e833f70dabfd"),
    },
    TokenFeed {
        token: address!("0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39"), // LINK
        chainlink: Some(address!("0xd9FFdb71EbE7496cC440152d43986Aae0AB76665")),
        pyth_id: Some("8ac0c70fff57e9aefdf5edf44b51d62c2d433653cbb2cf5cc06bb115af04d221"),
    },
    TokenFeed {
        token: address!("0xd6df932a45c0f255f85145f286ea0b292b21c90b"), // AAVE
        chainlink: Some(address!("0x72484B12719E23115761D5DA1646945632979bB6")),
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
    // SAND / MANA / UNI / GRT / GHST — addresses must match core::constants hubs.
    TokenFeed {
        token: address!("0xbbba073c31bf03b8acf7c28ef0738decf3695683"), // SAND
        chainlink: None,
        pyth_id: Some("cb7a1d45139117f8d3da0a4b67264579aa905e3b124efede272634f094e1e9d1"),
    },
    TokenFeed {
        token: address!("0xa1c57f48f0deb89f569dfbe6e2b7f46d33606fd4"), // MANA
        chainlink: None,
        pyth_id: Some("1dfffdcbc958d732750f53ff7f06d24bb01364b3f62abea511a390c74b8d16a5"),
    },
    TokenFeed {
        token: address!("0xb33eaad8d922b1083446dc23f610c2567fb5180f"), // UNI
        // Official Polygon UNI/USD proxy (typo eE6b11a9 returned empty eth_call / multicall fail).
        chainlink: Some(address!("0xdf0Fb4e4F928d2dCB76f438575fDD8682386e13C")),
        pyth_id: Some("78d185a741d07edb3412b09008b7c5cfb9bbbd7d568bf00ba737b456ba171501"),
    },
    TokenFeed {
        token: address!("0x5fe2b58c013d7601147dcdd68c143a77499f5531"), // GRT
        chainlink: None,
        pyth_id: Some("4d1f8dae0d96236fb98e8f47471a366ec3b1732b47041781934ca3a9bb2f35e7"),
    },
    TokenFeed {
        token: address!("0x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7"), // GHST
        chainlink: None,
        pyth_id: Some("c63e2a7f37a04e5e614c07238bedb25dcc38927fba8fe890597a593c0b2fa4ad"),
    },
    TokenFeed {
        token: address!("0x03b54A0eF8042C0f6A77B15e637c9f5d7c6790D0"),
        chainlink: None,
        pyth_id: Some("6df640f3b8963d8f8358f791f352b8364513f6ab1cca5ed3f1f7b5448980e784"),
    },
    // Polygon-bridged wstETH (Aave/runtime demand); same WSTETH/USD Pyth feed.
    TokenFeed {
        token: address!("0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD"),
        chainlink: None,
        pyth_id: Some("6df640f3b8963d8f8358f791f352b8364513f6ab1cca5ed3f1f7b5448980e784"),
    },
    TokenFeed {
        token: address!("0x45c32fA6DF82ead1e2EF74d32b0366496F5fDe09"),
        chainlink: None,
        pyth_id: Some("735f591e4fed988cd38df74d8fcedecf2fe8d9111664e0fd500db9aa78b316b1"),
    },
    // Polygon-bridged / runtime-demand mints (oracle_feeds verify 2026-07).
    TokenFeed {
        token: address!("0x61fFE097137d543f019F5257E1a1Ff7A6C5F0b68"),
        chainlink: None,
        pyth_id: Some("78d185a741d07edb3412b09008b7c5cfb9bbbd7d568bf00ba737b456ba171501"),
    },
    TokenFeed {
        token: address!("0x50B728D8D964fd00C2d0AAD81718b71311feF68a"),
        chainlink: None,
        pyth_id: Some("39d020f60982ed892abbcd4a06a276a9f9b7bfbce003204c110b6e488f502da3"),
    },
    TokenFeed {
        token: address!("0xbbC11D55375F0B37f8A30b102C9ce143B097671e"),
        chainlink: None,
        pyth_id: Some("26e4f737fde0263a9eea10ae63ac36dcedab2aaf629261a994e1eeb6ee0afe53"),
    },
    TokenFeed {
        token: address!("0x9c2C5fd7b07E95EE044DDeba0E97a665F142394f"),
        chainlink: None,
        pyth_id: Some("63f341689d98a12ef60a5cff1d7f85c70a9e17bf1575f0e7c0b2512d48b1c8b3"),
    },
    TokenFeed {
        token: address!("0x1C954E8fe737F99f68Fa1CCda3e51ebDB291948C"),
        chainlink: None,
        pyth_id: Some("b9ccc817bfeded3926af791f09f76c5ffbc9b789cac6e9699ec333a79cacbe2a"),
    },
    TokenFeed {
        token: address!("0x8505b9d2254A7Ae468c0E9dd10Ccea3A837aef5c"),
        chainlink: None,
        pyth_id: Some("4a8e42861cabc5ecb50996f92e7cfa2bce3fd0a2423b0c44c9b423fb2bd25478"),
    },
    TokenFeed {
        token: address!("0x2C89bbc92BD86F8075d1DEcc58C7F4E0107f286b"),
        chainlink: None,
        pyth_id: Some("93da3352f9f1d105fdfe4971cfa80e9dd777bfc5d0f683ebb6e1294b92137bb7"),
    },
    TokenFeed {
        token: address!("0xd93f7E271cB87c23AaA73edC008A79646d1F9912"),
        chainlink: None,
        pyth_id: Some("ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d"),
    },
    TokenFeed {
        token: address!("0x553d3D295e0f695B9228246232eDF400ed3560B5"),
        chainlink: None,
        pyth_id: Some("273717b49430906f4b0c230e99aa1007f83758e3199edbc887c0d06c3e332494"),
    },
    // Batch 2 — oracle_feeds verify 2026-07-16 (target/oracle-proposed-batch2.txt).
    TokenFeed {
        token: address!("0xA571963278014B5B3A686778747fDf8ad4dFBb94"),
        chainlink: None,
        pyth_id: Some("83aac6fae150e8850204ef5dce696c05ae2efa335a41c7e5c112bc73e5cbae35"),
    },
    TokenFeed {
        token: address!("0x6f8a06447Ff6FcF75d803135a7de15CE88C1d4ec"),
        chainlink: None,
        pyth_id: Some("f0d57deca57b3da2fe63a493f4c25925fdfd8edf834b20f93e1f84dbd1504d4a"),
    },
    // Gains GNS only — do not map GHST (0x385Ee…) to GNS/USD (live feed-id poison).
    TokenFeed {
        token: address!("0xE5417Af564e4bFDA1c483642db72007871397896"),
        chainlink: None,
        pyth_id: Some("5a5d5f7fb72cc84b579d74d1c06d258d751962e9a010c0b1cce7e6023aacb71b"),
    },
    TokenFeed {
        token: address!("0xBbba073C31bF03b8ACf7c28EF0738DeCF3695683"),
        chainlink: None,
        pyth_id: Some("cb7a1d45139117f8d3da0a4b67264579aa905e3b124efede272634f094e1e9d1"),
    },
    TokenFeed {
        token: address!("0x61299774020dA444Af134c82fa83E3810b309991"),
        chainlink: None,
        pyth_id: Some("3d4a2bd9535be6ce8059d75eadeba507b043257321aa544717c56fa19b49e35d"),
    },
    TokenFeed {
        token: address!("0xD14E0cd48CF32007D0F0b294Ee3d0b1530D8b04F"),
        chainlink: None,
        pyth_id: Some("07ad7b4a7662d19a6bc675f6b467172d2f3947fa653ca97555a9b20236406628"),
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
    matic_usd: RwLock<Option<PriceEntry>>,
    token_usd: RwLock<FxHashMap<Address, PriceEntry>>,
    /// Raw Chainlink USD answers (8 decimals) for integer profit conversion.
    chainlink_usd_raw: RwLock<FxHashMap<Address, I256>>,
    /// Config-driven Pyth feed overrides (token -> Pyth price ID).
    custom_pyth: RwLock<FxHashMap<Address, String>>,
    /// Config-driven Chainlink feed overrides (token -> aggregator address).
    custom_chainlink: RwLock<FxHashMap<Address, Address>>,
    /// Tokens whose current USD raw came from Hermes (vs on-chain Chainlink).
    pyth_sourced: RwLock<rustc_hash::FxHashSet<Address>>,
    /// Configurable cache TTL — overrides DEFAULT_CACHE_TTL_MS.
    cache_ttl: Duration,
    /// Coalesce concurrent MATIC/USD refreshes (HF ticks at 200ms can stampede).
    matic_refresh: std::sync::Arc<tokio::sync::Mutex<()>>,
}

impl PriceOracle {
    #[must_use]
    pub fn new(http: Client, pyth_hermes_url: String, cache_ttl_ms: u64) -> Self {
        Self {
            http,
            pyth_hermes_url,
            matic_usd: RwLock::new(None),
            token_usd: RwLock::new(FxHashMap::default()),
            chainlink_usd_raw: RwLock::new(FxHashMap::default()),
            custom_pyth: RwLock::new(FxHashMap::default()),
            custom_chainlink: RwLock::new(FxHashMap::default()),
            pyth_sourced: RwLock::new(rustc_hash::FxHashSet::default()),
            cache_ttl: Duration::from_millis(cache_ttl_ms),
            matic_refresh: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn register_pyth_feed(&self, token: Address, feed_id: String) {
        self.custom_pyth
            .write()
            .insert(token, normalize_pyth_feed_id(&feed_id));
    }

    pub fn unregister_pyth_feed(&self, token: &Address) {
        self.custom_pyth.write().remove(token);
    }

    pub fn register_chainlink_feed(&self, token: Address, feed: Address) {
        self.custom_chainlink.write().insert(token, feed);
    }

    fn fresh(&self, entry: &PriceEntry) -> bool {
        entry.updated_at.elapsed() < self.cache_ttl
    }

    /// LF/HF prefetch may warm `token_usd[WMATIC]` without touching `matic_usd` — sync before another Hermes call.
    fn promote_wmatic_from_token_cache(&self) -> Option<f64> {
        let usd = {
            let cache = self.token_usd.read();
            let entry = cache.get(&WMATIC)?;
            if !self.fresh(entry)
                || entry.value.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater)
            {
                return None;
            }
            entry.value
        };
        // Slot only — do not rewrite chainlink_usd_raw via f64 round-trip (clobbers
        // exact Chainlink/Pyth raw already stored by cache_token_usd).
        self.store_matic_usd_slot(usd);
        Some(usd)
    }

    pub(crate) fn cache_token_usd(
        &self,
        token: Address,
        usd: f64,
        chainlink_raw: I256,
        now: Instant,
    ) {
        if usd.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
            return;
        }
        self.token_usd.write().insert(
            token,
            PriceEntry {
                value: usd,
                updated_at: now,
            },
        );
        self.chainlink_usd_raw.write().insert(token, chainlink_raw);
        if token == WMATIC {
            // Slot only — raw already inserted above; store_matic_usd would round-trip f64.
            self.store_matic_usd_slot(usd);
        }
    }

    fn cache_token_usd_from_pyth(
        &self,
        token: Address,
        usd: f64,
        chainlink_raw: I256,
        now: Instant,
    ) {
        self.cache_token_usd(token, usd, chainlink_raw, now);
        self.pyth_sourced.write().insert(token);
    }

    fn mark_chainlink_sourced(&self, token: Address) {
        self.pyth_sourced.write().remove(&token);
    }

    /// True when the cached USD raw for `token` last came from Hermes.
    #[must_use]
    pub(crate) fn is_pyth_sourced(&self, token: &Address) -> bool {
        self.pyth_sourced.read().contains(token)
    }

    fn collect_pyth_id_groups(&self, tokens: &[Address]) -> FxHashMap<String, Vec<Address>> {
        let custom = self.custom_pyth.read();
        let mut out: FxHashMap<String, Vec<Address>> = FxHashMap::default();
        for token in tokens {
            let id = custom
                .get(token)
                .map(|s| normalize_pyth_feed_id(s))
                .or_else(|| pyth_feed(token).map(normalize_pyth_feed_id));
            let Some(id) = id else {
                continue;
            };
            out.entry(id).or_default().push(*token);
        }
        for addrs in out.values_mut() {
            addrs.sort_unstable();
            addrs.dedup();
        }
        out
    }

    fn usd_quote_fresh_for_token(&self, token: &Address) -> bool {
        if let Some(entry) = self.token_usd.read().get(token) {
            return self.fresh(entry) && entry.value > 0.0;
        }
        *token == WMATIC && self.cached_matic_usd().is_some()
    }

    fn dedupe_price_prefetch_tokens(tokens: &[Address]) -> Vec<Address> {
        let mut sorted = tokens.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        sorted
    }

    pub fn cached_matic_usd(&self) -> Option<f64> {
        self.matic_usd
            .read()
            .as_ref()
            .filter(|entry| self.fresh(entry))
            .map(|entry| entry.value)
    }

    /// Fresh MATIC/USD from cache slots populated by prefetch (no RPC).
    #[must_use]
    pub fn resolve_matic_usd_cached(&self) -> Option<f64> {
        if let Some(usd) = self.cached_matic_usd() {
            return Some(usd);
        }
        self.promote_wmatic_from_token_cache()
    }

    pub fn last_known_matic_usd(&self) -> Option<(f64, Duration)> {
        self.matic_usd.read().as_ref().and_then(|entry| {
            crate::pipeline::sim_sanity::matic_usd_for_flash_cap(entry.value)
                .map(|usd| (usd, entry.updated_at.elapsed()))
        })
    }

    pub async fn get_matic_usd<P: Provider<Ethereum> + Clone + Send + 'static>(
        &self,
        provider: Option<&P>,
    ) -> f64 {
        if let Some(usd) = self.resolve_matic_usd_cached() {
            return usd;
        }
        // Singleflight: LF enrich, HF flash-cap, background warm, and TUI all share this.
        let _guard = self.matic_refresh.lock().await;
        if let Some(usd) = self.resolve_matic_usd_cached() {
            return usd;
        }
        if let Some(p) = provider {
            self.refresh_chainlink_token_usd(p, &[WMATIC]).await;
            if let Some(usd) = self.resolve_matic_usd_cached() {
                crate::debug!("matic/usd refresh: source=chainlink usd={usd}");
                return usd;
            }
            crate::debug!("Chainlink MATIC/USD missing or stale after multicall — trying Pyth");
        } else {
            crate::debug!("no state RPC for Chainlink MATIC/USD — trying Pyth");
        }
        if let Some(usd) = self.fetch_pyth_matic_usd().await {
            self.store_matic_usd(usd);
            crate::debug!("matic/usd refresh: source=pyth usd={usd}");
            return usd;
        }
        if let Some(stale) = self.touch_stale_matic_usd("Chainlink and Pyth unavailable") {
            return stale;
        }
        crate::warn!(
            "oracle has no MATIC/USD price — Chainlink and Pyth unavailable, no cached value"
        );
        DEFAULT_MATIC_USD
    }

    /// Pyth + cache only — no Chainlink RPC required.
    pub async fn get_matic_usd_offline(&self) -> f64 {
        if let Some(usd) = self.resolve_matic_usd_cached() {
            return usd;
        }
        let _guard = self.matic_refresh.lock().await;
        if let Some(usd) = self.resolve_matic_usd_cached() {
            return usd;
        }
        if let Some(usd) = self.fetch_pyth_matic_usd().await {
            self.store_matic_usd(usd);
            crate::debug!("matic/usd refresh offline: source=pyth usd={usd}");
            return usd;
        }
        if let Some(stale) = self.touch_stale_matic_usd("Pyth unavailable") {
            return stale;
        }
        crate::warn!("oracle has no MATIC/USD price — Pyth unavailable, no cached value");
        DEFAULT_MATIC_USD
    }

    /// Refresh MATIC/USD for flash-cap sizing (fresh cache, then singleflight via get_matic_*).
    pub async fn ensure_matic_usd_for_flash_cap<P: Provider<Ethereum> + Clone + Send + 'static>(
        &self,
        state_provider: Option<&P>,
    ) -> Option<f64> {
        use crate::pipeline::sim_sanity::matic_usd_for_flash_cap;

        if let Some(cached) = self.resolve_matic_usd_cached()
            && let Some(usd) = matic_usd_for_flash_cap(cached)
        {
            return Some(usd);
        }
        // Lock lives inside get_matic_* — do not nest matic_refresh here (non-reentrant).
        let usd = match state_provider {
            Some(p) => self.get_matic_usd(Some(p)).await,
            None => self.get_matic_usd_offline().await,
        };
        matic_usd_for_flash_cap(usd)
    }

    fn touch_stale_matic_usd(&self, reason: &str) -> Option<f64> {
        let stale = self.matic_usd.read().as_ref().map(|e| e.value)?;
        if !(stale.is_finite() && stale > 0.0) {
            return None;
        }
        crate::warn!("oracle using stale MATIC/USD — {reason}");
        self.matic_usd.write().replace(PriceEntry {
            value: stale,
            updated_at: Instant::now(),
        });
        Some(stale)
    }

    pub async fn prefetch_token_usd_offline(&self, tokens: &[Address]) {
        let mut need = Vec::new();
        {
            let cache = self.token_usd.read();
            let custom_py = self.custom_pyth.read();
            for token in tokens {
                if cache.get(token).is_some_and(|entry| self.fresh(entry)) {
                    continue;
                }
                if pyth_feed(token).is_some() || custom_py.contains_key(token) {
                    need.push(*token);
                }
            }
        }
        need = Self::dedupe_price_prefetch_tokens(&need);
        if need.is_empty() {
            return;
        }
        let pyth_ids = self.collect_pyth_id_groups(&need);
        if pyth_ids.is_empty() {
            return;
        }
        let id_count = pyth_ids.len();
        let ids: Vec<String> = pyth_ids.keys().cloned().collect();
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let started = Instant::now();
        match self.fetch_pyth(&id_refs).await {
            Ok((prices, parse)) => {
                let cached = self.apply_pyth_quotes(pyth_ids, &prices);
                if parse.rejected > 0 || parse.chunk_errors > 0 {
                    crate::info!(
                        "pyth prefetch offline: ids={} accepted={} rejected={} cached={} chunk_err={} ms={}",
                        id_count,
                        parse.accepted,
                        parse.rejected,
                        cached,
                        parse.chunk_errors,
                        started.elapsed().as_millis()
                    );
                } else {
                    crate::debug!(
                        "pyth prefetch offline: ids={} accepted={} cached={} ms={}",
                        id_count,
                        parse.accepted,
                        cached,
                        started.elapsed().as_millis()
                    );
                }
            }
            Err(e) => {
                crate::warn!("pyth prefetch offline failed: ids={id_count} err={e}");
            }
        }
    }

    /// Batch `latestRoundData` via Multicall3 (deduped feeds, trusted-round gate).
    async fn refresh_chainlink_token_usd<P: Provider<Ethereum> + Clone + Send + 'static>(
        &self,
        provider: &P,
        tokens: &[Address],
    ) {
        if tokens.is_empty() {
            return;
        }
        let feed_map: FxHashMap<Address, Vec<Address>> = {
            let custom_cl = self.custom_chainlink.read();
            let mut feed_map: FxHashMap<Address, Vec<Address>> = FxHashMap::default();
            for token in tokens {
                let feed = custom_cl
                    .get(token)
                    .copied()
                    .or_else(|| chainlink_feed(token));
                if let Some(feed) = feed {
                    feed_map.entry(feed).or_default().push(*token);
                }
            }
            for addrs in feed_map.values_mut() {
                addrs.sort_unstable();
                addrs.dedup();
            }
            feed_map
        };
        if feed_map.is_empty() {
            return;
        }
        let feeds: Vec<Address> = feed_map.keys().copied().collect();
        let items: Vec<MulticallItem> = feeds
            .iter()
            .map(|feed| MulticallItem {
                target: *feed,
                data: encode_call(&IChainlinkAggregator::latestRoundDataCall {}),
            })
            .collect();
        let started = Instant::now();
        let results = match execute_multicall(provider, &items).await {
            Ok(r) => r,
            Err(e) => {
                crate::warn!(
                    "chainlink multicall failed: feeds={} err={e:#}",
                    feeds.len()
                );
                return;
            }
        };
        let mut stats = ChainlinkRefreshStats {
            feeds: feeds.len() as u32,
            ..ChainlinkRefreshStats::default()
        };
        let now = Instant::now();
        let mut failed_feeds: Vec<Address> = Vec::new();
        for (feed, bytes) in feeds.iter().zip(results) {
            let Some(bytes) = bytes else {
                stats.call_failed = stats.call_failed.saturating_add(1);
                failed_feeds.push(*feed);
                continue;
            };
            let Ok(data) = IChainlinkAggregator::latestRoundDataCall::abi_decode_returns(&bytes)
            else {
                stats.decode_failed = stats.decode_failed.saturating_add(1);
                failed_feeds.push(*feed);
                continue;
            };
            let Some((usd, answer)) = chainlink_latest_round_usd(
                data.roundId.to::<u128>(),
                data.answer,
                data.updatedAt,
                data.answeredInRound.to::<u128>(),
            ) else {
                stats.rejected_trust = stats.rejected_trust.saturating_add(1);
                failed_feeds.push(*feed);
                continue;
            };
            let Some(mapped) = feed_map.get(feed) else {
                continue;
            };
            for token in mapped {
                self.mark_chainlink_sourced(*token);
                self.cache_token_usd(*token, usd, answer, now);
                stats.cached_tokens = stats.cached_tokens.saturating_add(1);
            }
            stats.accepted = stats.accepted.saturating_add(1);
        }
        let ms = started.elapsed().as_millis();
        if stats.rejected_trust > 0
            || stats.call_failed > 0
            || stats.decode_failed > 0
            || stats.accepted == 0
        {
            let failed = failed_feeds
                .iter()
                .map(|a| format!("{a:#x}"))
                .collect::<Vec<_>>()
                .join(",");
            crate::info!(
                "chainlink refresh: feeds={} accepted={} cached={} call_fail={} decode_fail={} untrusted={} ms={} failed=[{failed}]",
                stats.feeds,
                stats.accepted,
                stats.cached_tokens,
                stats.call_failed,
                stats.decode_failed,
                stats.rejected_trust,
                ms
            );
        } else {
            crate::debug!(
                "chainlink refresh: feeds={} accepted={} cached={} ms={}",
                stats.feeds,
                stats.accepted,
                stats.cached_tokens,
                ms
            );
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
            let custom_cl = self.custom_chainlink.read();
            let custom_py = self.custom_pyth.read();
            for token in tokens {
                if cache.get(token).is_some_and(|entry| self.fresh(entry)) {
                    continue;
                }
                let has_cl = chainlink_feed(token).is_some() || custom_cl.contains_key(token);
                let has_py = pyth_feed(token).is_some() || custom_py.contains_key(token);
                if has_cl || has_py {
                    need.push(*token);
                }
            }
        }
        need = Self::dedupe_price_prefetch_tokens(&need);
        if need.is_empty() {
            return;
        }
        crate::debug!(
            "oracle prefetch: need={} provider={}",
            need.len(),
            provider.is_some()
        );

        if let Some(p) = provider {
            // Only tokens with a CL feed — Pyth-only majors skip the multicall.
            let cl_need: Vec<Address> = {
                let custom_cl = self.custom_chainlink.read();
                need.iter()
                    .copied()
                    .filter(|token| {
                        custom_cl.contains_key(token) || chainlink_feed(token).is_some()
                    })
                    .collect()
            };
            if !cl_need.is_empty() {
                self.refresh_chainlink_token_usd(p, &cl_need).await;
            }
        }

        let pyth_need: Vec<Address> = {
            let cache = self.token_usd.read();
            need.iter()
                .copied()
                .filter(|token| !cache.get(token).is_some_and(|e| self.fresh(e)))
                .collect()
        };
        let pyth_ids = self.collect_pyth_id_groups(&pyth_need);
        if pyth_ids.is_empty() {
            return;
        }
        let id_count = pyth_ids.len();
        let ids: Vec<String> = pyth_ids.keys().cloned().collect();
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let started = Instant::now();
        match self.fetch_pyth(&id_refs).await {
            Ok((prices, parse)) => {
                let cached = self.apply_pyth_quotes(pyth_ids, &prices);
                if parse.rejected > 0 || parse.chunk_errors > 0 {
                    crate::info!(
                        "pyth prefetch: ids={} accepted={} rejected={} cached={} chunk_err={} ms={}",
                        id_count,
                        parse.accepted,
                        parse.rejected,
                        cached,
                        parse.chunk_errors,
                        started.elapsed().as_millis()
                    );
                } else {
                    crate::debug!(
                        "pyth prefetch: ids={} accepted={} cached={} ms={}",
                        id_count,
                        parse.accepted,
                        cached,
                        started.elapsed().as_millis()
                    );
                }
            }
            Err(e) => {
                crate::warn!("pyth prefetch failed: ids={id_count} err={e}");
            }
        }
    }

    fn store_matic_usd_slot(&self, usd: f64) {
        self.matic_usd.write().replace(PriceEntry {
            value: usd,
            updated_at: Instant::now(),
        });
    }

    fn store_matic_usd(&self, usd: f64) {
        self.store_matic_usd_slot(usd);
        if let Some(raw) = usd_to_chainlink_raw(usd) {
            self.chainlink_usd_raw.write().insert(WMATIC, raw);
        }
    }

    async fn fetch_pyth_matic_usd(&self) -> Option<f64> {
        let (prices, _) = self.fetch_pyth(&[PYTH_MATIC_USD_ID]).await.ok()?;
        let quote = prices.get(PYTH_MATIC_USD_ID)?;
        if quote.usd.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
            return None;
        }
        self.cache_token_usd_from_pyth(WMATIC, quote.usd, quote.chainlink_raw, Instant::now());
        Some(quote.usd)
    }

    fn apply_pyth_quotes(
        &self,
        pyth_ids: FxHashMap<String, Vec<Address>>,
        prices: &FxHashMap<String, PythQuote>,
    ) -> u32 {
        let now = Instant::now();
        let mut cached = 0u32;
        for (id, tokens) in pyth_ids {
            let Some(quote) = prices.get(&id) else {
                continue;
            };
            for token in tokens {
                self.cache_token_usd_from_pyth(token, quote.usd, quote.chainlink_raw, now);
                cached = cached.saturating_add(1);
            }
        }
        cached
    }

    async fn fetch_pyth(
        &self,
        ids: &[&str],
    ) -> anyhow::Result<(FxHashMap<String, PythQuote>, PythParseStats)> {
        if ids.is_empty() {
            return Ok((FxHashMap::default(), PythParseStats::default()));
        }
        let chunks: Vec<&[&str]> = ids.chunks(PYTH_FETCH_CHUNK).collect();
        if chunks.len() == 1 {
            return self.fetch_pyth_chunk(chunks[0]).await;
        }
        // Parallel Hermes GETs — keep partial success if one chunk fails.
        let mut tasks = tokio::task::JoinSet::new();
        for chunk in chunks {
            let owned: Vec<String> = chunk.iter().map(|id| (*id).to_owned()).collect();
            let http = self.http.clone();
            let base = self.pyth_hermes_url.clone();
            tasks.spawn(async move {
                let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
                Self::fetch_pyth_chunk_with(&http, &base, &refs).await
            });
        }
        let mut out = FxHashMap::with_capacity_and_hasher(ids.len(), FxBuildHasher);
        let mut stats = PythParseStats::default();
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok((batch, chunk_stats))) => {
                    out.extend(batch);
                    stats.merge(chunk_stats);
                }
                Ok(Err(e)) => {
                    stats.chunk_errors = stats.chunk_errors.saturating_add(1);
                    crate::warn!("pyth chunk failed (keeping other chunks): {e}");
                }
                Err(e) => {
                    stats.chunk_errors = stats.chunk_errors.saturating_add(1);
                    crate::warn!("pyth chunk task join failed: {e}");
                }
            }
        }
        if out.is_empty() && stats.chunk_errors > 0 {
            anyhow::bail!(
                "all pyth chunks failed (chunk_errors={})",
                stats.chunk_errors
            );
        }
        Ok((out, stats))
    }

    async fn fetch_pyth_chunk(
        &self,
        ids: &[&str],
    ) -> anyhow::Result<(FxHashMap<String, PythQuote>, PythParseStats)> {
        Self::fetch_pyth_chunk_with(&self.http, &self.pyth_hermes_url, ids).await
    }

    async fn fetch_pyth_chunk_with(
        http: &Client,
        base_url: &str,
        ids: &[&str],
    ) -> anyhow::Result<(FxHashMap<String, PythQuote>, PythParseStats)> {
        match Self::fetch_pyth_once_with(http, base_url, ids).await {
            Ok(prices) => Ok(prices),
            Err(e) => {
                crate::debug!("Pyth Hermes request failed — retrying once: {e}");
                Self::fetch_pyth_once_with(http, base_url, ids).await
            }
        }
    }

    async fn fetch_pyth_once_with(
        http: &Client,
        base_url: &str,
        ids: &[&str],
    ) -> anyhow::Result<(FxHashMap<String, PythQuote>, PythParseStats)> {
        if ids.is_empty() {
            return Ok((FxHashMap::default(), PythParseStats::default()));
        }
        let url = pyth_updates_url(base_url, ids)?;
        let resp = http
            .get(url)
            .timeout(ORACLE_HTTP_TIMEOUT)
            .send()
            .await?
            .error_for_status()?;
        let body: PythHermesResponse = resp.json().await?;
        let mut out = FxHashMap::with_capacity_and_hasher(body.parsed.len(), FxBuildHasher);
        let mut stats = PythParseStats::default();
        for item in body.parsed {
            stats.parsed = stats.parsed.saturating_add(1);
            let Some(mantissa) = item.price.mantissa.as_i128() else {
                stats.rejected = stats.rejected.saturating_add(1);
                continue;
            };
            let conf = item.price.conf.as_ref().and_then(PythMantissa::as_i128);
            let publish_time = item.price.publish_time;
            let Some(quote) = pyth_fields_to_quote(mantissa, item.price.expo, conf, publish_time)
            else {
                stats.rejected = stats.rejected.saturating_add(1);
                continue;
            };
            out.insert(normalize_pyth_feed_id(&item.id), quote);
            stats.accepted = stats.accepted.saturating_add(1);
        }
        Ok((out, stats))
    }

    pub fn token_usd(&self, token: &Address) -> Option<f64> {
        self.token_usd.read().get(token).map(|e| e.value)
    }

    /// USD quote only when cache TTL is still valid (UI / rate-build).
    #[must_use]
    pub fn token_usd_fresh(&self, token: &Address) -> Option<f64> {
        self.fresh_token_usd(token)
    }

    /// USD quote only when cache TTL is still valid (rate-build path).
    pub(crate) fn fresh_token_usd(&self, token: &Address) -> Option<f64> {
        let cache = self.token_usd.read();
        let entry = cache.get(token)?;
        if !self.fresh(entry) || entry.value.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater)
        {
            return None;
        }
        Some(entry.value)
    }

    /// Integer-only token/MATIC rate when both feeds have Chainlink answers cached.
    pub fn token_matic_rate_per_unit_integer(&self, token: &Address) -> Option<U256> {
        if !self.usd_quote_fresh_for_token(token) || !self.usd_quote_fresh_for_token(&WMATIC) {
            return None;
        }
        let raw = self.chainlink_usd_raw.read();
        let token_raw = raw.get(token).copied()?;
        let matic_raw = raw.get(&WMATIC).copied()?;
        drop(raw);
        integer_matic_rate_from_raw(token_raw, matic_raw)
    }

    /// One lock for batch rate builds (LF/HF snapshot enrich).
    pub(crate) fn integer_matic_rates_batch(&self, addrs: &[Address]) -> FxHashMap<Address, U256> {
        if !self.usd_quote_fresh_for_token(&WMATIC) {
            return FxHashMap::default();
        }
        let raw = self.chainlink_usd_raw.read();
        let Some(matic_raw) = raw.get(&WMATIC).copied() else {
            return FxHashMap::default();
        };
        // Snapshot USD cache once — avoids per-addr `token_usd` re-locks on LF enrich.
        let token_usd = self.token_usd.read();
        let mut out = FxHashMap::with_capacity_and_hasher(addrs.len(), FxBuildHasher);
        for addr in addrs {
            let fresh = token_usd
                .get(addr)
                .is_some_and(|e| self.fresh(e) && e.value > 0.0);
            if !fresh {
                continue;
            }
            let Some(token_raw) = raw.get(addr).copied() else {
                continue;
            };
            if let Some(rate) = integer_matic_rate_from_raw(token_raw, matic_raw) {
                out.insert(*addr, rate);
            }
        }
        out
    }

    /// Fresh WMATIC Chainlink USD answer (8 decimals) for integer flash-cap sizing.
    #[must_use]
    pub fn fresh_matic_usd_chainlink_raw(&self) -> Option<I256> {
        if !self.usd_quote_fresh_for_token(&WMATIC) {
            return None;
        }
        let raw = self.chainlink_usd_raw.read();
        let matic_raw = raw.get(&WMATIC).copied()?;
        let Ok(v) = i128::try_from(matic_raw) else {
            return None;
        };
        (v > 0).then_some(matic_raw)
    }
}

#[inline]
fn integer_matic_rate_from_raw(token_raw: I256, matic_raw: I256) -> Option<U256> {
    let rate = chainlink_usd_to_matic_rate_per_unit(token_raw, matic_raw);
    if rate.is_zero() || rate < MIN_TOKEN_TO_MATIC_RATE {
        None
    } else {
        Some(rate)
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

/// Strip optional `0x` and lowercase — Hermes keys and config must match.
#[must_use]
fn normalize_pyth_feed_id(id: &str) -> String {
    let id = id.trim();
    let id = id
        .strip_prefix("0x")
        .or_else(|| id.strip_prefix("0X"))
        .unwrap_or(id);
    id.to_ascii_lowercase()
}

#[derive(Debug, Default, Clone, Copy)]
struct PythParseStats {
    parsed: u32,
    accepted: u32,
    rejected: u32,
    chunk_errors: u32,
}

impl PythParseStats {
    fn merge(&mut self, other: Self) {
        self.parsed = self.parsed.saturating_add(other.parsed);
        self.accepted = self.accepted.saturating_add(other.accepted);
        self.rejected = self.rejected.saturating_add(other.rejected);
        self.chunk_errors = self.chunk_errors.saturating_add(other.chunk_errors);
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct ChainlinkRefreshStats {
    feeds: u32,
    accepted: u32,
    cached_tokens: u32,
    call_failed: u32,
    decode_failed: u32,
    rejected_trust: u32,
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
fn chainlink_round_trusted(
    round_id: u128,
    answer: I256,
    updated_at: U256,
    answered_in_round: u128,
) -> bool {
    if round_id == 0 || answer <= I256::ZERO {
        return false;
    }
    if answered_in_round < round_id {
        return false;
    }
    let Ok(updated) = u64::try_from(updated_at) else {
        return false;
    };
    if updated == 0 {
        return false;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    if updated.saturating_sub(now) > 5 {
        return false;
    }
    now.saturating_sub(updated) <= CHAINLINK_MAX_STALENESS_SECS
}

#[inline]
fn chainlink_latest_round_usd(
    round_id: u128,
    answer: I256,
    updated_at: U256,
    answered_in_round: u128,
) -> Option<(f64, I256)> {
    if !chainlink_round_trusted(round_id, answer, updated_at, answered_in_round) {
        return None;
    }
    let usd = chainlink_answer_to_usd(answer)?;
    Some((usd, answer))
}

/// Built-in Chainlink aggregator for a Polygon token, if any.
#[must_use]
pub fn builtin_chainlink_feed(token: &Address) -> Option<Address> {
    chainlink_feed(token)
}

/// Built-in Pyth price feed id (hex, no `0x` prefix), if any.
#[must_use]
pub fn builtin_pyth_feed_id(token: &Address) -> Option<&'static str> {
    pyth_feed(token)
}

/// O(1) feed maps — first TOKEN_FEEDS entry wins on address collisions (do not last-write GNS over GHST).
static CHAINLINK_BY_TOKEN: LazyLock<FxHashMap<Address, Address>> = LazyLock::new(|| {
    let mut m = FxHashMap::with_capacity_and_hasher(TOKEN_FEEDS.len(), FxBuildHasher);
    for entry in TOKEN_FEEDS {
        if let Some(feed) = entry.chainlink {
            m.entry(entry.token).or_insert(feed);
        }
    }
    m
});

static PYTH_BY_TOKEN: LazyLock<FxHashMap<Address, &'static str>> = LazyLock::new(|| {
    let mut m = FxHashMap::with_capacity_and_hasher(TOKEN_FEEDS.len(), FxBuildHasher);
    for entry in TOKEN_FEEDS {
        if let Some(id) = entry.pyth_id {
            m.entry(entry.token).or_insert(id);
        }
    }
    m
});

#[inline]
fn chainlink_feed(token: &Address) -> Option<Address> {
    CHAINLINK_BY_TOKEN.get(token).copied()
}

#[inline]
fn pyth_feed(token: &Address) -> Option<&'static str> {
    PYTH_BY_TOKEN.get(token).copied()
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OracleFeedSources {
    pub builtin_pyth: bool,
    pub builtin_chainlink: bool,
    pub custom_pyth: bool,
    pub custom_chainlink: bool,
}

impl OracleFeedSources {
    #[must_use]
    pub fn any(&self) -> bool {
        self.builtin_pyth || self.builtin_chainlink || self.custom_pyth || self.custom_chainlink
    }
}

impl PriceOracle {
    /// Static + config oracle mappings (no network fetch).
    #[must_use]
    pub fn feed_sources(&self, token: &Address) -> OracleFeedSources {
        OracleFeedSources {
            builtin_pyth: pyth_feed(token).is_some(),
            builtin_chainlink: chainlink_feed(token).is_some(),
            custom_pyth: self.custom_pyth.read().contains_key(token),
            custom_chainlink: self.custom_chainlink.read().contains_key(token),
        }
    }

    #[must_use]
    pub fn has_configured_feed(&self, token: &Address) -> bool {
        self.feed_sources(token).any()
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
    #[serde(default)]
    conf: Option<PythMantissa>,
    expo: i32,
    publish_time: Option<i64>,
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

#[must_use]
fn pyth_quote_trusted(mantissa: i128, conf: Option<i128>, publish_time: Option<i64>) -> bool {
    if mantissa <= 0 {
        return false;
    }
    let Some(publish_time) = publish_time else {
        return false;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);
    if now.saturating_sub(publish_time) > PYTH_MAX_PUBLISH_AGE_SECS
        || publish_time.saturating_sub(now) > 5
    {
        return false;
    }
    let Some(conf) = conf.filter(|c| *c >= 0) else {
        return false;
    };
    conf.saturating_mul(10_000) <= mantissa.saturating_mul(PYTH_MAX_CONF_BPS)
}

fn pyth_fields_to_quote(
    mantissa: i128,
    expo: i32,
    conf: Option<i128>,
    publish_time: Option<i64>,
) -> Option<PythQuote> {
    if !pyth_quote_trusted(mantissa, conf, publish_time) {
        return None;
    }
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
impl PriceOracle {
    pub(crate) fn seed_float_usd_for_test(&self, token: Address, usd: f64) {
        self.token_usd.write().insert(
            token,
            PriceEntry {
                value: usd,
                updated_at: Instant::now(),
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;

    #[test]
    fn chainlink_round_trusted_rejects_stale_and_incomplete_rounds() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_secs();
        let answer = I256::from(U256::from(100_000_000u64));
        assert!(chainlink_round_trusted(1, answer, U256::from(now), 1));
        assert!(!chainlink_round_trusted(
            1,
            answer,
            U256::from(now.saturating_sub(CHAINLINK_MAX_STALENESS_SECS + 1)),
            1
        ));
        assert!(!chainlink_round_trusted(1, answer, U256::from(now), 0));
        assert!(!chainlink_round_trusted(0, answer, U256::from(now), 1));
    }

    #[test]
    fn normalize_pyth_feed_id_strips_0x_and_lowercases() {
        assert_eq!(normalize_pyth_feed_id("0xAaBbCc"), "aabbcc");
        assert_eq!(normalize_pyth_feed_id("AABBCC"), "aabbcc");
    }

    #[test]
    fn configured_pyth_feed_overrides_builtin_mapping() {
        let oracle = PriceOracle::new(
            reqwest::Client::new(),
            "https://hermes.pyth.network".to_string(),
            DEFAULT_CACHE_TTL_MS,
        );
        oracle.register_pyth_feed(WMATIC, "0xcustom".to_string());

        let groups = oracle.collect_pyth_id_groups(&[WMATIC]);
        assert_eq!(groups.get("custom"), Some(&vec![WMATIC]));
        assert_eq!(groups.len(), 1);
    }

    #[test]
    fn pyth_quote_trusted_rejects_stale_and_wide_confidence() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_secs() as i64;
        assert!(pyth_quote_trusted(1_000_000, Some(5_000), Some(now)));
        assert!(!pyth_quote_trusted(1_000_000, Some(20_000), Some(now)));
        assert!(!pyth_quote_trusted(
            1_000_000,
            Some(5_000),
            Some(now - PYTH_MAX_PUBLISH_AGE_SECS - 1)
        ));
    }

    #[test]
    fn promote_wmatic_from_token_cache_avoids_duplicate_matic_slot() {
        let oracle = PriceOracle::new(
            reqwest::Client::new(),
            "https://hermes.pyth.network".to_string(),
            10_000,
        );
        oracle.cache_token_usd(
            WMATIC,
            0.42,
            I256::from(U256::from(42_000_000u64)),
            Instant::now(),
        );
        assert!(oracle.cached_matic_usd().is_some());
        assert_eq!(oracle.promote_wmatic_from_token_cache(), Some(0.42));
    }

    #[test]
    fn pyth_to_chainlink_raw_matches_eight_decimal_usd() {
        let raw = pyth_to_chainlink_raw(73_717_820, -8).expect("raw");
        assert_eq!(raw, I256::from(U256::from(73_717_820u64)));
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_secs() as i64;
        let quote = pyth_fields_to_quote(100_000_000, -8, Some(50_000), Some(now)).expect("quote");
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
        use crate::core::constants::{
            AAVE, BAL, COMP, CRV, GHST, GRT, LINK, MANA, SAND, SNX, SUSHI, UNI, WST_ETH,
        };
        let tokens = [
            LINK, AAVE, CRV, SUSHI, BAL, SAND, MANA, UNI, GRT, GHST, WST_ETH, COMP, SNX,
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
        let now = Instant::now();
        let wmatic_raw = I256::from(U256::from(50_000_000u64));
        let usdc_raw = I256::from(U256::from(100_000_000u64));
        oracle.cache_token_usd(wmatic, 0.5, wmatic_raw, now);
        oracle.cache_token_usd(usdc, 1.0, usdc_raw, now);
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
    fn oracle_feeds_include_polygon_wsteth_and_frax() {
        let wsteth = address!("0x03b54A0eF8042C0f6A77B15e637c9f5d7c6790D0");
        let bridged_wsteth = address!("0x03b54A6e9a984069379fae1a4fC4dBAE93B3bCCD");
        let frax = address!("0x45c32fA6DF82ead1e2EF74d32b0366496F5fDe09");
        assert!(pyth_feed(&wsteth).is_some());
        assert!(pyth_feed(&bridged_wsteth).is_some());
        assert!(pyth_feed(&frax).is_some());
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
        let uni = address!("0xb33eaad8d922b1083446dc23f610c2567fb5180f");

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
        assert_eq!(
            chainlink_feed(&uni),
            Some(address!("0xdf0Fb4e4F928d2dCB76f438575fDD8682386e13C"))
        );
        // Hub majors that used to be Pyth-only now have Polygon CL proxies.
        assert!(chainlink_feed(&crate::core::constants::DAI).is_some());
        assert!(chainlink_feed(&crate::core::constants::LINK).is_some());
        assert!(chainlink_feed(&crate::core::constants::AAVE).is_some());
    }

    #[test]
    fn oracle_feeds_include_batch1_and_batch2_polygon_mints() {
        let oracle = PriceOracle::new(
            reqwest::Client::new(),
            "https://hermes.pyth.network".to_string(),
            DEFAULT_CACHE_TTL_MS,
        );
        let tokens = [
            (
                address!("0x61fFE097137d543f019F5257E1a1Ff7A6C5F0b68"),
                "UNI",
            ),
            (
                address!("0x50B728D8D964fd00C2d0AAD81718b71311feF68a"),
                "SNX",
            ),
            (address!("0xA571963278014B5B3A686778747fDf8ad4dFBb94"), "SD"),
            (
                address!("0x6f8a06447Ff6FcF75d803135a7de15CE88C1d4ec"),
                "SHIB",
            ),
            (
                // GHST hub — must keep GHST/USD, not GNS/USD.
                address!("0x385Eeac5cB85A38A9a07A70c73e0a3271CfB54A7"),
                "GHST",
            ),
            (
                address!("0xE5417Af564e4bFDA1c483642db72007871397896"),
                "GNS",
            ),
            (
                address!("0xBbba073C31bF03b8ACf7c28EF0738DeCF3695683"),
                "SAND",
            ),
            (
                address!("0x61299774020dA444Af134c82fa83E3810b309991"),
                "RNDR",
            ),
        ];
        for (token, label) in tokens {
            assert!(
                builtin_pyth_feed_id(&token).is_some(),
                "{label} missing builtin Pyth feed id"
            );
            assert!(
                oracle.has_configured_feed(&token),
                "{label} missing configured oracle feed"
            );
        }
        // GHST must not resolve to the Gains GNS Pyth id.
        let ghst = address!("0x385Eeac5cB85A38A9a07A70c73e0a3271CfB54A7");
        let gns = address!("0xE5417Af564e4bFDA1c483642db72007871397896");
        assert_ne!(
            builtin_pyth_feed_id(&ghst),
            builtin_pyth_feed_id(&gns),
            "GHST and GNS must not share a Pyth feed id"
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
        assert!(
            oracle
                .last_known_matic_usd()
                .is_some_and(|(usd, age)| usd == 1.0 && age >= Duration::from_millis(1))
        );
    }
}
