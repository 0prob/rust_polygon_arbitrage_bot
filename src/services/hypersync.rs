//! Envio HyperSync client — fast historical log querying via REST JSON API.
//!
//! HyperSync provides block-range log queries over HTTP at ~1000× the speed of
//! `eth_getLogs`.  We use it for two purposes:
//!
//! 1. **Gap-fill discovery**: catch pool creation events that the PostgreSQL
//!    indexer may not have processed yet (e.g. brand-new pools within the last
//!    few blocks, or blocks produced during a PG indexer restart).
//!
//! 2. **Indexer lag fallback**: when the PG indexer is more than
//!    `indexer_max_lag_blocks` behind chain head, HyperSync can supply the
//!    missing window so the bot stays current without waiting for PG to catch up.
//!
//! We intentionally do NOT replace the PG discovery path.  HyperSync gives us
//! the raw creation-event logs but none of the normalised metadata (token lists,
//! `poolType`, `poolId`, Balancer enrichment) that PG provides.  We use the log
//! decoder to build minimal `DiscoveredPool` entries for routable protocols only.
//!
//! Protocol: `POST <base>/query` with a JSON body (see `QueryRequest`).
//! The server returns blocks, logs, and a `next_block`/`archive_height` cursor.
//!
//! Env var: `HYPERSYNC_URL` (default: `https://polygon.hypersync.xyz`)
//! Env var: `HYPERSYNC_ENABLED` (`true`/`false`, default: `false`)

use std::sync::LazyLock;
use std::time::Duration;

use alloy::hex;
use alloy::primitives::{Address, B256, FixedBytes};
use alloy::sol_types::SolEvent;
use anyhow::Context;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::abis::{
    IAlgebraFactory, IUniswapV2Factory, IUniswapV3Factory, IUniswapV4Factory,
};
use crate::core::types::ProtocolType;
use crate::infra::http::{HttpClientOpts, build_static};
use crate::services::discovery::{DiscoveredPool, is_routable_pool, synthetic_cache_address};

// ── Canonical factory creation topics ────────────────────────────────────────

/// `PairCreated(address indexed,address indexed,address,uint256)`
pub const V2_PAIR_CREATED_TOPIC: B256 = IUniswapV2Factory::PairCreated::SIGNATURE_HASH;

/// `PoolCreated(address indexed,address indexed,uint24 indexed,int24,address)`
pub const V3_POOL_CREATED_TOPIC: B256 = IUniswapV3Factory::PoolCreated::SIGNATURE_HASH;

/// `Pool(address indexed,address indexed,address)` — Algebra V1.9 / QuickSwap Integral
pub const ALGEBRA_POOL_CREATED_TOPIC: B256 = IAlgebraFactory::Pool::SIGNATURE_HASH;

/// `Initialize(bytes32 indexed,address indexed,address indexed,uint24,int24,address,uint160,int24)`
pub const V4_INITIALIZE_TOPIC: B256 = IUniswapV4Factory::Initialize::SIGNATURE_HASH;

// ── HTTP client ───────────────────────────────────────────────────────────────

const DEFAULT_HYPERSYNC_URL: &str = "https://polygon.hypersync.xyz";

static HYPERSYNC_HTTP: LazyLock<Client> = LazyLock::new(|| {
    build_static(
        HttpClientOpts {
            timeout: Duration::from_secs(15),
            pool_max_idle_per_host: 4,
            max_redirects: 0,
        },
        "envio hypersync client",
    )
});

// ── Public client ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct HyperSyncClient {
    url: String,
    enabled: bool,
}

impl HyperSyncClient {
    pub fn new(url: Option<String>, enabled: bool) -> Self {
        let url = url
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_HYPERSYNC_URL.to_string());
        let url = url.trim_end_matches('/').to_string();
        Self { url, enabled }
    }

    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Fetch the latest archived block height from HyperSync.
    pub async fn fetch_height(&self) -> anyhow::Result<u64> {
        let endpoint = format!("{}/height", self.url);
        let body = HYPERSYNC_HTTP
            .get(&endpoint)
            .send()
            .await?
            .error_for_status()?
            .json::<HeightResponse>()
            .await?;
        body.height
            .or(body.archive_height)
            .context("HyperSync /height response missing height field")
    }

    /// Query raw logs from HyperSync.
    ///
    /// Returns `(archive_height, logs)`.  All four factory creation topics are
    /// requested in one pass; the caller may filter by `log.topics[0]`.
    pub async fn query_logs(
        &self,
        from_block: u64,
        to_block: Option<u64>,
        topics: &[B256],
    ) -> anyhow::Result<(u64, Vec<HyperSyncLog>)> {
        let endpoint = format!("{}/query", self.url);
        // Build topic filter: outer vec = OR across logs, inner = AND across positions.
        // We want any log whose topic[0] is one of `topics`.
        let topic0_hex: Vec<String> = topics.iter().map(|t| format!("{t:#x}")).collect();
        let topic0_refs: Vec<&str> = topic0_hex.iter().map(String::as_str).collect();

        let req = QueryRequest {
            from_block,
            to_block,
            logs: vec![LogSelection {
                // topics[0] = OR list; empty inner vecs = wildcard for positions 1-3.
                topics: vec![topic0_refs],
            }],
            field_selection: FieldSelection {
                log: vec!["block_number", "address", "topics", "data"],
            },
        };

        let resp = HYPERSYNC_HTTP
            .post(&endpoint)
            .json(&req)
            .send()
            .await?
            .error_for_status()?
            .json::<QueryResponse>()
            .await?;

        let archive_height = resp.archive_height.unwrap_or(from_block);
        // Flatten: logs may arrive at top-level or per-block depending on server version.
        let mut raw: Vec<RawLog> = resp.logs;
        for block in resp.data {
            raw.extend(block.logs);
        }

        let mut out = Vec::with_capacity(raw.len());
        for r in raw {
            if let Some(log) = decode_raw_log(r) {
                out.push(log);
            }
        }
        Ok((archive_height, out))
    }

    /// High-level pool discovery from HyperSync creation events.
    ///
    /// Returns `(archive_height, discovered_pools)`.  Only routable pools
    /// (as judged by [`is_routable_pool`]) are included.
    pub async fn discover_recent_pools(
        &self,
        from_block: u64,
        to_block: Option<u64>,
    ) -> anyhow::Result<(u64, Vec<DiscoveredPool>)> {
        let topics = [
            V2_PAIR_CREATED_TOPIC,
            V3_POOL_CREATED_TOPIC,
            ALGEBRA_POOL_CREATED_TOPIC,
            V4_INITIALIZE_TOPIC,
        ];
        let (height, logs) = self.query_logs(from_block, to_block, &topics).await?;
        let mut pools: Vec<DiscoveredPool> = logs
            .into_iter()
            .filter_map(|l| parse_creation_log(&l))
            .filter(is_routable_pool)
            .collect();
        // Stable ordering: newest-block-last (HyperSync already returns ascending order).
        pools.sort_unstable_by_key(|p| p.created_block);
        crate::info!(
            "hypersync discovery: blocks={from_block}..{} pools={}",
            to_block.unwrap_or(height),
            pools.len()
        );
        Ok((height, pools))
    }
}

// ── Wire types (REST API) ─────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct QueryRequest<'a> {
    from_block: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    to_block: Option<u64>,
    logs: Vec<LogSelection<'a>>,
    field_selection: FieldSelection<'a>,
}

#[derive(Debug, Serialize)]
struct LogSelection<'a> {
    /// Outer = per-topic position; inner = OR candidates for that position.
    topics: Vec<Vec<&'a str>>,
}

#[derive(Debug, Serialize)]
struct FieldSelection<'a> {
    log: Vec<&'a str>,
}

#[derive(Debug, Deserialize)]
struct HeightResponse {
    height: Option<u64>,
    archive_height: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct QueryResponse {
    archive_height: Option<u64>,
    #[serde(default)]
    logs: Vec<RawLog>,
    /// Some server versions embed logs per-block here.
    #[serde(default)]
    data: Vec<RawBlock>,
}

#[derive(Debug, Deserialize)]
struct RawBlock {
    #[serde(default)]
    logs: Vec<RawLog>,
}

/// Raw JSON log entry from HyperSync (all fields are optional for forward compat).
#[derive(Debug, Deserialize)]
struct RawLog {
    block_number: Option<u64>,
    address: Option<String>,
    topics: Option<Vec<String>>,
    data: Option<String>,
}

// ── Decoded log ───────────────────────────────────────────────────────────────

/// A fully decoded log with parsed primitives (no heap allocations beyond Vec).
#[derive(Debug, Clone)]
pub struct HyperSyncLog {
    pub block_number: u64,
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Vec<u8>,
}

fn decode_raw_log(r: RawLog) -> Option<HyperSyncLog> {
    let block_number = r.block_number?;
    let address = r.address?.parse::<Address>().ok()?;
    let topics = r
        .topics
        .unwrap_or_default()
        .into_iter()
        .filter_map(|t| {
            let s = t.trim_start_matches("0x");
            let bytes = hex::decode(s).ok()?;
            if bytes.len() == 32 {
                Some(B256::from_slice(&bytes))
            } else {
                None
            }
        })
        .collect();
    let data = r
        .data
        .as_deref()
        .map(|d| hex::decode(d.trim_start_matches("0x")).unwrap_or_default())
        .unwrap_or_default();
    Some(HyperSyncLog { block_number, address, topics, data })
}

// ── Pool creation log parser ──────────────────────────────────────────────────

fn parse_creation_log(log: &HyperSyncLog) -> Option<DiscoveredPool> {
    let topic0 = log.topics.first()?;
    if *topic0 == V2_PAIR_CREATED_TOPIC {
        parse_v2_pair_created(log)
    } else if *topic0 == V3_POOL_CREATED_TOPIC {
        parse_v3_pool_created(log)
    } else if *topic0 == ALGEBRA_POOL_CREATED_TOPIC {
        parse_algebra_pool_created(log)
    } else if *topic0 == V4_INITIALIZE_TOPIC {
        parse_v4_initialize(log)
    } else {
        None
    }
}

/// `PairCreated(address indexed token0, address indexed token1, address pair, uint256 pairCount)`
fn parse_v2_pair_created(log: &HyperSyncLog) -> Option<DiscoveredPool> {
    // topics[1] = token0, topics[2] = token1 (indexed)
    let token0 = Address::from_word(*log.topics.get(1)?);
    let token1 = Address::from_word(*log.topics.get(2)?);
    // data = abi.encode(pair, pairCount) — pair is the first 32-byte word, address in last 20 bytes
    if log.data.len() < 32 {
        return None;
    }
    let pair = Address::from_slice(&log.data[12..32]);
    if !crate::services::discovery::is_plausible_contract_address(pair) {
        return None;
    }
    Some(DiscoveredPool {
        pool_key: format!("{pair:#x}"),
        address: pair,
        protocol: ProtocolType::UniswapV2,
        protocol_label: "UNISWAP_V2".to_string(),
        tokens: vec![token0, token1],
        fee_bps: 30,
        tick_spacing: None,
        pool_id: None,
        pool_id_verified: false,
        hooks: None,
        pool_type: None,
        created_block: log.block_number,
    })
}

/// `PoolCreated(address indexed token0, address indexed token1, uint24 indexed fee, int24 tickSpacing, address pool)`
fn parse_v3_pool_created(log: &HyperSyncLog) -> Option<DiscoveredPool> {
    let token0 = Address::from_word(*log.topics.get(1)?);
    let token1 = Address::from_word(*log.topics.get(2)?);
    // fee in topics[3] low 24 bits
    let fee_topic = log.topics.get(3)?;
    let fee_raw = u32::from_be_bytes([fee_topic[28], fee_topic[29], fee_topic[30], fee_topic[31]]);
    let fee_bps = fee_raw / 100;
    // data = abi.encode(tickSpacing, pool) — 2 × 32-byte words
    if log.data.len() < 64 {
        return None;
    }
    let tick_spacing =
        i32::from_be_bytes([log.data[28], log.data[29], log.data[30], log.data[31]]);
    let pool = Address::from_slice(&log.data[44..64]);
    if !crate::services::discovery::is_plausible_contract_address(pool) {
        return None;
    }
    Some(DiscoveredPool {
        pool_key: format!("{pool:#x}"),
        address: pool,
        protocol: ProtocolType::UniswapV3,
        protocol_label: "UNISWAP_V3".to_string(),
        tokens: vec![token0, token1],
        fee_bps,
        tick_spacing: Some(tick_spacing),
        pool_id: None,
        pool_id_verified: false,
        hooks: None,
        pool_type: None,
        created_block: log.block_number,
    })
}

/// `Pool(address indexed token0, address indexed token1, address pool)`
/// Algebra V1.9 / QuickSwap Integral — no fee in creation event; default slot0 on-chain.
fn parse_algebra_pool_created(log: &HyperSyncLog) -> Option<DiscoveredPool> {
    let token0 = Address::from_word(*log.topics.get(1)?);
    let token1 = Address::from_word(*log.topics.get(2)?);
    // data = abi.encode(pool) — address in last 20 bytes of first word
    if log.data.len() < 32 {
        return None;
    }
    let pool = Address::from_slice(&log.data[12..32]);
    if !crate::services::discovery::is_plausible_contract_address(pool) {
        return None;
    }
    Some(DiscoveredPool {
        pool_key: format!("{pool:#x}"),
        address: pool,
        protocol: ProtocolType::UniswapV3,
        protocol_label: "QUICKSWAP_V3".to_string(),
        tokens: vec![token0, token1],
        fee_bps: 0, // dynamic — will be fetched on-chain at hydration
        tick_spacing: None,
        pool_id: None,
        pool_id_verified: false,
        hooks: None,
        pool_type: None,
        created_block: log.block_number,
    })
}

/// `Initialize(bytes32 indexed id, address indexed currency0, address indexed currency1,
///             uint24 fee, int24 tickSpacing, address hooks, uint160 sqrtPriceX96, int24 tick)`
fn parse_v4_initialize(log: &HyperSyncLog) -> Option<DiscoveredPool> {
    let pool_id_bytes = *log.topics.get(1)?;
    let pool_id: FixedBytes<32> = pool_id_bytes;
    let currency0 = Address::from_word(*log.topics.get(2)?);
    let currency1 = Address::from_word(*log.topics.get(3)?);
    // data = abi.encode(fee, tickSpacing, hooks, sqrtPriceX96, tick)
    // Each non-indexed param takes a 32-byte ABI word.
    if log.data.len() < 96 {
        return None;
    }
    let fee_raw = u32::from_be_bytes([log.data[28], log.data[29], log.data[30], log.data[31]]);
    let fee_bps = fee_raw / 100;
    let tick_spacing =
        i32::from_be_bytes([log.data[60], log.data[61], log.data[62], log.data[63]]);
    let hooks = Address::from_slice(&log.data[76..96]);
    let hooks = if hooks.is_zero() { None } else { Some(hooks) };
    let address = synthetic_cache_address(&pool_id);
    Some(DiscoveredPool {
        pool_key: format!("{pool_id:#x}"),
        address,
        protocol: ProtocolType::UniswapV4,
        protocol_label: "UNISWAP_V4".to_string(),
        tokens: vec![currency0, currency1],
        fee_bps,
        tick_spacing: Some(tick_spacing),
        pool_id: Some(pool_id),
        pool_id_verified: true,
        hooks,
        pool_type: None,
        created_block: log.block_number,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_hashes_are_non_zero() {
        assert_ne!(V2_PAIR_CREATED_TOPIC, B256::ZERO);
        assert_ne!(V3_POOL_CREATED_TOPIC, B256::ZERO);
        assert_ne!(ALGEBRA_POOL_CREATED_TOPIC, B256::ZERO);
        assert_ne!(V4_INITIALIZE_TOPIC, B256::ZERO);
        // Each topic must be distinct.
        assert_ne!(V2_PAIR_CREATED_TOPIC, V3_POOL_CREATED_TOPIC);
        assert_ne!(V3_POOL_CREATED_TOPIC, ALGEBRA_POOL_CREATED_TOPIC);
        assert_ne!(V3_POOL_CREATED_TOPIC, V4_INITIALIZE_TOPIC);
    }

    #[test]
    fn client_defaults_to_polygon_url() {
        let client = HyperSyncClient::new(None, true);
        assert_eq!(client.url(), DEFAULT_HYPERSYNC_URL);
        assert!(client.is_enabled());
    }

    #[test]
    fn client_respects_custom_url_and_enabled_flag() {
        let c = HyperSyncClient::new(Some("https://custom.hypersync.xyz/".into()), false);
        assert_eq!(c.url(), "https://custom.hypersync.xyz");
        assert!(!c.is_enabled());
    }

    fn make_log(topic0: B256, extra_topics: Vec<B256>, data: Vec<u8>) -> HyperSyncLog {
        let mut topics = vec![topic0];
        topics.extend(extra_topics);
        HyperSyncLog {
            block_number: 50_000_000,
            address: Address::repeat_byte(0xf0),
            topics,
            data,
        }
    }

    #[test]
    fn parses_v2_pair_created_log() {
        let token0 = Address::repeat_byte(0xaa);
        let token1 = Address::repeat_byte(0xbb);
        let pair   = Address::repeat_byte(0xcc);
        let mut data = vec![0u8; 64];
        data[12..32].copy_from_slice(pair.as_slice());
        let log = make_log(
            V2_PAIR_CREATED_TOPIC,
            vec![token0.into_word(), token1.into_word()],
            data,
        );
        let pool = parse_creation_log(&log).expect("v2 PairCreated");
        assert_eq!(pool.protocol, ProtocolType::UniswapV2);
        assert_eq!(pool.address, pair);
        assert_eq!(pool.tokens, vec![token0, token1]);
        assert_eq!(pool.fee_bps, 30);
        assert_eq!(pool.created_block, 50_000_000);
    }

    #[test]
    fn parses_v3_pool_created_log() {
        let token0 = Address::repeat_byte(0x11);
        let token1 = Address::repeat_byte(0x22);
        let fee_raw: u32 = 3000;
        let mut fee_word = [0u8; 32];
        fee_word[28..32].copy_from_slice(&fee_raw.to_be_bytes());
        let pool_addr = Address::repeat_byte(0x44);
        let mut data = vec![0u8; 64];
        // tickSpacing = 60 in first word
        let ts: i32 = 60;
        data[28..32].copy_from_slice(&ts.to_be_bytes());
        data[44..64].copy_from_slice(pool_addr.as_slice());
        let log = make_log(
            V3_POOL_CREATED_TOPIC,
            vec![token0.into_word(), token1.into_word(), B256::from_slice(&fee_word)],
            data,
        );
        let pool = parse_creation_log(&log).expect("v3 PoolCreated");
        assert_eq!(pool.protocol, ProtocolType::UniswapV3);
        assert_eq!(pool.address, pool_addr);
        assert_eq!(pool.fee_bps, 30);
        assert_eq!(pool.tick_spacing, Some(60));
    }

    #[test]
    fn parses_v4_initialize_log() {
        let pool_id = FixedBytes::<32>::repeat_byte(0xde);
        let c0 = Address::repeat_byte(0x55);
        let c1 = Address::repeat_byte(0x66);
        let mut data = vec![0u8; 160];
        // word 0: fee = 3000
        let fee: u32 = 3000;
        data[28..32].copy_from_slice(&fee.to_be_bytes());
        // word 1: tickSpacing = 60
        let ts: i32 = 60;
        data[60..64].copy_from_slice(&ts.to_be_bytes());
        // word 2: hooks = zero address (hooks = None)
        let log = make_log(
            V4_INITIALIZE_TOPIC,
            vec![*pool_id, c0.into_word(), c1.into_word()],
            data,
        );
        let pool = parse_creation_log(&log).expect("v4 Initialize");
        assert_eq!(pool.protocol, ProtocolType::UniswapV4);
        assert_eq!(pool.pool_id, Some(pool_id));
        assert!(pool.pool_id_verified);
        assert_eq!(pool.fee_bps, 30);
        assert_eq!(pool.tick_spacing, Some(60));
        assert_eq!(pool.hooks, None); // zero hooks → None
    }

    #[test]
    fn decode_raw_log_handles_0x_prefix_and_missing_fields() {
        let good = RawLog {
            block_number: Some(1),
            address: Some("0xCcCCccccCCCCcCCCCCCcCcCccCcCCCcCcccccccC".into()),
            topics: Some(vec!["0x".to_string() + &"aa".repeat(32)]),
            data: Some("0x1234".into()),
        };
        let log = decode_raw_log(good).expect("decode good raw log");
        assert_eq!(log.block_number, 1);

        // Missing block_number → None
        let bad = RawLog {
            block_number: None,
            address: Some("0xCcCCccccCCCCcCCCCCCcCcCccCcCCCcCcccccccC".into()),
            topics: None,
            data: None,
        };
        assert!(decode_raw_log(bad).is_none());
    }
}
