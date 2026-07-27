use std::path::Path;
use std::sync::LazyLock;

use parking_lot::Mutex;

use alloy::primitives::Address;
use alloy::primitives::address;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use super::RateEnrichStats;
use super::price_oracle::PriceOracle;
use super::pyth_catalog;
use crate::core::types::{FoundCycle, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::PoolMeta;

/// Human-reviewed Polygon tokens — Hermes search hints (not auto-enabled).
pub const CURATED_POLYGON_TOKEN_HINTS: &[(&str, Address, &str)] = &[
    (
        "stMATIC",
        address!("0x3A58a54C066FdC0f2D55FC9C89F0415A6B4066ff"),
        "STMATIC",
    ),
    (
        "stMATIC",
        address!("0x3A58a54C066FdC0f2D55FC9C89F0415C92eBf3C4"),
        "STMATIC",
    ),
    (
        "MaticX",
        address!("0xFa68FB4628dFF1028C0C610198bB4D9B5AfE0902"),
        "MATICX",
    ),
    (
        "MaticX",
        address!("0xfa68FB4628DFF1028CFEc22b4162FCcd0d45efb6"),
        "MATICX",
    ),
    (
        "wstETH",
        address!("0x03b54A0eF8042C0f6A77B15e637c9f5d7c6790D0"),
        "WSTETH",
    ),
    (
        "QUICK v2",
        address!("0xB5C064F955D8e7F38fE0460C3a0aB2c2C2599D2D"),
        "QUICK",
    ),
    (
        "GRT",
        // Must match `core::constants::GRT` (was typo 0x5fe2b58a… — dead digests).
        address!("0x5fe2b58c013d7601147dcdd68c143a77499f5531"),
        "GRT",
    ),
    (
        "FRAX",
        address!("0x45c32fA6DF82ead1e2EF74d32b0366496F5fDe09"),
        "FRAX",
    ),
    (
        "EURS",
        address!("0xE111178A87A3BFf0c8d18DECBa5798827539Ae99"),
        "EURS",
    ),
    (
        "EURA",
        address!("0xF32E6dC7709c596c5a5f328fa01eDd8eC3F62517"),
        "EURS",
    ),
    (
        "miMATIC",
        address!("0xa3Fa99A148fA48D14Ed51d610c367C61876997F1"),
        "MAI",
    ),
    (
        "miMATIC",
        address!("0x63d38FCf3cC014735B28339F47EC3FA9BA97b4B9"),
        "MAI",
    ),
    (
        "UNI",
        address!("0x61fFE097137d543f019F5257E1a1Ff7A6C5F0b68"),
        "UNI",
    ),
    (
        "SNX",
        address!("0x50B728D8D964fd00C2d0AAD81718b71311feF68a"),
        "SNX",
    ),
    (
        "SUSHI",
        address!("0xbbC11D55375F0B37f8A30b102C9ce143B097671e"),
        "SUSHI",
    ),
    (
        "1INCH",
        address!("0x9c2C5fd7b07E95EE044DDeba0E97a665F142394f"),
        "1INCH",
    ),
    (
        "KNC",
        address!("0x1C954E8fe737F99f68Fa1CCda3e51ebDB291948C"),
        "KNC",
    ),
    (
        "COMP",
        address!("0x8505b9d2254A7Ae468c0E9dd10Ccea3A837aef5c"),
        "COMP",
    ),
    (
        "AVAX",
        address!("0x2C89bbc92BD86F8075d1DEcc58C7F4E0107f286b"),
        "AVAX",
    ),
    (
        "SOL",
        address!("0xd93f7E271cB87c23AaA73edC008A79646d1F9912"),
        "SOL",
    ),
    (
        "PAXG",
        address!("0x553d3D295e0f695B9228246232eDF400ed3560B5"),
        "PAXG",
    ),
    (
        "SD",
        address!("0xA571963278014B5B3A686778747fDf8ad4dFBb94"),
        "SD",
    ),
    (
        "SHIB",
        address!("0x6f8a06447Ff6FcF75d803135a7de15CE88C1d4ec"),
        "SHIB",
    ),
    (
        // Gains Network GNS — not GHST (0x385Ee… is Aavegotchi).
        "GNS",
        address!("0xE5417Af564e4bFDA1c483642db72007871397896"),
        "GNS",
    ),
    (
        "SAND",
        address!("0xBbba073C31bF03b8ACf7c28EF0738DeCF3695683"),
        "SAND",
    ),
    (
        "RNDR",
        address!("0x61299774020dA444Af134c82fa83E3810b309991"),
        "RENDER",
    ),
    (
        "BAL",
        address!("0xD14E0cd48CF32007D0F0b294Ee3d0b1530D8b04F"),
        "BAL",
    ),
];

static RUNTIME_UNMAPPED_DEMAND: LazyLock<Mutex<FxHashMap<Address, u64>>> =
    LazyLock::new(|| Mutex::new(FxHashMap::default()));

#[must_use]
pub fn hint_label(addr: &Address) -> Option<&'static str> {
    token_symbol_label(addr).or_else(|| {
        CURATED_POLYGON_TOKEN_HINTS
            .iter()
            .find(|(_, a, _)| a == addr)
            .map(|(label, _, _)| *label)
    })
}

/// Known Polygon mainnet symbols for audit / demand logs (checksum-insensitive).
#[must_use]
pub fn token_symbol_label(addr: &Address) -> Option<&'static str> {
    super::token_labels::lookup_symbol(addr)
}

/// Record addresses seen on cycles / pool metas when they lack configured feeds.
pub fn record_unmapped_token_demand(
    oracle: &PriceOracle,
    arena: &StateArena,
    pool_metas: &[PoolMeta],
    cycles: &[FoundCycle],
) {
    let mut local: FxHashMap<Address, (u64, u64)> = FxHashMap::default();
    let mut touch = |token: TokenIndex, cycle_delta: u64, pool_delta: u64| {
        let Some(addr) = arena.token_address(token) else {
            return;
        };
        if oracle.has_configured_feed(&addr) {
            return;
        }
        let entry = local.entry(addr).or_insert((0, 0));
        entry.0 = entry.0.saturating_add(cycle_delta);
        entry.1 = entry.1.saturating_add(pool_delta);
    };
    for cycle in cycles {
        touch(cycle.start_token, 1, 0);
        for edge in &cycle.edges {
            touch(edge.token_in, 1, 0);
            touch(edge.token_out, 1, 0);
        }
    }
    for meta in pool_metas {
        for &token in &meta.tokens {
            touch(token, 0, 1);
        }
    }
    if local.is_empty() {
        return;
    }
    let addrs: Vec<Address> = local.keys().copied().collect();
    let Some(mut global) = RUNTIME_UNMAPPED_DEMAND.try_lock() else {
        return;
    };
    for (addr, (cycle_hits, pool_hits)) in local {
        let entry = global.entry(addr).or_insert(0);
        *entry = entry
            .saturating_add(cycle_hits.saturating_mul(4))
            .saturating_add(pool_hits);
    }
    drop(global);
    super::auto_feeds::note_unmapped_addresses(oracle, addrs);
}

/// Ranked unmapped-demand log (auto-feed scan is separate; see `auto_feeds`).
pub fn log_ranked_unmapped_demand(
    oracle: &PriceOracle,
    lf_pass: u64,
    rate_stats: &RateEnrichStats,
) {
    if rate_stats.unresolved == 0 {
        return;
    }
    if lf_pass > 2 && !lf_pass.is_multiple_of(30) {
        return;
    }
    let Some(global) = RUNTIME_UNMAPPED_DEMAND.try_lock() else {
        return;
    };
    if global.is_empty() {
        return;
    }
    let mut ranked: Vec<(Address, u64)> = global.iter().map(|(&a, &s)| (a, s)).collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    ranked.truncate(20);
    let mut parts = Vec::with_capacity(ranked.len());
    for (addr, score) in ranked {
        if oracle.has_configured_feed(&addr) {
            continue;
        }
        let label = hint_label(&addr).unwrap_or("?");
        parts.push(format!("{label}:{addr} score={score}"));
    }
    if parts.is_empty() {
        return;
    }
    crate::info!(
        "oracle unmapped demand (enrich unresolved={}): {}",
        rate_stats.unresolved,
        parts.join(", ")
    );
    if let Err(e) = persist_runtime_demand_snapshot(default_runtime_demand_path().as_path()) {
        crate::debug!("oracle demand snapshot write failed: {e}");
    }
}

/// Clone in-process LF/runtime demand scores (empty when CLI runs offline).
#[must_use]
pub fn snapshot_runtime_unmapped_demand() -> FxHashMap<Address, u64> {
    RUNTIME_UNMAPPED_DEMAND
        .try_lock()
        .map(|g| g.clone())
        .unwrap_or_default()
}

#[must_use]
pub fn default_runtime_demand_path() -> std::path::PathBuf {
    std::env::var_os("RPBOT_ORACLE_DEMAND_SNAPSHOT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("target/run-logs/oracle-demand.json"))
}

#[derive(Debug, Serialize, Deserialize)]
struct DemandSnapshotRow {
    address: String,
    demand_score: u64,
    #[serde(default)]
    symbol: String,
}

pub fn persist_runtime_demand_snapshot(path: &Path) -> anyhow::Result<()> {
    let snap = snapshot_runtime_unmapped_demand();
    if snap.is_empty() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut lines = Vec::with_capacity(snap.len());
    for (addr, score) in snap {
        lines.push((addr, score));
    }
    lines.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let body: Vec<DemandSnapshotRow> = lines
        .into_iter()
        .map(|(addr, score)| DemandSnapshotRow {
            address: format!("{addr:#x}"),
            demand_score: score,
            symbol: token_symbol_label(&addr).unwrap_or("").to_string(),
        })
        .collect();
    let file = std::fs::File::create(path)?;
    serde_json::to_writer_pretty(file, &body)?;
    Ok(())
}

pub fn load_runtime_demand_snapshot(path: &Path) -> anyhow::Result<FxHashMap<Address, u64>> {
    let raw = std::fs::read(path)?;
    let rows: Vec<DemandSnapshotRow> = crate::util::simd_json_parse(&raw)?;
    let mut out = FxHashMap::default();
    for row in rows {
        let Ok(addr) = row.address.parse::<Address>() else {
            continue;
        };
        out.insert(addr, row.demand_score);
    }
    Ok(out)
}

/// Parse `oracle unmapped demand` lines from an rpbot run log (fallback when snapshot missing).
pub fn parse_runtime_demand_from_log(path: &Path) -> anyhow::Result<FxHashMap<Address, u64>> {
    let raw = std::fs::read_to_string(path)?;
    let mut out = FxHashMap::default();
    for line in raw.lines() {
        if !line.contains("oracle unmapped demand") {
            continue;
        }
        let Some(rest) = line.split("oracle unmapped demand").nth(1) else {
            continue;
        };
        for part in rest.split(',') {
            let part = part.trim();
            let Some((left, score_part)) = part.rsplit_once(" score=") else {
                continue;
            };
            let Ok(score) = score_part.trim().parse::<u64>() else {
                continue;
            };
            let addr_s = left.rsplit_once(':').map(|(_, a)| a.trim()).unwrap_or(left);
            let Ok(addr) = addr_s.parse::<Address>() else {
                continue;
            };
            let prev = out.get(&addr).copied().unwrap_or(0);
            if score > prev {
                out.insert(addr, score);
            }
        }
    }
    Ok(out)
}

/// Hermes USD-spot scan result for one address.
#[derive(Debug, Clone)]
pub struct UsdFeedScanRow {
    pub address: Address,
    pub label: Option<&'static str>,
    pub query: Option<String>,
    pub status: UsdFeedScanStatus,
    pub feed_id: Option<String>,
    pub symbol: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsdFeedScanStatus {
    /// Crypto TOKEN/USD spot match (safe to register).
    UsdMatch,
    /// Hermes returned feeds but none passed USD-spot + hint rules.
    NoUsd,
    /// No ticker label — cannot query Hermes meaningfully.
    NoSymbol,
    /// Catalog request failed (retry later).
    Error,
}

/// Hermes-check addresses for a USD spot feed.
///
/// Catalog GETs run concurrently via JoinSet (cap 8) — sequential 20× scan was seconds of
/// wall time on every auto-feed batch.
pub async fn scan_addresses_for_usd_feeds(
    http: &reqwest::Client,
    hermes_url: &str,
    api_key: Option<&str>,
    rows: &[(Address, Option<&'static str>)],
) -> anyhow::Result<Vec<UsdFeedScanRow>> {
    const HERMES_SCAN_CONCURRENCY: usize = 8;

    let mut out = Vec::with_capacity(rows.len());
    let mut tasks = tokio::task::JoinSet::new();
    let mut inflight = 0usize;

    for &(address, label) in rows {
        let query = hermes_query_for_addr(address, label);
        let Some(query) = query else {
            out.push(UsdFeedScanRow {
                address,
                label,
                query: None,
                status: UsdFeedScanStatus::NoSymbol,
                feed_id: None,
                symbol: None,
            });
            continue;
        };
        // Bound concurrency without futures-util buffer_unordered.
        while inflight >= HERMES_SCAN_CONCURRENCY {
            if let Some(joined) = tasks.join_next().await {
                inflight = inflight.saturating_sub(1);
                match joined {
                    Ok(row) => out.push(row),
                    Err(e) => crate::warn!("oracle hermes scan task join failed: {e}"),
                }
            }
        }
        let http = http.clone();
        let hermes = hermes_url.to_string();
        let api_key = api_key.map(str::to_string);
        tasks.spawn(async move {
            match pyth_catalog::search_pyth_feeds(&http, &hermes, api_key.as_deref(), &query).await
            {
                Ok(candidates) => {
                    if let Some(best) =
                        pyth_catalog::pick_best_usd_candidate_for_hint(&candidates, &query)
                    {
                        UsdFeedScanRow {
                            address,
                            label,
                            query: Some(query),
                            status: UsdFeedScanStatus::UsdMatch,
                            feed_id: Some(best.id.clone()),
                            symbol: Some(best.symbol.clone()),
                        }
                    } else {
                        UsdFeedScanRow {
                            address,
                            label,
                            query: Some(query),
                            status: UsdFeedScanStatus::NoUsd,
                            feed_id: None,
                            symbol: None,
                        }
                    }
                }
                Err(_) => UsdFeedScanRow {
                    address,
                    label,
                    query: Some(query),
                    status: UsdFeedScanStatus::Error,
                    feed_id: None,
                    symbol: None,
                },
            }
        });
        inflight = inflight.saturating_add(1);
    }
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(row) => out.push(row),
            Err(e) => crate::warn!("oracle hermes scan task join failed: {e}"),
        }
    }
    Ok(out)
}

fn hermes_query_for_addr(address: Address, label: Option<&'static str>) -> Option<String> {
    if let Some((_, _, hint)) = CURATED_POLYGON_TOKEN_HINTS
        .iter()
        .find(|(_, a, _)| *a == address)
    {
        return Some((*hint).to_string());
    }
    let label = label?;
    // Strip common suffix noise so QUICKv2 → QUICK for catalog search.
    let q = label
        .trim_end_matches("v2")
        .trim_end_matches("V2")
        .trim_end_matches("v3")
        .trim_end_matches("V3");
    let q = if q.is_empty() { label } else { q };
    Some(q.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;
    use std::io::Write;

    #[test]
    fn parse_runtime_demand_log_takes_max_score_per_address() {
        let dir = std::env::temp_dir().join(format!("rpbot-demand-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let path = dir.join("run.log");
        let line = "INFO oracle oracle unmapped demand (enrich unresolved=3): \
            UNI:0x61fFE097137d543f019F5257E1a1Ff7A6C5F0b68 score=100, \
            ?:0x3d2bD0e15829AA5C362a4144FdF4A1112fa29B5c score=200";
        let mut f = std::fs::File::create(&path).expect("file");
        writeln!(f, "{line}").expect("write");
        writeln!(
            f,
            "INFO oracle oracle unmapped demand (enrich unresolved=2): \
             ?:0x3d2bD0e15829AA5C362a4144FdF4A1112fa29B5c score=500"
        )
        .expect("write");
        let map = parse_runtime_demand_from_log(&path).expect("parse");
        assert_eq!(
            map.get(&address!("0x3d2bD0e15829AA5C362a4144FdF4A1112fa29B5c")),
            Some(&500)
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
