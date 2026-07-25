//! Auto-discover Pyth USD feeds for unmapped tokens seen at runtime.
//!
//! Accumulates new unmapped addresses; when [`AUTO_FEED_SCAN_BATCH`] are pending,
//! Hermes-scans them, registers verified USD feeds, and marks the rest as no-feed.
//! State persists under `target/run-logs/oracle-auto-feeds.json`.
//!
//! **Safety:** only human-reviewed addresses ([`CURATED_POLYGON_TOKEN_HINTS`] /
//! hub tokens) may receive a Pyth feed. `token_labels` includes symbol clones
//! (fake USDC/LUNA/USDT, …); mapping those to Hermes majors overvalues dust and
//! caused live gas burns (LUNA profit dust priced as real Terra LUNA).

use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use alloy::primitives::Address;
use parking_lot::Mutex;
use reqwest::Client;
use rustc_hash::FxHashSet;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, watch};

use super::feed_audit::{
    CURATED_POLYGON_TOKEN_HINTS, UsdFeedScanStatus, hint_label, scan_addresses_for_usd_feeds,
    token_symbol_label,
};
use super::price_oracle::PriceOracle;
use crate::core::constants::{MIN_TOKEN_TO_MATIC_RATE, is_polygon_hub_token};

/// New unmapped tokens before a Hermes USD scan runs.
pub const AUTO_FEED_SCAN_BATCH: usize = 20;

static STORE: LazyLock<Mutex<AutoFeedStore>> =
    LazyLock::new(|| Mutex::new(AutoFeedStore::default()));
static PENDING: LazyLock<Mutex<FxHashSet<Address>>> =
    LazyLock::new(|| Mutex::new(FxHashSet::default()));
static SCAN_NOTIFY: LazyLock<Notify> = LazyLock::new(Notify::new);

/// Soft cap on persisted `no_feed` keys (live store hit 15k+ junk clones).
const NO_FEED_SOFT_CAP: usize = 2_048;

/// True when address is human-reviewed for a major-asset Pyth mapping.
#[must_use]
pub fn is_auto_feed_address_allowed(addr: Address) -> bool {
    if is_polygon_hub_token(addr) {
        return true;
    }
    // Built-in TOKEN_FEEDS majors are already mapped; allow re-verify if needed.
    if crate::services::oracle::price_oracle::builtin_pyth_feed_id(&addr).is_some()
        || crate::services::oracle::price_oracle::builtin_chainlink_feed(&addr).is_some()
    {
        return true;
    }
    CURATED_POLYGON_TOKEN_HINTS
        .iter()
        .any(|(_, a, _)| *a == addr)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AutoFeedStore {
    #[serde(default)]
    feeds: Vec<AutoFeedEntry>,
    #[serde(default)]
    no_feed: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AutoFeedEntry {
    address: String,
    feed_id: String,
    #[serde(default)]
    symbol: String,
}

#[must_use]
pub fn default_auto_feeds_path() -> PathBuf {
    std::env::var_os("RPBOT_ORACLE_AUTO_FEEDS")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/run-logs/oracle-auto-feeds.json"))
}

/// Load persisted auto-feeds, register into the oracle, seed no-feed set.
///
/// Drops persisted feeds for non-curated addresses (symbol-clone poison) and
/// rewrites the store so a restart does not re-apply junk→major Pyth maps.
/// Also un-blacklists curated/hub addresses that were no_feed'd by a prior
/// verify miss (e.g. MATIC not warm) so they can be re-scanned.
pub fn load_and_apply_auto_feeds(oracle: &PriceOracle) {
    let path = default_auto_feeds_path();
    let store = load_store(&path).unwrap_or_default();
    let mut feeds_n = 0u32;
    let mut purged = 0u32;
    let no_n;
    let unblocked;
    let cleared_no_feed;
    {
        let mut guard = STORE.lock();
        *guard = store;
        let mut keep = Vec::with_capacity(guard.feeds.len());
        for entry in std::mem::take(&mut guard.feeds) {
            let Ok(addr) = entry.address.parse::<Address>() else {
                purged += 1;
                continue;
            };
            if !is_auto_feed_address_allowed(addr) {
                // Poison: token_labels symbol hit Hermes major (LUNA/USDC/…).
                purged += 1;
                crate::warn!(
                    "oracle auto-feeds: purged non-curated feed {} → {} (symbol clone risk)",
                    entry.address,
                    entry.symbol
                );
                continue;
            }
            keep.push(entry);
        }
        guard.feeds = keep;
        // Curated/hub must not stay permanently no_feed (live: EURS/MAI blocked after
        // verify without MATIC warm → never re-scanned).
        let before_no = guard.no_feed.len();
        guard.no_feed.retain(|s| {
            s.parse::<Address>()
                .map(|a| !is_auto_feed_address_allowed(a))
                .unwrap_or(true)
        });
        unblocked = before_no.saturating_sub(guard.no_feed.len()) as u32;
        // Non-curated are never Hermes-queued; historical no_feed of junk clones
        // is pure disk bloat (live: 2048 soft-cap, feeds=0). Drop it.
        cleared_no_feed = guard.no_feed.len() as u32;
        if cleared_no_feed > 0 {
            crate::info!(
                "oracle auto-feeds: clearing {cleared_no_feed} obsolete no_feed entries (curated-only queue)"
            );
            guard.no_feed.clear();
        }
        for entry in &guard.feeds {
            let Ok(addr) = entry.address.parse::<Address>() else {
                continue;
            };
            if oracle.has_configured_feed(&addr) {
                continue;
            }
            oracle.register_pyth_feed(addr, entry.feed_id.clone());
            feeds_n += 1;
        }
        no_n = guard.no_feed.len() as u32;
    }
    if (purged > 0 || unblocked > 0 || cleared_no_feed > 0)
        && let Err(e) = persist_store(&path)
    {
        crate::warn!("oracle auto-feeds: purge persist failed: {e}");
    }
    if feeds_n > 0 || no_n > 0 || purged > 0 || unblocked > 0 || cleared_no_feed > 0 {
        crate::info!(
            "oracle auto-feeds: loaded feeds={feeds_n} purged={purged} unblocked_curated={unblocked} cleared_no_feed={cleared_no_feed} no_feed={no_n} path={}",
            path.display()
        );
    }
}

/// Note unmapped addresses for a future batch scan (skips configured / known no-feed).
///
/// Only curated/hub/builtin-feed addresses enter the Hermes queue. Long-tail junk
/// is hub-path priced and must never fill `no_feed` via failed major-symbol matches
/// (live store: feeds=0, no_feed=2048 of clones burning Hermes and disk).
pub fn note_unmapped_addresses(oracle: &PriceOracle, addrs: impl IntoIterator<Item = Address>) {
    let store = STORE.lock();
    let no_feed: FxHashSet<Address> = store
        .no_feed
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();
    let known_feeds: FxHashSet<Address> = store
        .feeds
        .iter()
        .filter_map(|e| e.address.parse().ok())
        .collect();
    drop(store);

    let mut pending = PENDING.lock();
    let before = pending.len();
    for addr in addrs {
        if !is_auto_feed_address_allowed(addr)
            || oracle.has_configured_feed(&addr)
            || no_feed.contains(&addr)
            || known_feeds.contains(&addr)
        {
            continue;
        }
        pending.insert(addr);
    }
    let after = pending.len();
    let added = after.saturating_sub(before);
    drop(pending);
    if added > 0 {
        crate::debug!(
            "oracle auto-feeds: pending_new={after} (+{added}, batch={AUTO_FEED_SCAN_BATCH})"
        );
        // Curated set is small — notify on any new allowed token (do not wait for 20).
        SCAN_NOTIFY.notify_one();
    }
}

#[must_use]
pub fn pending_auto_feed_count() -> usize {
    PENDING.lock().len()
}

/// Background sidecar: scan curated unmapped tokens (notify or 30s ticker).
pub fn spawn_auto_feed_sidecar(
    oracle: Arc<PriceOracle>,
    http: Client,
    hermes_url: String,
    mut shutdown: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return;
                    }
                }
                _ = SCAN_NOTIFY.notified() => {}
                _ = ticker.tick() => {}
            }
            if *shutdown.borrow() {
                return;
            }
            // Curated-only queue: scan as soon as anything is pending (batch was 20
            // for junk flood; with allow-list, waiting forever left EURS/MAI unmapped).
            if pending_auto_feed_count() == 0 {
                continue;
            }
            if let Err(e) = run_auto_feed_batch(&oracle, &http, &hermes_url).await {
                crate::warn!("oracle auto-feeds scan failed: {e:#}");
            }
        }
    });
}

async fn run_auto_feed_batch(
    oracle: &PriceOracle,
    http: &Client,
    hermes_url: &str,
) -> anyhow::Result<()> {
    let batch: Vec<Address> = {
        let mut pending = PENDING.lock();
        if pending.is_empty() {
            return Ok(());
        }
        let take: Vec<Address> = pending
            .iter()
            .copied()
            .filter(|a| is_auto_feed_address_allowed(*a))
            .take(AUTO_FEED_SCAN_BATCH)
            .collect();
        // Drop disallowed leftovers (legacy pending from pre-allow-list runs).
        pending.retain(|a| is_auto_feed_address_allowed(*a));
        for addr in &take {
            pending.remove(addr);
        }
        take
    };
    if batch.is_empty() {
        return Ok(());
    }
    let rows: Vec<(Address, Option<&'static str>)> = batch
        .iter()
        .map(|&addr| {
            (
                addr,
                hint_label(&addr).or_else(|| token_symbol_label(&addr)),
            )
        })
        .collect();
    crate::info!(
        "oracle auto-feeds: scanning batch={} (labeled={})",
        rows.len(),
        rows.iter().filter(|(_, l)| l.is_some()).count()
    );
    let results = scan_addresses_for_usd_feeds(http, hermes_url, &rows).await?;
    let mut added = 0u32;
    let mut marked = 0u32;
    let mut retry = 0u32;
    let path = default_auto_feeds_path();
    for r in results {
        match r.status {
            UsdFeedScanStatus::UsdMatch => {
                let Some(feed_id) = r.feed_id.clone() else {
                    mark_no_feed(r.address);
                    marked += 1;
                    continue;
                };
                // Symbol-only Hermes hits on non-curated addresses are poison
                // (live: fake LUNA @ 0x9cd6… → Crypto.LUNA/USD → dust "0.4 MATIC" arbs).
                if !is_auto_feed_address_allowed(r.address) {
                    mark_no_feed(r.address);
                    marked += 1;
                    crate::info!(
                        "oracle auto-feeds: reject non-curated {} → {} — marked no_feed",
                        r.address,
                        r.symbol.as_deref().unwrap_or("?")
                    );
                    continue;
                }
                if try_register_verified(oracle, r.address, &feed_id, r.symbol.as_deref()).await {
                    persist_feed(r.address, &feed_id, r.symbol.as_deref().unwrap_or(""));
                    added += 1;
                    crate::info!(
                        "oracle auto-feeds: added {} → {} ({})",
                        r.address,
                        r.symbol.as_deref().unwrap_or("?"),
                        feed_id
                    );
                } else if is_auto_feed_address_allowed(r.address) {
                    // Curated miss is usually MATIC/Hermes transient — retry, never permanent ban.
                    PENDING.lock().insert(r.address);
                    retry += 1;
                    crate::info!(
                        "oracle auto-feeds: verify failed curated {} ({}) — retry later",
                        r.address,
                        r.symbol.as_deref().unwrap_or("?")
                    );
                } else {
                    mark_no_feed(r.address);
                    marked += 1;
                    crate::info!(
                        "oracle auto-feeds: verify failed {} ({}) — marked no_feed",
                        r.address,
                        r.symbol.as_deref().unwrap_or("?")
                    );
                }
            }
            UsdFeedScanStatus::NoUsd | UsdFeedScanStatus::NoSymbol => {
                if is_auto_feed_address_allowed(r.address) {
                    // Keep curated pending so a later Hermes catalog hit can register.
                    PENDING.lock().insert(r.address);
                    retry += 1;
                } else {
                    mark_no_feed(r.address);
                    marked += 1;
                }
            }
            UsdFeedScanStatus::Error => {
                // Transient Hermes failure — retry later.
                PENDING.lock().insert(r.address);
                retry += 1;
            }
        }
    }
    if let Err(e) = persist_store(&path) {
        crate::debug!("oracle auto-feeds persist failed: {e}");
    }
    crate::info!(
        "oracle auto-feeds: batch done added={added} no_feed={marked} retry={retry} pending={}",
        pending_auto_feed_count()
    );
    Ok(())
}

async fn try_register_verified(
    oracle: &PriceOracle,
    token: Address,
    feed_id: &str,
    _symbol: Option<&str>,
) -> bool {
    // Integer token/MATIC needs WMATIC raw in cache — warm MATIC first (live: curated
    // EURS/MAI verify failed without this and were permanently no_feed'd).
    let _ = oracle.get_matic_usd_offline().await;
    oracle.register_pyth_feed(token, feed_id.to_string());
    oracle
        .prefetch_token_usd_offline(std::slice::from_ref(&token))
        .await;
    let usd_ok = oracle
        .token_usd(&token)
        .is_some_and(|u| u.partial_cmp(&0.0) == Some(std::cmp::Ordering::Greater));
    let rate_ok = oracle
        .token_matic_rate_per_unit_integer(&token)
        .is_some_and(|r| r >= MIN_TOKEN_TO_MATIC_RATE);
    if usd_ok && rate_ok {
        return true;
    }
    oracle.unregister_pyth_feed(&token);
    false
}

fn mark_no_feed(addr: Address) {
    // Never permanent-ban curated/hub — leave room for retry scans.
    if is_auto_feed_address_allowed(addr) {
        return;
    }
    let mut store = STORE.lock();
    let key = format!("{addr:#x}");
    if !store.no_feed.iter().any(|s| s.eq_ignore_ascii_case(&key)) {
        store.no_feed.push(key);
    }
    trim_no_feed(&mut store.no_feed);
    store.feeds.retain(|e| {
        e.address
            .parse::<Address>()
            .map(|a| a != addr)
            .unwrap_or(true)
    });
}

fn trim_no_feed(no_feed: &mut Vec<String>) {
    if no_feed.len() <= NO_FEED_SOFT_CAP {
        return;
    }
    // Drop oldest excess (push-order ≈ scan order).
    let drop_n = no_feed.len() - NO_FEED_SOFT_CAP;
    no_feed.drain(0..drop_n);
}

fn persist_feed(addr: Address, feed_id: &str, symbol: &str) {
    let mut store = STORE.lock();
    let key = format!("{addr:#x}");
    store.no_feed.retain(|s| !s.eq_ignore_ascii_case(&key));
    if let Some(existing) = store
        .feeds
        .iter_mut()
        .find(|e| e.address.eq_ignore_ascii_case(&key))
    {
        existing.feed_id = feed_id.to_string();
        existing.symbol = symbol.to_string();
    } else {
        store.feeds.push(AutoFeedEntry {
            address: key,
            feed_id: feed_id.to_string(),
            symbol: symbol.to_string(),
        });
    }
}

fn load_store(path: &Path) -> anyhow::Result<AutoFeedStore> {
    if !path.exists() {
        return Ok(AutoFeedStore::default());
    }
    let raw = std::fs::read(path)?;
    Ok(serde_json::from_slice(&raw)?)
}

fn persist_store(path: &Path) -> anyhow::Result<()> {
    let store = STORE.lock().clone();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(path)?;
    serde_json::to_writer_pretty(file, &store)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;

    #[test]
    fn batch_constant_is_twenty() {
        assert_eq!(AUTO_FEED_SCAN_BATCH, 20);
    }

    #[test]
    fn curated_sol_allowed_fake_luna_rejected() {
        // Human-reviewed Wormhole SOL (feed_audit curated list).
        let real_sol = address!("0xd93f7E271cB87c23AaA73edC008A79646d1F9912");
        assert!(is_auto_feed_address_allowed(real_sol));
        // Live poison: junk "LUNA" that was mapped to Crypto.LUNA/USD.
        let fake_luna = address!("0x9cd6746665D9557e1B9a775819625711d0693439");
        assert!(!is_auto_feed_address_allowed(fake_luna));
        // Fake USDC (not USDC.e / native).
        let fake_usdc = address!("0x576cf361711cd940cd9c397bb98c4c896cbd38de");
        assert!(!is_auto_feed_address_allowed(fake_usdc));
    }

    #[test]
    fn hub_wmatic_allowed() {
        assert!(is_auto_feed_address_allowed(crate::core::constants::WMATIC));
    }

    #[test]
    fn mark_no_feed_skips_curated_and_trims_cap() {
        let curated = address!("0xd93f7E271cB87c23AaA73edC008A79646d1F9912");
        // Clear and re-seed under lock for unit isolation.
        {
            let mut store = STORE.lock();
            store.no_feed.clear();
            store.feeds.clear();
        }
        mark_no_feed(curated);
        assert!(
            !STORE
                .lock()
                .no_feed
                .iter()
                .any(|s| s.eq_ignore_ascii_case(&format!("{curated:#x}"))),
            "curated must not enter no_feed"
        );
        let junk = address!("0x9cd6746665D9557e1B9a775819625711d0693439");
        for i in 0..NO_FEED_SOFT_CAP + 50 {
            let mut bytes = [0u8; 20];
            bytes[16..].copy_from_slice(&(i as u32).to_be_bytes());
            // Keep junk distinct from curated allow-list.
            bytes[0] = 0x9c;
            mark_no_feed(Address::from(bytes));
        }
        mark_no_feed(junk);
        assert!(STORE.lock().no_feed.len() <= NO_FEED_SOFT_CAP);
    }

    #[test]
    fn note_unmapped_only_queues_allowed_addresses() {
        PENDING.lock().clear();
        {
            let mut store = STORE.lock();
            store.no_feed.clear();
            store.feeds.clear();
        }
        // EURS is curated but not a builtin TOKEN_FEEDS major (live verify target).
        let curated = address!("0xE111178A87A3BFf0c8d18DECBa5798827539Ae99");
        let junk = address!("0x9cd6746665D9557e1B9a775819625711d0693439");
        assert!(is_auto_feed_address_allowed(curated));
        assert!(!is_auto_feed_address_allowed(junk));
        let oracle = PriceOracle::new(
            reqwest::Client::new(),
            "https://hermes.pyth.network".into(),
            10_000,
        );
        assert!(!oracle.has_configured_feed(&curated));
        note_unmapped_addresses(&oracle, [curated, junk, curated]);
        let pending = PENDING.lock().clone();
        assert!(pending.contains(&curated), "curated must queue");
        assert!(!pending.contains(&junk), "junk must never Hermes-queue");
        PENDING.lock().clear();
    }
}
