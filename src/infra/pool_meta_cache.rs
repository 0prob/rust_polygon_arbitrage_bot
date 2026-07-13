use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy::primitives::{Address, FixedBytes};
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PoolMetaData {
    #[serde(default)]
    balancer_pool_ids: FxHashMap<Address, String>,
    #[serde(default)]
    woofi_meta: FxHashMap<Address, WoofiMetaEntry>,
}

/// Coalesce burst hydration writes (Woofi/Balancer sweeps) before hitting disk.
const PERSIST_DEBOUNCE: Duration = Duration::from_millis(150);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WoofiMetaEntry {
    quote: String,
    wooracle: String,
}

#[derive(Debug)]
pub struct PoolMetaCache {
    inner: std::sync::Arc<RwLock<PoolMetaData>>,
    path: std::sync::Arc<PathBuf>,
    write_seq: std::sync::Arc<AtomicU64>,
    persist_revision: std::sync::Arc<AtomicU64>,
    persist_running: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl PoolMetaCache {
    pub fn new(path: PathBuf) -> Self {
        let data = std::fs::read(&path)
            .ok()
            .and_then(|raw| serde_json::from_slice(&raw).ok())
            .unwrap_or_default();
        Self {
            inner: std::sync::Arc::new(RwLock::new(data)),
            path: std::sync::Arc::new(path),
            write_seq: std::sync::Arc::new(AtomicU64::new(0)),
            persist_revision: std::sync::Arc::new(AtomicU64::new(0)),
            persist_running: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn balancer_pool_id(&self, addr: &Address) -> Option<FixedBytes<32>> {
        self.inner
            .read()
            .balancer_pool_ids
            .get(addr)
            .and_then(|s| s.parse().ok())
    }

    pub fn set_balancer_pool_id(&self, addr: &Address, id: FixedBytes<32>) {
        self.inner
            .write()
            .balancer_pool_ids
            .insert(*addr, format!("{id:#x}"));
        self.persist();
    }

    pub fn woofi_meta(&self, addr: &Address) -> Option<(Address, Address)> {
        self.inner.read().woofi_meta.get(addr).and_then(|entry| {
            let quote = entry.quote.parse().ok()?;
            let wooracle = entry.wooracle.parse().ok()?;
            Some((quote, wooracle))
        })
    }

    pub fn set_woofi_meta(&self, addr: &Address, quote: Address, wooracle: Address) {
        self.inner.write().woofi_meta.insert(
            *addr,
            WoofiMetaEntry {
                quote: format!("{quote:#x}"),
                wooracle: format!("{wooracle:#x}"),
            },
        );
        self.persist();
    }

    /// Write cache to disk off the async runtime (avoid blocking tokio worker threads).
    /// Clones data under read lock then releases it before serializing to prevent
    /// stalling concurrent `set_*` callers that need the write lock.
    fn persist(&self) {
        self.persist_revision.fetch_add(1, Ordering::AcqRel);
        if self.persist_running.swap(true, Ordering::AcqRel) {
            return;
        }
        let inner = std::sync::Arc::clone(&self.inner);
        let path = std::sync::Arc::clone(&self.path);
        let revision = std::sync::Arc::clone(&self.persist_revision);
        let running = std::sync::Arc::clone(&self.persist_running);
        let write_seq = std::sync::Arc::clone(&self.write_seq);
        tokio::task::spawn_blocking(move || {
            loop {
                let target_revision = revision.load(Ordering::Acquire);
                std::thread::sleep(PERSIST_DEBOUNCE);
                if revision.load(Ordering::Acquire) != target_revision {
                    continue;
                }
                let seq = write_seq.fetch_add(1, Ordering::Relaxed);
                let tmp = path.with_extension(format!("json.{seq}.tmp"));
                let Ok(file) = std::fs::File::create(&tmp) else {
                    running.store(false, Ordering::Release);
                    return;
                };
                let mut writer = BufWriter::new(file);
                let serialized = {
                    let data = inner.read();
                    serde_json::to_writer(&mut writer, &*data)
                };
                if serialized.is_err() || writer.flush().is_err() {
                    let _ = std::fs::remove_file(&tmp);
                    running.store(false, Ordering::Release);
                    return;
                }
                if std::fs::rename(&tmp, &*path).is_err() {
                    let _ = std::fs::remove_file(&tmp);
                }
                if revision.load(Ordering::Acquire) == target_revision {
                    running.store(false, Ordering::Release);
                    if revision.load(Ordering::Acquire) == target_revision {
                        break;
                    }
                    if running.swap(true, Ordering::AcqRel) {
                        break;
                    }
                }
            }
        });
    }
}
