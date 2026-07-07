use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy::primitives::{Address, FixedBytes};
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PoolMetaData {
    #[serde(default)]
    balancer_pool_ids: FxHashMap<String, String>,
    #[serde(default)]
    woofi_meta: FxHashMap<String, WoofiMetaEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WoofiMetaEntry {
    quote: String,
    wooracle: String,
}

#[derive(Debug)]
pub struct PoolMetaCache {
    inner: RwLock<PoolMetaData>,
    path: PathBuf,
    write_seq: AtomicU64,
}

impl PoolMetaCache {
    pub fn new(path: PathBuf) -> Self {
        let data = std::fs::read(&path)
            .ok()
            .and_then(|raw| serde_json::from_slice(&raw).ok())
            .unwrap_or_default();
        Self {
            inner: RwLock::new(data),
            path,
            write_seq: AtomicU64::new(0),
        }
    }

    pub fn balancer_pool_id(&self, addr: &Address) -> Option<FixedBytes<32>> {
        let key = format!("{addr:#x}");
        self.inner
            .read()
            .balancer_pool_ids
            .get(&key)
            .and_then(|s| s.parse().ok())
    }

    pub fn set_balancer_pool_id(&self, addr: &Address, id: FixedBytes<32>) {
        let key = format!("{addr:#x}");
        self.inner.write().balancer_pool_ids.insert(key, format!("{id:#x}"));
        self.persist();
    }

    pub fn woofi_meta(&self, addr: &Address) -> Option<(Address, Address)> {
        let key = format!("{addr:#x}");
        self.inner.read().woofi_meta.get(&key).and_then(|entry| {
            let quote = entry.quote.parse().ok()?;
            let wooracle = entry.wooracle.parse().ok()?;
            Some((quote, wooracle))
        })
    }

    pub fn set_woofi_meta(&self, addr: &Address, quote: Address, wooracle: Address) {
        let key = format!("{addr:#x}");
        self.inner.write().woofi_meta.insert(
            key,
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
        let cloned = {
            let data = self.inner.read();
            (*data).clone()
        };
        let Ok(raw) = serde_json::to_vec(&cloned) else {
            return;
        };
        let path = self.path.clone();
        let seq = self.write_seq.fetch_add(1, Ordering::Relaxed);
        let tmp = path.with_extension(format!("json.{seq}.tmp"));
        tokio::task::spawn_blocking(move || {
            if std::fs::write(&tmp, &raw).is_ok() {
                let _ = std::fs::rename(&tmp, &path);
            }
        });
    }
}
