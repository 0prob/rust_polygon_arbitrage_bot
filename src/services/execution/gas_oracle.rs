use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use alloy::eips::BlockNumberOrTag;
use alloy::network::Ethereum;
use alloy::providers::{Provider, ProviderBuilder};
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use ruint::aliases::U256;
use rustc_hash::FxHashMap;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tracing::{debug, warn};

use super::gas::{
    DEFAULT_CONSERVATIVE_GAS_PRICE_WEI, FeeSnapshot, compute_conservative_gas_price,
    conservative_gas_price_wei, default_priority_fee_wei,
};

const ROUTE_GAS_HISTORY: usize = 256;

#[derive(Debug)]
pub struct GasOracle {
    snapshot: ArcSwap<Option<FeeSnapshot>>,
    poll_interval: Duration,
    route_gas: Mutex<FxHashMap<u64, u32>>,
    /// Latest observed/simulated ratio in bps (10_000 = 1.0×) for heuristic uplift.
    sim_scale_bps: AtomicU32,
}

impl Default for GasOracle {
    fn default() -> Self {
        Self::new(Duration::from_secs(1))
    }
}

impl GasOracle {
    pub fn new(poll_interval: Duration) -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(None),
            poll_interval,
            route_gas: Mutex::new(FxHashMap::default()),
            sim_scale_bps: AtomicU32::new(10_000),
        }
    }

    /// Prefer dry-run / on-chain gas for this route fingerprint, else scaled heuristic.
    pub fn route_gas_or_heuristic(&self, route_fp: u64, heuristic: u32) -> u32 {
        if let Some(&observed) = self.route_gas.lock().get(&route_fp) {
            return observed;
        }
        let scale = self.sim_scale_bps.load(Ordering::Relaxed).max(10_000);
        let scaled = (u64::from(heuristic) * u64::from(scale) / 10_000) as u32;
        scaled.max(heuristic)
    }

    /// Record on-chain or dry-run gas for a route fingerprint.
    pub fn record_route_gas(&self, route_fp: u64, gas: u32) {
        if gas == 0 {
            return;
        }
        let mut map = self.route_gas.lock();
        if map.len() >= ROUTE_GAS_HISTORY {
            if let Some(key) = map.keys().next().copied() {
                map.remove(&key);
            }
        }
        map.insert(route_fp, gas);
    }

    /// Calibrate heuristic gas from estimate_gas / dry-run observations.
    pub fn record_sim_observed(&self, simulated: u32, observed: u64) {
        if simulated == 0 || observed == 0 {
            return;
        }
        let ratio_bps = ((observed * 10_000) / u64::from(simulated)).min(u64::from(u32::MAX)) as u32;
        let prev = self.sim_scale_bps.load(Ordering::Relaxed).max(10_000);
        let blended = ((u64::from(prev) * 3 + u64::from(ratio_bps)) / 4) as u32;
        self.sim_scale_bps.store(blended.max(10_000), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Option<FeeSnapshot> {
        *self.snapshot.load_full().as_ref()
    }

    pub fn conservative_gas_price(&self) -> U256 {
        self.snapshot()
            .map(compute_conservative_gas_price)
            .unwrap_or_else(conservative_gas_price_wei)
    }

    pub async fn refresh_once<P: Provider<Ethereum>>(&self, provider: &P) -> anyhow::Result<()> {
        let block = provider
            .get_block_by_number(BlockNumberOrTag::Latest)
            .await?
            .ok_or_else(|| anyhow::anyhow!("latest block unavailable"))?;

        let base_fee = block
            .header
            .base_fee_per_gas
            .map(U256::from)
            .unwrap_or(U256::from(DEFAULT_CONSERVATIVE_GAS_PRICE_WEI));

        let priority_fee = match provider.get_max_priority_fee_per_gas().await {
            Ok(v) => U256::from(v),
            Err(_) => default_priority_fee_wei(),
        };

        self.snapshot.store(Arc::new(Some(FeeSnapshot {
            base_fee,
            priority_fee,
        })));
        Ok(())
    }

    pub async fn start_background(
        self: Arc<Self>,
        rpc_url: &str,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
        if let Err(e) = self.refresh_once(&provider).await {
            warn!(error = %e, "initial gas oracle refresh failed");
        }
        let poll = self.poll_interval;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(poll);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        if let Err(e) = self.refresh_once(&provider).await {
                            debug!(error = %e, "gas oracle refresh failed");
                        }
                    }
                }
            }
        });
        Ok(())
    }
}
