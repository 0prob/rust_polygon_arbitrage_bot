use std::sync::Arc;
use std::time::Duration;

use alloy::eips::BlockNumberOrTag;
use alloy::network::Ethereum;
use alloy::providers::{Provider, ProviderBuilder};
use arc_swap::ArcSwap;
use ruint::aliases::U256;
use tokio::sync::watch;
use tracing::{debug, warn};

use super::gas::{
    DEFAULT_CONSERVATIVE_GAS_PRICE_WEI, FeeSnapshot, compute_conservative_gas_price,
    conservative_gas_price_wei, default_priority_fee_wei,
};

#[derive(Debug)]
pub struct GasOracle {
    snapshot: ArcSwap<Option<FeeSnapshot>>,
    poll_interval: Duration,
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
        }
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
