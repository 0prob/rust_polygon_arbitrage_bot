//! Live Balancer V2 vault flash-loan fee (ProtocolFeesCollector, 1e18 FixedPoint).

use alloy::network::Ethereum;
use alloy::providers::Provider;
use alloy::sol;
use anyhow::Context;

use crate::core::constants::BALANCER_VAULT;
use crate::infra::rpc::RpcPool;
use crate::services::execution::profit::{
    balancer_flash_loan_fee_pct_cached, set_balancer_flash_loan_fee_pct,
};

sol! {
    #[sol(rpc)]
    interface IBalancerVaultFees {
        function getProtocolFeesCollector() external view returns (address);
    }

    #[sol(rpc)]
    interface IProtocolFeesCollector {
        function getFlashLoanFeePercentage() external view returns (uint256);
    }
}

/// Fetch vault flash-loan fee percentage (1e18 FixedPoint) and cache it.
/// Zero is valid on Polygon today; unlike Aave we do not reject it.
pub async fn fetch_and_cache_balancer_flash_loan_fee_pct<P: Provider<Ethereum>>(
    provider: &P,
) -> anyhow::Result<u64> {
    let vault = IBalancerVaultFees::new(BALANCER_VAULT, provider);
    let collector = vault
        .getProtocolFeesCollector()
        .call()
        .await
        .context("Balancer getProtocolFeesCollector")?;
    anyhow::ensure!(
        !collector.is_zero(),
        "Balancer ProtocolFeesCollector is zero address"
    );
    let fees = IProtocolFeesCollector::new(collector, provider);
    let pct = fees
        .getFlashLoanFeePercentage()
        .call()
        .await
        .context("Balancer getFlashLoanFeePercentage")?;
    let pct_u64 = u64::try_from(pct)
        .with_context(|| format!("Balancer flash loan fee pct {pct} does not fit u64"))?;
    let prev = balancer_flash_loan_fee_pct_cached();
    set_balancer_flash_loan_fee_pct(pct_u64);
    if pct_u64 != prev {
        crate::info!("balancer: flash_fee_pct_1e18={pct_u64} (was {prev})");
    }
    Ok(pct_u64)
}

pub async fn refresh_balancer_flash_fee_with_fallback(rpc: &RpcPool) -> anyhow::Result<u64> {
    let candidates = rpc.state_url_candidates();
    anyhow::ensure!(!candidates.is_empty(), "no state RPC configured");
    let mut last_err: Option<anyhow::Error> = None;
    for url in candidates {
        let provider = match rpc.connect_state_at(&url) {
            Ok(p) => p,
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        };
        match fetch_and_cache_balancer_flash_loan_fee_pct(&provider).await {
            Ok(pct) => return Ok(pct),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        anyhow::anyhow!("balancer flash fee refresh failed on all RPCs")
    }))
}
