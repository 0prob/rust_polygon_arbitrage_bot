use std::sync::atomic::{AtomicU32, Ordering};

use alloy::network::Ethereum;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;

use anyhow::Context;

use crate::abis::IAaveV3Pool;
use crate::core::constants::AAVE_V3_POOL;
use crate::infra::rpc::RpcPool;
use crate::services::execution::profit::{
    aave_flash_loan_fee_bps_cached, set_aave_flash_loan_fee_bps,
};

pub async fn fetch_and_cache_aave_flash_loan_fee_bps<P: Provider<Ethereum>>(
    provider: &P,
) -> anyhow::Result<u64> {
    let pool = IAaveV3Pool::new(AAVE_V3_POOL, provider);
    let fee = pool.FLASHLOAN_PREMIUM_TOTAL().call().await?;
    let bps = u64::try_from(fee)
        .with_context(|| format!("Aave flash loan fee {fee} does not fit u64"))?;
    if bps == 0 {
        anyhow::bail!("Aave FLASHLOAN_PREMIUM_TOTAL returned zero — on-chain data unreliable");
    }
    let prev = aave_flash_loan_fee_bps_cached();
    set_aave_flash_loan_fee_bps(bps);
    if bps != prev {
        crate::info!("aave: flash_fee_bps={bps} (was {prev})");
    }
    Ok(bps)
}

/// Aave V3 `ReserveConfigurationMap` bit positions (Pool.sol / ReserveConfiguration.sol).
const AAVE_CFG_ACTIVE_BIT: u32 = 56;
#[allow(dead_code)] // documented bit; flash eligibility ignores frozen (validateFlashloanSimple)
const AAVE_CFG_FROZEN_BIT: u32 = 57;
const AAVE_CFG_PAUSED_BIT: u32 = 60;
const AAVE_CFG_FLASH_BIT: u32 = 63;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AaveReserveStatus {
    Viable,
    RpcError,
    NoAToken,
    Inactive,
    Paused,
    FlashDisabled,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AaveRefreshStats {
    pub listed_viable: u32,
    pub rpc_error: u32,
    pub no_atoken: u32,
    pub inactive: u32,
    pub paused: u32,
    pub flash_disabled: u32,
    pub pinned_inactive: u32,
}

impl AaveRefreshStats {
    pub fn record(&mut self, status: AaveReserveStatus, pinned: bool) {
        if pinned {
            self.pinned_inactive += 1;
            return;
        }
        match status {
            AaveReserveStatus::Viable => self.listed_viable += 1,
            AaveReserveStatus::RpcError => self.rpc_error += 1,
            AaveReserveStatus::NoAToken => self.no_atoken += 1,
            AaveReserveStatus::Inactive => self.inactive += 1,
            AaveReserveStatus::Paused => self.paused += 1,
            AaveReserveStatus::FlashDisabled => self.flash_disabled += 1,
        }
    }

    pub fn log_refresh_summary(&self, tokens_fetched: usize, generation: u64) {
        if tokens_fetched == 0 {
            return;
        }
        let ineligible = self.no_atoken
            + self.inactive
            + self.paused
            + self.flash_disabled
            + self.pinned_inactive
            + self.rpc_error;
        if self.listed_viable == 0 && ineligible == 0 {
            return;
        }
        // Routine refresh: debug. Elevate to warn when RPC/config collapses viability.
        let fee_bps = aave_flash_loan_fee_bps_cached();
        if self.rpc_error > 0 || (self.listed_viable == 0 && tokens_fetched > 0) {
            crate::warn!(
                "aave refresh: tokens={tokens_fetched} gen={generation} viable={} ineligible={} \
                 (rpc_err={} no_atoken={} inactive={} paused={} flash_off={} pinned={}) fee_bps={fee_bps} \
                 — check Aave aToken mapping / RPC",
                self.listed_viable,
                ineligible,
                self.rpc_error,
                self.no_atoken,
                self.inactive,
                self.paused,
                self.flash_disabled,
                self.pinned_inactive,
            );
        } else {
            crate::debug!(
                "aave refresh: tokens={tokens_fetched} gen={generation} viable={} ineligible={} \
                 (rpc_err={} no_atoken={} inactive={} paused={} flash_off={} pinned={}) fee_bps={fee_bps}",
                self.listed_viable,
                ineligible,
                self.rpc_error,
                self.no_atoken,
                self.inactive,
                self.paused,
                self.flash_disabled,
                self.pinned_inactive,
            );
        }
    }
}

static AAVE_PREPARE_SKIP_INACTIVE: AtomicU32 = AtomicU32::new(0);
static AAVE_MARK_INACTIVE: AtomicU32 = AtomicU32::new(0);

pub fn record_aave_prepare_skip_inactive() {
    AAVE_PREPARE_SKIP_INACTIVE.fetch_add(1, Ordering::Relaxed);
}

pub fn record_aave_mark_inactive() {
    AAVE_MARK_INACTIVE.fetch_add(1, Ordering::Relaxed);
}

pub fn log_aave_gate_summary(candidates: u32) {
    if candidates == 0 {
        return;
    }
    let prepare = AAVE_PREPARE_SKIP_INACTIVE.load(Ordering::Relaxed);
    let marked = AAVE_MARK_INACTIVE.load(Ordering::Relaxed);
    if prepare == 0 && marked == 0 {
        return;
    }
    crate::debug!(
        "aave dispatch_gate: candidates={candidates} prepare_skip_inactive={prepare} mark_inactive={marked} fee_bps={}",
        aave_flash_loan_fee_bps_cached(),
    );
}

#[inline]
fn aave_cfg_bit_set(configuration: U256, bit: u32) -> bool {
    (configuration >> bit) & U256::from(1) != U256::ZERO
}

/// Active, unfrozen, unpaused, flash-loan-enabled.
#[inline]
#[must_use]
pub fn aave_reserve_flash_eligible(configuration: U256) -> bool {
    matches!(
        reserve_status_from_config(configuration, true),
        AaveReserveStatus::Viable
    )
}

#[must_use]
pub fn reserve_status_from_config(configuration: U256, has_a_token: bool) -> AaveReserveStatus {
    if !has_a_token {
        return AaveReserveStatus::NoAToken;
    }
    if !aave_cfg_bit_set(configuration, AAVE_CFG_ACTIVE_BIT) {
        return AaveReserveStatus::Inactive;
    }
    // ponytail: validateFlashloanSimple checks paused/active/flashLoanEnabled only — not frozen.
    if aave_cfg_bit_set(configuration, AAVE_CFG_PAUSED_BIT) {
        return AaveReserveStatus::Paused;
    }
    if !aave_cfg_bit_set(configuration, AAVE_CFG_FLASH_BIT) {
        return AaveReserveStatus::FlashDisabled;
    }
    AaveReserveStatus::Viable
}

/// On-chain reserve check when cache is stale or not viable — mirrors `validateFlashloanSimple`.
pub async fn aave_flash_reserve_status_live<P: Provider<Ethereum>>(
    provider: &P,
    aave_pool: Address,
    token: Address,
) -> AaveReserveStatus {
    let pool = IAaveV3Pool::new(aave_pool, provider);
    let Ok(reserve) = pool.getReserveData(token).call().await else {
        return AaveReserveStatus::RpcError;
    };
    let has_a_token = !reserve.aTokenAddress.is_zero();
    reserve_status_from_config(reserve.configuration, has_a_token)
}

/// Periodic `FLASHLOAN_PREMIUM_TOTAL` refresh (global, cheap single call).
pub async fn refresh_aave_flash_fee_with_fallback(rpc: &RpcPool) -> anyhow::Result<u64> {
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
        match fetch_and_cache_aave_flash_loan_fee_bps(&provider).await {
            Ok(bps) => return Ok(bps),
            Err(e) => {
                last_err = Some(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("aave flash fee refresh failed on all RPCs")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_status_requires_active_and_flash_bits() {
        let active_flash =
            U256::from(1u128 << AAVE_CFG_ACTIVE_BIT) | U256::from(1u128 << AAVE_CFG_FLASH_BIT);
        assert_eq!(
            reserve_status_from_config(active_flash, true),
            AaveReserveStatus::Viable
        );
        assert_eq!(
            reserve_status_from_config(U256::ZERO, true),
            AaveReserveStatus::Inactive
        );
        assert_eq!(
            reserve_status_from_config(active_flash, false),
            AaveReserveStatus::NoAToken
        );
        // Frozen is not a flash-loan gate in aave-v3-core ValidationLogic.
        let frozen = active_flash | U256::from(1u128 << AAVE_CFG_FROZEN_BIT);
        assert_eq!(
            reserve_status_from_config(frozen, true),
            AaveReserveStatus::Viable
        );
        let paused = active_flash | U256::from(1u128 << AAVE_CFG_PAUSED_BIT);
        assert_eq!(
            reserve_status_from_config(paused, true),
            AaveReserveStatus::Paused
        );
    }

    #[test]
    fn eligible_matches_viable_status() {
        let active_flash =
            U256::from(1u128 << AAVE_CFG_ACTIVE_BIT) | U256::from(1u128 << AAVE_CFG_FLASH_BIT);
        assert!(aave_reserve_flash_eligible(active_flash));
        assert!(!aave_reserve_flash_eligible(U256::ZERO));
    }
}
