use anyhow::{Result, anyhow};
use ruint::aliases::U256;

#[derive(Debug, Clone, Copy)]
pub struct FeeSnapshot {
    pub base_fee: U256,
    pub priority_fee: U256,
}

pub const ROUTE_EXECUTION_GAS_OVERHEAD: u32 = 150_000;
pub const PER_HOP_EXECUTOR_GAS_OVERHEAD: u32 = 25_000;
/// 15% headroom over simulated / dry-run gas for state drift between estimate and mine.
pub const GAS_LIMIT_BUFFER_BPS: u64 = 1500;
pub const DEFAULT_CONSERVATIVE_GAS_PRICE_WEI: u128 = 30_000_000_000;
pub const DEFAULT_PRIORITY_FEE_WEI: u128 = 2_000_000_000;

pub fn u256_to_u128(v: U256) -> Result<u128> {
    v.try_into().map_err(|_| anyhow!("value {v} exceeds u128"))
}

pub fn buffer_gas_limit(simulated_gas: u32) -> Option<U256> {
    if simulated_gas == 0 {
        return None;
    }
    let units = U256::from(simulated_gas);
    Some((units * U256::from(GAS_LIMIT_BUFFER_BPS)) / U256::from(1000u64) + U256::from(1u8))
}

pub fn pick_buffered_gas_limit(simulated_gas: u32, dry_run_gas: Option<u64>) -> Option<U256> {
    let base = simulated_gas.max(dry_run_gas.unwrap_or(0) as u32);
    buffer_gas_limit(base)
}

/// Fail-closed gas limit for live submission — no silent fallback.
pub fn pick_live_gas_limit(simulated_gas: u32, dry_run_gas: u64) -> Result<u64> {
    let limit = pick_buffered_gas_limit(simulated_gas, Some(dry_run_gas))
        .ok_or_else(|| anyhow!("dry-run passed but gas estimate is zero"))?;
    u256_to_u128(limit).map(|g| g as u64)
}

pub fn estimate_route_gas_from_hops(hop_gas: u32, hop_count: usize) -> u32 {
    hop_gas + ROUTE_EXECUTION_GAS_OVERHEAD + hop_count as u32 * PER_HOP_EXECUTOR_GAS_OVERHEAD
}

pub fn compute_conservative_gas_price(snapshot: FeeSnapshot) -> U256 {
    snapshot.base_fee * U256::from(2u8) + snapshot.priority_fee
}

pub fn conservative_gas_price_wei() -> U256 {
    U256::from(DEFAULT_CONSERVATIVE_GAS_PRICE_WEI)
}

pub fn default_priority_fee_wei() -> U256 {
    U256::from(DEFAULT_PRIORITY_FEE_WEI)
}

/// Log gas drift between dry-run estimate and on-chain usage (basis points).
pub fn gas_drift_bps(estimated: u64, actual: u64) -> u64 {
    if estimated == 0 {
        return 0;
    }
    actual.saturating_mul(10_000).saturating_div(estimated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_live_gas_uses_max_of_sim_and_dry_run() {
        let limit = pick_live_gas_limit(100_000, 120_000).unwrap();
        assert!(limit >= 120_000);
    }

    #[test]
    fn pick_live_gas_fails_on_zero() {
        assert!(pick_live_gas_limit(0, 0).is_err());
    }
}
