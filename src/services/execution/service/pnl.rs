use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::U256;

use super::ExecutionService;

#[derive(Debug, Clone, Copy)]
pub(super) struct PnlState {
    total: i128,
    daily: i128,
    pub(super) daily_utc_day: u32,
}

impl PnlState {
    pub(super) const fn new() -> Self {
        Self {
            total: 0,
            daily: 0,
            daily_utc_day: 0,
        }
    }

    fn maybe_roll_daily(&mut self) {
        let today = utc_day_number();
        if self.daily_utc_day != today {
            self.daily = 0;
            self.daily_utc_day = today;
        }
    }

    fn record_profit(&mut self, net: i128) {
        self.maybe_roll_daily();
        self.total = self.total.saturating_add(net);
        self.daily = self.daily.saturating_add(net);
    }

    fn record_loss(&mut self, loss: i128) {
        self.maybe_roll_daily();
        self.total = self.total.saturating_sub(loss);
        self.daily = self.daily.saturating_sub(loss);
    }

    fn snapshot(self) -> (i128, i128) {
        (self.total, self.daily)
    }
}

fn utc_day_number() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| (d.as_secs() / 86_400) as u32)
}

pub(super) fn parse_max_daily_loss_wei(raw: &str) -> Option<U256> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    match trimmed.parse::<U256>() {
        Ok(v) if !v.is_zero() => Some(v),
        Ok(_) => None,
        Err(_) => {
            // validate() rejects bad values in live startup; keep disabled if reached.
            None
        }
    }
}

pub(super) fn token_profit_to_matic_wei(
    profit_token_units: U256,
    token_to_matic_rate: U256,
    token_decimals: u8,
) -> Option<U256> {
    if token_to_matic_rate < crate::services::execution::profit::MIN_TOKEN_TO_MATIC_RATE {
        return None;
    }
    profit_token_units
        .checked_mul(token_to_matic_rate)?
        .checked_div(crate::util::ten_pow_u256(token_decimals))
}

impl ExecutionService {
    pub fn pnl_snapshot(&self) -> (i128, i128) {
        let mut pnl = self.pnl.lock();
        pnl.maybe_roll_daily();
        pnl.snapshot()
    }

    pub(super) fn record_realized(&self, profit_wei: U256, gas_cost_wei: U256) {
        let daily = if profit_wei >= gas_cost_wei {
            // Break-even is neutral economically but still a successful confirmation.
            self.consecutive_fails.store(0, Ordering::Relaxed);
            let p = profit_wei
                .saturating_sub(gas_cost_wei)
                .min(U256::from(i128::MAX as u128))
                .to::<u128>() as i128;
            let mut pnl = self.pnl.lock();
            if p > 0 {
                pnl.record_profit(p);
                self.total_trades.fetch_add(1, Ordering::Relaxed);
            } else {
                // Still roll the day under the same lock so the limit check matches booking.
                pnl.maybe_roll_daily();
            }
            pnl.daily
        } else {
            let loss = gas_cost_wei
                .saturating_sub(profit_wei)
                .min(U256::from(i128::MAX as u128))
                .to::<u128>() as i128;
            let mut pnl = self.pnl.lock();
            pnl.record_loss(loss);
            self.total_losses.fetch_add(1, Ordering::Relaxed);
            self.consecutive_fails.fetch_add(1, Ordering::Relaxed);
            pnl.daily
        };
        self.enforce_daily_loss_limit_with_daily(daily);
    }

    /// Book gas-only loss without treating attribution failure as a trade outcome.
    pub(super) fn record_gas_cost_loss(&self, gas_cost_wei: U256) {
        if gas_cost_wei.is_zero() {
            return;
        }
        self.record_realized(U256::ZERO, gas_cost_wei);
    }

    fn enforce_daily_loss_limit_with_daily(&self, daily: i128) {
        let Some(max_loss) = self.max_daily_loss_matic_wei else {
            return;
        };
        if daily >= 0 {
            return;
        }
        let abs_loss = U256::from(daily.unsigned_abs());
        if abs_loss < max_loss {
            return;
        }
        self.quarantine_global(Duration::from_secs(3600), Instant::now());
        crate::error!(
            "DAILY LOSS LIMIT BREACHED: {abs_loss} >= {max_loss} wei — execution quarantined 1h"
        );
    }
}
