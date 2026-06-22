use crate::core::types::FlashLoanSource;

/// Flash-loan routing policy at dispatch time (`FLASH_LOAN_SOURCE=auto` enables the waterfall).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlashLoanPolicy {
    /// Balancer if sufficient, else Aave, else cap + re-optimize.
    Auto,
    BalancerOnly,
    AaveOnly,
}

pub fn parse_flash_policy(raw: &str) -> FlashLoanPolicy {
    let upper = raw.to_ascii_uppercase();
    if upper.contains("AUTO") {
        FlashLoanPolicy::Auto
    } else if upper.contains("AAVE") {
        FlashLoanPolicy::AaveOnly
    } else {
        FlashLoanPolicy::BalancerOnly
    }
}

pub fn parse_flash_source(raw: &str) -> FlashLoanSource {
    if raw.to_ascii_uppercase().contains("AAVE") {
        FlashLoanSource::AaveV3
    } else {
        FlashLoanSource::Balancer
    }
}

/// Pessimistic fee model for HF ranking — matches worst-case dispatch in auto mode.
pub fn hf_eval_flash_source(policy: FlashLoanPolicy) -> FlashLoanSource {
    match policy {
        FlashLoanPolicy::AaveOnly | FlashLoanPolicy::Auto => FlashLoanSource::AaveV3,
        FlashLoanPolicy::BalancerOnly => FlashLoanSource::Balancer,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_flash_policy_variants() {
        assert_eq!(parse_flash_policy("auto"), FlashLoanPolicy::Auto);
        assert_eq!(parse_flash_policy("AAVE"), FlashLoanPolicy::AaveOnly);
        assert_eq!(parse_flash_policy("BALANCER"), FlashLoanPolicy::BalancerOnly);
    }

    #[test]
    fn hf_eval_flash_source_pessimistic_in_auto() {
        assert_eq!(
            hf_eval_flash_source(FlashLoanPolicy::Auto),
            FlashLoanSource::AaveV3
        );
        assert_eq!(
            hf_eval_flash_source(FlashLoanPolicy::BalancerOnly),
            FlashLoanSource::Balancer
        );
    }
}
