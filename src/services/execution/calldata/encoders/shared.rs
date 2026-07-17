use alloy::primitives::{Address, FixedBytes, U256};

use crate::core::types::{PoolState, V3PoolState};
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::CalldataHop;
use crate::services::execution::profit::slippage_adjusted;
use crate::services::execution::quote::quote_hop_for_execution;
use crate::services::execution::support::ic;

pub fn compute_quoted_out(
    arena: &StateArena,
    hop: &CalldataHop,
    label: &str,
) -> anyhow::Result<U256> {
    quote_hop_for_execution(arena, hop)
        .filter(|q| !q.is_zero())
        .ok_or_else(|| anyhow::anyhow!("{label} hop execution quote unavailable"))
}

pub fn compute_min_out(
    arena: &StateArena,
    hop: &CalldataHop,
    slippage_bps: u64,
    label: &str,
) -> anyhow::Result<U256> {
    let quoted = compute_quoted_out(arena, hop, label)?;
    slippage_adjusted(quoted, slippage_bps)
        .ok_or_else(|| anyhow::anyhow!("{label} hop min out is zero"))
}

#[must_use]
pub fn to_v3_state(state: &PoolState) -> Option<V3PoolState> {
    match state {
        PoolState::V3(s) | PoolState::V4(s) => Some(s.clone()),
        _ => None,
    }
}

pub fn resolve_balancer_pool_id(
    pool_address: Address,
    pool_id: Option<FixedBytes<32>>,
) -> anyhow::Result<FixedBytes<32>> {
    pool_id.ok_or_else(|| anyhow::anyhow!("missing Balancer pool_id for {pool_address}"))
}

#[must_use]
pub fn curve_uses_receiver(pool_type: Option<&str>) -> bool {
    pool_type.is_some_and(|t| ic(t, "stable_ng"))
}

// ponytail: lookup table if new protocol variants become frequent.
// Must stay aligned with sol/src ArbExecutor protocol IDs:
//   1 Uni V3, 2 Sushi V3, 3 Algebra/Quick V3, 4 Algebra Integral/Quick V4, 6 Ramses.
#[must_use]
pub fn v3_callback_protocol_id(label: Option<&str>) -> u8 {
    let Some(l) = label else { return 1 };
    if ic(l, "sushiswap_v3") || ic(l, "sushi_v3") {
        2
    } else if ic(l, "ramses") {
        6
    } else if crate::core::protocol::is_algebra_integral_protocol_label(l) {
        // QuickSwap V4 / Algebra Integral → algebraSwapCallback id 4
        4
    } else if crate::core::protocol::is_algebra_protocol_label(l) {
        // Any Algebra (incl. bare "algebra", quickswap_v3) → id 3.
        // Prior path only matched "quickswap_v3"/"quick_v3", so labels like
        // "ALGEBRA" / "ALGEBRA_V3" silently used Uni V3 callback id 1 and would
        // fail factory verification / algebraSwapCallback dispatch.
        3
    } else {
        1
    }
}

// ponytail: lookup table if new protocol variants become frequent
#[must_use]
pub fn v2_callback_protocol_id(label: Option<&str>) -> u8 {
    let Some(l) = label else { return 7 };
    if ic(l, "sushiswap_v2") || ic(l, "sushi_v2") {
        8
    } else if ic(l, "quickswap_v2") || ic(l, "quick_v2") {
        9
    } else {
        7
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn algebra_labels_use_algebra_callback_ids() {
        assert_eq!(v3_callback_protocol_id(Some("ALGEBRA")), 3);
        assert_eq!(v3_callback_protocol_id(Some("ALGEBRA_V3")), 3);
        assert_eq!(v3_callback_protocol_id(Some("QUICKSWAP_V3")), 3);
        assert_eq!(v3_callback_protocol_id(Some("quick_v3")), 3);
        assert_eq!(v3_callback_protocol_id(Some("QUICKSWAP_V4")), 4);
        assert_eq!(v3_callback_protocol_id(Some("quick_v4")), 4);
    }

    #[test]
    fn uni_sushi_ramses_v3_callback_ids() {
        assert_eq!(v3_callback_protocol_id(None), 1);
        assert_eq!(v3_callback_protocol_id(Some("UNISWAP_V3")), 1);
        assert_eq!(v3_callback_protocol_id(Some("SUSHISWAP_V3")), 2);
        assert_eq!(v3_callback_protocol_id(Some("RAMSES_V3")), 6);
    }
}
