use alloy::primitives::{Address, FixedBytes, U256};

use crate::core::types::{PoolState, V3PoolState};
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::CalldataHop;
use crate::services::execution::profit::slippage_adjusted;
use crate::services::execution::quote::quote_hop_for_execution;
use crate::services::execution::support::ic;

pub fn compute_min_out(
    arena: &StateArena,
    hop: &CalldataHop,
    slippage_bps: u64,
    label: &str,
) -> anyhow::Result<U256> {
    let quoted = quote_hop_for_execution(arena, hop).unwrap_or(hop.amount_out);
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

// ponytail: lookup table if new protocol variants become frequent
#[must_use]
pub fn v3_callback_protocol_id(label: Option<&str>) -> u8 {
    let Some(l) = label else { return 1 };
    if ic(l, "sushiswap_v3") || ic(l, "sushi_v3") {
        2
    } else if ic(l, "quickswap_v4") || ic(l, "quick_v4") {
        4
    } else if ic(l, "quickswap_v3") || ic(l, "quick_v3") {
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
    } else if ic(l, "dfyn") {
        10
    } else if ic(l, "meshswap") || ic(l, "mesh_swap") {
        12
    } else if ic(l, "jetswap") || ic(l, "jet_swap") {
        13
    } else if ic(l, "cometh") {
        14
    } else {
        7
    }
}
