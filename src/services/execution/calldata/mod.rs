pub mod approvals;
pub mod encoders;
pub mod hash;
pub mod types;

pub use encoders::shared::{
    curve_uses_receiver, derive_balancer_pool_id, resolve_balancer_pool_id, to_v3_state,
};
pub use hash::compute_route_hash;
pub use types::{BuiltArbTx, CalldataHop, RouteEncodeConfig};

use alloy::primitives::{Address, U256};
use alloy::sol_types::SolCall;

use crate::abis::ExecutorCall;
use crate::core::types::Edge;
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::PoolMeta;

use encoders::encoder_for_protocol;

/// Encode route into executor calls using the ProtocolEncoder trait.
pub fn encode_route(
    arena: &StateArena,
    hops: &[CalldataHop],
    executor: Address,
    config: RouteEncodeConfig,
) -> anyhow::Result<Vec<ExecutorCall>> {
    let mut calls = Vec::new();
    for (i, hop) in hops.iter().enumerate() {
        let encoder = encoder_for_protocol(hop.edge.protocol, hop.protocol_label.as_deref());
        calls.extend(encoder.encode_hop(hop, executor, arena, &config, i == 0)?);
    }
    Ok(calls)
}

/// Build calldata hops from route edges, hop amounts, and pool metadata
pub fn build_calldata_hops(
    arena: &StateArena,
    edges: &[Edge],
    hop_amounts: &[U256],
    pool_metas: &[PoolMeta],
) -> Option<Vec<CalldataHop>> {
    if hop_amounts.len() != edges.len() + 1 {
        return None;
    }
    let mut hops = Vec::with_capacity(edges.len());
    for (i, edge) in edges.iter().enumerate() {
        let pool_address = arena.pool_address(edge.pool_index)?;
        let token_in = arena.token_address(edge.token_in)?;
        let token_out = arena.token_address(edge.token_out)?;
        let meta = pool_metas.iter().find(|m| m.pool_index == edge.pool_index);
        hops.push(CalldataHop {
            edge: *edge,
            pool_address,
            token_in,
            token_out,
            amount_in: hop_amounts[i],
            amount_out: hop_amounts[i + 1],
            pool_id: meta.and_then(|m| m.pool_id),
            protocol_label: meta.and_then(|m| m.protocol_label.clone()),
            router: meta.and_then(|m| m.router),
            hooks: meta.and_then(|m| m.hooks),
        });
    }
    Some(hops)
}

/// Build arbitrage transaction from calldata hops
pub fn build_arb_calldata(
    executor: Address,
    flash_token: Address,
    profit_token: Address,
    flash_amount: U256,
    min_profit: U256,
    deadline: U256,
    calls: Vec<ExecutorCall>,
    use_aave: bool,
) -> BuiltArbTx {
    let route_hash = compute_route_hash(&calls);
    let params = crate::abis::FlashParams {
        profitToken: profit_token,
        minProfit: min_profit,
        deadline,
        routeHash: route_hash,
        calls: calls.clone(),
    };

    let data = if use_aave {
        crate::abis::IArbExecutor::executeArbWithAaveCall {
            flashToken: flash_token,
            flashAmount: flash_amount,
            params,
        }
        .abi_encode()
    } else {
        crate::abis::IArbExecutor::executeArbCall {
            flashToken: flash_token,
            flashAmount: flash_amount,
            params,
        }
        .abi_encode()
    };

    BuiltArbTx {
        to: executor,
        data: data.into(),
        value: U256::ZERO,
        route_hash,
        calls,
    }
}
