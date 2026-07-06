pub mod approvals;
pub mod encoders;
pub mod hash;

pub use encoders::shared::{curve_uses_receiver, resolve_balancer_pool_id, to_v3_state};
pub use hash::{compute_route_hash, pack_executor_calls};

use std::fmt::Write;

use alloy::primitives::{Address, Bytes, FixedBytes, U256};
use alloy::sol_types::SolCall;
use rustc_hash::FxHashMap;

use crate::abis::ExecutorCall;
use crate::core::types::{Edge, FlashLoanSource, PoolIndex, PoolState, ProtocolType};
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::PoolMeta;

use encoders::encode_hop_for_protocol;

#[derive(Debug, Clone)]
pub struct CalldataHop {
    pub edge: Edge,
    pub pool_address: Address,
    pub token_in: Address,
    pub token_out: Address,
    pub amount_in: U256,
    pub amount_out: U256,
    pub pool_id: Option<FixedBytes<32>>,
    pub protocol_label: Option<String>,
    pub pool_type: Option<String>,
    pub router: Option<Address>,
    pub hooks: Option<Address>,
}

#[derive(Debug, Clone, Copy)]
pub struct RouteEncodeConfig {
    pub slippage_bps: u64,
    pub deadline: U256,
}

#[derive(Clone)]
pub struct BuiltArbTx {
    pub to: Address,
    pub data: Bytes,
    pub value: U256,
    pub route_hash: FixedBytes<32>,
    pub calls: Vec<ExecutorCall>,
}

pub fn build_packed_route_payload(
    flash_token: Address,
    flash_amount: U256,
    profit_token: Address,
    min_profit: U256,
    deadline: U256,
    calls: &[ExecutorCall],
) -> anyhow::Result<(Bytes, FixedBytes<32>)> {
    if flash_token == Address::ZERO || profit_token == Address::ZERO {
        anyhow::bail!("flash and profit token addresses must not be zero");
    }
    let packed_calls = pack_executor_calls(calls)?;
    let route_hash = compute_route_hash(&packed_calls);
    let mut payload = Vec::with_capacity(0xe0 + packed_calls.len());
    payload.extend_from_slice(&[0u8; 12]);
    payload.extend_from_slice(flash_token.as_slice());
    payload.extend_from_slice(&flash_amount.to_be_bytes::<32>());
    payload.extend_from_slice(&[0u8; 12]);
    payload.extend_from_slice(profit_token.as_slice());
    payload.extend_from_slice(&min_profit.to_be_bytes::<32>());
    payload.extend_from_slice(&deadline.to_be_bytes::<32>());
    payload.extend_from_slice(&route_hash.0);
    payload.extend_from_slice(&packed_calls);
    Ok((payload.into(), route_hash))
}

/// Encode route into executor calls via protocol-specific encoder functions.
pub fn encode_route(
    arena: &StateArena,
    hops: &[CalldataHop],
    executor: Address,
    config: RouteEncodeConfig,
    flash_source: FlashLoanSource,
) -> anyhow::Result<Vec<ExecutorCall>> {
    if executor == Address::ZERO {
        anyhow::bail!("executor address must not be zero");
    }
    if flash_source == FlashLoanSource::Direct
        && hops_are_balancer_only(hops)
        && hops.len() <= crate::pipeline::route_calls::MAX_BALANCER_BATCH_HOPS
    {
        return encoders::balancer::encode_balancer_batch_route(hops, executor, config.deadline);
    }
    let mut calls = Vec::with_capacity(hops.len().saturating_mul(2));
    for (i, hop) in hops.iter().enumerate() {
        calls.extend(encode_hop_for_protocol(
            hop,
            executor,
            arena,
            &config,
            i == 0,
        )?);
    }
    Ok(calls)
}

#[must_use]
pub fn hops_are_balancer_only(hops: &[CalldataHop]) -> bool {
    !hops.is_empty()
        && hops
            .iter()
            .all(|h| h.edge.protocol == ProtocolType::BalancerV2)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorEntrypoint {
    BalancerFlash,
    AaveFlash,
    Direct,
}

fn balancer_pool_id_from_arena(
    arena: &StateArena,
    pool_index: PoolIndex,
) -> Option<alloy::primitives::FixedBytes<32>> {
    match arena.pool_state(pool_index)? {
        PoolState::Balancer(b) => b.pool_id,
        _ => None,
    }
}

/// Build calldata hops from route edges, hop amounts, and pool metadata
#[must_use]
pub fn build_calldata_hops(
    arena: &StateArena,
    edges: &[Edge],
    hop_amounts: &[U256],
    pool_metas_by_pool: &FxHashMap<PoolIndex, &PoolMeta>,
) -> Option<Vec<CalldataHop>> {
    if hop_amounts.len() != edges.len() + 1 {
        return None;
    }
    let mut hops = Vec::with_capacity(edges.len());
    for (i, edge) in edges.iter().enumerate() {
        let pool_address = arena.pool_address(edge.pool_index)?;
        if !crate::services::discovery::is_plausible_contract_address(pool_address) {
            return None;
        }
        let token_in = arena.token_address(edge.token_in)?;
        let token_out = arena.token_address(edge.token_out)?;
        if !crate::services::discovery::is_plausible_contract_address(token_in)
            || !crate::services::discovery::is_plausible_contract_address(token_out)
        {
            return None;
        }
        let meta = pool_metas_by_pool.get(&edge.pool_index).copied();
        if meta.is_some_and(|pool| pool.protocol != edge.protocol) {
            return None;
        }
        if meta
            .and_then(|pool| pool.protocol_label.as_deref())
            .is_some_and(|label| !crate::core::protocol::is_known_protocol_label(label))
        {
            return None;
        }
        let meta_pool_id = meta.and_then(|m| m.pool_id);
        let arena_pool_id = balancer_pool_id_from_arena(arena, edge.pool_index);
        let pool_id = if edge.protocol == ProtocolType::BalancerV2 {
            arena_pool_id.or(meta_pool_id)
        } else {
            meta_pool_id
        };
        hops.push(CalldataHop {
            edge: *edge,
            pool_address,
            token_in,
            token_out,
            amount_in: hop_amounts[i],
            amount_out: hop_amounts[i + 1],
            pool_id,
            protocol_label: meta.and_then(|m| m.protocol_label.clone()),
            pool_type: meta.and_then(|m| m.pool_type.clone()),
            router: None,
            hooks: meta.and_then(|m| m.hooks),
        });
    }
    Some(hops)
}

/// Build arbitrage transaction from calldata hops
#[allow(clippy::too_many_arguments)]
pub fn build_arb_calldata(
    executor: Address,
    flash_token: Address,
    profit_token: Address,
    flash_amount: U256,
    min_profit: U256,
    deadline: U256,
    calls: Vec<ExecutorCall>,
    entrypoint: ExecutorEntrypoint,
) -> anyhow::Result<BuiltArbTx> {
    let (packed_route, route_hash) = build_packed_route_payload(
        flash_token,
        flash_amount,
        profit_token,
        min_profit,
        deadline,
        &calls,
    )?;

    let data = match entrypoint {
        ExecutorEntrypoint::AaveFlash => crate::abis::IArbExecutor::executeArbWithAaveCall {
            packedRoute: packed_route.clone(),
        }
        .abi_encode(),
        ExecutorEntrypoint::Direct => crate::abis::IArbExecutor::executeArbDirectCall {
            packedRoute: packed_route.clone(),
        }
        .abi_encode(),
        ExecutorEntrypoint::BalancerFlash => crate::abis::IArbExecutor::executeArbCall {
            packedRoute: packed_route.clone(),
        }
        .abi_encode(),
    };

    let data_bytes: Vec<u8> = data;
    let mut hex_preview = String::with_capacity(200);
    for b in data_bytes.iter().take(100) {
        let _ = write!(hex_preview, "{b:02x}");
    }
    crate::info!(
        "calldata len={}, preview=0x{}..., route_hash={}, entrypoint={entrypoint:?}",
        data_bytes.len(),
        hex_preview,
        route_hash,
    );

    Ok(BuiltArbTx {
        to: executor,
        data: data_bytes.into(),
        value: U256::ZERO,
        route_hash,
        calls,
    })
}
