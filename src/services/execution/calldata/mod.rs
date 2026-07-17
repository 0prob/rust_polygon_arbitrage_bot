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
use crate::services::execution::profit::slippage_adjusted;

use crate::services::execution::quote::quote_hop_for_execution;
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
    let mut payload = Vec::with_capacity(0xc0 + packed_calls.len());
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
    validate_flash_hop_compatibility(hops, flash_source)?;
    if flash_source == FlashLoanSource::Direct
        && hops_are_balancer_only(hops)
        && hops.len() <= crate::pipeline::route_calls::MAX_BALANCER_BATCH_HOPS
    {
        return encoders::balancer::encode_balancer_batch_route(hops, executor, config.deadline);
    }
    let hops = conservative_execution_hops(arena, hops, config.slippage_bps)?;
    let mut calls = Vec::with_capacity(hops.len().saturating_mul(2));
    for (i, hop) in hops.iter().enumerate() {
        calls.extend(encode_hop_for_protocol(
            hop,
            executor,
            arena,
            &config,
            i == 0,
            flash_source,
        )?);
    }
    Ok(calls)
}

/// Fail closed when flash-loan reentrancy forbids the encoded hop set.
fn validate_flash_hop_compatibility(
    hops: &[CalldataHop],
    flash_source: FlashLoanSource,
) -> anyhow::Result<()> {
    match flash_source {
        // Vault `flashLoan` → `receiveFlashLoan` rejects any Vault target in the route.
        FlashLoanSource::Balancer => {
            if hops
                .iter()
                .any(|h| h.edge.protocol == ProtocolType::BalancerV2)
            {
                anyhow::bail!(
                    "Balancer vault flash cannot include Balancer vault swap hops (reentrancy)"
                );
            }
        }
        // DODO `flashLoan` is `preventReentrant` on the lending pool; sellBase/sellQuote
        // on that pool inside the callback reverts. Until external DODO flash liquidity
        // is wired with hop-level pool exclusion, any DODO swap hop is incompatible.
        FlashLoanSource::Dodo => {
            if hops.iter().any(|h| h.edge.protocol == ProtocolType::Dodo) {
                anyhow::bail!(
                    "DODO flash cannot include DODO swap hops on the flash pool (reentrancy)"
                );
            }
        }
        FlashLoanSource::AaveV3 | FlashLoanSource::Direct => {}
    }
    Ok(())
}

/// Re-quote the route for execution sizing.
///
/// - `amount_in` of hop 0 is preserved (flash/borrow size).
/// - Each hop stores `amount_out` as the slippage floor (minOut for pull protocols).
/// - The **next** hop's `amount_in` is the **full quote** (not the floor) so exact-in
///   and V2 `amountOut` sizing match expected production, not a double haircut.
fn conservative_execution_hops(
    arena: &StateArena,
    hops: &[CalldataHop],
    slippage_bps: u64,
) -> anyhow::Result<Vec<CalldataHop>> {
    let mut amount_in = hops
        .first()
        .ok_or_else(|| anyhow::anyhow!("route requires at least one hop"))?
        .amount_in;
    let mut out = Vec::with_capacity(hops.len());
    for hop in hops {
        let mut hop = hop.clone();
        hop.amount_in = amount_in;
        let quoted_out = quote_hop_for_execution(arena, &hop)
            .ok_or_else(|| anyhow::anyhow!("execution quote unavailable"))?;
        hop.amount_out = slippage_adjusted(quoted_out, slippage_bps)
            .ok_or_else(|| anyhow::anyhow!("execution hop min out is zero"))?;
        // Chain full quote so intermediate exact-in hops are not systematically undersized.
        amount_in = quoted_out;
        out.push(hop);
    }
    Ok(out)
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
    DodoFlash,
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
    if edges.is_empty()
        || hop_amounts.len() != edges.len() + 1
        || edges
            .windows(2)
            .any(|pair| pair[0].token_out != pair[1].token_in)
    {
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
        if matches!(
            edge.protocol,
            ProtocolType::UniswapV2 | ProtocolType::UniswapV3 | ProtocolType::UniswapV4
        ) && edge.zero_for_one != (token_in < token_out)
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
        ExecutorEntrypoint::DodoFlash => crate::abis::IArbExecutor::executeArbWithDodoCall {
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
    let call_targets: String = calls
        .iter()
        .map(|c| format!("{:#x}", c.target))
        .collect::<Vec<_>>()
        .join(",");
    crate::info!(
        "calldata len={}, calls={}, preview=0x{}..., route_hash={}, entrypoint={entrypoint:?}, targets=[{call_targets}]",
        data_bytes.len(),
        calls.len(),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy::primitives::Address;

    use super::*;
    use crate::core::types::{TokenIndex, V2PoolState};

    fn v2_state() -> Arc<PoolState> {
        Arc::new(PoolState::V2(V2PoolState {
            reserve0: U256::from(1_000_000u64),
            reserve1: U256::from(1_000_000u64),
            fee: U256::from(997u64),
            fee_denominator: U256::from(1_000u64),
            block_timestamp_last: 1,
        }))
    }

    fn v2_hop(pool: PoolIndex, token_in: TokenIndex, token_out: TokenIndex) -> CalldataHop {
        CalldataHop {
            edge: Edge {
                pool_index: pool,
                token_in,
                token_out,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            pool_address: Address::ZERO,
            token_in: Address::ZERO,
            token_out: Address::ZERO,
            amount_in: U256::ZERO,
            amount_out: U256::ZERO,
            pool_id: None,
            protocol_label: None,
            pool_type: None,
            router: None,
            hooks: None,
        }
    }

    #[test]
    fn downstream_hop_chains_full_quote_and_stores_min_out() {
        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let t2 = arena.register_token(Address::from([3u8; 20]));
        let first_pool = arena.register_pool(Address::from([4u8; 20]), v2_state());
        let second_pool = arena.register_pool(Address::from([5u8; 20]), v2_state());
        let mut first = v2_hop(first_pool, t0, t1);
        first.amount_in = U256::from(1_000u64);
        let second = v2_hop(second_pool, t1, t2);

        let normalized = conservative_execution_hops(&arena, &[first.clone(), second], 100)
            .expect("route should quote");
        let quoted = quote_hop_for_execution(&arena, &first).expect("first quote");

        // amount_out is the slippage floor; next amount_in is the full quote.
        assert!(normalized[0].amount_out < quoted);
        assert_eq!(normalized[1].amount_in, quoted);
    }

    #[test]
    fn balancer_flash_rejects_vault_swap_hops() {
        let hop = CalldataHop {
            edge: Edge {
                pool_index: PoolIndex(0),
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::BalancerV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            pool_address: Address::from([9u8; 20]),
            token_in: Address::from([1u8; 20]),
            token_out: Address::from([2u8; 20]),
            amount_in: U256::from(1u64),
            amount_out: U256::from(1u64),
            pool_id: None,
            protocol_label: None,
            pool_type: None,
            router: None,
            hooks: None,
        };
        let err = validate_flash_hop_compatibility(&[hop], FlashLoanSource::Balancer)
            .expect_err("vault hops under vault flash");
        assert!(err.to_string().contains("reentrancy"));
    }

    #[test]
    fn dodo_flash_rejects_dodo_swap_hops() {
        let hop = CalldataHop {
            edge: Edge {
                pool_index: PoolIndex(0),
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::Dodo,
                fee_bps: 10,
                zero_for_one: true,
            },
            pool_address: Address::from([9u8; 20]),
            token_in: Address::from([1u8; 20]),
            token_out: Address::from([2u8; 20]),
            amount_in: U256::from(1u64),
            amount_out: U256::from(1u64),
            pool_id: None,
            protocol_label: None,
            pool_type: None,
            router: None,
            hooks: None,
        };
        let err = validate_flash_hop_compatibility(&[hop], FlashLoanSource::Dodo)
            .expect_err("dodo hops under dodo flash");
        assert!(err.to_string().contains("reentrancy"));
    }

    #[test]
    fn v4_hop_with_noncanonical_direction_is_rejected() {
        let mut arena = StateArena::default();
        let high = arena.register_token(Address::from([2u8; 20]));
        let low = arena.register_token(Address::from([1u8; 20]));
        let pool = arena.register_pool(Address::from([3u8; 20]), v2_state());
        let edges = [Edge {
            pool_index: pool,
            token_in: high,
            token_out: low,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV4,
            fee_bps: 30,
            zero_for_one: true,
        }];

        assert!(
            build_calldata_hops(
                &arena,
                &edges,
                &[U256::from(1u8), U256::from(1u8)],
                &FxHashMap::default(),
            )
            .is_none()
        );
    }
}
