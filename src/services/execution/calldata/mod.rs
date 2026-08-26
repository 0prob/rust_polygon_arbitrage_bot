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

/// Whether this hop can send `token_out` straight to an address other than the executor.
///
/// Used to fund the next V2 pair without Huff `transferAll` (live: Curve NG GHST/stGHST → V2
/// failed hop-2 transferAll with empty nested revert when intermediate stayed off-executor).
#[must_use]
fn hop_can_direct_output_to(hop: &CalldataHop) -> bool {
    match hop.edge.protocol {
        ProtocolType::UniswapV2
        | ProtocolType::UniswapV3
        | ProtocolType::Dodo
        | ProtocolType::BalancerV2
        | ProtocolType::Woofi => true,
        ProtocolType::CurveStable | ProtocolType::CurveCrypto => {
            // Only StableSwap-NG `exchange(..., receiver)`; plain Stable/Crypto always
            // pay `msg.sender` (executor) so the next hop must transferAll.
            curve_uses_receiver(hop.pool_type.as_deref())
        }
        ProtocolType::UniswapV4 => false,
    }
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
    let mut calls = Vec::with_capacity(hops.len().saturating_mul(2));
    // Encoded V2 `amountOut` becomes the next hop's `amount_in` (V2 pair-chain or V3 exact-in).
    let mut chain_in: Option<U256> = None;
    // Where the previous hop left `token_out` (pool address or executor). Flash credit
    // lands on the executor, so `None` means "on executor" for hop0 Exact prefund.
    let mut funds_at: Option<Address> = None;
    for (i, hop) in hops.iter().enumerate() {
        let mut hop = hop.clone();
        if let Some(ain) = chain_in.take() {
            // Cap by assess hop_amounts: encode re-quote can inflate intermediate
            // above Brent sizing when local tick walk is optimistic.
            let capped = if hop.amount_in.is_zero() {
                ain
            } else {
                ain.min(hop.amount_in)
            };
            crate::debug!(
                "chain_in apply: hop={i} proto={:?} ain_was={} chain={ain} ain_now={capped} aout={}",
                hop.edge.protocol,
                hop.amount_in,
                hop.amount_out,
            );
            hop.amount_in = capped;
        }
        if hop.edge.protocol == ProtocolType::UniswapV2 {
            // V2→V2 (and Curve-NG/DODO/… → V2) leave tokens on this pair: skip prefund.
            // transferAll only when residual is on the executor (else empty-balance revert).
            let next_v2 = hops
                .get(i + 1)
                .is_some_and(|h| h.edge.protocol == ProtocolType::UniswapV2);
            let swap_to = if next_v2 {
                hops[i + 1].pool_address
            } else {
                executor
            };
            let prefund = if funds_at == Some(hop.pool_address) {
                encoders::v2::V2Prefund::Skipped
            } else if i == 0 {
                encoders::v2::V2Prefund::Exact
            } else {
                encoders::v2::V2Prefund::TransferAll
            };
            let (hop_calls, amount_out) = encoders::v2::encode_v2_hop(
                arena,
                &hop,
                swap_to,
                executor,
                config.slippage_bps,
                prefund,
            )?;
            calls.extend(hop_calls);
            chain_in = Some(amount_out);
            funds_at = Some(swap_to);
            continue;
        }
        // Refresh amount_out for the (possibly chained) ain before encode + chain_in.
        // Stale conservative_execution_hops floors were sized for the pre-chain ain and
        // can exceed what Balancer/Curve will deliver → next hop transfer/IIA fails.
        let bps = config
            .slippage_bps
            .max(crate::core::constants::EXECUTION_MIN_SLIPPAGE_BPS);
        let quoted_out = quote_hop_for_execution(arena, &hop)
            .ok_or_else(|| anyhow::anyhow!("chain_in hop execution quote unavailable"))?;
        let min_out = slippage_adjusted(quoted_out, bps)
            .ok_or_else(|| anyhow::anyhow!("chain_in hop min out is zero"))?;
        if min_out != hop.amount_out {
            crate::debug!(
                "chain_in refresh_aout: hop={i} proto={:?} ain={} aout_was={} aout_now={min_out} quoted={quoted_out}",
                hop.edge.protocol,
                hop.amount_in,
                hop.amount_out,
            );
        }
        hop.amount_out = min_out;
        // Prefer paying the next V2 pair directly when the ABI supports a recipient —
        // skips transferAll and the live empty-balance class of failures.
        let next = hops.get(i + 1);
        let next_is_v2 = next.is_some_and(|h| h.edge.protocol == ProtocolType::UniswapV2);
        let recipient = if next_is_v2 && hop_can_direct_output_to(&hop) {
            hops[i + 1].pool_address
        } else {
            executor
        };
        if recipient != executor {
            crate::debug!(
                "direct_out: hop={i} proto={:?} to_v2={recipient:#x}",
                hop.edge.protocol
            );
        }
        // Intermediate V3 hops: full-range price limit so exact-in fully fills.
        // Tight limits + stale state partial-fill → under-delivery vs chain_in.
        let is_last = next.is_none();
        calls.extend(encode_hop_for_protocol(
            &hop,
            recipient,
            executor,
            arena,
            &config,
            i == 0,
            flash_source,
            is_last,
            Some(quoted_out),
        )?);
        funds_at = Some(recipient);
        if next.is_some() {
            // Exact-pay next hops need chain_in ≤ delivered intermediate.
            // minOut slip alone was insufficient (live BRZ/BRLA mid-hop TransferFailed).
            let chain_bps = bps.saturating_add(crate::core::constants::EXECUTION_CHAIN_IN_BUFFER_BPS);
            let chained = slippage_adjusted(quoted_out, chain_bps)
                .ok_or_else(|| anyhow::anyhow!("chain_in hop buffer out is zero"))?;
            crate::debug!(
                "chain_in haircut: hop={i}->{} quoted={quoted_out} min={min_out} chained={chained} chain_bps={chain_bps}",
                i + 1
            );
            chain_in = Some(chained);
        }
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

/// Build calldata hops from route edges, hop amounts, and pool metadata.
/// Err string names the reject branch (dispatch was failing opaque `None`).
pub fn build_calldata_hops(
    arena: &StateArena,
    edges: &[Edge],
    hop_amounts: &[U256],
    pool_metas_by_pool: &FxHashMap<PoolIndex, &PoolMeta>,
) -> Result<Vec<CalldataHop>, String> {
    if edges.is_empty() {
        return Err("empty_edges".into());
    }
    if hop_amounts.len() != edges.len() + 1 {
        return Err(format!(
            "hop_amounts_len={} edges={}",
            hop_amounts.len(),
            edges.len()
        ));
    }
    // Address-aware (same as sim): TokenIndex inequality false-fails aliases if any
    // ever diverge; prefer ERC-20 address continuity.
    if crate::pipeline::local_sim::first_hop_continuity_break_in_arena(arena, edges).is_some() {
        return Err("broken_token_chain".into());
    }
    let mut hops = Vec::with_capacity(edges.len());
    for (i, edge) in edges.iter().enumerate() {
        let Some(pool_address) = arena.pool_address(edge.pool_index) else {
            return Err(format!(
                "hop{i}: missing_pool_addr idx={}",
                edge.pool_index.0
            ));
        };
        if !crate::services::discovery::is_plausible_contract_address(pool_address) {
            return Err(format!("hop{i}: implausible_pool {pool_address}"));
        }
        let Some(token_in) = arena.token_address(edge.token_in) else {
            return Err(format!("hop{i}: missing_token_in idx={}", edge.token_in.0));
        };
        let Some(token_out) = arena.token_address(edge.token_out) else {
            return Err(format!(
                "hop{i}: missing_token_out idx={}",
                edge.token_out.0
            ));
        };
        if !crate::services::discovery::is_plausible_contract_address(token_in)
            || !crate::services::discovery::is_plausible_contract_address(token_out)
        {
            return Err(format!(
                "hop{i}: implausible_tokens in={token_in} out={token_out}"
            ));
        }
        if matches!(
            edge.protocol,
            ProtocolType::UniswapV2 | ProtocolType::UniswapV3 | ProtocolType::UniswapV4
        ) && edge.zero_for_one != (token_in < token_out)
        {
            return Err(format!(
                "hop{i}: zfo_mismatch zfo={} expect={} proto={:?} pool={pool_address}",
                edge.zero_for_one,
                token_in < token_out,
                edge.protocol
            ));
        }
        let meta = pool_metas_by_pool.get(&edge.pool_index).copied();
        if let Some(pool) = meta.filter(|p| p.protocol != edge.protocol) {
            // Meta can lag healed edges; trust edge when arena state agrees.
            let arena_ok = arena.pool_state(edge.pool_index).is_some_and(|s| {
                crate::pipeline::local_sim::protocol_matches_pool_state(edge.protocol, s)
            });
            if !arena_ok {
                return Err(format!(
                    "hop{i}: meta_proto_mismatch meta={:?} edge={:?} pool={pool_address}",
                    pool.protocol, edge.protocol
                ));
            }
        }
        // V2/V3/V4 sim ignores pair membership; stale TokenIndex on a live pool
        // address → V2 INSUFFICIENT_INPUT / V3 InvalidPoolCaller (factory resolves
        // a different pool from callback token0/token1/fee).
        // Curve: coins() order is meta.tokens — wrong idx → exchange pulls the wrong
        // coin, intermediate stays 0, next V2 transferAll reverts empty on executor
        // (live: GHST/stGHST Curve→V2 hop2 ExternalCallFailed target=executor).
        if matches!(
            edge.protocol,
            ProtocolType::UniswapV2
                | ProtocolType::UniswapV3
                | ProtocolType::UniswapV4
                | ProtocolType::CurveStable
                | ProtocolType::CurveCrypto
        ) {
            let tag = match edge.protocol {
                ProtocolType::UniswapV3 => "v3",
                ProtocolType::UniswapV4 => "v4",
                ProtocolType::CurveStable | ProtocolType::CurveCrypto => "curve",
                _ => "v2",
            };
            match meta {
                Some(m) if m.tokens.len() >= 2 => {
                    // Address compare: TokenIndex is not unique per ERC-20.
                    let meta_has = |addr| {
                        m.tokens
                            .iter()
                            .any(|&t| arena.token_address(t) == Some(addr))
                    };
                    if !(meta_has(token_in) && meta_has(token_out)) {
                        return Err(format!(
                            "hop{i}: {tag}_token_not_in_pool in={token_in} out={token_out} pool={pool_address}"
                        ));
                    }
                }
                Some(_) => {
                    return Err(format!(
                        "hop{i}: {tag}_meta_tokens_short pool={pool_address}"
                    ));
                }
                // Fail closed: missing meta cannot prove membership.
                None => {
                    return Err(format!("hop{i}: {tag}_no_meta pool={pool_address}"));
                }
            }
        }

        // Re-resolve Curve coin indices from meta.tokens order (matches coins()).
        // Stale edge.token_in_idx from graph attach can disagree with coin order
        // after token re-registration → exchange sells the wrong leg.
        let mut edge = *edge;
        if matches!(
            edge.protocol,
            ProtocolType::CurveStable | ProtocolType::CurveCrypto
        )
            && let Some(m) = meta {
                let pos = |addr: Address| {
                    m.tokens.iter().position(|&t| {
                        arena.token_address(t).is_some_and(|a| a == addr)
                    })
                };
                match (pos(token_in), pos(token_out)) {
                    (Some(i_pos), Some(j_pos)) if i_pos != j_pos => {
                        if edge.token_in_idx as usize != i_pos
                            || edge.token_out_idx as usize != j_pos
                        {
                            crate::debug!(
                                "curve idx remap: pool={pool_address:#x} edge={}->{} meta={i_pos}->{j_pos}",
                                edge.token_in_idx,
                                edge.token_out_idx,
                            );
                        }
                        edge.token_in_idx = i_pos as u8;
                        edge.token_out_idx = j_pos as u8;
                    }
                    _ => {
                        return Err(format!(
                            "hop{i}: curve_coin_idx_unresolved in={token_in} out={token_out} pool={pool_address}"
                        ));
                    }
                }
            }
        if let Some(label) = meta.and_then(|pool| pool.protocol_label.as_deref())
            && !crate::core::protocol::is_known_protocol_label(label)
        {
            return Err(format!("hop{i}: unknown_label {label} pool={pool_address}"));
        }
        let meta_pool_id = meta.and_then(|m| m.pool_id);
        let arena_pool_id = balancer_pool_id_from_arena(arena, edge.pool_index);
        let pool_id = if edge.protocol == ProtocolType::BalancerV2 {
            arena_pool_id.or(meta_pool_id)
        } else {
            meta_pool_id
        };
        hops.push(CalldataHop {
            edge,
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
    Ok(hops)
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
    // Include flash_token + flash_amount words (ABI head + first 2 packed words).
    for b in data_bytes.iter().take(140) {
        let _ = write!(hex_preview, "{b:02x}");
    }
    let call_targets: String = calls
        .iter()
        .map(|c| format!("{:#x}", c.target))
        .collect::<Vec<_>>()
        .join(",");
    let call0_data = calls.first().map_or_else(
        || "none".to_string(),
        |c| {
            let mut s = String::with_capacity(20 + c.data.len().saturating_mul(2));
            let _ = write!(s, "{}b:0x", c.data.len());
            for b in c.data.iter().take(68) {
                let _ = write!(s, "{b:02x}");
            }
            s
        },
    );
    crate::debug!(
        "calldata len={}, calls={}, flash_token={flash_token:#x}, flash_amount={flash_amount}, preview=0x{}..., route_hash={}, entrypoint={entrypoint:?}, call0={call0_data}, targets=[{call_targets}]",
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
    fn encode_route_requotes_chained_v2_hops() {
        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let t2 = arena.register_token(Address::from([3u8; 20]));
        let first_pool = arena.register_pool(Address::from([4u8; 20]), v2_state());
        let second_pool = arena.register_pool(Address::from([5u8; 20]), v2_state());
        let mut first = v2_hop(first_pool, t0, t1);
        first.amount_in = U256::from(1_000u64);
        first.pool_address = Address::from([4u8; 20]);
        first.token_in = Address::from([1u8; 20]);
        first.token_out = Address::from([2u8; 20]);
        let mut second = v2_hop(second_pool, t1, t2);
        second.pool_address = Address::from([5u8; 20]);
        second.token_in = Address::from([2u8; 20]);
        second.token_out = Address::from([3u8; 20]);

        let calls = encode_route(
            &arena,
            &[first, second],
            Address::from([9u8; 20]),
            RouteEncodeConfig {
                slippage_bps: 100,
                deadline: U256::from(1u8),
            },
            FlashLoanSource::AaveV3,
        )
        .expect("route should quote");
        assert_eq!(calls.len(), 3);
    }

    #[test]
    fn hop_can_direct_output_curve_only_when_ng() {
        let mut hop = CalldataHop {
            edge: Edge {
                pool_index: PoolIndex(0),
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::CurveStable,
                fee_bps: 4,
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
        assert!(
            !hop_can_direct_output_to(&hop),
            "plain Curve pays msg.sender only"
        );
        hop.pool_type = Some("stable_ng".into());
        assert!(
            hop_can_direct_output_to(&hop),
            "StableSwap-NG exchange supports receiver"
        );
        hop.edge.protocol = ProtocolType::Dodo;
        hop.pool_type = None;
        assert!(hop_can_direct_output_to(&hop));
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

        let err = build_calldata_hops(
            &arena,
            &edges,
            &[U256::from(1u8), U256::from(1u8)],
            &FxHashMap::default(),
        )
        .expect_err("noncanonical zfo");
        assert!(err.contains("zfo_mismatch"), "{err}");
    }

    #[test]
    fn v2_hop_rejects_tokens_absent_from_pool_meta() {
        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let foreign = arena.register_token(Address::from([9u8; 20]));
        let pool = arena.register_pool(Address::from([3u8; 20]), v2_state());
        let meta = PoolMeta {
            pool_index: pool,
            protocol: ProtocolType::UniswapV2,
            tokens: vec![t0, t1],
            fee_bps: 30,
            bpt_index: None,
            pool_id: None,
            protocol_label: Some("SUSHISWAP_V2".into()),
            pool_type: None,
            hooks: None,
            tick_spacing: None,
        };
        let edges = [Edge {
            pool_index: pool,
            token_in: foreign,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: Address::from([9u8; 20]) < Address::from([2u8; 20]),
        }];
        let mut map = FxHashMap::default();
        map.insert(pool, &meta);
        let err = build_calldata_hops(&arena, &edges, &[U256::from(1u8), U256::from(1u8)], &map)
            .expect_err("foreign token");
        assert!(err.contains("v2_token_not_in_pool"), "{err}");
    }

    #[test]
    fn v2_hop_rejects_missing_pool_meta() {
        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(Address::from([3u8; 20]), v2_state());
        let edges = [Edge {
            pool_index: pool,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        }];
        let err = build_calldata_hops(
            &arena,
            &edges,
            &[U256::from(1u8), U256::from(1u8)],
            &FxHashMap::default(),
        )
        .expect_err("missing meta");
        assert!(err.contains("v2_no_meta"), "{err}");
    }

    #[test]
    fn v3_hop_rejects_tokens_absent_from_pool_meta() {
        

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let foreign = arena.register_token(Address::from([9u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(crate::test_support::v3_pool_state_fixture()),
        );
        let meta = PoolMeta {
            pool_index: pool,
            protocol: ProtocolType::UniswapV3,
            tokens: vec![t0, t1],
            fee_bps: 30,
            bpt_index: None,
            pool_id: None,
            protocol_label: Some("UNISWAP_V3".into()),
            pool_type: None,
            hooks: None,
            tick_spacing: Some(60),
        };
        let edges = [Edge {
            pool_index: pool,
            token_in: foreign,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: Address::from([9u8; 20]) < Address::from([2u8; 20]),
        }];
        let mut map = FxHashMap::default();
        map.insert(pool, &meta);
        let err = build_calldata_hops(&arena, &edges, &[U256::from(1u8), U256::from(1u8)], &map)
            .expect_err("foreign token");
        assert!(err.contains("v3_token_not_in_pool"), "{err}");
    }
}
