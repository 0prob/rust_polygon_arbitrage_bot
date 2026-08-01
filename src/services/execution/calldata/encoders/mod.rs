pub mod balancer;
pub mod curve;
pub mod dodo;
/// Shared encoding utilities across all protocol encoders
pub mod shared;
pub mod v2;
pub mod v3;
pub mod v4;
pub mod woofi;

use alloy::primitives::Address;

use crate::abis::ExecutorCall;
use crate::core::types::{FlashLoanSource, ProtocolType};
use crate::pipeline::arena::StateArena;

use super::{CalldataHop, RouteEncodeConfig};

/// Dispatch to the protocol-specific encoder free function.
///
/// Protocol notes:
/// - **V2**: handled in [`super::encode_route`] (pair-chaining + prefund modes).
/// - **V3/V4**: exact-in via pool/manager; callback pays input (no pre-fund).
/// - **Balancer**: approve + vault `swap` (never under Balancer vault flash).
/// - **Curve / WooFi**: approve + exact-in exchange (minOut carries slippage).
/// - **DODO**: transfer + `sellBase`/`sellQuote` from on-chain base/quote; not under
///   DODO flash on the same pool (`preventReentrant`).
/// `is_last_hop`: last hop may use a tight V3/V4 price limit; intermediate hops use
/// full-range limits so exact-in fully fills for chain_in fidelity.
///
/// `prequoted_out`: execution quote already computed by `encode_route` for chain_in
/// (V3/V4 reuse it; other protocols re-quote via `compute_min_out`).
///
/// `executor` is the ArbExecutor address (required for DODO/V2 `transferAll` targets).
pub fn encode_hop_for_protocol(
    hop: &CalldataHop,
    recipient: Address,
    executor: Address,
    arena: &StateArena,
    config: &RouteEncodeConfig,
    is_first_hop: bool,
    _flash_source: FlashLoanSource,
    is_last_hop: bool,
    prequoted_out: Option<alloy::primitives::U256>,
) -> anyhow::Result<Vec<ExecutorCall>> {
    match hop.edge.protocol {
        ProtocolType::UniswapV2 => {
            // Fallback path (tests): no pair-chaining — fund + swap back to executor.
            let prefund = if is_first_hop {
                v2::V2Prefund::Exact
            } else {
                v2::V2Prefund::TransferAll
            };
            v2::encode_v2_hop(
                arena,
                hop,
                recipient,
                executor,
                config.slippage_bps,
                prefund,
            )
            .map(|(calls, _)| calls)
        }
        ProtocolType::UniswapV3 => v3::encode_v3_hop(
            hop,
            recipient,
            arena,
            config.slippage_bps,
            /* full_range_limit */ !is_last_hop,
            prequoted_out,
        ),
        ProtocolType::UniswapV4 => v4::encode_v4_hop(
            hop,
            arena,
            config.slippage_bps,
            /* full_range_limit */ !is_last_hop,
            prequoted_out,
        ),
        ProtocolType::CurveStable | ProtocolType::CurveCrypto => {
            curve::encode_curve_hop(hop, recipient, arena, config.slippage_bps)
        }
        ProtocolType::BalancerV2 => balancer::encode_balancer_hop(
            hop,
            recipient,
            executor,
            arena,
            config.slippage_bps,
            config.deadline,
        ),
        ProtocolType::Dodo => {
            let use_transfer_all = !is_first_hop;
            dodo::encode_dodo_hop(arena, hop, recipient, executor, use_transfer_all)
        }
        ProtocolType::Woofi => woofi::encode_woofi_hop(hop, recipient, arena, config.slippage_bps),
    }
}
