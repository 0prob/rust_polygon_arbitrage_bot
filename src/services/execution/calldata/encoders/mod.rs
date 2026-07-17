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
/// - **V2**: pre-fund + `swap(..., data="")`. `transferAll` after flash / on later hops.
/// - **V3/V4**: exact-in via pool/manager; callback pays input (no pre-fund).
/// - **Balancer**: approve + vault `swap` (never under Balancer vault flash).
/// - **Curve / WooFi**: approve + exact-in exchange (minOut carries slippage).
/// - **DODO**: transfer + `sellBase`/`sellQuote` from on-chain base/quote; not under
///   DODO flash on the same pool (`preventReentrant`).
pub fn encode_hop_for_protocol(
    hop: &CalldataHop,
    recipient: Address,
    arena: &StateArena,
    config: &RouteEncodeConfig,
    is_first_hop: bool,
    flash_source: FlashLoanSource,
) -> anyhow::Result<Vec<ExecutorCall>> {
    match hop.edge.protocol {
        ProtocolType::UniswapV2 => {
            // After any flash, and on every intermediate hop, sweep the credited balance.
            let use_transfer_all = !is_first_hop || flash_source != FlashLoanSource::Direct;
            v2::encode_v2_hop(arena, hop, recipient, config.slippage_bps, use_transfer_all)
        }
        ProtocolType::UniswapV3 => v3::encode_v3_hop(hop, recipient, arena, config.slippage_bps),
        ProtocolType::UniswapV4 => v4::encode_v4_hop(hop, arena, config.slippage_bps),
        ProtocolType::CurveStable | ProtocolType::CurveCrypto => {
            curve::encode_curve_hop(hop, recipient, arena, config.slippage_bps)
        }
        ProtocolType::BalancerV2 => balancer::encode_balancer_hop(
            hop,
            recipient,
            arena,
            config.slippage_bps,
            config.deadline,
        ),
        ProtocolType::Dodo => {
            let use_transfer_all = !is_first_hop || flash_source != FlashLoanSource::Direct;
            dodo::encode_dodo_hop(arena, hop, recipient, use_transfer_all)
        }
        ProtocolType::Woofi => woofi::encode_woofi_hop(hop, recipient, arena, config.slippage_bps),
    }
}
