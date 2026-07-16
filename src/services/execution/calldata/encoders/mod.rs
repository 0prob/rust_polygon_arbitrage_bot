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
use crate::core::types::ProtocolType;
use crate::pipeline::arena::StateArena;
use crate::services::execution::profit::slippage_adjusted;

use super::{CalldataHop, RouteEncodeConfig};
use crate::core::types::FlashLoanSource;

/// Dispatch to the protocol-specific encoder free function.
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
            if is_first_hop {
                // After vault/Aave/DODO flash, sweep the credited balance (exact `amount_in` can drift).
                let use_transfer_all = flash_source != FlashLoanSource::Direct;
                v2::encode_v2_hop(arena, hop, recipient, config.slippage_bps, use_transfer_all)
            } else {
                let mut h = hop.clone();
                let slip = config.slippage_bps.saturating_add(100);
                if let Some(adj) = slippage_adjusted(h.amount_in, slip) {
                    h.amount_in = adj;
                }
                v2::encode_v2_hop(arena, &h, recipient, config.slippage_bps, true)
            }
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
        ProtocolType::Dodo => dodo::encode_dodo_hop(hop, recipient, !is_first_hop),
        ProtocolType::Woofi => woofi::encode_woofi_hop(hop, recipient, arena, config.slippage_bps),
    }
}
