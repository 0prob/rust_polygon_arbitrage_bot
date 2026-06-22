pub mod balancer;
pub mod curve;
pub mod dodo;
pub mod kyber;
/// Shared encoding utilities across all protocol encoders
pub mod shared;
pub mod v2;
pub mod v3;
pub mod v4;
pub mod woofi;

pub use balancer::encode_balancer_hop;
pub use curve::encode_curve_hop;
pub use dodo::encode_dodo_hop;
pub use kyber::encode_kyber_hop;
pub use shared::{
    curve_uses_receiver, derive_balancer_pool_id, resolve_balancer_pool_id, to_v3_state,
};
pub use v2::encode_v2_hop;
pub use v3::encode_v3_hop;
pub use v4::encode_v4_hop;
pub use woofi::encode_woofi_hop;

use alloy::primitives::Address;

use crate::abis::ExecutorCall;
use crate::core::types::ProtocolType;
use crate::pipeline::arena::StateArena;
use crate::services::execution::profit::slippage_adjusted;
use crate::services::execution::quote::is_kyber_protocol;

use super::types::{CalldataHop, RouteEncodeConfig};

/// Trait for protocol-specific encoding.
///
/// Each protocol encoder module implements this trait.
pub trait ProtocolEncoder {
    fn encode_hop(
        &self,
        hop: &CalldataHop,
        recipient: Address,
        arena: &StateArena,
        config: &RouteEncodeConfig,
        is_first_hop: bool,
    ) -> anyhow::Result<Vec<ExecutorCall>>;
}

/// Factory: return the encoder for a given protocol type.
/// For V3, checks the protocol_label to distinguish Kyber from vanilla V3.
pub fn encoder_for_protocol(
    protocol: ProtocolType,
    protocol_label: Option<&str>,
) -> Box<dyn ProtocolEncoder> {
    match protocol {
        ProtocolType::UniswapV2 => Box::new(V2Encoder),
        ProtocolType::UniswapV3 if is_kyber_protocol(protocol_label) => Box::new(KyberEncoder),
        ProtocolType::UniswapV3 => Box::new(V3Encoder),
        ProtocolType::UniswapV4 => Box::new(V4Encoder),
        ProtocolType::CurveStable | ProtocolType::CurveCrypto => Box::new(CurveEncoder),
        ProtocolType::BalancerV2 => Box::new(BalancerEncoder),
        ProtocolType::Dodo => Box::new(DodoEncoder),
        ProtocolType::Woofi => Box::new(WoofiEncoder),
    }
}

// ---------------------------------------------------------------------------
// ProtocolEncoder implementations (thin wrappers around the free functions)
// ---------------------------------------------------------------------------

pub struct V2Encoder;

impl ProtocolEncoder for V2Encoder {
    fn encode_hop(
        &self,
        hop: &CalldataHop,
        recipient: Address,
        arena: &StateArena,
        config: &RouteEncodeConfig,
        is_first_hop: bool,
    ) -> anyhow::Result<Vec<ExecutorCall>> {
        let mut h = hop.clone();
        if !is_first_hop {
            let slip = config.slippage_bps.saturating_add(100);
            if let Some(adj) = slippage_adjusted(h.amount_in, slip) {
                h.amount_in = adj;
            }
        }
        v2::encode_v2_hop(
            Some(arena),
            &h,
            recipient,
            config.slippage_bps,
            !is_first_hop,
        )
    }
}

pub struct V3Encoder;

impl ProtocolEncoder for V3Encoder {
    fn encode_hop(
        &self,
        hop: &CalldataHop,
        recipient: Address,
        arena: &StateArena,
        config: &RouteEncodeConfig,
        _is_first_hop: bool,
    ) -> anyhow::Result<Vec<ExecutorCall>> {
        v3::encode_v3_hop(hop, recipient, arena, config.slippage_bps)
    }
}

pub struct V4Encoder;

impl ProtocolEncoder for V4Encoder {
    fn encode_hop(
        &self,
        hop: &CalldataHop,
        recipient: Address,
        arena: &StateArena,
        _config: &RouteEncodeConfig,
        _is_first_hop: bool,
    ) -> anyhow::Result<Vec<ExecutorCall>> {
        v4::encode_v4_hop(hop, recipient, arena)
    }
}

pub struct KyberEncoder;

impl ProtocolEncoder for KyberEncoder {
    fn encode_hop(
        &self,
        hop: &CalldataHop,
        recipient: Address,
        arena: &StateArena,
        config: &RouteEncodeConfig,
        _is_first_hop: bool,
    ) -> anyhow::Result<Vec<ExecutorCall>> {
        kyber::encode_kyber_hop(hop, recipient, arena, config.slippage_bps)
    }
}

pub struct CurveEncoder;

impl ProtocolEncoder for CurveEncoder {
    fn encode_hop(
        &self,
        hop: &CalldataHop,
        recipient: Address,
        arena: &StateArena,
        config: &RouteEncodeConfig,
        _is_first_hop: bool,
    ) -> anyhow::Result<Vec<ExecutorCall>> {
        curve::encode_curve_hop(hop, recipient, arena, config.slippage_bps)
    }
}

pub struct BalancerEncoder;

impl ProtocolEncoder for BalancerEncoder {
    fn encode_hop(
        &self,
        hop: &CalldataHop,
        recipient: Address,
        arena: &StateArena,
        config: &RouteEncodeConfig,
        _is_first_hop: bool,
    ) -> anyhow::Result<Vec<ExecutorCall>> {
        balancer::encode_balancer_hop(hop, recipient, arena, config.slippage_bps, config.deadline)
    }
}

pub struct DodoEncoder;

impl ProtocolEncoder for DodoEncoder {
    fn encode_hop(
        &self,
        hop: &CalldataHop,
        recipient: Address,
        _arena: &StateArena,
        _config: &RouteEncodeConfig,
        is_first_hop: bool,
    ) -> anyhow::Result<Vec<ExecutorCall>> {
        dodo::encode_dodo_hop(hop, recipient, !is_first_hop)
    }
}

pub struct WoofiEncoder;

impl ProtocolEncoder for WoofiEncoder {
    fn encode_hop(
        &self,
        hop: &CalldataHop,
        recipient: Address,
        arena: &StateArena,
        config: &RouteEncodeConfig,
        _is_first_hop: bool,
    ) -> anyhow::Result<Vec<ExecutorCall>> {
        woofi::encode_woofi_hop(hop, recipient, arena, config.slippage_bps)
    }
}
