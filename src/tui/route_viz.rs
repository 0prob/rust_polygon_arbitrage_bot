use alloy::primitives::Address;

use crate::core::types::{Edge, ProtocolType};
use crate::services::execution::candidate::hash_cycle_edges;
use crate::util::truncate_str;

#[must_use]
pub fn protocol_tag(protocol: ProtocolType) -> &'static str {
    match protocol {
        ProtocolType::UniswapV2 => "V2",
        ProtocolType::UniswapV3 => "V3",
        ProtocolType::UniswapV4 => "V4",
        ProtocolType::BalancerV2 => "BAL",
        ProtocolType::CurveStable => "CRV-S",
        ProtocolType::CurveCrypto => "CRV-C",
        ProtocolType::Dodo => "DODO",
        ProtocolType::Woofi => "WOOFI",
    }
}

#[must_use]
pub fn short_address(address: Address) -> String {
    let s = format!("{address}");
    truncate_str(&s, 12)
}

/// Same fingerprint as execution / HF eval (`hash_cycle_edges`).
#[must_use]
pub fn route_fingerprint(edges: &[Edge]) -> u64 {
    hash_cycle_edges(edges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{CycleEdges, Edge, PoolIndex, ProtocolType, TokenIndex};

    fn sample_edge(reverse: bool) -> Edge {
        let (token_in, token_out) = if reverse {
            (TokenIndex(1), TokenIndex(0))
        } else {
            (TokenIndex(0), TokenIndex(1))
        };
        Edge {
            pool_index: PoolIndex(7),
            token_in,
            token_out,
            token_in_idx: reverse as u8,
            token_out_idx: (!reverse) as u8,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: !reverse,
        }
    }

    #[test]
    fn route_fingerprint_matches_execution_hash() {
        let edges: CycleEdges = CycleEdges::from_slice(&[sample_edge(false)]);
        assert_eq!(route_fingerprint(&edges), hash_cycle_edges(&edges));
    }

    #[test]
    fn pool_meta_lookup_uses_pool_index_not_vec_position() {
        let metas = vec![
            PoolMeta {
                pool_index: PoolIndex(2),
                protocol: ProtocolType::UniswapV3,
                tokens: Vec::new(),
                fee_bps: 30,
                bpt_index: None,
                pool_id: None,
                protocol_label: None,
                pool_type: None,
                hooks: None,
                tick_spacing: None,
            },
            PoolMeta {
                pool_index: PoolIndex(5),
                protocol: ProtocolType::UniswapV2,
                tokens: Vec::new(),
                fee_bps: 30,
                bpt_index: None,
                pool_id: None,
                protocol_label: None,
                pool_type: None,
                hooks: None,
                tick_spacing: None,
            },
        ];
        assert_eq!(
            pool_meta_at(&metas, PoolIndex(5))
                .map(|m| m.protocol)
                .unwrap(),
            ProtocolType::UniswapV2
        );
        assert_eq!(
            pool_meta_at(&metas, PoolIndex(2))
                .map(|m| m.protocol)
                .unwrap(),
            ProtocolType::UniswapV3
        );
    }
}
