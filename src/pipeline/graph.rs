use crate::core::types::{Edge, PoolIndex, ProtocolType, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::spot_price::compute_edge_log_weight_with_state;
use crate::pipeline::types::{GraphEdge, PoolMeta, RoutingGraph};

const DEFAULT_FEE_BPS: u32 = 30;

/// Build directed swap edges for a two-token pool (V2/V3/DODO).
pub fn edges_for_pair(
    pool_index: PoolIndex,
    protocol: ProtocolType,
    token0: TokenIndex,
    token1: TokenIndex,
    fee_bps: u32,
) -> [Edge; 2] {
    [
        Edge {
            pool_index,
            token_in: token0,
            token_out: token1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol,
            fee_bps,
            zero_for_one: true,
        },
        Edge {
            pool_index,
            token_in: token1,
            token_out: token0,
            token_in_idx: 1,
            token_out_idx: 0,
            protocol,
            fee_bps,
            zero_for_one: false,
        },
    ]
}

/// Full multi-token edge expansion (Balancer-style); skips `bpt_index`.
pub fn edges_for_multi_token(
    pool_index: PoolIndex,
    protocol: ProtocolType,
    tokens: &[TokenIndex],
    fee_bps: u32,
    bpt_index: Option<usize>,
) -> Vec<Edge> {
    let token0 = tokens.first().copied();
    let mut out = Vec::new();
    for (i, &tin) in tokens.iter().enumerate() {
        if bpt_index == Some(i) {
            continue;
        }
        for (j, &tout) in tokens.iter().enumerate() {
            if i == j || bpt_index == Some(j) {
                continue;
            }
            out.push(Edge {
                pool_index,
                token_in: tin,
                token_out: tout,
                token_in_idx: i as u8,
                token_out_idx: j as u8,
                protocol,
                fee_bps,
                zero_for_one: token0.map(|t0| tin == t0).unwrap_or(i == 0),
            });
        }
    }
    out
}

pub fn build_graph(arena: &StateArena, pools: &[PoolMeta]) -> RoutingGraph {
    let mut graph = RoutingGraph::new(arena.token_count());

    for meta in pools {
        let tradable = arena
            .pool_state(meta.pool_index)
            .map(|s| s.is_tradable())
            .unwrap_or(false);
        if !tradable {
            continue;
        }

        let edges: Vec<Edge> = if meta.tokens.len() > 2 {
            edges_for_multi_token(
                meta.pool_index,
                meta.protocol,
                &meta.tokens,
                meta.fee_bps,
                meta.bpt_index,
            )
        } else {
            edges_for_pair(
                meta.pool_index,
                meta.protocol,
                meta.token0,
                meta.token1,
                meta.fee_bps,
            )
            .to_vec()
        };

        for edge in edges {
            let weight = compute_edge_log_weight_with_state(arena, &edge, None);
            graph.add_edge(
                edge.token_in,
                GraphEdge {
                    edge,
                    log_weight: weight,
                },
            );
        }
    }

    for adj in &mut graph.adjacency {
        adj.sort_by(|a, b| {
            a.log_weight
                .partial_cmp(&b.log_weight)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    graph
}

pub fn pool_meta_from_pair(
    pool_index: PoolIndex,
    protocol: ProtocolType,
    token0: TokenIndex,
    token1: TokenIndex,
    fee_bps: Option<u32>,
) -> PoolMeta {
    PoolMeta {
        pool_index,
        protocol,
        tokens: vec![token0, token1],
        fee_bps: fee_bps.unwrap_or(DEFAULT_FEE_BPS),
        token0,
        token1,
        bpt_index: None,
        pool_id: None,
        protocol_label: None,
        router: None,
        hooks: None,
        tick_spacing: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolState, V2PoolState};
    use alloy::primitives::Address;
    use ruint::aliases::U256;

    #[test]
    fn builds_two_pool_graph() {
        let mut arena = StateArena::new();
        let t0 = arena.register_token(Address::repeat_byte(1));
        let t1 = arena.register_token(Address::repeat_byte(2));
        let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
        let p = arena.register_pool(
            Address::repeat_byte(0x10),
            PoolState::V2(V2PoolState {
                reserve0: reserve,
                reserve1: reserve,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );

        let pools = vec![pool_meta_from_pair(
            p,
            ProtocolType::UniswapV2,
            t0,
            t1,
            Some(30),
        )];
        let graph = build_graph(&arena, &pools);
        assert_eq!(graph.adjacency[t0.0 as usize].len(), 1);
        assert_eq!(graph.adjacency[t1.0 as usize].len(), 1);
    }
}
