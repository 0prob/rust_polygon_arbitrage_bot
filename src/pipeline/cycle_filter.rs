use crate::core::types::{Edge, FoundCycle};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::simulate_route_minimal;
use crate::pipeline::spot_price::SPOT_PROBE;
use crate::pipeline::types::{compare_cycle_score, route_fingerprint};

/// Drop cycles that fail an atomic round-trip probe simulation (fast unprofitability filter).
pub fn prefilter_cycles_by_atomic_sim(
    arena: &StateArena,
    cycles: Vec<FoundCycle>,
    max_keep: usize,
) -> Vec<FoundCycle> {
    if cycles.is_empty() {
        return cycles;
    }
    let mut survivors: Vec<FoundCycle> = Vec::with_capacity(cycles.len().min(max_keep));
    for cycle in cycles {
        let profitable = simulate_route_minimal(arena, &cycle.edges, SPOT_PROBE)
            .map(|sim| sim.profit > ruint::aliases::U256::ZERO)
            .unwrap_or(false);
        if profitable || cycle.score < 0.0 {
            survivors.push(cycle);
        }
    }
    if survivors.len() > max_keep {
        survivors.sort_by(compare_cycle_score);
        survivors.truncate(max_keep);
    }
    survivors
}

/// Deduplicate by route fingerprint, keeping the best-scored variant.
pub fn dedupe_cycles_by_fingerprint(cycles: Vec<FoundCycle>) -> Vec<FoundCycle> {
    let mut best: rustc_hash::FxHashMap<u64, FoundCycle> = rustc_hash::FxHashMap::default();
    for cycle in cycles {
        let key = route_fingerprint(&cycle.edges);
        match best.get(&key) {
            Some(existing) if existing.score <= cycle.score => {}
            _ => {
                best.insert(key, cycle);
            }
        }
    }
    let mut out: Vec<FoundCycle> = best.into_values().collect();
    out.sort_by(compare_cycle_score);
    out
}

/// True when every hop uses a protocol family we can simulate on-chain.
pub fn is_fully_simulable_route(edges: &[Edge]) -> bool {
    use crate::core::types::ProtocolType;
    edges.iter().all(|e| {
        matches!(
            e.protocol,
            ProtocolType::UniswapV2
                | ProtocolType::UniswapV3
                | ProtocolType::UniswapV4
                | ProtocolType::BalancerV2
                | ProtocolType::CurveStable
                | ProtocolType::CurveCrypto
                | ProtocolType::Dodo
                | ProtocolType::Woofi
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolIndex, PoolState, ProtocolType, TokenIndex, V2PoolState};
    use alloy::primitives::Address;
    use ruint::aliases::U256;

    fn v2_pool(reserve: U256) -> PoolState {
        PoolState::V2(V2PoolState {
            reserve0: reserve,
            reserve1: reserve * U256::from(2u8),
            fee: U256::ZERO,
            fee_denominator: U256::ZERO,
        })
    }

    #[test]
    fn atomic_prefilter_keeps_mispriced_triangle() {
        let mut arena = StateArena::new();
        let t0 = arena.register_token(Address::repeat_byte(1));
        let t1 = arena.register_token(Address::repeat_byte(2));
        let t2 = arena.register_token(Address::repeat_byte(3));
        let reserve = U256::from(1_000_000u128) * U256::from(10u128).pow(U256::from(18));
        let p01 = arena.register_pool(Address::repeat_byte(0x10), v2_pool(reserve));
        let p12 = arena.register_pool(Address::repeat_byte(0x11), v2_pool(reserve));
        let p20 = arena.register_pool(
            Address::repeat_byte(0x12),
            PoolState::V2(V2PoolState {
                reserve0: reserve * U256::from(2u8),
                reserve1: reserve,
                fee: U256::ZERO,
                fee_denominator: U256::ZERO,
            }),
        );
        let edges = vec![
            Edge {
                pool_index: p01,
                token_in: t0,
                token_out: t1,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: p12,
                token_in: t1,
                token_out: t2,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: p20,
                token_in: t2,
                token_out: t0,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
        ];
        let cycle = FoundCycle {
            start_token: t0,
            edges: edges.into(),
            hop_count: 3,
            log_weight: -0.1,
            cumulative_fee_bps: 90,
            score: -0.1,
        };
        let kept = prefilter_cycles_by_atomic_sim(&arena, vec![cycle], 10);
        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn dedupe_keeps_best_score() {
        let mk = |score: f64, pool: u32| FoundCycle {
            start_token: TokenIndex(0),
            edges: vec![Edge {
                pool_index: PoolIndex(pool),
                token_in: TokenIndex(0),
                token_out: TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            }]
            .into(),
            hop_count: 1,
            log_weight: score,
            cumulative_fee_bps: 30,
            score,
        };
        let out = dedupe_cycles_by_fingerprint(vec![mk(0.5, 1), mk(-0.2, 1)]);
        assert_eq!(out.len(), 1);
        assert!(out[0].score < 0.0);
    }
}
