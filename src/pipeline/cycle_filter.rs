use std::collections::HashMap;

use alloy::primitives::Address;
use ruint::aliases::U256;
use rustc_hash::{FxBuildHasher, FxHashMap};

use crate::core::types::{Edge, FoundCycle, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::simulate_route_minimal;
use crate::pipeline::sim_sanity::profit_probe_amount;
use crate::pipeline::types::{MinimalSimResult, compare_cycle_score, route_fingerprint};

/// Reserved slots for graph-negative cycles with genuinely missing pool state.
pub fn graph_negative_rescue_cap(max_keep: usize) -> usize {
    if max_keep == 0 {
        return 0;
    }
    (max_keep / 8).clamp(4, 16).min(max_keep)
}

/// Why an atomic prefilter probe succeeded or failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtomicPrefilterOutcome {
    ProfitableAtProbe,
    /// Graph-negative cycle rescued because pool state was unavailable for simulation.
    MissingPoolStateRescue,
    UnprofitableAtProbe,
    MissingPoolState,
}

/// Classify a dust-probe simulation result for a cycle.
pub fn classify_atomic_probe(
    cycle: &FoundCycle,
    probe: &Option<MinimalSimResult>,
) -> AtomicPrefilterOutcome {
    match probe {
        Some(sim) if sim.profit > U256::ZERO => AtomicPrefilterOutcome::ProfitableAtProbe,
        Some(_) => AtomicPrefilterOutcome::UnprofitableAtProbe,
        None if cycle.score < 0.0 => AtomicPrefilterOutcome::MissingPoolStateRescue,
        None => AtomicPrefilterOutcome::MissingPoolState,
    }
}

/// Token metadata for per-cycle probe sizing during atomic prefilter.
pub struct ProbeContext<'a> {
    pub token_to_matic_rates: Option<&'a FxHashMap<TokenIndex, U256>>,
    pub token_decimals: Option<&'a HashMap<Address, u8>>,
}

fn probe_amount_for_cycle(
    arena: &StateArena,
    cycle: &FoundCycle,
    ctx: Option<&ProbeContext<'_>>,
) -> U256 {
    let decimals = arena
        .token_address(cycle.start_token)
        .and_then(|a| {
            ctx.and_then(|c| c.token_decimals)
                .and_then(|m| m.get(&a).copied())
        })
        .unwrap_or(18);
    let rate = ctx
        .and_then(|c| c.token_to_matic_rates)
        .and_then(|m| m.get(&cycle.start_token).copied())
        .unwrap_or(U256::ZERO);
    profit_probe_amount(decimals, rate)
}

/// Drop cycles that fail an atomic round-trip probe simulation (fast unprofitability filter).
/// Sorts by score first so the most promising cycles are simulated early, and stops
/// early once `max_keep` survivors are found among the top candidates.
pub fn prefilter_cycles_by_atomic_sim(
    arena: &StateArena,
    cycles: Vec<FoundCycle>,
    max_keep: usize,
) -> Vec<FoundCycle> {
    prefilter_cycles_by_atomic_sim_with_context(arena, cycles, max_keep, None)
}

pub fn prefilter_cycles_by_atomic_sim_with_context(
    arena: &StateArena,
    cycles: Vec<FoundCycle>,
    max_keep: usize,
    ctx: Option<&ProbeContext<'_>>,
) -> Vec<FoundCycle> {
    let mut cycles = cycles;
    if cycles.is_empty() {
        return cycles;
    }
    cycles.sort_by(compare_cycle_score);
    let sim_candidates = cycles
        .len()
        .min(max_keep.saturating_mul(3).max(max_keep + 100));
    let rescue_cap = graph_negative_rescue_cap(max_keep);
    let mut missing_state_rescued = 0usize;
    let mut survivors: Vec<FoundCycle> = Vec::with_capacity(max_keep);
    for cycle in cycles.drain(..sim_candidates) {
        let probe_amount = probe_amount_for_cycle(arena, &cycle, ctx);
        let probe = simulate_route_minimal(arena, &cycle.edges, probe_amount);
        let keep = match classify_atomic_probe(&cycle, &probe) {
            AtomicPrefilterOutcome::ProfitableAtProbe => true,
            AtomicPrefilterOutcome::MissingPoolStateRescue => {
                if missing_state_rescued >= rescue_cap {
                    false
                } else {
                    missing_state_rescued += 1;
                    true
                }
            }
            AtomicPrefilterOutcome::UnprofitableAtProbe | AtomicPrefilterOutcome::MissingPoolState => {
                false
            }
        };
        if keep {
            survivors.push(cycle);
            if survivors.len() >= max_keep {
                break;
            }
        }
    }
    survivors
}

/// Deduplicate by route fingerprint, keeping the best-scored variant.
pub fn dedupe_cycles_by_fingerprint(cycles: Vec<FoundCycle>) -> Vec<FoundCycle> {
    let cap = cycles.len();
    let mut best: rustc_hash::FxHashMap<u64, FoundCycle> =
        rustc_hash::FxHashMap::with_capacity_and_hasher(cap, FxBuildHasher);
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
    fn prefilter_drops_flat_probe_even_when_graph_negative() {
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
        let edges = vec![
            Edge {
                pool_index: p,
                token_in: t0,
                token_out: t1,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: p,
                token_in: t1,
                token_out: t0,
                token_in_idx: 1,
                token_out_idx: 0,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
        ];
        let cycle = FoundCycle {
            start_token: t0,
            edges: edges.into(),
            hop_count: 2,
            log_weight: -0.5,
            cumulative_fee_bps: 60,
            score: -0.5,
        };
        let kept = prefilter_cycles_by_atomic_sim(&arena, vec![cycle], 10);
        assert!(
            kept.is_empty(),
            "flat probe with loaded state should not be rescued by graph score"
        );
    }

    #[test]
    fn classify_missing_state_rescue_only_for_graph_negative() {
        let cycle = FoundCycle {
            start_token: TokenIndex(0),
            edges: vec![].into(),
            hop_count: 0,
            log_weight: -1.0,
            cumulative_fee_bps: 0,
            score: -1.0,
        };
        assert_eq!(
            classify_atomic_probe(&cycle, &None),
            AtomicPrefilterOutcome::MissingPoolStateRescue
        );
        let pos = FoundCycle {
            score: 0.1,
            ..cycle
        };
        assert_eq!(
            classify_atomic_probe(&pos, &None),
            AtomicPrefilterOutcome::MissingPoolState
        );
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
