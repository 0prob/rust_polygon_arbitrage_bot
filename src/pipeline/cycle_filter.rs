use alloy::primitives::Address;
use alloy::primitives::U256;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHasher};

use crate::core::types::{Edge, FoundCycle, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::simulate_route_minimal;
use crate::pipeline::sim_sanity::min_economic_amount_in;
use crate::pipeline::types::compare_cycle_score;

#[must_use]
pub fn graph_negative_rescue_cap(max_keep: usize) -> usize {
    (max_keep / 8).clamp(4, 16).min(max_keep)
}

/// Token metadata for per-cycle probe sizing during atomic prefilter.
pub struct ProbeContext<'a> {
    pub token_to_matic_rates: Option<&'a FxHashMap<TokenIndex, U256>>,
    pub token_decimals: Option<&'a FxHashMap<Address, u8>>,
}

fn probe_amount_for_cycle(
    arena: &StateArena,
    cycle: &FoundCycle,
    ctx: Option<&ProbeContext<'_>>,
) -> U256 {
    let mut decimals = 18;
    let mut rate = U256::ZERO;
    if let Some(c) = ctx {
        if let Some(token_address) = arena.token_address(cycle.start_token)
            && let Some(dec_map) = c.token_decimals
            && let Some(&dec) = dec_map.get(&token_address)
        {
            decimals = dec;
        }
        if let Some(rate_map) = c.token_to_matic_rates
            && let Some(&r) = rate_map.get(&cycle.start_token)
        {
            rate = r;
        }
    }
    min_economic_amount_in(decimals, rate)
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
    cycles.sort_unstable_by(compare_cycle_score);
    let rescue_cap = graph_negative_rescue_cap(max_keep);
    let sim_candidates = cycles.len().min(
        max_keep
            .saturating_mul(2)
            .max(max_keep.saturating_add(rescue_cap.saturating_mul(4))),
    );
    let mut missing_state_rescued = 0usize;
    let mut survivors: Vec<FoundCycle> = Vec::with_capacity(max_keep);
    for cycle in cycles.into_iter().take(sim_candidates) {
        if !is_fully_simulable_route(&cycle.edges) {
            continue;
        }
        let probe_amount = probe_amount_for_cycle(arena, &cycle, ctx);
        let probe = simulate_route_minimal(arena, &cycle.edges, probe_amount);
        let keep = match &probe {
            Some(sim) => sim.profit > U256::ZERO,
            None if cycle.score < 0.0 => {
                if missing_state_rescued >= rescue_cap {
                    false
                } else {
                    missing_state_rescued += 1;
                    true
                }
            }
            None => false,
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

#[inline]
#[must_use]
pub fn cycle_key(edges: &[Edge]) -> u64 {
    use std::hash::Hasher;
    let mut h = FxHasher::default();
    for edge in edges {
        h.write_u32(edge.pool_index.0);
        h.write_u32(edge.token_in.0);
        h.write_u32(edge.token_out.0);
        h.write_u8(edge.token_in_idx);
        h.write_u8(edge.token_out_idx);
    }
    h.finish()
}

/// Deduplicate by cycle key, keeping the best-scored variant.
pub fn dedupe_cycles_by_edges(cycles: Vec<FoundCycle>) -> Vec<FoundCycle> {
    let cap = cycles.len();
    let mut best: FxHashMap<u64, Vec<FoundCycle>> =
        FxHashMap::with_capacity_and_hasher(cap, FxBuildHasher);
    for cycle in cycles {
        let key = cycle_key(&cycle.edges);
        let bucket = best.entry(key).or_default();
        if let Some(existing) = bucket
            .iter_mut()
            .find(|existing| existing.edges == cycle.edges)
        {
            if cycle.score < existing.score {
                *existing = cycle;
            }
        } else {
            bucket.push(cycle);
        }
    }
    let mut out: Vec<FoundCycle> = best.into_values().flatten().collect();
    out.sort_unstable_by(compare_cycle_score);
    out
}

/// True when every hop uses a protocol family we can simulate on-chain.
#[must_use]
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
    use crate::core::types::{CycleEdges, PoolIndex, ProtocolType};

    fn cycle(score: f64, reverse: bool) -> FoundCycle {
        let (token_in, token_out) = if reverse {
            (TokenIndex(1), TokenIndex(0))
        } else {
            (TokenIndex(0), TokenIndex(1))
        };
        FoundCycle {
            start_token: token_in,
            edges: CycleEdges::from_slice(&[Edge {
                pool_index: PoolIndex(7),
                token_in,
                token_out,
                token_in_idx: reverse as u8,
                token_out_idx: (!reverse) as u8,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: !reverse,
            }]),
            hop_count: 1,
            log_weight: score,
            cumulative_fee_bps: 30,
            score,
        }
    }

    #[test]
    fn ordered_cycle_key_preserves_direction() {
        assert_ne!(
            cycle_key(&cycle(-0.1, false).edges),
            cycle_key(&cycle(-0.1, true).edges)
        );
    }

    #[test]
    fn dedupe_keeps_lowest_score() {
        let kept = dedupe_cycles_by_edges(vec![cycle(-0.1, false), cycle(-0.2, false)]);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].score, -0.2);
    }

    #[test]
    fn dedupe_keeps_distinct_cycles_even_if_key_collides() {
        let mut a = cycle(-0.1, false);
        let mut b = cycle(-0.2, false);
        a.edges[0].token_out = TokenIndex(2);
        b.edges[0].token_out = TokenIndex(3);
        assert_ne!(cycle_key(&a.edges), cycle_key(&b.edges));
        let kept = dedupe_cycles_by_edges(vec![a.clone(), b.clone(), a]);
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].score, -0.2);
        assert_eq!(kept[1].score, -0.1);
    }

    #[test]
    fn equal_scores_have_deterministic_route_order() {
        let forward = cycle(-0.1, false);
        let reverse = cycle(-0.1, true);
        let first = dedupe_cycles_by_edges(vec![reverse.clone(), forward.clone()]);
        let second = dedupe_cycles_by_edges(vec![forward, reverse]);
        assert_eq!(cycle_key(&first[0].edges), cycle_key(&second[0].edges));
        assert_eq!(cycle_key(&first[1].edges), cycle_key(&second[1].edges));
    }
}
