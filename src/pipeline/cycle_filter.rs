use alloy::primitives::Address;
use alloy::primitives::U256;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHasher};

use crate::core::math::fixed_point::ONE;
use crate::core::types::{Edge, FoundCycle, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::simulate_route_minimal;
use crate::pipeline::sim_sanity::min_economic_amount_in;
use crate::pipeline::types::compare_cycle_score;

#[must_use]
pub fn graph_negative_rescue_cap(max_keep: usize) -> usize {
    if max_keep == 0 {
        return 0;
    }
    // Spot-profitable routes often fail dust probes; reserve ~25% for Brent rescue.
    (max_keep / 4).clamp(32, 256).min(max_keep)
}

#[inline]
fn cycle_spot_negative(cycle: &FoundCycle) -> bool {
    cycle.cycle_ratio > ONE || (cycle.cycle_ratio.is_zero() && cycle.score < 0.0)
}

/// Token metadata for per-cycle probe sizing during atomic prefilter.
pub struct ProbeContext<'a> {
    pub token_to_matic_rates: Option<&'a FxHashMap<TokenIndex, U256>>,
    pub token_decimals: Option<&'a FxHashMap<Address, u8>>,
    pub gas_price_wei: Option<U256>,
}

#[must_use]
fn probe_beats_gas_floor(
    sim: &crate::pipeline::types::MinimalSimResult,
    rate: U256,
    decimals: u8,
    gas_price: U256,
) -> bool {
    if rate < crate::core::constants::MIN_TOKEN_TO_MATIC_RATE {
        return false;
    }
    let scale = crate::util::ten_pow_u256_cached(decimals);
    let profit_matic = sim.profit.saturating_mul(rate) / scale;
    let gas_matic = U256::from(sim.total_gas).saturating_mul(gas_price);
    profit_matic > gas_matic
}

fn probe_context_for_cycle(
    arena: &StateArena,
    cycle: &FoundCycle,
    ctx: Option<&ProbeContext<'_>>,
) -> (U256, U256, u8) {
    let mut decimals = arena.token_decimals(cycle.start_token);
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
    (min_economic_amount_in(decimals, rate), rate, decimals)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PrefilterVerdict {
    Keep,
    Rescue,
    Reject,
}

fn prefilter_verdict_for_cycle(
    arena: &StateArena,
    cycle: &FoundCycle,
    ctx: Option<&ProbeContext<'_>>,
) -> PrefilterVerdict {
    if !is_fully_simulable_route(&cycle.edges) {
        return PrefilterVerdict::Reject;
    }
    if !cycle_spot_negative(cycle) {
        return PrefilterVerdict::Reject;
    }
    let (probe_amount, rate, decimals) = probe_context_for_cycle(arena, cycle, ctx);
    let probe = simulate_route_minimal(arena, &cycle.edges, probe_amount);
    match &probe {
        Some(sim) if sim.profit > U256::ZERO => {
            if let Some(c) = ctx {
                if let Some(gas_price) = c.gas_price_wei {
                    if probe_beats_gas_floor(sim, rate, decimals, gas_price) {
                        PrefilterVerdict::Keep
                    } else if cycle_spot_negative(cycle) {
                        // Dust probe profit may not cover gas; Brent can find size.
                        PrefilterVerdict::Rescue
                    } else {
                        PrefilterVerdict::Reject
                    }
                } else {
                    PrefilterVerdict::Keep
                }
            } else {
                PrefilterVerdict::Keep
            }
        }
        // Simulable at probe size but zero profit, or sim failed: rescue when the
        // graph still shows a spot-negative cycle (cycle_ratio / score).
        Some(_) | None if cycle_spot_negative(cycle) => PrefilterVerdict::Rescue,
        Some(_) | None => PrefilterVerdict::Reject,
    }
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
    let sim_candidates = cycles.len().min(max_keep.saturating_add(rescue_cap));
    let candidates: Vec<FoundCycle> = cycles.into_iter().take(sim_candidates).collect();
    let verdicts: Vec<PrefilterVerdict> = candidates
        .par_iter()
        .map(|cycle| prefilter_verdict_for_cycle(arena, cycle, ctx))
        .collect();

    let mut missing_state_rescued = 0usize;
    let mut survivors: Vec<FoundCycle> = Vec::with_capacity(max_keep);
    for (cycle, verdict) in candidates.into_iter().zip(verdicts) {
        let keep = match verdict {
            PrefilterVerdict::Keep => true,
            PrefilterVerdict::Rescue => {
                if missing_state_rescued >= rescue_cap {
                    false
                } else {
                    missing_state_rescued += 1;
                    true
                }
            }
            PrefilterVerdict::Reject => false,
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
fn hash_edge_hop(h: &mut FxHasher, edge: &Edge) {
    use std::hash::Hasher;
    h.write_u32(edge.pool_index.0);
    h.write_u32(edge.token_in.0);
    h.write_u32(edge.token_out.0);
    h.write_u8(edge.token_in_idx);
    h.write_u8(edge.token_out_idx);
}

/// Stable identity for one directed hop (pool + graph tokens + pool-local indices).
#[inline]
#[must_use]
pub fn edge_hop_key(edge: &Edge) -> u64 {
    use std::hash::Hasher;
    let mut h = FxHasher::default();
    hash_edge_hop(&mut h, edge);
    h.finish()
}

/// Route fingerprint: ordered directed hops hashed with full pool-local direction.
#[inline]
#[must_use]
pub fn cycle_key(edges: &[Edge]) -> u64 {
    use std::hash::Hasher;
    let mut h = FxHasher::default();
    for edge in edges {
        hash_edge_hop(&mut h, edge);
    }
    h.finish()
}

/// Deduplicate by cycle key, keeping the best-scored variant.
///
/// Accepts any `IntoIterator<Item = FoundCycle>` so callers can pass an
/// iterator chain directly without an intermediate `.collect()`.
pub fn dedupe_cycles_by_edges(cycles: impl IntoIterator<Item = FoundCycle>) -> Vec<FoundCycle> {
    let mut best: FxHashMap<u64, FoundCycle> = FxHashMap::default();
    for cycle in cycles {
        let key = cycle_key(&cycle.edges);
        match best.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                if e.get().edges == cycle.edges && cycle.score < e.get().score {
                    *e.get_mut() = cycle;
                }
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(cycle);
            }
        }
    }
    let mut out: Vec<FoundCycle> = best.into_values().collect();
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
            cycle_ratio: U256::ZERO,
        }
    }

    #[test]
    fn edge_hop_key_distinguishes_pool_local_direction() {
        let mut forward = cycle(0.0, false);
        forward.edges[0].token_in_idx = 1;
        forward.edges[0].token_out_idx = 3;
        let mut reverse = forward.clone();
        reverse.edges[0].token_in_idx = 3;
        reverse.edges[0].token_out_idx = 1;
        assert_ne!(
            edge_hop_key(&forward.edges[0]),
            edge_hop_key(&reverse.edges[0])
        );
    }

    #[test]
    fn hash_cycle_edges_matches_cycle_key() {
        use crate::services::execution::candidate::hash_cycle_edges;
        let a = cycle(-0.1, false);
        let b = cycle(-0.2, true);
        assert_eq!(hash_cycle_edges(&a.edges), cycle_key(&a.edges));
        assert_eq!(hash_cycle_edges(&b.edges), cycle_key(&b.edges));
        assert_ne!(hash_cycle_edges(&a.edges), hash_cycle_edges(&b.edges));
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

    #[test]
    fn rescue_cap_scales_with_enumeration_budget() {
        assert_eq!(graph_negative_rescue_cap(650), 162);
        assert_eq!(graph_negative_rescue_cap(32), 32);
        assert_eq!(graph_negative_rescue_cap(0), 0);
    }

    #[test]
    fn spot_negative_cycle_survives_zero_profit_probe_rescue() {
        let mut arena = crate::pipeline::arena::StateArena::default();
        let a = arena.register_token(Address::from([1u8; 20]));
        let b = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            std::sync::Arc::new(crate::core::types::PoolState::V2(
                crate::core::types::V2PoolState {
                    reserve0: crate::core::constants::MIN_HOP_TOKEN_BALANCE * U256::from(1100u64),
                    reserve1: crate::core::constants::MIN_HOP_TOKEN_BALANCE * U256::from(900u64),
                    fee: U256::from(997u64),
                    fee_denominator: U256::from(1_000u64),
                    block_timestamp_last: 0,
                },
            )),
        );
        let pool2 = arena.register_pool(
            Address::from([4u8; 20]),
            std::sync::Arc::new(crate::core::types::PoolState::V2(
                crate::core::types::V2PoolState {
                    reserve0: crate::core::constants::MIN_HOP_TOKEN_BALANCE * U256::from(900u64),
                    reserve1: crate::core::constants::MIN_HOP_TOKEN_BALANCE * U256::from(1100u64),
                    fee: U256::from(997u64),
                    fee_denominator: U256::from(1_000u64),
                    block_timestamp_last: 0,
                },
            )),
        );
        let graph_negative = FoundCycle {
            start_token: a,
            edges: CycleEdges::from_slice(&[
                Edge {
                    pool_index: pool,
                    token_in: a,
                    token_out: b,
                    token_in_idx: 0,
                    token_out_idx: 1,
                    protocol: ProtocolType::UniswapV2,
                    fee_bps: 30,
                    zero_for_one: true,
                },
                Edge {
                    pool_index: pool2,
                    token_in: b,
                    token_out: a,
                    token_in_idx: 1,
                    token_out_idx: 0,
                    protocol: ProtocolType::UniswapV2,
                    fee_bps: 30,
                    zero_for_one: true,
                },
            ]),
            hop_count: 2,
            log_weight: -0.01,
            cumulative_fee_bps: 60,
            score: -0.01,
            cycle_ratio: U256::from(1_001_000_000_000_000_000u64),
        };
        // Tiny probe: simulable but not profitable at dust size.
        let dust = U256::from(1_000u64);
        let probe = simulate_route_minimal(&arena, &graph_negative.edges, dust);
        assert!(
            probe.as_ref().is_none_or(|sim| sim.profit.is_zero()),
            "dust probe should not show profit for marginal 2-hop cycle"
        );

        let kept = prefilter_cycles_by_atomic_sim_with_context(
            &arena,
            vec![graph_negative.clone()],
            8,
            None,
        );
        assert_eq!(kept.len(), 1, "spot-negative cycle should be rescued");
        assert_eq!(kept[0].cycle_ratio, graph_negative.cycle_ratio);
    }
}
