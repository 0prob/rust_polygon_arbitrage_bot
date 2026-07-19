use alloy::primitives::Address;
use alloy::primitives::U256;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHasher};

use crate::core::math::fixed_point::ONE;
use crate::core::types::{Edge, FoundCycle, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::local_sim::{precompute_route_shallow_caps, simulate_route_minimal_with_caps};
use crate::pipeline::sim_sanity::min_economic_amount_in;
use crate::pipeline::spot_price::spot_probe_for_decimals;
use crate::pipeline::types::{compare_cycle_score, cycle_prefers_candidate};
use crate::services::execution::flash_liquidity::rotate_cycle_to_start;
use crate::services::oracle::has_reliable_matic_rate;

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
    if cycle.cycle_ratio > ONE {
        return true;
    }
    // Pre-rescore / cache paths may only have graph f64 score.
    cycle.cycle_ratio.is_zero() && cycle.score < 0.0
}

/// Drop cycles with no oracle-priced start; rotate to the first priced hop when possible.
pub fn retain_cycles_with_priced_start(
    cycles: &mut Vec<FoundCycle>,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
) {
    if token_to_matic_rates.is_empty() {
        return;
    }
    cycles.retain_mut(|cycle| normalize_cycle_to_priced_start(cycle, token_to_matic_rates));
}

#[must_use]
fn normalize_cycle_to_priced_start(
    cycle: &mut FoundCycle,
    token_to_matic_rates: &FxHashMap<TokenIndex, U256>,
) -> bool {
    if has_reliable_matic_rate(cycle.start_token, token_to_matic_rates) {
        return true;
    }
    for edge in &cycle.edges {
        if !has_reliable_matic_rate(edge.token_in, token_to_matic_rates) {
            continue;
        }
        if let Some(rotated) = rotate_cycle_to_start(cycle, edge.token_in) {
            *cycle = rotated;
            return true;
        }
    }
    false
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
    let mut rate = U256::ZERO;
    let decimals = if let Some(c) = ctx.and_then(|c| c.token_decimals) {
        crate::services::oracle::resolve_token_decimals_for_index(cycle.start_token, arena, c)
    } else {
        arena.token_decimals(cycle.start_token)
    };
    if let Some(c) = ctx
        && let Some(rate_map) = c.token_to_matic_rates
    {
        // Same MIN floor as has_reliable_matic_rate — sub-dust entries must not
        // inflate min_economic_amount_in in the LF prefilter.
        rate = crate::services::oracle::resolve_token_to_matic_rate_or_bootstrap(
            cycle.start_token,
            rate_map,
        )
        .unwrap_or(U256::ZERO);
    }
    (min_economic_amount_in(decimals, rate), rate, decimals)
}

#[derive(Debug, Clone, Default)]
pub struct PrefilterDiagnostics {
    pub merged_in: usize,
    pub pruned_non_simulable: usize,
    pub pruned_protocol_mismatch: usize,
    pub pruned_spot_flat: usize,
    pub pruned_executor_budget: usize,
    pub simulable: usize,
    pub sim_window: usize,
    pub profit_keep: usize,
    pub gas_rescue: usize,
    pub spot_rescue: usize,
    pub rescue_cap_drop: usize,
    pub out: usize,
}

impl PrefilterDiagnostics {
    pub fn log_summary(&self) {
        if self.merged_in == 0 {
            return;
        }
        crate::info!(
            "cycle prefilter: in={} pruned_sim={} pruned_proto={} pruned_spot={} pruned_executor={} simulable={} window={} \
             keep={} gas_rescue={} spot_rescue={} rescue_cap_drop={} out={}",
            self.merged_in,
            self.pruned_non_simulable,
            self.pruned_protocol_mismatch,
            self.pruned_spot_flat,
            self.pruned_executor_budget,
            self.simulable,
            self.sim_window,
            self.profit_keep,
            self.gas_rescue,
            self.spot_rescue,
            self.rescue_cap_drop,
            self.out,
        );
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PrefilterVerdict {
    Keep,
    RescueGas,
    RescueSpot,
}

/// Atomic sim verdict — caller must already require simulable + spot-negative routes.
fn atomic_sim_verdict_for_cycle(
    arena: &StateArena,
    cycle: &FoundCycle,
    ctx: Option<&ProbeContext<'_>>,
) -> PrefilterVerdict {
    let (probe_amount, rate, decimals) = probe_context_for_cycle(arena, cycle, ctx);
    // One shallow-cap table for both probe sizes (avoids dual CL probe rebuild).
    let shallow_caps = precompute_route_shallow_caps(arena, &cycle.edges);
    let caps = shallow_caps.as_ref();
    let mut probe = simulate_route_minimal_with_caps(arena, &cycle.edges, probe_amount, caps);
    if probe.as_ref().is_none_or(|s| s.profit.is_zero()) {
        let spot = spot_probe_for_decimals(decimals);
        if spot != probe_amount && !spot.is_zero() {
            probe = simulate_route_minimal_with_caps(arena, &cycle.edges, spot, caps);
        }
    }
    match &probe {
        Some(sim) if sim.profit > U256::ZERO => {
            if let Some(c) = ctx {
                if let Some(gas_price) = c.gas_price_wei {
                    if probe_beats_gas_floor(sim, rate, decimals, gas_price) {
                        PrefilterVerdict::Keep
                    } else {
                        // Dust probe profit may not cover gas; Brent can find size.
                        PrefilterVerdict::RescueGas
                    }
                } else {
                    PrefilterVerdict::Keep
                }
            } else {
                PrefilterVerdict::Keep
            }
        }
        // Simulable at probe size but zero profit, or sim failed: Brent rescue when spot-negative.
        Some(_) | None => PrefilterVerdict::RescueSpot,
    }
}

pub fn prefilter_cycles_by_atomic_sim_with_context(
    arena: &StateArena,
    cycles: Vec<FoundCycle>,
    max_keep: usize,
    ctx: Option<&ProbeContext<'_>>,
) -> (Vec<FoundCycle>, PrefilterDiagnostics) {
    prefilter_cycles_by_atomic_sim_with_context_and_diag(arena, cycles, max_keep, ctx)
}

pub fn prefilter_cycles_by_atomic_sim_with_context_and_diag(
    arena: &StateArena,
    cycles: Vec<FoundCycle>,
    max_keep: usize,
    ctx: Option<&ProbeContext<'_>>,
) -> (Vec<FoundCycle>, PrefilterDiagnostics) {
    let merged_in = cycles.len();
    let mut diag = PrefilterDiagnostics {
        merged_in,
        ..PrefilterDiagnostics::default()
    };
    if max_keep == 0 || merged_in == 0 {
        return (Vec::new(), diag);
    }

    let mut simulable: Vec<FoundCycle> = Vec::with_capacity(merged_in);
    for c in cycles {
        if !is_fully_simulable_route(&c.edges) {
            diag.pruned_non_simulable += 1;
            continue;
        }
        if !crate::pipeline::local_sim::cycle_edges_match_arena_state(arena, &c.edges) {
            diag.pruned_protocol_mismatch += 1;
            continue;
        }
        if !cycle_spot_negative(&c) {
            diag.pruned_spot_flat += 1;
            continue;
        }
        if !crate::pipeline::route_calls::route_fits_executor(&c.edges) {
            diag.pruned_executor_budget += 1;
            continue;
        }
        simulable.push(c);
    }
    diag.simulable = simulable.len();
    if simulable.is_empty() {
        return (Vec::new(), diag);
    }

    simulable.sort_unstable_by(compare_cycle_score);
    let rescue_cap = max_keep;
    let sim_window = simulable.len().min(max_keep.saturating_add(rescue_cap));
    diag.sim_window = sim_window;
    let candidates: Vec<FoundCycle> = simulable.into_iter().take(sim_window).collect();
    let verdicts: Vec<PrefilterVerdict> = if crate::util::should_use_rayon(candidates.len()) {
        candidates
            .par_iter()
            .map(|cycle| atomic_sim_verdict_for_cycle(arena, cycle, ctx))
            .collect()
    } else {
        candidates
            .iter()
            .map(|cycle| atomic_sim_verdict_for_cycle(arena, cycle, ctx))
            .collect()
    };

    let mut rescue_used = 0usize;
    let mut survivors: Vec<FoundCycle> = Vec::with_capacity(max_keep);
    for (cycle, verdict) in candidates.into_iter().zip(verdicts) {
        let keep = match verdict {
            PrefilterVerdict::Keep => {
                diag.profit_keep += 1;
                true
            }
            PrefilterVerdict::RescueGas => {
                if rescue_used >= rescue_cap {
                    diag.rescue_cap_drop += 1;
                    false
                } else {
                    rescue_used += 1;
                    diag.gas_rescue += 1;
                    true
                }
            }
            PrefilterVerdict::RescueSpot => {
                if rescue_used >= rescue_cap {
                    diag.rescue_cap_drop += 1;
                    false
                } else {
                    rescue_used += 1;
                    diag.spot_rescue += 1;
                    true
                }
            }
        };
        if keep {
            survivors.push(cycle);
            if survivors.len() >= max_keep {
                break;
            }
        }
    }
    diag.out = survivors.len();
    (survivors, diag)
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
    let cycles = cycles.into_iter();
    let (lower, upper) = cycles.size_hint();
    let mut best: FxHashMap<u64, FoundCycle> = FxHashMap::default();
    best.reserve(upper.unwrap_or(lower));
    for cycle in cycles {
        let key = cycle_key(&cycle.edges);
        match best.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                if cycle_prefers_candidate(&cycle, e.get()) {
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
    use crate::pipeline::local_sim::simulate_route_minimal;

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

    fn v2_pool(reserve0: U256, reserve1: U256) -> std::sync::Arc<crate::core::types::PoolState> {
        std::sync::Arc::new(crate::core::types::PoolState::V2(
            crate::core::types::V2PoolState {
                reserve0,
                reserve1,
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 1,
            },
        ))
    }

    #[test]
    fn retain_cycles_drops_unpriced_and_keeps_priced_start() {
        use crate::core::constants::MIN_TOKEN_TO_MATIC_RATE;

        let mut rates = FxHashMap::default();
        rates.insert(TokenIndex(0), MIN_TOKEN_TO_MATIC_RATE);
        let mut cycles = vec![cycle(1.0, false)];
        retain_cycles_with_priced_start(&mut cycles, &rates);
        assert_eq!(cycles.len(), 1);

        rates.clear();
        let mut deferred = vec![cycle(1.0, false)];
        retain_cycles_with_priced_start(&mut deferred, &rates);
        assert_eq!(
            deferred.len(),
            1,
            "empty rate map defers filtering until oracle rates load"
        );

        rates.insert(TokenIndex(1), MIN_TOKEN_TO_MATIC_RATE);
        let mut unpriced = vec![cycle(1.0, false)];
        retain_cycles_with_priced_start(&mut unpriced, &rates);
        assert!(unpriced.is_empty());
    }

    #[test]
    fn priced_filter_after_enrich_keeps_cycle_for_newly_mapped_start() {
        use crate::core::constants::MIN_TOKEN_TO_MATIC_RATE;

        let mut prior = FxHashMap::default();
        prior.insert(TokenIndex(1), MIN_TOKEN_TO_MATIC_RATE);
        let mut cycles = vec![cycle(1.0, false)];
        retain_cycles_with_priced_start(&mut cycles, &prior);
        assert!(
            cycles.is_empty(),
            "start token 0 unpriced in prior map drops cycle"
        );

        let mut merged = FxHashMap::default();
        merged.insert(TokenIndex(0), MIN_TOKEN_TO_MATIC_RATE);
        merged.insert(TokenIndex(1), MIN_TOKEN_TO_MATIC_RATE);
        let mut restored = vec![cycle(1.0, false)];
        retain_cycles_with_priced_start(&mut restored, &merged);
        assert_eq!(restored.len(), 1, "post-enrich merged rates retain cycle");
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
    fn dedupe_prefers_higher_cycle_ratio_when_scores_tie() {
        let mut better = cycle(-0.1, false);
        better.cycle_ratio = ONE + ONE / U256::from(50u64);
        let expected_ratio = better.cycle_ratio;
        let mut worse = cycle(-0.1, false);
        worse.cycle_ratio = ONE + ONE / U256::from(100u64);
        let kept = dedupe_cycles_by_edges(vec![worse, better]);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].cycle_ratio, expected_ratio);
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
    fn prefilter_prunes_spot_flat_before_sim() {
        // The arena-state gate runs before the spot-flat prune; PoolIndex(7) needs
        // matching V2 state or the cycle dies as protocol_mismatch instead.
        let mut arena = StateArena::default();
        for id in 0u8..8 {
            arena.register_pool(Address::from([id + 1; 20]), v2_pool(ONE, ONE));
        }
        let mut flat = cycle(-0.1, false);
        flat.cycle_ratio = ONE;
        flat.score = 0.1;
        let (_, diag) =
            prefilter_cycles_by_atomic_sim_with_context_and_diag(&arena, vec![flat], 8, None);
        assert_eq!(diag.pruned_spot_flat, 1);
        assert_eq!(diag.sim_window, 0);
    }

    #[test]
    fn rescue_cap_scales_with_enumeration_budget() {
        assert_eq!(graph_negative_rescue_cap(650), 162);
        assert_eq!(graph_negative_rescue_cap(32), 32);
        assert_eq!(graph_negative_rescue_cap(0), 0);
    }

    #[test]
    fn spot_negative_cycles_fill_the_configured_route_budget() {
        let mut arena = StateArena::default();
        let a = arena.register_token(Address::from([1u8; 20]));
        let b = arena.register_token(Address::from([2u8; 20]));
        let deep = crate::core::constants::MIN_HOP_TOKEN_BALANCE * U256::from(1100u64);
        let shallow = crate::core::constants::MIN_HOP_TOKEN_BALANCE * U256::from(900u64);
        let pool = arena.register_pool(Address::from([3u8; 20]), v2_pool(deep, shallow));
        let pool2 = arena.register_pool(Address::from([4u8; 20]), v2_pool(shallow, deep));
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

        let (kept, diag) = prefilter_cycles_by_atomic_sim_with_context_and_diag(
            &arena,
            std::iter::repeat_n(graph_negative.clone(), 33).collect(),
            33,
            None,
        );
        assert_eq!(kept.len(), 33, "spot-negative cycles should be rescued");
        assert_eq!(kept[0].cycle_ratio, graph_negative.cycle_ratio);
        assert_eq!(
            diag.rescue_cap_drop, 0,
            "route budget should not pre-drop rescues"
        );
    }
}
