use crate::core::constants::{
    GAS_BALANCER_DIRECT_BATCH, GAS_BALANCER_HOP, GAS_CURVE_HOP, GAS_DODO_HOP, GAS_V2_HOP,
    GAS_V3_BASE, GAS_V4_BASE, GAS_WOOFI_HOP, HOP_CAP_USIZE,
};
use crate::core::math::balancer::simulate_balancer_swap;
use crate::core::math::dodo::get_dodo_amount_out;
use crate::core::math::uniswap_v2::simulate_v2_swap;
use crate::core::math::uniswap_v3::simulate_v3_swap;
use crate::core::math::woofi::get_woofi_amount_out;
use crate::core::types::{
    Edge, PoolState, ProtocolType, RouteSimulationResult, hop_amounts_zeroed,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::curve_sim::curve_hop_amount_out;
use crate::pipeline::spot_price::spot_probe_for_token;
use crate::pipeline::types::MinimalSimResult;
use alloy::primitives::U256;

/// Per-hop gas estimate for route ranking (matches simulation constants).
#[must_use]
pub fn estimate_hop_gas(protocol: ProtocolType) -> u32 {
    match protocol {
        ProtocolType::UniswapV2 => GAS_V2_HOP,
        ProtocolType::UniswapV3 => GAS_V3_BASE,
        ProtocolType::UniswapV4 => GAS_V4_BASE,
        ProtocolType::CurveStable | ProtocolType::CurveCrypto => GAS_CURVE_HOP,
        ProtocolType::BalancerV2 => GAS_BALANCER_HOP,
        ProtocolType::Dodo => GAS_DODO_HOP,
        ProtocolType::Woofi => GAS_WOOFI_HOP,
    }
}

/// Hop gas budget for ranking: one `batchSwap` for pure Balancer direct routes, else per-hop sum.
#[must_use]
pub fn route_hop_gas_budget(edges: &[Edge]) -> u32 {
    if crate::pipeline::route_calls::balancer_direct_batch_eligible(edges) {
        return GAS_BALANCER_DIRECT_BATCH;
    }
    edges.iter().map(|e| estimate_hop_gas(e.protocol)).sum()
}

/// Route gas for ranking: static hop budget vs walked hop gas (V3 tick crosses), whichever is higher.
#[must_use]
fn finalize_route_total_gas(edges: &[Edge], walked_hop_gas: u32) -> u32 {
    let hop_count = edges.len();
    if hop_count == 0 {
        return crate::services::execution::gas::ROUTE_EXECUTION_GAS_OVERHEAD;
    }
    // Direct batchSwap is one vault call; seed is all-in (do not pile ROUTE_EXECUTION_* × edges).
    if crate::pipeline::route_calls::balancer_direct_batch_eligible(edges) {
        return GAS_BALANCER_DIRECT_BATCH;
    }
    let hop_budget = route_hop_gas_budget(edges);
    let static_gas = crate::services::execution::gas::estimate_route_gas_from_hops_evm(
        hop_budget,
        hop_count,
        hop_count as u32,
    );
    if walked_hop_gas == 0 {
        return static_gas;
    }
    let dynamic =
        crate::services::execution::gas::estimate_route_gas_from_hops(walked_hop_gas, hop_count)
            .saturating_add(crate::services::execution::gas::estimate_route_storage_gas(
                hop_count,
                hop_count as u32,
            ));
    static_gas.max(dynamic)
}

/// Conservative gas units for a full route (overhead + per-hop + tick premium + storage reads).
#[must_use]
pub fn estimate_route_gas(edges: &[Edge]) -> u32 {
    if edges.is_empty() {
        return crate::services::execution::gas::ROUTE_EXECUTION_GAS_OVERHEAD;
    }
    if crate::pipeline::route_calls::balancer_direct_batch_eligible(edges) {
        return GAS_BALANCER_DIRECT_BATCH;
    }
    let hop_gas = route_hop_gas_budget(edges);
    let cold_slots = edges.len() as u32;
    crate::services::execution::gas::estimate_route_gas_from_hops_evm(
        hop_gas,
        edges.len(),
        cold_slots,
    )
}

#[derive(Debug, Clone, Copy)]
struct HopResult {
    amount_out: U256,
    gas: u32,
}

/// Per-hop input caps for **tickless** CL hops only (spot-probe sized).
/// Hops with tick coverage keep `U256::MAX` — `simulate_v3_swap` reports `shallow`
/// when the walk exhausts tick data. Capping ticked pools at the spot probe was
/// pinning Brent/eval at dust (`cl_cap` flood, best-eval input≈1e15).
/// Routes are short (≤ HOP_CAP); reuse probes by linear scan — no HashMap alloc.
#[inline]
fn route_shallow_caps_with(
    edges: &[Edge],
    mut probe_for_token: impl FnMut(crate::core::types::TokenIndex) -> U256,
    mut hop_is_tickless: impl FnMut(usize, &Edge) -> bool,
) -> [U256; HOP_CAP_USIZE] {
    let mut caps = [U256::MAX; HOP_CAP_USIZE];
    for (i, edge) in edges.iter().enumerate() {
        if !matches!(
            edge.protocol,
            ProtocolType::UniswapV3 | ProtocolType::UniswapV4
        ) {
            continue;
        }
        if !hop_is_tickless(i, edge) {
            continue;
        }
        let mut reused = None;
        for j in 0..i {
            if matches!(
                edges[j].protocol,
                ProtocolType::UniswapV3 | ProtocolType::UniswapV4
            ) && edges[j].token_in == edge.token_in
                && caps[j] < U256::MAX
            {
                reused = Some(caps[j]);
                break;
            }
        }
        caps[i] = reused.unwrap_or_else(|| probe_for_token(edge.token_in));
    }
    caps
}

fn route_shallow_caps(arena: &StateArena, edges: &[Edge]) -> [U256; HOP_CAP_USIZE] {
    route_shallow_caps_with(
        edges,
        |token| spot_probe_for_token(arena, token),
        |_, edge| match arena.pool_state(edge.pool_index) {
            Some(PoolState::V3(s) | PoolState::V4(s)) => s.ticks.is_empty(),
            // Missing/non-CL state: fail closed at probe size.
            _ => true,
        },
    )
}

#[inline]
fn route_has_cl_hop(edges: &[Edge]) -> bool {
    edges.iter().any(|edge| {
        matches!(
            edge.protocol,
            ProtocolType::UniswapV3 | ProtocolType::UniswapV4
        )
    })
}

fn simulate_hop(
    state: &PoolState,
    edge: &Edge,
    amount_in: U256,
    shallow_cap: U256,
) -> Option<HopResult> {
    if amount_in.is_zero() {
        return Some(HopResult {
            amount_out: U256::ZERO,
            gas: 0,
        });
    }
    if matches!(
        edge.protocol,
        ProtocolType::UniswapV3 | ProtocolType::UniswapV4
    ) && cl_hop_exceeds_shallow_cap(amount_in, shallow_cap)
    {
        return None;
    }

    match (state, edge.protocol) {
        (PoolState::V2(s), ProtocolType::UniswapV2) => {
            if amount_in >= v2_reserve_in(s, edge.zero_for_one) {
                return None;
            }
            let out = simulate_v2_swap(s, amount_in, edge.zero_for_one, Some(edge.fee_bps));
            Some(HopResult {
                amount_out: out,
                gas: GAS_V2_HOP,
            })
        }
        (PoolState::V3(s), ProtocolType::UniswapV3)
        | (PoolState::V4(s), ProtocolType::UniswapV4) => {
            let r = simulate_v3_swap(s, amount_in, edge.zero_for_one, Some(edge.fee_bps));
            if r.shallow {
                // Tickless pools always mark shallow, but the no-tick step path can
                // still quote within the spot-probe cap. Accept that; refuse larger.
                let tickless = s.ticks.is_empty();
                if !tickless
                    || cl_hop_exceeds_shallow_cap(amount_in, shallow_cap)
                    || r.amount_out.is_zero()
                {
                    return None;
                }
            }
            Some(HopResult {
                amount_out: r.amount_out,
                gas: r.gas_estimate,
            })
        }
        (PoolState::Curve(s), ProtocolType::CurveStable | ProtocolType::CurveCrypto) => {
            let out = curve_hop_amount_out(
                s,
                edge.protocol,
                amount_in,
                edge.token_in_idx as usize,
                edge.token_out_idx as usize,
            )?;
            Some(HopResult {
                amount_out: out,
                gas: GAS_CURVE_HOP,
            })
        }
        (PoolState::Balancer(s), ProtocolType::BalancerV2) => {
            let out = simulate_balancer_swap(
                s,
                amount_in,
                edge.token_in_idx as usize,
                edge.token_out_idx as usize,
            );
            Some(HopResult {
                amount_out: out,
                gas: GAS_BALANCER_HOP,
            })
        }
        (PoolState::Dodo(s), ProtocolType::Dodo) => {
            // Arena meta is always [base, quote]; sellBase iff token_in_idx == 0.
            // Do not key off zero_for_one alone — encode uses on-chain base/quote
            // and the two only agree after DODO meta canonicalization.
            let base_to_quote = edge.token_in_idx == 0;
            let out = get_dodo_amount_out(s, amount_in, base_to_quote);
            Some(HopResult {
                amount_out: out,
                gas: GAS_DODO_HOP,
            })
        }
        (PoolState::Woofi(s), ProtocolType::Woofi) => {
            let n_bases = s.base_states.len();
            let in_is_quote = edge.token_in_idx as usize >= n_bases;
            let out_is_quote = edge.token_out_idx as usize >= n_bases;
            let base_in = if in_is_quote {
                None
            } else {
                Some(edge.token_in_idx as usize)
            };
            let base_out = if out_is_quote {
                None
            } else {
                Some(edge.token_out_idx as usize)
            };
            let out =
                get_woofi_amount_out(s, amount_in, in_is_quote, out_is_quote, base_in, base_out);
            Some(HopResult {
                amount_out: out,
                gas: GAS_WOOFI_HOP,
            })
        }
        _ => None,
    }
}

/// Quote a single hop output for calldata encoding (reuses pipeline math).
#[must_use]
pub fn simulate_hop_amount_out(state: &PoolState, edge: &Edge, amount_in: U256) -> Option<U256> {
    simulate_hop_amount_out_with_cap(state, edge, amount_in, U256::MAX)
}

#[must_use]
pub fn simulate_hop_amount_out_with_cap(
    state: &PoolState,
    edge: &Edge,
    amount_in: U256,
    shallow_cap: U256,
) -> Option<U256> {
    simulate_hop(state, edge, amount_in, shallow_cap).map(|h| h.amount_out)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MinimalSimFailure {
    InvalidRoute,
    MissingPool {
        hop: usize,
    },
    NonTradable {
        hop: usize,
    },
    /// CL hop rejected: empty tick array (not yet hydrated).
    ClTickless {
        hop: usize,
    },
    /// CL hop input exceeds the spot-derived shallow liquidity cap.
    ClCapExceeded {
        hop: usize,
    },
    /// Legacy aggregate — prefer `ClTickless` / `ClCapExceeded` when classified.
    ShallowCl {
        hop: usize,
    },
    V2ReserveExhausted {
        hop: usize,
    },
    /// Edge TokenIndex addresses disagree with Balancer/Woofi `state.tokens[idx]`.
    TokenMismatch {
        hop: usize,
    },
    Math {
        hop: usize,
    },
    /// Edge protocol does not match arena `PoolState` variant (stale meta / wrong fetch).
    UnsupportedState {
        hop: usize,
        expected: ProtocolType,
        actual: UnsupportedStateKind,
    },
    /// Balancer vault `MAX_IN_RATIO` (30%) — sim returns zero / BAL#304.
    BalancerMaxInRatio {
        hop: usize,
    },
    ZeroOutput {
        hop: usize,
        protocol: ProtocolType,
    },
}

/// Coarse arena state tag for `UnsupportedState` attribution (probe + Brent).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnsupportedStateKind {
    Invalid,
    V2,
    V3,
    V4,
    Curve,
    Balancer,
    Dodo,
    Woofi,
}

#[inline]
#[must_use]
pub fn pool_state_kind(state: &PoolState) -> UnsupportedStateKind {
    match state {
        PoolState::Invalid => UnsupportedStateKind::Invalid,
        PoolState::V2(_) => UnsupportedStateKind::V2,
        PoolState::V3(_) => UnsupportedStateKind::V3,
        PoolState::V4(_) => UnsupportedStateKind::V4,
        PoolState::Curve(_) => UnsupportedStateKind::Curve,
        PoolState::Balancer(_) => UnsupportedStateKind::Balancer,
        PoolState::Dodo(_) => UnsupportedStateKind::Dodo,
        PoolState::Woofi(_) => UnsupportedStateKind::Woofi,
    }
}

/// True when edge/meta protocol agrees with the arena `PoolState` variant.
/// Live probe skips were dominated by `unsup_exp(v2)` × `unsup_act(v3)`.
#[inline]
#[must_use]
pub fn protocol_matches_pool_state(protocol: ProtocolType, state: &PoolState) -> bool {
    matches!(
        (state, protocol),
        (PoolState::V2(_), ProtocolType::UniswapV2)
            | (PoolState::V3(_), ProtocolType::UniswapV3)
            | (PoolState::V4(_), ProtocolType::UniswapV4)
            | (
                PoolState::Curve(_),
                ProtocolType::CurveStable | ProtocolType::CurveCrypto
            )
            | (PoolState::Balancer(_), ProtocolType::BalancerV2)
            | (PoolState::Dodo(_), ProtocolType::Dodo)
            | (PoolState::Woofi(_), ProtocolType::Woofi)
    )
}

/// Simulation-family protocol implied by arena state. Curve keeps a curve-family
/// `fallback` (stable vs crypto); other mismatches take the state's family.
#[inline]
#[must_use]
pub fn protocol_from_pool_state(state: &PoolState, fallback: ProtocolType) -> ProtocolType {
    match state {
        PoolState::V2(_) => ProtocolType::UniswapV2,
        PoolState::V3(_) => ProtocolType::UniswapV3,
        PoolState::V4(_) => ProtocolType::UniswapV4,
        PoolState::Curve(_) => match fallback {
            ProtocolType::CurveStable | ProtocolType::CurveCrypto => fallback,
            _ => ProtocolType::CurveStable,
        },
        PoolState::Balancer(_) => ProtocolType::BalancerV2,
        PoolState::Dodo(_) => ProtocolType::Dodo,
        PoolState::Woofi(_) => ProtocolType::Woofi,
        PoolState::Invalid => fallback,
    }
}

/// Every hop's edge protocol matches the arena `PoolState` variant (else probe is
/// `UnsupportedState` — live: V2 edges on V3/Balancer state crowding the HF window).
#[must_use]
pub fn cycle_edges_match_arena_state(arena: &StateArena, edges: &[Edge]) -> bool {
    edges.iter().all(|edge| {
        arena
            .pool_state(edge.pool_index)
            .is_some_and(|state| protocol_matches_pool_state(edge.protocol, state))
    })
}

/// V2/V3/V4 sim ignores pair membership — drop stale Direct edges before Brent.
/// Compare **addresses** (TokenIndex is not unique; index-equality over-filtered
/// stream snaps). Missing/short meta defers to calldata fail-closed.
#[must_use]
pub fn cycle_v2_edges_match_pool_meta(
    arena: &StateArena,
    pool_metas: &[crate::pipeline::types::PoolMeta],
    edges: &[Edge],
) -> bool {
    use crate::pipeline::types::pool_meta_at;
    edges.iter().all(|edge| {
        if !matches!(
            edge.protocol,
            ProtocolType::UniswapV2 | ProtocolType::UniswapV3 | ProtocolType::UniswapV4
        ) {
            return true;
        }
        let (Some(tin), Some(tout)) = (
            arena.token_address(edge.token_in),
            arena.token_address(edge.token_out),
        ) else {
            return false;
        };
        match pool_meta_at(pool_metas, edge.pool_index) {
            Some(m) if m.tokens.len() >= 2 => {
                m.tokens
                    .iter()
                    .any(|&t| arena.token_address(t) == Some(tin))
                    && m.tokens
                        .iter()
                        .any(|&t| arena.token_address(t) == Some(tout))
            }
            _ => true,
        }
    })
}

/// Vault/oracle token order must match edge TokenIndex addresses at the pool-local indices.
#[inline]
#[must_use]
pub fn multi_token_edge_aligned(
    state: &PoolState,
    edge: &Edge,
    token_in: alloy::primitives::Address,
    token_out: alloy::primitives::Address,
) -> bool {
    match state {
        PoolState::Balancer(s) if !s.tokens.is_empty() => {
            s.tokens.get(edge.token_in_idx as usize).copied() == Some(token_in)
                && s.tokens.get(edge.token_out_idx as usize).copied() == Some(token_out)
        }
        PoolState::Woofi(s) if !s.tokens.is_empty() => {
            s.tokens.get(edge.token_in_idx as usize).copied() == Some(token_in)
                && s.tokens.get(edge.token_out_idx as usize).copied() == Some(token_out)
        }
        _ => true,
    }
}

/// Look up vault/oracle leg indices by token address (meta order can diverge from
/// `getPoolTokens` after hot-cache refresh — live sticky Balancer `token_mismatch`).
#[inline]
#[must_use]
pub fn resolve_multi_token_vault_indices(
    tokens: &[alloy::primitives::Address],
    token_in: alloy::primitives::Address,
    token_out: alloy::primitives::Address,
) -> Option<(u8, u8)> {
    if token_in == token_out {
        return None;
    }
    let in_idx = tokens.iter().position(|&t| t == token_in)?;
    let out_idx = tokens.iter().position(|&t| t == token_out)?;
    Some((u8::try_from(in_idx).ok()?, u8::try_from(out_idx).ok()?))
}

/// Rewrite Balancer/Woofi `token_*_idx` to match live `state.tokens` addresses.
/// Returns `false` when the edge tokens are absent from the vault (unrecoverable).
#[must_use]
pub fn realign_multi_token_edge(arena: &StateArena, state: &PoolState, edge: &mut Edge) -> bool {
    if !matches!(
        edge.protocol,
        ProtocolType::BalancerV2 | ProtocolType::Woofi
    ) {
        return true;
    }
    let tokens = match state {
        PoolState::Balancer(s) if !s.tokens.is_empty() => s.tokens.as_slice(),
        PoolState::Woofi(s) if !s.tokens.is_empty() => s.tokens.as_slice(),
        _ => return true,
    };
    let Some(tin) = arena.token_address(edge.token_in) else {
        return false;
    };
    let Some(tout) = arena.token_address(edge.token_out) else {
        return false;
    };
    if multi_token_edge_aligned(state, edge, tin, tout) {
        return true;
    }
    let Some((in_idx, out_idx)) = resolve_multi_token_vault_indices(tokens, tin, tout) else {
        return false;
    };
    if !state.hop_pair_routable(in_idx as usize, out_idx as usize) {
        return false;
    }
    edge.token_in_idx = in_idx;
    edge.token_out_idx = out_idx;
    edge.zero_for_one = in_idx < out_idx;
    true
}

/// Clone-and-fix Balancer/Woofi vault indices on a ranked cycle. `None` = unrecoverable
/// mismatch (tokens not in vault) — caller should treat like micro-dead.
#[must_use]
pub fn realign_multi_token_found_cycle(
    arena: &StateArena,
    cycle: std::sync::Arc<crate::core::types::FoundCycle>,
) -> Option<std::sync::Arc<crate::core::types::FoundCycle>> {
    let needs_scan = cycle.edges.iter().any(|edge| {
        matches!(
            edge.protocol,
            ProtocolType::BalancerV2 | ProtocolType::Woofi
        )
    });
    if !needs_scan {
        return Some(cycle);
    }
    let mut owned = (*cycle).clone();
    let mut changed = false;
    for edge in &mut owned.edges {
        if !matches!(
            edge.protocol,
            ProtocolType::BalancerV2 | ProtocolType::Woofi
        ) {
            continue;
        }
        let state = arena.pool_state(edge.pool_index)?;
        let before = (edge.token_in_idx, edge.token_out_idx);
        if !realign_multi_token_edge(arena, state, edge) {
            return None;
        }
        if (edge.token_in_idx, edge.token_out_idx) != before {
            changed = true;
        }
    }
    if changed {
        Some(std::sync::Arc::new(owned))
    } else {
        Some(cycle)
    }
}

/// Rewrite hop `protocol` to match arena `PoolState` family (cached cycles can keep
/// V2 tags after hot-cache promotes the slot to V3 — live `proto_mismatch_skip`).
/// Also realign Uni `zero_for_one` to address order (sim can profit with stale zfo;
/// calldata encode rejects it).
#[must_use]
pub fn heal_cycle_edge_protocols(
    arena: &StateArena,
    cycle: std::sync::Arc<crate::core::types::FoundCycle>,
) -> Option<std::sync::Arc<crate::core::types::FoundCycle>> {
    let mut owned = (*cycle).clone();
    let mut changed = false;
    for edge in &mut owned.edges {
        let state = arena.pool_state(edge.pool_index)?;
        if matches!(state, PoolState::Invalid) {
            return None;
        }
        if !protocol_matches_pool_state(edge.protocol, state) {
            let corrected = protocol_from_pool_state(state, edge.protocol);
            if corrected == edge.protocol {
                return None;
            }
            edge.protocol = corrected;
            changed = true;
        }
        // ponytail: same rule as graph::apply_cl_zero_for_one
        if matches!(
            edge.protocol,
            ProtocolType::UniswapV2 | ProtocolType::UniswapV3 | ProtocolType::UniswapV4
        ) && let (Some(a_in), Some(a_out)) = (
            arena.token_address(edge.token_in),
            arena.token_address(edge.token_out),
        ) {
            let zfo = a_in < a_out;
            if edge.zero_for_one != zfo {
                edge.zero_for_one = zfo;
                changed = true;
            }
        }
    }
    if changed {
        Some(std::sync::Arc::new(owned))
    } else {
        Some(cycle)
    }
}

#[must_use]
pub fn minimal_sim_failure(
    arena: &StateArena,
    edges: &[Edge],
    amount_in: U256,
) -> Option<MinimalSimFailure> {
    if !route_edges_simulatable(edges) {
        return Some(MinimalSimFailure::InvalidRoute);
    }
    if amount_in.is_zero() {
        return None;
    }
    let shallow_caps = if route_has_cl_hop(edges) {
        route_shallow_caps(arena, edges)
    } else {
        [U256::MAX; HOP_CAP_USIZE]
    };
    let mut current = amount_in;
    for (hop, edge) in edges.iter().enumerate() {
        let Some(state) = arena.pool_state(edge.pool_index) else {
            return Some(MinimalSimFailure::MissingPool { hop });
        };
        if !state.is_tradable() {
            return Some(MinimalSimFailure::NonTradable { hop });
        }
        let mut edge = *edge;
        if matches!(
            edge.protocol,
            ProtocolType::BalancerV2 | ProtocolType::Woofi
        ) && !realign_multi_token_edge(arena, state, &mut edge)
        {
            return Some(MinimalSimFailure::TokenMismatch { hop });
        }
        // Classify failures without double-running expensive CL/curve math.
        match (state, edge.protocol) {
            (PoolState::V2(s), ProtocolType::UniswapV2)
                if current >= v2_reserve_in(s, edge.zero_for_one) =>
            {
                return Some(MinimalSimFailure::V2ReserveExhausted { hop });
            }
            (
                PoolState::V3(_) | PoolState::V4(_),
                ProtocolType::UniswapV3 | ProtocolType::UniswapV4,
            ) if cl_hop_tickless(state)
                && cl_hop_exceeds_shallow_cap(current, shallow_caps[hop]) =>
            {
                // Tickless is OK at spot-probe size; only fail when asking for more depth.
                return Some(MinimalSimFailure::ClTickless { hop });
            }
            (
                PoolState::V3(_) | PoolState::V4(_),
                ProtocolType::UniswapV3 | ProtocolType::UniswapV4,
            ) if !cl_hop_tickless(state)
                && cl_hop_exceeds_shallow_cap(current, shallow_caps[hop]) =>
            {
                return Some(MinimalSimFailure::ClCapExceeded { hop });
            }
            (PoolState::V2(_), ProtocolType::UniswapV2)
            | (PoolState::V3(_), ProtocolType::UniswapV3)
            | (PoolState::V4(_), ProtocolType::UniswapV4)
            | (PoolState::Curve(_), ProtocolType::CurveStable | ProtocolType::CurveCrypto)
            | (PoolState::Balancer(_), ProtocolType::BalancerV2)
            | (PoolState::Dodo(_), ProtocolType::Dodo)
            | (PoolState::Woofi(_), ProtocolType::Woofi) => {}
            _ => {
                return Some(MinimalSimFailure::UnsupportedState {
                    hop,
                    expected: edge.protocol,
                    actual: pool_state_kind(state),
                });
            }
        }
        let Some(result) = simulate_hop(state, &edge, current, shallow_caps[hop]) else {
            // CL shallow is the common simulate_hop None; curve math also returns None.
            if matches!(
                edge.protocol,
                ProtocolType::UniswapV3 | ProtocolType::UniswapV4
            ) {
                // Cap/tickless already classified above; residual = in-swap shallow walk.
                return Some(MinimalSimFailure::ShallowCl { hop });
            }
            return Some(MinimalSimFailure::Math { hop });
        };
        if result.amount_out.is_zero() {
            if let (PoolState::Balancer(s), ProtocolType::BalancerV2) = (state, edge.protocol) {
                let bal = s
                    .balances
                    .get(edge.token_in_idx as usize)
                    .copied()
                    .unwrap_or(U256::ZERO);
                if crate::core::math::balancer::exceeds_balancer_max_in_ratio(current, bal) {
                    return Some(MinimalSimFailure::BalancerMaxInRatio { hop });
                }
            }
            return Some(MinimalSimFailure::ZeroOutput {
                hop,
                protocol: edge.protocol,
            });
        }
        current = result.amount_out;
    }
    None
}

/// Micro-probe sizes that already cannot walk never rank — prune at HF select
/// so they do not crowd the probe window. Includes `ZeroOutput` / Balancer
/// `MAX_IN`, CL shallow/cap-dead at dust, any-hop `V2ReserveExhausted`
/// (hop-0 dust V2 is also caught earlier by `first_v2_hop_below_reserve`; this
/// covers mid-route V2 that dies even at micro), and Balancer/Woofi
/// `TokenMismatch` (amount-independent vault/meta index skew — live empty ranks
/// were dominated by a sticky Balancer hop-0 mismatch after other dead probes
/// cleared). Leave `ClTickless` alone — spot probe sizes can still rank those.
#[must_use]
pub fn micro_probe_liquidity_dead(
    arena: &StateArena,
    edges: &[Edge],
    micro_probe: U256,
) -> Option<MinimalSimFailure> {
    match minimal_sim_failure(arena, edges, micro_probe) {
        Some(fail @ MinimalSimFailure::ZeroOutput { .. })
        | Some(fail @ MinimalSimFailure::BalancerMaxInRatio { .. })
        | Some(fail @ MinimalSimFailure::ShallowCl { .. })
        | Some(fail @ MinimalSimFailure::ClCapExceeded { .. })
        | Some(fail @ MinimalSimFailure::V2ReserveExhausted { .. })
        | Some(fail @ MinimalSimFailure::TokenMismatch { .. }) => Some(fail),
        _ => None,
    }
}

/// Probe walk with gross>0 that already trips insane ROI/MATIC caps is a phantom —
/// live empty ranks still showed `sanity_why(matic=…)` after micro-only prune because
/// economic-floor sizes exceed the 1 MATIC cap while micro stayed under it. Floor/pin
/// rejects are left alone (other ladder sizes can still be sane).
#[must_use]
pub fn probe_insane_gross_phantom(
    arena: &StateArena,
    edges: &[Edge],
    amount: U256,
    token_decimals: u8,
    token_to_matic_rate: U256,
) -> bool {
    if amount.is_zero() {
        return false;
    }
    let Some(sim) = simulate_route_minimal(arena, edges, amount) else {
        return false;
    };
    if sim.profit.is_zero() {
        return false;
    }
    matches!(
        crate::pipeline::sim_sanity::check_sim_sanity(
            crate::pipeline::sim_sanity::SimSanityInput {
                amount_in: amount,
                gross_profit: sim.profit,
                search_low: U256::ZERO,
                token_decimals,
                token_to_matic_rate,
            }
        ),
        Err(
            crate::pipeline::sim_sanity::SimSanityReject::InsaneProfitRatio
                | crate::pipeline::sim_sanity::SimSanityReject::InsaneProfitMatic
        )
    )
}

#[must_use]
pub fn micro_probe_insane_gross_phantom(
    arena: &StateArena,
    edges: &[Edge],
    micro_probe: U256,
    token_decimals: u8,
    token_to_matic_rate: U256,
) -> bool {
    probe_insane_gross_phantom(
        arena,
        edges,
        micro_probe,
        token_decimals,
        token_to_matic_rate,
    )
}

/// Rank probe now starts at economic floor (except tickless CL spot-cap). Cycles
/// that already fail amount-dependent liquidity there never `kept` — prune at HF
/// select so micro-only survivors stop crowding empties as `v2_reserve` /
/// `shallow_cl` / `bal_max_in` (live probefloor: v2_reserve=27, shallow_cl=9,
/// bal_max_in=9 across probe_fail lines). Leave `ClTickless` / `TokenMismatch`
/// alone (tickless can still rank at spot; mismatch is micro-pruned).
#[must_use]
pub fn economic_floor_liquidity_dead(
    arena: &StateArena,
    edges: &[Edge],
    economic_floor: U256,
) -> Option<MinimalSimFailure> {
    if economic_floor.is_zero() {
        return None;
    }
    match minimal_sim_failure(arena, edges, economic_floor) {
        Some(fail @ MinimalSimFailure::ZeroOutput { .. })
        | Some(fail @ MinimalSimFailure::BalancerMaxInRatio { .. })
        | Some(fail @ MinimalSimFailure::ShallowCl { .. })
        | Some(fail @ MinimalSimFailure::ClCapExceeded { .. })
        | Some(fail @ MinimalSimFailure::V2ReserveExhausted { .. }) => Some(fail),
        _ => None,
    }
}

/// Balancer routes where even `economic_floor` trips vault `MAX_IN_RATIO` produce
/// Brent `bal_bounds_fail` (live: 100% of `bounds_fail`). Prune before select.
#[must_use]
pub fn balancer_economic_floor_max_in_dead(
    arena: &StateArena,
    edges: &[Edge],
    economic_floor: U256,
) -> bool {
    if !edges
        .iter()
        .any(|edge| edge.protocol == ProtocolType::BalancerV2)
    {
        return false;
    }
    matches!(
        economic_floor_liquidity_dead(arena, edges, economic_floor),
        Some(MinimalSimFailure::BalancerMaxInRatio { .. })
    )
}

fn cl_hop_tickless(state: &PoolState) -> bool {
    matches!(
        state,
        PoolState::V3(s) | PoolState::V4(s) if s.ticks.is_empty()
    )
}

#[inline]
fn cl_hop_exceeds_shallow_cap(amount_in: U256, shallow_cap: U256) -> bool {
    shallow_cap < U256::MAX && amount_in > shallow_cap
}

#[inline]
fn v2_reserve_in(state: &crate::core::types::V2PoolState, zero_for_one: bool) -> U256 {
    if zero_for_one {
        state.reserve0
    } else {
        state.reserve1
    }
}

/// Hop-0 V2 input reserve cannot cover `min_amount` (dead/dust pool).
///
/// Only the first hop is checked: `min_amount` is in cycle-start token units (micro/spot
/// probe). Comparing later V2 hops' reserves to that amount false-rejects e.g. USDC-side
/// pools when start is WMATIC (`1e9` USDC wei ≤ `1e12` WMATIC micro).
#[must_use]
pub fn first_v2_hop_below_reserve(
    arena: &StateArena,
    edges: &[Edge],
    min_amount: U256,
) -> Option<usize> {
    if min_amount.is_zero() {
        return None;
    }
    let edge = edges.first()?;
    if edge.protocol != ProtocolType::UniswapV2 {
        return None;
    }
    let Some(PoolState::V2(state)) = arena.pool_state(edge.pool_index) else {
        return None;
    };
    if v2_reserve_in(state, edge.zero_for_one) <= min_amount {
        return Some(0);
    }
    None
}

/// Whether arena `PoolState` variant matches the edge's declared protocol family.
#[inline]
#[must_use]
pub fn pool_state_matches_protocol(state: &PoolState, protocol: ProtocolType) -> bool {
    matches!(
        (state, protocol),
        (PoolState::V2(_), ProtocolType::UniswapV2)
            | (PoolState::V3(_), ProtocolType::UniswapV3)
            | (PoolState::V4(_), ProtocolType::UniswapV4)
            | (
                PoolState::Curve(_),
                ProtocolType::CurveStable | ProtocolType::CurveCrypto
            )
            | (PoolState::Balancer(_), ProtocolType::BalancerV2)
            | (PoolState::Dodo(_), ProtocolType::Dodo)
            | (PoolState::Woofi(_), ProtocolType::Woofi)
    )
}

/// First hop whose edge protocol disagrees with arena state (stale meta / bad hot overlay).
/// Live probe `unsup_exp` was dominated by V2 edges against V3 state.
#[must_use]
pub fn first_protocol_state_mismatch(
    arena: &StateArena,
    edges: &[Edge],
) -> Option<(usize, ProtocolType, UnsupportedStateKind)> {
    for (hop, edge) in edges.iter().enumerate() {
        let Some(state) = arena.pool_state(edge.pool_index) else {
            continue;
        };
        if !pool_state_matches_protocol(state, edge.protocol) {
            return Some((hop, edge.protocol, pool_state_kind(state)));
        }
    }
    None
}

/// Max start-token input when the route has tickless CL hops (shallow sim only).
/// `None` when the route has full tick coverage or no CL hops.
#[must_use]
pub fn tickless_cl_start_input_cap(
    arena: &StateArena,
    cycle_start: crate::core::types::TokenIndex,
    edges: &[Edge],
) -> Option<U256> {
    if cl_amount_cap(arena, edges) != Some(U256::ZERO) {
        return None;
    }
    Some(crate::pipeline::spot_price::spot_probe_for_token(
        arena,
        cycle_start,
    ))
}

/// Max trade size with faithful CL simulation. `None` = full tick coverage.
/// `Some(0)` = at least one CL hop lacks tick coverage and must not be quoted.
#[must_use]
pub fn cl_amount_cap(arena: &StateArena, edges: &[Edge]) -> Option<U256> {
    for edge in edges {
        if !matches!(
            edge.protocol,
            ProtocolType::UniswapV3 | ProtocolType::UniswapV4
        ) {
            continue;
        }
        let Some(state) = arena.pool_state(edge.pool_index) else {
            return Some(U256::ZERO);
        };
        if cl_hop_tickless(state) {
            return Some(U256::ZERO);
        }
    }
    None
}

/// Max gross-profit erosion (bps) tolerated between eval and post-refresh resim.
const RESIM_PROFIT_DRIFT_BPS: u64 = 1000;
/// Max per-hop amount drift (bps) tolerated between eval and post-refresh resim.
const RESIM_HOP_DRIFT_BPS: u64 = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HopFidelityReject {
    MissingPool(usize),
    PoolLocked(usize),
    ShallowCl(usize),
    V2ReserveExhausted(usize),
}

/// Counters from `route_hop_fidelity_reject_profiled` (CL depth sims are the expensive path).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct HopFidelityProfile {
    pub hops_checked: u32,
    pub cl_depth_sims: u32,
}

/// Drift metrics from a resim compare (populated even when the gate passes).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ResimFidelityProfile {
    pub profit_drift_bps: u64,
    pub max_hop_drift_bps: u64,
}

fn cl_hop_shallow_at_amount(
    state: &PoolState,
    edge: &Edge,
    hop_probe: U256,
    amount_in: U256,
    profile: Option<&mut HopFidelityProfile>,
) -> bool {
    if cl_hop_tickless(state) {
        return false;
    }
    if cl_hop_exceeds_shallow_cap(amount_in, hop_probe) {
        return true;
    }
    if let Some(p) = profile {
        p.cl_depth_sims = p.cl_depth_sims.saturating_add(1);
    }
    match state {
        PoolState::V3(s) | PoolState::V4(s) => {
            simulate_v3_swap(s, amount_in, edge.zero_for_one, Some(edge.fee_bps)).shallow
        }
        _ => false,
    }
}

#[inline]
fn u256_to_bps_u64(v: U256) -> u64 {
    if v > U256::from(u64::MAX) {
        u64::MAX
    } else {
        v.as_limbs()[0]
    }
}

fn hop_amount_within_drift(baseline: U256, refreshed: U256, max_drift_bps: u64) -> bool {
    if baseline == refreshed {
        return true;
    }
    // Either side zero with the other non-zero is infinite relative drift (and /0).
    if baseline.is_zero() || refreshed.is_zero() {
        return false;
    }
    let (lo, hi) = if baseline >= refreshed {
        (refreshed, baseline)
    } else {
        (baseline, refreshed)
    };
    let drift_bps = (hi - lo) * U256::from(10_000u64) / lo;
    drift_bps <= U256::from(max_drift_bps)
}

/// Per-hop CL fidelity: each V3/V4 hop uses its own decimal-aware spot probe.
#[must_use]
pub fn route_hop_fidelity_ok(arena: &StateArena, edges: &[Edge], hop_amounts: &[U256]) -> bool {
    route_hop_fidelity_reject(arena, edges, hop_amounts).is_none()
}

/// Fidelity after `simulate_route_detailed` / `walk_route_hops` on the same arena (skips redundant CL depth sims).
#[must_use]
pub fn route_hop_fidelity_ok_after_walk(
    arena: &StateArena,
    edges: &[Edge],
    hop_amounts: &[U256],
) -> bool {
    route_hop_fidelity_reject_profiled(arena, edges, hop_amounts, None, true, None).is_none()
}

/// First hop that fails tick-depth, tradability, or reserve-depth checks, if any.
#[must_use]
pub fn route_hop_fidelity_reject(
    arena: &StateArena,
    edges: &[Edge],
    hop_amounts: &[U256],
) -> Option<HopFidelityReject> {
    route_hop_fidelity_reject_profiled(arena, edges, hop_amounts, None, false, None)
}

#[must_use]
pub fn route_hop_fidelity_reject_profiled(
    arena: &StateArena,
    edges: &[Edge],
    hop_amounts: &[U256],
    mut profile: Option<&mut HopFidelityProfile>,
    cl_depth_already_verified: bool,
    precomputed_hop_probes: Option<&[U256; HOP_CAP_USIZE]>,
) -> Option<HopFidelityReject> {
    let hop_probes = if let Some(caps) = precomputed_hop_probes {
        *caps
    } else if route_has_cl_hop(edges) {
        route_shallow_caps(arena, edges)
    } else {
        [U256::MAX; HOP_CAP_USIZE]
    };
    for (i, edge) in edges.iter().enumerate() {
        if let Some(p) = profile.as_deref_mut() {
            p.hops_checked = p.hops_checked.saturating_add(1);
        }
        let amount_in = hop_amounts.get(i).copied().unwrap_or(U256::ZERO);
        let Some(state) = arena.pool_state(edge.pool_index) else {
            return Some(HopFidelityReject::MissingPool(i));
        };
        if !state.is_tradable() {
            return Some(HopFidelityReject::PoolLocked(i));
        }
        match (state, edge.protocol) {
            (PoolState::V2(s), ProtocolType::UniswapV2) => {
                let (reserve_in, _reserve_out) = if edge.zero_for_one {
                    (s.reserve0, s.reserve1)
                } else {
                    (s.reserve1, s.reserve0)
                };
                if amount_in >= reserve_in {
                    return Some(HopFidelityReject::V2ReserveExhausted(i));
                }
            }
            (
                PoolState::V3(_) | PoolState::V4(_),
                ProtocolType::UniswapV3 | ProtocolType::UniswapV4,
            ) if cl_hop_tickless(state) => {
                return Some(HopFidelityReject::ShallowCl(i));
            }
            (
                PoolState::V3(_) | PoolState::V4(_),
                ProtocolType::UniswapV3 | ProtocolType::UniswapV4,
            ) if !cl_depth_already_verified
                && cl_hop_shallow_at_amount(
                    state,
                    edge,
                    hop_probes[i],
                    amount_in,
                    profile.as_deref_mut(),
                ) =>
            {
                return Some(HopFidelityReject::ShallowCl(i));
            }
            (
                PoolState::V3(_) | PoolState::V4(_),
                ProtocolType::UniswapV3 | ProtocolType::UniswapV4,
            ) if cl_depth_already_verified
                && cl_hop_exceeds_shallow_cap(amount_in, hop_probes[i]) =>
            {
                return Some(HopFidelityReject::ShallowCl(i));
            }
            // After a successful walk, hop amounts already cleared simulate_hop.
            // Re-simulating Balancer/Curve/etc. here is pure CPU on the HF hot path.
            _ if cl_depth_already_verified => {}
            _ => {
                if !amount_in.is_zero() && simulate_hop_amount_out(state, edge, amount_in).is_none()
                {
                    return Some(HopFidelityReject::PoolLocked(i));
                }
            }
        }
    }
    None
}

/// Post-refresh resim must stay profitable and keep hop amounts aligned with eval.
#[must_use]
pub fn route_resim_fidelity_ok(
    baseline: &RouteSimulationResult,
    refreshed: &RouteSimulationResult,
) -> bool {
    route_resim_fidelity_reject(baseline, refreshed).is_none()
}

#[must_use]
pub fn route_resim_fidelity_reject(
    baseline: &RouteSimulationResult,
    refreshed: &RouteSimulationResult,
) -> Option<&'static str> {
    let mut profile = ResimFidelityProfile::default();
    route_resim_fidelity_reject_profiled(baseline, refreshed, &mut profile)
}

#[must_use]
pub fn route_resim_fidelity_reject_profiled(
    baseline: &RouteSimulationResult,
    refreshed: &RouteSimulationResult,
    profile: &mut ResimFidelityProfile,
) -> Option<&'static str> {
    if refreshed.profit.is_zero() {
        return Some("resim unprofitable");
    }
    if baseline.hop_amounts.len() != refreshed.hop_amounts.len() {
        return Some("hop count mismatch");
    }
    if !baseline.profit.is_zero() {
        if refreshed.profit >= baseline.profit {
            profile.profit_drift_bps = 0;
        } else {
            let lost = baseline.profit - refreshed.profit;
            let bps = lost * U256::from(10_000u64) / baseline.profit;
            profile.profit_drift_bps = u256_to_bps_u64(bps);
        }
        let min_profit = baseline.profit * U256::from(10_000u64 - RESIM_PROFIT_DRIFT_BPS)
            / U256::from(10_000u64);
        if refreshed.profit < min_profit {
            return Some("profit drift");
        }
    }
    for i in 0..baseline.hop_amounts.len() {
        let b = baseline.hop_amounts[i];
        let r = refreshed.hop_amounts[i];
        if b != r && !b.is_zero() && !r.is_zero() {
            let (lo, hi) = if b >= r { (r, b) } else { (b, r) };
            let drift = u256_to_bps_u64((hi - lo) * U256::from(10_000u64) / lo);
            profile.max_hop_drift_bps = profile.max_hop_drift_bps.max(drift);
        } else if b != r {
            profile.max_hop_drift_bps = profile.max_hop_drift_bps.max(10_000);
        }
        if !hop_amount_within_drift(b, r, RESIM_HOP_DRIFT_BPS) {
            return Some("hop amount drift");
        }
    }
    None
}

#[must_use]
/// Precomputed CL shallow caps for Brent — avoids rebuilding per `simulate_route_minimal` call.
pub fn precompute_route_shallow_caps(
    arena: &StateArena,
    edges: &[Edge],
) -> Option<[U256; HOP_CAP_USIZE]> {
    route_has_cl_hop(edges).then(|| route_shallow_caps(arena, edges))
}

fn walk_route_hops(
    arena: &StateArena,
    edges: &[Edge],
    amount_in: U256,
    mut hop_amounts: Option<&mut [U256]>,
    precomputed_shallow_caps: Option<&[U256; HOP_CAP_USIZE]>,
) -> Option<(U256, u32)> {
    if edges.len() > HOP_CAP_USIZE {
        return None;
    }
    let mut current = amount_in;
    let mut total_gas = 0u32;
    let shallow_caps = if let Some(caps) = precomputed_shallow_caps {
        *caps
    } else if route_has_cl_hop(edges) {
        route_shallow_caps(arena, edges)
    } else {
        [U256::MAX; HOP_CAP_USIZE]
    };
    if let Some(amounts) = hop_amounts.as_deref_mut() {
        *amounts.first_mut()? = amount_in;
    }

    for (i, edge) in edges.iter().enumerate() {
        let state = arena.pool_state(edge.pool_index)?;
        if !state.is_tradable() {
            return None;
        }
        let mut edge = *edge;
        if matches!(
            edge.protocol,
            ProtocolType::BalancerV2 | ProtocolType::Woofi
        ) && !realign_multi_token_edge(arena, state, &mut edge)
        {
            return None;
        }
        let shallow_cap = shallow_caps[i];
        let hop = simulate_hop(state, &edge, current, shallow_cap)?;
        if current > U256::ZERO && hop.amount_out.is_zero() {
            return None;
        }
        current = hop.amount_out;
        total_gas += hop.gas;
        if let Some(amounts) = hop_amounts.as_deref_mut() {
            *amounts.get_mut(i + 1)? = current;
        }
    }

    Some((current, total_gas))
}

#[inline]
fn route_edges_simulatable(edges: &[Edge]) -> bool {
    !edges.is_empty()
        && edges.len() <= HOP_CAP_USIZE
        && edges
            .windows(2)
            .all(|pair| pair[0].token_out == pair[1].token_in)
}

pub fn simulate_route_minimal(
    arena: &StateArena,
    edges: &[Edge],
    amount_in: U256,
) -> Option<MinimalSimResult> {
    simulate_route_minimal_with_caps(arena, edges, amount_in, None)
}

#[must_use]
pub fn simulate_route_minimal_with_caps(
    arena: &StateArena,
    edges: &[Edge],
    amount_in: U256,
    precomputed_shallow_caps: Option<&[U256; HOP_CAP_USIZE]>,
) -> Option<MinimalSimResult> {
    if !route_edges_simulatable(edges) {
        return None;
    }
    if amount_in.is_zero() {
        return Some(MinimalSimResult {
            profit: U256::ZERO,
            amount_out: U256::ZERO,
            total_gas: finalize_route_total_gas(edges, 0),
        });
    }
    let (amount_out, walked_gas) =
        walk_route_hops(arena, edges, amount_in, None, precomputed_shallow_caps)?;
    let profit = amount_out.saturating_sub(amount_in);
    let total_gas = finalize_route_total_gas(edges, walked_gas);
    Some(MinimalSimResult {
        profit,
        amount_out,
        total_gas,
    })
}

/// Full hop trace for calldata encoding and profit assessment.
#[must_use]
pub fn simulate_route_detailed(
    arena: &StateArena,
    edges: &[Edge],
    amount_in: U256,
) -> Option<RouteSimulationResult> {
    simulate_route_detailed_with_caps(arena, edges, amount_in, None)
}

/// Like [`simulate_route_detailed`] with precomputed CL shallow caps (Brent/HF reuse).
#[must_use]
pub fn simulate_route_detailed_with_caps(
    arena: &StateArena,
    edges: &[Edge],
    amount_in: U256,
    precomputed_shallow_caps: Option<&[U256; HOP_CAP_USIZE]>,
) -> Option<RouteSimulationResult> {
    let hop_count = edges.len();
    if !route_edges_simulatable(edges) {
        return None;
    }
    if amount_in.is_zero() {
        return Some(RouteSimulationResult {
            amount_in: U256::ZERO,
            amount_out: U256::ZERO,
            profit: U256::ZERO,
            profitable: false,
            hop_amounts: hop_amounts_zeroed(hop_count),
            total_gas: finalize_route_total_gas(edges, 0),
            hop_count: hop_count as u32,
        });
    }
    let mut hop_amounts = hop_amounts_zeroed(hop_count);
    let (amount_out, walked_gas) = walk_route_hops(
        arena,
        edges,
        amount_in,
        Some(&mut hop_amounts),
        precomputed_shallow_caps,
    )?;
    let profit = amount_out.saturating_sub(amount_in);
    let total_gas = finalize_route_total_gas(edges, walked_gas);
    Some(RouteSimulationResult {
        amount_in,
        amount_out,
        profit,
        profitable: profit > U256::ZERO,
        hop_amounts,
        total_gas,
        hop_count: hop_count as u32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::Edge;
    use alloy::primitives::Address;

    #[test]
    fn hop_drift_zero_refreshed_does_not_panic() {
        // Regression: refreshed hop amount 0 with non-zero baseline used to /0.
        assert!(!hop_amount_within_drift(
            U256::from(100u64),
            U256::ZERO,
            200
        ));
        assert!(!hop_amount_within_drift(
            U256::ZERO,
            U256::from(100u64),
            200
        ));
        assert!(hop_amount_within_drift(
            U256::from(100u64),
            U256::from(100u64),
            200
        ));
        assert!(hop_amount_within_drift(
            U256::from(10_000u64),
            U256::from(9_900u64),
            200
        ));
        assert!(!hop_amount_within_drift(
            U256::from(10_000u64),
            U256::from(9_000u64),
            200
        ));
    }

    #[test]
    fn protocol_matches_pool_state_rejects_cross_family() {
        // Live probe unsup was dominated by V2 edges on V3 arena slots.
        assert!(!protocol_matches_pool_state(
            ProtocolType::UniswapV2,
            &PoolState::Invalid
        ));
        let v2 = PoolState::V2(crate::core::types::V2PoolState {
            reserve0: U256::from(1_000_000u64),
            reserve1: U256::from(1_000_000u64),
            fee: U256::from(3u64),
            fee_denominator: U256::from(1000u64),
            block_timestamp_last: 0,
        });
        assert!(protocol_matches_pool_state(ProtocolType::UniswapV2, &v2));
        assert!(!protocol_matches_pool_state(ProtocolType::UniswapV3, &v2));
        assert_eq!(
            protocol_from_pool_state(&v2, ProtocolType::UniswapV3),
            ProtocolType::UniswapV2
        );
    }

    #[test]
    fn heal_cycle_edge_protocols_rewrites_v2_tag_on_v3_state() {
        use crate::core::types::{FoundCycle, V3PoolState, V3Tick};
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000_000_000_000_000u128,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![V3Tick {
                    tick: -60_000,
                    liquidity_gross: 1,
                    liquidity_net: 0,
                }]),
            })),
        );
        let cycle = Arc::new(FoundCycle {
            start_token: t0,
            edges: vec![Edge {
                pool_index: pool,
                token_in: t0,
                token_out: t1,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2, // stale tag
                fee_bps: 30,
                zero_for_one: true,
            }]
            .into(),
            hop_count: 1,
            log_weight: 0.0,
            cumulative_fee_bps: 30,
            score: 0.0,
            cycle_ratio: U256::ZERO,
        });
        assert!(!cycle_edges_match_arena_state(&arena, &cycle.edges));
        let healed = heal_cycle_edge_protocols(&arena, cycle).expect("heal");
        assert_eq!(healed.edges[0].protocol, ProtocolType::UniswapV3);
        // t0=[1..] < t1=[2..] ⇒ zero_for_one must be true after heal
        assert!(healed.edges[0].zero_for_one);
        assert!(cycle_edges_match_arena_state(&arena, &healed.edges));
    }

    #[test]
    fn heal_cycle_edge_protocols_realigns_stale_zfo() {
        use crate::core::types::{FoundCycle, V3PoolState, V3Tick};
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000_000_000_000_000u128,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![V3Tick {
                    tick: -60_000,
                    liquidity_gross: 1,
                    liquidity_net: 0,
                }]),
            })),
        );
        let cycle = Arc::new(FoundCycle {
            start_token: t0,
            edges: vec![Edge {
                pool_index: pool,
                token_in: t0,
                token_out: t1,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV3,
                fee_bps: 30,
                zero_for_one: false, // stale vs address order
            }]
            .into(),
            hop_count: 1,
            log_weight: 0.0,
            cumulative_fee_bps: 30,
            score: 0.0,
            cycle_ratio: U256::ZERO,
        });
        let healed = heal_cycle_edge_protocols(&arena, cycle).expect("heal");
        assert!(healed.edges[0].zero_for_one);
    }

    #[test]
    fn cycle_v2_edges_match_pool_meta_rejects_foreign_token() {
        use crate::core::types::V2PoolState;
        use crate::pipeline::types::PoolMeta;
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let foreign = arena.register_token(Address::from([9u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: U256::from(1_000_000u64),
                reserve1: U256::from(1_000_000u64),
                fee: U256::from(3u64),
                fee_denominator: U256::from(1000u64),
                block_timestamp_last: 0,
            })),
        );
        let meta = PoolMeta {
            pool_index: pool,
            protocol: ProtocolType::UniswapV2,
            tokens: vec![t0, t1],
            fee_bps: 30,
            bpt_index: None,
            pool_id: None,
            protocol_label: None,
            pool_type: None,
            hooks: None,
            tick_spacing: None,
        };
        let edges = [Edge {
            pool_index: pool,
            token_in: foreign,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        }];
        assert!(!cycle_v2_edges_match_pool_meta(&arena, &[meta], &edges));
        assert!(cycle_v2_edges_match_pool_meta(&arena, &[], &edges)); // defer when no meta
    }

    #[test]
    fn cycle_v3_edges_match_pool_meta_rejects_foreign_token() {
        use crate::core::types::V3PoolState;
        use crate::pipeline::types::PoolMeta;
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let foreign = arena.register_token(Address::from([9u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(Vec::new()),
            })),
        );
        let meta = PoolMeta {
            pool_index: pool,
            protocol: ProtocolType::UniswapV3,
            tokens: vec![t0, t1],
            fee_bps: 30,
            bpt_index: None,
            pool_id: None,
            protocol_label: None,
            pool_type: None,
            hooks: None,
            tick_spacing: None,
        };
        let edges = [Edge {
            pool_index: pool,
            token_in: foreign,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: true,
        }];
        assert!(!cycle_v2_edges_match_pool_meta(&arena, &[meta], &edges));
        assert!(cycle_v2_edges_match_pool_meta(&arena, &[], &edges));
    }

    #[test]
    fn balancer_token_mismatch_fails_closed_before_phantom_profit() {
        use crate::core::math::fixed_point::ONE;
        use crate::core::types::{BalancerPoolKind, BalancerPoolState};
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let wrong = arena.register_token(Address::from([9u8; 20]));
        let bal = U256::from(5u64) * ONE;
        let w = ONE / U256::from(2u64);
        let pool = arena.register_pool(
            Address::from([15u8; 20]),
            Arc::new(PoolState::Balancer(BalancerPoolState {
                pool_id: None,
                tokens: vec![Address::from([1u8; 20]), Address::from([2u8; 20])],
                balances: vec![bal, bal],
                weights: vec![w, w],
                scaling_factors: vec![ONE, ONE],
                amp: U256::ZERO,
                amp_precision: U256::ZERO,
                fee: U256::ZERO,
                pool_type: BalancerPoolKind::Weighted,
                linear: None,
                bpt_index: None,
                is_updating: false,
                last_change_block: 0,
            })),
        );
        let bad = Edge {
            pool_index: pool,
            token_in: wrong,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::BalancerV2,
            fee_bps: 10,
            zero_for_one: true,
        };
        let ok = Edge {
            token_in: t0,
            ..bad
        };
        let state = arena.pool_state(pool).expect("pool state registered");
        assert!(!multi_token_edge_aligned(
            state,
            &bad,
            Address::from([9u8; 20]),
            Address::from([2u8; 20])
        ));
        assert!(multi_token_edge_aligned(
            state,
            &ok,
            Address::from([1u8; 20]),
            Address::from([2u8; 20])
        ));
        assert_eq!(
            minimal_sim_failure(&arena, &[bad], U256::from(1_000u64)),
            Some(MinimalSimFailure::TokenMismatch { hop: 0 })
        );
        assert_ne!(
            minimal_sim_failure(&arena, &[ok], ONE / U256::from(10u64)),
            Some(MinimalSimFailure::TokenMismatch { hop: 0 })
        );
        assert!(simulate_route_minimal(&arena, &[ok], ONE / U256::from(10u64)).is_some());
        assert_eq!(
            micro_probe_liquidity_dead(&arena, &[bad], U256::from(1_000u64)),
            Some(MinimalSimFailure::TokenMismatch { hop: 0 })
        );
        assert!(micro_probe_liquidity_dead(&arena, &[ok], U256::from(1_000u64)).is_none());

        // Skewed idxs but both addresses present in vault → remap recovers the hop.
        let mut skewed = Edge {
            token_in: t0,
            token_out: t1,
            token_in_idx: 1,
            token_out_idx: 0,
            zero_for_one: false,
            ..ok
        };
        let state = arena.pool_state(pool).expect("pool state registered");
        assert!(realign_multi_token_edge(&arena, state, &mut skewed));
        assert_eq!((skewed.token_in_idx, skewed.token_out_idx), (0, 1));
        assert!(!matches!(
            minimal_sim_failure(&arena, &[skewed], ONE / U256::from(10u64)),
            Some(MinimalSimFailure::TokenMismatch { .. })
        ));
        assert!(simulate_route_minimal(&arena, &[skewed], ONE / U256::from(10u64)).is_some());
    }

    #[test]
    fn resim_fidelity_zero_hop_is_drift_not_panic() {
        let baseline = RouteSimulationResult {
            amount_in: U256::from(100u64),
            amount_out: U256::from(110u64),
            profit: U256::from(10u64),
            profitable: true,
            hop_amounts: {
                let mut h = hop_amounts_zeroed(1);
                h[0] = U256::from(100u64);
                h[1] = U256::from(110u64);
                h
            },
            total_gas: 100_000,
            hop_count: 1,
        };
        let mut refreshed = baseline.clone();
        refreshed.hop_amounts[1] = U256::ZERO;
        refreshed.amount_out = U256::ZERO;
        refreshed.profit = U256::ZERO;
        assert_eq!(
            route_resim_fidelity_reject(&baseline, &refreshed),
            Some("resim unprofitable")
        );
        refreshed.profit = U256::from(9u64);
        refreshed.amount_out = U256::from(109u64);
        assert_eq!(
            route_resim_fidelity_reject(&baseline, &refreshed),
            Some("hop amount drift")
        );
    }

    #[test]
    fn oversized_routes_fail_closed() {
        let arena = StateArena::default();
        let edge = Edge {
            pool_index: crate::core::types::PoolIndex(0),
            token_in: crate::core::types::TokenIndex(0),
            token_out: crate::core::types::TokenIndex(1),
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let edges = vec![edge; HOP_CAP_USIZE + 1];
        assert!(simulate_route_minimal(&arena, &edges, U256::ZERO).is_none());
        assert!(simulate_route_detailed(&arena, &edges, U256::ZERO).is_none());
    }

    #[test]
    fn test_estimate_hop_gas_v2() {
        assert!(estimate_hop_gas(ProtocolType::UniswapV2) > 0);
    }

    #[test]
    fn zero_amount_minimal_sim_skips_walk() {
        use crate::core::types::V2PoolState;
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: U256::from(1_000_000u64),
                reserve1: U256::from(1_000_000u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 1,
            })),
        );
        let edge = Edge {
            pool_index: pool,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        let sim = simulate_route_minimal(&arena, &[edge], U256::ZERO).expect("zero sim");
        assert!(sim.profit.is_zero());
        assert!(sim.amount_out.is_zero());
        // Matches simulate_hop: amount_in >= reserve_in is unusable.
        assert_eq!(
            first_v2_hop_below_reserve(&arena, &[edge], U256::from(1_000_000u64)),
            Some(0)
        );
        assert_eq!(
            first_v2_hop_below_reserve(&arena, &[edge], U256::from(999_999u64)),
            None
        );
        // Intermediate V2: do not compare its reserve to start-token min_amount.
        let v3_edge = Edge {
            protocol: ProtocolType::UniswapV3,
            ..edge
        };
        assert_eq!(
            first_v2_hop_below_reserve(&arena, &[v3_edge, edge], U256::from(1_000_000u64)),
            None
        );
    }

    #[test]
    fn disconnected_hops_fail_closed_even_for_zero_amount() {
        use crate::core::types::V2PoolState;
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let t2 = arena.register_token(Address::from([3u8; 20]));
        let state = Arc::new(PoolState::V2(V2PoolState {
            reserve0: U256::from(1_000_000u64),
            reserve1: U256::from(1_000_000u64),
            fee: U256::from(997u64),
            fee_denominator: U256::from(1_000u64),
            block_timestamp_last: 1,
        }));
        let first_pool = arena.register_pool(Address::from([4u8; 20]), Arc::clone(&state));
        let second_pool = arena.register_pool(Address::from([5u8; 20]), state);
        let edges = [
            Edge {
                pool_index: first_pool,
                token_in: t0,
                token_out: t1,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: second_pool,
                token_in: t2,
                token_out: t0,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
        ];

        assert!(simulate_route_minimal(&arena, &edges, U256::ZERO).is_none());
        assert!(simulate_route_detailed(&arena, &edges, U256::from(100u64)).is_none());
    }

    #[test]
    fn v3_route_gas_does_not_double_count_base_cost() {
        use crate::core::types::{V3PoolState, V3Tick};
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000_000_000_000_000u128,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![V3Tick {
                    tick: -60_000,
                    liquidity_gross: 1,
                    liquidity_net: 0,
                }]),
            })),
        );
        let edge = Edge {
            pool_index: pool,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: true,
        };

        let amount_in = crate::pipeline::spot_price::spot_probe_for_token(&arena, t0);
        let sim = simulate_route_minimal(&arena, &[edge], amount_in).expect("simulation");
        let expected = crate::services::execution::gas::estimate_route_gas_from_hops_evm(
            crate::core::constants::GAS_V3_BASE,
            1,
            1,
        );
        assert_eq!(sim.total_gas, expected);
    }

    #[test]
    fn balancer_direct_batch_uses_single_batch_gas_not_per_hop_sum() {
        use crate::core::constants::GAS_BALANCER_DIRECT_BATCH;
        use crate::services::execution::gas::estimate_route_gas_from_hops_evm;

        let edges = [
            Edge {
                pool_index: crate::core::types::PoolIndex(0),
                token_in: crate::core::types::TokenIndex(0),
                token_out: crate::core::types::TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::BalancerV2,
                fee_bps: 0,
                zero_for_one: true,
            },
            Edge {
                pool_index: crate::core::types::PoolIndex(1),
                token_in: crate::core::types::TokenIndex(1),
                token_out: crate::core::types::TokenIndex(0),
                token_in_idx: 1,
                token_out_idx: 0,
                protocol: ProtocolType::BalancerV2,
                fee_bps: 0,
                zero_for_one: true,
            },
        ];
        let batch = estimate_route_gas(&edges);
        let per_hop = estimate_route_gas_from_hops_evm(GAS_BALANCER_HOP * 2, 2, 2);
        assert_eq!(route_hop_gas_budget(&edges), GAS_BALANCER_DIRECT_BATCH);
        assert_eq!(batch, GAS_BALANCER_DIRECT_BATCH);
        assert!(batch < per_hop);
    }

    #[test]
    fn route_gas_formula_matches_executor_overhead_model() {
        use crate::core::constants::GAS_V2_HOP;
        use crate::services::execution::gas::estimate_route_gas_from_hops_evm;

        let edges = [
            Edge {
                pool_index: crate::core::types::PoolIndex(0),
                token_in: crate::core::types::TokenIndex(0),
                token_out: crate::core::types::TokenIndex(1),
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: crate::core::types::PoolIndex(1),
                token_in: crate::core::types::TokenIndex(1),
                token_out: crate::core::types::TokenIndex(0),
                token_in_idx: 1,
                token_out_idx: 0,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
        ];
        let hop_gas = GAS_V2_HOP * 2;
        let expected = estimate_route_gas_from_hops_evm(hop_gas, 2, 2);
        assert_eq!(estimate_route_gas(&edges), expected);
        assert!(expected > hop_gas);
    }

    #[test]
    fn v2_hop_fails_closed_when_amount_exhausts_reserve_in() {
        use crate::core::types::V2PoolState;

        let state = PoolState::V2(V2PoolState {
            reserve0: U256::from(1_000_000u64),
            reserve1: U256::from(2_000_000u64),
            fee: U256::from(997u64),
            fee_denominator: U256::from(1_000u64),
            block_timestamp_last: 0,
        });
        let edge = Edge {
            pool_index: crate::core::types::PoolIndex(0),
            token_in: crate::core::types::TokenIndex(0),
            token_out: crate::core::types::TokenIndex(1),
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };
        assert!(simulate_hop_amount_out(&state, &edge, U256::from(999_999u64)).is_some());
        assert!(simulate_hop_amount_out(&state, &edge, U256::from(1_000_000u64)).is_none());
    }

    #[test]
    fn minimal_sim_diagnoses_v2_reserve_exhaustion() {
        use crate::core::types::V2PoolState;
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let token_in = arena.register_token(Address::from([11u8; 20]));
        let token_out = arena.register_token(Address::from([12u8; 20]));
        let pool = arena.register_pool(
            Address::from([13u8; 20]),
            Arc::new(PoolState::V2(V2PoolState {
                reserve0: U256::from(1_000_000u64),
                reserve1: U256::from(2_000_000u64),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        );
        let edge = Edge {
            pool_index: pool,
            token_in,
            token_out,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        };

        assert_eq!(
            minimal_sim_failure(&arena, &[edge], U256::from(1_000_000u64)),
            Some(MinimalSimFailure::V2ReserveExhausted { hop: 0 })
        );
        // Micro walks; economic-sized start dies — select must prune (rank probe
        // no longer keeps below-floor dust that would hide this).
        assert!(micro_probe_liquidity_dead(&arena, &[edge], U256::from(1u64)).is_none());
        assert_eq!(
            economic_floor_liquidity_dead(&arena, &[edge], U256::from(1_000_000u64)),
            Some(MinimalSimFailure::V2ReserveExhausted { hop: 0 })
        );
    }

    #[test]
    fn cl_hop_rejects_amount_above_explicit_shallow_cap() {
        use crate::core::types::{V3PoolState, V3Tick};
        use std::sync::Arc;

        let state = PoolState::V3(V3PoolState {
            sqrt_price_x96: U256::from(1u128 << 96),
            liquidity: 10_000_000_000_000u128,
            tick: 0,
            fee: U256::from(3000u32),
            tick_spacing: 60,
            unlocked: true,
            fee_protocol: 0,
            observation_cardinality: 1,
            ticks: Arc::from(vec![V3Tick {
                tick: -60,
                liquidity_gross: 10_000_000_000_000,
                liquidity_net: 10_000_000_000_000,
            }]),
        });
        let edge = Edge {
            pool_index: crate::core::types::PoolIndex(0),
            token_in: crate::core::types::TokenIndex(0),
            token_out: crate::core::types::TokenIndex(1),
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: true,
        };
        let cap = U256::from(1_000u64);
        assert!(simulate_hop_amount_out_with_cap(&state, &edge, cap, cap).is_some());
        assert!(simulate_hop_amount_out_with_cap(&state, &edge, cap + U256::ONE, cap).is_none());
    }

    #[test]
    fn shallow_caps_probe_each_token_once_per_route() {
        use crate::core::types::{TokenIndex, V3PoolState};
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let token_a = arena.register_token(Address::from([1u8; 20]));
        let token_b = arena.register_token(Address::from([2u8; 20]));
        let pool_0 = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(Vec::new()),
            })),
        );
        let pool_1 = arena.register_pool(
            Address::from([4u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(Vec::new()),
            })),
        );
        let edges = [
            Edge {
                pool_index: pool_0,
                token_in: token_a,
                token_out: token_b,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV3,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: pool_1,
                token_in: token_a,
                token_out: token_b,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV4,
                fee_bps: 30,
                zero_for_one: true,
            },
        ];
        let mut probes = 0u32;
        // Both pools are tickless in this fixture — probe cap applies and is reused.
        let caps = route_shallow_caps_with(
            &edges,
            |token: TokenIndex| {
                probes += 1;
                if token == token_a {
                    U256::from(111u64)
                } else {
                    U256::from(222u64)
                }
            },
            |_, _| true,
        );
        assert_eq!(probes, 1);
        assert_eq!(caps[0], U256::from(111u64));
        assert_eq!(caps[1], U256::from(111u64));
    }

    #[test]
    fn shallow_caps_max_when_cl_ticks_present() {
        use crate::core::types::{V3PoolState, V3Tick};
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![V3Tick {
                    tick: -60,
                    liquidity_gross: 1,
                    liquidity_net: 0,
                }]),
            })),
        );
        let edges = [Edge {
            pool_index: pool,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: true,
        }];
        let caps = route_shallow_caps(&arena, &edges);
        assert_eq!(caps[0], U256::MAX);
    }

    #[test]
    fn cl_amount_cap_none_when_ticks_present() {
        use crate::core::types::{V3PoolState, V3Tick};
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: u128::MAX / 2,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(
                    (1..=32)
                        .map(|step| V3Tick {
                            tick: -(step * 60),
                            liquidity_gross: 1_000_000,
                            liquidity_net: 0,
                        })
                        .collect::<Vec<_>>(),
                ),
            })),
        );
        let edges = [Edge {
            pool_index: pool,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: true,
        }];
        assert!(cl_amount_cap(&arena, &edges).is_none());
    }

    #[test]
    fn hop_fidelity_rejects_shallow_cl_on_intermediate_hop_amount() {
        use crate::core::types::V3PoolState;
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let t2 = arena.register_token(Address::from([3u8; 20]));
        let v2_pool = arena.register_pool(
            Address::from([4u8; 20]),
            Arc::new(PoolState::V2(crate::core::types::V2PoolState {
                reserve0: U256::from(1_000_000u64) * U256::from(10u128.pow(18)),
                reserve1: U256::from(1_000_000u64) * U256::from(10u128.pow(18)),
                fee: U256::from(997u64),
                fee_denominator: U256::from(1_000u64),
                block_timestamp_last: 0,
            })),
        );
        let v3_pool = arena.register_pool(
            Address::from([5u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 0,
                ticks: Arc::from(Vec::new()),
            })),
        );
        let edges = [
            Edge {
                pool_index: v2_pool,
                token_in: t0,
                token_out: t1,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV2,
                fee_bps: 30,
                zero_for_one: true,
            },
            Edge {
                pool_index: v3_pool,
                token_in: t1,
                token_out: t2,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::UniswapV3,
                fee_bps: 30,
                zero_for_one: true,
            },
        ];
        let probe = crate::pipeline::spot_price::spot_probe_for_token(&arena, t0);
        let mut first_only = hop_amounts_zeroed(edges.len());
        first_only[0] = probe;
        assert!(!route_hop_fidelity_ok(&arena, &edges, &first_only));
        let mut hop_amounts = hop_amounts_zeroed(edges.len());
        hop_amounts[0] = probe;
        hop_amounts[1] = U256::from(10u128.pow(18));
        assert_eq!(
            route_hop_fidelity_reject(&arena, &edges, &hop_amounts),
            Some(HopFidelityReject::ShallowCl(1))
        );
    }

    #[test]
    fn hop_fidelity_rejects_loaded_cl_with_exhausted_tick_window_below_probe() {
        use crate::core::types::{V3PoolState, V3Tick};
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 1,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 1,
                ticks: Arc::from(vec![V3Tick {
                    tick: -1,
                    liquidity_gross: 1_000_000,
                    liquidity_net: 1_000_000,
                }]),
            })),
        );
        let edges = [Edge {
            pool_index: pool,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: true,
        }];
        let mut hop_amounts = hop_amounts_zeroed(edges.len());
        hop_amounts[0] = U256::from(100u64);

        assert_eq!(
            route_hop_fidelity_reject(&arena, &edges, &hop_amounts),
            Some(HopFidelityReject::ShallowCl(0))
        );
    }

    #[test]
    fn resim_fidelity_rejects_profit_drift() {
        let baseline = RouteSimulationResult {
            amount_in: U256::from(1000u64),
            amount_out: U256::from(1100u64),
            profit: U256::from(100u64),
            profitable: true,
            hop_amounts: hop_amounts_zeroed(1),
            total_gas: 0,
            hop_count: 1,
        };
        let refreshed = RouteSimulationResult {
            profit: U256::from(40u64),
            ..baseline.clone()
        };
        assert_eq!(
            route_resim_fidelity_reject(&baseline, &refreshed),
            Some("profit drift")
        );
    }

    #[test]
    fn resim_fidelity_rejects_hop_count_mismatch() {
        let baseline = RouteSimulationResult {
            amount_in: U256::from(1000u64),
            amount_out: U256::from(1100u64),
            profit: U256::from(100u64),
            profitable: true,
            hop_amounts: hop_amounts_zeroed(2),
            total_gas: 0,
            hop_count: 2,
        };
        let refreshed = RouteSimulationResult {
            hop_amounts: hop_amounts_zeroed(1),
            hop_count: 1,
            ..baseline.clone()
        };
        assert_eq!(
            route_resim_fidelity_reject(&baseline, &refreshed),
            Some("hop count mismatch")
        );
    }

    #[test]
    fn cl_amount_cap_is_zero_when_tickless() {
        use crate::core::types::V3PoolState;
        use std::sync::Arc;

        let mut arena = StateArena::default();
        let t0 = arena.register_token(Address::from([1u8; 20]));
        let t1 = arena.register_token(Address::from([2u8; 20]));
        let pool = arena.register_pool(
            Address::from([3u8; 20]),
            Arc::new(PoolState::V3(V3PoolState {
                sqrt_price_x96: U256::from(1u128 << 96),
                liquidity: 1_000_000,
                tick: 0,
                fee: U256::from(3000u32),
                tick_spacing: 60,
                unlocked: true,
                fee_protocol: 0,
                observation_cardinality: 0,
                ticks: Arc::from(Vec::new()),
            })),
        );
        let edges = [Edge {
            pool_index: pool,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV3,
            fee_bps: 30,
            zero_for_one: true,
        }];
        assert_eq!(cl_amount_cap(&arena, &edges), Some(U256::ZERO));
        let probe = crate::pipeline::spot_price::spot_probe_for_token(&arena, t0);
        assert_eq!(tickless_cl_start_input_cap(&arena, t0, &edges), Some(probe));
        // Ranking may quote tickless at spot-probe size; execution fidelity still refuses.
        assert!(
            simulate_route_minimal(&arena, &edges, probe).is_some(),
            "tickless CL must remain simulable at spot-probe size for ranking"
        );
        assert!(
            simulate_route_minimal(&arena, &edges, probe + U256::ONE).is_none(),
            "tickless CL must fail closed above spot-probe size"
        );
        let mut hop_amounts = hop_amounts_zeroed(edges.len());
        hop_amounts[0] = probe;
        assert_eq!(
            route_hop_fidelity_reject(&arena, &edges, &hop_amounts),
            Some(HopFidelityReject::ShallowCl(0))
        );
    }

    #[test]
    fn print_calldata_layout() {
        use crate::abis::{ExecutorCall, IArbExecutor};
        use crate::services::execution::calldata::{
            build_packed_route_payload, pack_executor_calls,
        };
        use alloy::sol_types::SolCall;

        let a1: Address = "0x0000000000000000000000000000010000000001"
            .parse()
            .expect("test address a1 should parse");
        let a2: Address = "0x0000000000000000000000000000010000000002"
            .parse()
            .expect("test address a2 should parse");
        let a3: Address = "0x0000000000000000000000000000010000000003"
            .parse()
            .expect("test address a3 should parse");

        let calls = vec![ExecutorCall {
            target: a1,
            value: U256::ZERO,
            data: vec![0xde, 0xad].into(),
        }];
        let packed_calls = pack_executor_calls(&calls).expect("test calls should pack");
        let route_hash = crate::services::execution::calldata::compute_route_hash(&packed_calls);
        let (packed_route, _) = build_packed_route_payload(
            a3,
            U256::from(1000u64),
            a2,
            U256::from(100u64),
            U256::from(9999999999u64),
            &calls,
        )
        .expect("test route payload should build");

        let cd = IArbExecutor::executeArbCall {
            packedRoute: packed_route.clone(),
        }
        .abi_encode();
        assert!(!cd.is_empty());
        assert_ne!(route_hash, alloy::primitives::B256::ZERO);
        assert!(!packed_route.is_empty());
    }

    #[test]
    fn dodo_sell_direction_follows_token_in_idx_not_zero_for_one() {
        use crate::core::math::fixed_point::ONE;
        use crate::core::types::{DodoPoolState, DodoRState, PoolIndex, TokenIndex};

        let base = Address::repeat_byte(0x0b);
        let quote = Address::repeat_byte(0x0a);
        let state = PoolState::Dodo(DodoPoolState {
            base_reserve: U256::from(1_000u64) * ONE,
            quote_reserve: U256::from(2_000u64) * ONE,
            base_token: base,
            quote_token: quote,
            base_target: U256::from(1_000u64) * ONE,
            quote_target: U256::from(2_000u64) * ONE,
            r_state: DodoRState::One,
            i: ONE,
            k: U256::ZERO,
            lp_fee_rate: U256::ZERO,
            mt_fee_rate: U256::ZERO,
        });
        // sellQuote: token_in_idx=1 (quote), even if zero_for_one is wrongly true.
        let edge = Edge {
            pool_index: PoolIndex(0),
            token_in: TokenIndex(1),
            token_out: TokenIndex(0),
            token_in_idx: 1,
            token_out_idx: 0,
            protocol: ProtocolType::Dodo,
            fee_bps: 0,
            zero_for_one: true,
        };
        let amount = U256::from(10u64) * ONE;
        let out = simulate_hop_amount_out(&state, &edge, amount).expect("sellQuote");
        // k=0, i=1 ⇒ out = min(amount, base_reserve) for sellQuote.
        assert_eq!(out, amount);
        // sellBase would pay base into a 2000-quote pool at i=1 → 10 quote, same amount here;
        // distinguish by exhausting quote capacity: sellQuote of 1500 base-units of quote
        // is capped by base_reserve (1000) via the k=0 branch.
        let big = U256::from(1_500u64) * ONE;
        let out_big = simulate_hop_amount_out(&state, &edge, big).expect("capped sellQuote");
        assert_eq!(out_big, U256::from(1_000u64) * ONE);
    }
}
