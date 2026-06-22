use std::collections::HashSet;

use alloy::primitives::Address;

use crate::core::types::{Edge, FoundCycle, TokenIndex};
use crate::pipeline::arena::StateArena;
use crate::pipeline::bellman_ford::route_call_count;
use crate::pipeline::types::{PoolMeta, route_fingerprint};
use crate::tui::update::{UiOpportunity, protocol_short_label};

const MAJOR_TOKENS: [&str; 6] = [
    "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270", // WMATIC
    "0x2791bca1f2de4661ed88a30c99a7a9489c09eb3f", // USDC
    "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359", // USDC native
    "0xc2132d05d31c914a87c6611c10748aeb04b58e8f", // USDT
    "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619", // WETH
    "0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6", // WBTC
];

pub fn short_addr(addr: &Address) -> String {
    let s = format!("{addr}");
    if s.len() >= 10 {
        format!("{}…{}", &s[..6], &s[s.len() - 4..])
    } else {
        s
    }
}

pub fn token_label(arena: &StateArena, token: TokenIndex) -> String {
    arena
        .token_address(token)
        .map(|a| short_addr(&a))
        .unwrap_or_else(|| format!("T{}", token.0))
}

pub fn is_major_token(addr: &Address) -> bool {
    let s = format!("{addr}").to_ascii_lowercase();
    MAJOR_TOKENS.iter().any(|m| s == *m)
}

pub fn cycle_has_long_tail(arena: &StateArena, cycle: &FoundCycle) -> bool {
    let mut tokens = HashSet::new();
    tokens.insert(cycle.start_token);
    for e in &cycle.edges {
        tokens.insert(e.token_in);
        tokens.insert(e.token_out);
    }
    tokens.iter().any(|&t| {
        arena
            .token_address(t)
            .is_some_and(|a| !is_major_token(&a))
    })
}

pub fn protocol_label_for_edge(edge: &Edge, pool_metas: &[PoolMeta]) -> String {
    let raw = pool_metas
        .get(edge.pool_index.0 as usize)
        .and_then(|m| m.protocol_label.as_deref());
    protocol_short_label(edge.protocol, raw)
}

pub fn compact_route(
    arena: &StateArena,
    cycle: &FoundCycle,
    pool_metas: &[PoolMeta],
) -> String {
    let mut parts = Vec::with_capacity(cycle.edges.len() * 2 + 1);
    parts.push(token_label(arena, cycle.start_token));
    for edge in &cycle.edges {
        let proto = protocol_label_for_edge(edge, pool_metas);
        parts.push(format!("→[{proto}]"));
        parts.push(token_label(arena, edge.token_out));
    }
    parts.join(" ")
}

pub fn detail_route_tree(
    arena: &StateArena,
    cycle: &FoundCycle,
    pool_metas: &[PoolMeta],
) -> String {
    let mut lines = Vec::new();
    lines.push(format!("Start: {}", token_label(arena, cycle.start_token)));
    for (i, edge) in cycle.edges.iter().enumerate() {
        let proto = protocol_label_for_edge(edge, pool_metas);
        let pool = arena
            .pool_address(edge.pool_index)
            .map(|a| short_addr(&a))
            .unwrap_or_else(|| format!("P{}", edge.pool_index.0));
        lines.push(format!(
            "  {}. {} → {}  [{}] pool={} fee={}bps",
            i + 1,
            token_label(arena, edge.token_in),
            token_label(arena, edge.token_out),
            proto,
            pool,
            edge.fee_bps
        ));
    }
    lines.push(format!(
        "Score: {:.6}  hops: {}  fees: {}bps",
        cycle.score, cycle.hop_count, cycle.cumulative_fee_bps
    ));
    lines.join("\n")
}

pub fn protocols_in_cycle(cycle: &FoundCycle, pool_metas: &[PoolMeta]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for edge in &cycle.edges {
        let label = protocol_label_for_edge(edge, pool_metas);
        if seen.insert(label.clone()) {
            out.push(label);
        }
    }
    out
}

pub fn liquidity_risk_score(cycle: &FoundCycle) -> f64 {
    let hops = cycle.hop_count as f64;
    let fee = cycle.cumulative_fee_bps as f64 / 10_000.0;
    (hops * 0.08 + fee).min(1.0)
}

pub fn cycle_to_ui_opportunity(
    arena: &StateArena,
    cycle: FoundCycle,
    pool_metas: &[PoolMeta],
    live_score: Option<f64>,
    freshness_ms: u64,
) -> UiOpportunity {
    let fingerprint = route_fingerprint(&cycle.edges);
    let route_summary = compact_route(arena, &cycle, pool_metas);
    let route_detail = detail_route_tree(arena, &cycle, pool_metas);
    let protocols = protocols_in_cycle(&cycle, pool_metas);
    let source_hub = token_label(arena, cycle.start_token);
    let call_count = route_call_count(&cycle.edges) as u32;
    let is_long_tail = cycle_has_long_tail(arena, &cycle);
    let liquidity_risk = liquidity_risk_score(&cycle);
    UiOpportunity {
        bf_score: cycle.score,
        live_score,
        cycle,
        fingerprint,
        route_summary,
        route_detail,
        protocols,
        est_profit_native: None,
        est_profit_usd: None,
        source_hub,
        call_count,
        freshness_ms,
        is_long_tail,
        liquidity_risk,
    }
}

pub fn format_score_delta(bf: f64, live: Option<f64>) -> String {
    match live {
        Some(l) => {
            let d = l - bf;
            if d.abs() < 1e-9 {
                "±0".into()
            } else {
                format!("{d:+.4}")
            }
        }
        None => "—".into(),
    }
}
