//! Shared tracing helpers for correlating arb lifecycle events across async tasks.
//!
//! Every stage uses `route_fingerprint` as the primary correlation key so logs from
//! discovery → simulation → execution can be joined in `RUST_LOG` / JSON output.

use alloy::primitives::Address;
use tracing::Span;

use crate::core::types::{Edge, EvaluatedRoute, FoundCycle};
use crate::pipeline::arena::StateArena;
use crate::pipeline::types::route_fingerprint;
use crate::services::execution::candidate::CandidateExecution;

/// Pool addresses along a route, comma-separated for span fields.
pub fn pool_addrs_csv(arena: &StateArena, edges: &[Edge]) -> String {
    edges
        .iter()
        .filter_map(|e| arena.pool_address(e.pool_index))
        .map(|a| format!("{a}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Token addresses along a route (unique, in hop order).
pub fn token_addrs_csv(arena: &StateArena, edges: &[Edge]) -> String {
    let mut addrs = Vec::with_capacity(edges.len() + 1);
    if let Some(first) = edges.first().and_then(|e| arena.token_address(e.token_in)) {
        addrs.push(format!("{first}"));
    }
    for edge in edges {
        if let Some(a) = arena.token_address(edge.token_out) {
            addrs.push(format!("{a}"));
        }
    }
    addrs.join("→")
}

pub fn record_cycle_route(span: &Span, arena: &StateArena, cycle: &FoundCycle) {
    span.record("route_fingerprint", route_fingerprint(&cycle.edges));
    span.record("hop_count", cycle.hop_count);
    span.record("pool_addrs", pool_addrs_csv(arena, &cycle.edges));
    span.record("token_path", token_addrs_csv(arena, &cycle.edges));
}

pub fn record_evaluated_route(span: &Span, arena: &StateArena, evaluated: &EvaluatedRoute) {
    record_cycle_route(span, arena, &evaluated.cycle);
    span.record(
        "amount_in",
        tracing::field::display(&evaluated.result.amount_in),
    );
    span.record(
        "gross_profit",
        tracing::field::display(&evaluated.result.profit),
    );
    span.record("simulated_gas", evaluated.result.total_gas);
    if let Some(a) = &evaluated.assessment {
        span.record(
            "net_profit_matic_wei",
            tracing::field::display(&a.net_profit_after_gas_matic_wei),
        );
        span.record("should_execute", a.should_execute);
    }
}

pub fn record_candidate(span: &Span, candidate: &CandidateExecution) {
    span.record("route_fingerprint", candidate.route_fingerprint);
    span.record(
        "expected_profit_matic_wei",
        tracing::field::display(&candidate.expected_profit_matic_wei),
    );
    span.record("simulated_gas", candidate.simulated_gas);
    span.record("target", tracing::field::display(candidate.target_address));
    span.record(
        "profit_token",
        tracing::field::display(candidate.profit_token),
    );
    if let Some(limit) = candidate.gas_limit {
        span.record("gas_limit", tracing::field::display(&limit));
    }
}

pub fn record_gas_fees(
    span: &Span,
    max_fee: alloy::primitives::U256,
    priority_fee: alloy::primitives::U256,
) {
    span.record("max_fee_per_gas", tracing::field::display(&max_fee));
    span.record(
        "max_priority_fee_per_gas",
        tracing::field::display(&priority_fee),
    );
}

pub fn record_tx(span: &Span, tx_hash: &str, nonce: u64, gas_limit: u64) {
    span.record("tx_hash", tx_hash);
    span.record("nonce", nonce);
    span.record("gas_limit", gas_limit);
}

pub fn record_receipt(span: &Span, success: bool, gas_used: u64) {
    span.record("success", success);
    span.record("gas_used", gas_used);
}

/// Start token address for a cycle, if resolvable.
pub fn start_token_addr(arena: &StateArena, cycle: &FoundCycle) -> Option<Address> {
    arena.token_address(cycle.start_token)
}
