use std::collections::HashMap;

use crate::core::types::{Edge, FoundCycle, PoolIndex, ProtocolType, TokenIndex};
use crate::tui::app::{BotStatus, TradeStatus};
use crate::tui::bridge::UiBridge;
use crate::tui::update::{
    GraphStatsSnapshot, ScannerMetrics, UiOpportunity, UiUpdate, alert_info, alert_warn,
    trade_from_outcome,
};

const PROTO_LABELS: &[&str] = &[
    "UNISWAP_V2",
    "UNISWAP_V3",
    "QUICKSWAP_V2",
    "QUICKSWAP_V3",
    "SUSHI_V2",
    "SUSHI_V3",
    "KYBER_ELASTIC",
    "RAMSES_V3",
    "CURVE",
    "BALANCER",
    "DODO",
    "WOOFI",
    "DFYN",
    "APESWAP",
];

pub fn spawn_mock_updates(bridge: UiBridge) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        bridge.notify_status(BotStatus::Mock);
        bridge.try_send(UiUpdate::GasUpdate { gwei: 28.5 });
        bridge.try_send(UiUpdate::BlockUpdate {
            block: 65_432_100,
            lag_ms: 120,
        });
        bridge.try_send(UiUpdate::GraphStats(mock_graph_stats()));

        let mut tick: u64 = 0;
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        loop {
            interval.tick().await;
            tick += 1;

            let cycles = mock_cycles(tick);
            bridge.try_send(UiUpdate::NewCycles(cycles.clone()));

            let sims: Vec<_> = cycles
                .iter()
                .take(8)
                .map(|c| crate::tui::app::UiSimulation {
                    fingerprint: c.fingerprint,
                    route_summary: c.route_summary.clone(),
                    bf_score: c.bf_score,
                    live_score: c.live_score.unwrap_or(c.bf_score),
                    result: None,
                })
                .collect();
            for s in sims {
                let _ = s;
            }

            bridge.try_send(UiUpdate::MetricsUpdate(ScannerMetrics {
                negative_cycles: 12 + (tick as usize % 20),
                routes_executed: tick / 3,
                win_rate_pct: 62.0 + (tick % 10) as f64,
                avg_hops: 3.4,
                avg_profit_usd: 1.25 + (tick % 5) as f64 * 0.3,
                last_search_ms: 450 + (tick % 400),
                cycles_pass_limited: 80,
                cycles_pass_full: 240,
                bf_sources_used: 42,
                call_count_total: tick * 120,
                global_pnl_usd: (tick as f64) * 0.85,
            }));

            if tick % 5 == 0 {
                if let Some(c) = mock_cycles(tick).into_iter().next() {
                    bridge.try_send(UiUpdate::TradeExecuted(trade_from_outcome(
                    c.fingerprint,
                    c.route_summary,
                    c.cycle.hop_count,
                    c.protocols,
                    "0.42 MATIC".into(),
                    0.29,
                    380_000,
                    Some(format!("0xdead{:08x}", tick)),
                    if tick % 10 == 0 {
                        TradeStatus::Reverted
                    } else {
                        TradeStatus::DryRun
                    },
                    )));
                bridge.try_send(UiUpdate::PnlTick(0.29));
                }
            }

            bridge.try_send(UiUpdate::Alert(if tick % 7 == 0 {
                alert_warn(format!("Mock: high gas {:.1} gwei", 28.5 + tick as f64 * 0.1))
            } else {
                alert_info(format!("Mock scan #{tick} complete"))
            }));

            bridge.try_send(UiUpdate::BlockUpdate {
                block: 65_432_100 + tick,
                lag_ms: 80 + tick % 200,
            });
        }
    })
}

fn mock_graph_stats() -> GraphStatsSnapshot {
    let mut protocol_counts = HashMap::new();
    for (i, p) in PROTO_LABELS.iter().enumerate() {
        protocol_counts.insert((*p).into(), 120 + i * 17);
    }
    GraphStatsSnapshot {
        pool_count: 4_821,
        edge_count: 9_642,
        token_count: 892,
        top_hubs: vec![
            ("WMATIC".into(), 142),
            ("USDC".into(), 118),
            ("WETH".into(), 96),
            ("USDT".into(), 88),
            ("QUICK".into(), 64),
        ],
        protocol_counts,
        recent_discoveries: 3,
    }
}

fn mock_cycles(seed: u64) -> Vec<UiOpportunity> {
    (0..15)
        .map(|i| mock_opportunity(seed, i))
        .collect()
}

fn mock_opportunity(seed: u64, i: u64) -> UiOpportunity {
    let hops = 2 + ((seed + i) % 5) as u32;
    let mut edges = Vec::new();
    let start = TokenIndex((i % 50) as u32);
    let mut curr = start;
    for h in 0..hops {
        let proto_idx = ((seed + i + u64::from(h)) as usize) % PROTO_LABELS.len();
        let label = PROTO_LABELS[proto_idx];
        let protocol = if label.contains("V4") {
            ProtocolType::UniswapV4
        } else if label.contains("V3") || label.contains("ELASTIC") || label.contains("RAMSES") {
            ProtocolType::UniswapV3
        } else if label.contains("CURVE") {
            ProtocolType::CurveStable
        } else if label.contains("BALANCER") {
            ProtocolType::BalancerV2
        } else if label.contains("DODO") {
            ProtocolType::Dodo
        } else if label.contains("WOOFI") {
            ProtocolType::Woofi
        } else {
            ProtocolType::UniswapV2
        };
        let next = TokenIndex((curr.0 + 1 + h as u32) % 200);
        edges.push(Edge {
            pool_index: PoolIndex((u64::from(h) + i * 3) as u32),
            token_in: curr,
            token_out: next,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol,
            fee_bps: 30,
            zero_for_one: true,
        });
        curr = next;
    }

    let score = -0.002 - (i as f64 * 0.0004) - (seed as f64 * 0.00001);
    let live = score - 0.0002 * (i as f64);
    let cycle = FoundCycle {
        start_token: start,
        edges: edges.clone().into(),
        hop_count: hops,
        log_weight: score,
        cumulative_fee_bps: 30 * hops,
        score,
    };

    let proto_short = crate::tui::update::protocol_short_label(
        edges[0].protocol,
        Some(PROTO_LABELS[((seed + i) as usize) % PROTO_LABELS.len()]),
    );
    let mut route_parts = vec![format!("T{}", start.0)];
    for (h, e) in edges.iter().enumerate() {
        let pl = crate::tui::update::protocol_short_label(
            e.protocol,
            Some(PROTO_LABELS[((seed + i + h as u64) as usize) % PROTO_LABELS.len()]),
        );
        route_parts.push(format!("→[{pl}]"));
        route_parts.push(format!("T{}", e.token_out.0));
    }

    let fingerprint = crate::pipeline::types::route_fingerprint(&edges);
    UiOpportunity {
        cycle,
        fingerprint,
        route_summary: route_parts.join(" "),
        route_detail: format!("Mock route via {proto_short}\n{hops} hops, score {score:.6}"),
        protocols: vec![proto_short],
        bf_score: score,
        live_score: Some(live),
        est_profit_native: Some(format!("{:.4} MATIC", 0.1 + i as f64 * 0.05)),
        est_profit_usd: Some(0.07 + i as f64 * 0.03),
        source_hub: format!("T{}", start.0),
        call_count: hops * 2 + 4,
        freshness_ms: i * 100,
        is_long_tail: i % 3 == 0,
        liquidity_risk: 0.15 + (hops as f64 * 0.08),
    }
}
