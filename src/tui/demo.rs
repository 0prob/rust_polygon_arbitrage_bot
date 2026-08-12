use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::{Address, U256};
use tokio::sync::mpsc;

use super::app::{
    DashboardSnapshot, GraphHealth, GraphHubRow, GraphSnapshot, HfPipelineRow, KeyValueRow,
    OverviewSnapshot, PortfolioRow, RouteStatus, RouteSummary, Severity, Tab, TradeRow,
};
use super::bridge::TuiBridge;
use super::events::UiEvent;
use super::terminal::TerminalGuard;
use super::update::apply_event;

pub fn build_demo_snapshot() -> Arc<DashboardSnapshot> {
    let now = Instant::now();

    let overview = OverviewSnapshot {
        uptime: Duration::from_secs(14250),
        total_trades: 42,
        total_losses: 1,
        daily_pnl_wei: 28_450_000_000_000_000_000i128, // ~28.45 MATIC
        profitable_routes: 14,
        discovered_pools: 3842,
        routable_pools: 2915,
        cycle_count: 18452,
        search_ms: 12,
        hf_ms: 4,
        gas_gwei: Some(32.5),
        win_rate: 0.976,
        snapshot_age_ms: 5,
        rates_age_ms: 120,
    };

    let graph_health = GraphHealth {
        graph_generation: 1420,
        token_count: 850,
        pool_count: 3842,
        top_out_degree: 64,
        protocol_count: 6,
        indexer_lag_blocks: 0,
        stale_indexer: false,
    };

    let graph_snapshot = Arc::new(GraphSnapshot {
        health: graph_health,
        protocol_counts: vec![
            KeyValueRow { key: "QuickSwap V2".into(), value: "1420 pools".into(), severity: Severity::Good },
            KeyValueRow { key: "Uniswap V3".into(), value: "1150 pools".into(), severity: Severity::Good },
            KeyValueRow { key: "SushiSwap V2".into(), value: "680 pools".into(), severity: Severity::Good },
            KeyValueRow { key: "Algebra V3".into(), value: "320 pools".into(), severity: Severity::Good },
            KeyValueRow { key: "Balancer V2".into(), value: "180 pools".into(), severity: Severity::Good },
            KeyValueRow { key: "Curve".into(), value: "92 pools".into(), severity: Severity::Good },
        ],
        hubs: vec![
            GraphHubRow { token: "WMATIC".into(), out_degree: 64 },
            GraphHubRow { token: "USDC".into(), out_degree: 58 },
            GraphHubRow { token: "USDT".into(), out_degree: 46 },
            GraphHubRow { token: "WETH".into(), out_degree: 42 },
            GraphHubRow { token: "DAI".into(), out_degree: 31 },
            GraphHubRow { token: "LINK".into(), out_degree: 19 },
        ],
        recent_discoveries: vec![
            KeyValueRow { key: "UniV3 WMATIC/USDC".into(), value: "0x8f3...e91".into(), severity: Severity::Info },
            KeyValueRow { key: "QuickV2 WMATIC/WETH".into(), value: "0x3a1...c04".into(), severity: Severity::Info },
            KeyValueRow { key: "Algebra USDC/USDT".into(), value: "0x1b4...f82".into(), severity: Severity::Info },
        ],
    });

    let dummy_addr1 = Address::repeat_byte(0x11);
    let dummy_addr2 = Address::repeat_byte(0x22);
    let dummy_addr3 = Address::repeat_byte(0x33);

    let opportunities = Arc::new(vec![
        RouteSummary {
            fingerprint: 0x9a8f12c34b,
            route: "WMATIC -> USDC -> WETH -> WMATIC".into(),
            route_detail: "QuickSwapV2 -> UniswapV3 -> SushiSwapV2".into(),
            search_blob: "wmatic usdc weth quickswapv2 uniswapv3 sushiswapv2".into(),
            protocols: "QuickV2, UniV3, SushiV2".into(),
            tokens: vec![dummy_addr1, dummy_addr2, dummy_addr3, dummy_addr1],
            hops: 3,
            raw_score: 98.5,
            rescored: 98.5,
            amount_in_token: "1000.00 WMATIC".into(),
            amount_in_matic: 1000.0,
            amount_out_token: "1008.45 WMATIC".into(),
            profit_matic: 8.45,
            net_profit_matic: 7.92,
            profit_usd: 4.12,
            gas_estimate: 210000,
            risk_score: 1,
            liquidity_score: 95,
            long_tail: false,
            status: RouteStatus::Hot,
        },
        RouteSummary {
            fingerprint: 0x8b7e65d43c,
            route: "USDC -> WMATIC -> USDT -> USDC".into(),
            route_detail: "UniswapV3 -> QuickSwapV2 -> AlgebraV3".into(),
            search_blob: "usdc wmatic usdt uniswapv3 quickswapv2 algebrav3".into(),
            protocols: "UniV3, QuickV2, AlgebraV3".into(),
            tokens: vec![dummy_addr2, dummy_addr1, dummy_addr3, dummy_addr2],
            hops: 3,
            raw_score: 92.1,
            rescored: 92.1,
            amount_in_token: "500.00 USDC".into(),
            amount_in_matic: 961.5,
            amount_out_token: "503.20 USDC".into(),
            profit_matic: 6.15,
            net_profit_matic: 5.68,
            profit_usd: 2.95,
            gas_estimate: 195000,
            risk_score: 2,
            liquidity_score: 90,
            long_tail: false,
            status: RouteStatus::New,
        },
        RouteSummary {
            fingerprint: 0x7c6d54e32f,
            route: "WMATIC -> WETH -> LINK -> WMATIC".into(),
            route_detail: "SushiSwapV2 -> UniswapV3 -> QuickSwapV2".into(),
            search_blob: "wmatic weth link sushiswapv2 uniswapv3 quickswapv2".into(),
            protocols: "SushiV2, UniV3, QuickV2".into(),
            tokens: vec![dummy_addr1, dummy_addr3, dummy_addr2, dummy_addr1],
            hops: 3,
            raw_score: 87.4,
            rescored: 87.4,
            amount_in_token: "750.00 WMATIC".into(),
            amount_in_matic: 750.0,
            amount_out_token: "754.10 WMATIC".into(),
            profit_matic: 4.10,
            net_profit_matic: 3.58,
            profit_usd: 1.86,
            gas_estimate: 225000,
            risk_score: 2,
            liquidity_score: 84,
            long_tail: false,
            status: RouteStatus::Executed,
        },
        RouteSummary {
            fingerprint: 0x6a5b43c21d,
            route: "USDT -> WMATIC -> USDC -> USDT".into(),
            route_detail: "AlgebraV3 -> QuickSwapV2 -> UniswapV3".into(),
            search_blob: "usdt wmatic usdc algebrav3 quickswapv2 uniswapv3".into(),
            protocols: "AlgebraV3, QuickV2, UniV3".into(),
            tokens: vec![dummy_addr3, dummy_addr1, dummy_addr2, dummy_addr3],
            hops: 3,
            raw_score: 81.0,
            rescored: 81.0,
            amount_in_token: "400.00 USDT".into(),
            amount_in_matic: 769.2,
            amount_out_token: "401.85 USDT".into(),
            profit_matic: 3.55,
            net_profit_matic: 3.02,
            profit_usd: 1.57,
            gas_estimate: 200000,
            risk_score: 1,
            liquidity_score: 88,
            long_tail: false,
            status: RouteStatus::New,
        },
    ]);

    let portfolio = Arc::new(vec![
        PortfolioRow {
            label: "Operator EOA (Gas)".into(),
            address: "0x71C...89A".into(),
            balance: "145.82 MATIC".into(),
            usd: "$75.82".into(),
            source: "Native Balance".into(),
            severity: Severity::Good,
        },
        PortfolioRow {
            label: "Arbitrage Executor Contract".into(),
            address: "0x3eF...912".into(),
            balance: "1,250.00 WMATIC".into(),
            usd: "$650.00".into(),
            source: "Polygon Mainnet".into(),
            severity: Severity::Good,
        },
        PortfolioRow {
            label: "Flash Liquidity (Aave V3)".into(),
            address: "0x794...608".into(),
            balance: "10,000,000.00 USDC".into(),
            usd: "$10,000,000.00".into(),
            source: "Flash Loan Pool".into(),
            severity: Severity::Good,
        },
        PortfolioRow {
            label: "Flash Liquidity (Balancer V2)".into(),
            address: "0xBA1...01B".into(),
            balance: "5,000,000.00 WMATIC".into(),
            usd: "$2,600,000.00".into(),
            source: "Flash Vault".into(),
            severity: Severity::Good,
        },
    ]);

    let diagnostics = vec![
        KeyValueRow { key: "RPC Provider".into(), value: "Alchemy Polygon Mainnet (Healthy, 24ms)".into(), severity: Severity::Good },
        KeyValueRow { key: "HyperSync State".into(), value: "Connected (0 blocks behind tip)".into(), severity: Severity::Good },
        KeyValueRow { key: "Pyth Hermes Oracle".into(), value: "Active (Price feed latency 120ms)".into(), severity: Severity::Good },
        KeyValueRow { key: "Flash Loan Policy".into(), value: "AaveV3 -> BalancerV2 Fallback".into(), severity: Severity::Good },
        KeyValueRow { key: "Private Relay (bloxRoute)".into(), value: "Authenticated & Probed".into(), severity: Severity::Good },
        KeyValueRow { key: "Mempool Gate".into(), value: "Strict Anti-Frontrun Active".into(), severity: Severity::Good },
    ];

    let config = vec![
        KeyValueRow { key: "Network".into(), value: "Polygon Mainnet (Chain ID 137)".into(), severity: Severity::Info },
        KeyValueRow { key: "Min Net Profit Threshold".into(), value: "0.50 MATIC".into(), severity: Severity::Info },
        KeyValueRow { key: "Max Hop Depth".into(), value: "4 hops".into(), severity: Severity::Info },
        KeyValueRow { key: "Dry-Run Simulation".into(), value: "eth_call pre-execution enabled".into(), severity: Severity::Info },
        KeyValueRow { key: "Gas Oracle Buffer".into(), value: "1.15x priority fee multiplier".into(), severity: Severity::Info },
        KeyValueRow { key: "Execution Mode".into(), value: "Private Relay + Direct Mempool Fallback".into(), severity: Severity::Info },
    ];

    Arc::new(DashboardSnapshot {
        generation: 100,
        captured_at: now,
        overview,
        graph: graph_snapshot,
        opportunities,
        portfolio,
        diagnostics,
        config,
    })
}

pub async fn run_tui_demo(_bridge: TuiBridge, mut rx: mpsc::Receiver<UiEvent>) -> anyhow::Result<()> {
    let mut terminal = TerminalGuard::enter()?;
    let mut app = super::app::App::new();

    let snapshot = build_demo_snapshot();
    app.set_snapshot(snapshot.clone());

    // Populate seed activity log entries
    app.push_activity(Severity::Good, "System initialized in DEMO mode");
    app.push_activity(Severity::Info, "Indexed 3,842 pools across 6 DEX protocols");
    app.push_activity(Severity::Good, "Discovered profitable 3-hop opportunity: 0x9a8f12c34b (+7.92 MATIC)");
    app.push_activity(Severity::Good, "Dry-run simulation passed for 0x9a8f12c34b (Gas: 210,000)");

    // Seed trade history
    app.push_trade(TradeRow {
        at: Instant::now(),
        fingerprint: 0x9a8f12c34b,
        tokens: "WMATIC -> USDC -> WETH -> WMATIC".into(),
        route: "QuickV2 -> UniV3 -> SushiV2".into(),
        outcome: "Executed (Tx Confirmed)".into(),
        tx_hash: Some("0x1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b".into()),
        gas_used: Some(208450),
        profit_wei: Some(U256::from(7_920_000_000_000_000_000u64)),
        explorer_tx: Some("https://polygonscan.com/tx/0x1a2b...1a2b".into()),
        explorer_contract: Some("https://polygonscan.com/address/0x3eF...912".into()),
        severity: Severity::Good,
    });

    app.push_trade(TradeRow {
        at: Instant::now(),
        fingerprint: 0x7c6d54e32f,
        tokens: "WMATIC -> WETH -> LINK -> WMATIC".into(),
        route: "SushiV2 -> UniV3 -> QuickV2".into(),
        outcome: "Executed (Tx Confirmed)".into(),
        tx_hash: Some("0xf9e8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a2f1e0d9c8b7a6f5e4d3c2b1a0f9e8".into()),
        gas_used: Some(221000),
        profit_wei: Some(U256::from(3_580_000_000_000_000_000u64)),
        explorer_tx: Some("https://polygonscan.com/tx/0xf9e8...f9e8".into()),
        explorer_contract: Some("https://polygonscan.com/address/0x3eF...912".into()),
        severity: Severity::Good,
    });

    // Populate simulations / HF candidate view
    app.hf_candidates = vec![
        HfPipelineRow {
            fingerprint: 0x9a8f12c34b,
            hops: 3,
            route: "WMATIC -> USDC -> WETH -> WMATIC".into(),
            amount_in: "1000.00 WMATIC".into(),
            amount_out: "1008.45 WMATIC".into(),
            gross_profit: "8.45 MATIC".into(),
            net_profit_matic: "+7.920000".into(),
            gas: 210000,
            flash: "AaveV3".into(),
            should_execute: true,
            reject_reason: None,
            slip_bps: 5,
            near_miss: false,
            outcome: Some("DryRunPassed".into()),
            outcome_severity: Severity::Good,
        },
        HfPipelineRow {
            fingerprint: 0x8b7e65d43c,
            hops: 3,
            route: "USDC -> WMATIC -> USDT -> USDC".into(),
            amount_in: "500.00 USDC".into(),
            amount_out: "503.20 USDC".into(),
            gross_profit: "6.15 MATIC".into(),
            net_profit_matic: "+5.680000".into(),
            gas: 195000,
            flash: "BalancerV2".into(),
            should_execute: true,
            reject_reason: None,
            slip_bps: 8,
            near_miss: false,
            outcome: Some("DryRunPassed".into()),
            outcome_severity: Severity::Good,
        },
    ];

    app.rebuild_route_view();

    let tab_duration = Duration::from_secs(5);
    let mut tab_timer = tokio::time::interval(tab_duration);
    tab_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tab_timer.tick().await; // Initial immediate tick consume

    let mut tick_timer = tokio::time::interval(Duration::from_millis(250));

    let mut current_tab_idx = 0;
    let tabs = Tab::ALL;

    loop {
        tokio::select! {
            biased;
            maybe_event = rx.recv() => {
                let Some(event) = maybe_event else { break; };
                apply_event(&mut app, event);
                if app.should_quit {
                    break;
                }
            }
            _ = tab_timer.tick() => {
                current_tab_idx = (current_tab_idx + 1) % tabs.len();
                app.tab = tabs[current_tab_idx];
                app.select_top();
                app.push_activity(Severity::Info, format!("Demo mode: Switched to {} tab", app.tab.title()));
            }
            _ = tick_timer.tick() => {
                if app.route_view_is_dirty() {
                    app.rebuild_route_view();
                }
                super::run::draw_frame_blocking(&mut terminal, &app)?;
            }
        }
    }

    terminal.restore().ok();
    Ok(())
}
