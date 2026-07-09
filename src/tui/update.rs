use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::sync::Arc;
use std::time::Instant;

use alloy::primitives::{Address, U256};
use alloy::sol_types::SolCall;
use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use crate::config::AppConfig;
use crate::core::types::FlashLoanSource;
use crate::core::types::{FoundCycle, PoolIndex, ProtocolType};
use crate::pipeline::local_sim::{estimate_route_gas, simulate_route_minimal};
use crate::pipeline::sim_sanity::min_economic_amount_in;
use crate::pipeline::types::{PoolMeta, compare_cycle_score};
use crate::services::execution::profit::{AssessProfitInput, assess_profit};
use crate::services::hf_snapshot::HfSnapshot;
use crate::services::oracle::price_oracle::PriceOracle;
use crate::services::oracle::resolve_token_to_matic_rate;
use crate::services::state_refresh::StateRefreshService;
use crate::util::{ten_pow_u256_cached as ten_pow_u256, truncate_str, u256_to_f64};

use super::app::{
    App, DashboardSnapshot, GraphHealth, GraphHubRow, GraphSnapshot, InputMode, KeyValueRow,
    OverviewSnapshot, PortfolioRow, RouteStatus, RouteSummary, Severity, SimulationRow, SortMode,
    TradeRow,
};
use super::events::UiEvent;
use super::route_viz::{protocol_tag, route_fingerprint, short_address};
use crate::pipeline::types::pool_metas_by_index;
use crate::services::discovery::{
    discovered_pool_by_address, pool_protocol_by_address, protocol_for_arena_pool,
};

pub fn apply_event(app: &mut App, event: UiEvent) {
    match event {
        UiEvent::Input(input) => handle_input(app, &input),
        UiEvent::Snapshot(snapshot) => {
            app.set_snapshot(*snapshot);
        }
        UiEvent::LfTick {
            search_ms,
            discoveries,
            cycles,
        } => {
            app.apply_lf_sample(cycles, search_ms, discoveries);
        }
        UiEvent::HfTick {
            cycles_considered,
            profitable_count,
            best_profit_wei,
            elapsed_ms,
        } => {
            app.apply_hf_sample(
                cycles_considered,
                profitable_count,
                &best_profit_wei,
                elapsed_ms,
            );
        }
        UiEvent::GasUpdate { gwei } => {
            app.apply_gas_sample(gwei);
        }
        UiEvent::ExecutionOutcome {
            outcome,
            route_fingerprint,
        } => app.register_trade_outcome(outcome, route_fingerprint),
        UiEvent::Message { severity, message } => app.push_activity(severity, message),
        UiEvent::Shutdown => app.should_quit = true,
    }
}

fn handle_input(app: &mut App, event: &Event) {
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => handle_key(app, *key),
        Event::Paste(text) => handle_paste(app, text),
        Event::Resize(_, _)
        | Event::FocusGained
        | Event::FocusLost
        | Event::Key(_)
        | Event::Mouse(_) => {}
    }
}

fn handle_paste(app: &mut App, text: &str) {
    if app.input_mode != InputMode::Search {
        return;
    }
    app.last_input_at = Some(Instant::now());
    for ch in text.chars().filter(|c| !c.is_control()) {
        app.search.push(ch);
    }
    app.rebuild_route_view();
}

fn handle_key(app: &mut App, key: KeyEvent) {
    app.last_input_at = Some(Instant::now());

    match app.input_mode {
        InputMode::Search => match key.code {
            KeyCode::Esc => {
                app.clear_search();
            }
            KeyCode::Enter => {
                app.input_mode = InputMode::Normal;
            }
            KeyCode::Backspace => {
                app.search.pop();
                app.rebuild_route_view();
            }
            KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                app.search.push(c);
                app.rebuild_route_view();
            }
            _ => {}
        },
        InputMode::Normal => match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                app.should_quit = true;
            }
            KeyCode::Char('q') => app.should_quit = true,
            KeyCode::Char('?') => app.toggle_help(),
            KeyCode::Char('/') => {
                app.search.clear();
                app.input_mode = InputMode::Search;
            }
            KeyCode::Char('j') | KeyCode::Down => app.select_next(),
            KeyCode::Char('k') | KeyCode::Up => app.select_prev(),
            KeyCode::Char('h') | KeyCode::Left | KeyCode::BackTab => app.cycle_tab(-1),
            KeyCode::Char('l') | KeyCode::Right | KeyCode::Tab => app.cycle_tab(1),
            KeyCode::Char('g') if !key.modifiers.contains(KeyModifiers::SHIFT) => app.select_top(),
            KeyCode::Char('G') => app.select_bottom(),
            KeyCode::Char('f') => cycle_sort(app),
            KeyCode::Char('r') => {
                app.snapshot_refresh_pending = true;
                app.push_activity(Severity::Info, "manual refresh requested");
            }
            _ => {}
        },
    }
}

fn cycle_sort(app: &mut App) {
    app.sort_mode = match app.sort_mode {
        SortMode::Score => SortMode::Profit,
        SortMode::Profit => SortMode::Risk,
        SortMode::Risk => SortMode::Hops,
        SortMode::Hops => SortMode::Freshness,
        SortMode::Freshness => SortMode::Score,
    };
    app.rebuild_route_view();
    app.push_activity(
        Severity::Info,
        format!("sort = {:?}", app.sort_mode).replace("SortMode::", ""),
    );
}

#[derive(Clone)]
pub struct RuntimeSnapshotInput {
    pub started_at: Instant,
    pub snapshot: Arc<HfSnapshot>,
    pub arena: crate::pipeline::arena::StateArena,
    pub config: Arc<AppConfig>,
    pub refresh: Arc<StateRefreshService>,
    pub execution_trades: u64,
    pub execution_losses: u64,
    pub daily_pnl_wei: i128,
    pub total_profit_wei: i128,
    pub total_trade_count: u64,
    pub gas_gwei: Option<f64>,
    pub hypersync_height: Option<u64>,
    pub matic_usd: f64,
    pub portfolio_rows: Vec<PortfolioRow>,
    pub diagnostics: Vec<KeyValueRow>,
    pub config_rows: Vec<KeyValueRow>,
    pub history: Vec<TradeRow>,
    pub last_search_ms: u64,
    pub last_hf_ms: u64,
    pub last_profitable: usize,
    pub last_cycles_considered: usize,
    pub last_best_profit_wei: Option<String>,
    pub route_cache: Option<RouteBuildCache>,
}

#[derive(Clone)]
pub struct RouteBuildCache {
    pub generation: u64,
    pub opportunities: Arc<Vec<RouteSummary>>,
    pub simulations: Arc<Vec<SimulationRow>>,
}

pub async fn build_snapshot(input: RuntimeSnapshotInput) -> DashboardSnapshot {
    let overview = OverviewSnapshot {
        uptime: input.started_at.elapsed(),
        total_trades: input.execution_trades,
        total_losses: input.execution_losses,
        daily_pnl_wei: input.daily_pnl_wei,
        profitable_routes: input.last_profitable,
        discovered_pools: input.snapshot.discovered_pools.len(),
        routable_pools: input.snapshot.pool_metas.len(),
        cycle_count: input.snapshot.cycles.len(),
        search_ms: input.last_search_ms,
        hf_ms: input.last_hf_ms,
        gas_gwei: input.gas_gwei,
        win_rate: if input.execution_trades + input.execution_losses > 0 {
            input.execution_trades as f64 / (input.execution_trades + input.execution_losses) as f64
        } else {
            0.0
        },
        snapshot_age_ms: 0,
    };

    let graph = build_graph_snapshot(
        &input.snapshot,
        input.hypersync_height,
        input.refresh.indexer_lag_blocks(),
        input.refresh.is_indexer_stale(),
    );

    let (opportunities, simulations) = if let Some(cache) = input.route_cache {
        (
            Arc::clone(&cache.opportunities),
            Arc::clone(&cache.simulations),
        )
    } else {
        let opportunities = build_routes(
            &input.snapshot,
            &input.arena,
            input.matic_usd,
            input.gas_gwei,
            input.config.execution.slippage_bps,
            48,
        );
        let simulations = build_simulations(&opportunities);
        (Arc::new(opportunities), Arc::new(simulations))
    };

    DashboardSnapshot {
        generation: input.snapshot.generation,
        captured_at: Instant::now(),
        overview,
        graph,
        opportunities,
        simulations,
        portfolio: input.portfolio_rows,
        diagnostics: input.diagnostics,
        config: input.config_rows,
    }
}

fn build_graph_snapshot(
    snap: &HfSnapshot,
    hypersync_height: Option<u64>,
    indexer_lag_blocks: u64,
    stale_indexer: bool,
) -> GraphSnapshot {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for meta in snap.pool_metas.iter() {
        let label = meta
            .protocol_label
            .as_deref()
            .unwrap_or(match meta.protocol {
                ProtocolType::UniswapV2 => "uniswap-v2",
                ProtocolType::UniswapV3 => "uniswap-v3",
                ProtocolType::UniswapV4 => "uniswap-v4",
                ProtocolType::BalancerV2 => "balancer-v2",
                ProtocolType::CurveStable => "curve-stable",
                ProtocolType::CurveCrypto => "curve-crypto",
                ProtocolType::Dodo => "dodo",
                ProtocolType::Woofi => "woofi",
            });
        *counts.entry(label).or_default() += 1;
    }

    let mut token_counts: HashMap<u32, usize> = HashMap::new();
    for cycle in snap.cycles.iter().take(48) {
        *token_counts.entry(cycle.start_token.0).or_default() += 1;
        for edge in &cycle.edges {
            *token_counts.entry(edge.token_in.0).or_default() += 1;
            *token_counts.entry(edge.token_out.0).or_default() += 1;
        }
    }
    let mut out_degrees: Vec<(u32, usize)> = token_counts.into_iter().collect();
    out_degrees.sort_by_key(|b| std::cmp::Reverse(b.1));

    let hubs: Vec<GraphHubRow> = out_degrees
        .into_iter()
        .take(8)
        .map(|(idx, out_degree)| GraphHubRow {
            token: snap
                .arena
                .token_address(crate::core::types::TokenIndex(idx))
                .map_or_else(|| format!("t{idx}"), short_address),
            out_degree,
        })
        .collect();

    let protocol_count = counts.len();
    let protocol_counts = counts
        .into_iter()
        .map(|(key, value)| KeyValueRow {
            key: key.to_string(),
            value: value.to_string(),
            severity: Severity::Info,
        })
        .collect();

    let recent_discoveries = snap
        .discovered_pools
        .iter()
        .rev()
        .take(8)
        .map(|pool| KeyValueRow {
            key: pool.protocol_label.clone(),
            value: format!("{}  {}", short_address(pool.address), pool.pool_key),
            severity: Severity::Info,
        })
        .collect();

    GraphSnapshot {
        health: GraphHealth {
            graph_generation: snap.generation,
            token_count: snap.arena.token_count() as usize,
            pool_count: snap.arena.pool_count(),
            top_out_degree: hubs.first().map_or(0, |h| h.out_degree),
            protocol_count,
            indexer_lag_blocks,
            hypersync_height,
            stale_indexer,
        },
        protocol_counts,
        hubs,
        recent_discoveries,
    }
}

fn build_search_blob(route: &str, route_detail: &str, protocols: &str, fingerprint: u64) -> String {
    format!(
        "{} {} {} {:x}",
        route.to_ascii_lowercase(),
        route_detail.to_ascii_lowercase(),
        protocols.to_ascii_lowercase(),
        fingerprint
    )
}

fn build_routes(
    snapshot: &HfSnapshot,
    arena: &crate::pipeline::arena::StateArena,
    matic_usd: f64,
    gas_gwei: Option<f64>,
    slippage_bps: u64,
    limit: usize,
) -> Vec<RouteSummary> {
    let mut ranked: Vec<&FoundCycle> = snapshot.cycles.iter().map(|c| c.as_ref()).collect();
    ranked.sort_by(|a, b| compare_cycle_score(a, b));
    ranked.truncate(limit);

    let pool_metas = snapshot.pool_metas.as_ref();
    let discovered = snapshot.discovered_pools.as_ref();
    let protocol_by_address = pool_protocol_by_address(discovered);
    let discovered_by_address = discovered_pool_by_address(discovered);
    let meta_by_pool = pool_metas_by_index(pool_metas);
    let mut out = Vec::with_capacity(ranked.len());
    for cycle in ranked {
        if arena.token_address(cycle.start_token).is_none() {
            continue;
        }
        let rate =
            resolve_token_to_matic_rate(cycle.start_token, snapshot.token_to_matic_rates.as_ref());
        let decimals = arena.token_decimals(cycle.start_token);
        let amount_in = min_economic_amount_in(decimals, rate);
        // Negative graph score ⇒ candidate arb; skip sim for clearly unprofitable routes.
        let sim = if cycle.score < 0.0 {
            simulate_route_minimal(arena, &cycle.edges, amount_in)
        } else {
            None
        };
        let simulation_available = sim.is_some();
        let (amount_out_token, gross_profit_token, gas_estimate) = match sim {
            Some(ref sim) => (sim.amount_out, sim.profit, sim.total_gas),
            None => (U256::ZERO, U256::ZERO, estimate_route_gas(&cycle.edges)),
        };
        let amount_in_matic = rate_to_matic(amount_in, rate, decimals);
        let profit_matic = rate_to_matic(gross_profit_token, rate, decimals);
        let net_profit_matic = if let (Some(sim), Some(gwei)) = (sim.as_ref(), gas_gwei) {
            let gas_price = U256::from((gwei * 1e9).max(0.0) as u128);
            let assessment = assess_profit(&AssessProfitInput {
                gross_profit: sim.profit,
                amount_in,
                gas_units: sim.total_gas,
                gas_price_wei: gas_price,
                token_to_matic_rate: rate,
                token_decimals: decimals,
                hop_count: cycle.hop_count,
                min_profit_matic_wei: U256::ZERO,
                min_profit_roi_bps: 0,
                slippage_bps,
                flash_loan_source: FlashLoanSource::Balancer,
                safety_multiplier_bps: 0,
                profit_priority_alpha_bps: 0,
            });
            u256_to_f64(assessment.net_profit_after_gas_matic_wei)
        } else {
            profit_matic
        };
        let profit_usd = if matic_usd > 0.0 {
            net_profit_matic * matic_usd
        } else {
            0.0
        };
        let raw_score = cycle.score;
        let rescored = cycle.score;
        let (route, detail, protocols, liquidity_score, risk_score, tokens) = build_route_row_parts(
            arena,
            cycle,
            &meta_by_pool,
            &discovered_by_address,
            &protocol_by_address,
            rate,
        );
        let fingerprint = route_fingerprint(&cycle.edges);
        let search_blob = build_search_blob(&route, &detail, &protocols, fingerprint);
        let risk_score = risk_score
            .saturating_add(if sim.is_none() { 20 } else { 0 })
            .min(100);
        out.push(RouteSummary {
            fingerprint,
            route,
            route_detail: detail,
            search_blob,
            protocols,
            tokens,
            hops: cycle.hop_count as usize,
            raw_score,
            rescored,
            amount_in_token: format_amount(amount_in, decimals),
            amount_in_matic,
            amount_out_token: if simulation_available {
                format_amount(amount_out_token, decimals)
            } else {
                "n/a".to_string()
            },
            profit_matic,
            net_profit_matic,
            profit_usd,
            gas_estimate,
            risk_score,
            liquidity_score,
            long_tail: cycle.hop_count > 4 || risk_score >= 60,
            status: RouteStatus::New,
        });
    }
    out.sort_by(|a, b| b.rescored.total_cmp(&a.rescored));
    out
}

pub fn build_route_cache(
    snapshot: &HfSnapshot,
    arena: &crate::pipeline::arena::StateArena,
    matic_usd: f64,
) -> RouteBuildCache {
    let opportunities = build_routes(snapshot, arena, matic_usd, None, 0, 48);
    let simulations = build_simulations(&opportunities);
    RouteBuildCache {
        generation: snapshot.generation,
        opportunities: Arc::new(opportunities),
        simulations: Arc::new(simulations),
    }
}

fn build_simulations(routes: &[RouteSummary]) -> Vec<SimulationRow> {
    routes
        .iter()
        .take(12)
        .map(|route| {
            let amount_in = route.amount_in_token.clone();
            let amount_out = route.amount_out_token.clone();
            SimulationRow {
                fingerprint: route.fingerprint,
                route: route.route.clone(),
                amount_in,
                amount_out,
                gross_profit: format!("{:.4} MATIC", route.profit_matic),
                net_profit: format!("{:.4} MATIC", route.net_profit_matic),
                gas: route.gas_estimate,
                note: format!("{} hops, score {:.4}", route.hops, route.rescored),
            }
        })
        .collect()
}

fn build_route_row_parts(
    arena: &crate::pipeline::arena::StateArena,
    cycle: &FoundCycle,
    meta_by_pool: &rustc_hash::FxHashMap<PoolIndex, &PoolMeta>,
    discovered_by_address: &rustc_hash::FxHashMap<
        Address,
        &crate::services::discovery::DiscoveredPool,
    >,
    protocol_by_address: &rustc_hash::FxHashMap<Address, ProtocolType>,
    rate: U256,
) -> (String, String, String, u8, u8, Vec<Address>) {
    let mut route_parts = Vec::with_capacity(cycle.edges.len().saturating_add(1));
    let mut detail = String::new();
    let mut protocols = String::new();
    let mut tokens = Vec::with_capacity(cycle.edges.len().saturating_mul(2));
    let mut liquidity_score = 100u8;
    let mut risk_score = (cycle.hop_count as u8).saturating_mul(10);

    if rate.is_zero() {
        liquidity_score = liquidity_score.saturating_sub(35);
    }

    let start = arena
        .token_address(cycle.start_token)
        .map_or_else(|| format!("t{}", cycle.start_token.0), short_address);
    route_parts.push(start.clone());
    let _ = writeln!(detail, "start {start}");

    for (idx, edge) in cycle.edges.iter().enumerate() {
        let pool_addr = arena.pool_address(edge.pool_index);
        let protocol_type =
            protocol_for_arena_pool(arena, edge.pool_index, protocol_by_address, edge.protocol);
        let protocol = protocol_tag(protocol_type);
        let meta = meta_by_pool.get(&edge.pool_index).copied();
        if !protocols.is_empty() {
            protocols.push_str(" · ");
        }
        protocols.push_str(protocol);

        let route_token = arena
            .token_address(edge.token_out)
            .map_or_else(|| format!("t{}", edge.token_out.0), short_address);
        route_parts.push(format!("{protocol}:{route_token}"));

        let from = arena
            .token_address(edge.token_in)
            .map_or_else(|| format!("t{}", edge.token_in.0), short_address);
        let to = arena
            .token_address(edge.token_out)
            .map_or_else(|| format!("t{}", edge.token_out.0), short_address);
        let pool_label = pool_addr
            .and_then(|addr| discovered_by_address.get(&addr))
            .map(|pool| pool.protocol_label.as_str())
            .or_else(|| meta.and_then(|m| m.protocol_label.as_deref()))
            .unwrap_or(protocol);
        let _ = writeln!(
            detail,
            "{:>2} {protocol} {pool_label} {from} -> {to} fee {}bps",
            idx + 1,
            edge.fee_bps
        );

        if let Some(addr) = arena.token_address(edge.token_in) {
            tokens.push(addr);
        }
        if let Some(addr) = arena.token_address(edge.token_out) {
            tokens.push(addr);
        }

        liquidity_score = liquidity_score.saturating_sub(match protocol_type {
            ProtocolType::UniswapV2 | ProtocolType::UniswapV3 => 0,
            ProtocolType::UniswapV4 => 4,
            ProtocolType::BalancerV2 => 8,
            ProtocolType::CurveStable => 10,
            ProtocolType::CurveCrypto => 14,
            ProtocolType::Dodo | ProtocolType::Woofi => 12,
        });
        risk_score = risk_score.saturating_add(match protocol_type {
            ProtocolType::UniswapV2 | ProtocolType::UniswapV3 => 0,
            ProtocolType::BalancerV2 | ProtocolType::CurveStable => 8,
            ProtocolType::CurveCrypto | ProtocolType::Dodo | ProtocolType::Woofi => 14,
            ProtocolType::UniswapV4 => 12,
        });
    }

    let _ = write!(detail, "hops {} score {:.4}", cycle.hop_count, cycle.score);

    (
        route_parts.join(" -> "),
        detail,
        protocols,
        liquidity_score.max(5),
        risk_score.min(100),
        tokens,
    )
}

fn rate_to_matic(amount: U256, rate: U256, decimals: u8) -> f64 {
    if amount.is_zero() || rate.is_zero() {
        return 0.0;
    }
    let scale = ten_pow_u256(decimals);
    let scaled = amount.saturating_mul(rate);
    u256_to_f64(scaled) / u256_to_f64(scale)
}

fn format_amount(amount: U256, decimals: u8) -> String {
    if amount.is_zero() {
        return "0".to_string();
    }
    let scale = ten_pow_u256(decimals);
    let whole = amount / scale;
    let frac = amount % scale;
    if frac.is_zero() {
        return whole.to_string();
    }
    let fractional = format!("{frac:0>width$}", width = decimals as usize);
    let fractional = truncate_str(fractional.trim_end_matches('0'), 8);
    format!("{whole}.{fractional}")
}

pub async fn build_portfolio_rows(
    provider: Option<&alloy::providers::DynProvider>,
    oracle: &PriceOracle,
    snapshot: &HfSnapshot,
    token_addresses: &[Address],
    balance_account: Option<Address>,
) -> Vec<PortfolioRow> {
    const PORTFOLIO_LIMIT: usize = 12;
    let mut rows = Vec::with_capacity(PORTFOLIO_LIMIT.min(token_addresses.len()));

    if let (Some(provider), Some(account)) = (provider, balance_account) {
        let targets: Vec<Address> = token_addresses
            .iter()
            .take(PORTFOLIO_LIMIT)
            .copied()
            .collect();
        if targets.is_empty() {
            return rows;
        }
        let items: Vec<crate::pipeline::multicall::MulticallItem> = targets
            .iter()
            .map(|token| crate::pipeline::multicall::MulticallItem {
                target: *token,
                data: crate::pipeline::multicall::encode_call(
                    &crate::abis::IERC20Metadata::balanceOfCall { account },
                ),
            })
            .collect();
        if let Ok(results) = crate::pipeline::multicall::execute_multicall(provider, &items).await {
            for (token, bytes) in targets.iter().zip(results) {
                let balance = bytes
                    .and_then(|b| {
                        crate::abis::IERC20Metadata::balanceOfCall::abi_decode_returns(&b).ok()
                    })
                    .map_or(U256::ZERO, U256::from);
                let decimals = snapshot.token_decimals.get(token).copied().unwrap_or(18);
                let unit_usd = oracle.token_usd(token);
                let balance_units = u256_to_f64(balance) / u256_to_f64(ten_pow_u256(decimals));
                let usd = unit_usd.map(|price| price * balance_units);
                rows.push(PortfolioRow {
                    label: short_address(*token),
                    address: format!("{token}"),
                    balance: format_amount(balance, decimals),
                    usd: match usd {
                        Some(value) => format!("{value:.2} USD"),
                        None => "n/a".to_string(),
                    },
                    source: "erc20 balanceOf".to_string(),
                    severity: if balance.is_zero() {
                        Severity::Warn
                    } else {
                        Severity::Good
                    },
                });
            }
        }
    }
    rows
}

pub fn build_diagnostics(
    config: &AppConfig,
    refresh: &StateRefreshService,
    gas_gwei: Option<f64>,
    matic_usd: f64,
    hypersync_height: Option<u64>,
) -> Vec<KeyValueRow> {
    vec![
        kv(
            "lf interval",
            format!("{} ms", config.lf_interval_ms),
            Severity::Info,
        ),
        kv(
            "hf interval",
            format!("{} ms", config.hf_interval_ms),
            Severity::Info,
        ),
        kv(
            "indexer lag",
            format!("{} blocks", refresh.indexer_lag_blocks()),
            if refresh.is_indexer_stale() {
                Severity::Warn
            } else {
                Severity::Info
            },
        ),
        kv(
            "gas",
            gas_gwei.map_or_else(|| "n/a".to_string(), |g| format!("{g:.1} gwei")),
            Severity::Info,
        ),
        kv(
            "MATIC/USD",
            if matic_usd > 0.0 {
                format!("{matic_usd:.2}")
            } else {
                "n/a".to_string()
            },
            Severity::Info,
        ),
        kv(
            "hypersync height",
            hypersync_height.map_or_else(|| "n/a".to_string(), |h| h.to_string()),
            Severity::Info,
        ),
        kv(
            "discovery",
            if refresh.is_discovery_bootstrapping() {
                format!(
                    "bootstrapping ({} indexed)",
                    refresh.discovered_pool_count()
                )
            } else {
                format!("ready ({} pools)", refresh.discovered_pool_count())
            },
            if refresh.is_discovery_bootstrapping() {
                Severity::Warn
            } else {
                Severity::Info
            },
        ),
        kv(
            "cache size",
            refresh.cache_size().to_string(),
            Severity::Info,
        ),
        kv(
            "routable pools",
            refresh.routable_pool_count().to_string(),
            Severity::Info,
        ),
    ]
}

#[must_use]
pub fn build_config_rows(config: &AppConfig) -> Vec<KeyValueRow> {
    vec![
        kv(
            "execution mode",
            config.execution.mode.clone(),
            Severity::Info,
        ),
        kv(
            "max hops",
            config.routing.max_hops.to_string(),
            Severity::Info,
        ),
        kv(
            "hf sim cap",
            config.pipeline.hf_sim_cap.to_string(),
            Severity::Info,
        ),
        kv(
            "hf score cap",
            config.pipeline.hf_score_cap.to_string(),
            Severity::Info,
        ),
        kv(
            "stream enabled",
            config.pipeline.stream_enabled.to_string(),
            Severity::Info,
        ),
        kv(
            "min profit",
            config.execution.min_profit_matic_wei.clone(),
            Severity::Info,
        ),
        kv(
            "slippage",
            format!("{} bps", config.execution.slippage_bps),
            Severity::Info,
        ),
    ]
}

fn kv(key: impl Into<String>, value: impl Into<String>, severity: Severity) -> KeyValueRow {
    KeyValueRow {
        key: key.into(),
        value: value.into(),
        severity,
    }
}

#[cfg(test)]
mod tests {
    use super::format_amount;
    use alloy::primitives::U256;

    #[test]
    fn format_amount_preserves_fractional_leading_zeroes() {
        assert_eq!(format_amount(U256::from(1_005_000u64), 6), "1.005");
        assert_eq!(format_amount(U256::from(5_000u64), 6), "0.005");
    }
}
