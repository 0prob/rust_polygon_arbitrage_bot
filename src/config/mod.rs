use std::fs;
use std::path::PathBuf;

use crate::services::execution::flash_policy::{FlashLoanPolicy, parse_flash_policy};
use crate::services::oracle::price_oracle::DEFAULT_CACHE_TTL_MS;
use alloy::primitives::Address;
use alloy::primitives::U256;
use anyhow::{Context, ensure};
use figment::{
    Figment,
    providers::{Env, Serialized},
};
use serde::{Deserialize, Serialize};

pub mod wallet;
pub use wallet::WalletSecrets;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcConfig {
    #[serde(default)]
    pub polygon_rpc_urls: Vec<String>,
    #[serde(default)]
    pub execution_rpc_url: String,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default)]
    pub state_rpc_url: Option<String>,
    #[serde(default)]
    pub hyper_sync_url: Option<String>,
    #[serde(default)]
    pub wss_url: Option<String>,
    #[serde(default)]
    pub polygon_wss_urls: Vec<String>,
    #[serde(default)]
    pub private_rpc_url: Option<String>,
    #[serde(default = "default_rpc_batch_pace_ms")]
    pub batch_pace_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
#[derive(Default)]
pub enum CycleFinderMode {
    #[default]
    Hybrid,
    Dfs,
    Johnson,
    #[serde(alias = "bellman_ford", alias = "bellmanford")]
    BellmanFord,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RoutingConfig {
    #[serde(default = "default_max_hops")]
    pub max_hops: u32,
    #[serde(default = "default_ternary_search_iterations")]
    pub ternary_search_iterations: u32,
    #[serde(default = "default_enumeration_max_paths")]
    pub enumeration_max_paths: u32,
    #[serde(default = "default_cycle_finder")]
    pub cycle_finder: CycleFinderMode,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExecutionConfig {
    #[serde(default = "default_execution_mode")]
    pub mode: String,
    #[serde(default)]
    pub executor_address: Option<Address>,
    #[serde(default)]
    pub private_key: Option<String>,
    #[serde(default = "default_min_profit_matic_wei")]
    pub min_profit_matic_wei: String,
    #[serde(default = "default_slippage_bps")]
    pub slippage_bps: u64,
    #[serde(default = "default_flash_loan_source")]
    pub flash_loan_source: String,
    #[serde(default = "default_receipt_timeout_ms")]
    pub receipt_timeout_ms: u64,
    #[serde(default = "default_receipt_poll_ms")]
    pub receipt_poll_ms: u64,
    #[serde(default = "default_max_flash_loan_usd")]
    pub max_flash_loan_usd: u64,
    #[serde(default = "default_deadline_secs")]
    pub deadline_secs: u64,
    #[serde(default)]
    pub require_private_submit: bool,
    #[serde(default = "default_profit_priority_fee_alpha_bps")]
    pub profit_priority_fee_alpha_bps: u64,
    #[serde(default = "default_profit_safety_multiplier_bps")]
    pub profit_safety_multiplier_bps: u64,
    #[serde(default = "default_min_operator_matic_wei")]
    pub min_operator_matic_wei: String,
    #[serde(default = "default_max_global_consecutive_failures")]
    pub max_global_consecutive_failures: u32,
    #[serde(default)]
    pub min_profit_roi_bps: u64,
    #[serde(default)]
    pub max_daily_loss_matic_wei: String,
    #[serde(default = "default_route_stats_path")]
    pub route_stats_path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PipelineConfig {
    #[serde(default)]
    pub balancer_backend_url: Option<String>,
    #[serde(default = "default_lf_bootstrap_batch")]
    pub lf_bootstrap_batch: usize,
    #[serde(default = "default_lf_hot_batch")]
    pub lf_hot_batch: usize,
    #[serde(default = "default_lf_full_sweep_interval")]
    pub lf_full_sweep_interval: u64,
    #[serde(default = "default_hf_prefetch_count")]
    pub hf_prefetch_count: usize,
    #[serde(default = "default_hf_prefetch_budget_ms")]
    pub hf_prefetch_budget_ms: u64,
    #[serde(default = "default_hf_score_cap")]
    pub hf_score_cap: usize,
    #[serde(default = "default_hf_sim_cap")]
    pub hf_sim_cap: usize,
    #[serde(default = "default_hf_max_dispatch")]
    pub hf_max_dispatch: usize,
    #[serde(default = "default_graph_rebuild_interval")]
    pub graph_rebuild_interval: u64,
    #[serde(default = "default_cycle_refind_interval")]
    pub cycle_refind_interval: u64,
    #[serde(default)]
    pub pool_meta_cache_path: String,
    #[serde(default)]
    pub stream_enabled: bool,
    #[serde(default = "default_stream_max_pools")]
    pub stream_max_pools: usize,
    #[serde(default = "default_indexer_max_lag_blocks")]
    pub indexer_max_lag_blocks: u64,
    #[serde(default = "default_indexer_pause_on_lag")]
    pub indexer_pause_on_lag: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    #[serde(default = "default_pg_url")]
    pub pg_url: String,
    #[serde(default = "default_discovery_interval_ms")]
    pub discovery_interval_ms: u64,
    #[serde(default = "default_discovery_bootstrap_batch")]
    pub discovery_bootstrap_batch: usize,
    #[serde(default = "default_lf_interval_ms")]
    pub lf_interval_ms: u64,
    #[serde(default = "default_hf_interval_ms")]
    pub hf_interval_ms: u64,
    #[serde(default = "default_max_multicall_calls")]
    pub max_multicall_calls: u32,
    #[serde(default)]
    pub rpc: RpcConfig,
    #[serde(default)]
    pub routing: RoutingConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub oracle: OracleConfig,
    #[serde(default)]
    pub pipeline: PipelineConfig,
    #[serde(skip)]
    pub min_profit_matic: U256,
    #[serde(skip)]
    pub flash_policy: FlashLoanPolicy,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OracleConfig {
    #[serde(default = "default_pyth_hermes_url")]
    pub pyth_hermes_url: String,
    #[serde(default = "default_tick_word_range")]
    pub tick_word_range: i16,
    #[serde(default)]
    pub pyth_feeds: String,
    #[serde(default)]
    pub chainlink_feeds: String,
    /// Oracle USD price cache TTL in milliseconds (default 10_000 = 10s).
    #[serde(default = "default_cache_ttl_ms")]
    pub cache_ttl_ms: u64,
    /// Derive base token→MATIC rates from hub paths on the routing graph (arena sim).
    #[serde(default = "default_hub_path_rates")]
    pub hub_path_rates: bool,
    /// Max direct hops for hub-path base pricing (WMATIC target).
    #[serde(default = "default_hub_path_max_hops")]
    pub hub_path_max_hops: u32,
}

fn default_hub_path_rates() -> bool {
    true
}
fn default_hub_path_max_hops() -> u32 {
    4
}

fn default_pg_url() -> String {
    "postgres://postgres@localhost:5433/envio-dev".to_string()
}
fn default_discovery_interval_ms() -> u64 {
    5_000
}
fn default_discovery_bootstrap_batch() -> usize {
    20_000
}
fn default_lf_interval_ms() -> u64 {
    1_000
}
fn default_hf_interval_ms() -> u64 {
    150
}
fn default_max_multicall_calls() -> u32 {
    200
}
fn default_request_timeout_ms() -> u64 {
    30_000
}
fn default_rpc_batch_pace_ms() -> u64 {
    5
}
fn default_max_hops() -> u32 {
    5
}
fn default_ternary_search_iterations() -> u32 {
    12
}
fn default_enumeration_max_paths() -> u32 {
    // 13: target <1.5s at 1.3k+ (1.52s@1.1k w/15). Lower budget; fresh near-misses on CRV/BAL in history, monitor for dispatch on non-bad.
    13
}
fn default_cycle_finder() -> CycleFinderMode {
    CycleFinderMode::Hybrid
}
fn default_execution_mode() -> String {
    "dry-run".to_string()
}
fn default_min_profit_matic_wei() -> String {
    "100000000000000000".to_string()
}
fn default_slippage_bps() -> u64 {
    0
}
fn default_flash_loan_source() -> String {
    "auto".to_string()
}
fn default_receipt_timeout_ms() -> u64 {
    30_000
}
fn default_receipt_poll_ms() -> u64 {
    200
}
fn default_max_flash_loan_usd() -> u64 {
    50_000
}
fn default_deadline_secs() -> u64 {
    120
}
fn default_profit_priority_fee_alpha_bps() -> u64 {
    1_000
}
fn default_profit_safety_multiplier_bps() -> u64 {
    25_000
}
fn default_min_operator_matic_wei() -> String {
    "500000000000000000".to_string()
}
fn default_max_global_consecutive_failures() -> u32 {
    8
}
fn default_route_stats_path() -> String {
    ".rpbot-route-stats.json".to_string()
}
fn default_lf_bootstrap_batch() -> usize {
    3_000
}
fn default_lf_hot_batch() -> usize {
    500
}
fn default_lf_full_sweep_interval() -> u64 {
    10
}
fn default_hf_prefetch_count() -> usize {
    80
}
fn default_hf_prefetch_budget_ms() -> u64 {
    1_200
}
fn default_hf_score_cap() -> usize {
    120
}
fn default_hf_sim_cap() -> usize {
    120
}
fn default_hf_max_dispatch() -> usize {
    8
}
fn default_graph_rebuild_interval() -> u64 {
    60
}
fn default_cycle_refind_interval() -> u64 {
    crate::pipeline::graph_cache::default_cycle_refind_interval()
}
fn default_stream_max_pools() -> usize {
    // Fewer high-quality venues beat 500 dust shells that never emit Sync/Swap.
    200
}
fn default_indexer_max_lag_blocks() -> u64 {
    200
}
fn default_indexer_pause_on_lag() -> bool {
    true
}
fn default_pyth_hermes_url() -> String {
    "https://hermes.pyth.network".to_string()
}
fn default_tick_word_range() -> i16 {
    10
}
fn default_cache_ttl_ms() -> u64 {
    DEFAULT_CACHE_TTL_MS
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            polygon_rpc_urls: Vec::default(),
            execution_rpc_url: String::default(),
            request_timeout_ms: default_request_timeout_ms(),
            state_rpc_url: Option::default(),
            hyper_sync_url: Option::default(),
            wss_url: Option::default(),
            polygon_wss_urls: Vec::default(),
            private_rpc_url: Option::default(),
            batch_pace_ms: default_rpc_batch_pace_ms(),
        }
    }
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            max_hops: default_max_hops(),
            ternary_search_iterations: default_ternary_search_iterations(),
            enumeration_max_paths: default_enumeration_max_paths(),
            cycle_finder: default_cycle_finder(),
        }
    }
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            mode: default_execution_mode(),
            executor_address: None,
            private_key: None,
            min_profit_matic_wei: default_min_profit_matic_wei(),
            slippage_bps: default_slippage_bps(),
            flash_loan_source: default_flash_loan_source(),
            receipt_timeout_ms: default_receipt_timeout_ms(),
            receipt_poll_ms: default_receipt_poll_ms(),
            max_flash_loan_usd: default_max_flash_loan_usd(),
            deadline_secs: default_deadline_secs(),
            require_private_submit: false,
            profit_priority_fee_alpha_bps: default_profit_priority_fee_alpha_bps(),
            profit_safety_multiplier_bps: default_profit_safety_multiplier_bps(),
            min_operator_matic_wei: default_min_operator_matic_wei(),
            max_global_consecutive_failures: default_max_global_consecutive_failures(),
            min_profit_roi_bps: 0,
            max_daily_loss_matic_wei: String::new(),
            route_stats_path: default_route_stats_path(),
        }
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            balancer_backend_url: None,
            lf_bootstrap_batch: default_lf_bootstrap_batch(),
            lf_hot_batch: default_lf_hot_batch(),
            lf_full_sweep_interval: default_lf_full_sweep_interval(),
            hf_prefetch_count: default_hf_prefetch_count(),
            hf_prefetch_budget_ms: default_hf_prefetch_budget_ms(),
            hf_score_cap: default_hf_score_cap(),
            hf_sim_cap: default_hf_sim_cap(),
            hf_max_dispatch: default_hf_max_dispatch(),
            graph_rebuild_interval: default_graph_rebuild_interval(),
            cycle_refind_interval: default_cycle_refind_interval(),
            pool_meta_cache_path: ".rpbot-pool-meta.json".to_string(),
            stream_enabled: false,
            stream_max_pools: default_stream_max_pools(),
            indexer_max_lag_blocks: default_indexer_max_lag_blocks(),
            indexer_pause_on_lag: default_indexer_pause_on_lag(),
        }
    }
}

impl Default for OracleConfig {
    fn default() -> Self {
        Self {
            pyth_hermes_url: default_pyth_hermes_url(),
            tick_word_range: default_tick_word_range(),
            pyth_feeds: String::new(),
            chainlink_feeds: String::new(),
            cache_ttl_ms: default_cache_ttl_ms(),
            hub_path_rates: default_hub_path_rates(),
            hub_path_max_hops: default_hub_path_max_hops(),
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            pg_url: default_pg_url(),
            discovery_interval_ms: default_discovery_interval_ms(),
            discovery_bootstrap_batch: default_discovery_bootstrap_batch(),
            lf_interval_ms: default_lf_interval_ms(),
            hf_interval_ms: default_hf_interval_ms(),
            max_multicall_calls: default_max_multicall_calls(),
            rpc: RpcConfig::default(),
            routing: RoutingConfig::default(),
            execution: ExecutionConfig::default(),
            oracle: OracleConfig::default(),
            pipeline: PipelineConfig::default(),
            min_profit_matic: U256::ZERO,
            flash_policy: FlashLoanPolicy::Auto,
        }
    }
}

/// Load `.env` from the working directory (or `DOTENV_PATH` if set).
/// Existing process environment variables are not overwritten.
// ponytail: unsafe set_var; switch to dotenvy crate if env loading gets more complex
pub fn load_dotenv() {
    let path = std::env::var_os("DOTENV_PATH")
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".env"));
    let Ok(contents) = fs::read_to_string(&path) else {
        return;
    };
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() || std::env::var(key).is_ok() {
            continue;
        }
        let value = value.trim().trim_matches('"').trim_matches('\'');
        // ponytail: ignore set_var errors (e.g. key contains '=')
        // ponytail: Rust 2024 marks set_var unsafe — dotenv loader only
        unsafe {
            std::env::set_var(key, value);
        }
    }
}

pub(crate) fn env_var(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.trim().is_empty())
}

/// Map an env var name to a figment key path (see `Env::filter_map` docs).
// ponytail: manual mapping, switch to convention-based approach if it grows further
fn env_key_to_figment_path(key: &str) -> Option<&'static str> {
    Some(match key {
        k if k.eq_ignore_ascii_case("pg_url") => "pg_url",
        k if k.eq_ignore_ascii_case("lf_interval_ms") => "lf_interval_ms",
        k if k.eq_ignore_ascii_case("hf_interval_ms") => "hf_interval_ms",
        k if k.eq_ignore_ascii_case("discovery_interval_ms") => "discovery_interval_ms",
        k if k.eq_ignore_ascii_case("discovery_bootstrap_batch") => "discovery_bootstrap_batch",
        k if k.eq_ignore_ascii_case("max_multicall_calls") => "max_multicall_calls",
        k if k.eq_ignore_ascii_case("execution_mode") => "execution.mode",

        k if k.eq_ignore_ascii_case("slippage_bps") => "execution.slippage_bps",
        k if k.eq_ignore_ascii_case("flash_loan_source") => "execution.flash_loan_source",
        k if k.eq_ignore_ascii_case("receipt_timeout_ms") => "execution.receipt_timeout_ms",
        k if k.eq_ignore_ascii_case("receipt_poll_ms") => "execution.receipt_poll_ms",
        k if k.eq_ignore_ascii_case("max_flash_loan_usd") => "execution.max_flash_loan_usd",
        k if k.eq_ignore_ascii_case("execution_deadline_secs") => "execution.deadline_secs",
        k if k.eq_ignore_ascii_case("require_private_submit") => "execution.require_private_submit",
        k if k.eq_ignore_ascii_case("profit_priority_fee_alpha_bps") => {
            "execution.profit_priority_fee_alpha_bps"
        }
        k if k.eq_ignore_ascii_case("profit_safety_multiplier_bps") => {
            "execution.profit_safety_multiplier_bps"
        }

        k if k.eq_ignore_ascii_case("max_global_consecutive_failures") => {
            "execution.max_global_consecutive_failures"
        }
        k if k.eq_ignore_ascii_case("min_profit_roi_bps") => "execution.min_profit_roi_bps",

        k if k.eq_ignore_ascii_case("route_stats_path") => "execution.route_stats_path",
        k if k.eq_ignore_ascii_case("lf_bootstrap_batch") => "pipeline.lf_bootstrap_batch",
        k if k.eq_ignore_ascii_case("lf_hot_batch") => "pipeline.lf_hot_batch",
        k if k.eq_ignore_ascii_case("lf_full_sweep_interval") => "pipeline.lf_full_sweep_interval",
        k if k.eq_ignore_ascii_case("hf_prefetch_count") => "pipeline.hf_prefetch_count",
        k if k.eq_ignore_ascii_case("hf_prefetch_budget_ms") => "pipeline.hf_prefetch_budget_ms",
        k if k.eq_ignore_ascii_case("hf_score_cap") => "pipeline.hf_score_cap",
        k if k.eq_ignore_ascii_case("hf_sim_cap") => "pipeline.hf_sim_cap",
        k if k.eq_ignore_ascii_case("hf_max_dispatch") => "pipeline.hf_max_dispatch",
        k if k.eq_ignore_ascii_case("graph_rebuild_interval") => "pipeline.graph_rebuild_interval",
        k if k.eq_ignore_ascii_case("cycle_refind_interval") => "pipeline.cycle_refind_interval",
        k if k.eq_ignore_ascii_case("stream_enabled") => "pipeline.stream_enabled",
        k if k.eq_ignore_ascii_case("stream_max_pools") => "pipeline.stream_max_pools",
        k if k.eq_ignore_ascii_case("indexer_max_lag_blocks") => "pipeline.indexer_max_lag_blocks",
        k if k.eq_ignore_ascii_case("indexer_pause_on_lag") => "pipeline.indexer_pause_on_lag",
        k if k.eq_ignore_ascii_case("pool_meta_cache_path") => "pipeline.pool_meta_cache_path",
        k if k.eq_ignore_ascii_case("balancer_backend_url") => "pipeline.balancer_backend_url",
        k if k.eq_ignore_ascii_case("routing_max_hops") => "routing.max_hops",
        k if k.eq_ignore_ascii_case("ternary_search_iterations") => {
            "routing.ternary_search_iterations"
        }
        k if k.eq_ignore_ascii_case("routing_enumeration_max_paths") => {
            "routing.enumeration_max_paths"
        }
        k if k.eq_ignore_ascii_case("routing_cycle_finder") => "routing.cycle_finder",
        k if k.eq_ignore_ascii_case("oracle_pyth_hermes_url") => "oracle.pyth_hermes_url",
        k if k.eq_ignore_ascii_case("tick_word_range") => "oracle.tick_word_range",
        k if k.eq_ignore_ascii_case("oracle_cache_ttl_ms") => "oracle.cache_ttl_ms",
        k if k.eq_ignore_ascii_case("oracle_pyth_feeds") => "oracle.pyth_feeds",
        k if k.eq_ignore_ascii_case("oracle_chainlink_feeds") => "oracle.chainlink_feeds",
        k if k.eq_ignore_ascii_case("rpc_batch_pace_ms") => "rpc.batch_pace_ms",
        k if k.eq_ignore_ascii_case("request_timeout_ms") => "rpc.request_timeout_ms",
        k if k.eq_ignore_ascii_case("rpc_request_timeout_ms") => "rpc.request_timeout_ms",
        k if k.eq_ignore_ascii_case("execution_rpc") => "rpc.execution_rpc_url",
        k if k.eq_ignore_ascii_case("execution_rpc_url") => "rpc.execution_rpc_url",
        k if k.eq_ignore_ascii_case("executor_address") => "execution.executor_address",
        k if k.eq_ignore_ascii_case("hyper_sync_url") => "rpc.hyper_sync_url",
        k if k.eq_ignore_ascii_case("wss_url") => "rpc.wss_url",
        // POLYGON_RPC_URLS / POLYGON_WSS_URLS are comma-split in apply_conditional_env_overrides.
        k if k.eq_ignore_ascii_case("private_rpc_url") => "rpc.private_rpc_url",
        k if k.eq_ignore_ascii_case("state_rpc_url") => "rpc.state_rpc_url",
        _ => return None,
    })
}

fn env_provider() -> Env {
    Env::raw().filter_map(|key| env_key_to_figment_path(key.as_str()).map(Into::into))
}

fn dedupe_preserve_order(urls: &mut Vec<String>) {
    let mut seen = std::collections::HashSet::with_capacity(urls.len());
    urls.retain(|url| !url.is_empty() && seen.insert(url.clone()));
}

fn split_rpc_urls(raw: &str) -> Vec<String> {
    let mut urls = Vec::with_capacity(raw.as_bytes().iter().filter(|&&b| b == b',').count() + 1);
    urls.extend(
        raw.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from),
    );
    urls
}

/// Env overrides that need comma-split lists or aliases not covered by figment paths.
fn apply_conditional_env_overrides(config: &mut AppConfig) -> anyhow::Result<()> {
    if let Some(url) = env_var("POLYGON_WSS_URL") {
        config.rpc.wss_url = Some(url);
    }
    if let Some(urls) = env_var("POLYGON_WSS_URLS") {
        config.rpc.polygon_wss_urls = split_rpc_urls(&urls);
    }
    if let Some(url) = env_var("EXECUTION_RPC_URL").or_else(|| env_var("EXECUTION_RPC")) {
        config.rpc.execution_rpc_url = url;
    }
    if let Some(addr) = env_var("EXECUTOR_ADDRESS") {
        config.execution.executor_address = Some(addr.parse()?);
    }
    if let Some(key) = env_var("PRIVATE_KEY") {
        config.execution.private_key = Some(key);
    }
    if let Some(wei) = env_var("MIN_PROFIT_MATIC_WEI") {
        config.execution.min_profit_matic_wei = wei;
    }
    if let Some(wei) = env_var("MIN_OPERATOR_MATIC_WEI") {
        config.execution.min_operator_matic_wei = wei;
    }
    if let Some(wei) = env_var("MAX_DAILY_LOSS_MATIC_WEI") {
        config.execution.max_daily_loss_matic_wei = wei;
    }
    if let Some(urls) = env_var("POLYGON_RPC_URLS") {
        config.rpc.polygon_rpc_urls = split_rpc_urls(&urls);
    } else if let Some(url) = env_var("POLYGON_RPC_URL") {
        config.rpc.polygon_rpc_urls = vec![url];
    }
    Ok(())
}

impl AppConfig {
    /// Figment provider chain: code defaults → environment (from process env / `.env` via [`load_dotenv`]).
    #[must_use]
    pub fn figment() -> Figment {
        Figment::from(Serialized::defaults(AppConfig::default())).merge(env_provider())
    }

    pub fn load() -> anyhow::Result<Self> {
        load_dotenv();

        let mut config: AppConfig = Self::figment()
            .extract_lossy()
            .map_err(|e| anyhow::anyhow!("config: {e}"))?;
        apply_conditional_env_overrides(&mut config)?;
        config.normalize();

        config.min_profit_matic = config
            .execution
            .min_profit_matic_wei
            .parse::<U256>()
            .with_context(|| {
                format!(
                    "invalid min_profit_matic_wei: {}",
                    config.execution.min_profit_matic_wei
                )
            })?;
        config.flash_policy = parse_flash_policy(&config.execution.flash_loan_source);

        Ok(config)
    }

    /// Coalesce duplicates and clamp values that commonly fight each other across env sources.
    pub fn normalize(&mut self) {
        dedupe_preserve_order(&mut self.rpc.polygon_rpc_urls);
        dedupe_preserve_order(&mut self.rpc.polygon_wss_urls);

        self.lf_interval_ms = self.lf_interval_ms.max(1);
        self.hf_interval_ms = self.hf_interval_ms.max(1);
        self.discovery_interval_ms = self.discovery_interval_ms.max(1);
        self.discovery_bootstrap_batch = self.discovery_bootstrap_batch.max(1);
        self.max_multicall_calls = self.max_multicall_calls.max(1);
        self.rpc.request_timeout_ms = self.rpc.request_timeout_ms.max(1);
        self.rpc.batch_pace_ms = self.rpc.batch_pace_ms.min(60_000);

        if self.pipeline.hf_score_cap < self.pipeline.hf_sim_cap {
            self.pipeline.hf_score_cap = self.pipeline.hf_sim_cap;
        }
        if self.pipeline.hf_max_dispatch > self.pipeline.hf_sim_cap {
            self.pipeline.hf_max_dispatch = self.pipeline.hf_sim_cap;
        }

        let mc = self.max_multicall_calls as usize;
        let prefetch_cap = mc.saturating_mul(2).min(200);
        if self.pipeline.hf_prefetch_count > prefetch_cap {
            self.pipeline.hf_prefetch_count = prefetch_cap;
        }
    }

    /// Non-fatal hints for configs that work but waste RPC budget or backlog LF/HF.
    pub fn warn_suboptimal(&self) {
        let state_urls = self.state_read_urls();
        if state_urls.len() > 1 && self.rpc.batch_pace_ms == 0 {
            crate::warn!(
                "RPC_BATCH_PACE_MS=0 with {} state RPCs — bursts may trigger rate limits; try 5–10ms",
                state_urls.len()
            );
        }
        if !self.rpc.execution_rpc_url.is_empty()
            && state_urls.iter().any(|u| u == &self.rpc.execution_rpc_url)
        {
            crate::warn!(
                "EXECUTION_RPC matches a state-read endpoint — multicall will burn premium execution quota; use a dedicated STATE_RPC_URL / POLYGON_RPC_URLS entry"
            );
        }
        if self.lf_interval_ms < 2_500 && !self.pipeline.stream_enabled {
            crate::warn!(
                "LF_INTERVAL_MS={} is below typical refresh latency (~2.5s) — LF may backlog",
                self.lf_interval_ms
            );
        }
        if self.routing.enumeration_max_paths > 1_000 {
            crate::warn!(
                "ROUTING_ENUMERATION_MAX_PATHS={} is high — cycle search may dominate LF ticks",
                self.routing.enumeration_max_paths
            );
        }
        if self.pipeline.hf_prefetch_count > 120 {
            crate::warn!(
                "HF_PREFETCH_COUNT={} is high — prefetch often times out; try 60–100",
                self.pipeline.hf_prefetch_count
            );
        }
        if self.rpc.request_timeout_ms > 20_000 {
            crate::debug!(
                "RPC request_timeout_ms={} is long — slow endpoints will block workers",
                self.rpc.request_timeout_ms
            );
        }
    }

    pub fn validate(&self, wallet: &WalletSecrets) -> anyhow::Result<()> {
        ensure!(
            self.pipeline.hf_score_cap > 0 && self.pipeline.hf_sim_cap > 0,
            "HF_SCORE_CAP and HF_SIM_CAP must be greater than zero"
        );
        ensure!(
            self.pipeline.hf_max_dispatch > 0,
            "HF_MAX_DISPATCH must be greater than zero"
        );
        ensure!(
            self.execution.slippage_bps < 10_000,
            "SLIPPAGE_BPS must be below 10000"
        );
        ensure!(
            self.execution.profit_priority_fee_alpha_bps <= 10_000,
            "PROFIT_PRIORITY_FEE_ALPHA_BPS must not exceed 10000"
        );
        // Dry-run may use 0 (min-profit only) to exercise eth_call; live keeps ≥1.0× gas.
        ensure!(
            if self.is_dry_run() {
                self.execution.profit_safety_multiplier_bps <= 100_000
            } else {
                (10_000..=100_000).contains(&self.execution.profit_safety_multiplier_bps)
            },
            "PROFIT_SAFETY_MULTIPLIER_BPS must be between 10000 and 100000 (or 0..=100000 in dry-run)"
        );
        ensure!(
            self.execution.max_flash_loan_usd > 0,
            "MAX_FLASH_LOAN_USD must be greater than zero"
        );
        ensure!(
            self.execution.deadline_secs > 0,
            "EXECUTION_DEADLINE_SECS must be greater than zero"
        );
        self.execution
            .min_operator_matic_wei
            .parse::<U256>()
            .context("MIN_OPERATOR_MATIC_WEI must be an unsigned integer")?;
        if !self.execution.max_daily_loss_matic_wei.trim().is_empty() {
            self.execution
                .max_daily_loss_matic_wei
                .parse::<U256>()
                .context("MAX_DAILY_LOSS_MATIC_WEI must be an unsigned integer")?;
        }
        if let Some(executor) = self.execution.executor_address {
            ensure!(
                executor != Address::ZERO,
                "EXECUTOR_ADDRESS must not be zero"
            );
        }
        ensure!(
            self.routing.max_hops >= 2,
            "ROUTING_MAX_HOPS must be at least 2"
        );
        ensure!(
            self.routing.max_hops <= crate::core::constants::HOP_CAP,
            "ROUTING_MAX_HOPS must not exceed {}",
            crate::core::constants::HOP_CAP
        );

        if self.is_dry_run() {
            return Ok(());
        }

        ensure!(
            self.execution
                .executor_address
                .is_some_and(|address| address != Address::ZERO),
            "live mode requires EXECUTOR_ADDRESS"
        );
        ensure!(
            wallet.has_signer(),
            "live mode requires PRIVATE_KEY or PRIVATE_KEY_FILE"
        );
        ensure!(
            !self.state_read_urls().is_empty(),
            "live mode requires STATE_RPC_URL or POLYGON_RPC_URL(S)"
        );
        ensure!(
            !self.rpc.execution_rpc_url.is_empty() || self.rpc.private_rpc_url.is_some(),
            "live mode requires EXECUTION_RPC or PRIVATE_RPC_URL"
        );
        let bloxroute_auth = std::env::var("BLOXROUTE_AUTH_HEADER")
            .ok()
            .is_some_and(|s| !s.is_empty());
        ensure!(
            !self.execution.require_private_submit
                || self.rpc.private_rpc_url.is_some()
                || bloxroute_auth,
            "REQUIRE_PRIVATE_SUBMIT is set but neither PRIVATE_RPC_URL nor BLOXROUTE_AUTH_HEADER is configured"
        );
        Ok(())
    }

    #[must_use]
    pub fn is_dry_run(&self) -> bool {
        self.execution.mode.eq_ignore_ascii_case("dry-run")
    }

    /// Ordered state-read endpoints used for multicall pool refresh (matches [`RpcPool`]).
    /// Never includes [`RpcConfig::execution_rpc_url`] so execution quota is not spent on multicall.
    #[must_use]
    pub fn state_read_urls(&self) -> Vec<String> {
        let exec = self.rpc.execution_rpc_url.trim();
        let mut urls = Vec::with_capacity(1 + self.rpc.polygon_rpc_urls.len());
        if let Some(url) = self.rpc.state_rpc_url.as_deref().filter(|u| !u.is_empty())
            && (exec.is_empty() || url != exec)
        {
            urls.push(url.to_string());
        }
        for url in &self.rpc.polygon_rpc_urls {
            if url.is_empty() || urls.iter().any(|u| u == url) {
                continue;
            }
            if !exec.is_empty() && url.as_str() == exec {
                continue;
            }
            urls.push(url.clone());
        }
        urls
    }

    #[must_use]
    pub fn state_rpc_url(&self) -> Option<&str> {
        self.rpc
            .state_rpc_url
            .as_deref()
            .filter(|u| !u.is_empty())
            .or_else(|| self.rpc.polygon_rpc_urls.first().map(String::as_str))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl CycleFinderMode {
        fn parse(raw: &str) -> anyhow::Result<Self> {
            let raw = raw.trim();
            if raw.eq_ignore_ascii_case("hybrid") {
                Ok(Self::Hybrid)
            } else if raw.eq_ignore_ascii_case("dfs") {
                Ok(Self::Dfs)
            } else if raw.eq_ignore_ascii_case("johnson") {
                Ok(Self::Johnson)
            } else if raw.eq_ignore_ascii_case("bellman-ford")
                || raw.eq_ignore_ascii_case("bellman_ford")
                || raw.eq_ignore_ascii_case("bellmanford")
            {
                Ok(Self::BellmanFord)
            } else {
                anyhow::bail!("unknown ROUTING_CYCLE_FINDER: {raw}")
            }
        }
    }

    #[test]
    fn default_config_has_discovery_bootstrap_batch() {
        let config = AppConfig::default();
        assert_eq!(config.discovery_bootstrap_batch, 20_000);
    }

    #[test]
    fn default_config_uses_no_static_slippage_allowance() {
        assert_eq!(AppConfig::default().execution.slippage_bps, 0);
    }

    #[test]
    fn cycle_finder_mode_parses_aliases() {
        assert_eq!(
            CycleFinderMode::parse("bellman-ford").expect("bellman-ford alias should parse"),
            CycleFinderMode::BellmanFord
        );
        assert_eq!(
            CycleFinderMode::parse("HYBRID").expect("case-insensitive hybrid alias should parse"),
            CycleFinderMode::Hybrid
        );
    }

    #[test]
    fn state_read_urls_orders_primary_then_failover() {
        let mut config = AppConfig::default();
        config.rpc.state_rpc_url = Some("https://state.example".into());
        config.rpc.polygon_rpc_urls = vec![
            "https://poly-a.example".into(),
            "https://state.example".into(),
        ];
        assert_eq!(
            config.state_read_urls(),
            vec![
                "https://state.example".to_string(),
                "https://poly-a.example".to_string(),
            ]
        );
    }

    #[test]
    fn state_rpc_url_does_not_fall_back_to_execution_rpc() {
        let mut config = AppConfig::default();
        config.rpc.execution_rpc_url = "https://exec.example".into();
        assert!(config.state_read_urls().is_empty());
        assert!(config.state_rpc_url().is_none());
    }

    #[test]
    fn state_read_urls_skip_execution_rpc_duplicates() {
        let mut config = AppConfig::default();
        config.rpc.execution_rpc_url = "https://shared.drpc".into();
        config.rpc.state_rpc_url = Some("https://state.example".into());
        config.rpc.polygon_rpc_urls =
            vec!["https://shared.drpc".into(), "https://poly.example".into()];
        assert_eq!(
            config.state_read_urls(),
            vec![
                "https://state.example".to_string(),
                "https://poly.example".to_string(),
            ]
        );
    }

    #[test]
    fn live_validation_requires_state_read_urls_not_execution_only() {
        let mut config = AppConfig::default();
        config.execution.mode = "live".into();
        config.execution.executor_address = Some(Address::repeat_byte(0x11));
        config.execution.private_key =
            Some("0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78627d".into());
        config.rpc.execution_rpc_url = "https://exec.example".into();
        config.rpc.private_rpc_url = Some("https://private.example".into());
        let wallet = WalletSecrets::load(&mut config).expect("test wallet");
        let err = config
            .validate(&wallet)
            .expect_err("execution-only live config should fail");
        assert!(err.to_string().contains("STATE_RPC_URL"));
    }

    #[test]
    fn normalize_dedupes_rpc_urls_and_clamps_prefetch() {
        let mut config = AppConfig::default();
        config.rpc.polygon_rpc_urls = vec![
            "https://a.example".into(),
            "https://b.example".into(),
            "https://a.example".into(),
        ];
        config.max_multicall_calls = 50;
        config.pipeline.hf_prefetch_count = 500;
        config.pipeline.hf_sim_cap = 100;
        config.pipeline.hf_score_cap = 50;
        config.normalize();
        assert_eq!(config.rpc.polygon_rpc_urls.len(), 2);
        assert_eq!(config.pipeline.hf_prefetch_count, 100);
        assert_eq!(config.pipeline.hf_score_cap, 100);
        config.pipeline.hf_max_dispatch = 200;
        config.normalize();
        assert_eq!(config.pipeline.hf_max_dispatch, 100);
    }

    #[test]
    fn validation_rejects_unsafe_bps_and_zero_executor() {
        let wallet = WalletSecrets::dry_run();
        let mut config = AppConfig::default();

        config.execution.profit_priority_fee_alpha_bps = 10_001;
        assert!(config.validate(&wallet).is_err());

        config.execution.profit_priority_fee_alpha_bps = 1_000;
        config.execution.executor_address = Some(Address::ZERO);
        assert!(config.validate(&wallet).is_err());
    }

    #[test]
    #[allow(clippy::result_large_err)]
    fn figment_defaults_without_files() {
        figment::Jail::expect_with(|jail| {
            jail.clear_env();
            let config: AppConfig = AppConfig::figment().extract_lossy()?;
            assert_eq!(config.discovery_bootstrap_batch, 20_000);
            assert_eq!(config.execution.mode, "dry-run");
            assert_eq!(config.rpc.batch_pace_ms, 5);
            assert_eq!(config.rpc.request_timeout_ms, 30_000);
            Ok(())
        });
    }

    #[test]
    #[allow(clippy::result_large_err)]
    fn figment_env_overrides_rpc_and_pipeline() {
        figment::Jail::expect_with(|jail| {
            jail.clear_env();
            jail.set_env("RPC_REQUEST_TIMEOUT_MS", 10_000);
            jail.set_env("STREAM_ENABLED", "true");
            let config: AppConfig = AppConfig::figment().extract_lossy()?;
            assert_eq!(config.rpc.batch_pace_ms, 5);
            assert_eq!(config.rpc.request_timeout_ms, 10_000);
            assert!(config.pipeline.stream_enabled);
            Ok(())
        });
    }

    #[test]
    #[allow(clippy::result_large_err)]
    fn figment_env_overrides_defaults() {
        figment::Jail::expect_with(|jail| {
            jail.clear_env();
            jail.set_env("LF_INTERVAL_MS", 42);
            let config: AppConfig = AppConfig::figment().extract_lossy()?;
            assert_eq!(config.lf_interval_ms, 42);
            Ok(())
        });
    }

    #[test]
    #[allow(clippy::result_large_err)]
    fn env_polygon_rpc_urls_from_env_list() {
        figment::Jail::expect_with(|jail| {
            jail.clear_env();
            jail.set_env(
                "POLYGON_RPC_URLS",
                "https://env-a.example,https://env-b.example",
            );
            let mut config: AppConfig = AppConfig::figment().extract_lossy()?;
            apply_conditional_env_overrides(&mut config)
                .expect("conditional env overrides should succeed");
            config.normalize();
            assert_eq!(
                config.state_read_urls(),
                vec![
                    "https://env-a.example".to_string(),
                    "https://env-b.example".to_string(),
                ]
            );
            Ok(())
        });
    }

    #[test]
    #[allow(clippy::result_large_err)]
    fn split_polygon_wss_urls_from_env() {
        figment::Jail::expect_with(|jail| {
            jail.clear_env();
            jail.set_env("POLYGON_WSS_URLS", "wss://a.example,wss://b.example");
            let mut config: AppConfig = AppConfig::figment().extract_lossy()?;
            apply_conditional_env_overrides(&mut config)
                .expect("conditional env overrides should succeed");
            assert_eq!(
                config.rpc.polygon_wss_urls,
                vec!["wss://a.example".to_string(), "wss://b.example".to_string()]
            );
            Ok(())
        });
    }

    #[test]
    #[allow(clippy::result_large_err)]
    fn load_respects_dotenv_and_defaults() {
        figment::Jail::expect_with(|jail| {
            jail.clear_env();
            jail.create_file(
                ".env",
                "PROFIT_SAFETY_MULTIPLIER_BPS=25000\nMIN_PROFIT_MATIC_WEI=100000000000000000\nSTREAM_ENABLED=true\nEXECUTION_MODE=dry-run\n",
            )?;
            jail.set_env("DOTENV_PATH", ".env");
            let config =
                AppConfig::load().map_err(|e| figment::Error::from(format!("load: {e}")))?;
            assert!(config.pipeline.stream_enabled);
            assert_eq!(config.execution.profit_safety_multiplier_bps, 25000);
            assert_eq!(config.rpc.batch_pace_ms, 5);
            assert_eq!(config.execution.mode, "dry-run");
            Ok(())
        });
    }
}
