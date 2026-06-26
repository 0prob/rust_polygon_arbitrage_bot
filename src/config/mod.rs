use alloy::primitives::Address;
use figment::{
    Figment,
    providers::{Format, Toml},
};
use ruint::aliases::U256;
use serde::Deserialize;

pub mod routing;
pub mod wallet;
pub use routing::CycleFinderKind;
pub use wallet::WalletSecrets;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RpcConfig {
    #[serde(default)]
    pub polygon_rpc_urls: Vec<String>,
    #[serde(default)]
    pub execution_rpc_url: String,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_batch_wait_ms")]
    pub batch_wait_ms: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    #[serde(default)]
    pub state_rpc_url: Option<String>,
    #[serde(default)]
    pub hyper_sync_url: Option<String>,
    /// WebSocket endpoint for filtered `eth_subscribe` pool log stream.
    #[serde(default)]
    pub wss_url: Option<String>,
    /// MEV-protected submission endpoint (Flashbots Protect, builder RPC, etc.).
    #[serde(default)]
    pub private_rpc_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RoutingConfig {
    #[serde(default = "default_max_hops")]
    pub max_hops: u32,
    #[serde(default = "default_ternary_search_iterations")]
    pub ternary_search_iterations: u32,
    #[serde(default = "default_enumeration_max_paths")]
    pub enumeration_max_paths: u32,
    #[serde(default = "default_cycle_finder")]
    pub cycle_finder: CycleFinderKind,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default = "default_execution_mode")]
    pub mode: String,
    #[serde(default)]
    pub executor_address: Option<Address>,
    /// Cleared after [`WalletSecrets::load`] — never retain raw key material in memory.
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
    /// When true, live submits require [`RpcConfig::private_rpc_url`].
    #[serde(default)]
    pub require_private_submit: bool,
    /// Share of expected profit (bps) added to priority fee cap at submit time.
    #[serde(default = "default_profit_priority_fee_alpha_bps")]
    pub profit_priority_fee_alpha_bps: u64,
    /// Net profit must exceed worst-case gas × this multiplier (bps). Default 30_000 = 3×.
    #[serde(default = "default_profit_safety_multiplier_bps")]
    pub profit_safety_multiplier_bps: u64,
    /// Pause execution when operator MATIC balance falls below this (wei, 18 decimals).
    #[serde(default = "default_min_operator_matic_wei")]
    pub min_operator_matic_wei: String,
    /// Trip global circuit breaker after this many consecutive execution failures.
    #[serde(default = "default_max_global_consecutive_failures")]
    pub max_global_consecutive_failures: u32,
    /// Minimum net ROI in basis points of borrow size (0 = disabled).
    #[serde(default)]
    pub min_profit_roi_bps: u64,
    /// Auto-resume execution after circuit breaker trip (seconds).
    #[serde(default = "default_circuit_breaker_cooldown_secs")]
    pub circuit_breaker_cooldown_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfig {
    #[serde(default = "default_lf_bootstrap_batch")]
    pub lf_bootstrap_batch: usize,
    #[serde(default = "default_lf_hot_batch")]
    pub lf_hot_batch: usize,
    #[serde(default = "default_lf_full_sweep_interval")]
    pub lf_full_sweep_interval: u64,
    #[serde(default = "default_hf_prefetch_count")]
    pub hf_prefetch_count: usize,
    #[serde(default = "default_hf_score_cap")]
    pub hf_score_cap: usize,
    #[serde(default = "default_hf_sim_cap")]
    pub hf_sim_cap: usize,
    #[serde(default = "default_hf_max_dispatch")]
    pub hf_max_dispatch: usize,
    /// Top profitable routes to preflight with `estimate_gas` before submission.
    #[serde(default = "default_hf_gas_estimate_top_n")]
    pub hf_gas_estimate_top_n: usize,
    #[serde(default = "default_graph_rebuild_interval")]
    pub graph_rebuild_interval: u64,
    /// Enable filtered WSS log stream for hot pool partial cache.
    #[serde(default)]
    pub stream_enabled: bool,
    /// Max WSS/V2+V3 pools tracked by the in-RAM partial cache + WSS subscriptions.
    #[serde(default = "default_stream_max_pools")]
    pub stream_max_pools: usize,
    /// Pause execution when indexer lag exceeds this many blocks behind chain head.
    #[serde(default = "default_indexer_max_lag_blocks")]
    pub indexer_max_lag_blocks: u64,
    /// Trip circuit breaker when indexer lag exceeds threshold.
    #[serde(default = "default_indexer_pause_on_lag")]
    pub indexer_pause_on_lag: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_hasura_url")]
    pub hasura_url: String,
    #[serde(default)]
    pub hasura_secret: Option<String>,
    #[serde(default = "default_discovery_interval_ms")]
    pub discovery_interval_ms: u64,
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct OracleConfig {
    #[serde(default = "default_pyth_hermes_url")]
    pub pyth_hermes_url: String,
    #[serde(default = "default_tick_word_range")]
    pub tick_word_range: i16,
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
            circuit_breaker_cooldown_secs: default_circuit_breaker_cooldown_secs(),
        }
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            lf_bootstrap_batch: default_lf_bootstrap_batch(),
            lf_hot_batch: default_lf_hot_batch(),
            lf_full_sweep_interval: default_lf_full_sweep_interval(),
            hf_prefetch_count: default_hf_prefetch_count(),
            hf_score_cap: default_hf_score_cap(),
            hf_sim_cap: default_hf_sim_cap(),
            hf_max_dispatch: default_hf_max_dispatch(),
            hf_gas_estimate_top_n: default_hf_gas_estimate_top_n(),
            graph_rebuild_interval: default_graph_rebuild_interval(),
            stream_enabled: default_stream_enabled(),
            stream_max_pools: default_stream_max_pools(),
            indexer_max_lag_blocks: default_indexer_max_lag_blocks(),
            indexer_pause_on_lag: default_indexer_pause_on_lag(),
        }
    }
}

fn default_hasura_url() -> String {
    "http://localhost:8080/v1/graphql".to_string()
}
fn default_request_timeout_ms() -> u64 {
    8_000
}
fn default_batch_wait_ms() -> u64 {
    16
}
fn default_batch_size() -> u32 {
    100
}
fn default_max_hops() -> u32 {
    5
}
fn default_ternary_search_iterations() -> u32 {
    12
}
fn default_enumeration_max_paths() -> u32 {
    5_000
}
fn default_cycle_finder() -> CycleFinderKind {
    CycleFinderKind::Hybrid
}
fn default_execution_mode() -> String {
    "dry-run".to_string()
}
fn default_min_profit_matic_wei() -> String {
    "100000000000000000".to_string()
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
    30_000
}
fn default_min_operator_matic_wei() -> String {
    "500000000000000000".to_string() // 0.5 MATIC gas runway
}
fn default_max_global_consecutive_failures() -> u32 {
    8
}
fn default_circuit_breaker_cooldown_secs() -> u64 {
    300
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
    100
}
fn default_hf_score_cap() -> usize {
    150
}
fn default_hf_sim_cap() -> usize {
    75
}
fn default_hf_max_dispatch() -> usize {
    8
}
fn default_hf_gas_estimate_top_n() -> usize {
    3
}
fn default_graph_rebuild_interval() -> u64 {
    60
}
fn default_stream_enabled() -> bool {
    false
}
fn default_stream_max_pools() -> usize {
    500
}
fn default_indexer_max_lag_blocks() -> u64 {
    200
}
fn default_indexer_pause_on_lag() -> bool {
    true
}
fn default_slippage_bps() -> u64 {
    50
}
fn default_flash_loan_source() -> String {
    "auto".to_string()
}
fn default_receipt_timeout_ms() -> u64 {
    30_000
}
fn default_receipt_poll_ms() -> u64 {
    500
}
fn default_discovery_interval_ms() -> u64 {
    60_000
}
fn default_lf_interval_ms() -> u64 {
    1_000
}
fn default_hf_interval_ms() -> u64 {
    200
}
fn default_max_multicall_calls() -> u32 {
    800
}

fn default_pyth_hermes_url() -> String {
    "https://hermes.pyth.network".to_string()
}

fn default_tick_word_range() -> i16 {
    4
}

impl Default for OracleConfig {
    fn default() -> Self {
        Self {
            pyth_hermes_url: default_pyth_hermes_url(),
            tick_word_range: default_tick_word_range(),
        }
    }
}

/// Load `.env` from the working directory (or `DOTENV_PATH` if set).
/// Existing process environment variables are not overwritten.
pub fn load_dotenv() {
    if let Ok(path) = std::env::var("DOTENV_PATH") {
        let _ = dotenvy::from_path(path);
        return;
    }
    let _ = dotenvy::dotenv();
}

fn env_var(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn parse_env_bool(raw: &str) -> Option<bool> {
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Apply flat environment variable overrides (see `.env.example`).
fn apply_env_overrides(config: &mut AppConfig) -> anyhow::Result<()> {
    if let Some(url) = env_var("HASURA_URL") {
        config.hasura_url = url;
    }

    if let Some(raw) = env_var("EXECUTION_MODE") {
        config.execution.mode = raw;
    }

    if let Some(raw) = env_var("MIN_PROFIT_MATIC_WEI") {
        config.execution.min_profit_matic_wei = raw;
    }
    if let Some(raw) = env_var("SLIPPAGE_BPS") {
        config.execution.slippage_bps = raw.parse()?;
    }
    if let Some(raw) = env_var("FLASH_LOAN_SOURCE") {
        config.execution.flash_loan_source = raw;
    }
    if let Some(raw) = env_var("RECEIPT_TIMEOUT_MS") {
        config.execution.receipt_timeout_ms = raw.parse()?;
    }
    if let Some(raw) = env_var("RECEIPT_POLL_MS") {
        config.execution.receipt_poll_ms = raw.parse()?;
    }
    if let Some(raw) = env_var("MAX_FLASH_LOAN_USD") {
        config.execution.max_flash_loan_usd = raw.parse()?;
    }
    if let Some(raw) = env_var("EXECUTION_DEADLINE_SECS") {
        config.execution.deadline_secs = raw.parse()?;
    }
    if let Some(raw) = env_var("REQUIRE_PRIVATE_SUBMIT")
        && let Some(v) = parse_env_bool(&raw)
    {
        config.execution.require_private_submit = v;
    }
    if let Some(raw) = env_var("PROFIT_PRIORITY_FEE_ALPHA_BPS") {
        config.execution.profit_priority_fee_alpha_bps = raw.parse()?;
    }
    if let Some(raw) = env_var("PROFIT_SAFETY_MULTIPLIER_BPS") {
        config.execution.profit_safety_multiplier_bps = raw.parse()?;
    }
    if let Some(raw) = env_var("MIN_OPERATOR_MATIC_WEI") {
        config.execution.min_operator_matic_wei = raw;
    }
    if let Some(raw) = env_var("MAX_GLOBAL_CONSECUTIVE_FAILURES") {
        config.execution.max_global_consecutive_failures = raw.parse()?;
    }
    if let Some(raw) = env_var("MIN_PROFIT_ROI_BPS") {
        config.execution.min_profit_roi_bps = raw.parse()?;
    }
    if let Some(raw) = env_var("CIRCUIT_BREAKER_COOLDOWN_SECS") {
        config.execution.circuit_breaker_cooldown_secs = raw.parse()?;
    }

    if let Some(raw) = env_var("LF_BOOTSTRAP_BATCH") {
        config.pipeline.lf_bootstrap_batch = raw.parse()?;
    }
    if let Some(raw) = env_var("LF_HOT_BATCH") {
        config.pipeline.lf_hot_batch = raw.parse()?;
    }
    if let Some(raw) = env_var("LF_FULL_SWEEP_INTERVAL") {
        config.pipeline.lf_full_sweep_interval = raw.parse()?;
    }
    if let Some(raw) = env_var("HF_PREFETCH_COUNT") {
        config.pipeline.hf_prefetch_count = raw.parse()?;
    }
    if let Some(raw) = env_var("HF_SCORE_CAP") {
        config.pipeline.hf_score_cap = raw.parse()?;
    }
    if let Some(raw) = env_var("HF_SIM_CAP") {
        config.pipeline.hf_sim_cap = raw.parse()?;
    }
    if let Some(raw) = env_var("HF_MAX_DISPATCH") {
        config.pipeline.hf_max_dispatch = raw.parse()?;
    }
    if let Some(raw) = env_var("HF_GAS_ESTIMATE_TOP_N") {
        config.pipeline.hf_gas_estimate_top_n = raw.parse()?;
    }
    if let Some(raw) = env_var("GRAPH_REBUILD_INTERVAL") {
        config.pipeline.graph_rebuild_interval = raw.parse()?;
    }
    if let Some(raw) = env_var("STREAM_ENABLED")
        && let Some(v) = parse_env_bool(&raw)
    {
        config.pipeline.stream_enabled = v;
    }
    if let Some(raw) = env_var("STREAM_MAX_POOLS") {
        config.pipeline.stream_max_pools = raw.parse()?;
    }
    if let Some(raw) = env_var("INDEXER_MAX_LAG_BLOCKS") {
        config.pipeline.indexer_max_lag_blocks = raw.parse()?;
    }
    if let Some(raw) = env_var("INDEXER_PAUSE_ON_LAG")
        && let Some(v) = parse_env_bool(&raw)
    {
        config.pipeline.indexer_pause_on_lag = v;
    }
    if let Some(url) = env_var("WSS_URL").or_else(|| env_var("POLYGON_WSS_URL")) {
        config.rpc.wss_url = Some(url);
    }

    if config.rpc.private_rpc_url.is_none() {
        config.rpc.private_rpc_url = env_var("PRIVATE_RPC_URL");
    }

    if let Some(raw) = env_var("ROUTING_MAX_HOPS") {
        config.routing.max_hops = raw.parse()?;
    }
    if let Some(raw) = env_var("TERNARY_SEARCH_ITERATIONS") {
        config.routing.ternary_search_iterations = raw.parse()?;
    }
    if let Some(raw) = env_var("ROUTING_ENUMERATION_MAX_PATHS") {
        config.routing.enumeration_max_paths = raw.parse()?;
    }
    if let Some(raw) = env_var("ROUTING_CYCLE_FINDER") {
        config.routing.cycle_finder = CycleFinderKind::parse_str(&raw)?;
    }

    if let Some(raw) = env_var("ORACLE_PYTH_HERMES_URL") {
        config.oracle.pyth_hermes_url = raw;
    }
    if let Some(raw) = env_var("TICK_WORD_RANGE") {
        config.oracle.tick_word_range = raw.parse()?;
    }

    if let Some(raw) = env_var("LF_INTERVAL_MS") {
        config.lf_interval_ms = raw.parse()?;
    }
    if let Some(raw) = env_var("HF_INTERVAL_MS") {
        config.hf_interval_ms = raw.parse()?;
    }
    if let Some(raw) = env_var("DISCOVERY_INTERVAL_MS") {
        config.discovery_interval_ms = raw.parse()?;
    }
    if let Some(raw) = env_var("MAX_MULTICALL_CALLS") {
        config.max_multicall_calls = raw.parse()?;
    }

    if config.rpc.execution_rpc_url.is_empty() {
        if let Some(url) = env_var("EXECUTION_RPC_URL") {
            config.rpc.execution_rpc_url = url;
        } else if let Some(url) = env_var("EXECUTION_RPC") {
            config.rpc.execution_rpc_url = url;
        }
    }

    if config.execution.executor_address.is_none()
        && let Some(addr) = env_var("EXECUTOR_ADDRESS")
    {
        config.execution.executor_address = Some(addr.parse()?);
    }

    if config.execution.private_key.is_none()
        && let Some(key) = env_var("PRIVATE_KEY")
    {
        config.execution.private_key = Some(key);
    }

    if config.hasura_secret.is_none() {
        config.hasura_secret = env_var("HASURA_SECRET");
    }

    if config.rpc.polygon_rpc_urls.is_empty() {
        if let Some(urls) = env_var("POLYGON_RPC_URLS") {
            config.rpc.polygon_rpc_urls = urls
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
        } else if let Some(url) = env_var("POLYGON_RPC_URL") {
            config.rpc.polygon_rpc_urls.push(url);
        }
    }

    if config.rpc.state_rpc_url.is_none() {
        config.rpc.state_rpc_url = env_var("STATE_RPC_URL");
    }

    Ok(())
}

impl AppConfig {
    pub fn load() -> anyhow::Result<Self> {
        load_dotenv();

        let mut figment = Figment::new();

        if let Ok(path) = std::env::var("CONFIG_PATH") {
            figment = figment.merge(Toml::file(path));
        } else if std::path::Path::new("config.toml").exists() {
            figment = figment.merge(Toml::file("config.toml"));
        }

        let mut config: AppConfig = figment.extract()?;
        apply_env_overrides(&mut config)?;

        config
            .execution
            .min_profit_matic_wei
            .parse::<U256>()
            .map_err(|_| {
                anyhow::anyhow!(
                    "invalid min_profit_matic_wei: {}",
                    config.execution.min_profit_matic_wei
                )
            })?;

        Ok(config)
    }

    /// Fail-fast validation after env/TOML merge and wallet load.
    pub fn validate(&self, wallet: &WalletSecrets) -> anyhow::Result<()> {
        if self.pipeline.hf_score_cap == 0 || self.pipeline.hf_sim_cap == 0 {
            anyhow::bail!("HF_SCORE_CAP and HF_SIM_CAP must be greater than zero");
        }
        if self.pipeline.hf_max_dispatch == 0 {
            anyhow::bail!("HF_MAX_DISPATCH must be greater than zero");
        }
        if self.routing.max_hops < 2 {
            anyhow::bail!("ROUTING_MAX_HOPS must be at least 2");
        }

        if self.is_dry_run() {
            if self.state_rpc_url().is_none() {
                tracing::warn!("no state RPC configured — pool refresh will be limited");
            }
            return Ok(());
        }

        if self.execution.executor_address.is_none() {
            anyhow::bail!("live mode requires EXECUTOR_ADDRESS");
        }
        if !wallet.has_signer() {
            anyhow::bail!("live mode requires PRIVATE_KEY or PRIVATE_KEY_FILE");
        }
        if self.state_rpc_url().is_none() {
            anyhow::bail!("live mode requires STATE_RPC_URL or POLYGON_RPC_URL");
        }
        if self.rpc.execution_rpc_url.is_empty() && self.rpc.private_rpc_url.is_none() {
            anyhow::bail!("live mode requires EXECUTION_RPC or PRIVATE_RPC_URL");
        }
        if self.execution.require_private_submit && self.rpc.private_rpc_url.is_none() {
            anyhow::bail!("REQUIRE_PRIVATE_SUBMIT is set but PRIVATE_RPC_URL is not configured");
        }
        Ok(())
    }

    /// Minimum net profit threshold in MATIC wei (18 decimals).
    pub fn min_profit_matic_wei(&self) -> &str {
        &self.execution.min_profit_matic_wei
    }

    pub fn is_dry_run(&self) -> bool {
        self.execution.mode.eq_ignore_ascii_case("dry-run")
    }

    pub fn state_rpc_url(&self) -> Option<&str> {
        self.rpc
            .state_rpc_url
            .as_deref()
            .or_else(|| self.rpc.polygon_rpc_urls.first().map(String::as_str))
            .or({
                if self.rpc.execution_rpc_url.is_empty() {
                    None
                } else {
                    Some(self.rpc.execution_rpc_url.as_str())
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_mode_env_override() {
        let key = "EXECUTION_MODE";
        let prev = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, "live");
        }

        let mut config = AppConfig {
            hasura_url: default_hasura_url(),
            hasura_secret: None,
            discovery_interval_ms: default_discovery_interval_ms(),
            lf_interval_ms: default_lf_interval_ms(),
            hf_interval_ms: default_hf_interval_ms(),
            max_multicall_calls: default_max_multicall_calls(),
            rpc: RpcConfig::default(),
            routing: RoutingConfig::default(),
            execution: ExecutionConfig::default(),
            oracle: OracleConfig::default(),
            pipeline: PipelineConfig::default(),
        };

        apply_env_overrides(&mut config).expect("env overrides");
        assert_eq!(config.execution.mode, "live");

        unsafe {
            match prev {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
        }
    }

    #[test]
    fn routing_and_oracle_env_overrides_apply() {
        let keys = [
            ("ROUTING_MAX_HOPS", "6"),
            ("TERNARY_SEARCH_ITERATIONS", "14"),
            ("TICK_WORD_RANGE", "8"),
        ];
        let prev: Vec<_> = keys
            .iter()
            .map(|(k, _)| (*k, std::env::var(k).ok()))
            .collect();

        unsafe {
            for (key, value) in keys {
                std::env::set_var(key, value);
            }
        }

        let mut config = AppConfig {
            hasura_url: default_hasura_url(),
            hasura_secret: None,
            discovery_interval_ms: default_discovery_interval_ms(),
            lf_interval_ms: default_lf_interval_ms(),
            hf_interval_ms: default_hf_interval_ms(),
            max_multicall_calls: default_max_multicall_calls(),
            rpc: RpcConfig::default(),
            routing: RoutingConfig::default(),
            execution: ExecutionConfig::default(),
            oracle: OracleConfig::default(),
            pipeline: PipelineConfig::default(),
        };

        apply_env_overrides(&mut config).expect("env overrides");
        assert_eq!(config.routing.max_hops, 6);
        assert_eq!(config.routing.ternary_search_iterations, 14);
        assert_eq!(config.oracle.tick_word_range, 8);

        unsafe {
            for (key, value) in prev {
                match value {
                    Some(v) => std::env::set_var(key, v),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}
