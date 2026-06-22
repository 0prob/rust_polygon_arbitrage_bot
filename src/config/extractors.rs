use crate::config::AppConfig;

/// Routing-specific configuration (for cycle finder, path enumeration, etc.)
#[derive(Debug, Clone)]
pub struct RoutingConfigView {
    pub max_hops: u32,
    pub ternary_search_iterations: u32,
    pub enumeration_max_paths: u32,
    pub cycle_finder: String,
}

impl RoutingConfigView {
    pub fn from_config(config: &AppConfig) -> Self {
        Self {
            max_hops: config.routing.max_hops,
            ternary_search_iterations: config.routing.ternary_search_iterations,
            enumeration_max_paths: config.routing.enumeration_max_paths,
            cycle_finder: config.routing.cycle_finder.to_string(),
        }
    }
}

/// Execution-specific configuration (profit targets, slippage, etc.)
#[derive(Debug, Clone)]
pub struct ExecutionConfigView {
    pub min_profit_matic_wei: String,
    pub slippage_bps: u64,
    pub mode: String,
    pub flash_loan_source: String,
    pub receipt_timeout_ms: u64,
    pub receipt_poll_ms: u64,
}

impl ExecutionConfigView {
    pub fn from_config(config: &AppConfig) -> Self {
        Self {
            min_profit_matic_wei: config.execution.min_profit_matic_wei.clone(),
            slippage_bps: config.execution.slippage_bps,
            mode: config.execution.mode.clone(),
            flash_loan_source: config.execution.flash_loan_source.clone(),
            receipt_timeout_ms: config.execution.receipt_timeout_ms,
            receipt_poll_ms: config.execution.receipt_poll_ms,
        }
    }
}

/// Discovery-specific configuration (intervals, batch sizes, etc.)
#[derive(Debug, Clone)]
pub struct DiscoveryConfigView {
    pub discovery_interval_ms: u64,
    pub lf_interval_ms: u64,
    pub hf_interval_ms: u64,
    pub max_multicall_calls: u32,
}

impl DiscoveryConfigView {
    pub fn from_config(config: &AppConfig) -> Self {
        Self {
            discovery_interval_ms: config.discovery_interval_ms,
            lf_interval_ms: config.lf_interval_ms,
            hf_interval_ms: config.hf_interval_ms,
            max_multicall_calls: config.max_multicall_calls,
        }
    }
}

/// RPC-specific configuration (endpoints, timeouts, batching)
#[derive(Debug, Clone)]
pub struct RpcConfigView {
    pub polygon_rpc_urls: Vec<String>,
    pub execution_rpc_url: String,
    pub request_timeout_ms: u64,
    pub batch_wait_ms: u64,
    pub batch_size: u32,
    pub state_rpc_url: Option<String>,
    pub hyper_sync_url: Option<String>,
}

impl RpcConfigView {
    pub fn from_config(config: &AppConfig) -> Self {
        Self {
            polygon_rpc_urls: config.rpc.polygon_rpc_urls.clone(),
            execution_rpc_url: config.rpc.execution_rpc_url.clone(),
            request_timeout_ms: config.rpc.request_timeout_ms,
            batch_wait_ms: config.rpc.batch_wait_ms,
            batch_size: config.rpc.batch_size,
            state_rpc_url: config.rpc.state_rpc_url.clone(),
            hyper_sync_url: config.rpc.hyper_sync_url.clone(),
        }
    }
}

/// Oracle-specific configuration (Pyth oracle settings)
#[derive(Debug, Clone)]
pub struct OracleConfigView {
    pub enabled: bool,
    pub pyth_hermes_url: String,
    pub tick_word_range: i16,
}

impl OracleConfigView {
    pub fn from_config(config: &AppConfig) -> Self {
        Self {
            enabled: config.oracle.enabled,
            pyth_hermes_url: config.oracle.pyth_hermes_url.clone(),
            tick_word_range: config.oracle.tick_word_range,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::CycleFinderKind;

    fn create_test_config() -> AppConfig {
        AppConfig {
            chain_id: 137,
            hasura_url: "http://localhost:8080/v1/graphql".to_string(),
            hasura_secret: None,
            discovery_interval_ms: 60_000,
            lf_interval_ms: 1_000,
            hf_interval_ms: 200,
            max_multicall_calls: 800,
            envio_api_token: None,
            rpc: crate::config::RpcConfig {
                polygon_rpc_urls: vec!["http://localhost:8545".to_string()],
                execution_rpc_url: "http://localhost:8545".to_string(),
                request_timeout_ms: 8_000,
                batch_wait_ms: 16,
                batch_size: 100,
                state_rpc_url: None,
                hyper_sync_url: None,
                wss_url: None,
                private_rpc_url: None,
            },
            routing: crate::config::RoutingConfig {
                max_hops: 5,
                ternary_search_iterations: 12,
                enumeration_max_paths: 5_000,
                cycle_finder: CycleFinderKind::BellmanFord,
            },
            execution: crate::config::ExecutionConfig {
                mode: "dry-run".to_string(),
                executor_address: None,
                private_key: None,
                min_profit_matic_wei: "100000000000000000".to_string(),
                slippage_bps: 50,
                flash_loan_source: "BALANCER".to_string(),
                receipt_timeout_ms: 30_000,
                receipt_poll_ms: 500,
                max_flash_loan_usd: 50_000,
                deadline_secs: 120,
                require_private_submit: false,
                profit_priority_fee_alpha_bps: 1_000,
                profit_safety_multiplier_bps: 30_000,
                min_operator_matic_wei: "500000000000000000".to_string(),
                max_global_consecutive_failures: 8,
                min_profit_roi_bps: 0,
                circuit_breaker_cooldown_secs: 300,
            },
            oracle: crate::config::OracleConfig {
                enabled: true,
                pyth_hermes_url: "https://hermes.pyth.network".to_string(),
                tick_word_range: 4,
            },
            pipeline: crate::config::PipelineConfig::default(),
        }
    }

    #[test]
    fn routing_config_view_extracts_correctly() {
        let config = create_test_config();
        let routing = RoutingConfigView::from_config(&config);
        assert_eq!(routing.max_hops, config.routing.max_hops);
        assert_eq!(routing.cycle_finder, config.routing.cycle_finder.to_string());
        assert_eq!(
            routing.ternary_search_iterations,
            config.routing.ternary_search_iterations
        );
        assert_eq!(
            routing.enumeration_max_paths,
            config.routing.enumeration_max_paths
        );
    }

    #[test]
    fn execution_config_view_extracts_correctly() {
        let config = create_test_config();
        let execution = ExecutionConfigView::from_config(&config);
        assert_eq!(execution.slippage_bps, config.execution.slippage_bps);
        assert_eq!(execution.min_profit_matic_wei, config.execution.min_profit_matic_wei);
        assert_eq!(execution.mode, config.execution.mode);
        assert_eq!(
            execution.flash_loan_source,
            config.execution.flash_loan_source
        );
        assert_eq!(
            execution.receipt_timeout_ms,
            config.execution.receipt_timeout_ms
        );
        assert_eq!(execution.receipt_poll_ms, config.execution.receipt_poll_ms);
    }

    #[test]
    fn discovery_config_view_extracts_correctly() {
        let config = create_test_config();
        let discovery = DiscoveryConfigView::from_config(&config);
        assert_eq!(
            discovery.discovery_interval_ms,
            config.discovery_interval_ms
        );
        assert_eq!(discovery.lf_interval_ms, config.lf_interval_ms);
        assert_eq!(discovery.hf_interval_ms, config.hf_interval_ms);
        assert_eq!(discovery.max_multicall_calls, config.max_multicall_calls);
    }

    #[test]
    fn rpc_config_view_extracts_correctly() {
        let config = create_test_config();
        let rpc = RpcConfigView::from_config(&config);
        assert_eq!(rpc.polygon_rpc_urls, config.rpc.polygon_rpc_urls);
        assert_eq!(rpc.execution_rpc_url, config.rpc.execution_rpc_url);
        assert_eq!(rpc.request_timeout_ms, config.rpc.request_timeout_ms);
        assert_eq!(rpc.batch_wait_ms, config.rpc.batch_wait_ms);
        assert_eq!(rpc.batch_size, config.rpc.batch_size);
    }

    #[test]
    fn oracle_config_view_extracts_correctly() {
        let config = create_test_config();
        let oracle = OracleConfigView::from_config(&config);
        assert_eq!(oracle.enabled, config.oracle.enabled);
        assert_eq!(oracle.pyth_hermes_url, config.oracle.pyth_hermes_url);
        assert_eq!(oracle.tick_word_range, config.oracle.tick_word_range);
    }
}
