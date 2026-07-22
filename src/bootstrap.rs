use std::sync::Arc;

use anyhow::Context;

use crate::config::{AppConfig, WalletSecrets};
use crate::infra::hypersync::{HyperSyncService, try_from_env};
use crate::orchestrator::{RuntimeContext, SharedUiHook};
use crate::services::state_refresh::StateRefreshService;

/// Load config + wallet, validate, and log startup summary.
pub fn load_config_and_wallet() -> anyhow::Result<(AppConfig, WalletSecrets)> {
    let mut config = AppConfig::load().context("failed to load configuration")?;
    let wallet = WalletSecrets::load(&mut config).context("failed to load wallet secrets")?;
    config.validate(&wallet).context("invalid configuration")?;
    Ok((config, wallet))
}

fn pg_host_label(url: &str) -> &str {
    // Extract host[:port] without scheme, path, query, or credentials (e.g. user:pass@).
    // Works for "postgres://user:pass@host:5432/db", "postgresql://host/db", or bare "host:5432".
    let without_scheme = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let after_auth = without_scheme
        .rsplit_once('@')
        .map(|(_, r)| r)
        .unwrap_or(without_scheme);
    after_auth
        .split(['/', '?', '#'])
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or(url)
}

pub fn log_startup(config: &AppConfig) {
    crate::info!(
        "rpbot starting (execution_mode={}, dry_run={}, cycle_finder={:?}, lf={}ms, hf={}ms, executor={:?})",
        config.execution.mode,
        config.is_dry_run(),
        config.routing.cycle_finder,
        config.lf_interval_ms,
        config.hf_interval_ms,
        config.execution.executor_address,
    );
    crate::info!(
        "discovery (pg={}, bootstrap_batch={}, interval={}ms)",
        pg_host_label(&config.pg_url),
        config.discovery_bootstrap_batch,
        config.discovery_interval_ms,
    );
    let pipeline = &config.pipeline;
    crate::info!(
        "pipeline (stream={}, hf_prefetch={}/{}ms, hf_sim={}/{}, enum_paths={})",
        pipeline.stream_enabled,
        pipeline.hf_prefetch_count,
        pipeline.hf_prefetch_budget_ms,
        pipeline.hf_sim_cap,
        pipeline.hf_score_cap,
        config.routing.enumeration_max_paths,
    );
    crate::info!(
        "rpc (state_urls={}, timeout={}ms, batch_pace={}ms, multicall_chunk={})",
        config.state_read_urls().len(),
        config.rpc.request_timeout_ms,
        config.rpc.batch_pace_ms,
        config.max_multicall_calls,
    );
    if let Some(dir) = crate::log::run_dir() {
        crate::info!("logs: {}", dir.display());
    }

    if config.state_read_urls().is_empty() {
        crate::warn!("no STATE_RPC_URL / POLYGON_RPC_URL(S) configured — pool refresh disabled");
    }
    config.warn_suboptimal();
}

/// Non-blocking postgres probe using the discovery pool (avoids a second `PgClient` at startup).
pub fn spawn_pg_probe(refresh: Arc<StateRefreshService>) {
    StateRefreshService::spawn_connectivity_probe(refresh);
}

/// Non-blocking connectivity probe; does not delay runtime startup.
/// If `hypersync` is `None`, the caller is expected to have logged the disabled state.
pub fn spawn_hypersync_probe(hypersync: Option<Arc<HyperSyncService>>) {
    if let Some(hs) = hypersync {
        tokio::spawn(async move {
            match hs.probe_height().await {
                Ok(height) => crate::info!("hypersync connected height={height}"),
                Err(e) => crate::warn!("hypersync height probe failed: {e}"),
            }
        });
    }
}

pub fn build_runtime(
    config: AppConfig,
    wallet: WalletSecrets,
    hypersync: Option<HyperSyncService>,
) -> anyhow::Result<RuntimeContext> {
    RuntimeContext::new(config, wallet, hypersync).context("failed to initialize runtime context")
}

struct BlockingBootstrap {
    runtime: RuntimeContext,
    hypersync_built: bool,
    envio_token_present: bool,
    config_ms: u64,
    runtime_ms: u64,
}

/// Config load, startup logs, hypersync client build, and runtime init on a blocking thread.
fn run_blocking_bootstrap() -> anyhow::Result<BlockingBootstrap> {
    let config_started = crate::util::now_ms();
    crate::debug!("bootstrap: loading config and wallet");
    let (config, wallet) = load_config_and_wallet()?;
    let config_ms = crate::util::now_ms().saturating_sub(config_started);
    crate::debug!("bootstrap: config loaded in {config_ms}ms");

    log_startup(&config);

    let envio_token_present = std::env::var("ENVIO_API_TOKEN")
        .ok()
        .is_some_and(|t| !t.trim().is_empty());
    let hypersync = try_from_env(&config.rpc);
    let hypersync_built = hypersync.is_some();

    let runtime_started = crate::util::now_ms();
    crate::debug!("bootstrap: building runtime context");
    let runtime = build_runtime(config, wallet, hypersync)?;
    let runtime_ms = crate::util::now_ms().saturating_sub(runtime_started);
    crate::debug!("bootstrap: runtime built in {runtime_ms}ms");

    Ok(BlockingBootstrap {
        runtime,
        hypersync_built,
        envio_token_present,
        config_ms,
        runtime_ms,
    })
}

/// Common bootstrap for both the rpbot and tui binaries.
/// Logs startup summary (including hypersync status), builds the runtime context (optionally
/// installing a UI hook for TUI), and spawns the non-blocking pg + hypersync connectivity probes
/// (their results log asynchronously).
///
/// Config/wallet load, hypersync client build, and `RuntimeContext::new` all run inside
/// `spawn_blocking` (`run_blocking_bootstrap`) so filesystem / sync init does not occupy an
/// async worker thread.
async fn bootstrap_inner(
    ui_hook: Option<SharedUiHook>,
    #[cfg(feature = "tui")] ui_snapshot_tx: Option<
        tokio::sync::watch::Sender<Option<Arc<crate::tui::app::DashboardSnapshot>>>,
    >,
) -> anyhow::Result<Arc<RuntimeContext>> {
    let bootstrap_started = crate::util::now_ms();
    let block = tokio::task::spawn_blocking(run_blocking_bootstrap)
        .await
        .context("bootstrap task panicked")??;

    match (block.hypersync_built, block.envio_token_present) {
        (false, true) => {
            crate::warn!("ENVIO_API_TOKEN set but hypersync client failed to build — disabled")
        }
        (false, false) => crate::info!("ENVIO_API_TOKEN not set — hypersync disabled"),
        _ => {}
    }

    let mut runtime = block.runtime;
    if let Some(hook) = ui_hook {
        runtime = runtime.with_ui_hook(hook);
    }
    #[cfg(feature = "tui")]
    if let Some(tx) = ui_snapshot_tx {
        runtime = runtime.with_ui_snapshot_tx(tx);
    }
    let ctx = Arc::new(runtime);

    let total_ms = crate::util::now_ms().saturating_sub(bootstrap_started);
    crate::info!(
        "bootstrap timing: config={}ms runtime_build={}ms total={}ms",
        block.config_ms,
        block.runtime_ms,
        total_ms
    );

    spawn_pg_probe(Arc::clone(&ctx.refresh));
    spawn_hypersync_probe(ctx.hypersync.clone());

    Ok(ctx)
}

/// Headless bin entry (`rpbot`). UI hook is optional for tests / custom drivers.
#[cfg(not(feature = "tui"))]
pub async fn bootstrap(ui_hook: Option<SharedUiHook>) -> anyhow::Result<Arc<RuntimeContext>> {
    bootstrap_inner(ui_hook).await
}

/// Shared by `tui` bin and headless `rpbot` when built with `--features tui`.
/// Pass `None, None` for headless; TUI supplies hook + snapshot channel.
#[cfg(feature = "tui")]
pub async fn bootstrap(
    ui_hook: Option<SharedUiHook>,
    ui_snapshot_tx: Option<
        tokio::sync::watch::Sender<Option<Arc<crate::tui::app::DashboardSnapshot>>>,
    >,
) -> anyhow::Result<Arc<RuntimeContext>> {
    bootstrap_inner(ui_hook, ui_snapshot_tx).await
}

/// Production headless bootstrap (no UI hook / snapshot channel).
///
/// Hides the `tui` feature's dual `bootstrap` signature from `src/main.rs`.
pub async fn bootstrap_headless() -> anyhow::Result<Arc<RuntimeContext>> {
    #[cfg(feature = "tui")]
    {
        bootstrap(None, None).await
    }
    #[cfg(not(feature = "tui"))]
    {
        bootstrap(None).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke timing for local profiling (`cargo test bootstrap_runtime_build_smoke -- --ignored`).
    #[test]
    #[ignore = "requires project .env"]
    fn bootstrap_runtime_build_smoke() {
        let block = run_blocking_bootstrap().expect("blocking bootstrap");
        assert!(block.config_ms < 60_000);
        assert!(block.runtime_ms < 60_000);
        assert!(!block.runtime.config.pg_url.is_empty());
    }

    #[test]
    fn pg_host_label_strips_credentials_and_scheme() {
        assert_eq!(
            pg_host_label("postgres://user:pass@db.example.com:5432/mydb"),
            "db.example.com:5432"
        );
        assert_eq!(
            pg_host_label("postgresql://user@host/db?sslmode=disable"),
            "host"
        );
        assert_eq!(pg_host_label("host:5433"), "host:5433");
        assert_eq!(
            pg_host_label("postgres://host.internal/db"),
            "host.internal"
        );
        // no userinfo
        assert_eq!(
            pg_host_label("postgresql://lb.example.net:6432/pools"),
            "lb.example.net:6432"
        );
        // fallback-ish
        assert_eq!(pg_host_label("weird"), "weird");
        // with fragment or query after host
        assert_eq!(pg_host_label("h:1/foo?bar#baz"), "h:1");
    }
}
