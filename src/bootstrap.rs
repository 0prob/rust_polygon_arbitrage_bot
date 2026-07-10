use std::sync::Arc;

use anyhow::Context;

use crate::config::{AppConfig, WalletSecrets};
use crate::infra::hypersync::{HyperSyncService, try_from_env};
use crate::infra::pg::PgClient;
use crate::orchestrator::{RuntimeContext, SharedUiHook};

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
        "rpbot starting (execution_mode={}, dry_run={}, cycle_finder={:?}, lf={}ms, hf={}ms)",
        config.execution.mode,
        config.is_dry_run(),
        config.routing.cycle_finder,
        config.lf_interval_ms,
        config.hf_interval_ms,
    );
    crate::info!(
        "discovery (pg={}, bootstrap_batch={}, interval={}ms)",
        pg_host_label(&config.pg_url),
        config.discovery_bootstrap_batch,
        config.discovery_interval_ms,
    );

    if config.state_rpc_url().is_none() {
        crate::warn!("no STATE_RPC_URL / POLYGON_RPC_URL configured — pool refresh disabled");
    }
}

/// Non-blocking postgres probe; does not delay runtime startup.
pub fn spawn_pg_probe(pg_url: String) {
    tokio::spawn(async move {
        let pg = match PgClient::new(pg_url) {
            Ok(c) => c,
            Err(e) => {
                crate::warn!("postgres connection failed: {e}");
                return;
            }
        };
        match pg.probe_pool_meta_count().await {
            Ok(count) => crate::info!("postgres connected pool_meta_rows={count}"),
            Err(e) => crate::warn!("postgres probe failed: {e}"),
        }
    });
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

/// Common bootstrap for both the rpbot and tui binaries.
/// Logs startup summary (including hypersync status), builds the runtime context (optionally
/// installing a UI hook for TUI), and spawns the non-blocking pg + hypersync connectivity probes
/// (their results log asynchronously).
///
/// The config/wallet load (which may do filesystem reads for .env / PRIVATE_KEY_FILE / config.toml)
/// is performed via `spawn_blocking` so it does not occupy an async worker thread.
#[cfg(not(feature = "tui"))]
pub async fn bootstrap(ui_hook: Option<SharedUiHook>) -> anyhow::Result<Arc<RuntimeContext>> {
    let (config, wallet) = tokio::task::spawn_blocking(load_config_and_wallet)
        .await
        .context("config load task panicked")??;

    log_startup(&config);

    let pg_url = config.pg_url.clone();
    let token_present = std::env::var("ENVIO_API_TOKEN")
        .ok()
        .is_some_and(|t| !t.trim().is_empty());
    let hypersync = try_from_env(&config.rpc);
    match (&hypersync, token_present) {
        (None, true) => {
            crate::warn!("ENVIO_API_TOKEN set but hypersync client failed to build — disabled")
        }
        (None, false) => crate::info!("ENVIO_API_TOKEN not set — hypersync disabled"),
        _ => {}
    }
    let mut runtime = build_runtime(config, wallet, hypersync)?;
    if let Some(hook) = ui_hook {
        runtime = runtime.with_ui_hook(hook);
    }
    let ctx = Arc::new(runtime);

    spawn_pg_probe(pg_url);
    spawn_hypersync_probe(ctx.hypersync.clone());

    Ok(ctx)
}

#[cfg(feature = "tui")]
pub async fn bootstrap(
    ui_hook: Option<SharedUiHook>,
    ui_snapshot_tx: Option<
        tokio::sync::watch::Sender<Option<Arc<crate::tui::app::DashboardSnapshot>>>,
    >,
) -> anyhow::Result<Arc<RuntimeContext>> {
    let (config, wallet) = tokio::task::spawn_blocking(load_config_and_wallet)
        .await
        .context("config load task panicked")??;

    log_startup(&config);

    let pg_url = config.pg_url.clone();
    let token_present = std::env::var("ENVIO_API_TOKEN")
        .ok()
        .is_some_and(|t| !t.trim().is_empty());
    let hypersync = try_from_env(&config.rpc);
    match (&hypersync, token_present) {
        (None, true) => {
            crate::warn!("ENVIO_API_TOKEN set but hypersync client failed to build — disabled")
        }
        (None, false) => crate::info!("ENVIO_API_TOKEN not set — hypersync disabled"),
        _ => {}
    }
    let mut runtime = build_runtime(config, wallet, hypersync)?;
    if let Some(hook) = ui_hook {
        runtime = runtime.with_ui_hook(hook);
    }
    if let Some(tx) = ui_snapshot_tx {
        runtime = runtime.with_ui_snapshot_tx(tx);
    }
    let ctx = Arc::new(runtime);

    spawn_pg_probe(pg_url);
    spawn_hypersync_probe(ctx.hypersync.clone());

    Ok(ctx)
}

#[cfg(test)]
mod tests {
    use super::*;

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
