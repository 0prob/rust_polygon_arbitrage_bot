use anyhow::Context;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::watch;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use rpbot::config::{AppConfig, WalletSecrets};
use rpbot::orchestrator::{RuntimeContext, run_pass_loop};

fn tokio_console_enabled() -> bool {
    std::env::var("TOKIO_CONSOLE")
        .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

fn json_logs_enabled() -> bool {
    std::env::var("TRACING_JSON")
        .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let json = json_logs_enabled();

    #[cfg(not(feature = "tokio-console"))]
    if tokio_console_enabled() {
        warn!(
            "TOKIO_CONSOLE=1 ignored — rebuild with \
             `cargo build --features tokio-console` and RUSTFLAGS='--cfg tokio_unstable'"
        );
    }

    #[cfg(feature = "tokio-console")]
    if tokio_console_enabled() {
        info!(
            "tokio-console enabled — run `tokio-console` in another terminal \
             (requires RUSTFLAGS='--cfg tokio_unstable' at compile time)"
        );
        if json {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(console_subscriber::spawn())
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        } else {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(console_subscriber::spawn())
                .with(tracing_subscriber::fmt::layer())
                .init();
        }
        return;
    }

    if json {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .init();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rpbot::config::load_dotenv();
    rpbot::services::execution::opportunity_journal::init_from_env();
    init_tracing();

    let mut config = AppConfig::load().context("failed to load configuration")?;
    let wallet = WalletSecrets::load(&mut config).context("failed to load wallet secrets")?;
    config.validate(&wallet).context("invalid configuration")?;

    info!(
        chain_id = config.chain_id,
        dry_run = config.is_dry_run(),
        wallet_configured = wallet.has_signer(),
        private_rpc = config.rpc.private_rpc_url.is_some(),
        tokio_console = tokio_console_enabled(),
        json_logs = json_logs_enabled(),
        "polarb-rust starting"
    );

    if config.state_rpc_url().is_none() {
        warn!("no STATE_RPC_URL / POLYGON_RPC_URL configured — pool refresh disabled");
    }

    let hypersync = rpbot::infra::hypersync::try_from_env(&config.rpc)?;
    if let Some(ref hs) = hypersync {
        match hs.get_height().await {
            Ok(height) => info!(height, "hypersync connected"),
            Err(e) => warn!(error = %e, "hypersync height probe failed"),
        }
    } else {
        info!("ENVIO_API_TOKEN not set — hypersync disabled");
    }

    let ctx = Arc::new(RuntimeContext::new(config, wallet, hypersync).context("failed to initialize runtime context")?);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let loop_ctx = ctx.clone();
    let loop_handle = tokio::spawn(async move {
        if let Err(e) = run_pass_loop(loop_ctx, shutdown_rx).await {
            tracing::error!(error = %e, "pass loop exited with error");
        }
    });

    shutdown_signal().await;
    let _ = shutdown_tx.send(true);
    let _ = loop_handle.await;

    info!("shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        match signal::ctrl_c().await {
            Ok(()) => {}
            Err(e) => {
                tracing::error!(error = %e, "failed to install Ctrl+C handler");
            }
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGTERM handler");
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
