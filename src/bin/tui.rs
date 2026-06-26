//! TUI dashboard entrypoint.
//!
//! ```bash
//! cargo run --bin tui --features tui -- --mock
//! cargo run --bin tui --features tui          # live pipeline + snapshot poller
//! ```
//!
//! Logging never goes to the terminal while the TUI is active (would corrupt raw mode).
//! Set `TUI_LOG_FILE=/path/to/log` to capture pipeline logs to a file instead.

use std::fs::OpenOptions;
use std::io;
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::{mpsc, oneshot, watch};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use rpbot::config::{AppConfig, WalletSecrets};
use rpbot::orchestrator::{RuntimeContext, run_pass_loop};
use rpbot::tui::mock::spawn_mock_updates;
use rpbot::tui::run::spawn_snapshot_poller;
use rpbot::tui::update::UiUpdate;
use rpbot::tui::{App, UiBridge, run_tui};

#[derive(Debug, Default)]
struct Args {
    mock: bool,
}

fn parse_args() -> Args {
    let mut args = Args::default();
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--mock" | "-m" => args.mock = true,
            "--help" | "-h" => {
                eprintln!(
                    "Usage: tui [--mock]\n\n\
                      --mock  Demo mode with synthetic cycles\n\n\
                     Env:\n\
                       TUI_LOG_FILE=/path   append pipeline logs to file\n\
                       RUST_LOG=info        filter when TUI_LOG_FILE is set\n"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(1);
            }
        }
    }
    args
}

fn init_tracing_for_tui() {
    let env_filter = if std::env::var("TUI_LOG_FILE").is_ok() {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    } else {
        // Errors only — info/warn would scroll over the alternate screen.
        EnvFilter::new("error")
    };

    if let Ok(path) = std::env::var("TUI_LOG_FILE") {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap_or_else(|e| {
                eprintln!("TUI_LOG_FILE open failed ({path}): {e}");
                std::process::exit(1);
            });
        tracing_subscriber::registry()
            .with(env_filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(file)
                    .with_ansi(false),
            )
            .init();
        return;
    }

    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(io::sink)
                .with_ansi(false),
        )
        .init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rpbot::config::load_dotenv();
    init_tracing_for_tui();

    let args = parse_args();
    let (ui_tx, ui_rx) = mpsc::channel::<UiUpdate>(256);
    let bridge = UiBridge::new(ui_tx);
    let app = App::new(args.mock);

    if args.mock {
        spawn_mock_updates(bridge.clone());
        return run_tui(app, ui_rx, None).await;
    }

    let mut config = AppConfig::load().context("failed to load configuration")?;
    let wallet = WalletSecrets::load(&mut config).context("failed to load wallet secrets")?;
    bridge.notify_config(&config);

    let hypersync = rpbot::infra::hypersync::try_from_env(&config.rpc)?;
    let ctx = Arc::new(
        RuntimeContext::new(config, wallet, hypersync)
            .context("failed to initialize runtime context")?
            .with_ui_bridge(bridge.clone()),
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (tui_ready_tx, tui_ready_rx) = oneshot::channel();

    let loop_ctx = ctx.clone();
    let loop_handle = tokio::spawn(async move {
        if tui_ready_rx.await.is_err() {
            return;
        }
        if let Err(e) = run_pass_loop(loop_ctx, shutdown_rx).await {
            tracing::error!(error = %e, "pass loop exited with error");
        }
    });

    spawn_snapshot_poller(bridge.clone(), ctx.snapshots.clone(), 500);

    let tui_result = run_tui(app, ui_rx, Some(tui_ready_tx)).await;

    let _ = shutdown_tx.send(true);
    let _ = loop_handle.await;

    tui_result
}
