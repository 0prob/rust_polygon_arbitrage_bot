//! Headless `rpbot` binary — production entrypoint.
//!
//! Startup order is intentional:
//! 1. `--help` (no FS / network / runtime)
//! 2. dotenv + logger (so peer-kill and bootstrap emit structured logs)
//! 3. single-instance peer kill (may sleep ≤~3s — **before** the multi-thread runtime)
//! 4. tokio runtime + bootstrap + pass loop until SIGINT/SIGTERM

use anyhow::Context;
use rpbot::bootstrap::bootstrap_headless;
use rpbot::orchestrator::run_pass_loop;
use rpbot::shutdown::run_pass_loop_until_signal;

fn main() -> anyhow::Result<()> {
    if rpbot::cli::help_requested(std::env::args()) {
        rpbot::cli::print_help("rpbot")?;
        return Ok(());
    }

    // Sync preamble: keep dotenv / peer-kill sleeps off the async worker pool.
    rpbot::config::load_dotenv();
    rpbot::log::init().context("failed to initialize logging")?;
    // Flush/join log worker on every exit path (bootstrap fail, pass-loop error, success).
    let _log_guard = rpbot::log::LogShutdownGuard;
    // Kill any other rpbot/tui (debug/release/bolt) before we open RPC/PG/WSS.
    rpbot::single_instance::ensure_single_instance();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("rpbot-tokio")
        .build()
        .context("failed to build tokio runtime")?;

    // `block_on` holds the runtime until pass-loop has joined (or aborted).
    // Dropping `runtime` afterward shuts workers down without orphaning tasks.
    runtime.block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    rpbot::info!("rpbot main starting pid={}", std::process::id());

    let ctx = match bootstrap_headless().await {
        Ok(ctx) => ctx,
        Err(error) => {
            rpbot::error!("bootstrap failed: {error:#}");
            return Err(error);
        }
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let pass_handle = tokio::spawn(run_pass_loop(ctx, shutdown_rx));

    match run_pass_loop_until_signal(pass_handle, shutdown_tx).await {
        Ok(()) => {
            rpbot::info!("shutdown complete");
            Ok(())
        }
        Err(e) => Err(e),
    }
}
