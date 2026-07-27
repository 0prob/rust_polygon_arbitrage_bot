use rpbot::bootstrap::bootstrap;
use rpbot::tui::{TuiBridge, run_tui};

fn main() -> anyhow::Result<()> {
    if rpbot::cli::help_requested(std::env::args()) {
        rpbot::cli::print_help("tui")?;
        return Ok(());
    }
    rpbot::config::load_dotenv();
    rpbot::log::init()?;
    // Flush/join log worker on every exit path (bootstrap fail, TUI error, success).
    let _log_guard = rpbot::log::LogShutdownGuard;
    // Kill any other rpbot/tui (debug/release/bolt) before we open RPC/PG/WSS.
    rpbot::single_instance::ensure_single_instance();
    rpbot::log::set_stdout_enabled(false);
    let runtime = rpbot::runtime::build()?;
    runtime.block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    if let Some(dir) = rpbot::log::run_dir() {
        rpbot::warn!("rpbot tui: logging to {}", dir.display());
    }

    let (bridge, rx, snapshot_rx) = TuiBridge::channel();
    let hook = bridge.hook();
    let snapshot_tx = bridge.snapshot_sender();
    #[cfg(feature = "tui")]
    let bootstrap = bootstrap(Some(hook), Some(snapshot_tx));
    #[cfg(not(feature = "tui"))]
    let bootstrap = bootstrap(Some(hook));

    run_tui(bridge, rx, snapshot_rx, bootstrap).await
}
