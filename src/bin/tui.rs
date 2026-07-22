use rpbot::bootstrap::bootstrap;
use rpbot::tui::{TuiBridge, run_tui};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
    if let Some(dir) = rpbot::log::run_dir() {
        rpbot::warn!("rpbot tui: logging to {}", dir.display());
    }

    let (bridge, rx, snapshot_rx) = TuiBridge::channel();
    let hook = bridge.hook();
    let snapshot_tx = bridge.snapshot_sender();

    run_tui(
        bridge,
        rx,
        snapshot_rx,
        bootstrap(Some(hook), Some(snapshot_tx)),
    )
    .await
}
