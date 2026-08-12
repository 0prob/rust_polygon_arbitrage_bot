use rpbot::bootstrap::bootstrap;
use rpbot::tui::{TuiBridge, run_tui, run_tui_demo};

fn main() -> anyhow::Result<()> {
    if rpbot::cli::help_requested(std::env::args()) {
        rpbot::cli::print_help("tui")?;
        return Ok(());
    }
    let is_demo = rpbot::cli::demo_requested(std::env::args());
    if !is_demo {
        rpbot::config::load_dotenv();
        rpbot::log::init()?;
        // Kill any other rpbot/tui (debug/release/bolt) before we open RPC/PG/WSS.
        rpbot::single_instance::ensure_single_instance();
        rpbot::log::set_stdout_enabled(false);
    }
    let _log_guard = rpbot::log::LogShutdownGuard;
    let runtime = rpbot::runtime::build()?;
    runtime.block_on(async_main(is_demo))
}

async fn async_main(is_demo: bool) -> anyhow::Result<()> {
    let (bridge, rx, snapshot_rx) = TuiBridge::channel();

    if is_demo {
        return run_tui_demo(bridge, rx).await;
    }

    if let Some(dir) = rpbot::log::run_dir() {
        rpbot::warn!("rpbot tui: logging to {}", dir.display());
    }

    let hook = bridge.hook();
    let snapshot_tx = bridge.snapshot_sender();
    #[cfg(feature = "tui")]
    let bootstrap = bootstrap(Some(hook), Some(snapshot_tx));
    #[cfg(not(feature = "tui"))]
    let bootstrap = bootstrap(Some(hook));

    run_tui(bridge, rx, snapshot_rx, bootstrap).await
}
