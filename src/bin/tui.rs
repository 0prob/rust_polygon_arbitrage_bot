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
    rpbot::log::set_stdout_enabled(false);
    if let Some(dir) = rpbot::log::run_dir() {
        eprintln!("rpbot tui: logging to {}", dir.display());
    }

    let (bridge, rx, snapshot_rx) = TuiBridge::channel();
    let hook = bridge.hook();
    let snapshot_tx = bridge.snapshot_sender();

    let result = run_tui(
        bridge,
        rx,
        snapshot_rx,
        bootstrap(Some(hook), Some(snapshot_tx)),
    )
    .await;
    rpbot::log::shutdown();
    result
}
