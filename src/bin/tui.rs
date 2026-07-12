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

    let (bridge, rx, snapshot_rx) = TuiBridge::channel();
    let hook = bridge.hook();
    let snapshot_tx = bridge.snapshot_sender();

    let ctx = match bootstrap(Some(hook), Some(snapshot_tx)).await {
        Ok(ctx) => ctx,
        Err(error) => {
            rpbot::error!("bootstrap failed: {error:#}");
            rpbot::log::shutdown();
            return Err(error);
        }
    };

    let result = run_tui(ctx, bridge, rx, snapshot_rx).await;
    rpbot::log::shutdown();
    result
}
