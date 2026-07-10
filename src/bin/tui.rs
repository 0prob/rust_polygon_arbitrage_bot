use rpbot::bootstrap::bootstrap;
use rpbot::tui::{TuiBridge, run_tui};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rpbot::log::init();
    rpbot::log::set_stderr_enabled(false);

    let (bridge, rx, snapshot_rx) = TuiBridge::channel();
    let hook = bridge.hook();

    #[cfg(feature = "tui")]
    {
        let ctx = bootstrap(Some(hook), Some(bridge.snapshot_sender())).await?;
        return run_tui(ctx, bridge, rx, snapshot_rx).await;
    }

    #[cfg(not(feature = "tui"))]
    {
        let ctx = bootstrap(Some(hook)).await?;
        run_tui(ctx, bridge, rx, snapshot_rx).await
    }
}
