use rpbot::bootstrap::bootstrap;
use rpbot::tui::{TuiBridge, run_tui};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rpbot::config::load_dotenv();
    rpbot::log::init()?;
    rpbot::log::set_stdout_enabled(false);

    let (bridge, rx, snapshot_rx) = TuiBridge::channel();
    let hook = bridge.hook();

    #[cfg(feature = "tui")]
    {
        let result = bootstrap(Some(hook), Some(bridge.snapshot_sender())).await;
        let ctx = match result {
            Ok(ctx) => ctx,
            Err(error) => {
                rpbot::error!("bootstrap failed: {error:#}");
                rpbot::log::shutdown();
                return Err(error);
            }
        };
        let result = run_tui(ctx, bridge, rx, snapshot_rx).await;
        rpbot::log::shutdown();
        return result;
    }

    #[cfg(not(feature = "tui"))]
    {
        let result = bootstrap(Some(hook)).await;
        let ctx = match result {
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
}
