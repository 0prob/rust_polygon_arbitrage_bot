use rpbot::bootstrap::bootstrap;
use rpbot::tui::{TuiBridge, run_tui};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rpbot::log::init();
    rpbot::log::set_stderr_enabled(false);

    let (bridge, rx) = TuiBridge::channel();
    let hook = bridge.hook();

    let ctx = bootstrap(Some(hook)).await?;

    run_tui(ctx, bridge, rx).await
}
