use rpbot::bootstrap::bootstrap;
use rpbot::orchestrator::run_pass_loop;
use rpbot::shutdown::run_pass_loop_until_signal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if rpbot::cli::help_requested(std::env::args()) {
        rpbot::cli::print_help("rpbot")?;
        return Ok(());
    }
    rpbot::config::load_dotenv();
    rpbot::log::init()?;

    #[cfg(feature = "tui")]
    let bootstrap_result = bootstrap(None, None).await;
    #[cfg(not(feature = "tui"))]
    let bootstrap_result = bootstrap(None).await;
    let ctx = match bootstrap_result {
        Ok(ctx) => ctx,
        Err(error) => {
            rpbot::error!("bootstrap failed: {error:#}");
            rpbot::log::shutdown();
            return Err(error);
        }
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let pass_handle = tokio::spawn(run_pass_loop(ctx, shutdown_rx));

    match run_pass_loop_until_signal(pass_handle, shutdown_tx).await {
        Ok(()) => rpbot::info!("shutdown complete"),
        Err(e) => {
            rpbot::log::shutdown();
            return Err(e);
        }
    }
    rpbot::log::shutdown();
    Ok(())
}
