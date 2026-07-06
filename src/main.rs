use anyhow::Context;
use tokio::signal;
use tokio::sync::watch;
use tokio::time::Duration;

use rpbot::bootstrap::bootstrap;
use rpbot::orchestrator::run_pass_loop;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rpbot::log::init();

    let ctx = bootstrap(None).await?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut pass_handle = tokio::spawn(run_pass_loop(ctx, shutdown_rx));

    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

    let outcome = tokio::select! {
        biased;
        () = shutdown_signal() => {
            let _ = shutdown_tx.send(true);
            // Race graceful shutdown against timeout; abort on expiry to avoid hangs.
            tokio::select! {
                res = &mut pass_handle => res,
                _ = tokio::time::sleep(SHUTDOWN_TIMEOUT) => {
                    pass_handle.abort();
                    let _ = pass_handle.await;
                    rpbot::warn!("pass loop shutdown timed out after {SHUTDOWN_TIMEOUT:?}; aborted");
                    Ok(Ok(()))
                }
            }
        }
        res = &mut pass_handle => res,
    };

    match outcome {
        Ok(Ok(())) => rpbot::info!("shutdown complete"),
        Ok(Err(e)) => {
            rpbot::error!("pass loop failed: {e:#}");
            return Err(e).context("pass loop failed");
        }
        Err(e) => {
            rpbot::error!("pass loop task panicked: {e}");
            anyhow::bail!("pass loop task panicked: {e}");
        }
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                rpbot::error!("failed to install SIGTERM handler: {e}");
                // Don't resolve — if we return here the select treats it as
                // a shutdown signal, which is wrong. Wait forever instead.
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = signal::ctrl_c() => {}
        () = terminate => {}
    }
}
