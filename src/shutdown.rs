//! Graceful shutdown helpers shared by rpbot binaries.

use std::time::Duration;

use anyhow::Context;
use tokio::signal;
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub const PASS_LOOP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Wait for Ctrl+C or (on Unix) SIGTERM.
pub async fn wait_for_os_shutdown() {
    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                crate::error!("failed to install SIGTERM handler: {e}");
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

/// Run until the pass loop exits on its own or an OS shutdown signal arrives.
pub async fn run_pass_loop_until_signal(
    mut pass_handle: JoinHandle<anyhow::Result<()>>,
    shutdown_tx: watch::Sender<bool>,
) -> anyhow::Result<()> {
    let outcome = tokio::select! {
        biased;
        () = wait_for_os_shutdown() => {
            let _ = shutdown_tx.send(true);
            tokio::select! {
                res = &mut pass_handle => res,
                _ = tokio::time::sleep(PASS_LOOP_SHUTDOWN_TIMEOUT) => {
                    pass_handle.abort();
                    let _ = pass_handle.await;
                    crate::warn!(
                        "pass loop shutdown timed out after {PASS_LOOP_SHUTDOWN_TIMEOUT:?}; aborted"
                    );
                    Ok(Ok(()))
                }
            }
        }
        res = &mut pass_handle => res,
    };

    match outcome {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => {
            crate::error!("pass loop failed: {e:#}");
            Err(e).context("pass loop failed")
        }
        Err(e) => {
            crate::error!("pass loop task panicked: {e}");
            anyhow::bail!("pass loop task panicked: {e}");
        }
    }
}

/// Drain the pass loop after shutdown was already signaled (TUI quit path).
pub async fn join_pass_loop_after_shutdown(
    mut pass_handle: JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    match tokio::time::timeout(PASS_LOOP_SHUTDOWN_TIMEOUT, &mut pass_handle).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(e))) => {
            crate::warn!("pass loop failed during shutdown: {e:#}");
            Ok(())
        }
        Ok(Err(e)) => {
            crate::warn!("pass loop panicked during shutdown: {e:#}");
            Ok(())
        }
        Err(_) => {
            pass_handle.abort();
            let _ = pass_handle.await;
            crate::warn!("pass loop shutdown timed out after {PASS_LOOP_SHUTDOWN_TIMEOUT:?}");
            Ok(())
        }
    }
}