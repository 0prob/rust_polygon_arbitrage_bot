//! Graceful shutdown helpers shared by rpbot binaries.

use std::time::Duration;

use anyhow::Context;
use tokio::signal;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// Outer join budget after SIGINT/SIGTERM (headless main and TUI quit).
///
/// `run_pass_loop` drains HF (≤10s) then LF (≤10s) and may await nonce resync;
/// the previous 10s outer cap aborted mid-drain and skipped `shutdown_resync`.
/// Keep under typical k8s `terminationGracePeriodSeconds` (30).
pub const PASS_LOOP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);

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
            // Drop the sender after signaling so receivers see closed if they
            // only watch for lag (defensive; primary path is the bool flag).
            let _ = shutdown_tx.send(true);
            drop(shutdown_tx);
            tokio::select! {
                res = &mut pass_handle => res,
                _ = tokio::time::sleep(PASS_LOOP_SHUTDOWN_TIMEOUT) => {
                    pass_handle.abort();
                    // Join after abort so we never leave a detached JoinHandle
                    // (leaks the task until process exit and can panic on drop
                    // in future tokio versions).
                    match pass_handle.await {
                        Ok(Ok(())) | Err(_) => {
                            crate::warn!(
                                "pass loop shutdown timed out after {PASS_LOOP_SHUTDOWN_TIMEOUT:?}; aborted"
                            );
                            Ok(Ok(()))
                        }
                        Ok(Err(e)) => {
                            crate::warn!(
                                "pass loop shutdown timed out after {PASS_LOOP_SHUTDOWN_TIMEOUT:?}; aborted with error: {e:#}"
                            );
                            Ok(Ok(()))
                        }
                    }
                }
            }
        }
        res = &mut pass_handle => {
            // Pass loop exited without OS signal — drop unused sender.
            drop(shutdown_tx);
            res
        }
    };

    match outcome {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => {
            crate::error!("pass loop failed: {e:#}");
            Err(e).context("pass loop failed")
        }
        Err(e) if e.is_cancelled() => {
            // Abort path already logged; treat as clean stop.
            Ok(())
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
            Err(e).context("pass loop failed during shutdown")
        }
        Ok(Err(e)) if e.is_cancelled() => Ok(()),
        Ok(Err(e)) => {
            crate::warn!("pass loop panicked during shutdown: {e:#}");
            anyhow::bail!("pass loop panicked during shutdown: {e}")
        }
        Err(_) => {
            pass_handle.abort();
            let _ = pass_handle.await;
            crate::warn!("pass loop shutdown timed out after {PASS_LOOP_SHUTDOWN_TIMEOUT:?}");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_budget_covers_pass_loop_task_drains() {
        // pass_loop: HF join ≤10s + LF join ≤10s (+ resync). Outer must be larger.
        const PASS_LOOP_INNER_DRAIN: Duration = Duration::from_secs(20);
        assert!(
            PASS_LOOP_SHUTDOWN_TIMEOUT > PASS_LOOP_INNER_DRAIN,
            "PASS_LOOP_SHUTDOWN_TIMEOUT={PASS_LOOP_SHUTDOWN_TIMEOUT:?} must exceed inner HF+LF drains ({PASS_LOOP_INNER_DRAIN:?})"
        );
        assert!(
            PASS_LOOP_SHUTDOWN_TIMEOUT <= Duration::from_secs(30),
            "keep outer budget within typical 30s termination grace"
        );
    }

    #[tokio::test]
    async fn shutdown_join_propagates_pass_loop_error() {
        let handle = tokio::spawn(async { anyhow::bail!("expected pass-loop failure") });
        let error = join_pass_loop_after_shutdown(handle)
            .await
            .expect_err("pass-loop error must remain visible during shutdown");
        assert!(format!("{error:#}").contains("expected pass-loop failure"));
    }

    #[tokio::test]
    async fn shutdown_join_propagates_pass_loop_panic() {
        let handle = tokio::spawn(async { panic!("expected pass-loop panic") });
        let error = join_pass_loop_after_shutdown(handle)
            .await
            .expect_err("pass-loop panic must remain visible during shutdown");
        assert!(
            error
                .to_string()
                .contains("pass loop panicked during shutdown")
        );
    }
}
