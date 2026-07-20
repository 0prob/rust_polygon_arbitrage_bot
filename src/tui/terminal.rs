use std::io::stdout;

use anyhow::Context;
use crossterm::event::{DisableBracketedPaste, EnableBracketedPaste};
use crossterm::execute;
use ratatui::DefaultTerminal;

/// Owns a ratatui terminal initialized via [`ratatui::try_init`] (panic hook + cleanup).
pub struct TerminalGuard {
    terminal: DefaultTerminal,
    restored: bool,
}

impl TerminalGuard {
    pub fn enter() -> anyhow::Result<Self> {
        ignore_job_control_tty_stops();
        let terminal = ratatui::try_init().context("failed to initialize ratatui terminal")?;
        // try_init only enables raw mode + alt screen. Bracketed paste is opt-in
        // (crossterm docs); without it Event::Paste never fires despite the feature flag.
        execute!(stdout(), EnableBracketedPaste).context("failed to enable bracketed paste")?;
        Ok(Self {
            terminal,
            restored: false,
        })
    }

    pub fn terminal(&mut self) -> &mut DefaultTerminal {
        &mut self.terminal
    }

    pub fn restore(&mut self) -> anyhow::Result<()> {
        if !self.restored {
            self.restored = true;
            // Leave paste mode before ratatui restores raw/alt-screen state.
            let _ = execute!(stdout(), DisableBracketedPaste);
            ratatui::try_restore().context("failed to restore terminal")?;
        }
        Ok(())
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

/// Prevent SIGTTIN/SIGTTOU from freezing the process when the TTY is not foreground
/// (e.g. job control, some multiplexer/wrapper setups).
#[cfg(unix)]
fn ignore_job_control_tty_stops() {
    // signal(2): avoid in multithreaded processes (behavior unspecified); use sigaction.
    ignore_signal(libc::SIGTTOU);
    ignore_signal(libc::SIGTTIN);
}

/// Install a permanent `SIG_IGN` disposition via [`libc::sigaction`].
#[cfg(unix)]
fn ignore_signal(signum: libc::c_int) {
    // SAFETY: zeroed `sigaction` is a valid C init pattern (see libc / signal-hook-registry).
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = libc::SIG_IGN;
    // SAFETY: `action` is fully initialized; null oldact means we discard the previous disposition.
    let rc = unsafe { libc::sigaction(signum, &action, std::ptr::null_mut()) };
    debug_assert_eq!(rc, 0, "sigaction({signum}, SIG_IGN) failed");
}

#[cfg(not(unix))]
fn ignore_job_control_tty_stops() {}
