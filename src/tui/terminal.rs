use anyhow::Context;
use ratatui::DefaultTerminal;

/// Owns a ratatui terminal initialized via [`ratatui::try_init`] (panic hook + cleanup).
pub struct TerminalGuard {
    terminal: DefaultTerminal,
    restored: bool,
}

impl TerminalGuard {
    pub fn enter() -> anyhow::Result<Self> {
        ignore_job_control_tty_stops();
        Ok(Self {
            terminal: ratatui::try_init().context("failed to initialize ratatui terminal")?,
            restored: false,
        })
    }

    pub fn terminal(&mut self) -> &mut DefaultTerminal {
        &mut self.terminal
    }

    pub fn restore(&mut self) -> anyhow::Result<()> {
        if !self.restored {
            self.restored = true;
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
    unsafe {
        libc::signal(libc::SIGTTOU, libc::SIG_IGN);
        libc::signal(libc::SIGTTIN, libc::SIG_IGN);
    }
}

#[cfg(not(unix))]
fn ignore_job_control_tty_stops() {}
