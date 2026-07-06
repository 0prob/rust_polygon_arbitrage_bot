use anyhow::Context;
use ratatui::DefaultTerminal;

/// Owns a ratatui terminal initialized via [`ratatui::try_init`] (panic hook + cleanup).
pub struct TerminalGuard {
    terminal: DefaultTerminal,
    restored: bool,
}

impl TerminalGuard {
    pub fn enter() -> anyhow::Result<Self> {
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
