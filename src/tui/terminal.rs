use std::io::{self, Stdout, Write};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};

static TERMINAL_ACTIVE: AtomicBool = AtomicBool::new(false);

/// Restore terminal state if the TUI was active (safe to call from panic hook).
pub fn emergency_restore() {
    if TERMINAL_ACTIVE.swap(false, Ordering::SeqCst) {
        let _ = disable_raw_mode();
        let mut stdout = io::stdout();
        let _ = execute!(stdout, LeaveAlternateScreen);
        let _ = stdout.flush();
    }
}

/// Owns raw mode + alternate screen; restores on drop and panic.
pub struct TerminalGuard {
    _panic_guard: AssertUnwindSafe<()>,
}

impl TerminalGuard {
    pub fn enter() -> anyhow::Result<(Self, Stdout)> {
        enable_raw_mode().context("enable raw mode")?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen).context("enter alternate screen")?;
        stdout.flush().ok();
        TERMINAL_ACTIVE.store(true, Ordering::SeqCst);

        let prior = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            emergency_restore();
            prior(info);
        }));

        Ok((
            Self {
                _panic_guard: AssertUnwindSafe(()),
            },
            stdout,
        ))
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        emergency_restore();
    }
}
