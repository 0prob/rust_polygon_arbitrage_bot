use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::thread::JoinHandle;

use parking_lot::Mutex;

mod format;
mod storage;

use format::{LogFiles, Palette, component_for_module};

pub const LEVEL_ERROR: u8 = 1;
pub const LEVEL_WARN: u8 = 2;
pub const LEVEL_INFO: u8 = 3;
pub const LEVEL_DEBUG: u8 = 4;
pub const LEVEL_TRACE: u8 = 5;

const QUEUE_CAPACITY: usize = 8_192;
const DEFAULT_LOG_DIR: &str = "/tmp/bot";
const MAX_MESSAGE_BYTES: usize = 16 * 1024;

pub static LOG_LEVEL: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(LEVEL_INFO);
static STDOUT_ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);
static DROPPED_EVENTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static LOGGER: OnceLock<Logger> = OnceLock::new();
static RUN_DIR: OnceLock<PathBuf> = OnceLock::new();

pub(super) struct Event {
    pub(super) timestamp_ms: u64,
    pub(super) level: &'static str,
    pub(super) module: &'static str,
    pub(super) message: String,
}

enum Command {
    Event(Event),
    Shutdown,
}

struct Logger {
    sender: SyncSender<Command>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

pub fn set_stdout_enabled(enabled: bool) {
    STDOUT_ENABLED.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

pub fn set_level(level: &str) {
    LOG_LEVEL.store(parse_level(level), std::sync::atomic::Ordering::Relaxed);
}

/// Active run directory (`RPBOT_LOG_DIR/run-<ts>-<pid>/`), set after successful `init`.
#[must_use]
pub fn run_dir() -> Option<&'static Path> {
    RUN_DIR.get().map(PathBuf::as_path)
}

pub fn init() -> io::Result<()> {
    if let Ok(value) = std::env::var("RPBOT_LOG") {
        set_level(&value);
    }
    if LOGGER.get().is_some() {
        return Ok(());
    }
    let root = std::env::var_os("RPBOT_LOG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_LOG_DIR));
    let run_dir = prepare_run_dir(&root)?;
    let _ = RUN_DIR.set(run_dir.clone());
    let (sender, receiver) = sync_channel(QUEUE_CAPACITY);
    let worker = std::thread::Builder::new()
        .name("rpbot-log".into())
        .spawn(move || run_writer(&run_dir, receiver))?;
    let logger = Logger {
        sender,
        worker: Mutex::new(Some(worker)),
    };
    LOGGER
        .set(logger)
        .map_err(|_| io::Error::new(io::ErrorKind::AlreadyExists, "logger already initialized"))?;
    Ok(())
}

pub fn shutdown() {
    let Some(logger) = LOGGER.get() else {
        return;
    };
    let _ = logger.sender.send(Command::Shutdown);
    if let Some(handle) = logger.worker.lock().take() {
        let _ = handle.join();
    }
}

pub fn log(level: &'static str, module: &'static str, mut message: String) {
    let Some(logger) = LOGGER.get() else {
        return;
    };
    if message.len() > MAX_MESSAGE_BYTES {
        let boundary = message.floor_char_boundary(MAX_MESSAGE_BYTES);
        message.truncate(boundary);
        message.push_str(" [truncated]");
    }
    let event = Event {
        timestamp_ms: crate::util::now_ms(),
        level,
        module,
        message,
    };
    match logger.sender.try_send(Command::Event(event)) {
        Err(TrySendError::Full(Command::Event(event))) => {
            // Silently discard DEBUG/TRACE under backpressure; count INFO+ so the
            // writer can emit a saturation warning (inverted `<=` used to hide ERROR).
            if log_level_rank(event.level) >= LEVEL_DEBUG {
                return;
            }
            DROPPED_EVENTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
        | Err(TrySendError::Full(Command::Shutdown))
        | Err(TrySendError::Disconnected(_)) => {}
    }
}

fn log_level_rank(level: &'static str) -> u8 {
    match level {
        "ERROR" => LEVEL_ERROR,
        "WARN" => LEVEL_WARN,
        "DEBUG" => LEVEL_DEBUG,
        "TRACE" => LEVEL_TRACE,
        _ => LEVEL_INFO,
    }
}

fn run_writer(run_dir: &Path, receiver: std::sync::mpsc::Receiver<Command>) {
    let palette = Palette::detect();
    let mut files = LogFiles::new(run_dir);
    let flush_interval = std::time::Duration::from_millis(250);
    let mut next_flush = std::time::Instant::now() + flush_interval;
    loop {
        let command = match receiver
            .recv_timeout(next_flush.saturating_duration_since(std::time::Instant::now()))
        {
            Ok(command) => command,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if let Err(error) = files.flush() {
                    with_stdout(|stdout| format::write_sink_error(stdout, &error, &palette));
                }
                next_flush = std::time::Instant::now() + flush_interval;
                continue;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
        };
        match command {
            Command::Event(event) => {
                let component = component_for_module(event.module);
                with_stdout(|stdout| format::write_stdout(stdout, &event, component, &palette));
                if let Err(error) = files.write(&event, component) {
                    with_stdout(|stdout| format::write_sink_error(stdout, &error, &palette));
                }
                let dropped = DROPPED_EVENTS.swap(0, std::sync::atomic::Ordering::Relaxed);
                if dropped > 0 {
                    let warning = Event {
                        timestamp_ms: crate::util::now_ms(),
                        level: "WARN",
                        module: "rpbot::log",
                        message: format!("log queue saturated; dropped_events={dropped}"),
                    };
                    with_stdout(|stdout| {
                        format::write_stdout(stdout, &warning, "system", &palette)
                    });
                    let _ = files.write(&warning, "system");
                }
                if std::time::Instant::now() >= next_flush {
                    if let Err(error) = files.flush() {
                        with_stdout(|stdout| format::write_sink_error(stdout, &error, &palette));
                    }
                    next_flush = std::time::Instant::now() + flush_interval;
                }
            }
            Command::Shutdown => {
                if let Err(error) = files.flush() {
                    with_stdout(|stdout| format::write_sink_error(stdout, &error, &palette));
                }
                return;
            }
        }
    }
}

fn with_stdout(write: impl FnOnce(&mut io::BufWriter<io::StdoutLock<'_>>) -> io::Result<()>) {
    if !STDOUT_ENABLED.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let stdout = io::stdout();
    let mut stdout = io::BufWriter::with_capacity(16 * 1024, stdout.lock());
    let _ = write(&mut stdout);
    let _ = stdout.flush();
}

fn prepare_run_dir(root: &Path) -> io::Result<PathBuf> {
    storage::secure_directory(root)?;
    let run_dir = root.join(format!(
        "run-{}-{}",
        crate::util::now_ms(),
        std::process::id()
    ));
    storage::create_private_directory(&run_dir)?;
    storage::prune_run_directories(root, 10)?;
    Ok(run_dir)
}

fn parse_level(level: &str) -> u8 {
    if level.eq_ignore_ascii_case("error") {
        LEVEL_ERROR
    } else if level.eq_ignore_ascii_case("warn") {
        LEVEL_WARN
    } else if level.eq_ignore_ascii_case("debug") {
        LEVEL_DEBUG
    } else if level.eq_ignore_ascii_case("trace") {
        LEVEL_TRACE
    } else {
        LEVEL_INFO
    }
}

#[macro_export]
macro_rules! log_if {
    ($level:expr, $threshold:expr, $($arg:tt)*) => {
        if $crate::log::LOG_LEVEL.load(::std::sync::atomic::Ordering::Relaxed) >= $threshold {
            $crate::log::log($level, module_path!(), format!($($arg)*))
        }
    };
}

#[macro_export]
macro_rules! error { ($($arg:tt)*) => { $crate::log_if!("ERROR", $crate::log::LEVEL_ERROR, $($arg)*) } }
#[macro_export]
macro_rules! warn { ($($arg:tt)*) => { $crate::log_if!("WARN", $crate::log::LEVEL_WARN, $($arg)*) } }
#[macro_export]
macro_rules! info { ($($arg:tt)*) => { $crate::log_if!("INFO", $crate::log::LEVEL_INFO, $($arg)*) } }
#[macro_export]
macro_rules! debug { ($($arg:tt)*) => { $crate::log_if!("DEBUG", $crate::log::LEVEL_DEBUG, $($arg)*) } }
#[macro_export]
macro_rules! trace { ($($arg:tt)*) => { $crate::log_if!("TRACE", $crate::log::LEVEL_TRACE, $($arg)*) } }

#[cfg(test)]
mod tests {
    use super::format::{component_for_module, render_terminal};

    #[test]
    fn routes_modules_to_stable_component_logs() {
        assert_eq!(component_for_module("rpbot::infra::rpc"), "infra");
        assert_eq!(
            component_for_module("rpbot::orchestrator::hf"),
            "orchestrator"
        );
        assert_eq!(
            component_for_module("rpbot::pipeline::cycle_finder"),
            "routing"
        );
        assert_eq!(
            component_for_module("rpbot::services::pipeline_survival"),
            "routing"
        );
        assert_eq!(
            component_for_module("rpbot::services::execution::service"),
            "execution"
        );
        assert_eq!(
            component_for_module("rpbot::services::oracle::price_oracle"),
            "oracle"
        );
        assert_eq!(component_for_module("rpbot::bootstrap"), "system");
    }

    #[test]
    fn terminal_line_is_concise_and_sanitized() {
        let line = render_terminal("WARN", "execution", "failed\n\u{1b}[2J", "", "");
        assert_eq!(line, "WARN  execution    failed\\n\\u{1b}[2J\n");
    }

    #[test]
    fn queue_full_counts_info_plus_not_debug_trace() {
        use super::{
            LEVEL_DEBUG, LEVEL_ERROR, LEVEL_INFO, LEVEL_TRACE, LEVEL_WARN, log_level_rank,
        };
        // INFO+ must be countable under saturation; DEBUG/TRACE are silent drops.
        assert!(log_level_rank("ERROR") < LEVEL_DEBUG);
        assert!(log_level_rank("WARN") < LEVEL_DEBUG);
        assert!(log_level_rank("INFO") < LEVEL_DEBUG);
        assert!(log_level_rank("DEBUG") >= LEVEL_DEBUG);
        assert!(log_level_rank("TRACE") >= LEVEL_DEBUG);
        assert_eq!(log_level_rank("ERROR"), LEVEL_ERROR);
        assert_eq!(log_level_rank("WARN"), LEVEL_WARN);
        assert_eq!(log_level_rank("INFO"), LEVEL_INFO);
        assert_eq!(log_level_rank("TRACE"), LEVEL_TRACE);
    }
}
