// Structured JSON logging with compile-time level gating.
//
// Env: RPBOT_LOG=debug    (default: info)
// Levels: error, warn, info, debug, trace
//
// In builds without debug_assertions (typically release): trace! and debug! compile away.
// info! and warn! are always present. error! is always present.
//
// Output: JSON lines to stderr with ms-precision timestamps.
//   {"ts":1700000000123,"lvl":"INFO","module":"orchestrator::hf","msg":"hf tick"}
//
// Usage:
//   info!("hf tick");
//   info!("hf tick: cycles={}", cycles);
//   warn!("pool {} stale", addr);
//   error!("submit failed: {}", e);

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

pub const LEVEL_ERROR: u8 = 1;
pub const LEVEL_WARN: u8 = 2;
pub const LEVEL_INFO: u8 = 3;
pub const LEVEL_DEBUG: u8 = 4;
pub const LEVEL_TRACE: u8 = 5;

pub static LOG_LEVEL: AtomicU8 = AtomicU8::new(LEVEL_INFO);
static STDERR_ENABLED: AtomicBool = AtomicBool::new(true);

pub fn set_stderr_enabled(enabled: bool) {
    STDERR_ENABLED.store(enabled, Ordering::Relaxed);
}

pub fn set_level(level: &str) {
    let lvl = if level.eq_ignore_ascii_case("error") {
        LEVEL_ERROR
    } else if level.eq_ignore_ascii_case("warn") {
        LEVEL_WARN
    } else if level.eq_ignore_ascii_case("info") {
        LEVEL_INFO
    } else if level.eq_ignore_ascii_case("debug") {
        LEVEL_DEBUG
    } else if level.eq_ignore_ascii_case("trace") {
        LEVEL_TRACE
    } else {
        LEVEL_INFO
    };
    LOG_LEVEL.store(lvl, Ordering::Relaxed);
}

pub fn init() {
    if let Ok(val) = std::env::var("RPBOT_LOG") {
        set_level(&val);
    }
}

thread_local! {
    static LOG_BUF: RefCell<String> = RefCell::new(String::with_capacity(256));
}

#[inline]
fn write_now_ms(out: &mut String) {
    use std::fmt::Write;
    let _ = write!(out, "{}", crate::util::now_ms());
}

fn push_escaped(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '"' => out.push_str(r#"\""#),
            '\n' => out.push_str(r"\n"),
            '\\' => out.push_str(r"\\"),
            '\r' => out.push_str(r"\r"),
            '\t' => out.push_str(r"\t"),
            _ => out.push(c),
        }
    }
}

pub fn log_json(level: &str, module: &str, msg: &str) {
    use std::io::Write;
    if !STDERR_ENABLED.load(Ordering::Relaxed) {
        return;
    }
    LOG_BUF.with(|buf| {
        let mut out = buf.borrow_mut();
        out.clear();
        out.push_str(r#"{"ts":"#);
        write_now_ms(&mut out);
        out.push_str(r#","lvl":""#);
        out.push_str(level);
        out.push_str(r#"","module":""#);
        out.push_str(module);
        out.push_str(r#"","msg":""#);
        push_escaped(&mut out, msg);
        out.push_str(r#""}"#);
        out.push('\n');
        let _ = std::io::stderr().lock().write_all(out.as_bytes());
    });
}

#[macro_export]
macro_rules! log_if {
    ($lvl:expr, $thresh:expr, $($arg:tt)*) => {
        if $crate::log::LOG_LEVEL.load(::std::sync::atomic::Ordering::Relaxed) >= $thresh {
            $crate::log::log_json($lvl, module_path!(), &format!($($arg)*))
        }
    };
}

#[macro_export]
macro_rules! error { ($($arg:tt)*) => { $crate::log_if!("ERROR", $crate::log::LEVEL_ERROR, $($arg)*) }}
#[macro_export]
macro_rules! warn  { ($($arg:tt)*) => { $crate::log_if!("WARN",  $crate::log::LEVEL_WARN,  $($arg)*) }}
#[macro_export]
macro_rules! info  { ($($arg:tt)*) => { $crate::log_if!("INFO",  $crate::log::LEVEL_INFO,  $($arg)*) }}

#[macro_export]
macro_rules! debug { ($($arg:tt)*) => { $crate::log_if!("DEBUG", $crate::log::LEVEL_DEBUG, $($arg)*) }}

#[macro_export]
macro_rules! trace { ($($arg:tt)*) => { $crate::log_if!("TRACE", $crate::log::LEVEL_TRACE, $($arg)*) }}
