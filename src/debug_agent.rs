//! Agent debug instrumentation — enable with `DEBUG_AGENT=1`.
//! NDJSON logs → `.cursor/debug-6372bf.log`

use std::io::Write;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

const LOG_PATH: &str = "/home/x/arb/c/.cursor/debug-6372bf.log";
const SESSION_ID: &str = "6372bf";

fn enabled() -> bool {
    static FLAG: OnceLock<bool> = OnceLock::new();
    *FLAG.get_or_init(|| {
        std::env::var("DEBUG_AGENT")
            .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
    })
}

pub fn is_enabled() -> bool {
    enabled()
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn log_file() -> &'static Mutex<std::fs::File> {
    static FILE: OnceLock<Mutex<std::fs::File>> = OnceLock::new();
    FILE.get_or_init(|| {
        Mutex::new(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(LOG_PATH)
                .expect("open debug log"),
        )
    })
}

/// Append one NDJSON line. No-op unless `DEBUG_AGENT=1`.
pub fn log(hypothesis_id: &str, location: &str, message: &str, data: serde_json::Value) {
    if !enabled() {
        return;
    }
    // #region agent log
    let line = serde_json::json!({
        "sessionId": SESSION_ID,
        "timestamp": now_ms(),
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
    });
    if let Ok(mut f) = log_file().lock() {
        let _ = writeln!(f, "{line}");
    }
    // #endregion
}
