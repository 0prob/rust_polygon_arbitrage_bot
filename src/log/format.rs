use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::Ordering;

use serde::Serialize;

use super::storage::{private_file, truncate_private_file};
use super::{Event, STDOUT_ENABLED};

#[derive(Default)]
pub(super) struct Palette {
    error: String,
    warn: String,
    info: String,
    debug: String,
    reset: String,
    bold: String,
}

impl Palette {
    pub(super) fn detect() -> Self {
        if !std::io::stdout().is_terminal() || tput(&["colors"]).is_empty() {
            return Self::default();
        }
        Self {
            error: tput(&["setaf", "1"]),
            warn: tput(&["setaf", "3"]),
            info: tput(&["setaf", "2"]),
            debug: tput(&["setaf", "6"]),
            reset: tput(&["sgr0"]),
            bold: tput(&["bold"]),
        }
    }

    fn level(&self, level: &str) -> &str {
        match level {
            "ERROR" => &self.error,
            "WARN" => &self.warn,
            "INFO" => &self.info,
            "DEBUG" | "TRACE" => &self.debug,
            _ => "",
        }
    }
}

pub(super) struct LogFiles {
    run_dir: PathBuf,
    files: HashMap<&'static str, BufWriter<File>>,
    sizes: HashMap<&'static str, usize>,
    record: Vec<u8>,
}

impl LogFiles {
    pub(super) fn new(run_dir: &Path) -> Self {
        Self {
            run_dir: run_dir.to_path_buf(),
            files: HashMap::new(),
            sizes: HashMap::new(),
            record: Vec::with_capacity(1024),
        }
    }

    pub(super) fn write(&mut self, event: &Event, component: &'static str) -> io::Result<()> {
        self.record.clear();
        serde_json::to_writer(&mut self.record, &FileEvent::new(event, component))?;
        self.record.push(b'\n');
        if !self.files.contains_key(component) {
            let file = private_file(&self.run_dir.join(format!("{component}.jsonl")))?;
            self.files
                .insert(component, BufWriter::with_capacity(64 * 1024, file));
            self.sizes.insert(component, 0);
        }
        let current_size = self.sizes.get(component).copied().unwrap_or_default();
        if current_size > 0 && current_size + self.record.len() > 16 * 1024 * 1024 {
            if let Some(mut file) = self.files.remove(component) {
                file.flush()?;
            }
            let file = truncate_private_file(&self.run_dir.join(format!("{component}.jsonl")))?;
            self.files
                .insert(component, BufWriter::with_capacity(64 * 1024, file));
            self.sizes.insert(component, 0);
        }
        let file = self
            .files
            .get_mut(component)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "component log unavailable"))?;
        file.write_all(&self.record)?;
        *self.sizes.entry(component).or_default() += self.record.len();
        Ok(())
    }

    pub(super) fn flush(&mut self) -> io::Result<()> {
        for file in self.files.values_mut() {
            file.flush()?;
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct FileEvent<'a> {
    ts: u64,
    level: &'a str,
    component: &'a str,
    /// Message tag before the first `:` (empty when absent). Greppable funnel key.
    event: &'a str,
    module: &'a str,
    message: &'a str,
}

impl<'a> FileEvent<'a> {
    fn new(event: &'a Event, component: &'a str) -> Self {
        Self {
            ts: event.timestamp_ms,
            level: event.level,
            component,
            event: event_tag(&event.message),
            module: event.module,
            message: &event.message,
        }
    }
}

/// Extract the greppable event tag from a log message (`"hf tick: …"` → `"hf tick"`).
pub(super) fn event_tag(message: &str) -> &str {
    let Some((tag, rest)) = message.split_once(':') else {
        return "";
    };
    let tag = tag.trim();
    if tag.is_empty() || tag.len() > 48 {
        return "";
    }
    // Reject URL-like prefixes (`https:`) and bare drive letters.
    if rest.starts_with("//") || rest.starts_with('\\') {
        return "";
    }
    if !tag
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, ' ' | '_' | '-' | '/' | '.'))
    {
        return "";
    }
    tag
}

pub(super) fn write_sink_error(
    stdout: &mut impl Write,
    error: &io::Error,
    palette: &Palette,
) -> io::Result<()> {
    if !STDOUT_ENABLED.load(Ordering::Relaxed) {
        return Ok(());
    }
    let line = render_terminal(
        "ERROR",
        "system",
        &format!("component log write failed: {error}"),
        &palette.error,
        &palette.reset,
    );
    stdout.write_all(line.as_bytes())
}

pub(super) fn write_stdout(
    stdout: &mut impl Write,
    event: &Event,
    component: &str,
    palette: &Palette,
) -> io::Result<()> {
    let prefix = format!("{}{}", palette.bold, palette.level(event.level));
    let line = render_terminal(
        event.level,
        component,
        &event.message,
        &prefix,
        &palette.reset,
    );
    stdout.write_all(line.as_bytes())
}

pub(super) fn render_terminal(
    level: &str,
    component: &str,
    message: &str,
    color: &str,
    reset: &str,
) -> String {
    let message = message
        .chars()
        .flat_map(char::escape_default)
        .collect::<String>();
    format!("{color}{level:<5}{reset} {component:<12} {message}\n")
}

pub(super) fn component_for_module(module: &str) -> &'static str {
    // Order matters: more specific paths first.
    if module.contains("::services::execution") || module.contains("::orchestrator::hf_execute") {
        "execution"
    } else if module.contains("::services::oracle") {
        "oracle"
    } else if module.contains("::services::partial_cache") || module.contains("::infra::wss_feed") {
        // WSS path + stream patch cache — shared stream surface for ops.
        "stream"
    } else if module.contains("::pipeline") || module.contains("::services::pipeline_survival") {
        "routing"
    } else if module.contains("::orchestrator") {
        "orchestrator"
    } else if module.contains("::infra") {
        "infra"
    } else if module.contains("::services::state")
        || module.contains("::services::discovery")
        || module.contains("::services::index_diag")
    {
        "state"
    } else if module.contains("::tui") {
        "tui"
    } else {
        "system"
    }
}

fn tput(args: &[&str]) -> String {
    Command::new("/usr/bin/tput")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{LogFiles, component_for_module};
    use crate::log::Event;

    #[test]
    fn rotation_preserves_bounded_valid_jsonl() {
        let directory = std::env::temp_dir().join(format!(
            "rpbot-rotation-{}-{}",
            std::process::id(),
            crate::util::now_ms()
        ));
        std::fs::create_dir(&directory).expect("create rotation directory");
        let event = Event {
            timestamp_ms: 42,
            level: "DEBUG",
            module: "rpbot::pipeline::cycle_finder",
            message: "x".repeat(16 * 1024),
        };
        let component = component_for_module(event.module);
        let mut files = LogFiles::new(&directory);
        for _ in 0..1_050 {
            files.write(&event, component).expect("write routed event");
        }
        files.flush().expect("flush routed events");

        let path = directory.join("routing.jsonl");
        assert!(std::fs::metadata(&path).expect("read log metadata").len() <= 16 * 1024 * 1024);
        for line in std::fs::read_to_string(&path)
            .expect("read routed log")
            .lines()
        {
            serde_json::from_str::<serde_json::Value>(line).expect("parse routed JSONL event");
        }
        std::fs::remove_dir_all(directory).expect("remove rotation directory");
    }
}
