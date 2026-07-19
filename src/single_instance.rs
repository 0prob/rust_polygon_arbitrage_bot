//! Ensure only one bot/TUI process runs at a time.
//!
//! On startup, scan `/proc` for other `rpbot` / `tui` binaries (debug, release,
//! bolt-instrumented/optimized, deleted-but-running) and SIGTERM then SIGKILL them.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

/// Env escape hatch for tests / intentional multi-instance runs.
const ALLOW_MULTIPLE_ENV: &str = "RPBOT_ALLOW_MULTIPLE";

const TERM_WAIT: Duration = Duration::from_secs(3);
const POLL: Duration = Duration::from_millis(100);

/// Terminate every other live bot/TUI instance, then continue.
///
/// Safe to call more than once; no-ops when `RPBOT_ALLOW_MULTIPLE` is set.
pub fn ensure_single_instance() {
    if std::env::var_os(ALLOW_MULTIPLE_ENV).is_some() {
        crate::info!("single-instance: skipped ({ALLOW_MULTIPLE_ENV} set)");
        return;
    }

    let self_pid = std::process::id();
    let others = find_other_instances(self_pid);
    if others.is_empty() {
        crate::debug!("single-instance: no other bot/tui processes");
        return;
    }

    let summary: Vec<String> = others
        .iter()
        .map(|p| format!("{}({})", p.pid, p.label))
        .collect();
    crate::warn!(
        "single-instance: terminating {} other bot/tui process(es): {}",
        others.len(),
        summary.join(", ")
    );

    for proc in &others {
        send_signal(proc.pid, "TERM");
    }

    let deadline = Instant::now() + TERM_WAIT;
    while Instant::now() < deadline {
        if !others.iter().any(|p| pid_alive(p.pid)) {
            break;
        }
        thread::sleep(POLL);
    }

    let mut killed = 0u32;
    for proc in &others {
        if pid_alive(proc.pid) {
            crate::warn!(
                "single-instance: pid {} still alive after SIGTERM — SIGKILL",
                proc.pid
            );
            send_signal(proc.pid, "KILL");
            killed += 1;
        }
    }
    if killed > 0 {
        thread::sleep(POLL);
    }

    let remaining: Vec<u32> = others
        .iter()
        .map(|p| p.pid)
        .filter(|&pid| pid_alive(pid))
        .collect();
    if remaining.is_empty() {
        crate::info!(
            "single-instance: cleared {} prior process(es)",
            others.len()
        );
    } else {
        crate::warn!(
            "single-instance: still alive after SIGKILL: {remaining:?} — continuing anyway"
        );
    }
}

#[derive(Debug, Clone)]
struct ForeignProcess {
    pid: u32,
    label: String,
}

fn find_other_instances(self_pid: u32) -> Vec<ForeignProcess> {
    let Ok(entries) = fs::read_dir("/proc") else {
        crate::warn!("single-instance: cannot read /proc — skip peer scan");
        return Vec::new();
    };

    let mut out = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(pid) = name.to_str().and_then(|s| s.parse::<u32>().ok()) else {
            continue;
        };
        if pid == self_pid || pid == 0 {
            continue;
        }
        if let Some(label) = instance_label(pid) {
            out.push(ForeignProcess { pid, label });
        }
    }
    out.sort_by_key(|p| p.pid);
    out
}

/// Return a short label when `pid` is an rpbot/tui binary we should replace.
fn instance_label(pid: u32) -> Option<String> {
    // Prefer exe symlink — survives argv0 tricks and shows "(deleted)".
    let exe_path = format!("/proc/{pid}/exe");
    if let Ok(link) = fs::read_link(&exe_path)
        && let Some(label) = label_if_instance(&link)
    {
        return Some(label);
    }

    let cmdline = fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    if cmdline.is_empty() {
        return None;
    }
    let argv0 = cmdline.split(|&b| b == 0).next().unwrap_or(&[]);
    if argv0.is_empty() {
        return None;
    }
    let argv0 = String::from_utf8_lossy(argv0);
    label_if_instance(Path::new(argv0.as_ref()))
}

fn label_if_instance(path: &Path) -> Option<String> {
    let raw = path.to_string_lossy();
    let stripped = raw.strip_suffix(" (deleted)").unwrap_or(raw.as_ref());
    let base = Path::new(stripped)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    if !is_instance_basename(base) {
        return None;
    }
    // `tui` alone is a common name — only claim cargo/target builds or obvious project paths.
    if base == "tui" && !is_project_tui_path(stripped) {
        return None;
    }
    Some(base.to_string())
}

fn is_instance_basename(base: &str) -> bool {
    base == "rpbot" || base == "tui" || base.starts_with("tui-bolt") || base.starts_with("rpbot-")
}

fn is_project_tui_path(path: &str) -> bool {
    path.contains("/target/")
        || path.contains("\\target\\")
        || path.starts_with("./target/")
        || path.contains("/arb/c/")
        || path.contains("rpbot")
}

fn pid_alive(pid: u32) -> bool {
    PathBuf::from(format!("/proc/{pid}")).exists()
}

fn send_signal(pid: u32, sig: &str) {
    // Use the kill(1) binary so we do not need a hard libc dep on the non-tui build.
    let status = Command::new("kill")
        .args([format!("-{sig}"), pid.to_string()])
        .status();
    match status {
        Ok(s) if s.success() => {}
        Ok(s) => crate::debug!("single-instance: kill -{sig} {pid} exit={s}"),
        Err(e) => crate::warn!("single-instance: kill -{sig} {pid} failed: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn matches_rpbot_release_and_debug_paths() {
        assert_eq!(
            label_if_instance(Path::new("/home/x/arb/c/target/release/rpbot")).as_deref(),
            Some("rpbot")
        );
        assert_eq!(
            label_if_instance(Path::new("./target/debug/rpbot")).as_deref(),
            Some("rpbot")
        );
        assert_eq!(
            label_if_instance(Path::new("/home/x/arb/c/target/release/rpbot (deleted)")).as_deref(),
            Some("rpbot")
        );
    }

    #[test]
    fn matches_tui_and_bolt_variants_under_target() {
        assert_eq!(
            label_if_instance(Path::new(
                "/home/x/arb/c/target/x86_64-unknown-linux-gnu/release/tui"
            ))
            .as_deref(),
            Some("tui")
        );
        assert_eq!(
            label_if_instance(Path::new(
                "/home/x/arb/c/target/x86_64-unknown-linux-gnu/release/tui-bolt-optimized"
            ))
            .as_deref(),
            Some("tui-bolt-optimized")
        );
        assert_eq!(
            label_if_instance(Path::new(
                "/home/x/arb/c/target/x86_64-unknown-linux-gnu/release/tui-bolt-instrumented"
            ))
            .as_deref(),
            Some("tui-bolt-instrumented")
        );
    }

    #[test]
    fn ignores_unrelated_tui_and_other_binaries() {
        assert!(label_if_instance(Path::new("/usr/bin/tui")).is_none());
        assert!(label_if_instance(Path::new("/usr/local/bin/tmux")).is_none());
        assert!(label_if_instance(Path::new("./target/release/oracle_feeds")).is_none());
        assert!(label_if_instance(Path::new("/bin/bash")).is_none());
    }

    #[test]
    fn accepts_tui_when_path_mentions_project() {
        assert_eq!(
            label_if_instance(Path::new("/home/x/arb/c/target/release/tui")).as_deref(),
            Some("tui")
        );
    }
}
