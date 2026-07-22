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

    let pids: Vec<u32> = others.iter().map(|p| p.pid).collect();
    send_signal_batch(&pids, "TERM");

    let deadline = Instant::now() + TERM_WAIT;
    while Instant::now() < deadline {
        if !pids.iter().any(|&pid| pid_alive(pid)) {
            break;
        }
        thread::sleep(POLL);
    }

    let still: Vec<u32> = pids.iter().copied().filter(|&pid| pid_alive(pid)).collect();
    if !still.is_empty() {
        for &pid in &still {
            crate::warn!("single-instance: pid {pid} still alive after SIGTERM — SIGKILL");
        }
        send_signal_batch(&still, "KILL");
        // One more short wait so zombies get reaped / /proc entries vanish.
        let kill_deadline = Instant::now() + POLL * 5;
        while Instant::now() < kill_deadline {
            if !still.iter().any(|&pid| pid_alive(pid)) {
                break;
            }
            thread::sleep(POLL);
        }
    }

    let remaining: Vec<u32> = pids.iter().copied().filter(|&pid| pid_alive(pid)).collect();
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
    // Never match cargo unit-test / rustc fingerprint bins under target/*/deps/.
    if is_cargo_deps_path(stripped) {
        return None;
    }
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

fn is_cargo_deps_path(path: &str) -> bool {
    path.contains("/deps/") || path.contains("\\deps\\") || path.contains("/deps\\")
}

fn is_instance_basename(base: &str) -> bool {
    if base == "rpbot" || base == "tui" {
        return true;
    }
    // Bolt / PGO renamed artifacts: tui-bolt-optimized, rpbot-bolt-instrumented, etc.
    // Exclude cargo fingerprint names: `rpbot-<hex>` under target/*/deps/ (also path-gated).
    if base.starts_with("tui-bolt") {
        return true;
    }
    if let Some(rest) = base.strip_prefix("rpbot-") {
        // cargo test/lib bins: rpbot-<16 hex chars>; real renames have letters like "bolt".
        return !is_cargo_fingerprint_suffix(rest);
    }
    false
}

/// `true` when `rest` looks like a rustc/cargo metadata hash (hex only, typically 16 chars).
fn is_cargo_fingerprint_suffix(rest: &str) -> bool {
    let len = rest.len();
    (8..=32).contains(&len) && rest.bytes().all(|b| b.is_ascii_hexdigit())
}

fn is_project_tui_path(path: &str) -> bool {
    path.contains("/target/")
        || path.contains("\\target\\")
        || path.starts_with("./target/")
        || path.contains("/arb/c/")
        || path.contains("rpbot")
}

/// Live, non-zombie process. Zombies still have `/proc/<pid>` until reaped, so a
/// bare exists() check produced false "still alive after SIGKILL" warnings.
fn pid_alive(pid: u32) -> bool {
    let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    // Format: `pid (comm) state ...` — comm may contain spaces/parens; state follows the last `)`.
    let Some(after_comm) = stat.rsplit_once(')').map(|(_, rest)| rest) else {
        return PathBuf::from(format!("/proc/{pid}")).exists();
    };
    match after_comm.trim_start().chars().next() {
        // Z=zombie, X/x=dead — treat as gone for single-instance purposes.
        Some('Z' | 'X' | 'x') | None => false,
        Some(_) => true,
    }
}

/// Signal many pids with one `kill(1)` invocation (avoids N process spawns).
fn send_signal_batch(pids: &[u32], sig: &str) {
    if pids.is_empty() {
        return;
    }
    // Use the kill(1) binary so we do not need a hard libc dep on the non-tui build.
    let mut cmd = Command::new("kill");
    cmd.arg(format!("-{sig}"));
    for pid in pids {
        cmd.arg(pid.to_string());
    }
    match cmd.status() {
        Ok(s) if s.success() => {}
        Ok(s) => crate::debug!("single-instance: kill -{sig} {pids:?} exit={s}"),
        Err(e) => crate::warn!("single-instance: kill -{sig} {pids:?} failed: {e}"),
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
        assert_eq!(
            label_if_instance(Path::new(
                "/home/x/arb/c/target/x86_64-unknown-linux-gnu/release/rpbot-bolt-optimized"
            ))
            .as_deref(),
            Some("rpbot-bolt-optimized")
        );
    }

    #[test]
    fn ignores_unrelated_tui_and_other_binaries() {
        assert!(label_if_instance(Path::new("/usr/bin/tui")).is_none());
        assert!(label_if_instance(Path::new("/usr/local/bin/tmux")).is_none());
        assert!(label_if_instance(Path::new("./target/release/oracle_live_test")).is_none());
        assert!(label_if_instance(Path::new("/bin/bash")).is_none());
    }

    #[test]
    fn ignores_cargo_deps_fingerprint_bins() {
        // cargo test / rustc unit-test artifacts live under deps/ as rpbot-<hex>.
        assert!(
            label_if_instance(Path::new(
                "/home/x/arb/c/target/debug/deps/rpbot-a1b2c3d4e5f67890"
            ))
            .is_none()
        );
        assert!(
            label_if_instance(Path::new(
                "/home/x/arb/c/target/debug/deps/rpbot-0123456789abcdef"
            ))
            .is_none()
        );
        // Basename-only hex fingerprint without path still rejected.
        assert!(label_if_instance(Path::new("rpbot-deadbeefcafebabe")).is_none());
    }

    #[test]
    fn accepts_tui_when_path_mentions_project() {
        assert_eq!(
            label_if_instance(Path::new("/home/x/arb/c/target/release/tui")).as_deref(),
            Some("tui")
        );
    }

    #[test]
    fn cargo_fingerprint_suffix_detection() {
        assert!(is_cargo_fingerprint_suffix("a1b2c3d4e5f67890"));
        assert!(is_cargo_fingerprint_suffix("01234567"));
        assert!(!is_cargo_fingerprint_suffix("bolt-optimized"));
        assert!(!is_cargo_fingerprint_suffix("bolt"));
        assert!(!is_cargo_fingerprint_suffix(""));
    }
}
