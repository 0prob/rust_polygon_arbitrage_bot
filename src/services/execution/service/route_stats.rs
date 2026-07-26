use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::mpsc::{self, Sender};
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;

use super::ExecutionService;

#[derive(Debug, Clone, Default)]
pub(super) struct RouteStats {
    pub(super) successes: u64,
    pub(super) failures: u64,
    pub(super) dry_run_failures: u64,
    pub(super) submit_failures: u64,
    pub(super) receipt_timeouts: u64,
    pub(super) reverts: u64,
    pub(super) realized_losses: u64,
    pub(super) adaptive_flash_loan_usd: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum RouteFailureKind {
    DryRun,
    Submit,
    Timeout,
    Revert,
    RealizedLoss,
}

impl RouteFailureKind {
    /// Stable on-disk tag (must match `replay_route_stats`).
    pub(super) const fn as_tag(self) -> &'static str {
        match self {
            Self::DryRun => "DryRun",
            Self::Submit => "Submit",
            Self::Timeout => "Timeout",
            Self::Revert => "Revert",
            Self::RealizedLoss => "RealizedLoss",
        }
    }
}

enum RouteStatsMsg {
    Line(String),
    Flush(Sender<()>),
}

#[derive(Debug)]
pub(super) struct RouteStatsWriter {
    tx: Sender<RouteStatsMsg>,
}

impl RouteStatsWriter {
    pub(super) fn spawn(path: PathBuf) -> Self {
        let (tx, rx) = mpsc::channel();
        if let Err(e) = std::thread::Builder::new()
            .name("route-stats-writer".into())
            .spawn(move || route_stats_writer_loop(path, rx))
        {
            crate::warn!("route stats writer thread spawn failed: {e}");
        }
        Self { tx }
    }

    fn enqueue(&self, line: String) {
        let _ = self.tx.send(RouteStatsMsg::Line(line));
    }

    pub(super) fn flush(&self) {
        let (ack_tx, ack_rx) = mpsc::channel();
        if self.tx.send(RouteStatsMsg::Flush(ack_tx)).is_ok() {
            let _ = ack_rx.recv();
        }
    }
}

impl Drop for RouteStatsWriter {
    fn drop(&mut self) {
        self.flush();
    }
}

fn route_stats_writer_loop(path: PathBuf, rx: mpsc::Receiver<RouteStatsMsg>) {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        let _ = std::fs::create_dir_all(parent);
    }
    let Ok(mut file) = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
    else {
        return;
    };
    let mut writer = BufWriter::new(&mut file);
    let mut pending = 0usize;
    let mut last_flush = Instant::now();
    while let Ok(msg) = rx.recv() {
        match msg {
            RouteStatsMsg::Line(line) => {
                let _ = writeln!(writer, "{line}");
                pending += 1;
                if pending >= 64 || last_flush.elapsed() >= Duration::from_millis(100) {
                    let _ = writer.flush();
                    pending = 0;
                    last_flush = Instant::now();
                }
            }
            RouteStatsMsg::Flush(ack) => {
                let _ = writer.flush();
                pending = 0;
                last_flush = Instant::now();
                let _ = ack.send(());
            }
        }
    }
    let _ = writer.flush();
}

impl ExecutionService {
    pub(super) fn write_route_event(&self, line: String) {
        self.route_stats_writer.enqueue(line);
    }

    pub(super) fn replay_route_stats(path: &std::path::Path) -> FxHashMap<u64, RouteStats> {
        let Ok(file) = std::fs::File::open(path) else {
            return FxHashMap::default();
        };
        let mut stats = FxHashMap::default();
        let mut reader = BufReader::with_capacity(64 * 1024, file);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Err(e) => {
                    crate::warn!(
                        "route stats replay stopped on IO error at {}: {e}",
                        path.display()
                    );
                    break;
                }
                Ok(_) => {
                    let mut parts = line.split_whitespace();
                    let Some(fp_str) = parts.next() else {
                        continue;
                    };
                    let Ok(fp) = fp_str.parse::<u64>() else {
                        continue;
                    };
                    let Some(tag) = parts.next() else {
                        continue;
                    };
                    let entry: &mut RouteStats = stats.entry(fp).or_default();
                    match tag {
                        "s" => entry.successes += 1,
                        "c" => {
                            entry.adaptive_flash_loan_usd =
                                parts.next().and_then(|value| value.parse().ok())
                        }
                        "f" => {
                            entry.failures += 1;
                            match parts.next() {
                                Some("DryRun") => entry.dry_run_failures += 1,
                                Some("Submit") => entry.submit_failures += 1,
                                Some("Timeout") => entry.receipt_timeouts += 1,
                                Some("Revert") => entry.reverts += 1,
                                Some("RealizedLoss") => entry.realized_losses += 1,
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        stats
    }

    pub(super) fn record_route_failure(&self, fp: u64, kind: RouteFailureKind) {
        let mut all = self.route_stats.write();
        let stats = all.entry(fp).or_default();
        stats.failures += 1;
        match kind {
            RouteFailureKind::DryRun => stats.dry_run_failures += 1,
            RouteFailureKind::Submit => stats.submit_failures += 1,
            RouteFailureKind::Timeout => stats.receipt_timeouts += 1,
            RouteFailureKind::Revert => stats.reverts += 1,
            RouteFailureKind::RealizedLoss => stats.realized_losses += 1,
        }
        drop(all);
        self.write_route_event(format!("{} f {}", fp, kind.as_tag()));
    }

    pub(super) fn record_route_success(&self, fp: u64) {
        self.route_stats.write().entry(fp).or_default().successes += 1;
        self.write_route_event(format!("{} s", fp));
    }

    /// Dry-run reassess pass (semantic + profit gate) — counts as success for risk
    /// so chronic dry-run fail streaks can recover without waiting for a mined receipt.
    /// Without this, `route_risk_multiplier` only ratchets up until an on-chain win.
    pub(super) fn record_route_dry_run_pass(&self, fp: u64) {
        self.record_route_success(fp);
    }
}
