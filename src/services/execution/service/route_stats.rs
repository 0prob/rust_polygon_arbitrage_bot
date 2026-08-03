use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender, SyncSender, TrySendError};
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;

use super::ExecutionService;
use crate::core::types::{CycleEdges, Edge};

#[derive(Debug, Clone, Default)]
pub(super) struct RouteStats {
    pub(super) successes: u64,
    pub(super) dry_run_successes: u64,
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

#[derive(Debug, Clone, Copy)]
pub(super) enum RouteStatsEvent {
    Success,
    DryRunPass,
    Failure(RouteFailureKind),
    AdaptiveFlashCap(u64),
}

const ROUTE_STATS_RECORD_VERSION: &str = "v1";

fn encode_cycle_edges(edges: &[Edge]) -> String {
    edges
        .iter()
        .map(|edge| {
            format!(
                "{}:{}:{}:{}:{}:{}:{}:{}",
                edge.pool_index.0,
                edge.token_in.0,
                edge.token_out.0,
                edge.token_in_idx,
                edge.token_out_idx,
                edge.protocol as u8,
                edge.fee_bps,
                u8::from(edge.zero_for_one),
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn encode_route_event(edges: &[Edge], fingerprint: u64, event: RouteStatsEvent) -> String {
    let encoded_edges = encode_cycle_edges(edges);
    match event {
        RouteStatsEvent::Success => {
            format!("{ROUTE_STATS_RECORD_VERSION} {fingerprint} {encoded_edges} s")
        }
        RouteStatsEvent::DryRunPass => {
            format!("{ROUTE_STATS_RECORD_VERSION} {fingerprint} {encoded_edges} d")
        }
        RouteStatsEvent::Failure(kind) => format!(
            "{ROUTE_STATS_RECORD_VERSION} {fingerprint} {encoded_edges} f {}",
            kind.as_tag()
        ),
        RouteStatsEvent::AdaptiveFlashCap(cap) => {
            format!("{ROUTE_STATS_RECORD_VERSION} {fingerprint} {encoded_edges} c {cap}")
        }
    }
}

enum RouteStatsMsg {
    Line(String),
    Flush(Sender<()>),
}

const ROUTE_STATS_QUEUE_CAPACITY: usize = 4_096;
const ROUTE_STATS_FLUSH_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub(super) struct RouteStatsWriter {
    tx: SyncSender<RouteStatsMsg>,
    dropped: AtomicU64,
}

impl RouteStatsWriter {
    pub(super) fn spawn(path: PathBuf) -> Self {
        let (tx, rx) = mpsc::sync_channel(ROUTE_STATS_QUEUE_CAPACITY);
        if let Err(e) = std::thread::Builder::new()
            .name("route-stats-writer".into())
            .spawn(move || route_stats_writer_loop(path, rx))
        {
            crate::warn!("route stats writer thread spawn failed: {e}");
        }
        Self {
            tx,
            dropped: AtomicU64::new(0),
        }
    }

    fn enqueue(&self, line: String) {
        if let Err(err) = self.tx.try_send(RouteStatsMsg::Line(line)) {
            self.record_drop(err);
        }
    }

    pub(super) fn flush(&self) {
        let (ack_tx, ack_rx) = mpsc::channel();
        match self.tx.try_send(RouteStatsMsg::Flush(ack_tx)) {
            Ok(()) => {
                if ack_rx.recv_timeout(ROUTE_STATS_FLUSH_TIMEOUT).is_err() {
                    crate::warn!("route stats flush timed out");
                }
            }
            Err(err) => self.record_drop(err),
        }
    }

    fn record_drop(&self, err: TrySendError<RouteStatsMsg>) {
        let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
        if dropped.is_power_of_two() {
            let reason = match err {
                TrySendError::Full(_) => "queue_full",
                TrySendError::Disconnected(_) => "writer_unavailable",
            };
            crate::warn!("route stats event dropped: reason={reason} dropped={dropped}");
        }
    }
}

impl Drop for RouteStatsWriter {
    fn drop(&mut self) {
        self.flush();
    }
}

fn route_stats_writer_loop(path: PathBuf, rx: mpsc::Receiver<RouteStatsMsg>) {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty())
        && let Err(err) = std::fs::create_dir_all(parent)
    {
        crate::error!(
            "route stats directory create failed: {}: {err}",
            parent.display()
        );
        return;
    }
    let mut file = match std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
    {
        Ok(file) => file,
        Err(err) => {
            crate::error!("route stats open failed: {}: {err}", path.display());
            return;
        }
    };
    let mut writer = BufWriter::new(&mut file);
    let mut pending = 0usize;
    let mut last_flush = Instant::now();
    while let Ok(msg) = rx.recv() {
        match msg {
            RouteStatsMsg::Line(line) => {
                if let Err(err) = writeln!(writer, "{line}") {
                    crate::error!("route stats write failed: {}: {err}", path.display());
                    return;
                }
                pending += 1;
                if pending >= 64 || last_flush.elapsed() >= Duration::from_millis(100) {
                    if let Err(err) = writer.flush() {
                        crate::error!("route stats flush failed: {}: {err}", path.display());
                        return;
                    }
                    pending = 0;
                    last_flush = Instant::now();
                }
            }
            RouteStatsMsg::Flush(ack) => {
                if let Err(err) = writer.flush() {
                    crate::error!("route stats flush failed: {}: {err}", path.display());
                    return;
                }
                pending = 0;
                last_flush = Instant::now();
                let _ = ack.send(());
            }
        }
    }
    if let Err(err) = writer.flush() {
        crate::error!("route stats final flush failed: {}: {err}", path.display());
    }
}

impl ExecutionService {
    pub(super) fn write_route_event(
        &self,
        edges: &[Edge],
        fingerprint: u64,
        event: RouteStatsEvent,
    ) {
        self.route_stats_writer
            .enqueue(encode_route_event(edges, fingerprint, event));
    }

    pub(super) fn replay_route_stats(path: &std::path::Path) -> FxHashMap<CycleEdges, RouteStats> {
        if std::fs::metadata(path).is_ok_and(|metadata| metadata.len() > 0) {
            crate::warn!(
                "route stats replay disabled: arena indexes are process-local; starting route learning fresh"
            );
        }
        FxHashMap::default()
    }

    pub(super) fn record_route_failure(&self, edges: &[Edge], fp: u64, kind: RouteFailureKind) {
        let mut all = self.route_stats.write();
        let stats = all.entry(CycleEdges::from_slice(edges)).or_default();
        stats.failures += 1;
        match kind {
            RouteFailureKind::DryRun => stats.dry_run_failures += 1,
            RouteFailureKind::Submit => stats.submit_failures += 1,
            RouteFailureKind::Timeout => stats.receipt_timeouts += 1,
            RouteFailureKind::Revert => stats.reverts += 1,
            RouteFailureKind::RealizedLoss => stats.realized_losses += 1,
        }
        drop(all);
        self.write_route_event(edges, fp, RouteStatsEvent::Failure(kind));
    }

    pub(super) fn record_route_success(&self, edges: &[Edge], fp: u64) {
        self.route_stats
            .write()
            .entry(CycleEdges::from_slice(edges))
            .or_default()
            .successes += 1;
        self.write_route_event(edges, fp, RouteStatsEvent::Success);
    }

    pub(super) fn record_route_dry_run_pass(&self, edges: &[Edge], fp: u64) {
        self.route_stats
            .write()
            .entry(CycleEdges::from_slice(edges))
            .or_default()
            .dry_run_successes += 1;
        self.write_route_event(edges, fp, RouteStatsEvent::DryRunPass);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{PoolIndex, ProtocolType, TokenIndex};

    #[test]
    fn replay_never_binds_process_local_route_indexes() {
        let path = std::env::temp_dir().join(format!(
            "rpbot-route-stats-v1-{}-{}.log",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_nanos()
        ));
        let route = CycleEdges::from_slice(&[Edge {
            pool_index: PoolIndex(7),
            token_in: TokenIndex(11),
            token_out: TokenIndex(13),
            token_in_idx: 2,
            token_out_idx: 3,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        }]);
        std::fs::write(
            &path,
            encode_route_event(&route, 99, RouteStatsEvent::Success),
        )
        .expect("write route stats fixture");

        let saved = ExecutionService::replay_route_stats(&path);
        assert!(saved.is_empty());

        let _ = std::fs::remove_file(path);
    }
}
