use std::sync::atomic::{AtomicU64, Ordering};

/// Lightweight in-process counters for pipeline observability (log / TUI hooks).
#[derive(Debug, Default)]
pub struct PipelineMetrics {
    hf_ticks: AtomicU64,
    hf_profitable_ticks: AtomicU64,
    hf_eval_ms_total: AtomicU64,
    dispatches_started: AtomicU64,
    dispatches_deferred: AtomicU64,
    dry_runs_passed: AtomicU64,
    txs_confirmed: AtomicU64,
    txs_reverted: AtomicU64,
    lf_ticks: AtomicU64,
    lf_skipped: AtomicU64,
    hf_skipped: AtomicU64,
    block_triggered_hf: AtomicU64,
    stream_logs: AtomicU64,
    stream_triggered_hf: AtomicU64,
}

impl PipelineMetrics {
    pub fn record_hf_tick(&self, elapsed_ms: u64, profitable: usize) {
        self.hf_ticks.fetch_add(1, Ordering::Relaxed);
        self.hf_eval_ms_total
            .fetch_add(elapsed_ms, Ordering::Relaxed);
        if profitable > 0 {
            self.hf_profitable_ticks.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_dispatch_started(&self) {
        self.dispatches_started.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dispatch_deferred(&self) {
        self.dispatches_deferred.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dry_run_passed(&self) {
        self.dry_runs_passed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_tx_confirmed(&self) {
        self.txs_confirmed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_tx_reverted(&self) {
        self.txs_reverted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_lf_tick(&self) {
        self.lf_ticks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_lf_skipped(&self) {
        self.lf_skipped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_hf_skipped(&self) {
        self.hf_skipped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_block_triggered_hf(&self) {
        self.block_triggered_hf.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_stream_log(&self) {
        self.stream_logs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_stream_triggered_hf(&self) {
        self.stream_triggered_hf.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let hf_ticks = self.hf_ticks.load(Ordering::Relaxed);
        MetricsSnapshot {
            hf_ticks,
            hf_profitable_ticks: self.hf_profitable_ticks.load(Ordering::Relaxed),
            hf_eval_ms_avg: if hf_ticks == 0 {
                0
            } else {
                self.hf_eval_ms_total.load(Ordering::Relaxed) / hf_ticks
            },
            dispatches_started: self.dispatches_started.load(Ordering::Relaxed),
            dispatches_deferred: self.dispatches_deferred.load(Ordering::Relaxed),
            dry_runs_passed: self.dry_runs_passed.load(Ordering::Relaxed),
            txs_confirmed: self.txs_confirmed.load(Ordering::Relaxed),
            txs_reverted: self.txs_reverted.load(Ordering::Relaxed),
            lf_ticks: self.lf_ticks.load(Ordering::Relaxed),
            lf_skipped: self.lf_skipped.load(Ordering::Relaxed),
            hf_skipped: self.hf_skipped.load(Ordering::Relaxed),
            block_triggered_hf: self.block_triggered_hf.load(Ordering::Relaxed),
            stream_logs: self.stream_logs.load(Ordering::Relaxed),
            stream_triggered_hf: self.stream_triggered_hf.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MetricsSnapshot {
    pub hf_ticks: u64,
    pub hf_profitable_ticks: u64,
    pub hf_eval_ms_avg: u64,
    pub dispatches_started: u64,
    pub dispatches_deferred: u64,
    pub dry_runs_passed: u64,
    pub txs_confirmed: u64,
    pub txs_reverted: u64,
    pub lf_ticks: u64,
    pub lf_skipped: u64,
    pub hf_skipped: u64,
    pub block_triggered_hf: u64,
    pub stream_logs: u64,
    pub stream_triggered_hf: u64,
}
