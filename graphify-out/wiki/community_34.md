# Community 34: PipelineMetrics

**Members:** 17

## Nodes

- **metrics** (`src_infra_metrics_rs`, File, degree: 3)
- **std::sync::atomic::{AtomicU64, Ordering}** (`src_infra_metrics_rs_import_std_sync_atomic_atomicu64_ordering`, Module, degree: 1)
- **MetricsSnapshot** (`src_infra_metrics_rs_metricssnapshot`, Struct, degree: 1)
- **PipelineMetrics** (`src_infra_metrics_rs_pipelinemetrics`, Struct, degree: 14)
- **.record_block_triggered_hf()** (`src_infra_metrics_rs_pipelinemetrics_record_block_triggered_hf`, Method, degree: 1)
- **.record_dispatch_deferred()** (`src_infra_metrics_rs_pipelinemetrics_record_dispatch_deferred`, Method, degree: 1)
- **.record_dispatch_started()** (`src_infra_metrics_rs_pipelinemetrics_record_dispatch_started`, Method, degree: 1)
- **.record_dry_run_passed()** (`src_infra_metrics_rs_pipelinemetrics_record_dry_run_passed`, Method, degree: 1)
- **.record_hf_skipped()** (`src_infra_metrics_rs_pipelinemetrics_record_hf_skipped`, Method, degree: 1)
- **.record_hf_tick()** (`src_infra_metrics_rs_pipelinemetrics_record_hf_tick`, Method, degree: 1)
- **.record_lf_skipped()** (`src_infra_metrics_rs_pipelinemetrics_record_lf_skipped`, Method, degree: 1)
- **.record_lf_tick()** (`src_infra_metrics_rs_pipelinemetrics_record_lf_tick`, Method, degree: 1)
- **.record_stream_log()** (`src_infra_metrics_rs_pipelinemetrics_record_stream_log`, Method, degree: 1)
- **.record_stream_triggered_hf()** (`src_infra_metrics_rs_pipelinemetrics_record_stream_triggered_hf`, Method, degree: 1)
- **.record_tx_confirmed()** (`src_infra_metrics_rs_pipelinemetrics_record_tx_confirmed`, Method, degree: 1)
- **.record_tx_reverted()** (`src_infra_metrics_rs_pipelinemetrics_record_tx_reverted`, Method, degree: 1)
- **.snapshot()** (`src_infra_metrics_rs_pipelinemetrics_snapshot`, Method, degree: 1)

## Relationships

- src_infra_metrics_rs → src_infra_metrics_rs_import_std_sync_atomic_atomicu64_ordering (imports)
- src_infra_metrics_rs → src_infra_metrics_rs_pipelinemetrics (defines)
- src_infra_metrics_rs_pipelinemetrics → src_infra_metrics_rs_pipelinemetrics_record_hf_tick (defines)
- src_infra_metrics_rs_pipelinemetrics → src_infra_metrics_rs_pipelinemetrics_record_dispatch_started (defines)
- src_infra_metrics_rs_pipelinemetrics → src_infra_metrics_rs_pipelinemetrics_record_dispatch_deferred (defines)
- src_infra_metrics_rs_pipelinemetrics → src_infra_metrics_rs_pipelinemetrics_record_dry_run_passed (defines)
- src_infra_metrics_rs_pipelinemetrics → src_infra_metrics_rs_pipelinemetrics_record_tx_confirmed (defines)
- src_infra_metrics_rs_pipelinemetrics → src_infra_metrics_rs_pipelinemetrics_record_tx_reverted (defines)
- src_infra_metrics_rs_pipelinemetrics → src_infra_metrics_rs_pipelinemetrics_record_lf_tick (defines)
- src_infra_metrics_rs_pipelinemetrics → src_infra_metrics_rs_pipelinemetrics_record_lf_skipped (defines)
- src_infra_metrics_rs_pipelinemetrics → src_infra_metrics_rs_pipelinemetrics_record_hf_skipped (defines)
- src_infra_metrics_rs_pipelinemetrics → src_infra_metrics_rs_pipelinemetrics_record_block_triggered_hf (defines)
- src_infra_metrics_rs_pipelinemetrics → src_infra_metrics_rs_pipelinemetrics_record_stream_log (defines)
- src_infra_metrics_rs_pipelinemetrics → src_infra_metrics_rs_pipelinemetrics_record_stream_triggered_hf (defines)
- src_infra_metrics_rs_pipelinemetrics → src_infra_metrics_rs_pipelinemetrics_snapshot (defines)
- src_infra_metrics_rs → src_infra_metrics_rs_metricssnapshot (defines)

