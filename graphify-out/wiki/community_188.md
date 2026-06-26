# Community 188: DeadlineGuard

**Members:** 5

## Nodes

- **deadline** (`src_pipeline_deadline_rs`, File, degree: 2)
- **DeadlineGuard** (`src_pipeline_deadline_rs_deadlineguard`, Struct, degree: 3)
- **.new()** (`src_pipeline_deadline_rs_deadlineguard_new`, Method, degree: 1)
- **.tick()** (`src_pipeline_deadline_rs_deadlineguard_tick`, Method, degree: 1)
- **std::time::{Duration, Instant}** (`src_pipeline_deadline_rs_import_std_time_duration_instant`, Module, degree: 1)

## Relationships

- src_pipeline_deadline_rs → src_pipeline_deadline_rs_import_std_time_duration_instant (imports)
- src_pipeline_deadline_rs → src_pipeline_deadline_rs_deadlineguard (defines)
- src_pipeline_deadline_rs_deadlineguard → src_pipeline_deadline_rs_deadlineguard_new (defines)
- src_pipeline_deadline_rs_deadlineguard → src_pipeline_deadline_rs_deadlineguard_tick (defines)

