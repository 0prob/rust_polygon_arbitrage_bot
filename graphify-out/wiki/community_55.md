# Community 55: JournalEntry

**Members:** 14

## Nodes

- **opportunity_journal** (`src_services_execution_opportunity_journal_rs`, File, degree: 13)
- **append()** (`src_services_execution_opportunity_journal_rs_append`, Function, degree: 3)
- **crate::core::types::ProfitAssessment** (`src_services_execution_opportunity_journal_rs_import_crate_core_types_profitassessment`, Module, degree: 1)
- **crate::services::execution::opportunity_log::OpportunityRecord** (`src_services_execution_opportunity_journal_rs_import_crate_services_execution_opportunity_log_opportunityrecord`, Module, degree: 1)
- **parking_lot::Mutex** (`src_services_execution_opportunity_journal_rs_import_parking_lot_mutex`, Module, degree: 1)
- **serde::Serialize** (`src_services_execution_opportunity_journal_rs_import_serde_serialize`, Module, degree: 1)
- **std::fs::OpenOptions** (`src_services_execution_opportunity_journal_rs_import_std_fs_openoptions`, Module, degree: 1)
- **std::io::Write** (`src_services_execution_opportunity_journal_rs_import_std_io_write`, Module, degree: 1)
- **std::path::PathBuf** (`src_services_execution_opportunity_journal_rs_import_std_path_pathbuf`, Module, degree: 1)
- **std::sync::LazyLock** (`src_services_execution_opportunity_journal_rs_import_std_sync_lazylock`, Module, degree: 1)
- **init_from_env()** (`src_services_execution_opportunity_journal_rs_init_from_env`, Function, degree: 1)
- **journal_from_record()** (`src_services_execution_opportunity_journal_rs_journal_from_record`, Function, degree: 2)
- **journal_outcome()** (`src_services_execution_opportunity_journal_rs_journal_outcome`, Function, degree: 2)
- **JournalEntry** (`src_services_execution_opportunity_journal_rs_journalentry`, Struct, degree: 1)

## Relationships

- src_services_execution_opportunity_journal_rs → src_services_execution_opportunity_journal_rs_import_std_fs_openoptions (imports)
- src_services_execution_opportunity_journal_rs → src_services_execution_opportunity_journal_rs_import_std_io_write (imports)
- src_services_execution_opportunity_journal_rs → src_services_execution_opportunity_journal_rs_import_std_path_pathbuf (imports)
- src_services_execution_opportunity_journal_rs → src_services_execution_opportunity_journal_rs_import_parking_lot_mutex (imports)
- src_services_execution_opportunity_journal_rs → src_services_execution_opportunity_journal_rs_import_serde_serialize (imports)
- src_services_execution_opportunity_journal_rs → src_services_execution_opportunity_journal_rs_import_std_sync_lazylock (imports)
- src_services_execution_opportunity_journal_rs → src_services_execution_opportunity_journal_rs_import_crate_core_types_profitassessment (imports)
- src_services_execution_opportunity_journal_rs → src_services_execution_opportunity_journal_rs_import_crate_services_execution_opportunity_log_opportunityrecord (imports)
- src_services_execution_opportunity_journal_rs → src_services_execution_opportunity_journal_rs_init_from_env (defines)
- src_services_execution_opportunity_journal_rs → src_services_execution_opportunity_journal_rs_journalentry (defines)
- src_services_execution_opportunity_journal_rs → src_services_execution_opportunity_journal_rs_append (defines)
- src_services_execution_opportunity_journal_rs → src_services_execution_opportunity_journal_rs_journal_from_record (defines)
- src_services_execution_opportunity_journal_rs → src_services_execution_opportunity_journal_rs_journal_outcome (defines)
- src_services_execution_opportunity_journal_rs_journal_from_record → src_services_execution_opportunity_journal_rs_append (calls)
- src_services_execution_opportunity_journal_rs_journal_outcome → src_services_execution_opportunity_journal_rs_append (calls)

