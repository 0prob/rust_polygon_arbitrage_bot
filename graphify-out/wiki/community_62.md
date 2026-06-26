# Community 62: DryRunResult

**Members:** 13

## Nodes

- **dryrun** (`src_services_execution_dryrun_rs`, File, degree: 12)
- **build_tx()** (`src_services_execution_dryrun_rs_build_tx`, Function, degree: 2)
- **dry_run_candidate()** (`src_services_execution_dryrun_rs_dry_run_candidate`, Function, degree: 2)
- **DryRunResult** (`src_services_execution_dryrun_rs_dryrunresult`, Struct, degree: 1)
- **alloy::network::Ethereum** (`src_services_execution_dryrun_rs_import_alloy_network_ethereum`, Module, degree: 1)
- **alloy::primitives::Address** (`src_services_execution_dryrun_rs_import_alloy_primitives_address`, Module, degree: 1)
- **alloy::providers::Provider** (`src_services_execution_dryrun_rs_import_alloy_providers_provider`, Module, degree: 1)
- **alloy::rpc::types::TransactionRequest** (`src_services_execution_dryrun_rs_import_alloy_rpc_types_transactionrequest`, Module, degree: 1)
- **crate::services::execution::candidate::CandidateExecution** (`src_services_execution_dryrun_rs_import_crate_services_execution_candidate_candidateexecution`, Module, degree: 1)
- **crate::services::execution::rpc_errors::classify_submit_error** (`src_services_execution_dryrun_rs_import_crate_services_execution_rpc_errors_classify_submit_error`, Module, degree: 1)
- **std::time::Duration** (`src_services_execution_dryrun_rs_import_std_time_duration`, Module, degree: 1)
- **tokio::time::timeout** (`src_services_execution_dryrun_rs_import_tokio_time_timeout`, Module, degree: 1)
- **tracing::{instrument, warn}** (`src_services_execution_dryrun_rs_import_tracing_instrument_warn`, Module, degree: 1)

## Relationships

- src_services_execution_dryrun_rs → src_services_execution_dryrun_rs_import_std_time_duration (imports)
- src_services_execution_dryrun_rs → src_services_execution_dryrun_rs_import_alloy_network_ethereum (imports)
- src_services_execution_dryrun_rs → src_services_execution_dryrun_rs_import_alloy_primitives_address (imports)
- src_services_execution_dryrun_rs → src_services_execution_dryrun_rs_import_alloy_providers_provider (imports)
- src_services_execution_dryrun_rs → src_services_execution_dryrun_rs_import_alloy_rpc_types_transactionrequest (imports)
- src_services_execution_dryrun_rs → src_services_execution_dryrun_rs_import_tokio_time_timeout (imports)
- src_services_execution_dryrun_rs → src_services_execution_dryrun_rs_import_tracing_instrument_warn (imports)
- src_services_execution_dryrun_rs → src_services_execution_dryrun_rs_import_crate_services_execution_candidate_candidateexecution (imports)
- src_services_execution_dryrun_rs → src_services_execution_dryrun_rs_import_crate_services_execution_rpc_errors_classify_submit_error (imports)
- src_services_execution_dryrun_rs → src_services_execution_dryrun_rs_dryrunresult (defines)
- src_services_execution_dryrun_rs → src_services_execution_dryrun_rs_build_tx (defines)
- src_services_execution_dryrun_rs → src_services_execution_dryrun_rs_dry_run_candidate (defines)
- src_services_execution_dryrun_rs_dry_run_candidate → src_services_execution_dryrun_rs_build_tx (calls)

