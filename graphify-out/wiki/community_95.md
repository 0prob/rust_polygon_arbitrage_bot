# Community 95: ReceiptData

**Members:** 11

## Nodes

- **receipt** (`src_services_execution_receipt_rs`, File, degree: 12)
- **alloy::network::Ethereum** (`src_services_execution_receipt_rs_import_alloy_network_ethereum`, Module, degree: 1)
- **alloy::primitives::B256** (`src_services_execution_receipt_rs_import_alloy_primitives_b256`, Module, degree: 1)
- **alloy::providers::Provider** (`src_services_execution_receipt_rs_import_alloy_providers_provider`, Module, degree: 1)
- **alloy::rpc::types::Log** (`src_services_execution_receipt_rs_import_alloy_rpc_types_log`, Module, degree: 1)
- **crate::infra::hypersync::HyperSyncService** (`src_services_execution_receipt_rs_import_crate_infra_hypersync_hypersyncservice`, Module, degree: 1)
- **crate::services::execution::rpc_errors::is_transient_receipt_error** (`src_services_execution_receipt_rs_import_crate_services_execution_rpc_errors_is_transient_receipt_error`, Module, degree: 1)
- **std::time::{Duration, Instant}** (`src_services_execution_receipt_rs_import_std_time_duration_instant`, Module, degree: 1)
- **tokio::sync::watch** (`src_services_execution_receipt_rs_import_tokio_sync_watch`, Module, degree: 1)
- **tracing::{debug, instrument, warn}** (`src_services_execution_receipt_rs_import_tracing_debug_instrument_warn`, Module, degree: 1)
- **ReceiptData** (`src_services_execution_receipt_rs_receiptdata`, Struct, degree: 1)

## Relationships

- src_services_execution_receipt_rs → src_services_execution_receipt_rs_import_std_time_duration_instant (imports)
- src_services_execution_receipt_rs → src_services_execution_receipt_rs_import_alloy_network_ethereum (imports)
- src_services_execution_receipt_rs → src_services_execution_receipt_rs_import_alloy_primitives_b256 (imports)
- src_services_execution_receipt_rs → src_services_execution_receipt_rs_import_alloy_providers_provider (imports)
- src_services_execution_receipt_rs → src_services_execution_receipt_rs_import_alloy_rpc_types_log (imports)
- src_services_execution_receipt_rs → src_services_execution_receipt_rs_import_tokio_sync_watch (imports)
- src_services_execution_receipt_rs → src_services_execution_receipt_rs_import_tracing_debug_instrument_warn (imports)
- src_services_execution_receipt_rs → src_services_execution_receipt_rs_import_crate_infra_hypersync_hypersyncservice (imports)
- src_services_execution_receipt_rs → src_services_execution_receipt_rs_import_crate_services_execution_rpc_errors_is_transient_receipt_error (imports)
- src_services_execution_receipt_rs → src_services_execution_receipt_rs_receiptdata (defines)

