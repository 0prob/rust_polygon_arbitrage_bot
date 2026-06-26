# Community 81: recover_after_receipt_timeout()

**Members:** 12

## Nodes

- **recovery** (`src_services_execution_recovery_rs`, File, degree: 11)
- **alloy::network::Ethereum** (`src_services_execution_recovery_rs_import_alloy_network_ethereum`, Module, degree: 1)
- **alloy::primitives::{Address, B256, U256}** (`src_services_execution_recovery_rs_import_alloy_primitives_address_b256_u256`, Module, degree: 1)
- **alloy::providers::Provider** (`src_services_execution_recovery_rs_import_alloy_providers_provider`, Module, degree: 1)
- **alloy::rpc::types::TransactionRequest** (`src_services_execution_recovery_rs_import_alloy_rpc_types_transactionrequest`, Module, degree: 1)
- **super::gas::u256_to_u128** (`src_services_execution_recovery_rs_import_super_gas_u256_to_u128`, Module, degree: 1)
- **super::nonce::NonceManager** (`src_services_execution_recovery_rs_import_super_nonce_noncemanager`, Module, degree: 1)
- **super::receipt::ReceiptData** (`src_services_execution_recovery_rs_import_super_receipt_receiptdata`, Module, degree: 1)
- **super::submit::{SubmitFees, bump_fees, FEE_BUMP_BPS}** (`src_services_execution_recovery_rs_import_super_submit_submitfees_bump_fees_fee_bump_bps`, Module, degree: 1)
- **tracing::{info, warn}** (`src_services_execution_recovery_rs_import_tracing_info_warn`, Module, degree: 1)
- **NonceRecoveryOutcome** (`src_services_execution_recovery_rs_noncerecoveryoutcome`, Enum, degree: 1)
- **recover_after_receipt_timeout()** (`src_services_execution_recovery_rs_recover_after_receipt_timeout`, Function, degree: 1)

## Relationships

- src_services_execution_recovery_rs → src_services_execution_recovery_rs_import_alloy_network_ethereum (imports)
- src_services_execution_recovery_rs → src_services_execution_recovery_rs_import_alloy_primitives_address_b256_u256 (imports)
- src_services_execution_recovery_rs → src_services_execution_recovery_rs_import_alloy_providers_provider (imports)
- src_services_execution_recovery_rs → src_services_execution_recovery_rs_import_alloy_rpc_types_transactionrequest (imports)
- src_services_execution_recovery_rs → src_services_execution_recovery_rs_import_tracing_info_warn (imports)
- src_services_execution_recovery_rs → src_services_execution_recovery_rs_import_super_gas_u256_to_u128 (imports)
- src_services_execution_recovery_rs → src_services_execution_recovery_rs_import_super_nonce_noncemanager (imports)
- src_services_execution_recovery_rs → src_services_execution_recovery_rs_import_super_receipt_receiptdata (imports)
- src_services_execution_recovery_rs → src_services_execution_recovery_rs_import_super_submit_submitfees_bump_fees_fee_bump_bps (imports)
- src_services_execution_recovery_rs → src_services_execution_recovery_rs_noncerecoveryoutcome (defines)
- src_services_execution_recovery_rs → src_services_execution_recovery_rs_recover_after_receipt_timeout (defines)

