# Community 106: test_encode_transfer_all_encodes_correctly()

**Members:** 10

## Nodes

- **approvals** (`src_services_execution_calldata_approvals_rs`, File, degree: 9)
- **encode_approve_if_needed()** (`src_services_execution_calldata_approvals_rs_encode_approve_if_needed`, Function, degree: 3)
- **encode_transfer_all()** (`src_services_execution_calldata_approvals_rs_encode_transfer_all`, Function, degree: 2)
- **alloy::primitives::{Address, U256}** (`src_services_execution_calldata_approvals_rs_import_alloy_primitives_address_u256`, Module, degree: 1)
- **alloy::sol_types::SolCall** (`src_services_execution_calldata_approvals_rs_import_alloy_sol_types_solcall`, Module, degree: 1)
- **crate::abis::{ExecutorCall, IArbExecutor}** (`src_services_execution_calldata_approvals_rs_import_crate_abis_executorcall_iarbexecutor`, Module, degree: 1)
- **super::*** (`src_services_execution_calldata_approvals_rs_import_super`, Module, degree: 1)
- **test_encode_approve_if_needed_different_amounts()** (`src_services_execution_calldata_approvals_rs_test_encode_approve_if_needed_different_amounts`, Function, degree: 2)
- **test_encode_approve_if_needed_encodes_correctly()** (`src_services_execution_calldata_approvals_rs_test_encode_approve_if_needed_encodes_correctly`, Function, degree: 2)
- **test_encode_transfer_all_encodes_correctly()** (`src_services_execution_calldata_approvals_rs_test_encode_transfer_all_encodes_correctly`, Function, degree: 2)

## Relationships

- src_services_execution_calldata_approvals_rs → src_services_execution_calldata_approvals_rs_import_alloy_primitives_address_u256 (imports)
- src_services_execution_calldata_approvals_rs → src_services_execution_calldata_approvals_rs_import_crate_abis_executorcall_iarbexecutor (imports)
- src_services_execution_calldata_approvals_rs → src_services_execution_calldata_approvals_rs_import_alloy_sol_types_solcall (imports)
- src_services_execution_calldata_approvals_rs → src_services_execution_calldata_approvals_rs_encode_approve_if_needed (defines)
- src_services_execution_calldata_approvals_rs → src_services_execution_calldata_approvals_rs_encode_transfer_all (defines)
- src_services_execution_calldata_approvals_rs → src_services_execution_calldata_approvals_rs_import_super (imports)
- src_services_execution_calldata_approvals_rs → src_services_execution_calldata_approvals_rs_test_encode_approve_if_needed_encodes_correctly (defines)
- src_services_execution_calldata_approvals_rs → src_services_execution_calldata_approvals_rs_test_encode_approve_if_needed_different_amounts (defines)
- src_services_execution_calldata_approvals_rs → src_services_execution_calldata_approvals_rs_test_encode_transfer_all_encodes_correctly (defines)
- src_services_execution_calldata_approvals_rs_test_encode_approve_if_needed_encodes_correctly → src_services_execution_calldata_approvals_rs_encode_approve_if_needed (calls)
- src_services_execution_calldata_approvals_rs_test_encode_approve_if_needed_different_amounts → src_services_execution_calldata_approvals_rs_encode_approve_if_needed (calls)
- src_services_execution_calldata_approvals_rs_test_encode_transfer_all_encodes_correctly → src_services_execution_calldata_approvals_rs_encode_transfer_all (calls)

