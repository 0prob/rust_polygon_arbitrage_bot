# Community 43: encode_kyber_hop_validates_recipient()

**Members:** 16

## Nodes

- **kyber** (`src_services_execution_calldata_encoders_kyber_rs`, File, degree: 15)
- **create_test_hop()** (`src_services_execution_calldata_encoders_kyber_rs_create_test_hop`, Function, degree: 3)
- **encode_kyber_hop()** (`src_services_execution_calldata_encoders_kyber_rs_encode_kyber_hop`, Function, degree: 3)
- **encode_kyber_hop_returns_single_call()** (`src_services_execution_calldata_encoders_kyber_rs_encode_kyber_hop_returns_single_call`, Function, degree: 3)
- **encode_kyber_hop_uses_correct_protocol_constant()** (`src_services_execution_calldata_encoders_kyber_rs_encode_kyber_hop_uses_correct_protocol_constant`, Function, degree: 1)
- **encode_kyber_hop_validates_recipient()** (`src_services_execution_calldata_encoders_kyber_rs_encode_kyber_hop_validates_recipient`, Function, degree: 3)
- **alloy::dyn_abi::DynSolValue** (`src_services_execution_calldata_encoders_kyber_rs_import_alloy_dyn_abi_dynsolvalue`, Module, degree: 1)
- **alloy::primitives::{Address, I256, U160, U256}** (`src_services_execution_calldata_encoders_kyber_rs_import_alloy_primitives_address_i256_u160_u256`, Module, degree: 1)
- **alloy::sol_types::SolCall** (`src_services_execution_calldata_encoders_kyber_rs_import_alloy_sol_types_solcall`, Module, degree: 1)
- **crate::abis::{ExecutorCall, IKyberElasticPool}** (`src_services_execution_calldata_encoders_kyber_rs_import_crate_abis_executorcall_ikyberelasticpool`, Module, degree: 1)
- **crate::core::types::{Edge, PoolIndex, TokenIndex}** (`src_services_execution_calldata_encoders_kyber_rs_import_crate_core_types_edge_poolindex_tokenindex`, Module, degree: 1)
- **crate::pipeline::arena::StateArena** (`src_services_execution_calldata_encoders_kyber_rs_import_crate_pipeline_arena_statearena`, Module, degree: 1)
- **crate::services::execution::calldata::types::CalldataHop** (`src_services_execution_calldata_encoders_kyber_rs_import_crate_services_execution_calldata_types_calldatahop`, Module, degree: 1)
- **crate::services::execution::quote::{
    derive_tight_v3_price_limit_kyber, pool_tokens_from_hop, quote_hop_for_execution,
    resolve_kyber_fee_pips,
}** (`src_services_execution_calldata_encoders_kyber_rs_import_crate_services_execution_quote_derive_tight_v3_price_limit_kyber_pool_tokens_from_hop_quote_hop_for_execution_resolve_kyber_fee_pips`, Module, degree: 1)
- **super::*** (`src_services_execution_calldata_encoders_kyber_rs_import_super`, Module, degree: 1)
- **super::shared::to_v3_state** (`src_services_execution_calldata_encoders_kyber_rs_import_super_shared_to_v3_state`, Module, degree: 1)

## Relationships

- src_services_execution_calldata_encoders_kyber_rs → src_services_execution_calldata_encoders_kyber_rs_import_alloy_dyn_abi_dynsolvalue (imports)
- src_services_execution_calldata_encoders_kyber_rs → src_services_execution_calldata_encoders_kyber_rs_import_alloy_primitives_address_i256_u160_u256 (imports)
- src_services_execution_calldata_encoders_kyber_rs → src_services_execution_calldata_encoders_kyber_rs_import_alloy_sol_types_solcall (imports)
- src_services_execution_calldata_encoders_kyber_rs → src_services_execution_calldata_encoders_kyber_rs_import_crate_abis_executorcall_ikyberelasticpool (imports)
- src_services_execution_calldata_encoders_kyber_rs → src_services_execution_calldata_encoders_kyber_rs_import_crate_pipeline_arena_statearena (imports)
- src_services_execution_calldata_encoders_kyber_rs → src_services_execution_calldata_encoders_kyber_rs_import_crate_services_execution_calldata_types_calldatahop (imports)
- src_services_execution_calldata_encoders_kyber_rs → src_services_execution_calldata_encoders_kyber_rs_import_crate_services_execution_quote_derive_tight_v3_price_limit_kyber_pool_tokens_from_hop_quote_hop_for_execution_resolve_kyber_fee_pips (imports)
- src_services_execution_calldata_encoders_kyber_rs → src_services_execution_calldata_encoders_kyber_rs_import_super_shared_to_v3_state (imports)
- src_services_execution_calldata_encoders_kyber_rs → src_services_execution_calldata_encoders_kyber_rs_encode_kyber_hop (defines)
- src_services_execution_calldata_encoders_kyber_rs → src_services_execution_calldata_encoders_kyber_rs_import_super (imports)
- src_services_execution_calldata_encoders_kyber_rs → src_services_execution_calldata_encoders_kyber_rs_import_crate_core_types_edge_poolindex_tokenindex (imports)
- src_services_execution_calldata_encoders_kyber_rs → src_services_execution_calldata_encoders_kyber_rs_create_test_hop (defines)
- src_services_execution_calldata_encoders_kyber_rs → src_services_execution_calldata_encoders_kyber_rs_encode_kyber_hop_returns_single_call (defines)
- src_services_execution_calldata_encoders_kyber_rs → src_services_execution_calldata_encoders_kyber_rs_encode_kyber_hop_validates_recipient (defines)
- src_services_execution_calldata_encoders_kyber_rs → src_services_execution_calldata_encoders_kyber_rs_encode_kyber_hop_uses_correct_protocol_constant (defines)
- src_services_execution_calldata_encoders_kyber_rs_encode_kyber_hop_returns_single_call → src_services_execution_calldata_encoders_kyber_rs_create_test_hop (calls)
- src_services_execution_calldata_encoders_kyber_rs_encode_kyber_hop_returns_single_call → src_services_execution_calldata_encoders_kyber_rs_encode_kyber_hop (calls)
- src_services_execution_calldata_encoders_kyber_rs_encode_kyber_hop_validates_recipient → src_services_execution_calldata_encoders_kyber_rs_create_test_hop (calls)
- src_services_execution_calldata_encoders_kyber_rs_encode_kyber_hop_validates_recipient → src_services_execution_calldata_encoders_kyber_rs_encode_kyber_hop (calls)

