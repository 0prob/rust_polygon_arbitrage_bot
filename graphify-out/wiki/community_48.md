# Community 48: resolve_v3_fee_pips_for_hop()

**Members:** 15

## Nodes

- **quote** (`src_services_execution_quote_rs`, File, degree: 14)
- **derive_tight_v3_price_limit()** (`src_services_execution_quote_rs_derive_tight_v3_price_limit`, Function, degree: 2)
- **derive_tight_v3_price_limit_inner()** (`src_services_execution_quote_rs_derive_tight_v3_price_limit_inner`, Function, degree: 3)
- **derive_tight_v3_price_limit_kyber()** (`src_services_execution_quote_rs_derive_tight_v3_price_limit_kyber`, Function, degree: 2)
- **alloy::primitives::{Address, U256}** (`src_services_execution_quote_rs_import_alloy_primitives_address_u256`, Module, degree: 1)
- **crate::core::math::uniswap_v3::{resolve_v3_fee_pips, simulate_v3_swap}** (`src_services_execution_quote_rs_import_crate_core_math_uniswap_v3_resolve_v3_fee_pips_simulate_v3_swap`, Module, degree: 1)
- **crate::core::types::{PoolState, V3PoolState}** (`src_services_execution_quote_rs_import_crate_core_types_poolstate_v3poolstate`, Module, degree: 1)
- **crate::pipeline::arena::StateArena** (`src_services_execution_quote_rs_import_crate_pipeline_arena_statearena`, Module, degree: 1)
- **crate::pipeline::local_sim::simulate_hop_amount_out** (`src_services_execution_quote_rs_import_crate_pipeline_local_sim_simulate_hop_amount_out`, Module, degree: 1)
- **crate::services::execution::calldata::CalldataHop** (`src_services_execution_quote_rs_import_crate_services_execution_calldata_calldatahop`, Module, degree: 1)
- **is_kyber_protocol()** (`src_services_execution_quote_rs_is_kyber_protocol`, Function, degree: 2)
- **pool_tokens_from_hop()** (`src_services_execution_quote_rs_pool_tokens_from_hop`, Function, degree: 1)
- **quote_hop_for_execution()** (`src_services_execution_quote_rs_quote_hop_for_execution`, Function, degree: 1)
- **resolve_kyber_fee_pips()** (`src_services_execution_quote_rs_resolve_kyber_fee_pips`, Function, degree: 2)
- **resolve_v3_fee_pips_for_hop()** (`src_services_execution_quote_rs_resolve_v3_fee_pips_for_hop`, Function, degree: 3)

## Relationships

- src_services_execution_quote_rs → src_services_execution_quote_rs_import_alloy_primitives_address_u256 (imports)
- src_services_execution_quote_rs → src_services_execution_quote_rs_import_crate_core_math_uniswap_v3_resolve_v3_fee_pips_simulate_v3_swap (imports)
- src_services_execution_quote_rs → src_services_execution_quote_rs_import_crate_core_types_poolstate_v3poolstate (imports)
- src_services_execution_quote_rs → src_services_execution_quote_rs_import_crate_pipeline_arena_statearena (imports)
- src_services_execution_quote_rs → src_services_execution_quote_rs_import_crate_pipeline_local_sim_simulate_hop_amount_out (imports)
- src_services_execution_quote_rs → src_services_execution_quote_rs_import_crate_services_execution_calldata_calldatahop (imports)
- src_services_execution_quote_rs → src_services_execution_quote_rs_quote_hop_for_execution (defines)
- src_services_execution_quote_rs → src_services_execution_quote_rs_resolve_v3_fee_pips_for_hop (defines)
- src_services_execution_quote_rs → src_services_execution_quote_rs_is_kyber_protocol (defines)
- src_services_execution_quote_rs → src_services_execution_quote_rs_resolve_kyber_fee_pips (defines)
- src_services_execution_quote_rs → src_services_execution_quote_rs_pool_tokens_from_hop (defines)
- src_services_execution_quote_rs → src_services_execution_quote_rs_derive_tight_v3_price_limit (defines)
- src_services_execution_quote_rs → src_services_execution_quote_rs_derive_tight_v3_price_limit_kyber (defines)
- src_services_execution_quote_rs → src_services_execution_quote_rs_derive_tight_v3_price_limit_inner (defines)
- src_services_execution_quote_rs_resolve_v3_fee_pips_for_hop → src_services_execution_quote_rs_is_kyber_protocol (calls)
- src_services_execution_quote_rs_resolve_v3_fee_pips_for_hop → src_services_execution_quote_rs_resolve_kyber_fee_pips (calls)
- src_services_execution_quote_rs_derive_tight_v3_price_limit → src_services_execution_quote_rs_derive_tight_v3_price_limit_inner (calls)
- src_services_execution_quote_rs_derive_tight_v3_price_limit_kyber → src_services_execution_quote_rs_derive_tight_v3_price_limit_inner (calls)

