# Community 67: v3_swap_min_length()

**Members:** 13

## Nodes

- **decode** (`src_services_partial_cache_decode_rs`, File, degree: 12)
- **decode_pool_log()** (`src_services_partial_cache_decode_rs_decode_pool_log`, Function, degree: 3)
- **decode_v2_sync()** (`src_services_partial_cache_decode_rs_decode_v2_sync`, Function, degree: 3)
- **decode_v3_swap()** (`src_services_partial_cache_decode_rs_decode_v3_swap`, Function, degree: 3)
- **alloy::primitives::{Address, B256, U256}** (`src_services_partial_cache_decode_rs_import_alloy_primitives_address_b256_u256`, Module, degree: 1)
- **alloy::sol_types::SolEvent** (`src_services_partial_cache_decode_rs_import_alloy_sol_types_solevent`, Module, degree: 1)
- **crate::abis::{IUniswapV2Pair, IUniswapV3Pool}** (`src_services_partial_cache_decode_rs_import_crate_abis_iuniswapv2pair_iuniswapv3pool`, Module, degree: 1)
- **super::*** (`src_services_partial_cache_decode_rs_import_super`, Module, degree: 1)
- **is_streamable_protocol()** (`src_services_partial_cache_decode_rs_is_streamable_protocol`, Function, degree: 1)
- **LogPatch** (`src_services_partial_cache_decode_rs_logpatch`, Enum, degree: 1)
- **pool_address_from_log()** (`src_services_partial_cache_decode_rs_pool_address_from_log`, Function, degree: 1)
- **v2_sync_min_length()** (`src_services_partial_cache_decode_rs_v2_sync_min_length`, Function, degree: 2)
- **v3_swap_min_length()** (`src_services_partial_cache_decode_rs_v3_swap_min_length`, Function, degree: 2)

## Relationships

- src_services_partial_cache_decode_rs → src_services_partial_cache_decode_rs_import_alloy_primitives_address_b256_u256 (imports)
- src_services_partial_cache_decode_rs → src_services_partial_cache_decode_rs_import_alloy_sol_types_solevent (imports)
- src_services_partial_cache_decode_rs → src_services_partial_cache_decode_rs_import_crate_abis_iuniswapv2pair_iuniswapv3pool (imports)
- src_services_partial_cache_decode_rs → src_services_partial_cache_decode_rs_logpatch (defines)
- src_services_partial_cache_decode_rs → src_services_partial_cache_decode_rs_decode_pool_log (defines)
- src_services_partial_cache_decode_rs → src_services_partial_cache_decode_rs_decode_v2_sync (defines)
- src_services_partial_cache_decode_rs → src_services_partial_cache_decode_rs_decode_v3_swap (defines)
- src_services_partial_cache_decode_rs → src_services_partial_cache_decode_rs_is_streamable_protocol (defines)
- src_services_partial_cache_decode_rs → src_services_partial_cache_decode_rs_pool_address_from_log (defines)
- src_services_partial_cache_decode_rs → src_services_partial_cache_decode_rs_import_super (imports)
- src_services_partial_cache_decode_rs → src_services_partial_cache_decode_rs_v2_sync_min_length (defines)
- src_services_partial_cache_decode_rs → src_services_partial_cache_decode_rs_v3_swap_min_length (defines)
- src_services_partial_cache_decode_rs_decode_pool_log → src_services_partial_cache_decode_rs_decode_v2_sync (calls)
- src_services_partial_cache_decode_rs_decode_pool_log → src_services_partial_cache_decode_rs_decode_v3_swap (calls)
- src_services_partial_cache_decode_rs_v2_sync_min_length → src_services_partial_cache_decode_rs_decode_v2_sync (calls)
- src_services_partial_cache_decode_rs_v3_swap_min_length → src_services_partial_cache_decode_rs_decode_v3_swap (calls)

