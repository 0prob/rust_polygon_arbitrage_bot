# Community 102: field_slots_are_deterministic()

**Members:** 10

## Nodes

- **v4_storage** (`src_core_utils_v4_storage_rs`, File, degree: 9)
- **compute_v4_pool_field_slot()** (`src_core_utils_v4_storage_rs_compute_v4_pool_field_slot`, Function, degree: 3)
- **compute_v4_pool_state_slot()** (`src_core_utils_v4_storage_rs_compute_v4_pool_state_slot`, Function, degree: 2)
- **decode_v4_liquidity()** (`src_core_utils_v4_storage_rs_decode_v4_liquidity`, Function, degree: 1)
- **decode_v4_slot0()** (`src_core_utils_v4_storage_rs_decode_v4_slot0`, Function, degree: 2)
- **DecodedV4Slot0** (`src_core_utils_v4_storage_rs_decodedv4slot0`, Struct, degree: 1)
- **decodes_v4_fixture_slot0_and_liquidity()** (`src_core_utils_v4_storage_rs_decodes_v4_fixture_slot0_and_liquidity`, Function, degree: 2)
- **field_slots_are_deterministic()** (`src_core_utils_v4_storage_rs_field_slots_are_deterministic`, Function, degree: 2)
- **alloy::primitives::{FixedBytes, U256, keccak256}** (`src_core_utils_v4_storage_rs_import_alloy_primitives_fixedbytes_u256_keccak256`, Module, degree: 1)
- **super::*** (`src_core_utils_v4_storage_rs_import_super`, Module, degree: 1)

## Relationships

- src_core_utils_v4_storage_rs → src_core_utils_v4_storage_rs_import_alloy_primitives_fixedbytes_u256_keccak256 (imports)
- src_core_utils_v4_storage_rs → src_core_utils_v4_storage_rs_decodedv4slot0 (defines)
- src_core_utils_v4_storage_rs → src_core_utils_v4_storage_rs_compute_v4_pool_state_slot (defines)
- src_core_utils_v4_storage_rs → src_core_utils_v4_storage_rs_compute_v4_pool_field_slot (defines)
- src_core_utils_v4_storage_rs → src_core_utils_v4_storage_rs_decode_v4_slot0 (defines)
- src_core_utils_v4_storage_rs → src_core_utils_v4_storage_rs_decode_v4_liquidity (defines)
- src_core_utils_v4_storage_rs → src_core_utils_v4_storage_rs_import_super (imports)
- src_core_utils_v4_storage_rs → src_core_utils_v4_storage_rs_decodes_v4_fixture_slot0_and_liquidity (defines)
- src_core_utils_v4_storage_rs → src_core_utils_v4_storage_rs_field_slots_are_deterministic (defines)
- src_core_utils_v4_storage_rs_compute_v4_pool_field_slot → src_core_utils_v4_storage_rs_compute_v4_pool_state_slot (calls)
- src_core_utils_v4_storage_rs_decodes_v4_fixture_slot0_and_liquidity → src_core_utils_v4_storage_rs_decode_v4_slot0 (calls)
- src_core_utils_v4_storage_rs_field_slots_are_deterministic → src_core_utils_v4_storage_rs_compute_v4_pool_field_slot (calls)

