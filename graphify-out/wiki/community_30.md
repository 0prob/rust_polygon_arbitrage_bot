# Community 30: TokenMeta

**Members:** 17

## Nodes

- **discovery** (`src_services_discovery_rs`, File, degree: 21)
- **discovered_to_pool_meta()** (`src_services_discovery_rs_discovered_to_pool_meta`, Function, degree: 2)
- **discovered_to_pool_meta_propagates_hooks_and_tick_spacing()** (`src_services_discovery_rs_discovered_to_pool_meta_propagates_hooks_and_tick_spacing`, Function, degree: 2)
- **DiscoveredPool** (`src_services_discovery_rs_discoveredpool`, Struct, degree: 1)
- **alloy::primitives::{Address, FixedBytes, keccak256}** (`src_services_discovery_rs_import_alloy_primitives_address_fixedbytes_keccak256`, Module, degree: 1)
- **crate::core::protocol::{fee_to_bps, is_fetchable_protocol, normalize_protocol}** (`src_services_discovery_rs_import_crate_core_protocol_fee_to_bps_is_fetchable_protocol_normalize_protocol`, Module, degree: 1)
- **crate::core::types::{PoolIndex, ProtocolType, TokenIndex}** (`src_services_discovery_rs_import_crate_core_types_poolindex_protocoltype_tokenindex`, Module, degree: 1)
- **crate::pipeline::types::PoolMeta** (`src_services_discovery_rs_import_crate_pipeline_types_poolmeta`, Module, degree: 1)
- **super::*** (`src_services_discovery_rs_import_super`, Module, degree: 1)
- **is_hookless_v4_hooks()** (`src_services_discovery_rs_is_hookless_v4_hooks`, Function, degree: 1)
- **is_routable_pool()** (`src_services_discovery_rs_is_routable_pool`, Function, degree: 2)
- **is_supported_v4_pool()** (`src_services_discovery_rs_is_supported_v4_pool`, Function, degree: 3)
- **is_valid_pool_key()** (`src_services_discovery_rs_is_valid_pool_key`, Function, degree: 2)
- **parse_optional_bytes32()** (`src_services_discovery_rs_parse_optional_bytes32`, Function, degree: 2)
- **resolve_pool_identity()** (`src_services_discovery_rs_resolve_pool_identity`, Function, degree: 5)
- **synthetic_cache_address()** (`src_services_discovery_rs_synthetic_cache_address`, Function, degree: 2)
- **TokenMeta** (`src_services_discovery_rs_tokenmeta`, Struct, degree: 1)

## Relationships

- src_services_discovery_rs → src_services_discovery_rs_import_alloy_primitives_address_fixedbytes_keccak256 (imports)
- src_services_discovery_rs → src_services_discovery_rs_import_crate_core_protocol_fee_to_bps_is_fetchable_protocol_normalize_protocol (imports)
- src_services_discovery_rs → src_services_discovery_rs_import_crate_core_types_poolindex_protocoltype_tokenindex (imports)
- src_services_discovery_rs → src_services_discovery_rs_import_crate_pipeline_types_poolmeta (imports)
- src_services_discovery_rs → src_services_discovery_rs_discoveredpool (defines)
- src_services_discovery_rs → src_services_discovery_rs_tokenmeta (defines)
- src_services_discovery_rs → src_services_discovery_rs_is_valid_pool_key (defines)
- src_services_discovery_rs → src_services_discovery_rs_parse_optional_bytes32 (defines)
- src_services_discovery_rs → src_services_discovery_rs_synthetic_cache_address (defines)
- src_services_discovery_rs → src_services_discovery_rs_is_hookless_v4_hooks (defines)
- src_services_discovery_rs → src_services_discovery_rs_is_supported_v4_pool (defines)
- src_services_discovery_rs → src_services_discovery_rs_is_routable_pool (defines)
- src_services_discovery_rs → src_services_discovery_rs_resolve_pool_identity (defines)
- src_services_discovery_rs → src_services_discovery_rs_discovered_to_pool_meta (defines)
- src_services_discovery_rs → src_services_discovery_rs_import_super (imports)
- src_services_discovery_rs → src_services_discovery_rs_discovered_to_pool_meta_propagates_hooks_and_tick_spacing (defines)
- src_services_discovery_rs_is_routable_pool → src_services_discovery_rs_is_supported_v4_pool (calls)
- src_services_discovery_rs_resolve_pool_identity → src_services_discovery_rs_is_valid_pool_key (calls)
- src_services_discovery_rs_resolve_pool_identity → src_services_discovery_rs_synthetic_cache_address (calls)
- src_services_discovery_rs_resolve_pool_identity → src_services_discovery_rs_parse_optional_bytes32 (calls)
- src_services_discovery_rs_discovered_to_pool_meta_propagates_hooks_and_tick_spacing → src_services_discovery_rs_discovered_to_pool_meta (calls)

