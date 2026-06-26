# Community 70: apply_slim_to_pool_state()

**Members:** 13

## Nodes

- **mod** (`src_services_partial_cache_mod_rs`, File, degree: 18)
- **apply_slim_to_pool_state()** (`src_services_partial_cache_mod_rs_apply_slim_to_pool_state`, Function, degree: 2)
- **alloy::primitives::{Address, B256, U256}** (`src_services_partial_cache_mod_rs_import_alloy_primitives_address_b256_u256`, Module, degree: 1)
- **crate::core::types::{PoolState, ProtocolType}** (`src_services_partial_cache_mod_rs_import_crate_core_types_poolstate_protocoltype`, Module, degree: 1)
- **crate::core::types::V2PoolState** (`src_services_partial_cache_mod_rs_import_crate_core_types_v2poolstate`, Module, degree: 1)
- **crate::services::state_cache::StateCache** (`src_services_partial_cache_mod_rs_import_crate_services_state_cache_statecache`, Module, degree: 1)
- **dashmap::DashMap** (`src_services_partial_cache_mod_rs_import_dashmap_dashmap`, Module, degree: 1)
- **parking_lot::RwLock** (`src_services_partial_cache_mod_rs_import_parking_lot_rwlock`, Module, degree: 1)
- **pub use decode::{LogPatch, V2_SYNC_TOPIC, V3_SWAP_TOPIC, decode_pool_log, is_streamable_protocol}** (`src_services_partial_cache_mod_rs_import_pub_use_decode_logpatch_v2_sync_topic_v3_swap_topic_decode_pool_log_is_streamable_protocol`, Module, degree: 1)
- **std::sync::Arc** (`src_services_partial_cache_mod_rs_import_std_sync_arc`, Module, degree: 1)
- **std::sync::atomic::{AtomicU64, Ordering}** (`src_services_partial_cache_mod_rs_import_std_sync_atomic_atomicu64_ordering`, Module, degree: 1)
- **super::*** (`src_services_partial_cache_mod_rs_import_super`, Module, degree: 1)
- **tokio::sync::watch** (`src_services_partial_cache_mod_rs_import_tokio_sync_watch`, Module, degree: 1)

## Relationships

- src_services_partial_cache_mod_rs → src_services_partial_cache_mod_rs_import_std_sync_arc (imports)
- src_services_partial_cache_mod_rs → src_services_partial_cache_mod_rs_import_std_sync_atomic_atomicu64_ordering (imports)
- src_services_partial_cache_mod_rs → src_services_partial_cache_mod_rs_import_alloy_primitives_address_b256_u256 (imports)
- src_services_partial_cache_mod_rs → src_services_partial_cache_mod_rs_import_dashmap_dashmap (imports)
- src_services_partial_cache_mod_rs → src_services_partial_cache_mod_rs_import_parking_lot_rwlock (imports)
- src_services_partial_cache_mod_rs → src_services_partial_cache_mod_rs_import_tokio_sync_watch (imports)
- src_services_partial_cache_mod_rs → src_services_partial_cache_mod_rs_import_pub_use_decode_logpatch_v2_sync_topic_v3_swap_topic_decode_pool_log_is_streamable_protocol (imports)
- src_services_partial_cache_mod_rs → src_services_partial_cache_mod_rs_import_crate_core_types_poolstate_protocoltype (imports)
- src_services_partial_cache_mod_rs → src_services_partial_cache_mod_rs_import_crate_services_state_cache_statecache (imports)
- src_services_partial_cache_mod_rs → src_services_partial_cache_mod_rs_apply_slim_to_pool_state (defines)
- src_services_partial_cache_mod_rs → src_services_partial_cache_mod_rs_import_super (imports)
- src_services_partial_cache_mod_rs → src_services_partial_cache_mod_rs_import_crate_core_types_v2poolstate (imports)

