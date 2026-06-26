# Community 168: test_expired_entry_is_evicted()

**Members:** 6

## Nodes

- **generation_increments_on_insert()** (`src_services_state_cache_rs_generation_increments_on_insert`, Function, degree: 3)
- **.default()** (`src_services_state_cache_rs_statecache_default`, Method, degree: 4)
- **.insert()** (`src_services_state_cache_rs_statecache_insert`, Method, degree: 5)
- **.new()** (`src_services_state_cache_rs_statecache_new`, Method, degree: 7)
- **.patch_pool()** (`src_services_state_cache_rs_statecache_patch_pool`, Method, degree: 2)
- **test_expired_entry_is_evicted()** (`src_services_state_cache_rs_test_expired_entry_is_evicted`, Function, degree: 3)

## Relationships

- src_services_state_cache_rs_statecache_default → src_services_state_cache_rs_statecache_new (calls)
- src_services_state_cache_rs_statecache_new → src_services_state_cache_rs_statecache_default (calls)
- src_services_state_cache_rs_statecache_patch_pool → src_services_state_cache_rs_statecache_new (calls)
- src_services_state_cache_rs_statecache_insert → src_services_state_cache_rs_statecache_new (calls)
- src_services_state_cache_rs_test_expired_entry_is_evicted → src_services_state_cache_rs_statecache_new (calls)
- src_services_state_cache_rs_test_expired_entry_is_evicted → src_services_state_cache_rs_statecache_insert (calls)
- src_services_state_cache_rs_generation_increments_on_insert → src_services_state_cache_rs_statecache_default (calls)
- src_services_state_cache_rs_generation_increments_on_insert → src_services_state_cache_rs_statecache_insert (calls)

