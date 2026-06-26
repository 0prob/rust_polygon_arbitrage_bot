# Community 96: StateCache

**Members:** 11

## Nodes

- **StateCache** (`src_services_state_cache_rs_statecache`, Struct, degree: 15)
- **.addresses()** (`src_services_state_cache_rs_statecache_addresses`, Method, degree: 1)
- **.classify_for_fetch()** (`src_services_state_cache_rs_statecache_classify_for_fetch`, Method, degree: 4)
- **.contains()** (`src_services_state_cache_rs_statecache_contains`, Method, degree: 2)
- **.generation()** (`src_services_state_cache_rs_statecache_generation`, Method, degree: 1)
- **.get()** (`src_services_state_cache_rs_statecache_get`, Method, degree: 4)
- **.get_arc()** (`src_services_state_cache_rs_statecache_get_arc`, Method, degree: 2)
- **.is_empty()** (`src_services_state_cache_rs_statecache_is_empty`, Method, degree: 3)
- **.len()** (`src_services_state_cache_rs_statecache_len`, Method, degree: 3)
- **.lookup_pool_state()** (`src_services_state_cache_rs_statecache_lookup_pool_state`, Method, degree: 5)
- **.with_ttls()** (`src_services_state_cache_rs_statecache_with_ttls`, Method, degree: 1)

## Relationships

- src_services_state_cache_rs_statecache → src_services_state_cache_rs_statecache_with_ttls (defines)
- src_services_state_cache_rs_statecache → src_services_state_cache_rs_statecache_generation (defines)
- src_services_state_cache_rs_statecache → src_services_state_cache_rs_statecache_len (defines)
- src_services_state_cache_rs_statecache → src_services_state_cache_rs_statecache_is_empty (defines)
- src_services_state_cache_rs_statecache → src_services_state_cache_rs_statecache_lookup_pool_state (defines)
- src_services_state_cache_rs_statecache → src_services_state_cache_rs_statecache_get (defines)
- src_services_state_cache_rs_statecache → src_services_state_cache_rs_statecache_get_arc (defines)
- src_services_state_cache_rs_statecache → src_services_state_cache_rs_statecache_contains (defines)
- src_services_state_cache_rs_statecache → src_services_state_cache_rs_statecache_addresses (defines)
- src_services_state_cache_rs_statecache → src_services_state_cache_rs_statecache_classify_for_fetch (defines)
- src_services_state_cache_rs_statecache_is_empty → src_services_state_cache_rs_statecache_len (calls)
- src_services_state_cache_rs_statecache_lookup_pool_state → src_services_state_cache_rs_statecache_get (calls)
- src_services_state_cache_rs_statecache_get → src_services_state_cache_rs_statecache_lookup_pool_state (calls)
- src_services_state_cache_rs_statecache_get_arc → src_services_state_cache_rs_statecache_lookup_pool_state (calls)
- src_services_state_cache_rs_statecache_contains → src_services_state_cache_rs_statecache_lookup_pool_state (calls)
- src_services_state_cache_rs_statecache_classify_for_fetch → src_services_state_cache_rs_statecache_get (calls)
- src_services_state_cache_rs_statecache_classify_for_fetch → src_services_state_cache_rs_statecache_is_empty (calls)

