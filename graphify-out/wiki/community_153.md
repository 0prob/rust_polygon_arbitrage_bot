# Community 153: StreamTrigger

**Members:** 7

## Nodes

- **.default()** (`src_services_partial_cache_mod_rs_partialpoolcache_default`, Method, degree: 2)
- **.new()** (`src_services_partial_cache_mod_rs_partialpoolcache_new`, Method, degree: 2)
- **.new()** (`src_services_partial_cache_mod_rs_streamaddressset_new`, Method, degree: 7)
- **StreamTrigger** (`src_services_partial_cache_mod_rs_streamtrigger`, Struct, degree: 6)
- **.default()** (`src_services_partial_cache_mod_rs_streamtrigger_default`, Method, degree: 2)
- **.new()** (`src_services_partial_cache_mod_rs_streamtrigger_new`, Method, degree: 2)
- **.take_stream_triggered()** (`src_services_partial_cache_mod_rs_streamtrigger_take_stream_triggered`, Method, degree: 1)

## Relationships

- src_services_partial_cache_mod_rs_streamtrigger → src_services_partial_cache_mod_rs_streamtrigger_new (defines)
- src_services_partial_cache_mod_rs_streamtrigger → src_services_partial_cache_mod_rs_streamtrigger_take_stream_triggered (defines)
- src_services_partial_cache_mod_rs_streamtrigger → src_services_partial_cache_mod_rs_streamtrigger_default (defines)
- src_services_partial_cache_mod_rs_streamtrigger_new → src_services_partial_cache_mod_rs_streamaddressset_new (calls)
- src_services_partial_cache_mod_rs_streamtrigger_default → src_services_partial_cache_mod_rs_streamaddressset_new (calls)
- src_services_partial_cache_mod_rs_partialpoolcache_new → src_services_partial_cache_mod_rs_streamaddressset_new (calls)
- src_services_partial_cache_mod_rs_partialpoolcache_default → src_services_partial_cache_mod_rs_streamaddressset_new (calls)

