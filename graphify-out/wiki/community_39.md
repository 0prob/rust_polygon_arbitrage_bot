# Community 39: StateArena

**Members:** 16

## Nodes

- **StateArena** (`src_pipeline_arena_rs_statearena`, Struct, degree: 16)
- **.address_to_pool()** (`src_pipeline_arena_rs_statearena_address_to_pool`, Method, degree: 1)
- **.apply_hot_cache()** (`src_pipeline_arena_rs_statearena_apply_hot_cache`, Method, degree: 1)
- **.clone()** (`src_pipeline_arena_rs_statearena_clone`, Method, degree: 4)
- **.default()** (`src_pipeline_arena_rs_statearena_default`, Method, degree: 4)
- **.new()** (`src_pipeline_arena_rs_statearena_new`, Method, degree: 4)
- **.pool_address()** (`src_pipeline_arena_rs_statearena_pool_address`, Method, degree: 1)
- **.pool_count()** (`src_pipeline_arena_rs_statearena_pool_count`, Method, degree: 1)
- **.pool_state()** (`src_pipeline_arena_rs_statearena_pool_state`, Method, degree: 1)
- **.pool_state_mut()** (`src_pipeline_arena_rs_statearena_pool_state_mut`, Method, degree: 1)
- **.refresh_pools_from_cache()** (`src_pipeline_arena_rs_statearena_refresh_pools_from_cache`, Method, degree: 2)
- **.register_pool()** (`src_pipeline_arena_rs_statearena_register_pool`, Method, degree: 2)
- **.register_token()** (`src_pipeline_arena_rs_statearena_register_token`, Method, degree: 2)
- **.sync_from_discovery()** (`src_pipeline_arena_rs_statearena_sync_from_discovery`, Method, degree: 5)
- **.token_address()** (`src_pipeline_arena_rs_statearena_token_address`, Method, degree: 1)
- **.token_count()** (`src_pipeline_arena_rs_statearena_token_count`, Method, degree: 1)

## Relationships

- src_pipeline_arena_rs_statearena → src_pipeline_arena_rs_statearena_default (defines)
- src_pipeline_arena_rs_statearena → src_pipeline_arena_rs_statearena_clone (defines)
- src_pipeline_arena_rs_statearena → src_pipeline_arena_rs_statearena_new (defines)
- src_pipeline_arena_rs_statearena → src_pipeline_arena_rs_statearena_pool_count (defines)
- src_pipeline_arena_rs_statearena → src_pipeline_arena_rs_statearena_token_count (defines)
- src_pipeline_arena_rs_statearena → src_pipeline_arena_rs_statearena_address_to_pool (defines)
- src_pipeline_arena_rs_statearena → src_pipeline_arena_rs_statearena_token_address (defines)
- src_pipeline_arena_rs_statearena → src_pipeline_arena_rs_statearena_pool_state (defines)
- src_pipeline_arena_rs_statearena → src_pipeline_arena_rs_statearena_pool_state_mut (defines)
- src_pipeline_arena_rs_statearena → src_pipeline_arena_rs_statearena_register_token (defines)
- src_pipeline_arena_rs_statearena → src_pipeline_arena_rs_statearena_pool_address (defines)
- src_pipeline_arena_rs_statearena → src_pipeline_arena_rs_statearena_register_pool (defines)
- src_pipeline_arena_rs_statearena → src_pipeline_arena_rs_statearena_apply_hot_cache (defines)
- src_pipeline_arena_rs_statearena → src_pipeline_arena_rs_statearena_refresh_pools_from_cache (defines)
- src_pipeline_arena_rs_statearena → src_pipeline_arena_rs_statearena_sync_from_discovery (defines)
- src_pipeline_arena_rs_statearena_default → src_pipeline_arena_rs_statearena_new (calls)
- src_pipeline_arena_rs_statearena_clone → src_pipeline_arena_rs_statearena_default (calls)
- src_pipeline_arena_rs_statearena_new → src_pipeline_arena_rs_statearena_default (calls)
- src_pipeline_arena_rs_statearena_refresh_pools_from_cache → src_pipeline_arena_rs_statearena_clone (calls)
- src_pipeline_arena_rs_statearena_sync_from_discovery → src_pipeline_arena_rs_statearena_new (calls)
- src_pipeline_arena_rs_statearena_sync_from_discovery → src_pipeline_arena_rs_statearena_register_token (calls)
- src_pipeline_arena_rs_statearena_sync_from_discovery → src_pipeline_arena_rs_statearena_register_pool (calls)
- src_pipeline_arena_rs_statearena_sync_from_discovery → src_pipeline_arena_rs_statearena_clone (calls)

