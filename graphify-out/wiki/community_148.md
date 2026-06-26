# Community 148: set_graph_rebuild_interval()

**Members:** 7

## Nodes

- **connectivity_fingerprint()** (`src_pipeline_graph_cache_rs_connectivity_fingerprint`, Function, degree: 4)
- **.get_or_rescore_graph()** (`src_pipeline_graph_cache_rs_graphcache_get_or_rescore_graph`, Method, degree: 5)
- **.new()** (`src_pipeline_graph_cache_rs_graphcache_new`, Method, degree: 6)
- **.store()** (`src_pipeline_graph_cache_rs_graphcache_store`, Method, degree: 3)
- **rebuilds_when_pool_becomes_tradable()** (`src_pipeline_graph_cache_rs_rebuilds_when_pool_becomes_tradable`, Function, degree: 4)
- **reuses_graph_when_only_reserves_change()** (`src_pipeline_graph_cache_rs_reuses_graph_when_only_reserves_change`, Function, degree: 5)
- **set_graph_rebuild_interval()** (`src_pipeline_graph_cache_rs_set_graph_rebuild_interval`, Function, degree: 2)

## Relationships

- src_pipeline_graph_cache_rs_set_graph_rebuild_interval → src_pipeline_graph_cache_rs_graphcache_store (calls)
- src_pipeline_graph_cache_rs_graphcache_get_or_rescore_graph → src_pipeline_graph_cache_rs_graphcache_new (calls)
- src_pipeline_graph_cache_rs_connectivity_fingerprint → src_pipeline_graph_cache_rs_graphcache_new (calls)
- src_pipeline_graph_cache_rs_reuses_graph_when_only_reserves_change → src_pipeline_graph_cache_rs_graphcache_new (calls)
- src_pipeline_graph_cache_rs_reuses_graph_when_only_reserves_change → src_pipeline_graph_cache_rs_connectivity_fingerprint (calls)
- src_pipeline_graph_cache_rs_reuses_graph_when_only_reserves_change → src_pipeline_graph_cache_rs_graphcache_get_or_rescore_graph (calls)
- src_pipeline_graph_cache_rs_reuses_graph_when_only_reserves_change → src_pipeline_graph_cache_rs_graphcache_store (calls)
- src_pipeline_graph_cache_rs_rebuilds_when_pool_becomes_tradable → src_pipeline_graph_cache_rs_graphcache_new (calls)
- src_pipeline_graph_cache_rs_rebuilds_when_pool_becomes_tradable → src_pipeline_graph_cache_rs_connectivity_fingerprint (calls)
- src_pipeline_graph_cache_rs_rebuilds_when_pool_becomes_tradable → src_pipeline_graph_cache_rs_graphcache_get_or_rescore_graph (calls)

