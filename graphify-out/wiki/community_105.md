# Community 105: GraphCache

**Members:** 10

## Nodes

- **full_rebuild_interval()** (`src_pipeline_graph_cache_rs_full_rebuild_interval`, Function, degree: 2)
- **GraphCache** (`src_pipeline_graph_cache_rs_graphcache`, Struct, degree: 12)
- **.can_rescore_cycles()** (`src_pipeline_graph_cache_rs_graphcache_can_rescore_cycles`, Method, degree: 2)
- **.cycles()** (`src_pipeline_graph_cache_rs_graphcache_cycles`, Method, degree: 1)
- **.graph()** (`src_pipeline_graph_cache_rs_graphcache_graph`, Method, degree: 1)
- **.lf_pass_count()** (`src_pipeline_graph_cache_rs_graphcache_lf_pass_count`, Method, degree: 1)
- **.needs_connectivity_rebuild()** (`src_pipeline_graph_cache_rs_graphcache_needs_connectivity_rebuild`, Method, degree: 4)
- **.needs_cycle_refind()** (`src_pipeline_graph_cache_rs_graphcache_needs_cycle_refind`, Method, degree: 2)
- **.rescore_cached_cycles()** (`src_pipeline_graph_cache_rs_graphcache_rescore_cached_cycles`, Method, degree: 3)
- **.state_generation()** (`src_pipeline_graph_cache_rs_graphcache_state_generation`, Method, degree: 1)

## Relationships

- src_pipeline_graph_cache_rs_graphcache → src_pipeline_graph_cache_rs_graphcache_needs_connectivity_rebuild (defines)
- src_pipeline_graph_cache_rs_graphcache → src_pipeline_graph_cache_rs_graphcache_needs_cycle_refind (defines)
- src_pipeline_graph_cache_rs_graphcache → src_pipeline_graph_cache_rs_graphcache_can_rescore_cycles (defines)
- src_pipeline_graph_cache_rs_graphcache → src_pipeline_graph_cache_rs_graphcache_lf_pass_count (defines)
- src_pipeline_graph_cache_rs_graphcache → src_pipeline_graph_cache_rs_graphcache_graph (defines)
- src_pipeline_graph_cache_rs_graphcache → src_pipeline_graph_cache_rs_graphcache_cycles (defines)
- src_pipeline_graph_cache_rs_graphcache → src_pipeline_graph_cache_rs_graphcache_state_generation (defines)
- src_pipeline_graph_cache_rs_graphcache → src_pipeline_graph_cache_rs_graphcache_rescore_cached_cycles (defines)
- src_pipeline_graph_cache_rs_graphcache_needs_connectivity_rebuild → src_pipeline_graph_cache_rs_full_rebuild_interval (calls)
- src_pipeline_graph_cache_rs_graphcache_needs_cycle_refind → src_pipeline_graph_cache_rs_graphcache_needs_connectivity_rebuild (calls)
- src_pipeline_graph_cache_rs_graphcache_rescore_cached_cycles → src_pipeline_graph_cache_rs_graphcache_can_rescore_cycles (calls)

