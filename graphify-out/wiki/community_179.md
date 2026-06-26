# Community 179: rescore_graph_in_place()

**Members:** 5

## Nodes

- **build_graph()** (`src_pipeline_graph_rs_build_graph`, Function, degree: 5)
- **builds_two_pool_graph()** (`src_pipeline_graph_rs_builds_two_pool_graph`, Function, degree: 2)
- **edges_for_multi_token()** (`src_pipeline_graph_rs_edges_for_multi_token`, Function, degree: 2)
- **edges_for_pair()** (`src_pipeline_graph_rs_edges_for_pair`, Function, degree: 2)
- **rescore_graph_in_place()** (`src_pipeline_graph_rs_rescore_graph_in_place`, Function, degree: 2)

## Relationships

- src_pipeline_graph_rs_build_graph → src_pipeline_graph_rs_edges_for_multi_token (calls)
- src_pipeline_graph_rs_build_graph → src_pipeline_graph_rs_edges_for_pair (calls)
- src_pipeline_graph_rs_build_graph → src_pipeline_graph_rs_rescore_graph_in_place (calls)
- src_pipeline_graph_rs_builds_two_pool_graph → src_pipeline_graph_rs_build_graph (calls)

