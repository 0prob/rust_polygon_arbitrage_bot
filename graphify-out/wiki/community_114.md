# Community 114: SharedCycleBudget

**Members:** 9

## Nodes

- **collect_cycles_dfs_parallel()** (`src_pipeline_cycle_finder_rs_collect_cycles_dfs_parallel`, Function, degree: 4)
- **find_cycles()** (`src_pipeline_cycle_finder_rs_find_cycles`, Function, degree: 4)
- **find_cycles_multi_pass()** (`src_pipeline_cycle_finder_rs_find_cycles_multi_pass`, Function, degree: 4)
- **find_cycles_multi_pass_arc()** (`src_pipeline_cycle_finder_rs_find_cycles_multi_pass_arc`, Function, degree: 8)
- **finds_triangle_cycle()** (`src_pipeline_cycle_finder_rs_finds_triangle_cycle`, Function, degree: 3)
- **parallel_dfs_finds_cycles_on_hub_graph()** (`src_pipeline_cycle_finder_rs_parallel_dfs_finds_cycles_on_hub_graph`, Function, degree: 3)
- **SharedCycleBudget** (`src_pipeline_cycle_finder_rs_sharedcyclebudget`, Struct, degree: 3)
- **.new()** (`src_pipeline_cycle_finder_rs_sharedcyclebudget_new`, Method, degree: 8)
- **.tick()** (`src_pipeline_cycle_finder_rs_sharedcyclebudget_tick`, Method, degree: 1)

## Relationships

- src_pipeline_cycle_finder_rs_sharedcyclebudget → src_pipeline_cycle_finder_rs_sharedcyclebudget_new (defines)
- src_pipeline_cycle_finder_rs_sharedcyclebudget → src_pipeline_cycle_finder_rs_sharedcyclebudget_tick (defines)
- src_pipeline_cycle_finder_rs_collect_cycles_dfs_parallel → src_pipeline_cycle_finder_rs_sharedcyclebudget_new (calls)
- src_pipeline_cycle_finder_rs_find_cycles → src_pipeline_cycle_finder_rs_find_cycles_multi_pass (calls)
- src_pipeline_cycle_finder_rs_find_cycles_multi_pass → src_pipeline_cycle_finder_rs_find_cycles_multi_pass_arc (calls)
- src_pipeline_cycle_finder_rs_find_cycles_multi_pass → src_pipeline_cycle_finder_rs_sharedcyclebudget_new (calls)
- src_pipeline_cycle_finder_rs_find_cycles_multi_pass_arc → src_pipeline_cycle_finder_rs_sharedcyclebudget_new (calls)
- src_pipeline_cycle_finder_rs_find_cycles_multi_pass_arc → src_pipeline_cycle_finder_rs_collect_cycles_dfs_parallel (calls)
- src_pipeline_cycle_finder_rs_finds_triangle_cycle → src_pipeline_cycle_finder_rs_sharedcyclebudget_new (calls)
- src_pipeline_cycle_finder_rs_finds_triangle_cycle → src_pipeline_cycle_finder_rs_find_cycles (calls)
- src_pipeline_cycle_finder_rs_parallel_dfs_finds_cycles_on_hub_graph → src_pipeline_cycle_finder_rs_sharedcyclebudget_new (calls)
- src_pipeline_cycle_finder_rs_parallel_dfs_finds_cycles_on_hub_graph → src_pipeline_cycle_finder_rs_find_cycles (calls)

