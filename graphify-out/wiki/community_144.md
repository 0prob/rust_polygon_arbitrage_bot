# Community 144: max_pool_index()

**Members:** 7

## Nodes

- **clamp_fee_bps()** (`src_pipeline_cycle_finder_rs_clamp_fee_bps`, Function, degree: 3)
- **collect_cycles_dfs()** (`src_pipeline_cycle_finder_rs_collect_cycles_dfs`, Function, degree: 5)
- **collect_cycles_dfs_single_start()** (`src_pipeline_cycle_finder_rs_collect_cycles_dfs_single_start`, Function, degree: 7)
- **CycleBudget** (`src_pipeline_cycle_finder_rs_cyclebudget`, Struct, degree: 2)
- **.tick()** (`src_pipeline_cycle_finder_rs_cyclebudget_tick`, Method, degree: 4)
- **edges_from_path()** (`src_pipeline_cycle_finder_rs_edges_from_path`, Function, degree: 3)
- **max_pool_index()** (`src_pipeline_cycle_finder_rs_max_pool_index`, Function, degree: 3)

## Relationships

- src_pipeline_cycle_finder_rs_cyclebudget → src_pipeline_cycle_finder_rs_cyclebudget_tick (defines)
- src_pipeline_cycle_finder_rs_collect_cycles_dfs → src_pipeline_cycle_finder_rs_cyclebudget_tick (calls)
- src_pipeline_cycle_finder_rs_collect_cycles_dfs → src_pipeline_cycle_finder_rs_edges_from_path (calls)
- src_pipeline_cycle_finder_rs_collect_cycles_dfs → src_pipeline_cycle_finder_rs_clamp_fee_bps (calls)
- src_pipeline_cycle_finder_rs_collect_cycles_dfs_single_start → src_pipeline_cycle_finder_rs_max_pool_index (calls)
- src_pipeline_cycle_finder_rs_collect_cycles_dfs_single_start → src_pipeline_cycle_finder_rs_cyclebudget_tick (calls)
- src_pipeline_cycle_finder_rs_collect_cycles_dfs_single_start → src_pipeline_cycle_finder_rs_edges_from_path (calls)
- src_pipeline_cycle_finder_rs_collect_cycles_dfs_single_start → src_pipeline_cycle_finder_rs_clamp_fee_bps (calls)

