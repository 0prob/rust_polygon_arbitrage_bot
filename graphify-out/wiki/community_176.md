# Community 176: selects_unfetched_pools_in_order()

**Members:** 5

## Nodes

- **fetch_missing_pool_states()** (`src_pipeline_fetcher_rs_fetch_missing_pool_states`, Function, degree: 2)
- **includes_curve_in_fetch_targets()** (`src_pipeline_fetcher_rs_includes_curve_in_fetch_targets`, Function, degree: 2)
- **prioritizes_never_fetched_over_invalid_retries()** (`src_pipeline_fetcher_rs_prioritizes_never_fetched_over_invalid_retries`, Function, degree: 2)
- **select_fetch_targets()** (`src_pipeline_fetcher_rs_select_fetch_targets`, Function, degree: 5)
- **selects_unfetched_pools_in_order()** (`src_pipeline_fetcher_rs_selects_unfetched_pools_in_order`, Function, degree: 2)

## Relationships

- src_pipeline_fetcher_rs_fetch_missing_pool_states → src_pipeline_fetcher_rs_select_fetch_targets (calls)
- src_pipeline_fetcher_rs_selects_unfetched_pools_in_order → src_pipeline_fetcher_rs_select_fetch_targets (calls)
- src_pipeline_fetcher_rs_includes_curve_in_fetch_targets → src_pipeline_fetcher_rs_select_fetch_targets (calls)
- src_pipeline_fetcher_rs_prioritizes_never_fetched_over_invalid_retries → src_pipeline_fetcher_rs_select_fetch_targets (calls)

