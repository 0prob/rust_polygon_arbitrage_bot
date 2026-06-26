# compute_spot_price()

- **ID:** `src_pipeline_spot_price_rs_compute_spot_price`
- **Type:** Function
- **File:** `./src/pipeline/spot_price.rs`
- **Location:** L143
- **Community:** 14 (v2_marginal_spot())

## Relationships

- src_pipeline_spot_price_rs → src_pipeline_spot_price_rs_compute_spot_price (defines, Extracted)
- src_pipeline_spot_price_rs_spottable_ensure_edge → src_pipeline_spot_price_rs_compute_spot_price (calls, Inferred)
- src_pipeline_spot_price_rs_compute_spot_price → src_pipeline_spot_price_rs_v2_marginal_spot (calls, Inferred)
- src_pipeline_spot_price_rs_compute_spot_price → src_pipeline_spot_price_rs_cl_marginal_spot (calls, Inferred)
- src_pipeline_spot_price_rs_compute_spot_price → src_pipeline_spot_price_rs_spot_ratio (calls, Inferred)
- src_pipeline_spot_price_rs_compute_edge_log_weight_with_state → src_pipeline_spot_price_rs_compute_spot_price (calls, Inferred)
- src_pipeline_spot_price_rs_rescored_v2_cycle_has_negative_log_weight → src_pipeline_spot_price_rs_compute_spot_price (calls, Inferred)

