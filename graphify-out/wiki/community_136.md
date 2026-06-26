# Community 136: reweight_graph_with_spot()

**Members:** 8

## Nodes

- **compute_edge_log_weight()** (`src_pipeline_spot_price_rs_compute_edge_log_weight`, Function, degree: 5)
- **compute_edge_log_weight_with_table()** (`src_pipeline_spot_price_rs_compute_edge_log_weight_with_table`, Function, degree: 5)
- **edge_log_weight_from_spot()** (`src_pipeline_spot_price_rs_edge_log_weight_from_spot`, Function, degree: 5)
- **fee_log_weight()** (`src_pipeline_spot_price_rs_fee_log_weight`, Function, degree: 2)
- **gas_log_penalty_for_cycle()** (`src_pipeline_spot_price_rs_gas_log_penalty_for_cycle`, Function, degree: 3)
- **hop_penalty()** (`src_pipeline_spot_price_rs_hop_penalty`, Function, degree: 3)
- **rescore_cycles_with_table_and_gas()** (`src_pipeline_spot_price_rs_rescore_cycles_with_table_and_gas`, Function, degree: 7)
- **reweight_graph_with_spot()** (`src_pipeline_spot_price_rs_reweight_graph_with_spot`, Function, degree: 3)

## Relationships

- src_pipeline_spot_price_rs_compute_edge_log_weight → src_pipeline_spot_price_rs_fee_log_weight (calls)
- src_pipeline_spot_price_rs_edge_log_weight_from_spot → src_pipeline_spot_price_rs_compute_edge_log_weight (calls)
- src_pipeline_spot_price_rs_compute_edge_log_weight_with_table → src_pipeline_spot_price_rs_compute_edge_log_weight (calls)
- src_pipeline_spot_price_rs_compute_edge_log_weight_with_table → src_pipeline_spot_price_rs_edge_log_weight_from_spot (calls)
- src_pipeline_spot_price_rs_rescore_cycles_with_table_and_gas → src_pipeline_spot_price_rs_compute_edge_log_weight (calls)
- src_pipeline_spot_price_rs_rescore_cycles_with_table_and_gas → src_pipeline_spot_price_rs_edge_log_weight_from_spot (calls)
- src_pipeline_spot_price_rs_rescore_cycles_with_table_and_gas → src_pipeline_spot_price_rs_gas_log_penalty_for_cycle (calls)
- src_pipeline_spot_price_rs_rescore_cycles_with_table_and_gas → src_pipeline_spot_price_rs_hop_penalty (calls)
- src_pipeline_spot_price_rs_reweight_graph_with_spot → src_pipeline_spot_price_rs_compute_edge_log_weight_with_table (calls)

