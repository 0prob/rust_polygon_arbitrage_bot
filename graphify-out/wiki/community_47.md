# Community 47: u256_to_u128()

**Members:** 16

## Nodes

- **gas** (`src_services_execution_gas_rs`, File, degree: 15)
- **buffer_gas_limit()** (`src_services_execution_gas_rs_buffer_gas_limit`, Function, degree: 2)
- **compute_conservative_gas_price()** (`src_services_execution_gas_rs_compute_conservative_gas_price`, Function, degree: 1)
- **conservative_gas_price_wei()** (`src_services_execution_gas_rs_conservative_gas_price_wei`, Function, degree: 1)
- **default_priority_fee_wei()** (`src_services_execution_gas_rs_default_priority_fee_wei`, Function, degree: 1)
- **estimate_route_gas_from_hops()** (`src_services_execution_gas_rs_estimate_route_gas_from_hops`, Function, degree: 1)
- **FeeSnapshot** (`src_services_execution_gas_rs_feesnapshot`, Struct, degree: 1)
- **gas_drift_bps()** (`src_services_execution_gas_rs_gas_drift_bps`, Function, degree: 1)
- **anyhow::{Result, anyhow}** (`src_services_execution_gas_rs_import_anyhow_result_anyhow`, Module, degree: 1)
- **ruint::aliases::U256** (`src_services_execution_gas_rs_import_ruint_aliases_u256`, Module, degree: 1)
- **super::*** (`src_services_execution_gas_rs_import_super`, Module, degree: 1)
- **pick_buffered_gas_limit()** (`src_services_execution_gas_rs_pick_buffered_gas_limit`, Function, degree: 3)
- **pick_live_gas_fails_on_zero()** (`src_services_execution_gas_rs_pick_live_gas_fails_on_zero`, Function, degree: 1)
- **pick_live_gas_limit()** (`src_services_execution_gas_rs_pick_live_gas_limit`, Function, degree: 4)
- **pick_live_gas_uses_max_of_sim_and_dry_run()** (`src_services_execution_gas_rs_pick_live_gas_uses_max_of_sim_and_dry_run`, Function, degree: 2)
- **u256_to_u128()** (`src_services_execution_gas_rs_u256_to_u128`, Function, degree: 2)

## Relationships

- src_services_execution_gas_rs → src_services_execution_gas_rs_import_anyhow_result_anyhow (imports)
- src_services_execution_gas_rs → src_services_execution_gas_rs_import_ruint_aliases_u256 (imports)
- src_services_execution_gas_rs → src_services_execution_gas_rs_feesnapshot (defines)
- src_services_execution_gas_rs → src_services_execution_gas_rs_u256_to_u128 (defines)
- src_services_execution_gas_rs → src_services_execution_gas_rs_buffer_gas_limit (defines)
- src_services_execution_gas_rs → src_services_execution_gas_rs_pick_buffered_gas_limit (defines)
- src_services_execution_gas_rs → src_services_execution_gas_rs_pick_live_gas_limit (defines)
- src_services_execution_gas_rs → src_services_execution_gas_rs_estimate_route_gas_from_hops (defines)
- src_services_execution_gas_rs → src_services_execution_gas_rs_compute_conservative_gas_price (defines)
- src_services_execution_gas_rs → src_services_execution_gas_rs_conservative_gas_price_wei (defines)
- src_services_execution_gas_rs → src_services_execution_gas_rs_default_priority_fee_wei (defines)
- src_services_execution_gas_rs → src_services_execution_gas_rs_gas_drift_bps (defines)
- src_services_execution_gas_rs → src_services_execution_gas_rs_import_super (imports)
- src_services_execution_gas_rs → src_services_execution_gas_rs_pick_live_gas_uses_max_of_sim_and_dry_run (defines)
- src_services_execution_gas_rs → src_services_execution_gas_rs_pick_live_gas_fails_on_zero (defines)
- src_services_execution_gas_rs_pick_buffered_gas_limit → src_services_execution_gas_rs_buffer_gas_limit (calls)
- src_services_execution_gas_rs_pick_live_gas_limit → src_services_execution_gas_rs_pick_buffered_gas_limit (calls)
- src_services_execution_gas_rs_pick_live_gas_limit → src_services_execution_gas_rs_u256_to_u128 (calls)
- src_services_execution_gas_rs_pick_live_gas_uses_max_of_sim_and_dry_run → src_services_execution_gas_rs_pick_live_gas_limit (calls)

