# Community 18: decode_v4()

**Members:** 21

## Nodes

- **decode** (`src_pipeline_pool_fetch_decode_rs`, File, degree: 20)
- **decode_balancer()** (`src_pipeline_pool_fetch_decode_rs_decode_balancer`, Function, degree: 2)
- **decode_curve_crypto()** (`src_pipeline_pool_fetch_decode_rs_decode_curve_crypto`, Function, degree: 3)
- **decode_curve_stable()** (`src_pipeline_pool_fetch_decode_rs_decode_curve_stable`, Function, degree: 4)
- **decode_dodo()** (`src_pipeline_pool_fetch_decode_rs_decode_dodo`, Function, degree: 3)
- **decode_plan()** (`src_pipeline_pool_fetch_decode_rs_decode_plan`, Function, degree: 8)
- **decode_u128_word()** (`src_pipeline_pool_fetch_decode_rs_decode_u128_word`, Function, degree: 2)
- **decode_u24_fee()** (`src_pipeline_pool_fetch_decode_rs_decode_u24_fee`, Function, degree: 2)
- **decode_u256()** (`src_pipeline_pool_fetch_decode_rs_decode_u256`, Function, degree: 3)
- **decode_v2()** (`src_pipeline_pool_fetch_decode_rs_decode_v2`, Function, degree: 3)
- **decode_v2_reserves()** (`src_pipeline_pool_fetch_decode_rs_decode_v2_reserves`, Function, degree: 2)
- **decode_v3()** (`src_pipeline_pool_fetch_decode_rs_decode_v3`, Function, degree: 5)
- **decode_v3_slot0()** (`src_pipeline_pool_fetch_decode_rs_decode_v3_slot0`, Function, degree: 2)
- **decode_v4()** (`src_pipeline_pool_fetch_decode_rs_decode_v4`, Function, degree: 2)
- **alloy::primitives::{Bytes, U256}** (`src_pipeline_pool_fetch_decode_rs_import_alloy_primitives_bytes_u256`, Module, degree: 1)
- **alloy::sol_types::SolCall** (`src_pipeline_pool_fetch_decode_rs_import_alloy_sol_types_solcall`, Module, degree: 1)
- **crate::abis::{
    IBalancerPool, IBalancerVaultRead, ICurvePool, IDodoPoolState, IUniswapV4PoolManager,
}** (`src_pipeline_pool_fetch_decode_rs_import_crate_abis_ibalancerpool_ibalancervaultread_icurvepool_idodopoolstate_iuniswapv4poolmanager`, Module, degree: 1)
- **crate::core::math::balancer::balancer_swap_fee_from_pool_meta_fee** (`src_pipeline_pool_fetch_decode_rs_import_crate_core_math_balancer_balancer_swap_fee_from_pool_meta_fee`, Module, degree: 1)
- **crate::core::types::{
    BalancerPoolKind, BalancerPoolState, CurvePoolState, DodoPoolState, PoolState, ProtocolType,
    V2PoolState, V3PoolState, V4PoolState,
}** (`src_pipeline_pool_fetch_decode_rs_import_crate_core_types_balancerpoolkind_balancerpoolstate_curvepoolstate_dodopoolstate_poolstate_protocoltype_v2poolstate_v3poolstate_v4poolstate`, Module, degree: 1)
- **crate::core::utils::v4_storage::{decode_v4_liquidity, decode_v4_slot0}** (`src_pipeline_pool_fetch_decode_rs_import_crate_core_utils_v4_storage_decode_v4_liquidity_decode_v4_slot0`, Module, degree: 1)
- **super::plans::PoolFetchPlan** (`src_pipeline_pool_fetch_decode_rs_import_super_plans_poolfetchplan`, Module, degree: 1)

## Relationships

- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_import_alloy_primitives_bytes_u256 (imports)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_import_alloy_sol_types_solcall (imports)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_import_crate_abis_ibalancerpool_ibalancervaultread_icurvepool_idodopoolstate_iuniswapv4poolmanager (imports)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_import_crate_core_math_balancer_balancer_swap_fee_from_pool_meta_fee (imports)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_import_crate_core_types_balancerpoolkind_balancerpoolstate_curvepoolstate_dodopoolstate_poolstate_protocoltype_v2poolstate_v3poolstate_v4poolstate (imports)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_import_crate_core_utils_v4_storage_decode_v4_liquidity_decode_v4_slot0 (imports)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_import_super_plans_poolfetchplan (imports)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_decode_u256 (defines)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_decode_v2_reserves (defines)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_decode_v3_slot0 (defines)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_decode_u128_word (defines)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_decode_u24_fee (defines)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_decode_plan (defines)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_decode_v2 (defines)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_decode_v3 (defines)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_decode_v4 (defines)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_decode_dodo (defines)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_decode_curve_stable (defines)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_decode_curve_crypto (defines)
- src_pipeline_pool_fetch_decode_rs → src_pipeline_pool_fetch_decode_rs_decode_balancer (defines)
- src_pipeline_pool_fetch_decode_rs_decode_plan → src_pipeline_pool_fetch_decode_rs_decode_v2 (calls)
- src_pipeline_pool_fetch_decode_rs_decode_plan → src_pipeline_pool_fetch_decode_rs_decode_v3 (calls)
- src_pipeline_pool_fetch_decode_rs_decode_plan → src_pipeline_pool_fetch_decode_rs_decode_v4 (calls)
- src_pipeline_pool_fetch_decode_rs_decode_plan → src_pipeline_pool_fetch_decode_rs_decode_dodo (calls)
- src_pipeline_pool_fetch_decode_rs_decode_plan → src_pipeline_pool_fetch_decode_rs_decode_curve_stable (calls)
- src_pipeline_pool_fetch_decode_rs_decode_plan → src_pipeline_pool_fetch_decode_rs_decode_curve_crypto (calls)
- src_pipeline_pool_fetch_decode_rs_decode_plan → src_pipeline_pool_fetch_decode_rs_decode_balancer (calls)
- src_pipeline_pool_fetch_decode_rs_decode_v2 → src_pipeline_pool_fetch_decode_rs_decode_v2_reserves (calls)
- src_pipeline_pool_fetch_decode_rs_decode_v3 → src_pipeline_pool_fetch_decode_rs_decode_v3_slot0 (calls)
- src_pipeline_pool_fetch_decode_rs_decode_v3 → src_pipeline_pool_fetch_decode_rs_decode_u128_word (calls)
- src_pipeline_pool_fetch_decode_rs_decode_v3 → src_pipeline_pool_fetch_decode_rs_decode_u24_fee (calls)
- src_pipeline_pool_fetch_decode_rs_decode_dodo → src_pipeline_pool_fetch_decode_rs_decode_u256 (calls)
- src_pipeline_pool_fetch_decode_rs_decode_curve_stable → src_pipeline_pool_fetch_decode_rs_decode_u256 (calls)
- src_pipeline_pool_fetch_decode_rs_decode_curve_crypto → src_pipeline_pool_fetch_decode_rs_decode_curve_stable (calls)

