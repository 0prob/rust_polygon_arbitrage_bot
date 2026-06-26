# Community 85: PoolFetchPlan

**Members:** 11

## Nodes

- **plans** (`src_pipeline_pool_fetch_plans_rs`, File, degree: 18)
- **CallKind** (`src_pipeline_pool_fetch_plans_rs_callkind`, Enum, degree: 1)
- **FetchPoolInfo** (`src_pipeline_pool_fetch_plans_rs_fetchpoolinfo`, Struct, degree: 2)
- **alloy::primitives::{Address, Bytes, FixedBytes, U256}** (`src_pipeline_pool_fetch_plans_rs_import_alloy_primitives_address_bytes_fixedbytes_u256`, Module, degree: 1)
- **crate::abis::{
    IBalancerPool, IBalancerVaultRead, ICurvePool, IDodoPoolState, IUniswapV2Pair, IUniswapV3Pool,
    IUniswapV4PoolManager,
}** (`src_pipeline_pool_fetch_plans_rs_import_crate_abis_ibalancerpool_ibalancervaultread_icurvepool_idodopoolstate_iuniswapv2pair_iuniswapv3pool_iuniswapv4poolmanager`, Module, degree: 1)
- **crate::core::constants::{BALANCER_VAULT, UNISWAP_V4_POOL_MANAGER}** (`src_pipeline_pool_fetch_plans_rs_import_crate_core_constants_balancer_vault_uniswap_v4_pool_manager`, Module, degree: 1)
- **crate::core::types::ProtocolType** (`src_pipeline_pool_fetch_plans_rs_import_crate_core_types_protocoltype`, Module, degree: 1)
- **crate::core::utils::v4_storage::{V4_LIQUIDITY_OFFSET, compute_v4_pool_field_slot}** (`src_pipeline_pool_fetch_plans_rs_import_crate_core_utils_v4_storage_v4_liquidity_offset_compute_v4_pool_field_slot`, Module, degree: 1)
- **crate::pipeline::multicall::{MulticallItem, encode_call}** (`src_pipeline_pool_fetch_plans_rs_import_crate_pipeline_multicall_multicallitem_encode_call`, Module, degree: 1)
- **crate::services::discovery::DiscoveredPool** (`src_pipeline_pool_fetch_plans_rs_import_crate_services_discovery_discoveredpool`, Module, degree: 1)
- **PoolFetchPlan** (`src_pipeline_pool_fetch_plans_rs_poolfetchplan`, Struct, degree: 1)

## Relationships

- src_pipeline_pool_fetch_plans_rs → src_pipeline_pool_fetch_plans_rs_import_alloy_primitives_address_bytes_fixedbytes_u256 (imports)
- src_pipeline_pool_fetch_plans_rs → src_pipeline_pool_fetch_plans_rs_import_crate_abis_ibalancerpool_ibalancervaultread_icurvepool_idodopoolstate_iuniswapv2pair_iuniswapv3pool_iuniswapv4poolmanager (imports)
- src_pipeline_pool_fetch_plans_rs → src_pipeline_pool_fetch_plans_rs_import_crate_core_constants_balancer_vault_uniswap_v4_pool_manager (imports)
- src_pipeline_pool_fetch_plans_rs → src_pipeline_pool_fetch_plans_rs_import_crate_core_types_protocoltype (imports)
- src_pipeline_pool_fetch_plans_rs → src_pipeline_pool_fetch_plans_rs_import_crate_core_utils_v4_storage_v4_liquidity_offset_compute_v4_pool_field_slot (imports)
- src_pipeline_pool_fetch_plans_rs → src_pipeline_pool_fetch_plans_rs_import_crate_pipeline_multicall_multicallitem_encode_call (imports)
- src_pipeline_pool_fetch_plans_rs → src_pipeline_pool_fetch_plans_rs_import_crate_services_discovery_discoveredpool (imports)
- src_pipeline_pool_fetch_plans_rs → src_pipeline_pool_fetch_plans_rs_callkind (defines)
- src_pipeline_pool_fetch_plans_rs → src_pipeline_pool_fetch_plans_rs_fetchpoolinfo (defines)
- src_pipeline_pool_fetch_plans_rs → src_pipeline_pool_fetch_plans_rs_poolfetchplan (defines)

