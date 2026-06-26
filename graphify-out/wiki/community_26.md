# Community 26: v4_single_hop_matches_ts_fixture()

**Members:** 18

## Nodes

- **calldata_parity** (`tests_calldata_parity_rs`, File, degree: 17)
- **addr()** (`tests_calldata_parity_rs_addr`, Function, degree: 5)
- **execute_arb_wrapper_matches_ts_fixture()** (`tests_calldata_parity_rs_execute_arb_wrapper_matches_ts_fixture`, Function, degree: 2)
- **fixture_reserve()** (`tests_calldata_parity_rs_fixture_reserve`, Function, degree: 2)
- **alloy::hex** (`tests_calldata_parity_rs_import_alloy_hex`, Module, degree: 1)
- **alloy::primitives::{Address, Bytes, FixedBytes, U256}** (`tests_calldata_parity_rs_import_alloy_primitives_address_bytes_fixedbytes_u256`, Module, degree: 1)
- **rpbot::abis::ExecutorCall** (`tests_calldata_parity_rs_import_rpbot_abis_executorcall`, Module, degree: 1)
- **rpbot::core::types::{Edge, PoolState, ProtocolType, V3PoolState, V4PoolState}** (`tests_calldata_parity_rs_import_rpbot_core_types_edge_poolstate_protocoltype_v3poolstate_v4poolstate`, Module, degree: 1)
- **rpbot::pipeline::arena::StateArena** (`tests_calldata_parity_rs_import_rpbot_pipeline_arena_statearena`, Module, degree: 1)
- **rpbot::pipeline::types::PoolMeta** (`tests_calldata_parity_rs_import_rpbot_pipeline_types_poolmeta`, Module, degree: 1)
- **rpbot::services::execution::calldata::{
    RouteEncodeConfig, build_arb_calldata, build_calldata_hops, compute_route_hash, encode_route,
}** (`tests_calldata_parity_rs_import_rpbot_services_execution_calldata_routeencodeconfig_build_arb_calldata_build_calldata_hops_compute_route_hash_encode_route`, Module, degree: 1)
- **std::str::FromStr** (`tests_calldata_parity_rs_import_std_str_fromstr`, Module, degree: 1)
- **kyber_single_hop_matches_ts_fixture()** (`tests_calldata_parity_rs_kyber_single_hop_matches_ts_fixture`, Function, degree: 3)
- **multi_hop_v2_matches_ts_fixture()** (`tests_calldata_parity_rs_multi_hop_v2_matches_ts_fixture`, Function, degree: 3)
- **register_v2_pool()** (`tests_calldata_parity_rs_register_v2_pool`, Function, degree: 3)
- **route_hash_matches_ts_simple_fixture()** (`tests_calldata_parity_rs_route_hash_matches_ts_simple_fixture`, Function, degree: 1)
- **test_encode_cfg()** (`tests_calldata_parity_rs_test_encode_cfg`, Function, degree: 3)
- **v4_single_hop_matches_ts_fixture()** (`tests_calldata_parity_rs_v4_single_hop_matches_ts_fixture`, Function, degree: 3)

## Relationships

- tests_calldata_parity_rs → tests_calldata_parity_rs_import_std_str_fromstr (imports)
- tests_calldata_parity_rs → tests_calldata_parity_rs_import_alloy_hex (imports)
- tests_calldata_parity_rs → tests_calldata_parity_rs_import_alloy_primitives_address_bytes_fixedbytes_u256 (imports)
- tests_calldata_parity_rs → tests_calldata_parity_rs_import_rpbot_abis_executorcall (imports)
- tests_calldata_parity_rs → tests_calldata_parity_rs_import_rpbot_core_types_edge_poolstate_protocoltype_v3poolstate_v4poolstate (imports)
- tests_calldata_parity_rs → tests_calldata_parity_rs_import_rpbot_pipeline_arena_statearena (imports)
- tests_calldata_parity_rs → tests_calldata_parity_rs_import_rpbot_pipeline_types_poolmeta (imports)
- tests_calldata_parity_rs → tests_calldata_parity_rs_import_rpbot_services_execution_calldata_routeencodeconfig_build_arb_calldata_build_calldata_hops_compute_route_hash_encode_route (imports)
- tests_calldata_parity_rs → tests_calldata_parity_rs_addr (defines)
- tests_calldata_parity_rs → tests_calldata_parity_rs_fixture_reserve (defines)
- tests_calldata_parity_rs → tests_calldata_parity_rs_register_v2_pool (defines)
- tests_calldata_parity_rs → tests_calldata_parity_rs_test_encode_cfg (defines)
- tests_calldata_parity_rs → tests_calldata_parity_rs_route_hash_matches_ts_simple_fixture (defines)
- tests_calldata_parity_rs → tests_calldata_parity_rs_multi_hop_v2_matches_ts_fixture (defines)
- tests_calldata_parity_rs → tests_calldata_parity_rs_execute_arb_wrapper_matches_ts_fixture (defines)
- tests_calldata_parity_rs → tests_calldata_parity_rs_v4_single_hop_matches_ts_fixture (defines)
- tests_calldata_parity_rs → tests_calldata_parity_rs_kyber_single_hop_matches_ts_fixture (defines)
- tests_calldata_parity_rs_register_v2_pool → tests_calldata_parity_rs_fixture_reserve (calls)
- tests_calldata_parity_rs_multi_hop_v2_matches_ts_fixture → tests_calldata_parity_rs_addr (calls)
- tests_calldata_parity_rs_multi_hop_v2_matches_ts_fixture → tests_calldata_parity_rs_register_v2_pool (calls)
- tests_calldata_parity_rs_execute_arb_wrapper_matches_ts_fixture → tests_calldata_parity_rs_addr (calls)
- tests_calldata_parity_rs_v4_single_hop_matches_ts_fixture → tests_calldata_parity_rs_addr (calls)
- tests_calldata_parity_rs_v4_single_hop_matches_ts_fixture → tests_calldata_parity_rs_test_encode_cfg (calls)
- tests_calldata_parity_rs_kyber_single_hop_matches_ts_fixture → tests_calldata_parity_rs_addr (calls)
- tests_calldata_parity_rs_kyber_single_hop_matches_ts_fixture → tests_calldata_parity_rs_test_encode_cfg (calls)

