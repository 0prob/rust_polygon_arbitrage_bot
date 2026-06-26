# Community 84: gas_oracle

**Members:** 11

## Nodes

- **gas_oracle** (`src_services_execution_gas_oracle_rs`, File, degree: 11)
- **alloy::eips::BlockNumberOrTag** (`src_services_execution_gas_oracle_rs_import_alloy_eips_blocknumberortag`, Module, degree: 1)
- **alloy::network::Ethereum** (`src_services_execution_gas_oracle_rs_import_alloy_network_ethereum`, Module, degree: 1)
- **alloy::providers::{Provider, ProviderBuilder}** (`src_services_execution_gas_oracle_rs_import_alloy_providers_provider_providerbuilder`, Module, degree: 1)
- **arc_swap::ArcSwap** (`src_services_execution_gas_oracle_rs_import_arc_swap_arcswap`, Module, degree: 1)
- **ruint::aliases::U256** (`src_services_execution_gas_oracle_rs_import_ruint_aliases_u256`, Module, degree: 1)
- **std::sync::Arc** (`src_services_execution_gas_oracle_rs_import_std_sync_arc`, Module, degree: 1)
- **std::time::Duration** (`src_services_execution_gas_oracle_rs_import_std_time_duration`, Module, degree: 1)
- **super::gas::{
    DEFAULT_CONSERVATIVE_GAS_PRICE_WEI, FeeSnapshot, compute_conservative_gas_price,
    conservative_gas_price_wei, default_priority_fee_wei,
}** (`src_services_execution_gas_oracle_rs_import_super_gas_default_conservative_gas_price_wei_feesnapshot_compute_conservative_gas_price_conservative_gas_price_wei_default_priority_fee_wei`, Module, degree: 1)
- **tokio::sync::watch** (`src_services_execution_gas_oracle_rs_import_tokio_sync_watch`, Module, degree: 1)
- **tracing::{debug, warn}** (`src_services_execution_gas_oracle_rs_import_tracing_debug_warn`, Module, degree: 1)

## Relationships

- src_services_execution_gas_oracle_rs → src_services_execution_gas_oracle_rs_import_std_sync_arc (imports)
- src_services_execution_gas_oracle_rs → src_services_execution_gas_oracle_rs_import_std_time_duration (imports)
- src_services_execution_gas_oracle_rs → src_services_execution_gas_oracle_rs_import_alloy_eips_blocknumberortag (imports)
- src_services_execution_gas_oracle_rs → src_services_execution_gas_oracle_rs_import_alloy_network_ethereum (imports)
- src_services_execution_gas_oracle_rs → src_services_execution_gas_oracle_rs_import_alloy_providers_provider_providerbuilder (imports)
- src_services_execution_gas_oracle_rs → src_services_execution_gas_oracle_rs_import_arc_swap_arcswap (imports)
- src_services_execution_gas_oracle_rs → src_services_execution_gas_oracle_rs_import_ruint_aliases_u256 (imports)
- src_services_execution_gas_oracle_rs → src_services_execution_gas_oracle_rs_import_tokio_sync_watch (imports)
- src_services_execution_gas_oracle_rs → src_services_execution_gas_oracle_rs_import_tracing_debug_warn (imports)
- src_services_execution_gas_oracle_rs → src_services_execution_gas_oracle_rs_import_super_gas_default_conservative_gas_price_wei_feesnapshot_compute_conservative_gas_price_conservative_gas_price_wei_default_priority_fee_wei (imports)

