# Community 97: load_key_material()

**Members:** 11

## Nodes

- **wallet** (`src_config_wallet_rs`, File, degree: 15)
- **env_var()** (`src_config_wallet_rs_env_var`, Function, degree: 2)
- **alloy::primitives::Address** (`src_config_wallet_rs_import_alloy_primitives_address`, Module, degree: 1)
- **alloy::signers::local::PrivateKeySigner** (`src_config_wallet_rs_import_alloy_signers_local_privatekeysigner`, Module, degree: 1)
- **crate::config::{ExecutionConfig, OracleConfig, RoutingConfig, RpcConfig}** (`src_config_wallet_rs_import_crate_config_executionconfig_oracleconfig_routingconfig_rpcconfig`, Module, degree: 1)
- **std::fs** (`src_config_wallet_rs_import_std_fs`, Module, degree: 1)
- **std::path::Path** (`src_config_wallet_rs_import_std_path_path`, Module, degree: 1)
- **super::*** (`src_config_wallet_rs_import_super`, Module, degree: 1)
- **super::AppConfig** (`src_config_wallet_rs_import_super_appconfig`, Module, degree: 1)
- **zeroize::Zeroizing** (`src_config_wallet_rs_import_zeroize_zeroizing`, Module, degree: 1)
- **load_key_material()** (`src_config_wallet_rs_load_key_material`, Function, degree: 3)

## Relationships

- src_config_wallet_rs → src_config_wallet_rs_import_std_fs (imports)
- src_config_wallet_rs → src_config_wallet_rs_import_std_path_path (imports)
- src_config_wallet_rs → src_config_wallet_rs_import_alloy_primitives_address (imports)
- src_config_wallet_rs → src_config_wallet_rs_import_alloy_signers_local_privatekeysigner (imports)
- src_config_wallet_rs → src_config_wallet_rs_import_zeroize_zeroizing (imports)
- src_config_wallet_rs → src_config_wallet_rs_import_super_appconfig (imports)
- src_config_wallet_rs → src_config_wallet_rs_load_key_material (defines)
- src_config_wallet_rs → src_config_wallet_rs_env_var (defines)
- src_config_wallet_rs → src_config_wallet_rs_import_super (imports)
- src_config_wallet_rs → src_config_wallet_rs_import_crate_config_executionconfig_oracleconfig_routingconfig_rpcconfig (imports)
- src_config_wallet_rs_load_key_material → src_config_wallet_rs_env_var (calls)

