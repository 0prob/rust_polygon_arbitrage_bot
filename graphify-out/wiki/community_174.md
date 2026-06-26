# Community 174: AppConfig

**Members:** 5

## Nodes

- **AppConfig** (`src_config_mod_rs_appconfig`, Struct, degree: 6)
- **.is_dry_run()** (`src_config_mod_rs_appconfig_is_dry_run`, Method, degree: 2)
- **.min_profit_matic_wei()** (`src_config_mod_rs_appconfig_min_profit_matic_wei`, Method, degree: 1)
- **.state_rpc_url()** (`src_config_mod_rs_appconfig_state_rpc_url`, Method, degree: 2)
- **.validate()** (`src_config_mod_rs_appconfig_validate`, Method, degree: 3)

## Relationships

- src_config_mod_rs_appconfig → src_config_mod_rs_appconfig_validate (defines)
- src_config_mod_rs_appconfig → src_config_mod_rs_appconfig_min_profit_matic_wei (defines)
- src_config_mod_rs_appconfig → src_config_mod_rs_appconfig_is_dry_run (defines)
- src_config_mod_rs_appconfig → src_config_mod_rs_appconfig_state_rpc_url (defines)
- src_config_mod_rs_appconfig_validate → src_config_mod_rs_appconfig_is_dry_run (calls)
- src_config_mod_rs_appconfig_validate → src_config_mod_rs_appconfig_state_rpc_url (calls)

