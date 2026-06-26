# Community 161: RoutingConfig

**Members:** 6

## Nodes

- **default_cycle_finder()** (`src_config_mod_rs_default_cycle_finder`, Function, degree: 2)
- **default_enumeration_max_paths()** (`src_config_mod_rs_default_enumeration_max_paths`, Function, degree: 2)
- **default_max_hops()** (`src_config_mod_rs_default_max_hops`, Function, degree: 2)
- **default_ternary_search_iterations()** (`src_config_mod_rs_default_ternary_search_iterations`, Function, degree: 2)
- **RoutingConfig** (`src_config_mod_rs_routingconfig`, Struct, degree: 2)
- **.default()** (`src_config_mod_rs_routingconfig_default`, Method, degree: 5)

## Relationships

- src_config_mod_rs_routingconfig → src_config_mod_rs_routingconfig_default (defines)
- src_config_mod_rs_routingconfig_default → src_config_mod_rs_default_max_hops (calls)
- src_config_mod_rs_routingconfig_default → src_config_mod_rs_default_ternary_search_iterations (calls)
- src_config_mod_rs_routingconfig_default → src_config_mod_rs_default_enumeration_max_paths (calls)
- src_config_mod_rs_routingconfig_default → src_config_mod_rs_default_cycle_finder (calls)

