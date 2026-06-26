# Community 178: preserves_balancer_pool_id_from_row()

**Members:** 5

## Nodes

- **accepts_32_byte_v4_pool_id()** (`src_services_discovery_rs_accepts_32_byte_v4_pool_id`, Function, degree: 2)
- **accepts_v4_when_hooks_field_missing_assumes_hookless()** (`src_services_discovery_rs_accepts_v4_when_hooks_field_missing_assumes_hookless`, Function, degree: 2)
- **drops_v4_hook_pools()** (`src_services_discovery_rs_drops_v4_hook_pools`, Function, degree: 2)
- **parse_pool_meta_row()** (`src_services_discovery_rs_parse_pool_meta_row`, Function, degree: 7)
- **preserves_balancer_pool_id_from_row()** (`src_services_discovery_rs_preserves_balancer_pool_id_from_row`, Function, degree: 2)

## Relationships

- src_services_discovery_rs_accepts_v4_when_hooks_field_missing_assumes_hookless → src_services_discovery_rs_parse_pool_meta_row (calls)
- src_services_discovery_rs_accepts_32_byte_v4_pool_id → src_services_discovery_rs_parse_pool_meta_row (calls)
- src_services_discovery_rs_drops_v4_hook_pools → src_services_discovery_rs_parse_pool_meta_row (calls)
- src_services_discovery_rs_preserves_balancer_pool_id_from_row → src_services_discovery_rs_parse_pool_meta_row (calls)

