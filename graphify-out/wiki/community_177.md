# Community 177: liq()

**Members:** 5

## Nodes

- **auto_caps_when_neither_sufficient()** (`src_services_execution_flash_liquidity_rs_auto_caps_when_neither_sufficient`, Function, degree: 3)
- **auto_falls_back_to_aave()** (`src_services_execution_flash_liquidity_rs_auto_falls_back_to_aave`, Function, degree: 3)
- **auto_rejects_when_no_liquidity()** (`src_services_execution_flash_liquidity_rs_auto_rejects_when_no_liquidity`, Function, degree: 3)
- **balancer_only_caps_partial()** (`src_services_execution_flash_liquidity_rs_balancer_only_caps_partial`, Function, degree: 3)
- **liq()** (`src_services_execution_flash_liquidity_rs_liq`, Function, degree: 7)

## Relationships

- src_services_execution_flash_liquidity_rs_auto_falls_back_to_aave → src_services_execution_flash_liquidity_rs_liq (calls)
- src_services_execution_flash_liquidity_rs_auto_caps_when_neither_sufficient → src_services_execution_flash_liquidity_rs_liq (calls)
- src_services_execution_flash_liquidity_rs_auto_rejects_when_no_liquidity → src_services_execution_flash_liquidity_rs_liq (calls)
- src_services_execution_flash_liquidity_rs_balancer_only_caps_partial → src_services_execution_flash_liquidity_rs_liq (calls)

