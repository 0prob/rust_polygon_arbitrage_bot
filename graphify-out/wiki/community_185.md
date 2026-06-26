# Community 185: plan_single()

**Members:** 5

## Nodes

- **auto_prefers_balancer_when_sufficient()** (`src_services_execution_flash_liquidity_rs_auto_prefers_balancer_when_sufficient`, Function, degree: 3)
- **balancer_only_rejects_when_vault_and_route_cap_zero()** (`src_services_execution_flash_liquidity_rs_balancer_only_rejects_when_vault_and_route_cap_zero`, Function, degree: 3)
- **plan_auto()** (`src_services_execution_flash_liquidity_rs_plan_auto`, Function, degree: 2)
- **plan_flash_loan()** (`src_services_execution_flash_liquidity_rs_plan_flash_loan`, Function, degree: 10)
- **plan_single()** (`src_services_execution_flash_liquidity_rs_plan_single`, Function, degree: 2)

## Relationships

- src_services_execution_flash_liquidity_rs_plan_flash_loan → src_services_execution_flash_liquidity_rs_plan_auto (calls)
- src_services_execution_flash_liquidity_rs_plan_flash_loan → src_services_execution_flash_liquidity_rs_plan_single (calls)
- src_services_execution_flash_liquidity_rs_auto_prefers_balancer_when_sufficient → src_services_execution_flash_liquidity_rs_plan_flash_loan (calls)
- src_services_execution_flash_liquidity_rs_balancer_only_rejects_when_vault_and_route_cap_zero → src_services_execution_flash_liquidity_rs_plan_flash_loan (calls)

