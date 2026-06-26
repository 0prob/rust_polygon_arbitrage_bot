# plan_flash_loan()

- **ID:** `src_services_execution_flash_liquidity_rs_plan_flash_loan`
- **Type:** Function
- **File:** `./src/services/execution/flash_liquidity.rs`
- **Location:** L182
- **Community:** 185 (plan_single())

## Relationships

- src_services_execution_flash_liquidity_rs → src_services_execution_flash_liquidity_rs_plan_flash_loan (defines, Extracted)
- src_services_execution_flash_liquidity_rs_plan_flash_loan → src_services_execution_flash_liquidity_rs_plan_auto (calls, Inferred)
- src_services_execution_flash_liquidity_rs_plan_flash_loan → src_services_execution_flash_liquidity_rs_plan_single (calls, Inferred)
- src_services_execution_flash_liquidity_rs_prepare_evaluated_route → src_services_execution_flash_liquidity_rs_plan_flash_loan (calls, Inferred)
- src_services_execution_flash_liquidity_rs_auto_prefers_balancer_when_sufficient → src_services_execution_flash_liquidity_rs_plan_flash_loan (calls, Inferred)
- src_services_execution_flash_liquidity_rs_auto_falls_back_to_aave → src_services_execution_flash_liquidity_rs_plan_flash_loan (calls, Inferred)
- src_services_execution_flash_liquidity_rs_auto_caps_when_neither_sufficient → src_services_execution_flash_liquidity_rs_plan_flash_loan (calls, Inferred)
- src_services_execution_flash_liquidity_rs_auto_rejects_when_no_liquidity → src_services_execution_flash_liquidity_rs_plan_flash_loan (calls, Inferred)
- src_services_execution_flash_liquidity_rs_balancer_only_caps_partial → src_services_execution_flash_liquidity_rs_plan_flash_loan (calls, Inferred)
- src_services_execution_flash_liquidity_rs_balancer_only_rejects_when_vault_and_route_cap_zero → src_services_execution_flash_liquidity_rs_plan_flash_loan (calls, Inferred)

