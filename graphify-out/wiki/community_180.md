# Community 180: slippage_applied_to_gross_profit_not_amount_in()

**Members:** 5

## Nodes

- **assess_profit()** (`src_services_execution_profit_rs_assess_profit`, Function, degree: 9)
- **flash_loan_fee_bps()** (`src_services_execution_profit_rs_flash_loan_fee_bps`, Function, degree: 2)
- **net_profit_after_gas_from_sim()** (`src_services_execution_profit_rs_net_profit_after_gas_from_sim`, Function, degree: 2)
- **roi_threshold_rejects_low_margin()** (`src_services_execution_profit_rs_roi_threshold_rejects_low_margin`, Function, degree: 2)
- **slippage_applied_to_gross_profit_not_amount_in()** (`src_services_execution_profit_rs_slippage_applied_to_gross_profit_not_amount_in`, Function, degree: 2)

## Relationships

- src_services_execution_profit_rs_net_profit_after_gas_from_sim → src_services_execution_profit_rs_assess_profit (calls)
- src_services_execution_profit_rs_assess_profit → src_services_execution_profit_rs_flash_loan_fee_bps (calls)
- src_services_execution_profit_rs_slippage_applied_to_gross_profit_not_amount_in → src_services_execution_profit_rs_assess_profit (calls)
- src_services_execution_profit_rs_roi_threshold_rejects_low_margin → src_services_execution_profit_rs_assess_profit (calls)

