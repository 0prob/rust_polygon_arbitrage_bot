# Community 99: simulate_woofi_swap()

**Members:** 10

## Nodes

- **woofi** (`src_core_math_woofi_rs`, File, degree: 9)
- **apply_woofi_fee()** (`src_core_math_woofi_rs_apply_woofi_fee`, Function, degree: 2)
- **calc_base_amount_sell_quote()** (`src_core_math_woofi_rs_calc_base_amount_sell_quote`, Function, degree: 3)
- **calc_quote_amount_sell_base()** (`src_core_math_woofi_rs_calc_quote_amount_sell_base`, Function, degree: 3)
- **get_woofi_amount_out()** (`src_core_math_woofi_rs_get_woofi_amount_out`, Function, degree: 5)
- **has_positive_swap_factor()** (`src_core_math_woofi_rs_has_positive_swap_factor`, Function, degree: 3)
- **crate::core::types::{WoofiBaseTokenState, WoofiPoolState}** (`src_core_math_woofi_rs_import_crate_core_types_woofibasetokenstate_woofipoolstate`, Module, degree: 1)
- **ruint::aliases::U256** (`src_core_math_woofi_rs_import_ruint_aliases_u256`, Module, degree: 1)
- **super::fixed_point::ONE** (`src_core_math_woofi_rs_import_super_fixed_point_one`, Module, degree: 1)
- **simulate_woofi_swap()** (`src_core_math_woofi_rs_simulate_woofi_swap`, Function, degree: 2)

## Relationships

- src_core_math_woofi_rs → src_core_math_woofi_rs_import_ruint_aliases_u256 (imports)
- src_core_math_woofi_rs → src_core_math_woofi_rs_import_crate_core_types_woofibasetokenstate_woofipoolstate (imports)
- src_core_math_woofi_rs → src_core_math_woofi_rs_import_super_fixed_point_one (imports)
- src_core_math_woofi_rs → src_core_math_woofi_rs_has_positive_swap_factor (defines)
- src_core_math_woofi_rs → src_core_math_woofi_rs_calc_quote_amount_sell_base (defines)
- src_core_math_woofi_rs → src_core_math_woofi_rs_calc_base_amount_sell_quote (defines)
- src_core_math_woofi_rs → src_core_math_woofi_rs_apply_woofi_fee (defines)
- src_core_math_woofi_rs → src_core_math_woofi_rs_get_woofi_amount_out (defines)
- src_core_math_woofi_rs → src_core_math_woofi_rs_simulate_woofi_swap (defines)
- src_core_math_woofi_rs_calc_quote_amount_sell_base → src_core_math_woofi_rs_has_positive_swap_factor (calls)
- src_core_math_woofi_rs_calc_base_amount_sell_quote → src_core_math_woofi_rs_has_positive_swap_factor (calls)
- src_core_math_woofi_rs_get_woofi_amount_out → src_core_math_woofi_rs_apply_woofi_fee (calls)
- src_core_math_woofi_rs_get_woofi_amount_out → src_core_math_woofi_rs_calc_quote_amount_sell_base (calls)
- src_core_math_woofi_rs_get_woofi_amount_out → src_core_math_woofi_rs_calc_base_amount_sell_quote (calls)
- src_core_math_woofi_rs_simulate_woofi_swap → src_core_math_woofi_rs_get_woofi_amount_out (calls)

