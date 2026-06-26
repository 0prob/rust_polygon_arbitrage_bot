# Community 112: get_next_sqrt_price_from_output()

**Members:** 9

## Nodes

- **sqrt_price_math** (`src_core_math_sqrt_price_math_rs`, File, degree: 8)
- **get_amount0_delta()** (`src_core_math_sqrt_price_math_rs_get_amount0_delta`, Function, degree: 1)
- **get_amount1_delta()** (`src_core_math_sqrt_price_math_rs_get_amount1_delta`, Function, degree: 1)
- **get_next_sqrt_price_from_amount0_rounding_up()** (`src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_amount0_rounding_up`, Function, degree: 3)
- **get_next_sqrt_price_from_amount1_rounding_down()** (`src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_amount1_rounding_down`, Function, degree: 3)
- **get_next_sqrt_price_from_input()** (`src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_input`, Function, degree: 3)
- **get_next_sqrt_price_from_output()** (`src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_output`, Function, degree: 3)
- **ruint::aliases::U256** (`src_core_math_sqrt_price_math_rs_import_ruint_aliases_u256`, Module, degree: 1)
- **super::full_math::{div_rounding_up, mul_div, mul_div_rounding_up}** (`src_core_math_sqrt_price_math_rs_import_super_full_math_div_rounding_up_mul_div_mul_div_rounding_up`, Module, degree: 1)

## Relationships

- src_core_math_sqrt_price_math_rs → src_core_math_sqrt_price_math_rs_import_ruint_aliases_u256 (imports)
- src_core_math_sqrt_price_math_rs → src_core_math_sqrt_price_math_rs_import_super_full_math_div_rounding_up_mul_div_mul_div_rounding_up (imports)
- src_core_math_sqrt_price_math_rs → src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_amount0_rounding_up (defines)
- src_core_math_sqrt_price_math_rs → src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_amount1_rounding_down (defines)
- src_core_math_sqrt_price_math_rs → src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_input (defines)
- src_core_math_sqrt_price_math_rs → src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_output (defines)
- src_core_math_sqrt_price_math_rs → src_core_math_sqrt_price_math_rs_get_amount0_delta (defines)
- src_core_math_sqrt_price_math_rs → src_core_math_sqrt_price_math_rs_get_amount1_delta (defines)
- src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_input → src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_amount0_rounding_up (calls)
- src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_input → src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_amount1_rounding_down (calls)
- src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_output → src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_amount1_rounding_down (calls)
- src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_output → src_core_math_sqrt_price_math_rs_get_next_sqrt_price_from_amount0_rounding_up (calls)

