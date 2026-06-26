# Community 155: pow_down()

**Members:** 7

## Nodes

- **fixed_point** (`src_core_math_fixed_point_rs`, File, degree: 6)
- **complement()** (`src_core_math_fixed_point_rs_complement`, Function, degree: 1)
- **ruint::aliases::U256** (`src_core_math_fixed_point_rs_import_ruint_aliases_u256`, Module, degree: 1)
- **super::log_exp_math::log_exp_pow** (`src_core_math_fixed_point_rs_import_super_log_exp_math_log_exp_pow`, Module, degree: 1)
- **mul_down()** (`src_core_math_fixed_point_rs_mul_down`, Function, degree: 2)
- **mul_up()** (`src_core_math_fixed_point_rs_mul_up`, Function, degree: 2)
- **pow_down()** (`src_core_math_fixed_point_rs_pow_down`, Function, degree: 3)

## Relationships

- src_core_math_fixed_point_rs → src_core_math_fixed_point_rs_import_ruint_aliases_u256 (imports)
- src_core_math_fixed_point_rs → src_core_math_fixed_point_rs_import_super_log_exp_math_log_exp_pow (imports)
- src_core_math_fixed_point_rs → src_core_math_fixed_point_rs_mul_down (defines)
- src_core_math_fixed_point_rs → src_core_math_fixed_point_rs_mul_up (defines)
- src_core_math_fixed_point_rs → src_core_math_fixed_point_rs_complement (defines)
- src_core_math_fixed_point_rs → src_core_math_fixed_point_rs_pow_down (defines)
- src_core_math_fixed_point_rs_pow_down → src_core_math_fixed_point_rs_mul_down (calls)
- src_core_math_fixed_point_rs_pow_down → src_core_math_fixed_point_rs_mul_up (calls)

