# Community 187: u128_fast_path_matches_u256_for_realistic_reserves()

**Members:** 5

## Nodes

- **constant_product_swap()** (`src_core_math_uniswap_v2_rs_constant_product_swap`, Function, degree: 3)
- **get_amount_out()** (`src_core_math_uniswap_v2_rs_get_amount_out`, Function, degree: 7)
- **get_amount_out_u128()** (`src_core_math_uniswap_v2_rs_get_amount_out_u128`, Function, degree: 2)
- **simulate_v2_swap()** (`src_core_math_uniswap_v2_rs_simulate_v2_swap`, Function, degree: 5)
- **u128_fast_path_matches_u256_for_realistic_reserves()** (`src_core_math_uniswap_v2_rs_u128_fast_path_matches_u256_for_realistic_reserves`, Function, degree: 4)

## Relationships

- src_core_math_uniswap_v2_rs_get_amount_out → src_core_math_uniswap_v2_rs_get_amount_out_u128 (calls)
- src_core_math_uniswap_v2_rs_simulate_v2_swap → src_core_math_uniswap_v2_rs_get_amount_out (calls)
- src_core_math_uniswap_v2_rs_constant_product_swap → src_core_math_uniswap_v2_rs_simulate_v2_swap (calls)
- src_core_math_uniswap_v2_rs_constant_product_swap → src_core_math_uniswap_v2_rs_get_amount_out (calls)
- src_core_math_uniswap_v2_rs_u128_fast_path_matches_u256_for_realistic_reserves → src_core_math_uniswap_v2_rs_simulate_v2_swap (calls)
- src_core_math_uniswap_v2_rs_u128_fast_path_matches_u256_for_realistic_reserves → src_core_math_uniswap_v2_rs_get_amount_out (calls)

