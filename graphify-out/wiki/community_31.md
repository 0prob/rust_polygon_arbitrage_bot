# Community 31: V3SwapResult

**Members:** 17

## Nodes

- **uniswap_v3** (`src_core_math_uniswap_v3_rs`, File, degree: 16)
- **default_no_tick_step()** (`src_core_math_uniswap_v3_rs_default_no_tick_step`, Function, degree: 2)
- **crate::core::constants::FEE_PIPS_SCALE** (`src_core_math_uniswap_v3_rs_import_crate_core_constants_fee_pips_scale`, Module, degree: 1)
- **crate::core::math::tick_math::get_sqrt_ratio_at_tick** (`src_core_math_uniswap_v3_rs_import_crate_core_math_tick_math_get_sqrt_ratio_at_tick`, Module, degree: 1)
- **crate::core::types::{V3PoolState, V3Tick}** (`src_core_math_uniswap_v3_rs_import_crate_core_types_v3poolstate_v3tick`, Module, degree: 1)
- **ruint::aliases::U256** (`src_core_math_uniswap_v3_rs_import_ruint_aliases_u256`, Module, degree: 1)
- **super::*** (`src_core_math_uniswap_v3_rs_import_super`, Module, degree: 1)
- **super::swap_math::compute_swap_step** (`src_core_math_uniswap_v3_rs_import_super_swap_math_compute_swap_step`, Module, degree: 1)
- **super::tick_math::{
    MAX_SQRT_RATIO, MAX_TICK, MIN_SQRT_RATIO, MIN_TICK, get_sqrt_ratio_at_tick,
    get_tick_at_sqrt_ratio_in_range,
}** (`src_core_math_uniswap_v3_rs_import_super_tick_math_max_sqrt_ratio_max_tick_min_sqrt_ratio_min_tick_get_sqrt_ratio_at_tick_get_tick_at_sqrt_ratio_in_range`, Module, degree: 1)
- **next_initialized_tick()** (`src_core_math_uniswap_v3_rs_next_initialized_tick`, Function, degree: 3)
- **next_initialized_tick_handles_boundaries()** (`src_core_math_uniswap_v3_rs_next_initialized_tick_handles_boundaries`, Function, degree: 2)
- **resolve_v3_fee_pips()** (`src_core_math_uniswap_v3_rs_resolve_v3_fee_pips`, Function, degree: 2)
- **simulate_v3_swap()** (`src_core_math_uniswap_v3_rs_simulate_v3_swap`, Function, degree: 7)
- **single_tick_swap_produces_output()** (`src_core_math_uniswap_v3_rs_single_tick_swap_produces_output`, Function, degree: 2)
- **sorted_tick_indices()** (`src_core_math_uniswap_v3_rs_sorted_tick_indices`, Function, degree: 2)
- **tick_liquidity_net()** (`src_core_math_uniswap_v3_rs_tick_liquidity_net`, Function, degree: 2)
- **V3SwapResult** (`src_core_math_uniswap_v3_rs_v3swapresult`, Struct, degree: 1)

## Relationships

- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_import_ruint_aliases_u256 (imports)
- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_import_crate_core_constants_fee_pips_scale (imports)
- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_import_crate_core_types_v3poolstate_v3tick (imports)
- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_import_super_swap_math_compute_swap_step (imports)
- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_import_super_tick_math_max_sqrt_ratio_max_tick_min_sqrt_ratio_min_tick_get_sqrt_ratio_at_tick_get_tick_at_sqrt_ratio_in_range (imports)
- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_v3swapresult (defines)
- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_next_initialized_tick (defines)
- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_sorted_tick_indices (defines)
- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_tick_liquidity_net (defines)
- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_default_no_tick_step (defines)
- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_resolve_v3_fee_pips (defines)
- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_simulate_v3_swap (defines)
- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_import_super (imports)
- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_import_crate_core_math_tick_math_get_sqrt_ratio_at_tick (imports)
- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_single_tick_swap_produces_output (defines)
- src_core_math_uniswap_v3_rs → src_core_math_uniswap_v3_rs_next_initialized_tick_handles_boundaries (defines)
- src_core_math_uniswap_v3_rs_simulate_v3_swap → src_core_math_uniswap_v3_rs_resolve_v3_fee_pips (calls)
- src_core_math_uniswap_v3_rs_simulate_v3_swap → src_core_math_uniswap_v3_rs_sorted_tick_indices (calls)
- src_core_math_uniswap_v3_rs_simulate_v3_swap → src_core_math_uniswap_v3_rs_next_initialized_tick (calls)
- src_core_math_uniswap_v3_rs_simulate_v3_swap → src_core_math_uniswap_v3_rs_default_no_tick_step (calls)
- src_core_math_uniswap_v3_rs_simulate_v3_swap → src_core_math_uniswap_v3_rs_tick_liquidity_net (calls)
- src_core_math_uniswap_v3_rs_single_tick_swap_produces_output → src_core_math_uniswap_v3_rs_simulate_v3_swap (calls)
- src_core_math_uniswap_v3_rs_next_initialized_tick_handles_boundaries → src_core_math_uniswap_v3_rs_next_initialized_tick (calls)

