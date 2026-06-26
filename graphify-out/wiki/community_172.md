# Community 172: profile_routing()

**Members:** 5

## Nodes

- **build_ring()** (`src_bin_flame_profile_rs_build_ring`, Function, degree: 4)
- **main()** (`src_bin_flame_profile_rs_main`, Function, degree: 4)
- **profile_math()** (`src_bin_flame_profile_rs_profile_math`, Function, degree: 3)
- **profile_price()** (`src_bin_flame_profile_rs_profile_price`, Function, degree: 3)
- **profile_routing()** (`src_bin_flame_profile_rs_profile_routing`, Function, degree: 3)

## Relationships

- src_bin_flame_profile_rs_profile_routing → src_bin_flame_profile_rs_build_ring (calls)
- src_bin_flame_profile_rs_profile_price → src_bin_flame_profile_rs_build_ring (calls)
- src_bin_flame_profile_rs_profile_math → src_bin_flame_profile_rs_build_ring (calls)
- src_bin_flame_profile_rs_main → src_bin_flame_profile_rs_profile_routing (calls)
- src_bin_flame_profile_rs_main → src_bin_flame_profile_rs_profile_price (calls)
- src_bin_flame_profile_rs_main → src_bin_flame_profile_rs_profile_math (calls)

