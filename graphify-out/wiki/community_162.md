# Community 162: v2_state()

**Members:** 6

## Nodes

- **bench_compute_spot_all_edges()** (`benches_spot_price_rs_bench_compute_spot_all_edges`, Function, degree: 2)
- **bench_hf_rescore_cap()** (`benches_spot_price_rs_bench_hf_rescore_cap`, Function, degree: 2)
- **bench_rescore_cycles()** (`benches_spot_price_rs_bench_rescore_cycles`, Function, degree: 2)
- **build_fixture()** (`benches_spot_price_rs_build_fixture`, Function, degree: 6)
- **reserve()** (`benches_spot_price_rs_reserve`, Function, degree: 2)
- **v2_state()** (`benches_spot_price_rs_v2_state`, Function, degree: 2)

## Relationships

- benches_spot_price_rs_build_fixture → benches_spot_price_rs_v2_state (calls)
- benches_spot_price_rs_build_fixture → benches_spot_price_rs_reserve (calls)
- benches_spot_price_rs_bench_compute_spot_all_edges → benches_spot_price_rs_build_fixture (calls)
- benches_spot_price_rs_bench_rescore_cycles → benches_spot_price_rs_build_fixture (calls)
- benches_spot_price_rs_bench_hf_rescore_cap → benches_spot_price_rs_build_fixture (calls)

