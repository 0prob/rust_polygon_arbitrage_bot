# Community 116: push_call()

**Members:** 9

## Nodes

- **build_balancer_plan()** (`src_pipeline_pool_fetch_plans_rs_build_balancer_plan`, Function, degree: 3)
- **build_curve_plan()** (`src_pipeline_pool_fetch_plans_rs_build_curve_plan`, Function, degree: 4)
- **build_dodo_plan()** (`src_pipeline_pool_fetch_plans_rs_build_dodo_plan`, Function, degree: 3)
- **build_plan()** (`src_pipeline_pool_fetch_plans_rs_build_plan`, Function, degree: 8)
- **build_v2_plan()** (`src_pipeline_pool_fetch_plans_rs_build_v2_plan`, Function, degree: 3)
- **build_v3_plan()** (`src_pipeline_pool_fetch_plans_rs_build_v3_plan`, Function, degree: 3)
- **build_v4_plan()** (`src_pipeline_pool_fetch_plans_rs_build_v4_plan`, Function, degree: 3)
- **.from()** (`src_pipeline_pool_fetch_plans_rs_fetchpoolinfo_from`, Method, degree: 3)
- **push_call()** (`src_pipeline_pool_fetch_plans_rs_push_call`, Function, degree: 7)

## Relationships

- src_pipeline_pool_fetch_plans_rs_build_v2_plan → src_pipeline_pool_fetch_plans_rs_push_call (calls)
- src_pipeline_pool_fetch_plans_rs_build_v3_plan → src_pipeline_pool_fetch_plans_rs_push_call (calls)
- src_pipeline_pool_fetch_plans_rs_build_v4_plan → src_pipeline_pool_fetch_plans_rs_push_call (calls)
- src_pipeline_pool_fetch_plans_rs_build_dodo_plan → src_pipeline_pool_fetch_plans_rs_push_call (calls)
- src_pipeline_pool_fetch_plans_rs_build_curve_plan → src_pipeline_pool_fetch_plans_rs_push_call (calls)
- src_pipeline_pool_fetch_plans_rs_build_curve_plan → src_pipeline_pool_fetch_plans_rs_fetchpoolinfo_from (calls)
- src_pipeline_pool_fetch_plans_rs_build_balancer_plan → src_pipeline_pool_fetch_plans_rs_push_call (calls)
- src_pipeline_pool_fetch_plans_rs_build_plan → src_pipeline_pool_fetch_plans_rs_fetchpoolinfo_from (calls)
- src_pipeline_pool_fetch_plans_rs_build_plan → src_pipeline_pool_fetch_plans_rs_build_v2_plan (calls)
- src_pipeline_pool_fetch_plans_rs_build_plan → src_pipeline_pool_fetch_plans_rs_build_v3_plan (calls)
- src_pipeline_pool_fetch_plans_rs_build_plan → src_pipeline_pool_fetch_plans_rs_build_v4_plan (calls)
- src_pipeline_pool_fetch_plans_rs_build_plan → src_pipeline_pool_fetch_plans_rs_build_dodo_plan (calls)
- src_pipeline_pool_fetch_plans_rs_build_plan → src_pipeline_pool_fetch_plans_rs_build_curve_plan (calls)
- src_pipeline_pool_fetch_plans_rs_build_plan → src_pipeline_pool_fetch_plans_rs_build_balancer_plan (calls)

