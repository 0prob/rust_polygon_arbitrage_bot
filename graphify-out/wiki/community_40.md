# Community 40: token_addrs_csv()

**Members:** 16

## Nodes

- **tracing_util** (`src_infra_tracing_util_rs`, File, degree: 15)
- **alloy::primitives::Address** (`src_infra_tracing_util_rs_import_alloy_primitives_address`, Module, degree: 1)
- **crate::core::types::{Edge, EvaluatedRoute, FoundCycle}** (`src_infra_tracing_util_rs_import_crate_core_types_edge_evaluatedroute_foundcycle`, Module, degree: 1)
- **crate::pipeline::arena::StateArena** (`src_infra_tracing_util_rs_import_crate_pipeline_arena_statearena`, Module, degree: 1)
- **crate::pipeline::types::route_fingerprint** (`src_infra_tracing_util_rs_import_crate_pipeline_types_route_fingerprint`, Module, degree: 1)
- **crate::services::execution::candidate::CandidateExecution** (`src_infra_tracing_util_rs_import_crate_services_execution_candidate_candidateexecution`, Module, degree: 1)
- **tracing::Span** (`src_infra_tracing_util_rs_import_tracing_span`, Module, degree: 1)
- **pool_addrs_csv()** (`src_infra_tracing_util_rs_pool_addrs_csv`, Function, degree: 2)
- **record_candidate()** (`src_infra_tracing_util_rs_record_candidate`, Function, degree: 1)
- **record_cycle_route()** (`src_infra_tracing_util_rs_record_cycle_route`, Function, degree: 4)
- **record_evaluated_route()** (`src_infra_tracing_util_rs_record_evaluated_route`, Function, degree: 2)
- **record_gas_fees()** (`src_infra_tracing_util_rs_record_gas_fees`, Function, degree: 1)
- **record_receipt()** (`src_infra_tracing_util_rs_record_receipt`, Function, degree: 1)
- **record_tx()** (`src_infra_tracing_util_rs_record_tx`, Function, degree: 1)
- **start_token_addr()** (`src_infra_tracing_util_rs_start_token_addr`, Function, degree: 1)
- **token_addrs_csv()** (`src_infra_tracing_util_rs_token_addrs_csv`, Function, degree: 2)

## Relationships

- src_infra_tracing_util_rs → src_infra_tracing_util_rs_import_alloy_primitives_address (imports)
- src_infra_tracing_util_rs → src_infra_tracing_util_rs_import_tracing_span (imports)
- src_infra_tracing_util_rs → src_infra_tracing_util_rs_import_crate_core_types_edge_evaluatedroute_foundcycle (imports)
- src_infra_tracing_util_rs → src_infra_tracing_util_rs_import_crate_pipeline_arena_statearena (imports)
- src_infra_tracing_util_rs → src_infra_tracing_util_rs_import_crate_pipeline_types_route_fingerprint (imports)
- src_infra_tracing_util_rs → src_infra_tracing_util_rs_import_crate_services_execution_candidate_candidateexecution (imports)
- src_infra_tracing_util_rs → src_infra_tracing_util_rs_pool_addrs_csv (defines)
- src_infra_tracing_util_rs → src_infra_tracing_util_rs_token_addrs_csv (defines)
- src_infra_tracing_util_rs → src_infra_tracing_util_rs_record_cycle_route (defines)
- src_infra_tracing_util_rs → src_infra_tracing_util_rs_record_evaluated_route (defines)
- src_infra_tracing_util_rs → src_infra_tracing_util_rs_record_candidate (defines)
- src_infra_tracing_util_rs → src_infra_tracing_util_rs_record_gas_fees (defines)
- src_infra_tracing_util_rs → src_infra_tracing_util_rs_record_tx (defines)
- src_infra_tracing_util_rs → src_infra_tracing_util_rs_record_receipt (defines)
- src_infra_tracing_util_rs → src_infra_tracing_util_rs_start_token_addr (defines)
- src_infra_tracing_util_rs_record_cycle_route → src_infra_tracing_util_rs_pool_addrs_csv (calls)
- src_infra_tracing_util_rs_record_cycle_route → src_infra_tracing_util_rs_token_addrs_csv (calls)
- src_infra_tracing_util_rs_record_evaluated_route → src_infra_tracing_util_rs_record_cycle_route (calls)

