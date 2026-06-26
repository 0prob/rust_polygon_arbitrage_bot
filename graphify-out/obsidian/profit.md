---
id: src_services_execution_profit_rs
type: File
source: ./src/services/execution/profit.rs
community: 12
community_label: slippage_adjusted()
---

## Connections

- [[std__collections__HashMap_4]] (imports)
- [[alloy__primitives__Address_9]] (imports)
- [[ruint__aliases__U256_22]] (imports)
- [[rustc_hash__FxHashMap_2]] (imports)
- [[tracing__instrument_1]] (imports)
- [[crate__core__constants__BPS_SCALE_0]] (imports)
- [[crate__core__types___FlashLoanSource_ ProfitAssessment_ TokenIndex_]] (imports)
- [[crate__pipeline__arena__StateArena_3]] (imports)
- [[crate__pipeline__types__MinimalSimResult_0]] (imports)
- [[crate__services__oracle__resolve_token_to_matic_rate]] (imports)
- [[pub use crate__core__constants___MIN_TOKEN_TO_MATIC_RATE_ RATE_PRECISION_]] (imports)
- [[flash_loan_fee_bps__]] (defines)
- [[on_chain_min_profit__]] (defines)
- [[on_chain_min_profit_for_route__]] (defines)
- [[slippage_adjusted__]] (defines)
- [[AssessProfitInput]] (defines)
- [[ProfitEvalContext]] (defines)
- [[RouteProfitParams]] (defines)
- [[ProfitThresholds]] (defines)
- [[build_assess_input__]] (defines)
- [[net_profit_after_gas_from_sim__]] (defines)
- [[assess_profit__]] (defines)
- [[super____14]] (imports)
- [[default_input__]] (defines)
- [[on_chain_min_profit_95_percent__]] (defines)
- [[non_18_decimal_token_gas_cost_is_accurate__]] (defines)
- [[rate_failure_rejects_trade__]] (defines)
- [[safety_multiplier_override_works__]] (defines)
- [[zero_gas_means_no_revert_penalty__]] (defines)
- [[slippage_applied_to_gross_profit_not_amount_in__]] (defines)
- [[roi_threshold_rejects_low_margin__]] (defines)
