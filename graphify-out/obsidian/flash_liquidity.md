---
id: src_services_execution_flash_liquidity_rs
type: File
source: ./src/services/execution/flash_liquidity.rs
community: 3
community_label: TokenFlashLiquidity
---

## Connections

- [[std__collections__HashMap_5]] (imports)
- [[std__time___Duration_ Instant__2]] (imports)
- [[alloy__network__Ethereum_2]] (imports)
- [[alloy__primitives__Address_13]] (imports)
- [[alloy__providers__Provider_2]] (imports)
- [[alloy__sol_types__SolCall_10]] (imports)
- [[parking_lot__RwLock_2]] (imports)
- [[ruint__aliases__U256 as RU256_0]] (imports)
- [[rustc_hash__FxHashMap_3]] (imports)
- [[tracing__debug_0]] (imports)
- [[crate__abis___IAaveV3Pool_ IERC20Metadata_]] (imports)
- [[crate__core__constants___AAVE_V3_POOL_ BALANCER_VAULT_]] (imports)
- [[crate__core__types____    EvaluatedRoute_ FlashLoanSource_ FoundCycle_ PoolState_ ProfitAssessment_ ProtocolType__    TokenIndex___]] (imports)
- [[crate__pipeline__arena__StateArena_14]] (imports)
- [[crate__pipeline__local_sim__simulate_route_detailed_0]] (imports)
- [[crate__pipeline__multicall___MulticallItem_ encode_call_ execute_multicall__0]] (imports)
- [[crate__pipeline__ternary__optimize_cycle_0]] (imports)
- [[crate__services__execution__profit____    ProfitEvalContext_ ProfitThresholds_ RouteProfitParams_ assess_profit_ build_assess_input____0]] (imports)
- [[crate__services__execution__flash_policy__FlashLoanPolicy]] (imports)
- [[FlashPlanAction]] (defines)
- [[FlashPlan]] (defines)
- [[TokenFlashLiquidity]] (defines)
- [[CachedLiquidity]] (defines)
- [[FlashLiquidityCache]] (defines)
- [[decode_balance__]] (defines)
- [[plan_flash_loan__]] (defines)
- [[plan_auto__]] (defines)
- [[plan_single__]] (defines)
- [[route_balancer_flash_capacity__]] (defines)
- [[PrepareDispatchInput]] (defines)
- [[PreparedDispatch]] (defines)
- [[prepare_evaluated_route__]] (defines)
- [[reoptimize_capped__]] (defines)
- [[reassess_route__]] (defines)
- [[collect_flash_tokens__]] (defines)
- [[super____27]] (imports)
- [[liq__]] (defines)
- [[auto_prefers_balancer_when_sufficient__]] (defines)
- [[auto_falls_back_to_aave__]] (defines)
- [[auto_caps_when_neither_sufficient__]] (defines)
- [[auto_rejects_when_no_liquidity__]] (defines)
- [[balancer_only_caps_partial__]] (defines)
- [[balancer_only_rejects_when_vault_and_route_cap_zero__]] (defines)
