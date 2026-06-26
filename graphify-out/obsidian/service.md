---
id: src_services_execution_service_rs
type: File
source: ./src/services/execution/service.rs
community: 4
community_label: ExecutionOutcome
---

## Connections

- [[std__collections__HashMap_6]] (imports)
- [[std__sync__Arc_6]] (imports)
- [[parking_lot__RwLock_4]] (imports)
- [[std__time___Duration_ Instant__4]] (imports)
- [[alloy__network__Ethereum_7]] (imports)
- [[alloy__primitives___Address_ U256__7]] (imports)
- [[alloy__providers__Provider_7]] (imports)
- [[tokio__sync__watch_3]] (imports)
- [[tracing___Instrument_ info_ info_span_ instrument_ warn_]] (imports)
- [[crate__config__AppConfig_2]] (imports)
- [[crate__config__WalletSecrets_0]] (imports)
- [[crate__infra__hypersync__HyperSyncService_1]] (imports)
- [[crate__infra__metrics__PipelineMetrics_0]] (imports)
- [[crate__infra__rpc__RpcPool_1]] (imports)
- [[crate__infra__tracing_util___record_candidate_ record_gas_fees_ record_receipt_ record_tx_]] (imports)
- [[crate__services__execution__candidate__CandidateExecution_1]] (imports)
- [[crate__services__execution__circuit_breaker__CircuitBreaker]] (imports)
- [[crate__services__execution__dryrun__dry_run_candidate]] (imports)
- [[crate__services__execution__flash_liquidity__FlashLiquidityCache]] (imports)
- [[crate__services__execution__gas___gas_drift_bps_ pick_live_gas_limit_]] (imports)
- [[crate__services__execution__gas_oracle__GasOracle_1]] (imports)
- [[crate__services__execution__nonce__NonceManager]] (imports)
- [[crate__services__execution__opportunity_log__log_opportunity_outcome]] (imports)
- [[crate__services__execution__profit___AssessProfitInput_ assess_profit_]] (imports)
- [[crate__services__execution__profit_logs__parse_transfer_profit]] (imports)
- [[crate__services__execution__receipt__ReceiptPoller]] (imports)
- [[crate__services__execution__recovery___NonceRecoveryOutcome_ recover_after_receipt_timeout_]] (imports)
- [[crate__services__execution__rpc_errors___SubmitAction_ classify_submit_error_]] (imports)
- [[crate__services__execution__submit____    resolve_submit_fees_with_profit_ submit_with_recovery___]] (imports)
- [[ExecutionService]] (defines)
- [[ExecutionOutcome]] (defines)
- [[min_operator_balance_wei__]] (defines)
