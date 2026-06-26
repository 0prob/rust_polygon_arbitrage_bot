---
id: src_services_execution_submit_rs
type: File
source: ./src/services/execution/submit.rs
community: 15
community_label: SubmitFees
---

## Connections

- [[alloy__network__Ethereum_5]] (imports)
- [[alloy__primitives___B256_ U256_]] (imports)
- [[alloy__providers__Provider_5]] (imports)
- [[alloy__rpc__types__TransactionRequest_1]] (imports)
- [[anyhow___Result_ anyhow__1]] (imports)
- [[tracing___info_ instrument_ warn_]] (imports)
- [[super__candidate__CandidateExecution]] (imports)
- [[super__gas___default_priority_fee_wei_ u256_to_u128_]] (imports)
- [[super__gas_oracle__GasOracle]] (imports)
- [[super__nonce__NonceManager_1]] (imports)
- [[super__rpc_errors___SubmitAction_ classify_submit_error_ extract_tx_hash_from_error_]] (imports)
- [[SubmitFees]] (defines)
- [[bump_fees__]] (defines)
- [[resolve_submit_fees__]] (defines)
- [[resolve_submit_fees_with_profit__]] (defines)
- [[build_transaction_request__]] (defines)
- [[submit_live_candidate__]] (defines)
- [[submit_with_recovery__]] (defines)
- [[super____33]] (imports)
- [[crate__services__execution__gas_oracle__GasOracle_0]] (imports)
- [[profit_boost_increases_priority_fee__]] (defines)
