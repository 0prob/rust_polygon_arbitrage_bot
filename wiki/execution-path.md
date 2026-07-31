---
created: 2026-07-30
updated: 2026-07-30
sources:
  - raw/README.md
---

# Execution Path

## Modes

| `EXECUTION_MODE` | Behavior |
|------------------|----------|
| `dry-run` (code default) | Full sim path; no on-chain submit |
| `live` | Submit via Huff `ArbExecutor` |

Live needs: `PRIVATE_KEY` or `PRIVATE_KEY_FILE`, `EXECUTOR_ADDRESS`, state + execution RPCs. If `REQUIRE_PRIVATE_SUBMIT=true`, private RPC and/or bloXroute auth required.

## Submit path

1. Encode route calldata for selected [[flash-loans]] entrypoint.
2. Gas oracle + profit-scaled priority fee.
3. Nonce management; route cooldown/quarantine on failures.
4. Send via private URL or bloXroute BDN.
5. Receipt poll (`RECEIPT_TIMEOUT_MS` / `RECEIPT_POLL_MS`).

Executor repo: `0prob/solidity_and_huff_evm_contract`.

## Safety surface

- Circuit breaker / consecutive failure caps
- Daily loss limits (`MAX_DAILY_LOSS_MATIC_WEI`)
- Operator MATIC floor
- Single-instance lock (override `RPBOT_ALLOW_MULTIPLE`)

## Related

- [[hf-pass]]
- [[profit-gate]]
- [[config-env]]
