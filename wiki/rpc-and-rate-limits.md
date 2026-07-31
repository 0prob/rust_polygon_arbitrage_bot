---
created: 2026-07-30
updated: 2026-07-30
sources:
  - raw/README.md
  - raw/session-2026-07-30-graph-expansion.md
  - raw/env.example.md
---

# RPC and Rate Limits

## Endpoint roles

| Role | Env | Use |
|------|-----|-----|
| State reads | `STATE_RPC_URL` / `POLYGON_RPC_URLS` | Multicall pool state, flash liquidity |
| Execution | `EXECUTION_RPC` | HF `eth_call`, gas, receipts |
| Private submit | `PRIVATE_RPC_URL` | MEV-protected send |
| bloXroute | `BLOXROUTE_AUTH_HEADER` | BDN `polygon_private_tx` (not full node) |
| Stream | `POLYGON_WSS_URLS` / `WSS_URL` | Sync/Swap logs |

## Health selection

`RpcPool` orders state URLs by rate-limit headroom then probe latency. Rate-limited URLs cool off / deprioritize. Budget scope: `rpc_budget` + `RPC_BATCH_PACE_MS`.

## Load knobs that matter

- `MAX_MULTICALL_CALLS` — chunk size (250 used successfully with multi-URL).
- `LF_BOOTSTRAP_BATCH` × `LF_INTERVAL_MS` — **primary** state RPC product.
- `STREAM_RPC_FANOUT` (max 3) and `STREAM_MAX_POOLS` — WSS interest set size.
- HF prefetch budget and probe-tick pool cap — secondary.

## Symptoms

| Log | Meaning |
|-----|---------|
| `state refresh overrun` | Fetch longer than LF interval |
| `rate limited` / deprioritize | Back off that URL |
| `TickLens empty` | CL tick hydrate failed (often quota) |
| `rpc_attempts=N/7` | Fallback chain length |

## Expansion lesson (2026-07-30)

2.5k bootstrap batches at 4s interval → mild overruns, **zero** 429s on a 7-URL state pool. Raising interval to **4.5s** cleared overruns without shrinking the graph growth rate materially.

## Related

- [[state-refresh]]
- [[graph-poolset-expansion]]
- [[architecture-pass-loop]]
