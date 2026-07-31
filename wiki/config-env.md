---
created: 2026-07-30
updated: 2026-07-30
sources:
  - raw/env.example.md
  - raw/README.md
  - raw/session-2026-07-30-graph-expansion.md
---

# Configuration and Environment

## Load order

1. **Process environment** (wins; `.env` will not override set vars)
2. `.env` or `DOTENV_PATH`
3. Code defaults in `src/config/mod.rs`

Blank optional values are ignored.

## Ops snapshot (graph expansion era)

Non-secret knobs proven under dry-run expansion:

| Variable | Expansion value | Notes |
|----------|-----------------|-------|
| `LF_INTERVAL_MS` | 4500 | Headroom vs 2500 batch |
| `LF_BOOTSTRAP_BATCH` | 2500 | Warm fill |
| `LF_HOT_BATCH` | 800 | Post-warm |
| `LF_FULL_SWEEP_INTERVAL` | 20 | Periodic full window |
| `STREAM_MAX_POOLS` | 300 | WSS interest |
| `MAX_MULTICALL_CALLS` | 250 | Chunk size |
| `STREAM_ENABLED` | true (ops) | Code default false |
| V2 `*_ENABLED` | false | Skip dust |

## Critical live keys (names only)

`PRIVATE_KEY` / `PRIVATE_KEY_FILE`, `EXECUTOR_ADDRESS`, `PG_URL`, `STATE_RPC_URL` / `POLYGON_RPC_URLS`, `EXECUTION_RPC`, `PRIVATE_RPC_URL`, `BLOXROUTE_AUTH_HEADER`, `REQUIRE_PRIVATE_SUBMIT`.

Full catalog: `raw/env.example.md` (immutable snapshot of `.env.example`).

## Related

- [[rpc-and-rate-limits]]
- [[lf-pass]]
- [[execution-path]]
