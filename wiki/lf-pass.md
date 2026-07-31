---
created: 2026-07-30
updated: 2026-07-30
sources:
  - raw/README.md
  - raw/session-2026-07-30-graph-expansion.md
---

# LF Pass (Low Frequency)

Implementation: `src/orchestrator/lf.rs`.

## Cadence

- Config: `LF_INTERVAL_MS` (code default **4000**; ops often **4500** after expansion to avoid multicall overruns — see [[graph-poolset-expansion]]).
- Each pass may: refresh pool states, sync arena, rebuild or `attach_missing` on the [[routing-graph]], re-find cycles, refresh oracle rates, update stream targets.

## Batching

| Knob | Role |
|------|------|
| `LF_BOOTSTRAP_BATCH` | Full multicall window while cache is warming |
| `LF_HOT_BATCH` | Smaller window once warm |
| `LF_FULL_SWEEP_INTERVAL` | Periodic full bootstrap-sized sweep after warm |
| Warm target | `bootstrap_batch * 6` tradable pools (`refresh_batch_for`) |

Warm multiplier raised 4→6 so the graph does not plateau early when only ~20–25% of discovered pools are tradable.

## Attach / arena caps (CPU, not RPC)

| Constant | Value (post-2026-07-30) | Purpose |
|----------|-------------------------|---------|
| `ATTACH_BATCH_CAP` | 768 | Per-tick graph attach catch-up |
| `ARENA_REBUILD_CAP` | 3072 | Full rebuild ingest remainder on later ticks |

## Outputs

- Updated arena + graph edges
- Cycle snapshot for [[hf-pass]]
- Stream interest set (top pools, capped by `STREAM_MAX_POOLS`)
- `token_to_matic_rates` for [[oracle-pricing]] / [[profit-gate]]

## Related

- [[state-refresh]]
- [[routing-graph]]
- [[architecture-pass-loop]]
