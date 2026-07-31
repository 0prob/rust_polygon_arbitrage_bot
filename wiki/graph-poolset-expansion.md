---
created: 2026-07-30
updated: 2026-07-30
sources:
  - raw/session-2026-07-30-graph-expansion.md
  - raw/README.md
---

# Graph Poolset Expansion

Operational goal: grow **working** `graph_pools` (live-edge membership) toward the tradable ceiling without 429 storms.

## Funnel

```
index rows (~260k)
  → retain_routable (V2 skip → ~74k disc)
  → multicall cache (batch/tick)
  → tradable (~18k ceiling observed)
  → arena membership
  → graph attach (ATTACH_BATCH_CAP)
  → cycle_capable (~7k observed)
  → enumerated cycles (path cap, e.g. 24)
```

## Levers that grew the set (2026-07-30)

| Lever | Effect on graph | RPC impact |
|-------|-----------------|------------|
| Higher `LF_BOOTSTRAP_BATCH` | Faster tradable cache fill | Higher multicall volume |
| Warm `*6` | Longer full-batch phase | Sustained until large tradable set |
| `ATTACH_BATCH_CAP` 768 | Faster edge catch-up | CPU only |
| `ARENA_REBUILD_CAP` 3072 | Faster arena ingest | CPU only |
| `LF_INTERVAL_MS` 4500 | Headroom for 2.5k batches | **Lowers** overrun pressure |
| Multi state URLs + pace | Failover / budget | Prevents sticky 429 |

## Observed peaks (dry-run)

| Run | Duration | Max `graph_pools` | Rate limits | Overruns |
|-----|----------|-------------------|-------------|----------|
| 1 (LF 4s) | ~5.5 min | **16_610** | 0 | 10 mild |
| 2 (LF 4.5s) | ~3.5 min | **14_890** | 0 | **0** |

## Anti-patterns

- Raising bootstrap without raising LF interval → refresh overruns.
- Raising `STREAM_MAX_POOLS` aggressively → WSS/log noise, not graph size.
- Enabling all V2 without liquidity floors → dust graph, dead cycles.
- Expecting graph == discovered: only tradable + admissible edges count.

## Related

- [[routing-graph]]
- [[lf-pass]]
- [[state-refresh]]
- [[rpc-and-rate-limits]]
