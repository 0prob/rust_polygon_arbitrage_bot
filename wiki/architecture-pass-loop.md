---
created: 2026-07-30
updated: 2026-07-30
sources:
  - raw/README.md
---

# Architecture — Pass Loop

Control plane lives in `src/orchestrator/pass_loop.rs` and wires three concurrent paths:

```
WSS RPC fanout ──eth_subscribe logs──► dedup ──► PartialPoolCache
                                                    │ flush on stream trigger
PostgreSQL ──► StateRefreshService ──► StateCache ◄┘
               │
pass_loop
├── [[lf-pass]]  (discovery → multicall → graph → cycles → snapshot)
│       └── updates WSS subscription target set
├── WSS feeds (up to STREAM_RPC_FANOUT Sync/Swap feeds)
└── [[hf-pass]] (interval + block + stream-triggered)
        └── dry-run / submit
```

## Data flow summary

1. [[pool-discovery]] loads pool meta into discovery state.
2. [[state-refresh]] fills `StateCache` with on-chain pool state.
3. Arena sync + [[routing-graph]] build/attach produce live edges.
4. Cycle finder publishes a snapshot; HF reads lock-free via `SnapshotStore` (ArcSwap).
5. Stream patches merge into `StateCache` on the hot path without a full node refresh.

## Invariants

- LF owns graph connectivity and cycle membership growth.
- HF owns evaluation latency and execution safety ([[profit-gate]], preflight).
- Stream is **off by default** in code (`STREAM_ENABLED`); local ops often enable it (see [[config-env]]).

## See also

- [[rpc-and-rate-limits]]
- [[graph-poolset-expansion]]
