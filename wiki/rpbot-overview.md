---
created: 2026-07-30
updated: 2026-07-30
sources:
  - raw/README.md
  - raw/session-2026-07-30-graph-expansion.md
---

# rpbot Overview

**rpbot** is a Polygon mainnet MEV arbitrage bot. It discovers pools from an Envio/HyperIndex Postgres feed, builds a multi-protocol [[routing-graph]], finds profitable cycles, sizes inputs with Brent search, simulates swaps locally, and executes via a Huff flash-loan executor.

> **Risk:** work in progress; live capital can be lost permanently.

## Dual-frequency control plane

| Loop | Default cadence | Role |
|------|-----------------|------|
| [[lf-pass]] | ~4–4.5s | Discovery, state multicall, graph build/attach, cycle enum, oracle snapshot |
| [[hf-pass]] | ~200ms | Prefetch, probe/Brent, profit gate, dry-run or live submit |

Orchestrated by [[architecture-pass-loop]] (`pass_loop`).

## Major subsystems

- [[pool-discovery]] — HyperIndex / Postgres metadata
- [[state-refresh]] — multi-RPC multicall into state cache
- [[routing-graph]] / [[graph-poolset-expansion]] — arena + attach + eligibility gates
- [[oracle-pricing]] — token→MATIC rates, hub paths, Chainlink/Pyth
- [[profit-gate]] — net profit after flash fee + gas
- [[flash-loans]] — Balancer / Aave V3 selection
- [[execution-path]] — Huff `ArbExecutor`, private submit / bloXroute
- [[rpc-and-rate-limits]] — URL health, budgets, stream WSS
- [[config-env]] — env + code defaults
- [[protocols]] — V2/V3/V4, Algebra, Balancer, Curve, DODO, WooFi

## Binaries

| Binary | Path | Notes |
|--------|------|-------|
| `rpbot` | default | Headless production path |
| `tui` | `--features tui --bin tui` | Optional Ratatui dashboard |

## Related raw

- [[wiki-ops]] for how this knowledge base is maintained
