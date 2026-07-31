# rpbot Knowledge Wiki — Index

Master map of synthesized pages under [`wiki/`](wiki/). Immutable sources live in [`raw/`](raw/). Change log: [`log.md`](log.md).

## Hierarchy

### Core

- [[rpbot-overview]] — product scope, subsystems, binaries
- [[architecture-pass-loop]] — LF / HF / stream control plane
- [[wiki-ops]] — how this knowledge base is maintained

### Runtime loops

- [[lf-pass]] — discovery, refresh, graph, cycles
- [[hf-pass]] — probe, Brent, preflight, submit

### Data & graph

- [[pool-discovery]] — HyperIndex Postgres metadata
- [[state-refresh]] — multicall cache & warm policy
- [[routing-graph]] — arena, edges, attach, cycles
- [[graph-poolset-expansion]] — ops campaign to grow working graph
- [[protocols]] — venue families and modeling

### Economics & execution

- [[oracle-pricing]] — token→MATIC rates
- [[profit-gate]] — assess_profit and preflight stack
- [[flash-loans]] — Balancer / Aave selection
- [[execution-path]] — dry-run vs live, private submit

### Platform

- [[config-env]] — load order and ops knobs
- [[rpc-and-rate-limits]] — endpoints, budgets, symptoms

## Raw sources (immutable)

| Path | Description |
|------|-------------|
| [raw/README-raw.md](raw/README-raw.md) | Raw directory contract |
| [raw/README.md](raw/README.md) | Project README snapshot |
| [raw/env.example.md](raw/env.example.md) | Env catalog snapshot |
| [raw/flash_tokens.md](raw/flash_tokens.md) | Flash liquidity notes |
| [raw/session-2026-07-30-graph-expansion.md](raw/session-2026-07-30-graph-expansion.md) | Expansion dry-run notes |

## Cross-reference matrix (high traffic)

| From | Often links to |
|------|----------------|
| lf-pass | state-refresh, routing-graph, oracle-pricing |
| hf-pass | profit-gate, flash-loans, execution-path |
| graph-poolset-expansion | lf-pass, state-refresh, rpc-and-rate-limits |
| profit-gate | oracle-pricing, flash-loans, hf-pass |

## Page count

**16** wiki nodes at init (2026-07-30).

## Ingest queue

_Awaiting `/ingest` or new files under `raw/`._
