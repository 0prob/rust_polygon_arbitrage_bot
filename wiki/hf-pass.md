---
created: 2026-07-30
updated: 2026-07-30
sources:
  - raw/README.md
---

# HF Pass (High Frequency)

Implementation: `src/orchestrator/hf.rs`, `hf_eval.rs`, `hf_execute.rs`.

## Cadence

- Config: `HF_INTERVAL_MS` (code default **200**).
- Also triggered by new blocks and stream events (stream ticks can skip redundant prefetch).

## Pipeline sketch

1. Filter cycles from LF snapshot (quarantine, tickless stuck, proto mismatch, rate skips, …).
2. Prefetch / probe-tick hydrate for tickless CL pools (budget-capped).
3. Probe rank + Brent input sizing (`local_sim` / route cache).
4. [[profit-gate]] (`assess_profit`) — flash fee, slippage, gas.
5. Preflight: local sim → Balancer `queryBatchSwap` when needed → executor `eth_call`.
6. Dry-run log or live [[execution-path]] submit.

## Caps (CPU / RPC)

- `HF_SCORE_CAP`, `HF_SIM_CAP`, `HF_MAX_DISPATCH`
- `HF_PREFETCH_COUNT`, `HF_PREFETCH_BUDGET_MS`
- Probe tick pool cap scales with residual budget

## Failure modes often seen in logs

- `shallow_cl` / tickless: TickLens empty under RPC pressure — see [[rpc-and-rate-limits]]
- Quarantine / route stats — learned risk at `ROUTE_STATS_PATH`
- Non-positive net after gas

## Related

- [[lf-pass]]
- [[flash-loans]]
- [[profit-gate]]
