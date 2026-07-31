---
created: 2026-07-30
updated: 2026-07-30
sources:
  - raw/README.md
  - raw/env.example.md
---

# Pool Discovery

Source of truth for **which pools exist**: Envio HyperIndex Postgres (`PG_URL`).

## Pipeline

1. Bootstrap keyset pages of `PoolMeta` (and related) via `src/infra/pg.rs`.
2. Parse → protocol-specific `DiscoveredPool` (`src/services/discovery.rs`).
3. `retain_routable_pool` / `routable_skip_reason` drops non-fetchable or disabled families.
4. Incremental LISTEN/NOTIFY + watermark cursor for updates.
5. Optional HyperSync gap-fill (`HYPERSYNC_*` flags).

## V2 toggles

Env (unset = enabled):

- `QUICKSWAP_V2_ENABLED`
- `UNISWAP_V2_ENABLED`
- `SUSHISWAP_V2_ENABLED`

Ops often disable all three to cut dust and RPC load; then discovered set is ~70k+ non-V2 rows vs 260k+ raw index — see [[graph-poolset-expansion]].

## Downstream

Discovery alone does **not** put pools on the [[routing-graph]]. [[state-refresh]] must fetch tradable state; only then arena sync + attach can create edges.

## Config

- `DISCOVERY_INTERVAL_MS`, `DISCOVERY_BOOTSTRAP_BATCH`
- Indexer lag: `INDEXER_MAX_LAG_BLOCKS`, `INDEXER_PAUSE_ON_LAG`

## Related

- [[lf-pass]]
- [[protocols]]
- [[config-env]]
