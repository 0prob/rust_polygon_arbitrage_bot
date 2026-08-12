---
created: 2026-07-30
updated: 2026-08-13
sources:
  - raw/README.md
---

# Oracle Pricing

Purpose: produce a reliable `token_to_matic_rates` map for [[routing-graph]] gating and [[profit-gate]] USD/POL floors.

## Sources (merged)

| Source | Role |
|--------|------|
| Hub-path arena sim | Token → WMATIC/WPOL multi-hop (default on in `OracleConfig`) |
| Chainlink | Configured feeds |
| Pyth Hermes | Spot / integer feeds; auto-feed scan for unmapped tokens |
| LST helpers | e.g. MaticX (may suppress after view failure) |

## LF snapshot

Rates refresh on [[lf-pass]]; HF uses the snapshot (not a full re-oracle every 200ms). Merge retains prior rates when fresh enrich is partial (`retain_stale`).

## Auto feeds

Allow-listed unmapped addresses trigger a Hermes USD-spot scan immediately, in batches of up to 20 with a 30s fallback tick. Hits persist under `target/run-logs/oracle-auto-feeds.json` (override `RPBOT_ORACLE_AUTO_FEEDS`); misses are marked `no_feed`.

## Related

- [[lf-pass]]
- [[profit-gate]]
- [[config-env]]
