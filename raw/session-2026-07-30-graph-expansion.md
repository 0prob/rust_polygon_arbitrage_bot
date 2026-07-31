# Session notes — graph poolset expansion (2026-07-30)

Operational dry-run loop to enlarge the working routing graph without overwhelming Polygon RPCs.

## Baseline (pre-change)

- Short multi-rpc dry runs: `graph_pools` ~2k–7k after a few LF passes.
- Discovery (V2 disabled): ~74k routable metadata rows from HyperIndex Postgres.
- Tradable fraction of discovered ≈ 20–25% after multicall state fetch.
- Attach catch-up lag when `ATTACH_BATCH_CAP=512` and `LF_BOOTSTRAP_BATCH≥2k`.

## Code changes

- `ATTACH_BATCH_CAP`: 512 → 768 (`src/core/constants.rs`)
- `ARENA_REBUILD_CAP`: 2048 → 3072
- Warm cache target: `lf_bootstrap_batch * 4` → `* 6` (`src/services/state_refresh.rs`)
- Unit test `keeps_bootstrap_batch_until_routable_set_is_warm` updated for *6 (default batch 3000 → warm 18_000)

## Env knobs (local `.env`, not secrets)

- `LF_BOOTSTRAP_BATCH=2500`
- `LF_HOT_BATCH=800`
- `LF_INTERVAL_MS=4500` (was 4000; eliminates mild refresh overruns)
- `LF_FULL_SWEEP_INTERVAL=20`
- `STREAM_MAX_POOLS=300`
- `MAX_MULTICALL_CALLS=250` (unchanged)
- V2 protocols remain disabled (`QUICKSWAP_V2_ENABLED=false`, etc.)

## Run results (dry-run, `target/release-fast/rpbot`)

### Run 1 — ~5.5 min, LF 4000ms

- `graph_pools`: 409 → **16_610**
- `arena_pools` ≈ **17_639**
- `routable` (tradable cache) ≈ **18_028**
- Rate limits: **0**
- Refresh overruns: **10** (mild, ~4.1–4.5s vs 4s interval)
- Errors/panics: **0**

### Run 2 — ~3.5 min, LF 4500ms

- `graph_pools` → **14_890** (still climbing; shorter window)
- Rate limits: **0**
- Refresh overruns: **0**
- Errors/panics: **0**
- Stream targets: 300 pools

## Practical ceiling

With V2 skipped, ~74k discovered → ~18k tradable in cache is near the on-chain tradable ceiling for this indexer snapshot. Further growth requires more tradable state (protocols, liquidity floors) not larger bootstrap alone.

## RPC safety notes

- Multi-URL state pool with rate-limit headroom + deprioritize on 429.
- Bootstrap batches stay on full size until `routable >= bootstrap * 6`, then hot batch; full sweep every `LF_FULL_SWEEP_INTERVAL` passes still uses bootstrap size.
- Attach/arena caps are **CPU** costs; multicall batch size and LF interval dominate RPC load.
