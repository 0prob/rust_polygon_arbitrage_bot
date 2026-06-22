# Execution Loop Audit, Debug, Simplify & Optimize

## Overview

Audit, debug, consolidate, and optimize the main execution loop of the Polarb
MEV arbitrage bot — a dual-frequency (LF 1s / HF 200ms) tick loop in
`src/orchestrator/pass_loop.rs`.

## Motivation

The current loop has accumulated duplicated code, inline magic numbers,
CPU-bound LF ticks that starve HF ticks, and expensive snapshot cloning. This
work addresses all four categories systematically.

## Design

### Section 1: Shared Utility Module (`src/util.rs`)

Consolidate three duplicated helpers into a single module:

- **`now_ms() -> u64`** — (was in `pass_lf.rs`, `pass_hf.rs`, `state_refresh.rs`)
- **`u256_to_f64(v: U256) -> f64`** — (was in `spot_price.rs`, `profit.rs`);
  verify and unify the constant values to eliminate subtle precision mismatch
- **`parse_flash_source(raw: &str) -> FlashLoanSource`** — (was in `pass_hf.rs`,
  `hf_execute.rs`)

Also add named constants for all inline magic numbers:

| Constant | Value | Used In |
|---|---|---|
| `LF_BOOTSTRAP_BATCH` | 300 | `pass_lf.rs` |
| `HF_PREFETCH_COUNT` | 40 | `pass_hf.rs` |
| `HF_SCORE_CAP` | 48 | `pass_hf.rs` |
| `HF_SIM_CAP` | 24 | `pass_hf.rs` |
| `HF_MAX_DISPATCH` | 3 | `pass_hf.rs` |

### Section 2: Background LF Task

**Problem:** The LF tick (discovery + cycle enumeration) is CPU-bound and runs
inline in `tokio::select!`. When it exceeds the LF interval, HF ticks are
silently dropped due to `MissedTickBehavior::Skip`.

**Solution:** Move LF enumeration to a background `tokio::spawn` task:

```
pass_loop (tokio::select!)
├── hf_timer.tick() → run_hf_tick()
└── shutdown.changed()
    ↑
    │ (if LF task errors fatally)
    │
    └── lf_error_rx (watch::Receiver)

Background LF task:
  loop {
    lf_timer.tick().await;
    run_lf_tick();
    snapshots.publish(...);
  }
```

- The `SnapshotStore` (ArcSwap) already handles lock-free publish/read between
  LF and HF — no new channel needed
- Remove `lf_enumeration_in_flight` flag — HF just reads latest snapshot
- Add a `watch::Sender<bool>` that the LF task triggers on fatal error to
  signal shutdown to main loop

### Section 3: Snapshot & Cycle Caching

**Snapshot optimization:**
- Remove `SnapshotStore::update()` — no longer needed after removing
  `lf_enumeration_in_flight`
- The LF task builds the snapshot from scratch and calls `publish()` directly

**Cycle caching:**
- Extend `GraphCache` to store cached cycles alongside graph:
  `cycles: Option<Vec<FoundCycle>>`
- When `pool_fingerprint` is unchanged and no full rebuild is forced, return
  cached cycles too
- This avoids re-running DFS/Bellman-Ford on every LF tick when the pool set
  is stable

**Remove redundant rescore:**
- `finalize_enumerated_cycles()` currently calls `rescore_cycles_by_spot_price`
  internally, then HF tick also rescales. Remove the rescore from
  `finalize_enumerated_cycles` — HF tick always rescales against current state.

### Section 4: File Restructuring

Rename and reorganize orchestrator files:

```
orchestrator/
├── mod.rs          (pub mod loop, lf_task, hf_tick, hf_execute, constants)
├── constants.rs    (NEW — all named constants)
├── loop.rs         (was pass_loop.rs — main select loop, background LF spawn)
├── lf_task.rs      (was pass_lf.rs — background enumeration task)
├── hf_tick.rs      (was pass_hf.rs — HF tick, HfTickResult)
└── hf_execute.rs   (unchanged)
```

Also move `sync_arena_from_discovery` into `pipeline/arena.rs` as
`StateArena::sync_from_discovery(...)`.

Update `main.rs` imports to match new module paths.

## Files Changed

| File | Change |
|---|---|
| `src/util.rs` | NEW — shared helpers and constants |
| `src/orchestrator/constants.rs` | NEW — named constants |
| `src/orchestrator/loop.rs` | NEW — renamed from pass_loop.rs, + background LF spawn |
| `src/orchestrator/lf_task.rs` | NEW — renamed from pass_lf.rs, + cycle caching |
| `src/orchestrator/hf_tick.rs` | NEW — renamed from pass_hf.rs, reduced imports |
| `src/orchestrator/hf_execute.rs` | Remove `parse_flash_source`, import from util |
| `src/orchestrator/mod.rs` | Update module declarations |
| `src/main.rs` | Update import paths |
| `src/services/hf_snapshot.rs` | Remove `update()` method, remove in_flight flag |
| `src/pipeline/graph_cache.rs` | Add cycle caching |
| `src/pipeline/arena.rs` | Add `sync_from_discovery` method |
| `src/pipeline/spot_price.rs` | Remove `u256_to_f64` (import from util), skip rescore in finalize |
| `src/services/execution/profit.rs` | Remove `u256_to_f64` (import from util) |
| `src/services/state_refresh.rs` | Remove `now_ms()` (import from util) |

## Out of Scope

- Prometheus /metrics endpoint (tracing-only remains sufficient for now)
- Connection pooling for RPC providers (can be addressed separately)
- Full event-driven architecture (keeping the timer-based model)