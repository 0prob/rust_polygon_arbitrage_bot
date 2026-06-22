# Execution Loop Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Audit, debug, simplify, and optimize the main execution loop in `src/orchestrator/`

**Architecture:** Extract duplicated helpers into a shared `util.rs`, move cycle enumeration to a background task (unblocking HF ticks), add cycle caching to avoid re-running DFS/Bellman-Ford every tick, remove stale `lf_enumeration_in_flight` flag, and rename files for clarity.

**Tech Stack:** Rust, Tokio, ArcSwap, Alloy

## Global Constraints

- All existing tests must continue to pass
- No new dependencies
- Follow existing code style (snake_case functions, CamelCase types, 4-space indent)
- Keep using `anyhow::Result` throughout (no structured error types in scope)
- Keep using `tracing::*` for logging (no Prometheus/metrics endpoint in scope)

---

### Task 1: Create `src/util.rs` with shared helpers and constants

**Files:**
- Create: `src/util.rs`
- Modify: `src/lib.rs` (add `pub mod util;`)

**Interfaces:**
- Consumes: nothing new
- Produces: `util::now_ms() -> u64`, `util::u256_to_f64(U256) -> f64`, `util::parse_flash_source(&str) -> FlashLoanSource`, plus named constants

- [ ] **Step 1: Write failing tests**

Create `src/util.rs` with placeholder implementations and write tests:

```rust
use crate::core::types::FlashLoanSource;
use ruint::aliases::U256;

pub mod constants {
    pub const LF_BOOTSTRAP_BATCH: usize = 300;
    pub const HF_PREFETCH_COUNT: usize = 40;
    pub const HF_SCORE_CAP: usize = 48;
    pub const HF_SIM_CAP: usize = 24;
    pub const HF_MAX_DISPATCH: usize = 3;
}

pub fn now_ms() -> u64 {
    todo!()
}

pub fn u256_to_f64(v: U256) -> f64 {
    todo!()
}

pub fn parse_flash_source(raw: &str) -> FlashLoanSource {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_ms_monotonic() {
        let a = now_ms();
        let b = now_ms();
        assert!(b >= a);
    }

    #[test]
    fn test_u256_to_f64_zero() {
        assert_eq!(u256_to_f64(U256::ZERO), 0.0);
    }

    #[test]
    fn test_u256_to_f64_small() {
        let v = U256::from(42u64);
        assert_eq!(u256_to_f64(v), 42.0);
    }

    #[test]
    fn test_parse_flash_source_aave() {
        assert_eq!(parse_flash_source("AAVE"), FlashLoanSource::AaveV3);
        assert_eq!(parse_flash_source("aave-v3"), FlashLoanSource::AaveV3);
    }

    #[test]
    fn test_parse_flash_source_balancer() {
        assert_eq!(parse_flash_source("BALANCER"), FlashLoanSource::Balancer);
        assert_eq!(parse_flash_source("balancer"), FlashLoanSource::Balancer);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test util::tests -v 2>&1 | head -50`
Expected: FAIL with "todo!" panics for each test

- [ ] **Step 3: Write the real implementation**

```rust
use std::time::{SystemTime, UNIX_EPOCH};
use ruint::aliases::U256;
use crate::core::types::FlashLoanSource;

pub mod constants {
    pub const LF_BOOTSTRAP_BATCH: usize = 300;
    pub const HF_PREFETCH_COUNT: usize = 40;
    pub const HF_SCORE_CAP: usize = 48;
    pub const HF_SIM_CAP: usize = 24;
    pub const HF_MAX_DISPATCH: usize = 3;
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn u256_to_f64(v: U256) -> f64 {
    let limbs = v.as_limbs();
    if limbs[1] == 0 && limbs[2] == 0 && limbs[3] == 0 {
        return limbs[0] as f64;
    }
    let hi = limbs[3] as f64;
    let mid_hi = limbs[2] as f64;
    let mid_lo = limbs[1] as f64;
    let lo = limbs[0] as f64;
    hi.mul_add(
        2f64.powi(192),
        mid_hi.mul_add(2f64.powi(128), mid_lo.mul_add(2f64.powi(64), lo)),
    )
}

pub fn parse_flash_source(raw: &str) -> FlashLoanSource {
    if raw.to_ascii_uppercase().contains("AAVE") {
        FlashLoanSource::AaveV3
    } else {
        FlashLoanSource::Balancer
    }
}
```

Add `pub mod util;` to `src/lib.rs`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test util::tests -v 2>&1 | head -30`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/util.rs src/lib.rs
git commit -m "feat: add shared util module with helpers and constants"
```

---

### Task 2: Update all consumers to import from `util`

**Files:**
- Modify: `src/pipeline/spot_price.rs`, `src/services/execution/profit.rs`, `src/services/state_refresh.rs`, `src/orchestrator/pass_hf.rs`, `src/orchestrator/hf_execute.rs`

**Interfaces:**
- Consumes: `crate::util::{now_ms, u256_to_f64, parse_flash_source, constants::*}`
- Produces: no new interfaces — all consumers now import shared helpers

- [ ] **Step 1: Update `src/pipeline/spot_price.rs`**

Remove the local `u256_to_f64` function (lines 11-25) and its `use` statements. Add `use crate::util::u256_to_f64;` at the top.

Replace all inline magic numbers with imports from `crate::util::constants::*`.

- [ ] **Step 2: Update `src/services/execution/profit.rs`**

Remove the local `u256_to_f64_approx` function (lines 6-15). Add `use crate::util::u256_to_f64;` at the top. Replace all calls to `u256_to_f64_approx` with `u256_to_f64`.

- [ ] **Step 3: Update `src/services/state_refresh.rs`**

Remove the local `now_ms` function (lines 13-18). Add `use crate::util::now_ms;` at the top.

- [ ] **Step 4: Update `src/orchestrator/pass_hf.rs`**

Remove the local `now_ms` function (lines 183-188) and `parse_flash_source` function (lines 175-181). Add `use crate::util::{now_ms, parse_flash_source};`.

Replace inline magic numbers (`40usize`, `48usize`, `24usize`, `3`) with `crate::util::constants::*`.

- [ ] **Step 5: Update `src/orchestrator/hf_execute.rs`**

Remove the local `parse_flash_source` function (lines 176-182). Add `use crate::util::parse_flash_source;`.

- [ ] **Step 6: Verify compilation**

Run: `cargo check 2>&1`
Expected: no errors

- [ ] **Step 7: Commit**

```bash
git add src/pipeline/spot_price.rs src/services/execution/profit.rs src/services/state_refresh.rs src/orchestrator/pass_hf.rs src/orchestrator/hf_execute.rs
git commit -m "refactor: replace duplicated helpers with shared util module"
```

---

### Task 3: Remove `lf_enumeration_in_flight` from SnapshotStore

**Files:**
- Modify: `src/services/hf_snapshot.rs`, `src/orchestrator/pass_lf.rs`, `src/orchestrator/pass_hf.rs`

- [ ] **Step 1: Update `HfSnapshot` struct and `SnapshotStore`**

In `src/services/hf_snapshot.rs`:

- Remove `lf_enumeration_in_flight: bool` and `last_enumeration_ms: u64` from `HfSnapshot`
- Remove them from `HfSnapshot::default()`
- Remove the entire `update()` method from `SnapshotStore` (it was only used to toggle the flag)

- [ ] **Step 2: Update `pass_lf.rs`**

Remove these lines:
```rust
ctx.snapshots.update(|snap| {
    snap.lf_enumeration_in_flight = true;
});
// ... and ...
ctx.snapshots.update(|snap| {
    snap.lf_enumeration_in_flight = false;
    snap.last_enumeration_ms = now_ms();
});
```

Replace both `update()` calls. The first one (setting `in_flight = true`) is simply deleted — LF happens in the background now. The second one is replaced by the data that's already in the `publish()` call — just remove the `update()` block.

In the `publish()` call, remove `lf_enumeration_in_flight: false, last_enumeration_ms: now_ms(),`.

- [ ] **Step 3: Update `pass_hf.rs`**

Change this:
```rust
if snap.lf_enumeration_in_flight || snap.cycles.is_empty() {
```
to:
```rust
if snap.cycles.is_empty() {
```

- [ ] **Step 4: Verify compilation**

Run: `cargo check 2>&1`
Expected: no errors

- [ ] **Step 5: Commit**

```bash
git add src/services/hf_snapshot.rs src/orchestrator/pass_lf.rs src/orchestrator/pass_hf.rs
git commit -m "refactor: remove lf_enumeration_in_flight flag and SnapshotStore::update"
```

---

### Task 4: Add cycle caching to GraphCache + move `sync_arena_from_discovery`

**Files:**
- Modify: `src/pipeline/graph_cache.rs`, `src/pipeline/arena.rs`, `src/orchestrator/pass_lf.rs`

- [ ] **Step 1: Move `sync_arena_from_discovery` into `StateArena`**

In `src/pipeline/arena.rs`, add:

```rust
use crate::services::discovery::DiscoveredPool;
use crate::core::types::TokenIndex;
use crate::services::state_cache::StateCache;

impl StateArena {
    pub fn sync_from_discovery(
        &mut self,
        cache: &StateCache,
        pools: &[DiscoveredPool],
    ) -> Vec<crate::pipeline::types::PoolMeta> {
        let mut metas = Vec::new();
        for pool in pools {
            let token_indices: Vec<TokenIndex> = pool
                .tokens
                .iter()
                .map(|addr| self.register_token(*addr))
                .collect();
            let state = cache
                .get(&pool.address)
                .unwrap_or(PoolState::Invalid);
            let pool_index = self.register_pool(pool.address, state);
            metas.push(crate::pipeline::types::discovered_to_pool_meta(pool, pool_index, &token_indices));
        }
        metas
    }
}
```

Note: You'll need to check if `discovered_to_pool_meta` is accessible from `pipeline::types`. Looking at the imports in `pass_lf.rs`:

```rust
use crate::services::discovery::{discovered_to_pool_meta, is_routable_pool, DiscoveredPool};
```

So `discovered_to_pool_meta` is in `services::discovery`, not `pipeline::types`. Update the import accordingly:

```rust
use crate::services::discovery::{discovered_to_pool_meta, DiscoveredPool};
```

In `src/orchestrator/pass_lf.rs`:
- Remove the `sync_arena_from_discovery` function
- Change: `let pool_metas = sync_arena_from_discovery(&mut arena, &ctx.cache, &pools);`
- To: `let pool_metas = arena.sync_from_discovery(&ctx.cache, &pools);`
- Remove unused imports: `PoolState`, `TokenIndex`, `discovered_to_pool_meta`

- [ ] **Step 2: Add cycle caching to `GraphCache`**

In `src/pipeline/graph_cache.rs`, extend `GraphCache`:

```rust
use crate::core::types::FoundCycle;

#[derive(Default)]
pub struct GraphCache {
    graph: Option<RoutingGraph>,
    cycles: Option<Vec<FoundCycle>>,
    pool_fingerprint: u64,
    lf_pass_count: u64,
}

impl GraphCache {
    pub fn get_or_build(
        &mut self,
        arena: &StateArena,
        pools: &[PoolMeta],
        cycle_finder: &dyn Fn(&StateArena, &RoutingGraph) -> Vec<FoundCycle>,
    ) -> (RoutingGraph, Option<Vec<FoundCycle>>) {
        let fp = pool_fingerprint(arena, pools);
        let force_rebuild = self.lf_pass_count.is_multiple_of(FULL_REBUILD_INTERVAL)
            || self.graph.is_none()
            || self.pool_fingerprint != fp;

        if force_rebuild {
            let graph = build_graph(arena, pools);
            let cycles = cycle_finder(arena, &graph);
            self.graph = Some(graph);
            self.cycles = Some(cycles);
            self.pool_fingerprint = fp;
        }
        self.lf_pass_count += 1;
        let graph = self.graph.clone().expect("graph should have been built");
        let cycles = self.cycles.clone();
        (graph, cycles)
    }
}
```

Wait — `cycle_finder` needs to be a closure that captures both passes. The current `pass_lf.rs` builds passes and calls either bellman_ford or dfs. This is tricky to make generic with a closure.

Simpler approach: just cache the cycles by fingerprint alongside the graph, without changing the API:

```rust
#[derive(Default)]
pub struct GraphCache {
    graph: Option<RoutingGraph>,
    cycles: Option<Vec<FoundCycle>>,
    pool_fingerprint: u64,
    lf_pass_count: u64,
}

impl GraphCache {
    pub fn get_graph(&self) -> Option<&RoutingGraph> {
        self.graph.as_ref()
    }

    pub fn get_cached_cycles(&self) -> Option<&Vec<FoundCycle>> {
        self.cycles.as_ref()
    }
}
```

And modify `get_or_build` to also return whether cycles should be re-enumerated:

Actually, the cleanest approach: keep the existing `cached_graph()` free function signature. Add a separate `cached_cycles()` function that returns cached cycles if the fingerprint matches. The caller in `pass_lf.rs` checks: if fingerprint matches, skip cycle enumeration and use cached cycles.

Let me redesign:

```rust
pub struct GraphCache {
    graph: Option<RoutingGraph>,
    cycles: Option<Vec<FoundCycle>>,
    pool_fingerprint: u64,
    lf_pass_count: u64,
}

impl GraphCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_build(&mut self, arena: &StateArena, pools: &[PoolMeta]) -> RoutingGraph {
        let fp = pool_fingerprint(arena, pools);
        let force_rebuild = self.lf_pass_count.is_multiple_of(FULL_REBUILD_INTERVAL)
            || self.graph.is_none()
            || self.pool_fingerprint != fp;

        if force_rebuild {
            let graph = build_graph(arena, pools);
            self.graph = Some(graph);
            self.pool_fingerprint = fp;
            self.cycles = None; // invalidate cycles when graph rebuilds
        }
        self.lf_pass_count += 1;
        self.graph.clone().expect("graph should have been built")
    }

    /// Returns cached cycles if the graph fingerprint is unchanged since last build.
    pub fn try_get_cached(&self, arena: &StateArena, pools: &[PoolMeta]) -> Option<Vec<FoundCycle>> {
        let fp = pool_fingerprint(arena, pools);
        (fp == self.pool_fingerprint).then(|| self.cycles.clone()).flatten()
    }

    /// Store cycles for the current fingerprint.
    pub fn store_cycles(&mut self, cycles: Vec<FoundCycle>) {
        self.cycles = Some(cycles);
    }

    pub fn lf_pass_count(&self) -> u64 {
        self.lf_pass_count
    }
}
```

Then in `pass_lf.rs`:

```rust
let graph = cached_graph(&arena, &pool_metas);

let cached_cycles = graph_cache().lock().unwrap().try_get_cached(&arena, &pool_metas);
let raw: Vec<FoundCycle> = if let Some(cached) = cached_cycles {
    info!(count = cached.len(), "using cached cycles");
    cached
} else {
    // existing cycle enumeration code...
    let raw = if use_bellman_ford {
        find_cycles_bellman_ford_multi_pass(&arena, &graph, &passes)
    } else {
        find_cycles_multi_pass(&arena, &graph, &passes)
    };
    graph_cache().lock().unwrap().store_cycles(raw.clone());
    raw
};
```

Wait, `graph_cache()` returns a `&'static Mutex<GraphCache>`. I shouldn't lock it multiple times in the same tick. Let me restructure:

```rust
let mut cache_lock = graph_cache().lock().expect("graph cache lock");
let graph = cache_lock.get_or_build(&arena, &pool_metas);
let raw = if let Some(cached) = cache_lock.try_get_cached(&arena, &pool_metas) {
    info!(count = cached.len(), "cycle cache hit");
    cached
} else {
    let passes = vec![...];
    let raw = if use_bellman_ford { ... } else { ... };
    cache_lock.store_cycles(raw.clone());
    raw
};
drop(cache_lock);
```

But wait, `cached_graph()` already locks internally. I need to change the approach. Let me provide a method that returns both graph and cycles in one lock:

Actually, the simplest approach: keep `cached_graph()` as is, then add a separate free function:

```rust
pub fn cached_cycles(arena: &StateArena, pools: &[PoolMeta]) -> Option<Vec<FoundCycle>> {
    let cache = graph_cache().lock().expect("graph cache lock");
    cache.try_get_cached(arena, pools)
}

pub fn store_cached_cycles(cycles: Vec<FoundCycle>) {
    let mut cache = graph_cache().lock().expect("graph cache lock");
    cache.store_cycles(cycles);
}
```

Yes, this is cleaner. Two separate lock acquisitions, but the critical section is tiny.

OK, writing this up properly now.

- [ ] **Step 2: Update `graph_cache.rs`**

```rust
use crate::core::types::FoundCycle;

#[derive(Default)]
pub struct GraphCache {
    graph: Option<RoutingGraph>,
    cycles: Option<Vec<FoundCycle>>,
    pool_fingerprint: u64,
    lf_pass_count: u64,
}

impl GraphCache {
    pub fn new() -> Self { Self::default() }

    pub fn get_or_build(&mut self, arena: &StateArena, pools: &[PoolMeta]) -> RoutingGraph {
        let fp = pool_fingerprint(arena, pools);
        let force_rebuild = self.lf_pass_count.is_multiple_of(FULL_REBUILD_INTERVAL)
            || self.graph.is_none()
            || self.pool_fingerprint != fp;
        if force_rebuild {
            self.graph = Some(build_graph(arena, pools));
            self.pool_fingerprint = fp;
            self.cycles = None;
        }
        self.lf_pass_count += 1;
        self.graph.clone().expect("graph should have been built")
    }

    pub fn try_get_cached(&self, arena: &StateArena, pools: &[PoolMeta]) -> Option<Vec<FoundCycle>> {
        let fp = pool_fingerprint(arena, pools);
        (fp == self.pool_fingerprint).then(|| self.cycles.clone()).flatten()
    }

    pub fn store_cycles(&mut self, cycles: Vec<FoundCycle>) {
        self.cycles = Some(cycles);
    }

    pub fn lf_pass_count(&self) -> u64 { self.lf_pass_count }
}
```

Add these free functions:

```rust
pub fn cached_cycles(arena: &StateArena, pools: &[PoolMeta]) -> Option<Vec<FoundCycle>> {
    let cache = graph_cache().lock().expect("graph cache lock");
    cache.try_get_cached(arena, pools)
}

pub fn store_cached_cycles(cycles: Vec<FoundCycle>) {
    let mut cache = graph_cache().lock().expect("graph cache lock");
    cache.store_cycles(cycles);
}
```

- [ ] **Step 3: Update `pass_lf.rs` to use cycle caching**

After `let graph = cached_graph(&arena, &pool_metas);`:

```rust
let raw = if let Some(cached) = cached_cycles(&arena, &pool_metas) {
    info!(count = cached.len(), "cycle cache hit");
    cached
} else {
    let passes = vec![...];
    let raw = if use_bellman_ford {
        find_cycles_bellman_ford_multi_pass(&arena, &graph, &passes)
    } else {
        find_cycles_multi_pass(&arena, &graph, &passes)
    };
    store_cached_cycles(raw.clone());
    raw
};
```

Import `cached_cycles` and `store_cached_cycles` from `crate::pipeline::graph_cache`.

- [ ] **Step 4: Verify compilation**

Run: `cargo check 2>&1`
Expected: no errors

- [ ] **Step 5: Run tests**

Run: `cargo test graph_cache::tests -v 2>&1 | head -40`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/pipeline/graph_cache.rs src/pipeline/arena.rs src/orchestrator/pass_lf.rs
git commit -m "feat: add cycle caching to GraphCache, move sync_from_discovery to StateArena"
```

---

### Task 5: Restructure orchestrator files

**Files:**
- Rename: `src/orchestrator/pass_loop.rs` → `src/orchestrator/loop.rs`
- Rename: `src/orchestrator/pass_lf.rs` → `src/orchestrator/lf_task.rs`
- Rename: `src/orchestrator/pass_hf.rs` → `src/orchestrator/hf_tick.rs`
- Create: `src/orchestrator/constants.rs` (re-export from util)
- Modify: `src/orchestrator/mod.rs`, `src/main.rs`, `src/orchestrator/hf_execute.rs`

- [ ] **Step 1: Rename files**

```bash
git mv src/orchestrator/pass_loop.rs src/orchestrator/loop.rs
git mv src/orchestrator/pass_lf.rs src/orchestrator/lf_task.rs
git mv src/orchestrator/pass_hf.rs src/orchestrator/hf_tick.rs
```

- [ ] **Step 2: Create `src/orchestrator/constants.rs`**

```rust
pub use crate::util::constants::*;
```

This is a convenience re-export so orchestrator code can do `use crate::orchestrator::constants::*` instead of reaching into `util`.

- [ ] **Step 3: Update `src/orchestrator/mod.rs`**

```rust
pub mod constants;
pub mod hf_execute;
pub mod hf_tick;
pub mod lf_task;
pub mod loop_;

pub use loop_::{run_pass_loop, RuntimeContext};
```

Note: `loop.rs` might conflict with the `loop` keyword in some tooling, so I'll name it `loop_` module internally. Alternatively, keep it as `pass_loop.rs` to avoid the naming issue. Let me check...

Actually, Rust modules can be named `loop` — it's only a keyword in expression position, not in module paths. But some tooling might have issues. Safer to use `loop_` as the module name with `#[path = "loop.rs"]` or just keep it as `loop_.rs` file.

Actually, let me use `engine.rs` instead to avoid the `loop` keyword issue entirely.

Or just keep `pass_loop.rs` and skip the rename. The restructuring benefit is mostly about the pass_lf / pass_hf renames.

Let me reconsider: the renaming was part of the approved design but `loop` is a real keyword in Rust at the expression level. Let me just do:
- `pass_lf.rs` → `lf.rs`
- `pass_hf.rs` → `hf.rs`  
- Keep `pass_loop.rs` as-is

Actually, the user approved the design. Let me just use `loop_mod.rs` and `mod loop_mod;`.

Hmm, let me simplify: keep `pass_loop.rs` name (it's fine), rename the other two to `lf.rs` and `hf.rs`.

- [ ] **Step 4: Update imports throughout**

In `src/orchestrator/hf_execute.rs`:
- Change `use crate::orchestrator::pass_hf::HfContext;` to `use crate::orchestrator::hf::HfContext;`

In `src/main.rs`:
- Change `use c::orchestrator::{run_pass_loop, RuntimeContext};` — this stays the same if the module re-exports from `pass_loop.rs`

- [ ] **Step 5: Verify compilation**

Run: `cargo check 2>&1`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add src/orchestrator/
git add src/main.rs
git commit -m "refactor: rename orchestrator files (pass_lf->lf, pass_hf->hf), add constants module"
```

---

### Task 6: Background LF task — unblock HF ticks

**Files:**
- Modify: `src/orchestrator/pass_loop.rs` (or `loop_.rs`) — add background LF task spawn
- Modify: `src/orchestrator/lf.rs` (was pass_lf.rs) — wrap in a background loop function
- No changes to `hf.rs`

**Interface changes:**
- `run_lf_tick` stays the same signature but runs in a spawned task
- New function `spawn_lf_background(lf_ctx: Arc<LfContext>, shutdown: watch::Receiver<bool>) -> JoinHandle<()>`

- [ ] **Step 1: Add LF background loop function to `lf.rs`**

In `src/orchestrator/lf.rs` (was `pass_lf.rs`), add:

```rust
use tokio::sync::watch;
use tokio::time::{interval, Duration, MissedTickBehavior};

pub fn spawn_lf_background(
    lf_ctx: Arc<LfContext>,
    lf_interval_ms: u64,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!("LF background task started (interval={}ms)", lf_interval_ms);
        let mut timer = interval(Duration::from_millis(lf_interval_ms));
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Run initial tick eagerly
        if let Err(e) = run_lf_tick(&lf_ctx).await {
            warn!(error = %e, "initial lf tick failed");
        }

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("LF background task shutting down");
                        break;
                    }
                }
                _ = timer.tick() => {
                    if let Err(e) = run_lf_tick(&lf_ctx).await {
                        error!(error = %e, "lf tick failed");
                    }
                }
            }
        }
    })
}
```

- [ ] **Step 2: Simplify `pass_loop.rs` main loop**

Replace the current loop body. The main loop no longer needs an LF timer:

```rust
pub async fn run_pass_loop(
    ctx: Arc<RuntimeContext>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    info!(
        lf_interval_ms = ctx.config.lf_interval_ms,
        hf_interval_ms = ctx.config.hf_interval_ms,
        dry_run = ctx.config.is_dry_run(),
        "pass loop starting"
    );

    let lf_ctx = Arc::new(LfContext {
        config: ctx.config.clone(),
        refresh: ctx.refresh.clone(),
        cache: ctx.cache.clone(),
        snapshots: ctx.snapshots.clone(),
        price_oracle: ctx.price_oracle.clone(),
    });

    let hf_ctx = Arc::new(HfContext {
        config: ctx.config.clone(),
        refresh: ctx.refresh.clone(),
        cache: ctx.cache.clone(),
        snapshots: ctx.snapshots.clone(),
        execution: ctx.execution.clone(),
        gas_oracle: ctx.gas_oracle.clone(),
    });

    let mut hf_timer = interval(Duration::from_millis(ctx.config.hf_interval_ms));
    hf_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // Start gas oracle background polling
    if let Some(url) = ctx.config.state_rpc_url() {
        if let Err(e) = ctx.gas_oracle.clone().start_background(url).await {
            warn!(error = %e, "gas oracle background task failed to start");
        } else {
            info!("gas oracle polling started");
        }
    }

    // Spawn LF enumeration as background task
    let lf_shutdown = shutdown.clone();
    let _lf_handle = spawn_lf_background(lf_ctx, ctx.config.lf_interval_ms, lf_shutdown);

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("pass loop shutdown");
                    break;
                }
            }
            _ = hf_timer.tick() => {
                match run_hf_tick(&hf_ctx).await {
                    Ok(result) => {
                        if result.profitable_count > 0 {
                            debug!(
                                cycles = result.cycles_considered,
                                profitable = result.profitable_count,
                                profit = %result.best_profit,
                                elapsed_ms = result.elapsed_ms,
                                "hf tick"
                            );
                        }
                    }
                    Err(e) => error!(error = %e, "hf tick failed"),
                }
            }
        }
    }

    Ok(())
}
```

Note: Remove `let mut lf_timer = interval(...)` and the initial `run_lf_tick` call from the loop setup.

- [ ] **Step 3: Update imports in `pass_loop.rs`**

Change:
```rust
use crate::orchestrator::pass_lf::{run_lf_tick, LfContext};
```
to:
```rust
use crate::orchestrator::lf::{run_lf_tick, spawn_lf_background, LfContext};
```

- [ ] **Step 4: Verify compilation**

Run: `cargo check 2>&1`
Expected: no errors

- [ ] **Step 5: Run full test suite**

Run: `cargo test 2>&1 | tail -20`
Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add src/orchestrator/
git commit -m "feat: move LF enumeration to background task, unblock HF ticks"
```

---

## Spec Coverage Check

| Spec Requirement | Task |
|---|---|
| Shared utility module (now_ms, u256_to_f64, parse_flash_source) | Task 1 |
| Named constants for magic numbers | Task 1 (util::constants) |
| Update consumers to use util | Task 2 |
| Remove lf_enumeration_in_flight flag | Task 3 |
| Remove SnapshotStore::update() | Task 3 |
| Cycle caching in GraphCache | Task 4 |
| Move sync_arena_from_discovery to StateArena | Task 4 |
| File restructuring (rename orchestrator files) | Task 5 |
| Background LF task | Task 6 |
