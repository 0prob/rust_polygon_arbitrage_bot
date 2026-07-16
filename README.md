> [!CAUTION]
> **CRITICAL WARNING: Work in Progress**
> This bot is actively under development. Running this software with real capital carries a severe risk of **permanent loss of funds**. Use at your own risk.

# rpbot

Polygon mainnet MEV arbitrage bot. Discovers pools from an Envio/HyperIndex indexer, builds a multi-protocol routing graph, finds profitable cycles, simulates swaps locally, and executes via a Huff flash-loan executor contract.

## Features

- **Dual-frequency loop** — LF pass (default 1s): pool discovery, state refresh, graph build, cycle enumeration. HF pass (default 150ms): prefetch, Brent input sizing, local simulation, dry-run or live execution.
- **Multi-protocol routing** — Uniswap V2/V3/V4 (hookless pools via `unlock`/`unlockCallback`), QuickSwap Algebra V3/Integral, Balancer V2, Curve (stable & crypto), Dodo, Woofi.
- **Cycle search** — Hybrid parallel DFS + Johnson hub search + Bellman-Ford (default), or `dfs` / `johnson` / `bellman-ford` alone; spot-weighted adjacency graph, atomic probe prefilter, graph/cycle caching.
- **Pool discovery** — PostgreSQL direct SQL feed from HyperIndex; periodic refresh and dead-pool pruning.
- **State refresh** — Archival RPC multicall for reserves, V3 ticks (TickLens), V4 storage slots, and protocol-specific fields.
- **Profit scoring** — Hop simulation uses on-chain pool state in-memory; base pricing uses an LF snapshot (`token_to_matic_rates`) from hub-path arena sim (token → WPOL; enabled by default in `OracleConfig`) plus Chainlink/Pyth for configured feeds and POL/USD caps. Gas oracle, flash-loan fees, slippage, circuit breaker. Min profit is MATIC-denominated (`MIN_PROFIT_MATIC_WEI`).
- **Learned route risk** — Per-route success/failure history persists at `ROUTE_STATS_PATH`; unreliable routes require proportionally more expected net profit before preflight.
- **Flash-loan routing** — `FLASH_LOAN_SOURCE=auto` (default) uses a Balancer-first waterfall: on-chain liquidity checks per token, Aave fallback, and cap-and-reoptimize when borrow size exceeds provider liquidity. HF eval uses pessimistic Aave fees in auto mode.
- **Execution** — Dry-run simulation or live submit via Huff `ArbExecutor`; optional MEV-protected `PRIVATE_RPC_URL`, profit-scaled priority fees, nonce management, route cooldown/quarantine, receipt polling.
- **HyperSync** (optional) — Block head feed and receipt lookups when `ENVIO_API_TOKEN` is set.
- **TUI dashboard** — Ratatui terminal UI with live pipeline metrics, opportunities, route visualization, simulations, trades, portfolio, diagnostics, and config panels (requires the same RPC/indexer setup as `rpbot`).

## Binaries

| Binary | Purpose |
|---|---|
| `rpbot` | Main bot (default) |
| `tui` | Terminal dashboard (`--features tui`) |
| `oracle_feeds` | Audit / propose / verify Pyth feed mappings (human-in-the-loop); verified Polygon mints are merged into `TOKEN_FEEDS` in `price_oracle.rs` |

## Prerequisites

- **Rust nightly** — pinned in `rust-toolchain.toml`; `.cargo/config.toml` uses `-Zthreads`; crate is edition 2024.
- **Polygon RPC** — archival endpoint recommended for pool-state reads (`STATE_RPC_URL` / `POLYGON_RPC_URLS`).
- **Envio indexer** — PostgreSQL from sibling HyperIndex repo (`PG_URL`; typical default `postgres://postgres@localhost:5433/envio-dev`).
- **Live execution** — deployed Huff executor from sibling `sol` repo (Foundry + `script/deploy_mainnet.sh`).

## Setup

Start the HyperIndex discovery feed (sibling repo):

```bash
cd ../h && bun install && cp .env.example .env   # first time only
cd ../h && bun run dev
```

Configure the bot:

```bash
cp .env.example .env
# Edit .env — all documented options are in .env.example
```

**Dry-run minimum**

| Variable | Purpose |
|---|---|
| `PG_URL` | PostgreSQL for HyperIndex pool metadata |
| `STATE_RPC_URL` or `POLYGON_RPC_URLS` | Multicall pool-state reads (not execution quota) |
| `EXECUTION_RPC` | HF `eth_call` simulation, gas, receipts |
| `EXECUTION_MODE=dry-run` | No on-chain submits (default) |

**Live trading** additionally requires `PRIVATE_KEY` or `PRIVATE_KEY_FILE`, `EXECUTOR_ADDRESS`, and `EXECUTION_MODE=live`. Use `PRIVATE_RPC_URL` and/or `BLOXROUTE_AUTH_HEADER` for private submission; if `REQUIRE_PRIVATE_SUBMIT=true`, at least one of those must be set.

Deploy the Huff executor (requires Foundry):

```bash
cd ../sol && ./script/deploy_mainnet.sh
# Set EXECUTOR_ADDRESS in .env to the deployed address
```

**Config precedence:** code defaults → `.env` (or `DOTENV_PATH`) → variables already set in the process environment. Env names map to nested config in `src/config/mod.rs` (`env_key_to_figment_path`); see `.env.example` for tuned LF/HF/RPC values.

## Run

Main bot:

```bash
cargo run --release
```

TUI dashboard (live pipeline):

```bash
cargo run --bin tui --features tui --release
```

Oracle feed workflow:

```bash
cargo run --bin oracle_feeds --release -- audit --top 50
cargo run --bin oracle_feeds --release -- propose --curated-only --verify --out proposed-feeds.txt
cargo run --bin oracle_feeds --release -- verify --file proposed-feeds.txt
```

Continuous runner and log tail (after `cargo build --release`):

```bash
./scripts/run-continuous.sh   # restart loop; stdout → target/run-logs/
./scripts/watch.sh            # metrics from latest run log (RPBOT_LOG_DIR, default /tmp/bot)
```

`run-continuous.sh` forces `EXECUTION_MODE=dry-run` for the child process regardless of `.env`.

## Development

```bash
cargo test
cargo bench   # routing benches: v2/v3 swap, route sim, graph rescore, cycle find, optimize
```

Calldata golden tests in `tests/calldata_test.rs` verify route encoding and executor selectors.

## Project docs

| Path | Content |
|---|---|
| `doc/routing.md` | Protocol graph rules, liquidity gates, simulation fidelity |
| `.env.example` | Environment reference (with `src/config/mod.rs`) |
| `graphify-out/`, `docs/` | Regenerable analysis output (gitignored) |

## Architecture

```
Premium RPC (WSS) ──eth_subscribe logs──► PartialPoolCache (DashMap, target pools only)
                                               │ flush on stream trigger
PostgreSQL ──► StateRefreshService ──► StateCache ◄┘
               │
pass_loop
├── LF background (discovery → multicall refresh → graph → cycles → snapshot)
│       └── updates WSS subscription target set (top V2/V3 pools)
├── WSS feed (filtered Sync/Swap logs → partial cache patches)
└── HF (interval + block trigger + stream-triggered)
        └── prefetch skipped on stream ticks (stream patches already fresh)
        └── dry-run / submit via private RPC or bloXroute BDN
```

Pool metadata flows PostgreSQL → `StateRefreshService` → `StateCache` → routing graph. LF publishes cycle snapshots; HF reads them lock-free via `SnapshotStore` (ArcSwap). Stream patches merge into `StateCache` on the hot path without a full node refresh.

Set `STREAM_ENABLED=true` and configure `POLYGON_WSS_URLS` or `WSS_URL` (or rely on `wss://` conversion from HTTP state RPCs). Live submits should not use the public execution RPC for mempool injection — use `PRIVATE_RPC_URL` or `BLOXROUTE_AUTH_HEADER`.

**Logging:** compact colored stdout (`RPBOT_LOG`, default `info`). Component JSONL under `$RPBOT_LOG_DIR/run-<timestamp>-<pid>/` (default `/tmp/bot`). The TUI suppresses stdout logs while retaining JSONL files.
