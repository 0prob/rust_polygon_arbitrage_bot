# rpbot

Polygon mainnet MEV arbitrage bot. Discovers pools from an Envio/HyperIndex indexer, builds a multi-protocol routing graph, finds profitable cycles, simulates swaps locally, and executes via a Huff flash-loan executor contract.

## Features

- **Dual-frequency loop** — LF pass (default 1s): pool discovery, state refresh, graph build, cycle enumeration. HF pass (default 150ms): prefetch, Brent input sizing, local simulation, dry-run or live execution.
- **Multi-protocol routing** — Uniswap V2/V3/V4 (hookless pools via `unlock`/`unlockCallback`), QuickSwap Algebra V3/Integral, Balancer V2, Curve (stable & crypto), Dodo, Woofi.
- **Cycle search** — Hybrid parallel DFS + Johnson hub search + Bellman-Ford (default), or `dfs` / `johnson` / `bellman-ford` alone; spot-weighted adjacency graph, atomic probe prefilter, graph/cycle caching.
- **Pool discovery** — PostgreSQL direct SQL feed from HyperIndex; periodic refresh and dead-pool pruning.
- **State refresh** — Archival RPC multicall for reserves, V3 ticks (TickLens), V4 storage slots, and protocol-specific fields.
- **Profit scoring** — Optional Chainlink + Pyth oracle enrichment (token → MATIC rates), gas oracle, flash-loan fee deduction, slippage buffer, circuit breaker. Min-profit threshold is MATIC-denominated (`MIN_PROFIT_MATIC_WEI`).
- **Learned route risk** — Per-route success/failure history persists at `ROUTE_STATS_PATH`; unreliable routes require proportionally more expected net profit before preflight.
- **Flash-loan routing** — `FLASH_LOAN_SOURCE=auto` (default) uses a Balancer-first waterfall: on-chain liquidity checks per token, Aave fallback, and cap-and-reoptimize when borrow size exceeds provider liquidity. HF eval uses pessimistic Aave fees in auto mode.
- **Execution** — Dry-run simulation or live submit via Huff `ArbExecutor`; optional MEV-protected `PRIVATE_RPC_URL`, profit-scaled priority fees, nonce management, route cooldown/quarantine, receipt polling.
- **HyperSync** (optional) — Block head feed and receipt lookups when `ENVIO_API_TOKEN` is set.
- **TUI dashboard** — Ratatui terminal UI with live pipeline metrics, opportunities, route visualization, simulations, trades, portfolio, diagnostics, and config panels (requires the same RPC/indexer setup as `rpbot`).

## Binaries

| Binary | Purpose |
|---|---|
| `rpbot` | Main bot (default) |
| `tui` | Terminal dashboard |

## Prerequisites

- **Rust** nightly (uses `-Zthreads` in `.cargo/config.toml` and edition 2024)
- **Polygon RPC** — archival endpoint recommended for pool-state reads (`STATE_RPC_URL`)
- **Envio indexer** — PostgreSQL database from `/home/x/arb/h` (`PG_URL`; start with `cd ../h && bun run dev`)
- **Live execution** — deployed Huff executor from sibling repo `/home/x/arb/sol` (Foundry + `script/deploy_mainnet.sh`)

## Setup

Start the HyperIndex discovery feed (sibling repo):

```bash
cd ../h && bun install && cp .env.example .env   # first time only
cd ../h && bun run dev
```

Configure the bot:

```bash
cp .env.example .env
# Edit .env — see comments in .env.example for all options
# Ensure PG_URL points to the running PostgreSQL (default: postgres://postgres@localhost:5433/envio-dev)
```

Minimum to run in dry-run mode:

| Variable | Purpose |
|---|---|
| `PG_URL` | PostgreSQL connection string for HyperIndex data |
| `STATE_RPC_URL` or `POLYGON_RPC_URLS` | Pool state reads |
| `EXECUTION_RPC` | Tx simulation (dry-run) |
| `EXECUTION_MODE=dry-run` | No on-chain submits |

For live trading, also set `PRIVATE_KEY` (or `PRIVATE_KEY_FILE`), `EXECUTOR_ADDRESS`, and `EXECUTION_MODE=live`. Optionally set `PRIVATE_RPC_URL` for MEV-protected submission, or `BLOXROUTE_AUTH_HEADER` for bloXroute BDN.

Deploy the Huff executor (requires Foundry):

```bash
cd ../sol && ./script/deploy_mainnet.sh
# Set EXECUTOR_ADDRESS in .env to the logged address
```

Config precedence: code defaults → `.env` (or `DOTENV_PATH`) → variables already set in the process environment. Tuned LF/HF/routing/RPC values live in `.env.example`; copy to `.env` and adjust for your RPC tier.

## Run

Main bot:

```bash
cargo run --release
```

TUI dashboard (live pipeline):

```bash
cargo run --bin tui --release
```

Current active env values in this checkout:

```bash
RPBOT_LOG=info
EXECUTION_MODE=live
DISCOVERY_INTERVAL_MS=5000
LF_INTERVAL_MS=4000
LF_BOOTSTRAP_BATCH=5000
STREAM_ENABLED=true
REQUIRE_PRIVATE_SUBMIT=true
FLASH_LOAN_SOURCE=auto
QUICKSWAP_V2_ENABLED=false
UNISWAP_V2_ENABLED=false
MAX_MULTICALL_CALLS=200
RPC_BATCH_PACE_MS=8
```

The checked-in `.env` currently points at:

- `PG_URL=postgres://postgres:testing@localhost:5433/envio-dev`
- `EXECUTION_RPC` on Alchemy
- `STATE_RPC_URL` / `POLYGON_RPC_URLS` on dRPC + Chainstack + Ankr + PublicNode
- `POLYGON_WSS_URLS` on dRPC + Alchemy + Chainstack
- `ENVIO_API_TOKEN` enabled
- `EXECUTION_MODE=live`
- `REQUIRE_PRIVATE_SUBMIT=true`
- `PRIVATE_RPC_URL=https://api.blxrbdn.com`
- `BLOXROUTE_AUTH_HEADER=...`

Continuous runner and log dashboard (after `cargo build --release`):

```bash
./scripts/run-continuous.sh   # restart loop with backoff, logs to target/run-logs/
./scripts/watch.sh              # tail metrics from the latest run log
```

## Development

```bash
cargo test
cargo bench   # simulate_v2_swap, simulate_v3_swap_ticks, simulate_route_3hop,
              # rescore_graph_64_pools, find_cycles_hybrid_3pool, optimize_cycle_2hop
```

Calldata golden tests in `tests/calldata_test.rs` verify route encoding and executor selectors.

## Project docs

- **`docs/`**, **`graphify-rs-out/`** — regenerable, gitignored

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

Pool metadata flows from PostgreSQL → `StateRefreshService` → `StateCache` → routing graph. LF publishes cycle snapshots; HF reads them lock-free via `SnapshotStore` (ArcSwap). Stream patches merge into `StateCache` on the hot path without a full node.

Set `STREAM_ENABLED=true` and `WSS_URL` (or rely on `wss://` auto-conversion from `STATE_RPC_URL`). Live submits should use `PRIVATE_RPC_URL` or `BLOXROUTE_AUTH_HEADER` for direct mempool injection (not the public execution RPC).

Live logs use a compact, `tput`-colored stdout format with fixed level and component columns. The same events are buffered asynchronously as JSONL under `/tmp/bot/run-<timestamp>-<pid>/{system,infra,state,oracle,routing,orchestrator,execution,tui}.jsonl`; private run directories prevent mixed sessions, the newest 10 runs are retained, and each active component is capped at 16 MiB. Set `RPBOT_LOG=debug` for more detail or `RPBOT_LOG_DIR` to move the run directories. The TUI suppresses stdout logs while retaining the component files.

When the TUI is running, `UiBridge` receives snapshot and event updates from the orchestrator for live display.
