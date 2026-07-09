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

Config precedence: code defaults → `config.toml` (or `CONFIG_PATH`) → environment variables (later wins). The shipped `config.toml` lowers `min_profit_matic_wei` to 0.01 MATIC, raises HF caps, and sets `enumeration_max_paths = 500`.

## Run

Main bot:

```bash
cargo run --release
```

TUI dashboard (live pipeline):

```bash
cargo run --bin tui --release
```

Useful env vars:

```bash
RPBOT_LOG=info                          # log level filter (error|warn|info|debug|trace)
EXECUTION_MODE=dry-run                  # default-safe mode
ROUTING_CYCLE_FINDER=hybrid             # hybrid | dfs | johnson | bellman-ford
BLOXROUTE_AUTH_HEADER=your_bloxroute_auth   # private mempool via bloXroute BDN
PRIVATE_RPC_URL=https://...                  # MEV-protected submission endpoint
REQUIRE_PRIVATE_SUBMIT=false            # force submissions through PRIVATE_RPC_URL
FLASH_LOAN_SOURCE=auto                  # auto | balancer | aave | aave_v3
STREAM_ENABLED=true                     # WSS log stream for hot pool partial cache
WSS_URL=wss://...                       # WebSocket pool log feed endpoint
QUICKSWAP_V2_ENABLED=true             # include QuickSwap V2 pools (off unless set to true)
UNISWAP_V2_ENABLED=true                 # include Uniswap V2 pools (off unless set to true)
```

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

Logs are structured JSON lines to stderr. Default level is `info`; set `RPBOT_LOG=debug` for more detail. In release builds, `debug!` and `trace!` compile away.

When the TUI is running, `UiBridge` receives snapshot and event updates from the orchestrator for live display.
