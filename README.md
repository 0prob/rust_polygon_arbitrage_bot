# Polarb (Rust)

Polygon mainnet MEV arbitrage bot. Discovers pools from an Envio/HyperIndex indexer, builds a multi-protocol routing graph, finds profitable cycles, simulates swaps locally, and executes via a Huff flash-loan executor contract.

## Features

- **Dual-frequency loop** — LF pass (~1s): pool discovery, state refresh, graph build, cycle enumeration. HF pass (~200ms): prefetch, Brent input sizing, local simulation, dry-run or live execution.
- **Multi-protocol routing** — Uniswap V2/V3/V4, Balancer V2, Curve (stable & crypto), Dodo, Woofi; Kyber Elastic and Ramses V3 treated as V3 variants.
- **Cycle search** — Hybrid parallel DFS + Johnson hub search + Bellman-Ford (default), or `dfs` / `johnson` / `bellman-ford` alone; spot-weighted adjacency graph, atomic probe prefilter, graph/cycle caching.
- **Pool discovery** — Hasura GraphQL feed from HyperIndex; periodic refresh and dead-pool pruning.
- **State refresh** — Archival RPC multicall for reserves, V3 ticks (TickLens), V4 storage slots, and protocol-specific fields.
- **Profit scoring** — Optional Chainlink + Pyth oracle enrichment (token → MATIC rates), gas oracle, flash-loan fee deduction, slippage buffer, circuit breaker. Min-profit threshold is MATIC-denominated (`MIN_PROFIT_MATIC_WEI`; also accepts `MIN_PROFIT_WEI` as fallback).
- **Flash-loan routing** — `FLASH_LOAN_SOURCE=auto` (default) uses a Balancer-first waterfall: on-chain liquidity checks per token, Aave fallback, and cap-and-reoptimize when borrow size exceeds provider liquidity. HF eval uses pessimistic Aave fees in auto mode.
- **Execution** — Dry-run simulation or live submit via Huff `ArbExecutor`; optional MEV-protected `PRIVATE_RPC_URL`, profit-scaled priority fees, nonce management, route cooldown/quarantine, receipt polling.
- **HyperSync** (optional) — Block head feed and receipt lookups when `ENVIO_API_TOKEN` is set.
- **TUI dashboard** (optional) — Ratatui terminal UI with live pipeline metrics, opportunities, route visualization, simulations, trades, portfolio, diagnostics, and config panels. Mock mode for demos without RPC/indexer.

## Binaries

| Binary | Purpose |
|---|---|
| `rpbot` | Main bot (default) |
| `tui` | Terminal dashboard (`--features tui`) |
| `flame_profile` | CPU flamegraph profiling targets (`cargo flamegraph --bin flame_profile -- <target>`) |

## Prerequisites

- **Rust** nightly (uses `-Zthreads` in `.cargo/config.toml` and edition 2024)
- **Polygon RPC** — archival endpoint recommended for pool-state reads (`STATE_RPC_URL`)
- **Envio indexer** — Hasura endpoint serving pool/token metadata (`HASURA_URL`, `HASURA_SECRET`)
- **Live execution** — deployed Huff executor (`https://github.com/0prob/solidity_and_huff_evm_contract`), Foundry for deploy script

## Setup

```bash
cp .env.example .env
# Edit .env — see comments in .env.example for all options
```

Minimum to run in dry-run mode:

| Variable | Purpose |
|---|---|
| `HASURA_URL` | Envio/HyperIndex GraphQL endpoint |
| `HASURA_SECRET` | Hasura admin secret |
| `STATE_RPC_URL` or `POLYGON_RPC_URLS` | Pool state reads |
| `EXECUTION_RPC` | Tx simulation (dry-run) |
| `EXECUTION_MODE=dry-run` | No on-chain submits |

For live trading, also set `PRIVATE_KEY` (or `PRIVATE_KEY_FILE`), `EXECUTOR_ADDRESS`, and `EXECUTION_MODE=live`. Optionally set `PRIVATE_RPC_URL` for MEV-protected submission, or `BLOXROUTE_AUTH_HEADER` for bloXroute BDN.

Deploy the Huff executor (requires Foundry and sibling repo `https://github.com/0prob/solidity_and_huff_evm_contract`):
# Set EXECUTOR_ADDRESS in .env to the logged address

Optional TOML config overrides env vars: set `CONFIG_PATH=./config.toml` (Figment merge).

## Run

Main bot:

```bash
cargo run --release
```

TUI dashboard (live pipeline):

```bash
cargo run --bin tui --features tui --release
```

TUI mock demo (no RPC/indexer required):

```bash
cargo run --bin tui --features tui -- --mock
```

Useful env vars:

```bash
RUST_LOG=info                          # tracing filter
TRACING_JSON=1                         # structured JSON logs
EXECUTION_MODE=dry-run                 # default-safe mode
ROUTING_CYCLE_FINDER=hybrid            # hybrid | dfs | johnson | bellman-ford
BLOXROUTE_AUTH_HEADER=your_bloxroute_auth  # private mempool via bloXroute BDN
PRIVATE_RPC_URL=https://...                 # MEV-protected submission endpoint
REQUIRE_PRIVATE_SUBMIT=false           # force submissions through PRIVATE_RPC_URL
FLASH_LOAN_SOURCE=auto                 # auto | BALANCER | AAVE_V3
STREAM_ENABLED=true                    # WSS log stream for hot pool partial cache
WSS_URL=wss://...                      # WebSocket pool log feed endpoint
OPPORTUNITY_JOURNAL_PATH=./opportunities.jsonl  # JSONL profit/loss journal
```

## Development

```bash
cargo test
cargo bench                            # cycle_finder, local_sim, spot_price, hf_tick
cargo flamegraph --bin flame_profile -- routing   # requires cargo-flamegraph
```

Calldata parity tests in `tests/calldata_parity.rs` verify encoding against TypeScript fixtures.

Optional tokio-console task introspection:

```bash
RUSTFLAGS='--cfg tokio_unstable' cargo build --features tokio-console --release
TOKIO_CONSOLE=1 cargo run --release
```

## Architecture

```
Premium RPC (WSS) ──eth_subscribe logs──► PartialPoolCache (DashMap, target pools only)
                                              │ flush on stream trigger
Hasura ──► StateRefreshService ──► StateCache ◄┘
              │
pass_loop
├── LF background (discovery → multicall refresh → graph → cycles → snapshot)
│       └── updates WSS subscription target set (top V2/V3 pools)
├── WSS feed (filtered Sync/Swap logs → partial cache patches)
└── HF (interval + stream-triggered)
        └── prefetch skipped on stream ticks when HF_SKIP_PREFETCH_ON_STREAM=1
        └── dry-run / submit via private RPC or bloXroute BDN
```

Pool metadata flows from Hasura → `StateRefreshService` → `StateCache` → routing graph. LF publishes cycle snapshots; HF reads them lock-free via `SnapshotStore` (ArcSwap). Stream patches merge into `StateCache` on the hot path without a full node.

Set `STREAM_ENABLED=true` and `WSS_URL` (or rely on `wss://` auto-conversion from `STATE_RPC_URL`). Live submits should use `PRIVATE_RPC_URL` or `BLOXROUTE_AUTH_HEADER` for direct mempool injection (not the public execution RPC).

When `OPPORTUNITY_JOURNAL_PATH` is set, evaluated routes and dispatch outcomes are appended as JSONL for offline analysis.

When the TUI is running, `UiBridge` receives snapshot and event updates from the orchestrator for live display.
