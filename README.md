> [!CAUTION]
> **CRITICAL WARNING: Work in Progress**
> This bot is actively under development. Running this software with real capital carries a severe risk of **permanent loss of funds**. Use at your own risk.

# rpbot

Polygon mainnet MEV arbitrage bot. Discovers pools from an Envio/HyperIndex indexer, builds a multi-protocol routing graph, finds profitable cycles, sizes inputs with golden-section (Brent) search, simulates swaps locally, and executes via a Huff flash-loan executor contract ([`0prob/solidity_and_huff_evm_contract`](https://github.com/0prob/solidity_and_huff_evm_contract)).

## Features

- **Dual-frequency loop** — LF pass (code default 4s): pool discovery, state refresh, graph build, cycle enumeration. HF pass (code default 200ms): prefetch, Brent input sizing, local simulation, dry-run or live execution.
- **Multi-protocol routing** — Uniswap V2/V3/V4 (hookless pools via `unlock`/`unlockCallback`), QuickSwap Algebra V3/Integral, Balancer V2, Curve (stable & crypto), DODO, WooFi.
- **Cycle search** — Hybrid parallel DFS + Bellman-Ford (default), or `dfs` / `bellman-ford` alone (`johnson` is an env alias for Bellman-Ford); spot-weighted adjacency graph, atomic probe prefilter, graph/cycle caching.
- **Pool discovery** — PostgreSQL direct SQL feed from HyperIndex; periodic refresh and dead-pool pruning. Optional V2 protocol toggles: `QUICKSWAP_V2_ENABLED`, `UNISWAP_V2_ENABLED`, `SUSHISWAP_V2_ENABLED` (unset = enabled).
- **State refresh** — Multi-RPC archival multicall for reserves, V3 ticks (TickLens), V4 storage slots, and protocol-specific fields. Healthy endpoints are selected by rate-limit headroom, then probe latency; rate-limited endpoints cool off before reuse.
- **Profit scoring** — Hop simulation uses in-memory pool state; base pricing uses an LF snapshot (`token_to_matic_rates`) from hub-path arena sim (token → WMATIC/WPOL; enabled by default in `OracleConfig`) plus Chainlink/Pyth for configured feeds and POL/USD caps. Gas oracle, live flash-loan fees, depth impact + optional static slippage, circuit breaker. Execution requires at least $0.01 final profit and final profit to cover gas; an unavailable POL/USD price fails closed.
- **Learned route risk** — Per-route success/failure history at `ROUTE_STATS_PATH` (default `.rpbot-route-stats.json`); unreliable routes need proportionally more expected net profit before preflight.
- **Flash-loan routing** — `FLASH_LOAN_SOURCE=auto` (default) picks a provider per cycle from live liquidity + Vault reentrancy rules (see [Flash loans](#flash-loans)). Aave V3 premium and Balancer protocol flash fee are RPC-refreshed.
- **Execution** — Dry-run simulation or live submit via Huff `ArbExecutor`; optional MEV-protected `PRIVATE_RPC_URL` and/or bloXroute (`BLOXROUTE_AUTH_HEADER`), profit-scaled priority fees, nonce management, route cooldown/quarantine, receipt polling.
- **TUI dashboard** (optional) — Ratatui UI behind `--features tui`; headless `rpbot` is the production path.

## Terminal UI Demo

![rpbot TUI Demo](assets/tui_demo.gif)

Run the interactive dashboard or preview demo mode (cycles through tabs every 5s):

```bash
cargo run --release --features tui --bin tui -- --demo
```

## Binaries

| Binary | Purpose |
|---|---|
| `rpbot` | Main bot (default, headless) |
| `tui` | Terminal dashboard (`cargo run --release --features tui --bin tui`, supports `--demo`) |

## Prerequisites

- **Rust nightly 2026-07-20** — pinned by `rust-toolchain.toml` (verify with `rustc --version`).
- **Polygon RPC** — archival endpoint recommended for pool-state reads (`STATE_RPC_URL` / `POLYGON_RPC_URLS`); separate `EXECUTION_RPC` for HF `eth_call` / gas / receipts.
- **Envio indexer** — PostgreSQL from [`0prob/polygon_envio_hyperindex`](https://github.com/0prob/polygon_envio_hyperindex) (`PG_URL`; code default `postgres://postgres@localhost:5433/envio-dev`).
- **Live execution** — deployed Huff executor from [`0prob/solidity_and_huff_evm_contract`](https://github.com/0prob/solidity_and_huff_evm_contract) (Foundry + `huffc`; `OWNER` + `PRIVATE_KEY` for deploy).

## Setup

Start the HyperIndex discovery feed:

```bash
git clone https://github.com/0prob/polygon_envio_hyperindex.git
cd polygon_envio_hyperindex
bun install && cp .env.example .env   # first time only — fill the indexer's credentials and RPC URLs
bun run dev
```

Configure the bot (this repo):

```bash
cp .env.example .env
# Edit .env — full variable reference is in .env.example (paired with src/config/mod.rs)
# Point PG_URL at the HyperIndex Postgres from the indexer above
```

**Dry-run operational setup**

`EXECUTION_MODE=dry-run` passes configuration validation without endpoints, but discovery and simulation need the services below.

| Variable | Purpose |
|---|---|
| `PG_URL` | PostgreSQL for HyperIndex pool metadata |
| `STATE_RPC_URL` or `POLYGON_RPC_URLS` | Multicall pool-state reads (not execution quota) |
| `EXECUTION_RPC` | HF `eth_call` simulation, gas, receipts |
| `EXECUTION_MODE=dry-run` | No on-chain submits (code default) |

**Live trading** additionally requires `PRIVATE_KEY` or `PRIVATE_KEY_FILE`, `EXECUTOR_ADDRESS`, and `EXECUTION_MODE=live`. Use `PRIVATE_RPC_URL` and/or `BLOXROUTE_AUTH_HEADER` for private submission; if `REQUIRE_PRIVATE_SUBMIT=true`, at least one of those must be set. Live mode also requires state-read URLs and either `EXECUTION_RPC` or `PRIVATE_RPC_URL`.

Deploy the Huff executor:

```bash
git clone https://github.com/0prob/solidity_and_huff_evm_contract.git
cd solidity_and_huff_evm_contract
OWNER=<bot_wallet> PRIVATE_KEY=0x... ./script/deploy_mainnet.sh
# Copy printed EXECUTOR_ADDRESS into this bot's .env
```

**Config load order:** process environment (wins) ← `.env` or `DOTENV_PATH` ← code defaults in `src/config/mod.rs`. Variables already set in the process environment are **not** overwritten by `.env`. Blank optional values are ignored.

## Run

```bash
cargo run --release                    # headless bot (default bin: rpbot)
cargo run --release --features tui --bin tui
cargo build --profile release-fast     # thinner LTO, faster link for local iteration
```

Allow-listed unmapped tokens trigger a background Hermes USD-spot scan immediately (up to 20 per batch, with a 30s fallback tick). Verified feeds are registered and persisted to `target/run-logs/oracle-auto-feeds.json` (override with `RPBOT_ORACLE_AUTO_FEEDS`); misses are marked `no_feed` so they are not rescanned.

Help: `cargo run -- --help` (or `rpbot --help` after build). Concurrent `rpbot`/`tui` processes are killed at startup unless `RPBOT_ALLOW_MULTIPLE` is set.

## Development

```bash
python3 scripts/health_check.py    # end-to-end endpoint & connectivity health check (.env)
cargo test
cargo bench --bench routing        # v2/v3 swap, route sim, graph rescore, cycle find, optimize
cargo build --profile release-fast # near-prod binary without full fat LTO wall time
```

The health check requires the Python packages `psycopg2` and `websockets`.

| Feature | Default | Notes |
|---|---|---|
| `tui` | off | Ratatui dashboard binary |

Integration tests live under `tests/` (`oracle_feed_proposal_test`, `oracle_live_test`). Clippy deny-list includes `unwrap_used`, `panic`, `todo`, and `unimplemented` (see `Cargo.toml` / `clippy.toml`).

## Architecture

```
WSS RPC fanout ──eth_subscribe logs──► dedup ──► PartialPoolCache (DashMap, target pools only)
                                                    │ flush on stream trigger
PostgreSQL ──► StateRefreshService ──► StateCache ◄┘
               │
pass_loop
├── LF background (discovery → multicall refresh → graph → cycles → snapshot)
│       └── updates WSS subscription target set (top V2/V3 pools)
├── WSS feeds (up to STREAM_RPC_FANOUT concurrent filtered Sync/Swap feeds)
└── HF (interval + block trigger + stream-triggered)
        └── prefetch skipped on stream ticks (stream patches already fresh)
        └── dry-run / submit via private RPC or bloXroute BDN
```

Pool metadata flows PostgreSQL → `StateRefreshService` → `StateCache` → routing graph. LF publishes cycle snapshots; HF reads them lock-free via `SnapshotStore` (ArcSwap). Stream patches merge into `StateCache` on the hot path without a full node refresh.

### Profitability gate

`assess_profit` is the execution gate. It applies the full-route slippage haircut to output once, subtracts the selected flash premium, then subtracts `gas_units × (base fee + charged priority fee)`; a profit-derived priority bid subtracts only its incremental uplift above that charged tip. It requires positive post-gas net profit, `max(MIN_PROFIT_MATIC_WEI, $0.01 in POL)` after gas, the optional ROI floor, and the configured gas safety cover. Missing or invalid POL/USD fails closed.

Brent/probe ranking uses the same fee, slippage, gas-scale, and priority basis as that gate. A source change that changes the flash fee is re-optimized before assessment. `MAX_FLASH_LOAN_USD` is a hard configured ceiling; adaptive per-route caps start at one quarter and rise only after profitable, receipt-confirmed cap-bound executions.

Preflight is local simulation, then `queryBatchSwap` plus executor `eth_call` for Direct Balancer routes, then the final calldata `eth_call`/gas reassessment before submit. The three checks cover distinct boundaries and are deliberately not interchangeable. A collapsed depth estimate (`depth_bps >= 10000`) is rejected, while an unavailable +5% depth probe receives the explicit 2500-bps haircut from the shared depth helper. Inputs for tokens with eight or fewer decimals must also be at least one whole token; 18-decimal hubs use only the economic notional floor.

Stream is **off by default** (`STREAM_ENABLED` code default `false`). Set `STREAM_ENABLED=true` and configure `POLYGON_WSS_URLS`; the bot probes the list and starts up to `STREAM_RPC_FANOUT` feeds (default `2`, maximum `3`), preferring distinct provider families. Duplicate `(transaction hash, log index)` events are discarded before they patch state. `WSS_URL` overrides the list and therefore uses one feed. HTTP→WSS fallback is used only when no explicit WSS URL is configured. Live submits should not use the public execution RPC for mempool injection — use `PRIVATE_RPC_URL` or `BLOXROUTE_AUTH_HEADER`.

### Flash loans

Provider selection is per cycle (`FLASH_LOAN_SOURCE`; default `auto`). Accepted env values: `auto` | `balancer` | `balancer_only` | `aave` | `aave_v3`. The bot never invents liquidity: Balancer vault ERC20 balances and Aave aToken underlying balances are multicall-refreshed into `FlashLiquidityCache`.

| Entrypoint | When the bot uses it |
|---|---|
| `executeArbDirect` | Pure Balancer V2 routes — one Vault `batchSwap` flash-swap (Vault `nonReentrant` forbids vault flash + vault swap) |
| `executeArb` | Non-Balancer routes funded by Balancer V2 `flashLoan` (callback rejects any Vault hop) |
| `executeArbWithAave` | Mixed Balancer hops, or when Aave is the viable funder — Polygon **Aave V3** `flashLoanSimple` (pool `0x794a…14aD`) |
| `executeArbWithDodo` | **Disabled** until external (non-route) DODO lenders exist (`DODO_EXTERNAL_FLASH_ENABLED = false` in `profit.rs`; not an env var) |

Constraints:

- Balancer Vault flash and Vault swaps share `nonReentrant` — mixed Balancer routes must borrow elsewhere (Aave).
- Aave V3 pulls `amount + premium` **after** `executeOperation`; on-chain `minProfit` is checked on the post-pull balance when flash token == profit token.
- Flash fees are live-fetched: Aave `FLASHLOAN_PREMIUM_TOTAL` (PercentageMath half-up); Balancer `ProtocolFeesCollector.getFlashLoanFeePercentage` (FixedPoint `mulUp`, often `0` on Polygon). `executeArbDirect` pays no Vault flash fee.
- Aave V4 is not used on Polygon (no deployed flash surface for this bot).
- DODO is **not** selectable via `FLASH_LOAN_SOURCE`.

Executor ABI/details: [`0prob/solidity_and_huff_evm_contract`](https://github.com/0prob/solidity_and_huff_evm_contract).

### Logging

Compact colored stdout (`RPBOT_LOG`, default `info`). Component JSONL under `$RPBOT_LOG_DIR/run-<timestamp>-<pid>/` (default `/tmp/bot`). The TUI suppresses stdout logs while retaining JSONL files.

| Level | Surface |
|---|---|
| `error` | Critical path death / hard misconfig (live submit blocked, task panic) |
| `warn` | Degraded mode or operator action (rate limits, lag, whole-tick skips, reconnects) |
| `info` | Milestones at default verbosity (startup, LF/HF tick summaries, profitable outcomes) |
| `debug` | Per-pass funnels, sample routes, routine refreshes, WSS patch counters |
| `trace` | Hot-path math (Brent/ternary hop detail) |

Message shape: `event: key=value …` (JSONL also stores `event` and `component`). Grep by component file (`orchestrator.jsonl`, `stream.jsonl`, `execution.jsonl`, …) or `event` field.

## Project layout (docs)

| Path | Content |
|---|---|
| `.env.example` | Environment reference (code defaults + example overrides; maps to `src/config/mod.rs`) |
| `src/config/mod.rs` | `AppConfig`, defaults, `env_key_to_figment_path`, validation |

Regenerable analysis output (`docs/`, `graphify-out/`) is gitignored.

## Related repositories

| Repository | Role |
|---|---|
| [0prob/rust_polygon_arbitrage_bot](https://github.com/0prob/rust_polygon_arbitrage_bot) | This bot: routing, simulation, execution |
| [0prob/polygon_envio_hyperindex](https://github.com/0prob/polygon_envio_hyperindex) | Envio HyperIndex discovery feed → Postgres (`PG_URL`) |
| [0prob/solidity_and_huff_evm_contract](https://github.com/0prob/solidity_and_huff_evm_contract) | Huff `ArbExecutor` contract (deploy + ABI) |
