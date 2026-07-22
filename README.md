> [!CAUTION]
> **CRITICAL WARNING: Work in Progress**
> This bot is actively under development. Running this software with real capital carries a severe risk of **permanent loss of funds**. Use at your own risk.

# rpbot

Polygon mainnet MEV arbitrage bot. Discovers pools from an Envio/HyperIndex indexer, builds a multi-protocol routing graph, finds profitable cycles, sizes inputs with golden-section (Brent) search, simulates swaps locally, and executes via a Huff flash-loan executor contract ([`0prob/solidity_and_huff_evm_contract`](https://github.com/0prob/solidity_and_huff_evm_contract)).

## Features

- **Dual-frequency loop** — LF pass (code default 1s): pool discovery, state refresh, graph build, cycle enumeration. HF pass (code default 150ms): prefetch, Brent input sizing, local simulation, dry-run or live execution.
- **Multi-protocol routing** — Uniswap V2/V3/V4 (hookless pools via `unlock`/`unlockCallback`), QuickSwap Algebra V3/Integral, Balancer V2, Curve (stable & crypto), DODO, WooFi.
- **Cycle search** — Hybrid parallel DFS + Bellman-Ford (default), or `dfs` / `bellman-ford` alone (`johnson` is an env alias for Bellman-Ford); spot-weighted adjacency graph, atomic probe prefilter, graph/cycle caching.
- **Pool discovery** — PostgreSQL direct SQL feed from HyperIndex; periodic refresh and dead-pool pruning. Optional V2 protocol toggles: `QUICKSWAP_V2_ENABLED`, `UNISWAP_V2_ENABLED`, `SUSHISWAP_V2_ENABLED` (unset = enabled).
- **State refresh** — Archival RPC multicall for reserves, V3 ticks (TickLens), V4 storage slots, and protocol-specific fields.
- **Profit scoring** — Hop simulation uses in-memory pool state; base pricing uses an LF snapshot (`token_to_matic_rates`) from hub-path arena sim (token → WMATIC/WPOL; enabled by default in `OracleConfig`) plus Chainlink/Pyth for configured feeds and POL/USD caps. Gas oracle, live flash-loan fees, depth impact + optional static slippage, circuit breaker. Min profit is MATIC-denominated (`MIN_PROFIT_MATIC_WEI`; code default 0.01 MATIC).
- **Learned route risk** — Per-route success/failure history at `ROUTE_STATS_PATH` (default `.rpbot-route-stats.json`); unreliable routes need proportionally more expected net profit before preflight.
- **Flash-loan routing** — `FLASH_LOAN_SOURCE=auto` (default) picks a provider per cycle from live liquidity + Vault reentrancy rules (see [Flash loans](#flash-loans)). Aave V3 premium and Balancer protocol flash fee are RPC-refreshed.
- **Execution** — Dry-run simulation or live submit via Huff `ArbExecutor`; optional MEV-protected `PRIVATE_RPC_URL` and/or bloXroute (`BLOXROUTE_AUTH_HEADER`), profit-scaled priority fees, nonce management, route cooldown/quarantine, receipt polling.
- **HyperSync** (optional) — Block head feed and receipt lookups when `ENVIO_API_TOKEN` is set.
- **TUI dashboard** (optional) — Ratatui UI behind `--features tui`; headless `rpbot` is the production path.

## Binaries

| Binary | Purpose |
|---|---|
| `rpbot` | Main bot (default, headless) |
| `tui` | Optional terminal dashboard (`cargo run --release --features tui --bin tui`) |

## Prerequisites

- **Rust nightly** — crate uses edition 2024 (verify with `rustc --version`).
- **Polygon RPC** — archival endpoint recommended for pool-state reads (`STATE_RPC_URL` / `POLYGON_RPC_URLS`); separate `EXECUTION_RPC` for HF `eth_call` / gas / receipts.
- **Envio indexer** — PostgreSQL from [`0prob/polygon_envio_hyperindex`](https://github.com/0prob/polygon_envio_hyperindex) (`PG_URL`; code default `postgres://postgres@localhost:5433/envio-dev`).
- **Live execution** — deployed Huff executor from [`0prob/solidity_and_huff_evm_contract`](https://github.com/0prob/solidity_and_huff_evm_contract) (Foundry + `huffc`; `OWNER` + `PRIVATE_KEY` for deploy).

## Setup

Start the HyperIndex discovery feed:

```bash
git clone https://github.com/0prob/polygon_envio_hyperindex.git
cd polygon_envio_hyperindex
bun install && cp .env.example .env   # first time only — fill ENVIO_API_TOKEN + RPC URLs
bun run dev
```

Configure the bot (this repo):

```bash
cp .env.example .env
# Edit .env — full variable reference is in .env.example (paired with src/config/mod.rs)
# Point PG_URL at the HyperIndex Postgres from the indexer above
```

**Dry-run minimum**

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
```

Unmapped tokens accumulate at runtime; every 20 **new** addresses trigger a Hermes USD-spot scan. Verified feeds are registered and persisted to `target/run-logs/oracle-auto-feeds.json` (override with `RPBOT_ORACLE_AUTO_FEEDS`); misses are marked `no_feed` so they are not rescanned.

Help: `cargo run -- --help` (or `rpbot --help` after build). Concurrent `rpbot`/`tui` processes are killed at startup unless `RPBOT_ALLOW_MULTIPLE` is set.

## Development

```bash
cargo test
cargo bench --bench routing   # v2/v3 swap, route sim, graph rescore, cycle find, optimize
```

Integration tests live under `tests/` (`oracle_feed_proposal_test`, `oracle_live_test`). Clippy deny-list includes `unwrap_used`, `panic`, `todo`, and `unimplemented` (see `Cargo.toml` / `clippy.toml`).

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

Stream is **off by default** (`STREAM_ENABLED` code default `false`). Set `STREAM_ENABLED=true` and configure `POLYGON_WSS_URLS` or `WSS_URL` (HTTP→WSS conversion from state RPCs is unreliable on many providers). Live submits should not use the public execution RPC for mempool injection — use `PRIVATE_RPC_URL` or `BLOXROUTE_AUTH_HEADER`.

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

## Project layout (docs)

| Path | Content |
|---|---|
| `.env.example` | Environment reference (code defaults + example overrides; maps to `src/config/mod.rs`) |
| `src/config/mod.rs` | `AppConfig`, defaults, `env_key_to_figment_path`, validation |
| `sentinel/` | Optional research notifier (cross-chain atomicity watch); see `sentinel/README.md` |

Regenerable analysis output (`docs/`, `graphify-out/`) is gitignored.

## Related repositories

| Repository | Role |
|---|---|
| [0prob/rust_polygon_arbitrage_bot](https://github.com/0prob/rust_polygon_arbitrage_bot) | This bot: routing, simulation, execution |
| [0prob/polygon_envio_hyperindex](https://github.com/0prob/polygon_envio_hyperindex) | Envio HyperIndex discovery feed → Postgres (`PG_URL`) |
| [0prob/solidity_and_huff_evm_contract](https://github.com/0prob/solidity_and_huff_evm_contract) | Huff `ArbExecutor` contract (deploy + ABI) |
