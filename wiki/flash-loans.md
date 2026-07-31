---
created: 2026-07-30
updated: 2026-07-30
sources:
  - raw/README.md
  - raw/flash_tokens.md
---

# Flash Loans

Provider selection is **per cycle** via `FLASH_LOAN_SOURCE` (default `auto`). Liquidity is measured, never invented: Balancer vault ERC20 balances and Aave aToken underlying balances refresh into `FlashLiquidityCache`.

## Entrypoints (Huff executor)

| Entrypoint | When |
|------------|------|
| `executeArbDirect` | Pure Balancer V2 — one Vault `batchSwap` flash-swap |
| `executeArb` | Non-Balancer funded by Balancer `flashLoan` (callback rejects Vault hops) |
| `executeArbWithAave` | Mixed Balancer hops or Aave funder — Polygon **Aave V3** `flashLoanSimple` |
| `executeArbWithDodo` | **Disabled** (`DODO_EXTERNAL_FLASH_ENABLED = false` in code) |

## Hard constraints

- Balancer Vault flash and Vault swaps share `nonReentrant` → mixed Balancer routes must borrow from Aave.
- Aave pulls `amount + premium` after `executeOperation`; `minProfit` checked post-pull when flash token == profit token.
- Fees live-fetched: Aave `FLASHLOAN_PREMIUM_TOTAL`; Balancer protocol flash fee (often 0 on Polygon).
- Aave V4 not used on Polygon for this bot.

## Env

`FLASH_LOAN_SOURCE=auto|balancer|balancer_only|aave|aave_v3`  
`MAX_FLASH_LOAN_USD` hard ceiling; adaptive per-route caps start lower and rise after confirmed fills.

## Related

- [[profit-gate]]
- [[execution-path]]
- [[oracle-pricing]]
