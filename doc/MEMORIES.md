# Bug Memory

| Bug | Status | Recorded |
|-----|--------|----------|
| `service.rs` receipt timeout: recovery `Mined` + failed second receipt poll left nonce in-flight, blocking all later submits until resync | fixed | 2026-06-22 08:50 |
| `ternary.rs` `get_dynamic_search_bounds`: min-width guard could push `out_high` above flash liquidity cap, oversizing capped re-optimize | fixed | 2026-06-22 08:50 |
| `recovery.rs` and `service.rs`: `Mined` returned receipt found on submit_provider, but second poll used sim_provider — different RPC, receipt lost; `recovery.rs` `unwrap_or(0)` on fee capped cancel tx at zero | fixed | 2026-06-22 12:00 |
| `profit.rs` `net_profit_after_gas_from_sim`: safety_multiplier_bps hardcoded to 0 (used DEFAULT 30000) ignoring configured value, causing Brent optimizer to use wrong objective | fixed | 2026-06-22 12:00 |
| Duplicate `FEE_PIPS_SCALE` constant in `swap_math.rs` and `uniswap_v3.rs` — centralized to `core/constants.rs` | fixed | 2026-06-22 12:00 |
| `graph_cache.rs`/`lf.rs`: `rescore_cached_cycles` reused stale cycle enumeration after reserve-only updates, skipping re-discovery of routes that enter the capped set | fixed | 2026-06-23 12:00 |
| `graph_cache.rs` `get_or_rescore_graph`: in-place rescore updated `state_generation` before `needs_cycle_refind`, so stale cycles were served on reserve-only LF ticks | fixed | 2026-06-24 18:23 |
