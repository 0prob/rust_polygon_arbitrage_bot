use alloy::primitives::U256;
use alloy::primitives::{Address, address};

/// Hard cap on hop count during cycle search (independent of config `max_hops`).
pub const HOP_CAP: u32 = 8;
pub const HOP_CAP_USIZE: usize = HOP_CAP as usize;
/// Default max hops for hub-path token→WMATIC base rates (`OracleConfig`).
pub const DEFAULT_HUB_PATH_MAX_HOPS: u32 = 4;
/// Maximum tokens per pool metadata row (Curve/Balancer upper bound in this bot).
pub const MAX_POOL_TOKENS: usize = 8;

/// Structural nonzero-liquidity floor (non-V2). Decimal- and price-aware economic
/// floors are applied before simulation and execution.
pub const MIN_HOP_TOKEN_BALANCE: U256 = U256::ONE;

/// Absolute wei floor on **either** Uniswap V2 reserve for graph routing and HF
/// dust cull. Alias: [`crate::pipeline::local_sim::V2_DUST_RESERVE_WEI`].
/// 1e8 ≈ 100 units of a 6dp stable / dust for 18dp tokens — keeps real stables,
/// drops junk that used to fill the cycle snap (`v2_dead_skip` 90%+ of HF filter).
pub const V2_MIN_RESERVE_WEI: u64 = 100_000_000;
pub const V2_MIN_RESERVE: U256 = U256::from_limbs([V2_MIN_RESERVE_WEI, 0, 0, 0]);

/// UniV2-style fee denominator (997/1000 = 30 bps).
pub const FEE_DENOMINATOR: U256 = U256::from_limbs([1000, 0, 0, 0]);
/// Basis-point scale (10_000 = 100%).
pub const BPS_SCALE: U256 = U256::from_limbs([10_000, 0, 0, 0]);
/// Default UniV2 fee numerator with [`FEE_DENOMINATOR`].
pub const DEFAULT_FEE_NUMERATOR: U256 = U256::from_limbs([997, 0, 0, 0]);

/// Polygon mainnet chain id.
pub const POLYGON_CHAIN_ID: u64 = 137;

/// Canonical Balancer Polygon deployment artifact used to validate addresses.
pub const BALANCER_DEPLOYMENTS_POLYGON: &str =
    "https://github.com/balancer/balancer-deployments/blob/master/addresses/polygon.json";

/// Balancer V2 vault on Polygon mainnet.
pub const BALANCER_VAULT: Address = address!("0xba12222222228d8ba445958a75a0704d566bf2c8");
/// Woofi router v2 on Polygon mainnet.
pub const WOOFI_ROUTER_V2: Address = address!("0x4c4af8dbc524681930a27b2f1af5bcc8062e6fb7");
/// Uniswap v4 PoolManager on Polygon mainnet.
pub const UNISWAP_V4_POOL_MANAGER: Address = address!("0x67366782805870060151383f4bbff9dab53e5cd6");
/// Multicall3 canonical deployment.
pub const MULTICALL3: Address = address!("0xcA11bde05977b3631167028862bE2a173976CA11");
/// Aave V3 Pool on Polygon mainnet (flash-loan liquidity checks).
pub const AAVE_V3_POOL: Address = address!("0x794a61358D6845594F94dc1DB02A252b5b4814aD");
/// Uniswap V3 TickLens on Polygon.
pub const TICK_LENS_POLYGON: Address = address!("0xbfd8137f7d1516D3ea5cA83523914859ec47F573");

// ─── Polygon ERC-20 hubs (never put routers/contracts here) ─────────────────

/// Wrapped MATIC on Polygon.
pub const WMATIC: Address = address!("0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270");
/// Bridged USDC.e on Polygon (PoS).
pub const USDC_E: Address = address!("0x2791bca1f2de4661ed88a30c99a7a9449aa84174");
/// Native USDC on Polygon.
pub const USDC_NATIVE: Address = address!("0x3c499c542cef5e3811e1192ce70d8cc03d5c3359");
/// Bridged USDT on Polygon.
pub const USDT: Address = address!("0xc2132d05d31c914a87c6611c10748aeb04b58e8f");
/// Bridged WETH on Polygon.
pub const WETH: Address = address!("0x7ceb23fd6bc0add59e62ac25578270cff1b9f619");
/// Bridged WBTC on Polygon.
pub const WBTC: Address = address!("0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6");
/// Bridged DAI on Polygon.
pub const DAI: Address = address!("0x8f3cf7ad23cd3cadbd9735aff958023239c6a063");
/// Chainlink LINK on Polygon.
pub const LINK: Address = address!("0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39");
/// Aave AAVE on Polygon.
pub const AAVE: Address = address!("0xd6df932a45c0f255f85145f286ea0b292b21c90b");
/// Curve CRV on Polygon.
pub const CRV: Address = address!("0x172370d5cd63279efa6d502dab29171933a610af");
/// SushiToken (ERC-20) on Polygon — **not** the SushiSwap router.
pub const SUSHI: Address = address!("0x0b3f868e0be5597d5db7feb59e1cadbb0fdda50a");
/// Balancer BAL on Polygon.
pub const BAL: Address = address!("0x9a71012b13ca4d3d0cdc72a177df3ef03b0e76a3");
/// The Sandbox SAND on Polygon.
pub const SAND: Address = address!("0xbbba073c31bf03b8acf7c28ef0738decf3695683");
/// Decentraland MANA on Polygon.
pub const MANA: Address = address!("0xa1c57f48f0deb89f569dfbe6e2b7f46d33606fd4");
/// Uniswap UNI on Polygon.
pub const UNI: Address = address!("0xb33eaad8d922b1083446dc23f610c2567fb5180f");
/// The Graph GRT on Polygon.
pub const GRT: Address = address!("0x5fe2b58c013d7601147dcdd68c143a77499f5531");
/// Aavegotchi GHST on Polygon.
pub const GHST: Address = address!("0x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7");
/// Bridged wstETH on Polygon (Aave market mint).
pub const WST_ETH: Address = address!("0x03b54a6e9a984069379fae1a4fc4dbae93b3bccd");
/// Compound COMP on Polygon.
pub const COMP: Address = address!("0x8505b9d2254a7ae468c0e9dd10ccea3a837aef5c");
/// Synthetix SNX on Polygon.
pub const SNX: Address = address!("0x50b728d8d964fd00c2d0aad81718b71311fef68a");
/// QuickSwap QUICK (legacy) on Polygon.
pub const QUICK: Address = address!("0x831753dd7087cac61ab5644b308642cc1c33dc13");
/// Lido stMATIC on Polygon (LST path rates).
pub const ST_MATIC: Address = address!("0x3a58a54c066fdc0f2d55fc9c89f0415c92ebf3c4");
/// Stader MaticX on Polygon.
pub const MATIC_X: Address = address!("0xfa68fb4628dff1028cfec22b4162fccd0d45efb6");

/// Oracle-priced hub tokens on Polygon (flash/path seeds, LF prefetch, graph hubs).
///
/// **Must be ERC-20s only** — routers/vaults corrupt hub-path rates and flash
/// eligibility (was: SushiSwap router `0x1b02…` and corrupted SAND/GRT digests).
pub const POLYGON_HUB_TOKENS: [Address; 20] = [
    WMATIC,
    USDC_E,
    USDC_NATIVE,
    USDT,
    WETH,
    WBTC,
    DAI,
    LINK,
    AAVE,
    CRV,
    SUSHI,
    BAL,
    SAND,
    MANA,
    UNI,
    GRT,
    GHST,
    WST_ETH,
    COMP,
    SNX,
];

#[inline]
#[must_use]
pub fn is_polygon_hub_token(addr: Address) -> bool {
    POLYGON_HUB_TOKENS.contains(&addr)
}

/// Bridged USDC.e and native USDC share oracle feeds; meta/realign may treat them
/// as the same leg. Hop continuity stays address-strict (distinct ERC-20s).
#[inline]
#[must_use]
pub fn is_polygon_usd_stable(addr: Address) -> bool {
    addr == USDC_E || addr == USDC_NATIVE
}

/// True when addresses are identical or both Polygon USD stables (USDC.e / native).
#[inline]
#[must_use]
pub fn polygon_usd_stable_equivalent(a: Address, b: Address) -> bool {
    a == b || (is_polygon_usd_stable(a) && is_polygon_usd_stable(b))
}

/// Fee precision for per-gas-amount fee computation (1e6).
pub const FEE_PIPS_SCALE: U256 = U256::from_limbs([1_000_000, 0, 0, 0]);

/// Oracle rate precision: MATIC wei per whole token unit.
/// Must equal [`crate::core::math::fixed_point::ONE`] (1e18).
pub const RATE_PRECISION: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);
/// Largest ERC-20 precision accepted for execution metadata.
/// The on-chain enrichment path shares this bound; higher values are rejected rather than scaled.
pub const MAX_SUPPORTED_TOKEN_DECIMALS: u8 = 30;
/// Reject opportunities when the token/MATIC rate rounds to zero or is untrustworthy.
pub const MIN_TOKEN_TO_MATIC_RATE: U256 = U256::from_limbs([1_000_000, 0, 0, 0]);
/// Minimum borrow size expressed as ~0.1 MATIC of notional (wei, 18 decimals).
/// Lowered to surface more marginal-but-positive routes that pass min_profit gate.
pub const MIN_ECONOMIC_VALUE_MATIC_WEI: u128 = 10u128.pow(17);
/// Reject simulations whose gross profit exceeds this ROI (bps of `amount_in`).
/// Floor probes can show >100% ROI on real mispricings; 10× (100_000) let a V4
/// phantom through (`input=1e5`, ~10× → ~3.9 MATIC net) that dry-ran ExternalCallFailed.
/// 5× still allows aggressive dust edges while cutting full-range quote artifacts.
pub const MAX_SANE_PROFIT_RATIO_BPS: u64 = 50_000;
/// Reject simulated net/gross MATIC above this notional (phantom-state guard).
/// 50 POL let multi-hop V4 quotes with modest token ROI through as 4–40 MATIC
/// "profits" that dry-ran `ExternalCallFailed`. Even 2 POL still admitted a
/// ~1.1 MATIC DODO→V4 phantom. Cap at 1 POL: above gas (~0.2) for real near-misses,
/// below typical V4 local-sim artifacts.
pub const MAX_SANE_PROFIT_MATIC_WEI: u128 = 10u128.pow(18);

/// Per-hop minOut / amountOut haircut floor shared by V2, Curve, and Balancer
/// encoders. Assessment must compound this via `effective_slippage_bps` so
/// on-chain `minProfit` is not set above what encode can realize (default
/// config slippage is 0).
/// 100 bps: 50 cleared Direct V2×2 but Balancer-flash V2→V2 hop1 still hit
/// `UniswapV2: K` under reserve drift (parity3). Assess uses the same floor.
pub const EXECUTION_MIN_SLIPPAGE_BPS: u64 = 100;
/// Extra haircut on multi-hop `chain_in` beyond per-hop minOut slip. Exact-pay
/// hops (V3 callback, Curve dx, …) fail hard when prior hop under-delivers;
/// 100 bps alone still left live mid-hop TransferFailed on BRZ/BRLA routes.
pub const EXECUTION_CHAIN_IN_BUFFER_BPS: u64 = 300;

/// Per-hop gas seeds for route simulation (pool swap only — executor glue is
/// [`crate::services::execution::gas::PER_HOP_EXECUTOR_GAS_OVERHEAD`] + route overhead).
/// GasOracle.record_sim_observed calibrates global uplift; per-route fingerprint
/// cache overrides from dry-run / receipts.
/// Live 2×V2 flash ≈250–320k (parity dry-runs); prior 100k/hop → assess 360.8k @ 2 hops
/// and invented ~0.03 MATIC extra shortfall at 285 gwei. 85k keeps margin vs ~250k floor.
pub const GAS_V2_HOP: u32 = 85_000;
/// Live Uni V3 single swap ~130–160k; 170k was top-of-band and stacked badly on 4-hop.
pub const GAS_V3_BASE: u32 = 155_000;
pub const GAS_V4_BASE: u32 = 170_000;
pub const GAS_CURVE_HOP: u32 = 250_000;
/// Per-hop seed for mixed/Aave-flash Balancer vault swaps (each hop is a separate call).
/// Reverse from live Aave+BAL×2+Woofi dry-run 942k:
///   (942k − Woofi150 − route_overhead154 − Aave~50) / 2 ≈ 294k per BAL hop.
/// Prior 340k stacked to ~996k on BAL+BAL+V3 (half-cover sticky monopolist) and
/// understated cover by ~16% vs reverse-calc — keep ~300k with a small margin.
pub const GAS_BALANCER_HOP: u32 = 300_000;
/// All-in gas for Direct vault `batchSwap` (`executeArbDirect`, ≤4 hops → one call).
/// Not passed through per-edge ROUTE_EXECUTION_* overhead (that double-counted hops).
/// Prefer [`balancer_direct_batch_gas`] (hop-scaled). This constant is the 2-hop seed
/// used by tests / callers that do not know hop count.
/// Live Direct BAL×2 ~200–220k; flat 300k killed near-miss edges (net 0.054 vs floor 0.083).
pub const GAS_BALANCER_DIRECT_BATCH: u32 = 220_000;

/// Hop-scaled Direct `batchSwap` gas seed for assess/rank (tx limit still buffers).
/// Live: BAL×2 ~200–220k, BAL×3 ~244k. Prior flat 300k overstated 2-hop by ~1.4×.
#[inline]
#[must_use]
pub const fn balancer_direct_batch_gas(hop_count: usize) -> u32 {
    match hop_count {
        0 | 1 => 180_000,
        2 => 220_000,
        3 => 250_000,
        _ => 280_000, // 4-hop Direct cap
    }
}
pub const GAS_DODO_HOP: u32 = 180_000;
/// All-in gas for pure-DODO Balancer-flash routes (no per-hop ROUTE_EXECUTION_* stack).
/// Prefer [`dodo_flash_batch_gas`]. Prior 340k → cover=8672; 300k → cover=9791 still
/// short ~6k gas after 199-bps slip at ~282 gwei. 293k clears assess so dry-run can
/// calibrate; receipt/`record_route_gas` lifts if real gas is higher.
pub const GAS_DODO_FLASH_BATCH: u32 = 293_000;

/// Hop-scaled all-in gas for pure-DODO flash routes (assess/rank; tx limit still buffers).
/// Anchored near 2×V2 flash (~310k) for PMM; 340k overstated and quarantined a
/// 86.7% cover edge on first strike. Dry-run / receipt `record_route_gas` lifts
/// cold seeds if real gas is higher.
#[inline]
#[must_use]
pub const fn dodo_flash_batch_gas(hop_count: usize) -> u32 {
    match hop_count {
        0 | 1 => 220_000,
        2 => 293_000,
        3 => 350_000,
        _ => 410_000, // 4-hop cap
    }
}
pub const GAS_WOOFI_HOP: u32 = 150_000;
/// Per-tick-crossed gas increment for V3/V4 pools (~15–20k on-chain; 28k was loose).
pub const GAS_PER_TICK_CROSSED: u32 = 20_000;

/// LF graph attach / arena append batch size (growth catch-up per tick).
/// 768 lagged when eligible jumped ~700/LF (live: hit_cap missing_after=629 at
/// ~21k eligible). 1024 clears a full LF bootstrap wave without rebuild thrash
/// (CPU-only, not RPC).
pub const ATTACH_BATCH_CAP: usize = 1024;
/// Full arena rebuild ingest cap (remainder appends on later LF ticks).
/// Keep ≥ ATTACH_BATCH_CAP × 3 so arena stays ahead of multi-tick attach catch-up.
pub const ARENA_REBUILD_CAP: usize = 4096;

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;
    use rustc_hash::FxHashSet;

    #[test]
    fn polygon_constants_are_distinct() {
        assert_ne!(BALANCER_VAULT, WOOFI_ROUTER_V2);
        assert_ne!(USDC_E, USDC_NATIVE);
        assert!(is_polygon_usd_stable(USDC_E));
        assert!(is_polygon_usd_stable(USDC_NATIVE));
        assert!(polygon_usd_stable_equivalent(USDC_E, USDC_NATIVE));
        assert!(!polygon_usd_stable_equivalent(USDC_E, WMATIC));
    }

    #[test]
    fn hub_token_list_has_unique_erc20s_not_routers() {
        let mut seen = FxHashSet::default();
        for &hub in &POLYGON_HUB_TOKENS {
            assert!(seen.insert(hub), "duplicate hub token {hub}");
            assert_ne!(hub, BALANCER_VAULT, "vault is not a hub token");
            assert_ne!(hub, WOOFI_ROUTER_V2, "router is not a hub token");
            assert_ne!(hub, AAVE_V3_POOL, "pool is not a hub token");
            assert_ne!(hub, MULTICALL3, "multicall is not a hub token");
            // Historical bug: SushiSwap router was listed as a "hub token".
            assert_ne!(
                hub,
                address!("0x1b02da8cb0d097eb8d57a175b88c7d8b47997506"),
                "SushiSwap router must not be a hub token"
            );
        }
        assert_eq!(POLYGON_HUB_TOKENS.len(), 20);
        assert!(is_polygon_hub_token(WMATIC));
        assert!(is_polygon_hub_token(SAND));
        assert!(is_polygon_hub_token(GRT));
        assert!(is_polygon_hub_token(WST_ETH));
        assert!(is_polygon_hub_token(COMP));
        assert!(is_polygon_hub_token(SNX));
    }

    #[test]
    fn rate_precision_matches_fixed_point_one() {
        assert_eq!(RATE_PRECISION, crate::core::math::fixed_point::ONE);
    }

    #[test]
    fn v2_min_reserve_matches_local_sim_alias() {
        assert_eq!(
            V2_MIN_RESERVE_WEI,
            crate::pipeline::local_sim::V2_DUST_RESERVE_WEI
        );
        assert_eq!(V2_MIN_RESERVE, U256::from(V2_MIN_RESERVE_WEI));
        assert!(V2_MIN_RESERVE > MIN_HOP_TOKEN_BALANCE);
    }

    #[test]
    fn fee_scales_are_consistent() {
        assert_eq!(FEE_DENOMINATOR, U256::from(1000u64));
        assert_eq!(BPS_SCALE, U256::from(10_000u64));
        assert_eq!(DEFAULT_FEE_NUMERATOR, U256::from(997u64));
        assert!(DEFAULT_FEE_NUMERATOR < FEE_DENOMINATOR);
        const { assert!(MAX_SANE_PROFIT_RATIO_BPS > 10_000) };
        assert_eq!(EXECUTION_MIN_SLIPPAGE_BPS, 100);
        assert_eq!(EXECUTION_CHAIN_IN_BUFFER_BPS, 300);
    }

    #[test]
    fn hop_cap_matches_usize_mirror() {
        assert_eq!(HOP_CAP_USIZE, HOP_CAP as usize);
        const { assert!(DEFAULT_HUB_PATH_MAX_HOPS <= HOP_CAP) };
        assert_eq!(MAX_POOL_TOKENS, 8);
    }
}
