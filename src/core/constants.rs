use alloy::primitives::U256;
use alloy::primitives::{Address, address};

/// Hard cap on hop count during cycle search (independent of config `max_hops`).
pub const HOP_CAP: u32 = 8;
pub const HOP_CAP_USIZE: usize = HOP_CAP as usize;
/// Default max hops for hub-path token→WMATIC base rates (`OracleConfig`).
pub const DEFAULT_HUB_PATH_MAX_HOPS: u32 = 4;
/// Maximum tokens per pool metadata row (Curve/Balancer upper bound in this bot).
pub const MAX_POOL_TOKENS: usize = 8;

/// Structural nonzero-liquidity floor. Decimal- and price-aware economic floors
/// are applied before simulation and execution.
pub const MIN_HOP_TOKEN_BALANCE: U256 = U256::ONE;

pub const FEE_DENOMINATOR: U256 = U256::from_limbs([1000, 0, 0, 0]);
pub const BPS_SCALE: U256 = U256::from_limbs([10_000, 0, 0, 0]);
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
/// Wrapped MATIC on Polygon.
pub const WMATIC: Address = address!("0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270");
/// Bridged USDC.e on Polygon (PoS).
pub const USDC_E: Address = address!("0x2791bca1f2de4661ed88a30c99a7a9449aa84174");
/// Native USDC on Polygon.
pub const USDC_NATIVE: Address = address!("0x3c499c542cef5e3811e1192ce70d8cc03d5c3359");

/// Oracle-priced hub tokens on Polygon.
pub const POLYGON_HUB_TOKENS: [Address; 20] = [
    WMATIC,
    USDC_E,
    USDC_NATIVE,
    address!("0xc2132d05d31c914a87c6611c10748aeb04b58e8f"),
    address!("0x7ceb23fd6bc0add59e62ac25578270cff1b9f619"),
    address!("0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6"),
    address!("0x8f3cf7ad23cd3cadbd9735aff958023239c6a063"),
    address!("0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39"),
    address!("0xd6df932a45c0f255f85145f286ea0b292b21c90b"),
    address!("0x172370d5cd63279efa6d502dab29171933a610af"),
    address!("0x0b3f868e0be5597d5db7feb59e1cadbb0fdda50a"),
    address!("0x9a71012b13ca4d3d0cdc72a177df3ef03b0e76a3"),
    address!("0xbbba073c31bf03b8acf7c28ef0738decf2b0bcee"),
    address!("0xa1c57f48f0deb89f569dfbe6e2b7f46d33606fd4"),
    address!("0xb33eaad8d922b1083446dc23f610c2567fb5180f"),
    address!("0x5fe2b58a29225b59dadf811f5c49472a056ebff0"),
    address!("0x1b02da8cb0d097eb8d57a175b88c7d8b47997506"),
    address!("0x9c2c5fd7b9e403564dc385c89d647e8bd6566614"),
    address!("0x53a0b3a00de21b8cf755f75ed53af39ecd158171"),
    address!("0xc9e3f325b6e02f3ca7e3ae0f329aee1014537c14"),
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
/// Convert base-unit amounts by dividing by `10^token_decimals`.
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

/// Per-hop gas seeds for route simulation (Polygon executor context).
/// GasOracle.record_sim_observed calibrates global uplift; per-route
/// fingerprint cache overrides from dry-run / receipts. Re-tune here when
/// median sim/observed drift exceeds ~20% for a protocol.
/// Recent: 1.87M actual vs 720k sim on dispatch -> uplift ~2.6x observed; raise bases.
pub const GAS_V2_HOP: u32 = 110_000;
pub const GAS_V3_BASE: u32 = 200_000;
pub const GAS_V4_BASE: u32 = 220_000;
pub const GAS_CURVE_HOP: u32 = 270_000;
/// Per-hop seed for mixed/Aave-flash Balancer vault swaps (each hop is a separate call).
/// parity5 dry-run Aave+BAL×2+Woofi: sim 2.41M vs gas_used 942k — 1M/hop was ~2.5× hot.
pub const GAS_BALANCER_HOP: u32 = 450_000;
/// All-in gas for Direct vault `batchSwap` (`executeArbDirect`, ≤4 hops → one call).
/// Not passed through per-edge ROUTE_EXECUTION_* overhead (that double-counted hops).
/// Live Direct BAL×3 ~244k; 320k ≈ 1.3× (safety assess). Tx gas_limit still has buffer_gas_limit.
pub const GAS_BALANCER_DIRECT_BATCH: u32 = 320_000;
pub const GAS_DODO_HOP: u32 = 200_000;
pub const GAS_WOOFI_HOP: u32 = 160_000;
/// Per-tick-crossed gas increment for V3/V4 pools.
pub const GAS_PER_TICK_CROSSED: u32 = 28_000;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn polygon_constants_are_distinct() {
        assert_ne!(BALANCER_VAULT, WOOFI_ROUTER_V2);
        assert_ne!(USDC_E, USDC_NATIVE);
        assert!(is_polygon_usd_stable(USDC_E));
        assert!(is_polygon_usd_stable(USDC_NATIVE));
        assert!(polygon_usd_stable_equivalent(USDC_E, USDC_NATIVE));
        assert!(!polygon_usd_stable_equivalent(USDC_E, WMATIC));
    }
}
