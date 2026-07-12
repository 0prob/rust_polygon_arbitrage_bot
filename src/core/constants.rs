use alloy::primitives::U256;
use alloy::primitives::{Address, address};

/// Hard cap on hop count during cycle search (independent of config `max_hops`).
pub const HOP_CAP: u32 = 8;
pub const HOP_CAP_USIZE: usize = HOP_CAP as usize;
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

/// Oracle-priced hub tokens on Polygon.
pub const POLYGON_HUB_TOKENS: [Address; 20] = [
    WMATIC,
    address!("0x2791bca1f2de4661ed88a30c99a7a9449aa84174"),
    address!("0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"),
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

/// Fee precision for per-gas-amount fee computation (1e6).
pub const FEE_PIPS_SCALE: U256 = U256::from_limbs([1_000_000, 0, 0, 0]);

/// Oracle rate precision (MATIC wei per token smallest unit, 1e18 scaled).
pub const RATE_PRECISION: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);
/// Reject opportunities when the token/MATIC rate rounds to zero or is untrustworthy.
pub const MIN_TOKEN_TO_MATIC_RATE: U256 = U256::from_limbs([1_000_000, 0, 0, 0]);
/// Minimum borrow size expressed as ~0.1 MATIC of notional (wei, 18 decimals).
/// Lowered to surface more marginal-but-positive routes that pass min_profit gate.
pub const MIN_ECONOMIC_VALUE_MATIC_WEI: u128 = 10u128.pow(17);
/// Reject simulations whose gross profit exceeds this ROI (bps of `amount_in`).
/// Raised from 10_000 to 100_000 because probes at the economic floor (~0.001 token)
/// can legitimately show >100% ROI on a real mispricing — the sanity gate should
/// catch phantom-state artifact, not small-probe high-multiplier returns.
pub const MAX_SANE_PROFIT_RATIO_BPS: u64 = 100_000;
/// Reject simulated gross profit above this MATIC notional (phantom-state guard).
/// Raised from 1 → 50 POL to accommodate small-cap probe returns that are still
/// small relative to the configured USD flash loan cap ($3.75 vs $50k notional).
pub const MAX_SANE_PROFIT_MATIC_WEI: u128 = 50u128 * 10u128.pow(18);

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
pub const GAS_BALANCER_HOP: u32 = 1_000_000;
/// Single vault `batchSwap` for `executeArbDirect` (≤4 hops collapse to one call).
/// Calibrated ~720k sim / 1.87M live on Polygon; HF ranking uses this + overhead (~800k for 2-hop).
pub const GAS_BALANCER_DIRECT_BATCH: u32 = 580_000;
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
    }
}
