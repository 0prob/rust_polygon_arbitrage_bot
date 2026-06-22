use alloy::primitives::{Address, address};
use ruint::aliases::U256;

/// Minimum raw token balance for a hop to be routable (dust guard).
pub const MIN_HOP_TOKEN_BALANCE: U256 = U256::from_limbs([1_000_000_000_000_000, 0, 0, 0]); // 1e15

pub const FEE_DENOMINATOR: U256 = U256::from_limbs([1000, 0, 0, 0]);
pub const BPS_SCALE: U256 = U256::from_limbs([10_000, 0, 0, 0]);
pub const DEFAULT_FEE_NUMERATOR: U256 = U256::from_limbs([997, 0, 0, 0]);

/// Polygon mainnet chain id.
pub const POLYGON_CHAIN_ID: u64 = 137;

/// Balancer V2 vault (same address on Polygon mainnet).
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
/// MATIC/USD Chainlink feed on Polygon.
pub const CHAINLINK_MATIC_USD: Address = address!("0xAB594600376Ec9fD91F8e885dADF0CE036862dE0");

/// Fee precision for per-gas-amount fee computation (1e6).
pub const FEE_PIPS_SCALE: U256 = U256::from_limbs([1_000_000, 0, 0, 0]);

/// Oracle rate precision (MATIC wei per token smallest unit, 1e18 scaled).
pub const RATE_PRECISION: U256 = U256::from_limbs([1_000_000_000_000_000_000, 0, 0, 0]);
/// Reject opportunities when the token/MATIC rate rounds to zero or is untrustworthy.
pub const MIN_TOKEN_TO_MATIC_RATE: U256 = U256::from_limbs([1_000_000, 0, 0, 0]);
