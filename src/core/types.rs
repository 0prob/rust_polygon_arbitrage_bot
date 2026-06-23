use ruint::aliases::U256;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// Stack-backed edge list for typical ≤6-hop arb routes.
pub type CycleEdges = SmallVec<[Edge; 6]>;

/// Stack-backed hop amount buffer for typical ≤6-hop simulation traces.
pub type HopAmounts = SmallVec<[U256; 7]>;

use crate::core::constants::MIN_HOP_TOKEN_BALANCE;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TokenIndex(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PoolIndex(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProtocolType {
    UniswapV2,
    UniswapV3,
    UniswapV4,
    BalancerV2,
    CurveStable,
    CurveCrypto,
    Dodo,
    Woofi,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Edge {
    pub pool_index: PoolIndex,
    pub token_in: TokenIndex,
    pub token_out: TokenIndex,
    pub token_in_idx: u8,
    pub token_out_idx: u8,
    pub protocol: ProtocolType,
    pub fee_bps: u32,
    pub zero_for_one: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V3Tick {
    pub tick: i32,
    pub liquidity_gross: u128,
    pub liquidity_net: i128,
}

#[derive(Debug, Clone)]
pub struct V2PoolState {
    pub reserve0: U256,
    pub reserve1: U256,
    pub fee: U256,
    pub fee_denominator: U256,
}

#[derive(Debug, Clone)]
pub struct ConcentratedLiquidityPoolState {
    pub sqrt_price_x96: U256,
    pub tick: i32,
    pub liquidity: u128,
    pub fee: U256,
    pub tick_spacing: i32,
    pub ticks: Box<[V3Tick]>,
}

pub type V3PoolState = ConcentratedLiquidityPoolState;
pub type V4PoolState = ConcentratedLiquidityPoolState;

#[derive(Debug, Clone)]
pub struct CurvePoolState {
    pub balances: Vec<U256>,
    pub a: U256,
    pub fee: U256,
    pub rates: Vec<U256>,
    pub n_coins: u8,
    pub gamma: Option<U256>,
    pub d: Option<U256>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BalancerPoolKind {
    Weighted,
    Stable,
}

#[derive(Debug, Clone)]
pub struct BalancerPoolState {
    pub balances: Vec<U256>,
    pub weights: Vec<U256>,
    pub scaling_factors: Vec<U256>,
    pub amp: U256,
    pub amp_precision: U256,
    pub fee: U256,
    pub pool_type: BalancerPoolKind,
    pub bpt_index: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct DodoPoolState {
    pub base_reserve: U256,
    pub quote_reserve: U256,
    pub base_target: U256,
    pub quote_target: U256,
    pub i: U256,
    pub k: U256,
    pub r_status: u8,
    pub lp_fee_rate: U256,
    pub mt_fee_rate: U256,
}

#[derive(Debug, Clone)]
pub struct WoofiBaseTokenState {
    pub price: U256,
    pub spread: U256,
    pub coeff: U256,
    pub reserve: U256,
    pub base_dec: U256,
    pub quote_dec: U256,
    pub price_dec: U256,
    pub fee_rate: U256,
    pub max_gamma: U256,
    pub max_notional_swap: U256,
}

#[derive(Debug, Clone)]
pub struct WoofiPoolState {
    pub quote_reserve: U256,
    pub base_states: Vec<WoofiBaseTokenState>,
    pub fee: U256,
}

#[derive(Debug, Clone)]
pub enum PoolState {
    Invalid,
    V2(V2PoolState),
    V3(V3PoolState),
    V4(V4PoolState),
    Curve(CurvePoolState),
    Balancer(BalancerPoolState),
    Dodo(DodoPoolState),
    Woofi(WoofiPoolState),
}

impl PoolState {
    pub fn is_tradable(&self) -> bool {
        match self {
            PoolState::Invalid => false,
            PoolState::V2(s) => !s.reserve0.is_zero() && !s.reserve1.is_zero(),
            PoolState::V3(s) => s.liquidity > 0 && !s.sqrt_price_x96.is_zero(),
            PoolState::V4(s) => s.liquidity > 0 && !s.sqrt_price_x96.is_zero(),
            PoolState::Curve(s) => {
                s.balances
                    .iter()
                    .filter(|b| **b >= MIN_HOP_TOKEN_BALANCE)
                    .count()
                    >= 2
            }
            PoolState::Balancer(s) => {
                let bpt = s.bpt_index;
                s.balances
                    .iter()
                    .enumerate()
                    .filter(|(i, b)| bpt != Some(*i) && **b >= MIN_HOP_TOKEN_BALANCE)
                    .count()
                    >= 2
            }
            PoolState::Dodo(s) => !s.base_reserve.is_zero() && !s.quote_reserve.is_zero(),
            PoolState::Woofi(s) => {
                !s.quote_reserve.is_zero() && s.base_states.iter().any(|b| !b.reserve.is_zero())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct FoundCycle {
    pub start_token: TokenIndex,
    pub edges: CycleEdges,
    pub hop_count: u32,
    pub log_weight: f64,
    pub cumulative_fee_bps: u32,
    pub score: f64,
}

#[derive(Debug, Clone)]
pub struct RouteSimulationResult {
    pub amount_in: U256,
    pub amount_out: U256,
    pub profit: U256,
    pub profitable: bool,
    pub hop_amounts: HopAmounts,
    pub total_gas: u32,
    pub hop_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlashLoanSource {
    Balancer,
    AaveV3,
}

#[derive(Debug, Clone)]
pub struct ProfitAssessment {
    pub should_execute: bool,
    pub gross_profit: U256,
    pub gas_cost_wei: U256,
    pub gas_cost_in_tokens: U256,
    pub flash_loan_fee: U256,
    pub slippage_deduction: U256,
    pub revert_penalty: U256,
    pub net_profit: U256,
    pub net_profit_after_gas: U256,
    pub net_profit_after_gas_matic_wei: U256,
    pub roi: f64,
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EvaluatedRoute {
    pub cycle: FoundCycle,
    pub result: RouteSimulationResult,
    pub assessment: Option<ProfitAssessment>,
    /// Effective slippage used at eval time (config + depth impact).
    pub effective_slippage_bps: u64,
}
