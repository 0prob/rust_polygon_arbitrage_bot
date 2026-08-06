use alloy::primitives::{Address, U256};
use smallvec::SmallVec;
use std::sync::Arc;

use crate::core::constants::{HOP_CAP, MAX_POOL_TOKENS, MIN_HOP_TOKEN_BALANCE, V2_MIN_RESERVE};

const EDGE_CAP: usize = HOP_CAP as usize;
const HOP_AMOUNT_CAP: usize = EDGE_CAP + 1;
const POOL_TOKEN_CAP: usize = MAX_POOL_TOKENS;

/// Stack-backed edge list for routes up to [`HOP_CAP`] hops.
pub type CycleEdges = SmallVec<[Edge; EDGE_CAP]>;

/// Stack-backed hop amount buffer (`hop_count + 1` slots) for routes up to [`HOP_CAP`] hops.
pub type HopAmounts = SmallVec<[U256; HOP_AMOUNT_CAP]>;

/// Pool token addresses resolved from metadata (inline up to [`MAX_POOL_TOKENS`](crate::core::constants::MAX_POOL_TOKENS)).
pub type PoolTokenAddrs = SmallVec<[Address; POOL_TOKEN_CAP]>;

#[inline]
#[must_use]
pub fn hop_amounts_zeroed(hop_count: usize) -> HopAmounts {
    // SmallVec::new keeps ≤HOP_AMOUNT_CAP on the stack; resize fills zeros.
    let mut amounts = HopAmounts::new();
    amounts.resize(hop_count.saturating_add(1), U256::ZERO);
    amounts
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TokenIndex(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PoolIndex(pub u32);

/// Compact protocol discriminant (fits in one byte; denser `Edge` / graph storage).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ProtocolType {
    UniswapV2 = 0,
    UniswapV3 = 1,
    UniswapV4 = 2,
    BalancerV2 = 3,
    CurveStable = 4,
    CurveCrypto = 5,
    Dodo = 6,
    Woofi = 7,
}

impl ProtocolType {
    /// Round-robin fetch queue slot for [`crate::pipeline::fetcher::FETCHABLE_PROTOCOLS`].
    #[inline]
    #[must_use]
    pub const fn fetch_slot(self) -> Option<usize> {
        // Discriminant order matches the fetch queue (see `#[repr(u8)]` values).
        Some(self as u8 as usize)
    }

    /// Short tag for logs / TUI route viz.
    #[inline]
    #[must_use]
    pub const fn tag(self) -> &'static str {
        match self {
            Self::UniswapV2 => "V2",
            Self::UniswapV3 => "V3",
            Self::UniswapV4 => "V4",
            Self::BalancerV2 => "BAL",
            Self::CurveStable => "CRV-S",
            Self::CurveCrypto => "CRV-C",
            Self::Dodo => "DODO",
            Self::Woofi => "WOOFI",
        }
    }
}

/// Short protocol tag for logs / TUI (see [`ProtocolType::tag`]).
#[inline]
#[must_use]
pub const fn protocol_tag(protocol: ProtocolType) -> &'static str {
    protocol.tag()
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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
    /// Last swap timestamp from getReserves (0 = never swapped).
    pub block_timestamp_last: u32,
}

#[derive(Debug, Clone)]
pub struct ConcentratedLiquidityPoolState {
    pub sqrt_price_x96: U256,
    pub tick: i32,
    pub liquidity: u128,
    pub fee: U256,
    pub tick_spacing: i32,
    pub ticks: Arc<[V3Tick]>,
    /// False = pool is paused (slot0.unlocked == 0).
    pub unlocked: bool,
    /// V3: slot0 protocol-fee nibbles (`uint8`). V4: packed directional protocol fee (`uint24`).
    pub fee_protocol: u32,
    /// Cardinality of oracle observations (0 = never observed, pool likely dead).
    pub observation_cardinality: u16,
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
    /// Pre-computed D invariant for this balance snapshot (see `curve_stable_cache_d`).
    pub d: Option<U256>,
    /// Stable-NG `offpeg_fee_multiplier` (1e10 denom); `None` = classic
    /// CurveStableSwap (static fee scaled by N/(4·(N−1))).
    pub offpeg_fee_multiplier: Option<U256>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BalancerPoolKind {
    Weighted,
    Stable,
    Linear,
}

#[derive(Debug, Clone)]
pub struct BalancerLinearState {
    pub main_index: usize,
    pub wrapped_index: usize,
    pub lower_target: U256,
    pub upper_target: U256,
    /// Main-token value of one wrapped token, scaled to 1e18.
    pub wrapped_rate: U256,
}

#[derive(Debug, Clone)]
pub struct BalancerPoolState {
    pub pool_id: Option<alloy::primitives::FixedBytes<32>>,
    /// Vault token order from `getPoolTokens`; used for routing edge indices.
    pub tokens: Vec<Address>,
    pub balances: Vec<U256>,
    pub weights: Vec<U256>,
    pub scaling_factors: Vec<U256>,
    pub amp: U256,
    pub amp_precision: U256,
    pub fee: U256,
    pub pool_type: BalancerPoolKind,
    pub linear: Option<BalancerLinearState>,
    pub bpt_index: Option<usize>,
    /// True = amplification parameter is mid-ramp (math changes per block).
    pub is_updating: bool,
    /// Block number of last liquidity change from getPoolTokens.
    pub last_change_block: u64,
}

#[derive(Debug, Clone)]
pub struct DodoPoolState {
    pub base_reserve: U256,
    pub quote_reserve: U256,
    pub base_token: Address,
    pub quote_token: Address,
    pub base_target: U256,
    pub quote_target: U256,
    pub r_state: DodoRState,
    pub i: U256,
    pub k: U256,
    pub lp_fee_rate: U256,
    /// Maintainer fee on gross PMM output (1e18 scale). From `_MT_FEE_RATE_()` when that
    /// view succeeds; `0` when it reverts (common on Polygon DVM/DPP/DSP fee-model pools).
    pub mt_fee_rate: U256,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DodoRState {
    One,
    AboveOne,
    BelowOne,
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
    /// Canonical simulation order: feasible base tokens followed by quote token.
    pub tokens: Vec<Address>,
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
    /// Token leg has enough inventory for a single-hop swap (protocol dust guard).
    #[inline]
    #[must_use]
    pub fn hop_token_funded(&self, token_idx: usize) -> bool {
        match self {
            PoolState::Invalid => false,
            // Align with HF `v2_any_hop_dust_reserves` — 1-wei floors admitted dust
            // V2 into the routing graph and poisoned the cycle snap (live: v2_dead
            // 300+/325).
            PoolState::V2(s) => match token_idx {
                0 => s.reserve0 >= V2_MIN_RESERVE,
                1 => s.reserve1 >= V2_MIN_RESERVE,
                _ => false,
            },
            PoolState::V3(s) | PoolState::V4(s) => {
                token_idx <= 1 && s.unlocked && s.liquidity > 0 && !s.sqrt_price_x96.is_zero()
            }
            PoolState::Curve(s) => s
                .balances
                .get(token_idx)
                .is_some_and(|balance| *balance >= MIN_HOP_TOKEN_BALANCE),
            PoolState::Balancer(s) => {
                if s.bpt_index == Some(token_idx) {
                    return false;
                }
                s.balances
                    .get(token_idx)
                    .is_some_and(|balance| *balance >= MIN_HOP_TOKEN_BALANCE)
            }
            // Align with V2 dust floor — 1-wei DODO legs were is_tradable and
            // entered the arena with useless PMM edges, then re-burned LF slots
            // as class-2 invalid after failing graph eligibility.
            PoolState::Dodo(s) => match token_idx {
                0 => s.base_reserve >= V2_MIN_RESERVE,
                1 => s.quote_reserve >= V2_MIN_RESERVE,
                _ => false,
            },
            PoolState::Woofi(s) => {
                let quote_idx = s.base_states.len();
                if token_idx < quote_idx {
                    s.base_states.get(token_idx).is_some_and(|base| {
                        base.reserve >= (base.base_dec / U256::from(1_000u64)).max(U256::ONE)
                    })
                } else if token_idx == quote_idx {
                    s.base_states.first().is_some_and(|base| {
                        s.quote_reserve >= (base.quote_dec / U256::from(1_000u64)).max(U256::ONE)
                    })
                } else {
                    false
                }
            }
        }
    }

    /// Directed swap is structurally valid and both legs are funded.
    #[inline]
    #[must_use]
    pub fn hop_pair_routable(&self, token_in_idx: usize, token_out_idx: usize) -> bool {
        if token_in_idx == token_out_idx {
            return false;
        }
        match self {
            PoolState::Invalid => false,
            PoolState::Woofi(s) => {
                let quote_idx = s.base_states.len();
                if token_in_idx > quote_idx || token_out_idx > quote_idx {
                    return false;
                }
                self.hop_token_funded(token_in_idx) && self.hop_token_funded(token_out_idx)
            }
            _ => self.hop_token_funded(token_in_idx) && self.hop_token_funded(token_out_idx),
        }
    }

    /// Returns true when at least 2 distinct token indices are funded.
    /// Shared by Curve and Balancer is_tradable checks.
    fn at_least_two_funded(&self, n: usize) -> bool {
        let mut funded = 0usize;
        for i in 0..n {
            if self.hop_token_funded(i) {
                funded += 1;
                if funded >= 2 {
                    return true;
                }
            }
        }
        false
    }

    #[inline]
    #[must_use]
    pub fn is_tradable(&self) -> bool {
        match self {
            PoolState::Invalid => false,
            PoolState::V2(_) | PoolState::Dodo(_) => {
                self.hop_token_funded(0) && self.hop_token_funded(1)
            }
            PoolState::V3(s) | PoolState::V4(s) => {
                s.unlocked && s.liquidity > 0 && !s.sqrt_price_x96.is_zero()
            }
            PoolState::Curve(s) => {
                let n = usize::from(s.n_coins);
                if n < 2 || s.balances.len() != n || s.rates.len() != n || s.a.is_zero() {
                    return false;
                }
                if s.rates.iter().any(U256::is_zero) {
                    return false;
                }
                self.at_least_two_funded(n)
            }
            PoolState::Balancer(s) => {
                let bpt = s.bpt_index;
                let n = s.balances.len();
                let family_valid = match s.pool_type {
                    BalancerPoolKind::Weighted => {
                        s.weights.len() == n && s.weights.iter().all(|weight| !weight.is_zero())
                    }
                    BalancerPoolKind::Stable => {
                        !s.amp.is_zero() && !s.amp_precision.is_zero() && !s.is_updating
                    }
                    BalancerPoolKind::Linear => s.linear.as_ref().is_some_and(|linear| {
                        linear.main_index < n
                            && linear.wrapped_index < n
                            && linear.main_index != linear.wrapped_index
                            && !linear.wrapped_rate.is_zero()
                    }),
                };
                if n < 2
                    || s.tokens.len() != n
                    || s.scaling_factors.len() != n
                    || !family_valid
                    || bpt.is_some_and(|index| index >= n)
                {
                    return false;
                }
                if s.scaling_factors.iter().any(U256::is_zero) {
                    return false;
                }
                self.at_least_two_funded(n)
            }
            PoolState::Woofi(s) => {
                if s.tokens.len() != s.base_states.len() + 1 {
                    return false;
                }
                let quote_idx = s.base_states.len();
                self.hop_token_funded(quote_idx) && (0..quote_idx).any(|i| self.hop_token_funded(i))
            }
        }
    }
}

impl AsRef<FoundCycle> for FoundCycle {
    fn as_ref(&self) -> &FoundCycle {
        self
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
    /// U256 fixed-point cycle ratio = product(edge_ratios) / ONE^(hop_count-1).
    /// cycle_ratio > ONE means the cycle is gross-profitable (more output than input).
    /// Used for precision-critical profitability checks; eliminates f64 rounding in
    /// the final profit decision.
    pub cycle_ratio: U256,
}

impl FoundCycle {
    /// Canonical hop count from `edges` (source of truth over cached `hop_count`).
    #[inline]
    #[must_use]
    pub fn edge_hops(&self) -> u32 {
        u32::try_from(self.edges.len()).unwrap_or(self.hop_count)
    }

    /// Prefer `edges.len()`; keep `hop_count` in sync when present.
    #[inline]
    #[must_use]
    pub fn hops_consistent(&self) -> bool {
        self.hop_count as usize == self.edges.len()
    }
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
    /// `executeArb` — Balancer V2 vault `flashLoan`.
    Balancer,
    /// `executeArbWithAave` — Aave V3 `flashLoanSimple`.
    AaveV3,
    /// `executeArbWithDodo` — DODO V2 pool `flashLoan`.
    Dodo,
    /// `executeArbDirect` — no flash loan; Balancer `batchSwap` flash-swap routes.
    Direct,
}

impl FlashLoanSource {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Balancer => "balancer",
            Self::AaveV3 => "aave",
            Self::Dodo => "dodo",
            Self::Direct => "direct",
        }
    }
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
    pub priority_bid_basis_matic_wei: U256,
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

#[cfg(test)]
mod hop_routing_tests {
    use super::*;
    use alloy::primitives::Address;

    const FUNDED_TEST_BALANCE: U256 = U256::from_limbs([1_000_000_000_000_000, 0, 0, 0]);

    fn funded() -> U256 {
        FUNDED_TEST_BALANCE
    }

    fn dust() -> U256 {
        U256::ZERO
    }

    #[test]
    fn v2_requires_both_legs_funded() {
        let state = PoolState::V2(V2PoolState {
            reserve0: funded(),
            reserve1: dust(),
            fee: U256::from(30u8),
            fee_denominator: U256::from(10_000u64),
            block_timestamp_last: 0,
        });
        assert!(!state.is_tradable());
        assert!(state.hop_token_funded(0));
        assert!(!state.hop_token_funded(1));
        assert!(!state.hop_pair_routable(0, 1));
    }

    #[test]
    fn v2_rejects_dust_reserves_below_graph_hf_floor() {
        // Below V2_MIN_RESERVE (1e8) — must not be graph-tradable (aligned with HF).
        let dust = PoolState::V2(V2PoolState {
            reserve0: U256::from(1_000_000u64),
            reserve1: U256::from(2_000_000u64),
            fee: U256::from(30u8),
            fee_denominator: U256::from(10_000u64),
            block_timestamp_last: 0,
        });
        assert!(!dust.is_tradable());
        assert!(!dust.hop_pair_routable(0, 1));

        let live = PoolState::V2(V2PoolState {
            reserve0: crate::core::constants::V2_MIN_RESERVE,
            reserve1: crate::core::constants::V2_MIN_RESERVE + U256::from(1u64),
            fee: U256::from(30u8),
            fee_denominator: U256::from(10_000u64),
            block_timestamp_last: 0,
        });
        assert!(live.is_tradable());
        assert!(live.hop_pair_routable(0, 1));
    }

    #[test]
    fn dodo_rejects_dust_reserves_below_v2_floor() {
        let dust = PoolState::Dodo(DodoPoolState {
            base_reserve: U256::from(1_000u64),
            quote_reserve: U256::from(2_000u64),
            base_token: Address::with_last_byte(1),
            quote_token: Address::with_last_byte(2),
            base_target: U256::from(1_000u64),
            quote_target: U256::from(2_000u64),
            r_state: DodoRState::One,
            i: U256::from(1u64) << 18,
            k: U256::from(1u64) << 17,
            lp_fee_rate: U256::ZERO,
            mt_fee_rate: U256::ZERO,
        });
        assert!(!dust.is_tradable());
        assert!(!dust.hop_pair_routable(0, 1));

        let live = PoolState::Dodo(DodoPoolState {
            base_reserve: crate::core::constants::V2_MIN_RESERVE,
            quote_reserve: crate::core::constants::V2_MIN_RESERVE + U256::from(1u64),
            base_token: Address::with_last_byte(1),
            quote_token: Address::with_last_byte(2),
            base_target: crate::core::constants::V2_MIN_RESERVE,
            quote_target: crate::core::constants::V2_MIN_RESERVE,
            r_state: DodoRState::One,
            i: U256::from(1u64) << 18,
            k: U256::from(1u64) << 17,
            lp_fee_rate: U256::ZERO,
            mt_fee_rate: U256::ZERO,
        });
        assert!(live.is_tradable());
        assert!(live.hop_pair_routable(0, 1));
    }

    #[test]
    fn woofi_skips_underfunded_base_legs() {
        let state = PoolState::Woofi(WoofiPoolState {
            tokens: vec![Address::ZERO; 3],
            quote_reserve: funded(),
            base_states: vec![
                WoofiBaseTokenState {
                    price: U256::from(1u8),
                    spread: U256::ZERO,
                    coeff: U256::ZERO,
                    reserve: funded(),
                    base_dec: funded() * U256::from(1_000u64),
                    quote_dec: funded() * U256::from(1_000u64),
                    price_dec: U256::from(1u8),
                    fee_rate: U256::ZERO,
                    max_gamma: U256::ZERO,
                    max_notional_swap: U256::ZERO,
                },
                WoofiBaseTokenState {
                    price: U256::from(1u8),
                    spread: U256::ZERO,
                    coeff: U256::ZERO,
                    reserve: dust(),
                    base_dec: funded() * U256::from(1_000u64),
                    quote_dec: funded() * U256::from(1_000u64),
                    price_dec: U256::from(1u8),
                    fee_rate: U256::ZERO,
                    max_gamma: U256::ZERO,
                    max_notional_swap: U256::ZERO,
                },
            ],
            fee: U256::ZERO,
        });
        assert!(state.is_tradable());
        assert!(state.hop_pair_routable(0, 2));
        assert!(!state.hop_pair_routable(1, 2));
        assert!(!state.hop_pair_routable(1, 0));
    }

    #[test]
    fn woofi_six_decimal_quote_reserve_is_tradable() {
        // Given: a funded base and a six-decimal quote reserve above one token.
        let state = PoolState::Woofi(WoofiPoolState {
            tokens: vec![Address::ZERO; 2],
            quote_reserve: U256::from(98_047_508_855u64),
            base_states: vec![WoofiBaseTokenState {
                price: U256::from(1u8),
                spread: U256::ZERO,
                coeff: U256::ZERO,
                reserve: U256::from(10u128.pow(18)),
                base_dec: U256::from(10u128.pow(18)),
                quote_dec: U256::from(10u64.pow(6)),
                price_dec: U256::from(10u128.pow(8)),
                fee_rate: U256::ZERO,
                max_gamma: U256::ZERO,
                max_notional_swap: U256::ZERO,
            }],
            fee: U256::ZERO,
        });

        // When: protocol-aware funding checks are applied.
        let tradable = state.is_tradable();

        // Then: raw 1e15 units must not reject a healthy six-decimal reserve.
        assert!(tradable);
    }

    #[test]
    fn balancer_stable_mid_ramp_not_tradable() {
        let state = PoolState::Balancer(BalancerPoolState {
            pool_id: None,
            tokens: vec![Address::with_last_byte(1), Address::with_last_byte(2)],
            balances: vec![funded(), funded()],
            weights: vec![funded(); 2],
            scaling_factors: vec![funded(); 2],
            amp: funded(),
            amp_precision: U256::from(1u64),
            fee: U256::from(10u8),
            pool_type: BalancerPoolKind::Stable,
            linear: None,
            bpt_index: None,
            is_updating: true,
            last_change_block: 0,
        });
        assert!(!state.is_tradable());
    }

    #[test]
    fn balancer_stable_ramp_complete_is_tradable() {
        let state = PoolState::Balancer(BalancerPoolState {
            pool_id: None,
            tokens: vec![Address::with_last_byte(1), Address::with_last_byte(2)],
            balances: vec![funded(), funded()],
            weights: vec![funded(); 2],
            scaling_factors: vec![funded(); 2],
            amp: funded(),
            amp_precision: U256::from(1u64),
            fee: U256::from(10u8),
            pool_type: BalancerPoolKind::Stable,
            linear: None,
            bpt_index: None,
            is_updating: false,
            last_change_block: 0,
        });
        assert!(state.is_tradable());
    }
}
