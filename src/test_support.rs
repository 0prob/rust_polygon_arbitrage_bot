//! Shared fixture builder for tests and benches that need a small arena +
//! routing graph. `benches/routing.rs` and integration tests both compile as
//! separate crates linking `rpbot` externally, so this must be a plain `pub
//! mod` (not `#[cfg(test)]`-gated) to be visible from there.

use alloy::primitives::{Address, U256};

use crate::core::types::{
    PoolIndex, PoolState, ProtocolType, TokenIndex, V2PoolState, V3PoolState, V3Tick,
};
use crate::pipeline::arena::StateArena;
use crate::pipeline::graph::{build_graph, pool_meta_from_pair};
use crate::pipeline::types::{PoolMeta, RoutingGraph};

/// Builds a small `StateArena` + `PoolMeta` set for graph/cycle fixtures,
/// generalizing the arena→token→pool→graph pattern repeated by hand across
/// `benches/routing.rs`'s benchmark functions.
#[derive(Default)]
pub struct FixtureBuilder {
    pub arena: StateArena,
    metas: Vec<PoolMeta>,
}

impl FixtureBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a token at a deterministic address derived from `seed`.
    pub fn token(&mut self, seed: u8) -> TokenIndex {
        self.arena.register_token(Address::from([seed; 20]))
    }

    /// Registers a V2 pool between `token_a`/`token_b` and its routing `PoolMeta`.
    #[allow(clippy::too_many_arguments)]
    pub fn v2_pool(
        &mut self,
        seed: u8,
        protocol: ProtocolType,
        token_a: TokenIndex,
        token_b: TokenIndex,
        reserve_a: U256,
        reserve_b: U256,
        fee_bps: u32,
    ) -> PoolIndex {
        let pool_index = self.arena.register_pool(
            Address::from([seed; 20]),
            std::sync::Arc::new(PoolState::V2(V2PoolState {
                reserve0: reserve_a,
                reserve1: reserve_b,
                fee: U256::from(10_000u32 - fee_bps),
                fee_denominator: U256::from(10_000u32),
                block_timestamp_last: 0,
            })),
        );
        self.metas.push(pool_meta_from_pair(
            pool_index, protocol, token_a, token_b, fee_bps,
        ));
        pool_index
    }

    pub fn metas(&self) -> &[PoolMeta] {
        &self.metas
    }

    pub fn build_graph(&self) -> RoutingGraph {
        build_graph(&self.arena, &self.metas)
    }
}

/// A 1:1-priced, deep, 0.30% V3 pool with a single far out-of-range tick.
///
/// This is the neutral "pool exists and is tradable" fixture — use it when the
/// test is about routing/encoding/healing and the pool's own curve is incidental.
/// Tests that exercise V3 math itself should keep building an explicit
/// [`V3PoolState`] so the values under test stay visible at the assertion.
#[must_use]
pub fn v3_pool_state_fixture() -> PoolState {
    PoolState::V3(V3PoolState {
        sqrt_price_x96: U256::from(1u128 << 96),
        liquidity: 1_000_000_000_000_000_000u128,
        tick: 0,
        fee: U256::from(3000u32),
        tick_spacing: 60,
        unlocked: true,
        fee_protocol: 0,
        observation_cardinality: 1,
        ticks: std::sync::Arc::from(vec![V3Tick {
            tick: -60_000,
            liquidity_gross: 1,
            liquidity_net: 0,
        }]),
    })
}
