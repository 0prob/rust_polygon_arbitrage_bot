//! Shared fixture builder for tests and benches that need a small arena +
//! routing graph. `benches/routing.rs` and integration tests both compile as
//! separate crates linking `rpbot` externally, so this must be a plain `pub
//! mod` (not `#[cfg(test)]`-gated) to be visible from there.

use alloy::primitives::{Address, U256};

use crate::core::types::{PoolIndex, PoolState, ProtocolType, TokenIndex, V2PoolState};
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
