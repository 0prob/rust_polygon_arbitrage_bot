pub mod price_oracle;
pub mod rates;

pub use rates::{
    has_reliable_matic_rate, resolve_token_to_matic_rate, resolve_token_to_matic_rate_or_bootstrap,
};

use std::collections::HashMap;

use alloy::network::Ethereum;
use alloy::primitives::Address;
use alloy::providers::Provider;
use ruint::aliases::U256;
use rustc_hash::FxHashMap;

use crate::core::constants::WMATIC;
use crate::core::types::TokenIndex;
use crate::pipeline::arena::StateArena;
use crate::services::discovery::TokenMeta;

use self::price_oracle::{PriceOracle, token_usd_to_matic_rate_per_unit};

pub fn token_decimals_map(metas: &[TokenMeta]) -> HashMap<Address, u8> {
    metas.iter().map(|m| (m.address, m.decimals)).collect()
}

pub fn resolve_token_decimals(token: Address, hints: &HashMap<Address, u8>) -> u8 {
    hints.get(&token).copied().unwrap_or(18)
}

pub fn resolve_token_decimals_for_index(
    token: TokenIndex,
    arena: &StateArena,
    hints: &HashMap<Address, u8>,
) -> u8 {
    arena
        .token_address(token)
        .map(|addr| resolve_token_decimals(addr, hints))
        .unwrap_or(18)
}

pub async fn enrich_token_to_matic_rates<P: Provider<Ethereum>>(
    oracle: &PriceOracle,
    arena: &StateArena,
    tokens: &[TokenIndex],
    _decimals: &HashMap<Address, u8>,
    provider: Option<&P>,
) -> FxHashMap<TokenIndex, U256> {
    let addrs = token_addresses(arena, tokens);
    oracle.prefetch_token_usd(&addrs, provider).await;
    let matic_usd = oracle.get_matic_usd(provider).await;
    build_token_to_matic_rates(oracle, arena, tokens, matic_usd)
}

/// Pyth + in-memory cache only (no Chainlink RPC). Used when state RPC is down.
pub async fn enrich_token_to_matic_rates_offline(
    oracle: &PriceOracle,
    arena: &StateArena,
    tokens: &[TokenIndex],
    _decimals: &HashMap<Address, u8>,
) -> FxHashMap<TokenIndex, U256> {
    let addrs = token_addresses(arena, tokens);
    oracle.prefetch_token_usd_offline(&addrs).await;
    let matic_usd = oracle.get_matic_usd_offline().await;
    build_token_to_matic_rates(oracle, arena, tokens, matic_usd)
}

fn token_addresses(arena: &StateArena, tokens: &[TokenIndex]) -> Vec<Address> {
    let mut addrs: Vec<Address> = tokens
        .iter()
        .filter_map(|idx| arena.token_address(*idx))
        .collect();
    addrs.sort();
    addrs.dedup();
    addrs
}

fn build_token_to_matic_rates(
    oracle: &PriceOracle,
    arena: &StateArena,
    tokens: &[TokenIndex],
    matic_usd: f64,
) -> FxHashMap<TokenIndex, U256> {
    let wmatic = WMATIC;
    let mut out = FxHashMap::default();
    for idx in tokens {
        let Some(addr) = arena.token_address(*idx) else {
            continue;
        };
        let rate = oracle
            .token_matic_rate_per_unit_integer(&addr)
            .or_else(|| {
                if matic_usd > 0.0 {
                    oracle
                        .token_usd(&addr)
                        .map(|usd| token_usd_to_matic_rate_per_unit(usd, matic_usd))
                } else {
                    None
                }
            })
            .filter(|r| *r >= crate::core::constants::MIN_TOKEN_TO_MATIC_RATE);
        if let Some(rate) = rate {
            out.insert(*idx, rate);
        }
        if addr == wmatic
            && let Some(r) = oracle.token_matic_rate_per_unit_integer(&wmatic)
        {
            out.insert(*idx, r);
        }
    }
    out
}
