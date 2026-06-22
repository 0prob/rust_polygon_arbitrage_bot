pub mod price_oracle;
pub mod rates;

pub use rates::resolve_token_to_matic_rate;

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

use self::price_oracle::{
    PriceOracle, bootstrap_matic_rate_per_unit, token_usd_to_matic_rate_per_unit,
};

pub fn token_decimals_map(metas: &[TokenMeta]) -> HashMap<Address, u8> {
    metas.iter().map(|m| (m.address, m.decimals)).collect()
}

pub fn resolve_token_decimals(token: Address, hints: &HashMap<Address, u8>) -> u8 {
    hints.get(&token).copied().unwrap_or(18)
}

pub async fn enrich_token_to_matic_rates<P: Provider<Ethereum>>(
    oracle: &PriceOracle,
    arena: &StateArena,
    tokens: &[TokenIndex],
    _decimals: &HashMap<Address, u8>,
    provider: Option<&P>,
) -> FxHashMap<TokenIndex, U256> {
    let mut addrs: Vec<Address> = tokens
        .iter()
        .filter_map(|idx| arena.token_address(*idx))
        .collect();
    addrs.sort();
    addrs.dedup();
    oracle.prefetch_token_usd(&addrs, provider).await;
    let matic_usd = oracle.get_matic_usd(provider).await;
    let wmatic = WMATIC;

    let mut out = FxHashMap::default();
    for idx in tokens {
        let Some(addr) = arena.token_address(*idx) else {
            continue;
        };
        let rate = oracle
            .token_matic_rate_per_unit_integer(&addr)
            .or_else(|| {
                oracle
                    .token_usd(&addr)
                    .map(|usd| token_usd_to_matic_rate_per_unit(usd, matic_usd))
            })
            .unwrap_or_else(bootstrap_matic_rate_per_unit);
        if !rate.is_zero() {
            out.insert(*idx, rate);
        }
        // Ensure WMATIC self-rate is populated when integer path available.
        if addr == wmatic
            && let Some(r) = oracle.token_matic_rate_per_unit_integer(&wmatic)
        {
            out.insert(*idx, r);
        }
    }
    out
}
