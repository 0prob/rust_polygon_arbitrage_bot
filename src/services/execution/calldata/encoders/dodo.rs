use alloy::primitives::{Address, U256};
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, IDodoPool};
use crate::core::types::PoolState;
use crate::pipeline::arena::StateArena;
use crate::services::execution::calldata::CalldataHop;
use crate::services::execution::calldata::approvals::encode_token_transfer;

/// Encode a DODO pool hop into executor calls.
///
/// Returns:
/// 1. Transfer `token_in` to the pool (exact or `transferAll`)
/// 2. `sellBase` / `sellQuote` based on **on-chain base/quote tokens**, not
///    `zero_for_one` / meta token index (indexer order can disagree with base).
///
/// DODO `flashLoan` is reentrancy-locked: never flash from a pool that also
/// appears as a swap hop in the same route (gated in flash selection + encode).
pub fn encode_dodo_hop(
    arena: &StateArena,
    hop: &CalldataHop,
    recipient: Address,
    use_transfer_all: bool,
) -> anyhow::Result<Vec<ExecutorCall>> {
    let sell_base = dodo_sell_base(arena, hop)?;

    Ok(vec![
        encode_token_transfer(
            recipient,
            hop.token_in,
            hop.pool_address,
            hop.amount_in,
            use_transfer_all,
        ),
        ExecutorCall {
            target: hop.pool_address,
            value: U256::ZERO,
            data: if sell_base {
                IDodoPool::sellBaseCall { to: recipient }.abi_encode()
            } else {
                IDodoPool::sellQuoteCall { to: recipient }.abi_encode()
            }
            .into(),
        },
    ])
}

fn dodo_sell_base(arena: &StateArena, hop: &CalldataHop) -> anyhow::Result<bool> {
    match arena.pool_state(hop.edge.pool_index) {
        Some(PoolState::Dodo(state)) => {
            if hop.token_in == state.base_token && hop.token_out == state.quote_token {
                Ok(true)
            } else if hop.token_in == state.quote_token && hop.token_out == state.base_token {
                Ok(false)
            } else {
                anyhow::bail!(
                    "dodo hop tokens do not match pool base/quote (in={} out={} base={} quote={})",
                    hop.token_in,
                    hop.token_out,
                    state.base_token,
                    state.quote_token
                )
            }
        }
        // Missing state: fall back to graph direction (token_in_idx 0 ⇒ base in meta order).
        _ => Ok(hop.edge.zero_for_one),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{DodoPoolState, DodoRState, Edge, ProtocolType};
    use std::sync::Arc;

    #[test]
    fn dodo_direction_follows_base_quote_not_zero_for_one() {
        let base = Address::repeat_byte(0x0b);
        let quote = Address::repeat_byte(0x0a); // quote < base by address
        let pool = Address::repeat_byte(0xdd);
        let mut arena = StateArena::default();
        let bi = arena.register_token(base);
        let qi = arena.register_token(quote);
        let pool_index = arena.register_pool(
            pool,
            Arc::new(PoolState::Dodo(DodoPoolState {
                base_reserve: U256::from(1_000_000u64),
                quote_reserve: U256::from(1_000_000u64),
                base_token: base,
                quote_token: quote,
                base_target: U256::from(1_000_000u64),
                quote_target: U256::from(1_000_000u64),
                r_state: DodoRState::One,
                i: U256::from(1u64) << 18,
                k: U256::from(1u64) << 17,
                lp_fee_rate: U256::ZERO,
                mt_fee_rate: U256::ZERO,
            })),
        );
        // zero_for_one true would mean sellBase if keyed off index, but token_in is quote.
        let hop = CalldataHop {
            edge: Edge {
                pool_index,
                token_in: qi,
                token_out: bi,
                token_in_idx: 0,
                token_out_idx: 1,
                protocol: ProtocolType::Dodo,
                fee_bps: 10,
                zero_for_one: true,
            },
            pool_address: pool,
            token_in: quote,
            token_out: base,
            amount_in: U256::from(100u64),
            amount_out: U256::from(90u64),
            pool_id: None,
            protocol_label: None,
            pool_type: None,
            router: None,
            hooks: None,
        };
        assert!(!dodo_sell_base(&arena, &hop).expect("direction"));
        let calls = encode_dodo_hop(&arena, &hop, Address::repeat_byte(0xee), false).expect("enc");
        assert_eq!(calls.len(), 2);
        // sellQuote selector 0xdd93f59a
        assert_eq!(&calls[1].data.as_ref()[..4], &[0xdd, 0x93, 0xf5, 0x9a]);
    }
}
