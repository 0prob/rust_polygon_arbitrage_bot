use alloy::primitives::Address;

use crate::core::constants::{
    AAVE, BAL, COMP, CRV, DAI, GHST, GRT, LINK, MANA, SAND, SNX, SUSHI, UNI, USDC_E, USDC_NATIVE,
    USDT, WBTC, WETH, WMATIC, WST_ETH,
};
use crate::core::types::Edge;
use crate::services::execution::candidate::hash_cycle_edges;
use crate::services::oracle::token_labels::lookup_symbol;
use crate::util::truncate_str;

pub use crate::core::types::protocol_tag;

const POLYGONSCAN_TX: &str = "https://polygonscan.com/tx/";
const POLYGONSCAN_ADDRESS: &str = "https://polygonscan.com/address/";

#[must_use]
pub fn short_address(address: Address) -> String {
    let s = format!("{address}");
    truncate_str(&s, 12).into_owned()
}

/// Human label for a Polygon token: known symbol, hub name, else short address.
#[must_use]
pub fn token_label(address: Address) -> String {
    if let Some(sym) = lookup_symbol(&address) {
        return sym.to_string();
    }
    if let Some(sym) = hub_token_symbol(address) {
        return sym.to_string();
    }
    short_address(address)
}

/// Compact path like `WMATIC → USDC → WETH → WMATIC` (dedupes consecutive duplicates).
#[must_use]
pub fn format_token_path(tokens: &[Address]) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(tokens.len());
    for addr in tokens {
        let label = token_label(*addr);
        if parts.last().is_some_and(|prev| prev == &label) {
            continue;
        }
        parts.push(label);
    }
    parts.join(" → ")
}

#[must_use]
pub fn polygonscan_tx_url(tx_hash: &str) -> String {
    let hash = tx_hash.trim();
    let hash = hash.strip_prefix("0x").unwrap_or(hash);
    format!("{POLYGONSCAN_TX}0x{hash}")
}

#[must_use]
pub fn polygonscan_address_url(address: Address) -> String {
    format!("{POLYGONSCAN_ADDRESS}{address}")
}

#[inline]
fn hub_token_symbol(address: Address) -> Option<&'static str> {
    // Match on identity — `Address` is not a useful pattern for const arms.
    if address == WMATIC {
        Some("WMATIC")
    } else if address == USDC_E {
        Some("USDC.e")
    } else if address == USDC_NATIVE {
        Some("USDC")
    } else if address == USDT {
        Some("USDT")
    } else if address == WETH {
        Some("WETH")
    } else if address == WBTC {
        Some("WBTC")
    } else if address == DAI {
        Some("DAI")
    } else if address == LINK {
        Some("LINK")
    } else if address == AAVE {
        Some("AAVE")
    } else if address == CRV {
        Some("CRV")
    } else if address == SUSHI {
        Some("SUSHI")
    } else if address == BAL {
        Some("BAL")
    } else if address == SAND {
        Some("SAND")
    } else if address == MANA {
        Some("MANA")
    } else if address == UNI {
        Some("UNI")
    } else if address == GRT {
        Some("GRT")
    } else if address == GHST {
        Some("GHST")
    } else if address == WST_ETH {
        Some("wstETH")
    } else if address == COMP {
        Some("COMP")
    } else if address == SNX {
        Some("SNX")
    } else {
        None
    }
}

/// Same fingerprint as execution / HF eval (`hash_cycle_edges`).
#[must_use]
pub fn route_fingerprint(edges: &[Edge]) -> u64 {
    hash_cycle_edges(edges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{CycleEdges, Edge, PoolIndex, ProtocolType, TokenIndex};
    use crate::pipeline::types::{PoolMeta, pool_meta_at};

    fn sample_edge(reverse: bool) -> Edge {
        let (token_in, token_out) = if reverse {
            (TokenIndex(1), TokenIndex(0))
        } else {
            (TokenIndex(0), TokenIndex(1))
        };
        Edge {
            pool_index: PoolIndex(7),
            token_in,
            token_out,
            token_in_idx: reverse as u8,
            token_out_idx: (!reverse) as u8,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: !reverse,
        }
    }

    #[test]
    fn route_fingerprint_matches_execution_hash() {
        let edges: CycleEdges = CycleEdges::from_slice(&[sample_edge(false)]);
        assert_eq!(route_fingerprint(&edges), hash_cycle_edges(&edges));
    }

    #[test]
    fn token_label_uses_hub_symbols() {
        assert_eq!(token_label(WMATIC), "WMATIC");
        assert_eq!(token_label(USDC_NATIVE), "USDC");
        assert_eq!(token_label(WETH), "WETH");
    }

    #[test]
    fn format_token_path_dedupes_consecutive() {
        let path = format_token_path(&[WMATIC, USDC_E, USDC_E, WETH, WMATIC]);
        assert_eq!(path, "WMATIC → USDC.e → WETH → WMATIC");
    }

    #[test]
    fn polygonscan_urls_format_tx_and_address() {
        let tx = polygonscan_tx_url("0xabc");
        assert_eq!(tx, "https://polygonscan.com/tx/0xabc");
        let bare = polygonscan_tx_url("def");
        assert_eq!(bare, "https://polygonscan.com/tx/0xdef");
        let addr = polygonscan_address_url(WMATIC);
        assert!(addr.starts_with("https://polygonscan.com/address/0x"));
    }

    #[test]
    fn pool_meta_lookup_uses_pool_index_not_vec_position() {
        let metas = vec![
            PoolMeta {
                pool_index: PoolIndex(2),
                protocol: ProtocolType::UniswapV3,
                tokens: Vec::new(),
                fee_bps: 30,
                bpt_index: None,
                pool_id: None,
                protocol_label: None,
                pool_type: None,
                hooks: None,
                tick_spacing: None,
            },
            PoolMeta {
                pool_index: PoolIndex(5),
                protocol: ProtocolType::UniswapV2,
                tokens: Vec::new(),
                fee_bps: 30,
                bpt_index: None,
                pool_id: None,
                protocol_label: None,
                pool_type: None,
                hooks: None,
                tick_spacing: None,
            },
        ];
        assert_eq!(
            pool_meta_at(&metas, PoolIndex(5))
                .map(|m| m.protocol)
                .expect("PoolIndex(5) should exist in the test metas"),
            ProtocolType::UniswapV2
        );
        assert_eq!(
            pool_meta_at(&metas, PoolIndex(2))
                .map(|m| m.protocol)
                .expect("PoolIndex(2) should exist in the test metas"),
            ProtocolType::UniswapV3
        );
    }
}
