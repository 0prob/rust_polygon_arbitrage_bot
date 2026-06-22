use alloy::primitives::{Address, Bytes, FixedBytes, U256};

use crate::abis::ExecutorCall;
use crate::core::types::Edge;

#[derive(Debug, Clone)]
pub struct CalldataHop {
    pub edge: Edge,
    pub pool_address: Address,
    pub token_in: Address,
    pub token_out: Address,
    pub amount_in: U256,
    pub amount_out: U256,
    pub pool_id: Option<FixedBytes<32>>,
    pub protocol_label: Option<String>,
    pub router: Option<Address>,
    pub hooks: Option<Address>,
}

#[derive(Debug, Clone, Copy)]
pub struct RouteEncodeConfig {
    pub slippage_bps: u64,
    pub deadline: U256,
}

#[derive(Clone)]
pub struct BuiltArbTx {
    pub to: Address,
    pub data: Bytes,
    pub value: U256,
    pub route_hash: FixedBytes<32>,
    pub calls: Vec<ExecutorCall>,
}
