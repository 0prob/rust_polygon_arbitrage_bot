use alloy::network::Ethereum;
use alloy::primitives::{Address, Bytes};
use alloy::providers::Provider;
use alloy::sol_types::SolCall;

use crate::abis::IMulticall3;
use crate::core::constants::MULTICALL3;

#[derive(Debug, Clone)]
pub struct MulticallItem {
    pub target: Address,
    pub data: Bytes,
}

pub async fn execute_multicall<P: Provider<Ethereum>>(
    provider: &P,
    items: &[MulticallItem],
) -> anyhow::Result<Vec<Option<Bytes>>> {
    if items.is_empty() {
        return Ok(Vec::new());
    }

    let contract = IMulticall3::new(MULTICALL3, provider);
    let calls: Vec<IMulticall3::Call3> = items
        .iter()
        .map(|item| IMulticall3::Call3 {
            target: item.target,
            allowFailure: true,
            callData: item.data.clone(),
        })
        .collect();

    let results = contract.aggregate3(calls).call().await?;
    Ok(results
        .into_iter()
        .map(|r| {
            if r.success && !r.returnData.is_empty() {
                Some(r.returnData)
            } else {
                None
            }
        })
        .collect())
}

pub fn chunk_items(items: Vec<MulticallItem>, max_calls: usize) -> Vec<Vec<MulticallItem>> {
    if items.is_empty() {
        return Vec::new();
    }
    let max = max_calls.max(1);
    items.chunks(max).map(|c| c.to_vec()).collect()
}

pub fn encode_call<C: SolCall>(call: &C) -> Bytes {
    Bytes::from(call.abi_encode())
}
