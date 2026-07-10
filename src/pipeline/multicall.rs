use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::network::Ethereum;
use alloy::primitives::{Address, Bytes};
use alloy::providers::Provider;
use alloy::sol_types::SolCall;
use anyhow::Context;
use std::sync::Arc;
use std::time::Duration;

use crate::abis::IMulticall3;
use crate::core::constants::MULTICALL3;

/// Max `aggregate3` sub-calls per RPC round-trip (provider payload limits).
pub const MULTICALL_CHUNK: usize = 200;
/// Max concurrent chunk RPCs for batched multicalls. Parallelizes IO for large
/// refreshes (e.g. 1000+ pools) while a semaphore prevents thundering herd.
pub const MAX_CONCURRENT_CHUNKS: usize = 8;

/// Plan-batch call budget sized so `execute_multicall_at_chunked` can fan out
/// across [`MAX_CONCURRENT_CHUNKS`] RPC chunks instead of serializing one chunk.
#[must_use]
pub fn plan_batch_call_budget(max_chunk: usize) -> usize {
    let max_chunk = max_chunk.max(1);
    max_chunk.saturating_mul(MAX_CONCURRENT_CHUNKS)
}

#[derive(Debug, Clone)]
pub struct MulticallItem {
    pub target: Address,
    pub data: Bytes,
}

async fn retry_sleep(attempt: u32) {
    tokio::time::sleep(Duration::from_millis(250u64 << attempt)).await;
}

fn is_retryable_rpc_error(e: &anyhow::Error) -> bool {
    let msg = e.to_string();
    // Fast path: case-sensitive patterns (numbers, fixed codes) skip allocation.
    if msg.contains("429") || msg.contains("error code 15") {
        return true;
    }
    // Case-insensitive patterns — scan bytes without allocating a lowered copy.
    let b = msg.as_bytes();
    const PATTERNS: &[&[u8]] = &[
        b"rate limit",
        b"usage limit",
        b"too many request",
        b"unknown block",
        b"header not found",
    ];
    PATTERNS
        .iter()
        .any(|pat| b.windows(pat.len()).any(|w| w.eq_ignore_ascii_case(pat)))
}

fn build_calls(items: &[MulticallItem]) -> Vec<IMulticall3::Call3> {
    let mut calls = Vec::with_capacity(items.len());
    for item in items {
        calls.push(IMulticall3::Call3 {
            target: item.target,
            allowFailure: true,
            callData: item.data.clone(),
        });
    }
    calls
}

async fn execute_multicall_chunk<P: Provider<Ethereum>>(
    provider: &P,
    items: &[MulticallItem],
    block_number: Option<u64>,
) -> anyhow::Result<Vec<Option<Bytes>>> {
    if items.is_empty() {
        return Ok(Vec::new());
    }

    let contract = IMulticall3::new(MULTICALL3, provider);
    let calls = build_calls(items); // Build once — reused across retries.

    let mut attempt = 0u32;
    loop {
        let mut call = contract.aggregate3(calls.clone());
        if let Some(number) = block_number {
            call = call.block(BlockId::Number(BlockNumberOrTag::Number(number)));
        }
        match call.call().await {
            Ok(results) => {
                return Ok(results
                    .into_iter()
                    .map(|r| {
                        if r.success && !r.returnData.is_empty() {
                            Some(r.returnData)
                        } else {
                            None
                        }
                    })
                    .collect());
            }
            Err(e) => {
                let e: anyhow::Error = e.into();
                if is_retryable_rpc_error(&e) && attempt < 4 {
                    retry_sleep(attempt).await;
                    attempt += 1;
                    continue;
                }
                return Err(e);
            }
        }
    }
}

pub async fn execute_multicall<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: &P,
    items: &[MulticallItem],
) -> anyhow::Result<Vec<Option<Bytes>>> {
    if items.len() <= MULTICALL_CHUNK {
        execute_multicall_chunk(provider, items, None).await
    } else {
        execute_multicall_at_chunked(provider.clone(), items, None, MULTICALL_CHUNK).await
    }
}

/// Execute every item against one explicit block when supplied. This prevents
/// multi-batch refreshes from assembling a synthetic state that never existed.
pub async fn execute_multicall_at<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: &P,
    items: &[MulticallItem],
    block_number: Option<u64>,
) -> anyhow::Result<Vec<Option<Bytes>>> {
    if items.len() <= MULTICALL_CHUNK {
        execute_multicall_chunk(provider, items, block_number).await
    } else {
        execute_multicall_at_chunked(provider.clone(), items, block_number, MULTICALL_CHUNK).await
    }
}

/// Like [`execute_multicall_at`] but with a configurable per-RPC chunk size.
/// Chunks are executed concurrently (bounded) to optimize IO latency for
/// large state refreshes / pool fetches. Requires P: Clone (cheap for alloy providers).
pub async fn execute_multicall_at_chunked<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: P,
    items: &[MulticallItem],
    block_number: Option<u64>,
    max_chunk: usize,
) -> anyhow::Result<Vec<Option<Bytes>>> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    let max_chunk = max_chunk.max(1);
    if items.len() <= max_chunk {
        return execute_multicall_chunk(&provider, items, block_number).await;
    }

    let chunks: Vec<_> = items.chunks(max_chunk).collect();
    let num_chunks = chunks.len();
    let sem = Arc::new(tokio::sync::Semaphore::new(
        MAX_CONCURRENT_CHUNKS.min(num_chunks).max(1),
    ));

    let mut tasks = tokio::task::JoinSet::new();
    for (i, chunk) in chunks.into_iter().enumerate() {
        let sem = Arc::clone(&sem);
        let p = provider.clone();
        let bn = block_number;
        let chunk_vec: Vec<_> = chunk.to_vec();
        tasks.spawn(async move {
            let Ok(_permit) = sem.acquire().await else {
                return (i, Err(anyhow::anyhow!("multicall semaphore closed")));
            };
            let res = execute_multicall_chunk(&p, &chunk_vec, bn).await;
            (i, res)
        });
    }

    let mut indexed: Vec<(usize, Vec<Option<Bytes>>)> = Vec::with_capacity(num_chunks);
    while let Some(res) = tasks.join_next().await {
        let (idx, chunk_res) = res.context("chunk task panicked")?;
        let chunk_res = chunk_res.context("chunk multicall failed")?;
        indexed.push((idx, chunk_res));
    }
    indexed.sort_unstable_by_key(|&(i, _)| i);
    let mut out = Vec::with_capacity(items.len());
    for (_, chunk) in indexed {
        out.extend(chunk);
    }
    Ok(out)
}

pub fn encode_call<C: SolCall>(call: &C) -> Bytes {
    Bytes::from(call.abi_encode())
}

#[cfg(test)]
mod tests {
    use super::{MULTICALL_CHUNK, is_retryable_rpc_error, plan_batch_call_budget};

    #[test]
    fn retries_transient_rpc_responses_only() {
        for message in [
            "error code -32000: header not found",
            "429 Too Many Requests",
            "provider usage limit exceeded",
            "error code 15: Too many request, try again later",
            "error code 26: Unknown block",
        ] {
            assert!(is_retryable_rpc_error(&anyhow::anyhow!(message)));
        }
        assert!(!is_retryable_rpc_error(&anyhow::anyhow!(
            "execution reverted"
        )));
    }

    #[test]
    fn default_chunk_matches_config_default() {
        assert_eq!(MULTICALL_CHUNK, 200);
    }

    #[test]
    fn plan_batch_budget_enables_parallel_chunks() {
        assert_eq!(plan_batch_call_budget(200), 1_600);
        assert_eq!(plan_batch_call_budget(0), 8);
    }

    #[test]
    fn chunking_splits_large_batches() {
        let items_len = 450usize;
        let chunk = MULTICALL_CHUNK;
        let chunk_count = items_len.div_ceil(chunk);
        assert_eq!(chunk_count, 3);
    }

    #[test]
    fn serial_chunks_cover_all_items_in_order() {
        let max_chunk = 200usize;
        let items_len = 450usize;
        let items: Vec<_> = (0..items_len).collect();
        let chunks: Vec<_> = items.chunks(max_chunk).collect();
        let mut out = Vec::with_capacity(items_len);
        for chunk in chunks {
            out.extend_from_slice(chunk);
        }
        assert_eq!(out.len(), items_len);
        assert_eq!(out, (0..items_len).collect::<Vec<_>>());
    }
}
