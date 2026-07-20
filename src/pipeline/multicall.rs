use alloy::eips::{BlockId, BlockNumberOrTag};
use alloy::network::Ethereum;
use alloy::primitives::{Address, Bytes};
use alloy::providers::Provider;
use alloy::sol_types::SolCall;
use anyhow::Context;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::sync::Semaphore;

use crate::abis::IMulticall3;
use crate::core::constants::MULTICALL3;

/// Max `aggregate3` sub-calls per RPC round-trip (provider payload limits).
pub const MULTICALL_CHUNK: usize = 200;
/// TickLens `getPopulatedTicksInWord` returns fat ABI arrays. Polygon nodes
/// reject eth_calls with `out of gas` or `result length … exceeding limit 100000`
/// when many words share one aggregate3 — keep chunks modest; resilient bisects.
pub const TICK_LENS_MULTICALL_CHUNK: usize = 12;
/// Max concurrent chunk RPCs for batched multicalls. Parallelizes IO for large
/// refreshes while a semaphore prevents thundering herd. 8×200-call chunks
/// (1600-call plan batches) routinely tripped free-tier 1200 req/min limits;
/// 4 keeps fan-out without the bisect storm under 429.
pub const MAX_CONCURRENT_CHUNKS: usize = 4;

static GLOBAL_MULTICALL_ADMISSION: LazyLock<Semaphore> =
    LazyLock::new(|| Semaphore::new(MAX_CONCURRENT_CHUNKS));

#[cfg(test)]
fn global_multicall_available_permits() -> usize {
    GLOBAL_MULTICALL_ADMISSION.available_permits()
}

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

fn is_rate_limited_rpc_error(e: &anyhow::Error) -> bool {
    crate::services::execution::rpc_errors::is_rpc_rate_limited(e)
}

fn is_retryable_rpc_error(e: &anyhow::Error) -> bool {
    // Rate limits must not retry or bisect — that amplifies 429s (see is_rpc_rate_limited).
    if is_rate_limited_rpc_error(e) {
        return false;
    }
    // Prefer alternate Display so nested anyhow contexts still match.
    let msg = format!("{e:#}");
    let b = msg.as_bytes();
    const PATTERNS: &[&[u8]] = &[b"unknown block", b"header not found"];
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

async fn execute_multicall_chunk_resilient<P: Provider<Ethereum>>(
    provider: &P,
    items: &[MulticallItem],
    block_number: Option<u64>,
) -> anyhow::Result<Vec<Option<Bytes>>> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    let mut pending: Vec<(usize, usize)> = vec![(0, items.len())];
    let mut out = vec![None; items.len()];
    while let Some((start, end)) = pending.pop() {
        let slice = &items[start..end];
        match execute_multicall_chunk(provider, slice, block_number).await {
            Ok(results) => {
                for (i, result) in results.into_iter().enumerate() {
                    out[start + i] = result;
                }
            }
            Err(e) if is_rate_limited_rpc_error(&e) => return Err(e),
            Err(_e) if slice.len() > 1 => {
                let mid = start + slice.len() / 2;
                pending.push((mid, end));
                pending.push((start, mid));
            }
            Err(e) => {
                // Polygon: single TickLens word can still exceed the ~100KB
                // eth_call response cap — leave None (caller treats incomplete).
                let msg = format!("{e:#}");
                if msg.contains("exceeding limit") || msg.contains("out of gas") {
                    crate::debug!(
                        "multicall call skipped (payload/gas at index {start}): {e:#}"
                    );
                } else {
                    crate::warn!(
                        "multicall chunk failed ({} call(s) at index {start}): {e:#}",
                        slice.len()
                    );
                }
            }
        }
    }
    Ok(out)
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
    let mut calls = build_calls(items);

    let mut attempt = 0u32;
    loop {
        let _permit = GLOBAL_MULTICALL_ADMISSION
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("global multicall admission closed"))?;
        let mut call = contract.aggregate3(std::mem::take(&mut calls));
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
                drop(_permit);
                if is_rate_limited_rpc_error(&e) {
                    return Err(e);
                }
                if is_retryable_rpc_error(&e) && attempt < 4 {
                    retry_sleep(attempt).await;
                    attempt += 1;
                    if calls.is_empty() {
                        calls = build_calls(items);
                    }
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
    if items.is_empty() {
        return Ok(Vec::new());
    }
    if items.len() <= MULTICALL_CHUNK {
        execute_multicall_chunk(provider, items, None).await
    } else {
        execute_multicall_at_chunked(
            provider.clone(),
            Arc::from(items.to_vec()),
            None,
            MULTICALL_CHUNK,
        )
        .await
    }
}

/// Execute every item against one explicit block when supplied. This prevents
/// multi-batch refreshes from assembling a synthetic state that never existed.
pub async fn execute_multicall_at<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: &P,
    items: &[MulticallItem],
    block_number: Option<u64>,
) -> anyhow::Result<Vec<Option<Bytes>>> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    // Always resilient: Polygon OOG / response-size errors must bisect instead of
    // failing the whole TickLens/state batch (live: 1–9 pools → full rpc_failed).
    if items.len() <= MULTICALL_CHUNK {
        execute_multicall_chunk_resilient(provider, items, block_number).await
    } else {
        execute_multicall_at_chunked(
            provider.clone(),
            Arc::from(items.to_vec()),
            block_number,
            MULTICALL_CHUNK,
        )
        .await
    }
}

/// TickLens-sized chunks + bisect — use for V3 tick hydration on Polygon.
pub async fn execute_tick_lens_multicall_at<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: &P,
    items: &[MulticallItem],
    block_number: Option<u64>,
) -> anyhow::Result<Vec<Option<Bytes>>> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    execute_multicall_at_chunked(
        provider.clone(),
        Arc::from(items.to_vec()),
        block_number,
        TICK_LENS_MULTICALL_CHUNK,
    )
    .await
}

/// Like [`execute_multicall_at`] but with a configurable per-RPC chunk size.
/// Chunks are executed concurrently (bounded) to optimize IO latency for
/// large state refreshes / pool fetches. Requires P: Clone (cheap for alloy providers).
pub async fn execute_multicall_at_chunked<P: Provider<Ethereum> + Clone + Send + 'static>(
    provider: P,
    items: Arc<[MulticallItem]>,
    block_number: Option<u64>,
    max_chunk: usize,
) -> anyhow::Result<Vec<Option<Bytes>>> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    let max_chunk = max_chunk.max(1);
    if items.len() <= max_chunk {
        return execute_multicall_chunk_resilient(&provider, &items, block_number).await;
    }
    let num_chunks = items.len().div_ceil(max_chunk);
    let sem = Arc::new(tokio::sync::Semaphore::new(
        MAX_CONCURRENT_CHUNKS.min(num_chunks).max(1),
    ));

    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..num_chunks {
        let start = i * max_chunk;
        let end = (start + max_chunk).min(items.len());
        let sem = Arc::clone(&sem);
        let p = provider.clone();
        let bn = block_number;
        let items = Arc::clone(&items);
        tasks.spawn(async move {
            let Ok(_permit) = sem.acquire().await else {
                return (i, Err(anyhow::anyhow!("multicall semaphore closed")));
            };
            let res = execute_multicall_chunk_resilient(&p, &items[start..end], bn).await;
            (i, res)
        });
    }

    let mut indexed: Vec<(usize, Vec<Option<Bytes>>)> = Vec::with_capacity(num_chunks);
    while let Some(res) = tasks.join_next().await {
        let (idx, chunk_res) = res.context("chunk task panicked")?;
        let chunk_res = chunk_res.context("chunk multicall failed")?;
        indexed.push((idx, chunk_res));
    }
    if indexed.len() != num_chunks {
        anyhow::bail!(
            "multicall chunked join incomplete: expected {num_chunks} chunks, got {}",
            indexed.len()
        );
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
    use super::{
        MULTICALL_CHUNK, global_multicall_available_permits, is_retryable_rpc_error,
        plan_batch_call_budget,
    };

    #[test]
    fn global_multicall_admission_matches_chunk_limit() {
        assert_eq!(global_multicall_available_permits(), 4);
    }

    #[test]
    fn retries_transient_rpc_responses_only() {
        for message in [
            "error code -32000: header not found",
            "error code 26: Unknown block",
        ] {
            assert!(is_retryable_rpc_error(&anyhow::anyhow!(message)));
        }
        for message in [
            "429 Too Many Requests",
            "provider usage limit exceeded",
            "error code 15: Too many request, try again later",
        ] {
            assert!(!is_retryable_rpc_error(&anyhow::anyhow!(message)));
        }
        assert!(!is_retryable_rpc_error(&anyhow::anyhow!(
            "execution reverted"
        )));
    }

    #[test]
    fn default_chunk_matches_config_default() {
        assert_eq!(MULTICALL_CHUNK, 200);
        assert_eq!(super::TICK_LENS_MULTICALL_CHUNK, 12);
        assert!(super::TICK_LENS_MULTICALL_CHUNK < MULTICALL_CHUNK);
    }

    #[test]
    fn plan_batch_budget_enables_parallel_chunks() {
        assert_eq!(plan_batch_call_budget(200), 800);
        assert_eq!(plan_batch_call_budget(0), 4);
    }

    #[test]
    fn chunking_splits_large_batches() {
        let items_len = 450usize;
        let chunk = MULTICALL_CHUNK;
        let chunk_count = items_len.div_ceil(chunk);
        assert_eq!(chunk_count, 3);
    }

    #[test]
    fn rate_limited_errors_are_not_bisected() {
        let err = anyhow::anyhow!("429 Too Many Requests");
        assert!(super::is_rate_limited_rpc_error(&err));
        assert!(!is_retryable_rpc_error(&err));
    }

    #[test]
    fn rate_limit_detected_through_anyhow_context_wrapper() {
        // Live bug: chunked multicall wraps the RPC body with `.context(...)`,
        // and plain Display only shows the outer layer — bisect storms followed.
        let err = anyhow::anyhow!("error code 15: Too many request, try again later")
            .context("chunk multicall failed");
        assert!(super::is_rate_limited_rpc_error(&err));
        assert!(!is_retryable_rpc_error(&err));
        let err429 = anyhow::anyhow!("HTTP error 429 with body: Rate limit (1200rqs/60s) reached")
            .context("chunk multicall failed");
        assert!(super::is_rate_limited_rpc_error(&err429));
        assert!(!is_retryable_rpc_error(&err429));
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
