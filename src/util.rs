use std::sync::{LazyLock, Once};
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::{U256, U512};

use crate::core::math::fixed_point::ONE;

#[inline]
#[must_use]
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Token decimal scale (10^decimals) with fast paths for common ERC-20 precisions.
#[inline]
#[must_use]
pub fn ten_pow_u256(decimals: u8) -> U256 {
    match decimals {
        0 => U256::from(1u8),
        2 => U256::from(100u128),
        6 => U256::from(1_000_000u128),
        8 => U256::from(100_000_000u128),
        9 => U256::from(1_000_000_000u128),
        12 => U256::from(1_000_000_000_000u128),
        18 => ONE,
        // Avoid U256::from(other) in the hot path — use the raw limb directly.
        other => {
            const BASE: U256 = U256::from_limbs([10, 0, 0, 0]);
            BASE.pow(U256::from_limbs([u64::from(other), 0, 0, 0]))
        }
    }
}

/// Sign-extend an ABI-encoded int24 tick (lower 3 bytes of a 32-byte word).
#[inline]
#[must_use]
pub fn sign_extend_tick24(tick_word: U256) -> i32 {
    let tick_raw = (tick_word & U256::from(0xFF_FFFFu64)).as_limbs()[0] as u32;
    if tick_raw & 0x80_0000 != 0 {
        (tick_raw | !0xFF_FFFF) as i32
    } else {
        tick_raw as i32
    }
}

static TEN_POW_BY_DECIMALS: LazyLock<[U256; 31]> =
    LazyLock::new(|| std::array::from_fn(|d| ten_pow_u256(d as u8)));

/// Hot-path scale lookup — table for 0..=30 (MAX_SUPPORTED_TOKEN_DECIMALS).
#[inline]
#[must_use]
pub fn ten_pow_u256_cached(decimals: u8) -> U256 {
    if decimals <= 30 {
        TEN_POW_BY_DECIMALS[decimals as usize]
    } else {
        ten_pow_u256(decimals)
    }
}

static RAYON_GLOBAL_INIT: Once = Once::new();
static RAYON_THREADS: LazyLock<usize> = LazyLock::new(|| {
    // Ensure global rayon is capped to 1 thread as soon as we query worker count
    // (defense against stray par_iter before our named pools are first used).
    init_rayon_global_pool();
    // Reuses Rayon's env name for *our* HF+LF pool budget (global stays 1 via build_global).
    std::env::var("RAYON_NUM_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(compute_default_rayon_threads)
});
#[allow(clippy::unwrap_used)] // ponytail: fatal at first pool use; misconfig is not recoverable
static CPU_POOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    init_rayon_global_pool();
    build_named_cpu_pool("rpbot-hf", hf_worker_threads())
        .unwrap_or_else(|_| unreachable!("hf cpu pool should initialize"))
});
#[allow(clippy::unwrap_used)]
static LF_CPU_POOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    init_rayon_global_pool();
    build_named_cpu_pool("rpbot-lf", lf_worker_threads())
        .unwrap_or_else(|_| unreachable!("lf cpu pool should initialize"))
});

/// Cap the implicit global pool so stray `par_iter`/`join` without pool context cannot
/// spin up a full CPU pool. Per rayon 1.x [`ThreadPool`](rayon::ThreadPool) docs, custom
/// pools need `install`/`spawn` — free `par_iter`/`join` use the ambient (global) registry.
fn init_rayon_global_pool() {
    RAYON_GLOBAL_INIT.call_once(|| {
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .thread_name(|i| format!("rayon-global-{i}"))
            .build_global();
    });
}

fn compute_default_rayon_threads() -> usize {
    let cores = std::thread::available_parallelism().map_or(2, std::num::NonZero::<usize>::get);
    cores.saturating_sub(1).max(1)
}

/// Worker threads for the shared CPU pool (reserve one core for tokio + I/O).
/// Value is computed once (on first access) from RAYON_NUM_THREADS or available_parallelism.
#[must_use]
pub fn rayon_worker_threads() -> usize {
    *RAYON_THREADS
}

/// HF evaluation pool — half of available workers (minimum 1).
#[must_use]
pub fn hf_worker_threads() -> usize {
    let total = rayon_worker_threads();
    (total / 2).max(1)
}

/// LF graph/cycle search pool — remaining workers (minimum 1).
#[must_use]
pub fn lf_worker_threads() -> usize {
    let total = rayon_worker_threads();
    total.saturating_sub(hf_worker_threads()).max(1)
}

#[inline]
#[must_use]
pub fn rayon_parallel_min_work() -> usize {
    rayon_worker_threads().saturating_mul(2).max(4)
}

#[inline]
#[must_use]
pub fn should_use_rayon(len: usize) -> bool {
    len >= rayon_parallel_min_work()
}

fn build_named_cpu_pool(
    prefix: &str,
    threads: usize,
) -> Result<rayon::ThreadPool, rayon::ThreadPoolBuildError> {
    let name = prefix.to_string();
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(move |i| format!("{name}-{i}"))
        .build()
}

/// Rayon pool for HF cycle evaluation and Brent optimization.
#[must_use]
pub fn cpu_pool() -> &'static rayon::ThreadPool {
    &CPU_POOL
}

/// Dedicated pool for LF graph build and cycle enumeration.
#[must_use]
pub fn lf_cpu_pool() -> &'static rayon::ThreadPool {
    &LF_CPU_POOL
}

/// Sync entry into [`cpu_pool`] via `install` (blocks the caller).
/// Prefer [`cpu_pool`]`.spawn` from async code — nested `install` inside `spawn` is
/// redundant (worker TLS already binds the pool; see rayon `ThreadPool::spawn`).
pub fn run_cpu<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send,
    R: Send,
{
    cpu_pool().install(f)
}

/// Sync entry into [`lf_cpu_pool`] via `install` (blocks the caller).
/// Prefer [`lf_cpu_pool`]`.spawn` from async code — see [`run_cpu`].
pub fn run_lf_cpu<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send,
    R: Send,
{
    lf_cpu_pool().install(f)
}

// Powers of two as f64 (exactly representable for integer powers of two).
const F64_2_POW_64: f64 = 18446744073709551616.0;
const F64_2_POW_128: f64 = F64_2_POW_64 * F64_2_POW_64;
const F64_2_POW_192: f64 = F64_2_POW_128 * F64_2_POW_64;

#[inline]
#[must_use]
pub fn u256_to_f64(v: U256) -> f64 {
    let limbs = v.as_limbs();
    if limbs[1] == 0 && limbs[2] == 0 && limbs[3] == 0 {
        limbs[0] as f64
    } else {
        let hi = limbs[3] as f64;
        let mid_hi = limbs[2] as f64;
        let mid_lo = limbs[1] as f64;
        let lo = limbs[0] as f64;
        hi.mul_add(
            F64_2_POW_192,
            mid_hi.mul_add(F64_2_POW_128, mid_lo.mul_add(F64_2_POW_64, lo)),
        )
    }
}

/// Lossless truncation of U512 to U256 (low 256 bits).
/// U512 stores little-endian limbs; we extract the lower 4 limbs (256 bits).
/// **Panics in debug** when the high 256 bits are non-zero (caller guarantees no overflow).
#[inline]
#[must_use]
pub fn u512_to_u256(v: U512) -> U256 {
    let raw = v.as_le_slice();
    debug_assert!(
        raw[32..].iter().all(|&b| b == 0),
        "u512_to_u256: high 256 bits non-zero — value exceeds U256 range"
    );
    let mut buf = [0u8; 32];
    buf.copy_from_slice(&raw[..32]);
    U256::from_le_bytes(buf)
}

/// **Checked** U512 → U256 truncation. Returns `None` when the value exceeds U256::MAX.
/// Prefer this over [`u512_to_u256`] at call sites where overflow is possible.
#[inline]
#[must_use]
pub fn u512_to_u256_checked(v: U512) -> Option<U256> {
    let raw = v.as_le_slice();
    if raw[32..].iter().any(|&b| b != 0) {
        return None;
    }
    let mut buf = [0u8; 32];
    buf.copy_from_slice(&raw[..32]);
    Some(U256::from_le_bytes(buf))
}

/// Truncate to at most `max_chars` Unicode scalar values (not bytes).
/// Returns `&str` when no truncation is needed, avoiding allocation.
#[inline]
#[must_use]
pub fn truncate_str(s: &str, max_chars: usize) -> std::borrow::Cow<'_, str> {
    if max_chars == 0 {
        return std::borrow::Cow::Owned('…'.to_string());
    }
    let mut last_kept_end = 0usize;
    for (i, (pos, _)) in s.char_indices().enumerate() {
        if i == max_chars {
            let mut truncated = s[..last_kept_end].to_string();
            truncated.push('…');
            return std::borrow::Cow::Owned(truncated);
        }
        last_kept_end = pos;
    }
    std::borrow::Cow::Borrowed(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rayon_worker_threads_stays_below_core_count() {
        let cores = std::thread::available_parallelism()
            .map(std::num::NonZero::<usize>::get)
            .unwrap_or(2)
            .max(2);
        let workers = rayon_worker_threads().max(1);
        assert!(workers <= cores);
        assert!(workers >= 1);
    }

    #[test]
    fn hf_and_lf_pools_partition_workers() {
        let total = rayon_worker_threads().max(2);
        let hf = hf_worker_threads();
        let lf = lf_worker_threads();
        assert!(hf >= 1);
        assert!(lf >= 1);
        assert_eq!(hf + lf, total);
    }

    #[test]
    fn truncate_str_limits_unicode_chars() {
        assert_eq!(truncate_str("hello", 10), "hello");
        assert_eq!(truncate_str("hello", 5), "hello");
        assert_eq!(truncate_str("hello", 4), "hel…");
        assert_eq!(truncate_str("hello", 1), "…");
        assert_eq!(truncate_str("hello", 0), "…");
        // unicode scalars
        let s = "a😀b😀c";
        assert_eq!(truncate_str(s, 5), s);
        assert_eq!(truncate_str(s, 4), "a😀b…");
        assert_eq!(truncate_str(s, 3), "a😀…");
    }

    #[test]
    fn u256_to_f64_small_and_large() {
        assert_eq!(u256_to_f64(U256::from(0u64)), 0.0);
        assert_eq!(u256_to_f64(U256::from(123u64)), 123.0);
        // 2^64 is exactly representable in f64
        let pow64 = U256::from(1u64) << 64;
        assert_eq!(u256_to_f64(pow64), F64_2_POW_64);
        // larger values are approximate
        let big = U256::from_limbs([0, 0, 0, 1]);
        let fbig = u256_to_f64(big);
        assert!((fbig - F64_2_POW_192).abs() < F64_2_POW_192 * 1e-10);
    }
}

/// Parse JSON bytes using SIMD-accelerated simd-json with a serde_json fallback.
///
/// simd-json processes JSON with SIMD instructions (AVX2/NEON) and writes a tape
/// into the buffer in-place, so this function clones the input once. The clone is
/// still cheaper than serde_json's recursive descent on large payloads.
///
/// # Errors
/// Returns a serde_json error if both parsers fail.
pub fn simd_json_parse<T>(bytes: &[u8]) -> Result<T, serde_json::Error>
where
    T: serde::de::DeserializeOwned,
{
    let mut buf = bytes.to_vec();
    // simd-json fallback path: on error, re-parse with serde_json for better diagnostics.
    simd_json::serde::from_slice::<T>(&mut buf).or_else(|_| serde_json::from_slice(bytes))
}

/// Borrow-preserving SIMD parse for zero-copy `Cow<'a, str>` / `&'a str` fields.
///
/// The caller provides a `&mut Vec<u8>` that simd-json writes its tape into.
/// The returned value borrows from `buf`. No extra heap allocation is performed.
///
/// # Errors
/// Returns a serde_json error if both parsers fail.
pub fn simd_json_parse_borrowed<'de, T>(buf: &'de mut Vec<u8>) -> Result<T, serde_json::Error>
where
    T: serde::de::Deserialize<'de>,
{
    simd_json::serde::from_slice::<T>(buf).or_else(|_| serde_json::from_slice(buf))
}
