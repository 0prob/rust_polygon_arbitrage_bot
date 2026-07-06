use std::sync::{LazyLock, Once};
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::U256;

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
        other => U256::from(10u128).pow(U256::from(other as u32)),
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

/// Hot-path scale lookup — avoids `pow` for the dominant 18-decimal case.
#[inline]
#[must_use]
pub fn ten_pow_u256_cached(decimals: u8) -> U256 {
    if decimals == 18 {
        ONE
    } else {
        ten_pow_u256(decimals)
    }
}

static RAYON_GLOBAL_INIT: Once = Once::new();
static RAYON_THREADS: LazyLock<usize> = LazyLock::new(|| {
    // Ensure global rayon is capped to 1 thread as soon as we query worker count
    // (defense against stray par_iter before our named pools are first used).
    init_rayon_global_pool();
    std::env::var("RAYON_NUM_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(compute_default_rayon_threads)
});
static CPU_POOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    init_rayon_global_pool();
    build_named_cpu_pool("rpbot-hf", hf_worker_threads()).expect("hf cpu pool should initialize")
});
static LF_CPU_POOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    init_rayon_global_pool();
    build_named_cpu_pool("rpbot-lf", lf_worker_threads()).expect("lf cpu pool should initialize")
});

/// Cap the implicit global pool so stray `par_iter`/`join` without `install()` cannot
/// spin up a full CPU pool (per rayon 0.13 docs: custom pools require `install()`).
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

/// Run CPU-bound HF work on [`cpu_pool`].
pub fn run_cpu<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send,
    R: Send,
{
    cpu_pool().install(f)
}

/// Run CPU-bound LF work on [`lf_cpu_pool`].
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

/// Truncate to at most `max_chars` Unicode scalar values (not bytes).
/// Avoids full scan of long strings.
#[inline]
#[must_use]
pub fn truncate_str(s: &str, max_chars: usize) -> String {
    let mut iter = s.chars();
    // Collect up to max_chars; if no more chars, return as-is (no alloc if possible, but collect does).
    let collected: String = iter.by_ref().take(max_chars).collect();
    if iter.next().is_none() {
        collected
    } else if max_chars == 0 {
        "…".to_string()
    } else {
        // Had more chars: drop the last collected char to make room for … (matches prior take(max-1)+…)
        let mut short = collected;
        short.pop();
        short.push('…');
        short
    }
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
