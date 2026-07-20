//! Shared [`reqwest::Client`] construction (connection pooling, timeouts, TLS).
//!
//! Per reqwest 0.13 docs: reuse one `Client` per role, set `user_agent`, and call
//! `no_proxy()` so env proxy vars cannot redirect RPC/API traffic.
//! TLS is rustls (aligned with alloy's default); TCP_NODELAY + 90s idle for RPC.

use std::time::Duration;

use reqwest::{Client, redirect::Policy};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const TCP_KEEPALIVE: Duration = Duration::from_secs(30);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

/// Tunables for a pooled HTTP client.
#[derive(Clone, Copy, Debug)]
pub struct HttpClientOpts {
    /// Total per-request deadline (connect + headers + body).
    pub timeout: Duration,
    /// Max idle connections kept per host (reqwest default is unlimited).
    pub pool_max_idle_per_host: usize,
    /// Max redirect hops; `0` disables redirects (preferred for JSON-RPC POST).
    pub max_redirects: usize,
}

/// Build a connection-pooled async client.
pub fn build(opts: HttpClientOpts) -> Result<Client, reqwest::Error> {
    let redirect = if opts.max_redirects == 0 {
        Policy::none()
    } else {
        Policy::limited(opts.max_redirects)
    };

    Client::builder()
        .user_agent(USER_AGENT)
        .no_proxy()
        .timeout(opts.timeout)
        .connect_timeout(CONNECT_TIMEOUT)
        .tcp_keepalive(TCP_KEEPALIVE)
        .tcp_nodelay(true)
        .pool_max_idle_per_host(opts.pool_max_idle_per_host)
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .redirect(redirect)
        .build()
}

/// Like [`build`], but for process-lifetime static clients (startup failure is fatal).
#[must_use]
#[allow(clippy::unwrap_used)] // ponytail: fatal at process init; no recovery path
pub fn build_static(opts: HttpClientOpts, label: &'static str) -> Client {
    build(opts).unwrap_or_else(|_| unreachable!("{label}"))
}
