//! HyperSync client wrapper (optional at compile time via feature `hypersync`).
//!
//! Complements (does not replace) the Envio HyperIndex data:
//! - **PostgreSQL** — pool/token discovery metadata (LF path, direct SQL)
//! - **HyperSync** — fast head feed, receipts, historical log scans
//!
//! Disable with `cargo build --no-default-features` to skip the heavy
//! `hypersync-client` → arrow/parquet dependency tree (~minutes of compile).

#[cfg(feature = "hypersync")]
mod client;
#[cfg(feature = "hypersync")]
pub use client::*;

#[cfg(not(feature = "hypersync"))]
mod stub;
#[cfg(not(feature = "hypersync"))]
pub use stub::*;
