use std::sync::LazyLock;
use std::time::Duration;

use alloy::eips::eip2718::Encodable2718;
use alloy::hex;
use alloy::network::{Ethereum, EthereumWallet, NetworkTransactionBuilder, TransactionBuilder};
use alloy::primitives::B256;
use alloy::providers::Provider;
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use reqwest::Client;

use crate::infra::http::{HttpClientOpts, build_static};
use crate::infra::json_rpc::{
    BloxroutePrivateTxParams, BloxrouteRequest, JsonRpcRequest, JsonRpcResponse, JsonRpcResult,
};

const BLOXROUTE_PROBE_TIMEOUT: Duration = Duration::from_secs(8);
const SUBMIT_TIMEOUT: Duration = Duration::from_secs(15);
const BLOXROUTE_API_URL: &str = "https://api.blxrbdn.com";

static HTTP: LazyLock<Client> = LazyLock::new(|| {
    build_static(
        HttpClientOpts {
            timeout: SUBMIT_TIMEOUT,
            pool_max_idle_per_host: 4,
            max_redirects: 0,
        },
        "private submit",
    )
});

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrivateSubmitMode {
    /// Standard `eth_sendRawTransaction` on configured RPC (public mempool).
    Standard,
    /// Polygon relay `eth_sendRawTransactionPrivate` (VeBloP private path).
    PolygonPrivateRpc,
    /// bloXroute BDN `polygon_private_tx` (paid, requires auth header).
    Bloxroute,
}

#[derive(Debug, Clone)]
pub struct PrivateSubmitProbe {
    pub url: String,
    pub chain_id_ok: bool,
    pub supports_private_rpc_method: bool,
    pub private_method_error: Option<String>,
    pub recommended_mode: PrivateSubmitMode,
}

/// Probe an RPC URL for private-transaction capabilities (no wallet required).
pub async fn probe_submit_endpoint(url: &str) -> PrivateSubmitProbe {
    let client = &*HTTP;
    let chain_id_ok = match client
        .post(url)
        .timeout(BLOXROUTE_PROBE_TIMEOUT)
        .json(&JsonRpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "eth_chainId",
            params: Vec::<String>::new(),
        })
        .send()
        .await
        .and_then(|resp| resp.error_for_status())
    {
        Ok(resp) => match resp.bytes().await {
            Ok(bytes) => serde_json::from_slice::<JsonRpcResponse<'_>>(&bytes)
                .ok()
                .and_then(|parsed| parsed.result)
                .and_then(|v| match v {
                    JsonRpcResult::Hex(s) => Some(s),
                    _ => None,
                })
                .is_some_and(|s| s.eq_ignore_ascii_case("0x89")),
            Err(_) => false,
        },
        Err(_) => false,
    };

    let (supports_private_rpc_method, private_method_error) = match client
        .post(url)
        .timeout(BLOXROUTE_PROBE_TIMEOUT)
        .json(&JsonRpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "eth_sendRawTransactionPrivate",
            params: vec!["0x00".to_string()],
        })
        .send()
        .await
        .and_then(|resp| resp.error_for_status())
    {
        Ok(resp) => match resp.bytes().await {
            Ok(bytes) => match serde_json::from_slice::<JsonRpcResponse<'_>>(&bytes) {
                Ok(parsed) => match parsed.error {
                    Some(err) => {
                        let msg = err.message.to_string();
                        // Distinguish "method exists but tx invalid" from "method missing".
                        let exists = msg.contains("invalid")
                            || msg.contains("rlp")
                            || msg.contains("transaction")
                            || msg.contains("not accepted");
                        (exists, Some(msg))
                    }
                    None => (true, None),
                },
                Err(e) => (false, Some(e.to_string())),
            },
            Err(e) => (false, Some(e.to_string())),
        },
        Err(e) => (false, Some(e.to_string())),
    };

    let recommended_mode = if supports_private_rpc_method {
        PrivateSubmitMode::PolygonPrivateRpc
    } else {
        PrivateSubmitMode::Standard
    };

    PrivateSubmitProbe {
        url: url.to_string(),
        chain_id_ok,
        supports_private_rpc_method,
        private_method_error,
        recommended_mode,
    }
}

pub async fn probe_bloxroute_auth(auth_header: &str) -> bool {
    let body = BloxrouteRequest {
        jsonrpc: "2.0",
        id: 1,
        method: "polygon_private_tx",
        params: BloxroutePrivateTxParams { transaction: "00" },
    };
    let Ok(resp) = post_bloxroute(&body, auth_header, BLOXROUTE_PROBE_TIMEOUT).await else {
        return false;
    };
    if resp.status().as_u16() == 401 {
        return false;
    }
    let Ok(bytes) = resp.bytes().await else {
        return false;
    };
    let Ok(parsed) = serde_json::from_slice::<JsonRpcResponse<'_>>(&bytes) else {
        return false;
    };
    // Auth OK when the gateway accepts the method and rejects only the dummy tx bytes.
    parsed
        .error
        .as_ref()
        .is_some_and(|e| e.message.contains("invalid") || e.message.contains("transaction"))
}

/// Submit signed raw transaction bytes via bloXroute `polygon_private_tx`.
pub async fn submit_bloxroute_private(raw_tx: &[u8], auth_header: &str) -> anyhow::Result<B256> {
    let tx_hex = hex::encode(raw_tx);
    let body = BloxrouteRequest {
        jsonrpc: "2.0",
        id: 1,
        method: "polygon_private_tx",
        params: BloxroutePrivateTxParams {
            transaction: &tx_hex,
        },
    };
    let resp = post_bloxroute(&body, auth_header, SUBMIT_TIMEOUT).await?;
    let status = resp.status();
    // bloXroute returns HTTP 400 for JSON-RPC application errors; the body still has details.
    let bytes = resp
        .bytes()
        .await
        .context("bloxroute response read failed")?;
    parse_bloxroute_submit_response(status, &bytes)
}

fn parse_bloxroute_submit_response(
    status: reqwest::StatusCode,
    body: &[u8],
) -> anyhow::Result<B256> {
    let trimmed = body.trim_ascii();
    if trimmed.is_empty() {
        anyhow::bail!("bloxroute returned empty response body (HTTP {status})");
    }

    let parsed: JsonRpcResponse<'_> = serde_json::from_slice(trimmed).with_context(|| {
        format!(
            "bloxroute response decode failed (HTTP {status}): {}",
            preview_response_body(trimmed)
        )
    })?;

    if let Some(err) = parsed.error {
        anyhow::bail!("bloxroute polygon_private_tx: {}", err.message);
    }
    if !status.is_success() {
        anyhow::bail!(
            "bloxroute polygon_private_tx: HTTP {status} without JSON-RPC error: {}",
            preview_response_body(trimmed)
        );
    }

    let hash_str = parsed
        .result
        .as_ref()
        .and_then(JsonRpcResult::as_tx_hash)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "bloxroute response missing tx_hash: {}",
                preview_response_body(trimmed)
            )
        })?;
    parse_bloxroute_tx_hash(hash_str)
}

fn preview_response_body(body: &[u8]) -> String {
    const MAX: usize = 300;
    let preview = &body[..body.len().min(MAX)];
    let text = String::from_utf8_lossy(preview);
    if body.len() <= MAX {
        text.into_owned()
    } else {
        format!("{text}...")
    }
}

fn normalize_bloxroute_auth(auth_header: &str) -> &str {
    let trimmed = auth_header.trim();
    trimmed
        .strip_prefix("Authorization:")
        .or_else(|| trimmed.strip_prefix("authorization:"))
        .map(str::trim)
        .unwrap_or(trimmed)
}

fn parse_bloxroute_tx_hash(hash: &str) -> anyhow::Result<B256> {
    let prefixed = if hash.starts_with("0x") {
        hash.to_string()
    } else {
        format!("0x{hash}")
    };
    prefixed.parse().context("invalid tx_hash from bloxroute")
}

async fn post_bloxroute(
    body: &BloxrouteRequest<'_>,
    auth_header: &str,
    timeout: Duration,
) -> anyhow::Result<reqwest::Response> {
    HTTP.post(BLOXROUTE_API_URL)
        .timeout(timeout)
        .header("Authorization", normalize_bloxroute_auth(auth_header))
        .json(body)
        .send()
        .await
        .context("bloxroute request failed")
}

/// Submit via `eth_sendRawTransactionPrivate` JSON-RPC.
pub async fn submit_polygon_private_rpc(url: &str, raw_tx: &[u8]) -> anyhow::Result<B256> {
    let raw_hex = format!("0x{}", hex::encode(raw_tx));
    let body = JsonRpcRequest {
        jsonrpc: "2.0",
        id: 1,
        method: "eth_sendRawTransactionPrivate",
        params: vec![raw_hex],
    };
    let resp = HTTP
        .post(url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    let bytes = resp.bytes().await?;
    let parsed: JsonRpcResponse<'_> = serde_json::from_slice(&bytes)?;
    if let Some(err) = parsed.error {
        anyhow::bail!("eth_sendRawTransactionPrivate: {}", err.message);
    }
    let hash_str = parsed
        .result
        .as_ref()
        .and_then(JsonRpcResult::as_tx_hash)
        .ok_or_else(|| anyhow::anyhow!("private RPC response missing tx hash"))?;
    hash_str.parse().context("invalid tx hash")
}

/// Signed-raw private paths need `chain_id` for EIP-155 signing; must not fall back to public mempool.
#[must_use]
pub fn private_submit_mode_requires_chain_id(mode: PrivateSubmitMode) -> bool {
    matches!(
        mode,
        PrivateSubmitMode::Bloxroute | PrivateSubmitMode::PolygonPrivateRpc
    )
}

pub fn resolve_submit_mode(
    private_rpc_url: Option<&str>,
    bloxroute_auth: Option<&str>,
    probe: Option<&PrivateSubmitProbe>,
) -> PrivateSubmitMode {
    if bloxroute_auth.is_some() {
        return PrivateSubmitMode::Bloxroute;
    }
    if let Some(p) = probe
        && p.supports_private_rpc_method
    {
        return PrivateSubmitMode::PolygonPrivateRpc;
    }
    if private_rpc_url.is_some() {
        // URL configured but private method not verified — still prefer it over public execution RPC
        // (Polygon official private mempool uses standard eth_sendRawTransaction on private URL).
        return PrivateSubmitMode::Standard;
    }
    PrivateSubmitMode::Standard
}

/// Configuration for private transaction submission.
#[derive(Debug, Clone)]
pub struct PrivateSubmitConfig {
    pub mode: PrivateSubmitMode,
    pub signer: PrivateKeySigner,
    pub chain_id: u64,
    pub private_url: Option<String>,
    pub bloxroute_auth: Option<String>,
}

/// Sign a [`TransactionRequest`] and return EIP-2718-encoded raw bytes
/// suitable for `eth_sendRawTransaction*` or `polygon_private_tx`.
///
/// Uses Alloy's `TransactionRequest::build` + `Encodable2718` (official raw-tx path).
pub async fn sign_tx_to_raw(
    tx: TransactionRequest,
    signer: &PrivateKeySigner,
    chain_id: u64,
) -> anyhow::Result<Vec<u8>> {
    let wallet = EthereumWallet::from(signer.clone());
    let envelope = tx
        .with_chain_id(chain_id)
        .build(&wallet)
        .await
        .context("tx signing failed")?;
    Ok(envelope.encoded_2718())
}

/// Dispatch a signed raw transaction to the configured private submit endpoint.
pub async fn submit_signed_raw(raw: &[u8], cfg: &PrivateSubmitConfig) -> anyhow::Result<B256> {
    match cfg.mode {
        PrivateSubmitMode::Bloxroute => {
            let auth = cfg
                .bloxroute_auth
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("bloxroute_auth required for Bloxroute mode"))?;
            submit_bloxroute_private(raw, auth).await
        }
        PrivateSubmitMode::PolygonPrivateRpc => {
            let url = cfg.private_url.as_deref().ok_or_else(|| {
                anyhow::anyhow!("private_url required for PolygonPrivateRpc mode")
            })?;
            submit_polygon_private_rpc(url, raw).await
        }
        PrivateSubmitMode::Standard => {
            anyhow::bail!("submit_signed_raw called with Standard mode — use provider path")
        }
    }
}
/// Fallback: standard provider send (public or private URL with normal JSON-RPC).
pub async fn submit_via_provider<P: Provider<Ethereum>>(
    provider: &P,
    tx: alloy::rpc::types::TransactionRequest,
) -> anyhow::Result<B256> {
    let pending = provider.send_transaction(tx).await?;
    Ok(*pending.tx_hash())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, U256};

    #[tokio::test]
    async fn sign_tx_to_raw_produces_eip1559_type_byte() {
        // Anvil account #0 — local signing only, no network.
        let signer: PrivateKeySigner =
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
                .parse()
                .expect("test key");
        let tx = TransactionRequest::default()
            .to(Address::repeat_byte(0x11))
            .nonce(0)
            .gas_limit(21_000)
            .max_fee_per_gas(20_000_000_000)
            .max_priority_fee_per_gas(1_000_000_000)
            .value(U256::from(1u64));
        let raw = sign_tx_to_raw(tx, &signer, 137)
            .await
            .expect("sign eip1559");
        assert_eq!(raw.first().copied(), Some(0x02));
    }

    #[test]
    fn bloxroute_request_matches_official_shape() {
        let body = BloxrouteRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "polygon_private_tx",
            params: BloxroutePrivateTxParams {
                transaction: "abcdef",
            },
        };
        let json = serde_json::to_string(&body).expect("serialize bloxroute request");
        assert!(json.contains("\"method\":\"polygon_private_tx\""));
        assert!(json.contains("\"transaction\":\"abcdef\""));
        assert!(!json.contains("\"0x\""));
    }

    #[test]
    fn normalize_bloxroute_auth_strips_prefix_and_whitespace() {
        assert_eq!(
            normalize_bloxroute_auth("  Authorization: abc123 \n"),
            "abc123"
        );
        assert_eq!(normalize_bloxroute_auth("abc123"), "abc123");
    }

    #[test]
    fn parse_bloxroute_tx_hash_accepts_with_or_without_0x() {
        let hash = "ffd59870844e5bfa54a69ab0123456789abcdef0123456789abcdef012345678";
        let with_prefix = parse_bloxroute_tx_hash(hash).expect("hash without 0x");
        let prefixed = parse_bloxroute_tx_hash(&format!("0x{hash}")).expect("hash with 0x");
        assert_eq!(with_prefix, prefixed);
    }

    #[test]
    fn resolve_submit_mode_prefers_bloxroute_when_auth_present() {
        assert_eq!(
            resolve_submit_mode(Some("https://private.example"), Some("auth"), None),
            PrivateSubmitMode::Bloxroute
        );
    }

    #[test]
    fn resolve_submit_mode_uses_verified_private_rpc_capability() {
        let probe = PrivateSubmitProbe {
            url: "https://private.example".into(),
            chain_id_ok: true,
            supports_private_rpc_method: true,
            private_method_error: Some("invalid transaction".into()),
            recommended_mode: PrivateSubmitMode::PolygonPrivateRpc,
        };
        assert_eq!(
            resolve_submit_mode(Some("https://private.example"), None, Some(&probe)),
            PrivateSubmitMode::PolygonPrivateRpc
        );
    }

    #[test]
    fn private_submit_mode_requires_chain_id_for_signed_raw_paths() {
        assert!((private_submit_mode_requires_chain_id(PrivateSubmitMode::Bloxroute)));
        assert!(private_submit_mode_requires_chain_id(
            PrivateSubmitMode::PolygonPrivateRpc
        ));
        assert!(!private_submit_mode_requires_chain_id(
            PrivateSubmitMode::Standard
        ));
    }

    #[test]
    fn parse_bloxroute_submit_response_accepts_documented_shapes() {
        let err = br#"{"id":1,"error":{"code":-32602,"message":"The transaction is invalid."},"jsonrpc":"2.0"}"#;
        let err_out = parse_bloxroute_submit_response(reqwest::StatusCode::BAD_REQUEST, err);
        assert!(
            err_out
                .expect_err("bad request should produce an error")
                .to_string()
                .contains("The transaction is invalid.")
        );

        let ok = br#"{"jsonrpc":"2.0","id":"1","result":{"tx_hash":"ffd59870844e5bfa54a69ab0123456789abcdef0123456789abcdef012345678"}}"#;
        let hash =
            parse_bloxroute_submit_response(reqwest::StatusCode::OK, ok).expect("snake_case");
        assert_eq!(
            hash.to_string(),
            "0xffd59870844e5bfa54a69ab0123456789abcdef0123456789abcdef012345678"
        );

        let camel = br#"{"jsonrpc":"2.0","id":1,"result":{"txHash":"ffd59870844e5bfa54a69ab0123456789abcdef0123456789abcdef012345678"}}"#;
        let hash =
            parse_bloxroute_submit_response(reqwest::StatusCode::OK, camel).expect("camelCase");
        assert_eq!(
            hash.to_string(),
            "0xffd59870844e5bfa54a69ab0123456789abcdef0123456789abcdef012345678"
        );
    }
}
