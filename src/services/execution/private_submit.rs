use std::sync::LazyLock;
use std::time::Duration;

use alloy::eips::eip2718::Encodable2718;
use alloy::hex;
use alloy::network::Ethereum;
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
use serde_json::Value;

const PROBE_TIMEOUT: Duration = Duration::from_secs(8);
const SUBMIT_TIMEOUT: Duration = Duration::from_secs(15);
const BLOXROUTE_API_URL: &str = "https://api.blxrbdn.com";

static HTTP: LazyLock<Client> =
    LazyLock::new(|| build_static(HttpClientOpts { timeout: SUBMIT_TIMEOUT, pool_max_idle_per_host: 4, max_redirects: 5 }, "private submit"));

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
    let chain_id_ok = match rpc_call::<Vec<String>>(client, url, "eth_chainId", vec![]).await {
        Ok(v) => matches!(v.as_ref(), Some(JsonRpcResult::Hex(s)) if s.eq_ignore_ascii_case("0x89")),
        Err(_) => false,
    };

    let (supports_private_rpc_method, private_method_error) = match rpc_call(
        client,
        url,
        "eth_sendRawTransactionPrivate",
        vec!["0x00".to_string()],
    )
    .await
    {
        Ok(_) => (true, None),
        Err(e) => {
            let msg = e.to_string();
            // Distinguish "method exists but tx invalid" from "method missing".
            let exists = msg.contains("invalid")
                || msg.contains("rlp")
                || msg.contains("transaction")
                || msg.contains("not accepted");
            (exists, Some(msg))
        }
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
    let Ok(resp) = post_bloxroute(&body, auth_header, PROBE_TIMEOUT).await else {
        return false;
    };
    if resp.status().as_u16() == 401 {
        return false;
    }
    let Ok(parsed) = resp.json::<JsonRpcResponse>().await else {
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
    let body = resp
        .text()
        .await
        .context("bloxroute response read failed")?;
    parse_bloxroute_submit_response(status, &body)
}

fn parse_bloxroute_submit_response(
    status: reqwest::StatusCode,
    body: &str,
) -> anyhow::Result<B256> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        anyhow::bail!("bloxroute returned empty response body (HTTP {status})");
    }

    let value: Value = serde_json::from_str(trimmed).with_context(|| {
        format!(
            "bloxroute response decode failed (HTTP {status}): {}",
            preview_response_body(trimmed)
        )
    })?;

    if let Some(err) = value.get("error") {
        let message = jsonrpc_error_message(err).unwrap_or_else(|| err.to_string());
        anyhow::bail!("bloxroute polygon_private_tx: {message}");
    }
    if !status.is_success() {
        anyhow::bail!(
            "bloxroute polygon_private_tx: HTTP {status} without JSON-RPC error: {}",
            preview_response_body(trimmed)
        );
    }

    let hash_str = value
        .get("result")
        .and_then(extract_bloxroute_tx_hash)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "bloxroute response missing tx_hash: {}",
                preview_response_body(trimmed)
            )
        })?;
    parse_bloxroute_tx_hash(hash_str)
}

fn jsonrpc_error_message(err: &Value) -> Option<String> {
    err.get("message").and_then(|msg| match msg {
        Value::String(s) => Some(s.clone()),
        Value::Null => None,
        other => Some(other.to_string()),
    })
}

fn extract_bloxroute_tx_hash(result: &Value) -> Option<&str> {
    match result {
        Value::String(hash) => Some(hash.as_str()),
        Value::Object(map) => ["tx_hash", "txHash", "transactionHash", "hash"]
            .iter()
            .find_map(|key| map.get(*key))
            .and_then(Value::as_str),
        _ => None,
    }
}

fn preview_response_body(body: &str) -> String {
    const MAX: usize = 300;
    if body.len() <= MAX {
        body.to_string()
    } else {
        format!("{}...", &body[..MAX])
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
    let parsed: JsonRpcResponse = resp.json().await?;
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

#[must_use]
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
pub async fn sign_tx_to_raw(
    tx: TransactionRequest,
    signer: &PrivateKeySigner,
    chain_id: u64,
) -> anyhow::Result<Vec<u8>> {
    use alloy::consensus::{SignableTransaction, TxEip1559, TxEnvelope};
    use alloy::network::TxSigner;

    let mut unsigned = TxEip1559 {
        chain_id,
        nonce: tx.nonce.ok_or_else(|| anyhow::anyhow!("nonce required"))?,
        gas_limit: tx
            .gas
            .ok_or_else(|| anyhow::anyhow!("gas_limit required"))?,
        max_fee_per_gas: tx
            .max_fee_per_gas
            .ok_or_else(|| anyhow::anyhow!("max_fee_per_gas required"))?,
        max_priority_fee_per_gas: tx
            .max_priority_fee_per_gas
            .ok_or_else(|| anyhow::anyhow!("max_priority_fee_per_gas required"))?,
        to: tx
            .to
            .ok_or_else(|| anyhow::anyhow!("to address required"))?,
        value: tx.value.unwrap_or_default(),
        access_list: tx.access_list.unwrap_or_default(),
        input: tx.input.into_input().unwrap_or_default(),
    };

    let sig = signer
        .sign_transaction(&mut unsigned)
        .await
        .context("tx signing failed")?;
    let envelope = TxEnvelope::Eip1559(unsigned.into_signed(sig));
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

async fn rpc_call<P: serde::Serialize>(
    client: &Client,
    url: &str,
    method: &str,
    params: P,
) -> anyhow::Result<Option<JsonRpcResult>> {
    let body = JsonRpcRequest {
        jsonrpc: "2.0",
        id: 1,
        method,
        params,
    };
    let resp = client
        .post(url)
        .timeout(PROBE_TIMEOUT)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    let parsed: JsonRpcResponse = resp.json().await?;
    if let Some(err) = parsed.error {
        anyhow::bail!("{}", err.message);
    }
    Ok(parsed.result)
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
    fn parse_bloxroute_submit_response_accepts_documented_shapes() {
        let err = r#"{"id":1,"error":{"code":-32602,"message":"The transaction is invalid."},"jsonrpc":"2.0"}"#;
        let err_out = parse_bloxroute_submit_response(reqwest::StatusCode::BAD_REQUEST, err);
        assert!(
            err_out
                .expect_err("bad request should produce an error")
                .to_string()
                .contains("The transaction is invalid.")
        );

        let ok = r#"{"jsonrpc":"2.0","id":"1","result":{"tx_hash":"ffd59870844e5bfa54a69ab0123456789abcdef0123456789abcdef012345678"}}"#;
        let hash =
            parse_bloxroute_submit_response(reqwest::StatusCode::OK, ok).expect("snake_case");
        assert_eq!(
            hash.to_string(),
            "0xffd59870844e5bfa54a69ab0123456789abcdef0123456789abcdef012345678"
        );

        let camel = r#"{"jsonrpc":"2.0","id":1,"result":{"txHash":"ffd59870844e5bfa54a69ab0123456789abcdef0123456789abcdef012345678"}}"#;
        let hash =
            parse_bloxroute_submit_response(reqwest::StatusCode::OK, camel).expect("camelCase");
        assert_eq!(
            hash.to_string(),
            "0xffd59870844e5bfa54a69ab0123456789abcdef0123456789abcdef012345678"
        );
    }
}
