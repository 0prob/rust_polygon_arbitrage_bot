use std::sync::LazyLock;
use std::time::Duration;

use alloy::eips::eip2718::Encodable2718;
use alloy::hex;
use alloy::network::Ethereum;
use alloy::primitives::B256;
use alloy::providers::Provider;
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

const PROBE_TIMEOUT: Duration = Duration::from_secs(8);
const SUBMIT_TIMEOUT: Duration = Duration::from_secs(15);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

static PROBE_HTTP: LazyLock<Client> = LazyLock::new(|| {
    Client::builder()
        .timeout(PROBE_TIMEOUT)
        .connect_timeout(CONNECT_TIMEOUT)
        .pool_max_idle_per_host(4)
        .build()
        .expect("private submit probe reqwest client")
});

static SUBMIT_HTTP: LazyLock<Client> = LazyLock::new(|| {
    Client::builder()
        .timeout(SUBMIT_TIMEOUT)
        .connect_timeout(CONNECT_TIMEOUT)
        .pool_max_idle_per_host(4)
        .build()
        .expect("private submit reqwest client")
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
    pub notes: Vec<String>,
}

#[derive(Serialize)]
struct JsonRpcRequest<'a> {
    jsonrpc: &'static str,
    id: u32,
    method: &'a str,
    params: serde_json::Value,
}

#[derive(Deserialize)]
struct JsonRpcResponse {
    result: Option<serde_json::Value>,
    error: Option<JsonRpcError>,
}

#[derive(Deserialize)]
struct JsonRpcError {
    message: String,
}

/// Probe an RPC URL for private-transaction capabilities (no wallet required).
pub async fn probe_submit_endpoint(url: &str) -> PrivateSubmitProbe {
    let client = &*PROBE_HTTP;
    let mut notes = Vec::new();
    let chain_id_ok = match rpc_call(client, url, "eth_chainId", serde_json::json!([])).await {
        Ok(v) => v
            .and_then(|r| r.as_str().map(String::from))
            .is_some_and(|id| id.eq_ignore_ascii_case("0x89")),
        Err(e) => {
            notes.push(format!("eth_chainId failed: {e}"));
            false
        }
    };

    let (supports_private_rpc_method, private_method_error) = match rpc_call(
        client,
        url,
        "eth_sendRawTransactionPrivate",
        serde_json::json!(["0x00"]),
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

    if !supports_private_rpc_method {
        notes.push(
            "eth_sendRawTransactionPrivate unavailable — use Polygon Private Mempool signup \
             (polygon.technology blog Apr 2026) or bloXroute polygon_private_tx (paid)"
                .into(),
        );
    }

    PrivateSubmitProbe {
        url: url.to_string(),
        chain_id_ok,
        supports_private_rpc_method,
        private_method_error,
        recommended_mode,
        notes,
    }
}

pub async fn probe_bloxroute_auth(auth_header: &str) -> bool {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "polygon_private_tx",
        "params": { "transaction": "00" }
    });
    PROBE_HTTP
        .post("https://api.blxrbdn.com/")
        .header("Authorization", auth_header)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .is_ok_and(|r| r.status().is_success() || r.status().as_u16() == 400)
}

/// Submit signed raw transaction bytes via bloXroute `polygon_private_tx`.
pub async fn submit_bloxroute_private(raw_tx: &[u8], auth_header: &str) -> anyhow::Result<B256> {
    let tx_hex = hex::encode(raw_tx);
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "polygon_private_tx",
        "params": { "transaction": tx_hex }
    });
    let resp = SUBMIT_HTTP
        .post("https://api.blxrbdn.com/")
        .header("Authorization", auth_header)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    let parsed: JsonRpcResponse = resp.json().await?;
    if let Some(err) = parsed.error {
        anyhow::bail!("bloxroute polygon_private_tx: {}", err.message);
    }
    let hash_str = parsed
        .result
        .and_then(|v| v.get("tx_hash").and_then(|h| h.as_str().map(String::from)))
        .ok_or_else(|| anyhow::anyhow!("bloxroute response missing tx_hash"))?;
    hash_str
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid tx_hash from bloxroute: {e}"))
}

/// Submit via `eth_sendRawTransactionPrivate` JSON-RPC.
pub async fn submit_polygon_private_rpc(url: &str, raw_tx: &[u8]) -> anyhow::Result<B256> {
    let raw_hex = format!("0x{}", hex::encode(raw_tx));
    let body = JsonRpcRequest {
        jsonrpc: "2.0",
        id: 1,
        method: "eth_sendRawTransactionPrivate",
        params: serde_json::json!([raw_hex]),
    };
    let resp = SUBMIT_HTTP
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
        .and_then(|v| v.as_str().map(String::from))
        .ok_or_else(|| anyhow::anyhow!("private RPC response missing tx hash"))?;
    hash_str
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid tx hash: {e}"))
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

pub fn log_probe_report(probe: &PrivateSubmitProbe) {
    info!(
        url = %probe.url,
        chain_id_ok = probe.chain_id_ok,
        private_method = probe.supports_private_rpc_method,
        recommended = ?probe.recommended_mode,
        "private submit probe"
    );
    for note in &probe.notes {
        warn!(note = %note, "private submit");
    }
    if let Some(ref err) = probe.private_method_error {
        warn!(error = %err, "private RPC method probe detail");
    }
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
        gas_limit: tx.gas.ok_or_else(|| anyhow::anyhow!("gas_limit required"))?,
        max_fee_per_gas: tx.max_fee_per_gas.ok_or_else(|| anyhow::anyhow!("max_fee_per_gas required"))?,
        max_priority_fee_per_gas: tx.max_priority_fee_per_gas.ok_or_else(|| anyhow::anyhow!("max_priority_fee_per_gas required"))?,
        to: tx.to.ok_or_else(|| anyhow::anyhow!("to address required"))?,
        value: tx.value.unwrap_or_default(),
        access_list: tx.access_list.unwrap_or_default(),
        input: tx.input.into_input().unwrap_or_default(),
    };

    let sig = signer
        .sign_transaction(&mut unsigned)
        .await
        .map_err(|e| anyhow::anyhow!("tx signing failed: {e}"))?;
    let envelope = TxEnvelope::Eip1559(unsigned.into_signed(sig));
    Ok(envelope.encoded_2718())
}

/// Dispatch a signed raw transaction to the configured private submit endpoint.
pub async fn submit_signed_raw(
    raw: &[u8],
    cfg: &PrivateSubmitConfig,
) -> anyhow::Result<B256> {
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

async fn rpc_call(
    client: &Client,
    url: &str,
    method: &str,
    params: serde_json::Value,
) -> anyhow::Result<Option<serde_json::Value>> {
    let body = JsonRpcRequest {
        jsonrpc: "2.0",
        id: 1,
        method,
        params,
    };
    let resp = client
        .post(url)
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
