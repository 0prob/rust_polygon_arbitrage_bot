use serde::{Deserialize, Serialize};

/// JSON-RPC 2.0 request envelope (serde docs: prefer typed structs over `json!`).
#[derive(Debug, Serialize)]
pub struct JsonRpcRequest<'a, P> {
    pub jsonrpc: &'static str,
    pub id: u32,
    pub method: &'a str,
    pub params: P,
}

#[derive(Debug, Deserialize)]
pub struct JsonRpcResponse<R = JsonRpcResult> {
    pub result: Option<R>,
    #[serde(default)]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
pub struct JsonRpcError {
    pub message: String,
}

/// Polygon private-RPC and bloXroute results differ in shape.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum JsonRpcResult {
    TxHash(BloxrouteTxResult),
    Hex(String),
}

#[derive(Debug, Deserialize)]
pub struct BloxrouteTxResult {
    #[serde(alias = "txHash")]
    pub tx_hash: String,
}

impl JsonRpcResult {
    #[inline]
    #[must_use]
    pub fn as_tx_hash(&self) -> Option<&str> {
        match self {
            Self::TxHash(r) => Some(r.tx_hash.as_str()),
            Self::Hex(s) => Some(s.as_str()),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct BloxroutePrivateTxParams<'a> {
    pub transaction: &'a str,
}

pub type BloxrouteRequest<'a> = JsonRpcRequest<'static, BloxroutePrivateTxParams<'a>>;

#[cfg(test)]
mod bloxroute_deser_tests {
    use super::*;

    #[test]
    fn deserializes_bloxroute_error_and_success_shapes() {
        let err = r#"{"id":1,"error":{"code":-32602,"message":"The transaction is invalid."},"jsonrpc":"2.0"}"#;
        let parsed: JsonRpcResponse = serde_json::from_str(err).expect("error shape");
        assert!(parsed.error.is_some());

        let ok = r#"{"jsonrpc":"2.0","id":"1","result":{"tx_hash":"abc123"}}"#;
        let parsed: JsonRpcResponse = serde_json::from_str(ok).expect("success shape");
        assert_eq!(
            parsed.result.as_ref().and_then(JsonRpcResult::as_tx_hash),
            Some("abc123")
        );

        let camel = r#"{"jsonrpc":"2.0","id":1,"result":{"txHash":"abc123"}}"#;
        let parsed: JsonRpcResponse = serde_json::from_str(camel).expect("camelCase shape");
        assert_eq!(
            parsed.result.as_ref().and_then(JsonRpcResult::as_tx_hash),
            Some("abc123")
        );
    }
}
