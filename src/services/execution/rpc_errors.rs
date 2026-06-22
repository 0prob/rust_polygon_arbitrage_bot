use alloy::primitives::B256;

/// Recovery action for a failed transaction submission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitAction {
    /// Nonce conflict — resync from chain and retry with a fresh nonce.
    ResyncAndRetry,
    /// Bump fees on the same nonce and retry (replacement).
    BumpFeesAndRetry,
    /// Tx is already in the mempool — treat as submitted.
    AlreadyKnown,
    /// Wallet cannot pay — do not retry.
    InsufficientFunds,
    /// Hard failure — quarantine route.
    Fail(String),
}

/// Classify a JSON-RPC / transport error from submission or simulation.
pub fn classify_submit_error(err: &impl std::fmt::Display) -> SubmitAction {
    let msg = err.to_string().to_ascii_lowercase();

    if msg.contains("nonce too low") || msg.contains("nonce has already been used") {
        return SubmitAction::ResyncAndRetry;
    }
    if msg.contains("already known") || msg.contains("already imported") {
        return SubmitAction::AlreadyKnown;
    }
    if msg.contains("replacement transaction underpriced")
        || msg.contains("fee too low")
        || msg.contains("underpriced")
    {
        return SubmitAction::BumpFeesAndRetry;
    }
    if msg.contains("insufficient funds") || msg.contains("insufficient balance") {
        return SubmitAction::InsufficientFunds;
    }
    if msg.contains("429") || msg.contains("rate limit") || msg.contains("timeout") {
        return SubmitAction::BumpFeesAndRetry;
    }

    SubmitAction::Fail(err.to_string())
}

/// Whether a receipt-fetch error is transient (keep polling).
pub fn is_transient_receipt_error(err: &impl std::fmt::Display) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("429")
        || msg.contains("rate limit")
        || msg.contains("timeout")
        || msg.contains("connection")
        || msg.contains("temporarily unavailable")
        || msg.contains("server error")
}

/// Parse a 32-byte tx hash from an "already known" error message when present.
pub fn extract_tx_hash_from_error(err: &str) -> Option<B256> {
    err.split("0x").skip(1).find_map(|segment| {
        let hex: String = segment.chars().take(64).collect();
        if hex.len() == 64 {
            format!("0x{hex}").parse().ok()
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_nonce_too_low() {
        assert_eq!(
            classify_submit_error(&"nonce too low"),
            SubmitAction::ResyncAndRetry
        );
    }

    #[test]
    fn classifies_already_known() {
        assert_eq!(
            classify_submit_error(&"transaction already known"),
            SubmitAction::AlreadyKnown
        );
    }

    #[test]
    fn classifies_underpriced() {
        assert_eq!(
            classify_submit_error(&"replacement transaction underpriced"),
            SubmitAction::BumpFeesAndRetry
        );
    }
}
