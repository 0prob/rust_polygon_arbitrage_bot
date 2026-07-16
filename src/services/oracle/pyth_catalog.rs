use anyhow::Context;
use reqwest::Client;
use serde::Deserialize;

const ORACLE_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(12);

#[derive(Debug, Clone)]
pub struct PythFeedCandidate {
    pub id: String,
    pub symbol: String,
    pub asset_type: String,
    pub quote_currency: String,
    pub description: String,
    pub usd_spot: bool,
}

#[derive(Debug, Deserialize)]
struct PythFeedMeta {
    id: String,
    attributes: PythFeedAttributes,
}

#[derive(Debug, Deserialize)]
struct PythFeedAttributes {
    symbol: String,
    #[serde(default)]
    asset_type: String,
    #[serde(default)]
    quote_currency: String,
    #[serde(default)]
    description: String,
}

/// Hermes catalog search — suggestions only; human must confirm token ↔ feed mapping.
pub async fn search_pyth_feeds(
    http: &Client,
    hermes_base: &str,
    query: &str,
) -> anyhow::Result<Vec<PythFeedCandidate>> {
    let base = hermes_base.trim_end_matches('/');
    let encoded: String = query
        .chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            ' ' => "%20".to_string(),
            _ => format!("%{:02X}", c as u8),
        })
        .collect();
    let url = format!("{base}/v2/price_feeds?query={encoded}");
    let resp = http
        .get(url)
        .timeout(ORACLE_HTTP_TIMEOUT)
        .send()
        .await
        .context("pyth catalog request failed")?
        .error_for_status()
        .context("pyth catalog non-success status")?;
    let feeds: Vec<PythFeedMeta> = resp.json().await.context("pyth catalog json")?;
    Ok(feeds
        .into_iter()
        .map(|feed| {
            let usd_spot = feed.attributes.quote_currency.eq_ignore_ascii_case("USD")
                && feed.attributes.symbol.contains("/USD")
                && !feed.attributes.symbol.contains(".RR");
            PythFeedCandidate {
                id: feed.id,
                symbol: feed.attributes.symbol,
                asset_type: feed.attributes.asset_type,
                quote_currency: feed.attributes.quote_currency,
                description: feed.attributes.description,
                usd_spot,
            }
        })
        .collect())
}

#[must_use]
pub fn pyth_symbol_matches_hint(symbol: &str, hint: &str) -> bool {
    let sym = symbol.to_ascii_uppercase();
    let h = hint.to_ascii_uppercase();
    if h.is_empty() || h == "USD" {
        return false;
    }
    // Require exact asset segment (e.g. Crypto.SD/USD), not substring hits (MUSD/USD).
    sym.contains(&format!(".{h}/USD"))
}

#[must_use]
pub fn pick_best_usd_candidate(candidates: &[PythFeedCandidate]) -> Option<&PythFeedCandidate> {
    candidates
        .iter()
        .filter(|c| c.usd_spot && c.asset_type.eq_ignore_ascii_case("Crypto"))
        .max_by_key(|c| c.symbol.len())
        .or_else(|| candidates.iter().find(|c| c.usd_spot))
}

#[must_use]
pub fn pick_best_usd_candidate_for_hint<'a>(
    candidates: &'a [PythFeedCandidate],
    hint: &str,
) -> Option<&'a PythFeedCandidate> {
    let mut best: Option<&PythFeedCandidate> = None;
    for c in candidates {
        if !c.usd_spot || !c.asset_type.eq_ignore_ascii_case("Crypto") {
            continue;
        }
        if !pyth_symbol_matches_hint(&c.symbol, hint) {
            continue;
        }
        if best.is_none_or(|b| c.symbol.len() < b.symbol.len()) {
            best = Some(c);
        }
    }
    best
}

/// Redemption-rate (`.RR`) feed for LST/stable review — never auto-merge without human sign-off.
#[must_use]
pub fn pick_best_rr_candidate_for_hint<'a>(
    candidates: &'a [PythFeedCandidate],
    hint: &str,
) -> Option<&'a PythFeedCandidate> {
    let mut best: Option<&PythFeedCandidate> = None;
    for c in candidates {
        if !c.symbol.contains(".RR") {
            continue;
        }
        if !pyth_symbol_matches_hint(&c.symbol, hint) {
            continue;
        }
        if best.is_none_or(|b| c.symbol.len() > b.symbol.len()) {
            best = Some(c);
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(symbol: &str) -> PythFeedCandidate {
        PythFeedCandidate {
            id: symbol.into(),
            symbol: symbol.into(),
            asset_type: "Crypto".into(),
            quote_currency: "USD".into(),
            description: String::new(),
            usd_spot: symbol.contains("/USD") && !symbol.contains(".RR"),
        }
    }

    #[test]
    fn hint_match_rejects_musd_for_sd() {
        assert!(!pyth_symbol_matches_hint("Crypto.MUSD/USD", "SD"));
        assert!(pyth_symbol_matches_hint("Crypto.SD/USD", "SD"));
    }

    #[test]
    fn hint_picker_prefers_exact_shorter_ticker() {
        let candidates = [
            cand("Crypto.WFRAGSOL/USD"),
            cand("Crypto.SOL/USD"),
        ];
        let best = pick_best_usd_candidate_for_hint(&candidates, "SOL").expect("SOL feed");
        assert_eq!(best.symbol, "Crypto.SOL/USD");
    }
}
