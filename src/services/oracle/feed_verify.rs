use alloy::primitives::Address;
use anyhow::Context;

use crate::core::constants::MIN_TOKEN_TO_MATIC_RATE;
use crate::services::oracle::price_oracle::PriceOracle;

#[derive(Debug, Clone)]
pub struct ProposedPythFeed {
    pub token: Address,
    pub feed_id: String,
    pub comment: Option<String>,
}

#[derive(Debug, Clone)]
pub struct VerifiedPythFeed {
    pub token: Address,
    pub feed_id: String,
    pub token_usd: f64,
    pub matic_rate_ok: bool,
}

/// Parse `oracle.pyth_feeds` lines: `0xToken=feed_id` with optional `#` comment.
pub fn parse_proposed_pyth_feed_lines(text: &str) -> anyhow::Result<Vec<ProposedPythFeed>> {
    let mut out = Vec::new();
    for (line_no, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or(raw).trim();
        if line.is_empty() {
            continue;
        }
        let Some((token_str, feed_id)) = line.split_once('=') else {
            anyhow::bail!("line {}: expected 0xToken=feed_id", line_no + 1);
        };
        let token: Address = token_str
            .trim()
            .parse()
            .with_context(|| format!("line {}: bad token address", line_no + 1))?;
        let feed_id = feed_id.trim().to_string();
        if feed_id.is_empty() {
            anyhow::bail!("line {}: empty feed id", line_no + 1);
        }
        let comment = raw
            .split('#')
            .nth(1)
            .map(str::trim)
            .filter(|s| !s.is_empty());
        out.push(ProposedPythFeed {
            token,
            feed_id,
            comment: comment.map(str::to_string),
        });
    }
    Ok(out)
}

/// Live Hermes check (oracle_live_test-style) — does not persist feeds into config.
pub async fn verify_proposed_pyth_feeds(
    oracle: &PriceOracle,
    proposals: &[ProposedPythFeed],
) -> anyhow::Result<Vec<VerifiedPythFeed>> {
    if proposals.is_empty() {
        return Ok(Vec::new());
    }
    let _ = oracle.get_matic_usd_offline().await;
    let mut verified = Vec::with_capacity(proposals.len());
    for proposal in proposals {
        oracle.register_pyth_feed(proposal.token, proposal.feed_id.clone());
        oracle
            .prefetch_token_usd_offline(std::slice::from_ref(&proposal.token))
            .await;
        let usd = oracle
            .token_usd(&proposal.token)
            .ok_or_else(|| anyhow::anyhow!("no USD quote for {}", proposal.token))?;
        if usd.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
            anyhow::bail!("non-positive USD for {}", proposal.token);
        }
        let matic_rate_ok = oracle
            .token_matic_rate_per_unit_integer(&proposal.token)
            .is_some_and(|r| r >= MIN_TOKEN_TO_MATIC_RATE);
        if !matic_rate_ok {
            anyhow::bail!(
                "missing or dust token/MATIC integer rate for {} (usd={usd})",
                proposal.token
            );
        }
        verified.push(VerifiedPythFeed {
            token: proposal.token,
            feed_id: proposal.feed_id.clone(),
            token_usd: usd,
            matic_rate_ok,
        });
    }
    Ok(verified)
}

#[must_use]
pub fn format_config_pyth_feeds(proposals: &[ProposedPythFeed]) -> String {
    let mut lines: Vec<String> = proposals
        .iter()
        .map(|p| {
            if let Some(c) = &p.comment {
                format!("{}={} # {c}", p.token, p.feed_id)
            } else {
                format!("{}={}", p.token, p.feed_id)
            }
        })
        .collect();
    lines.sort_unstable();
    lines.join(",")
}
