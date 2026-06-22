use anyhow::Context;
use serde::Deserialize;

use crate::services::discovery::{DiscoveredPool, TokenMeta, parse_pool_meta_row};
use crate::error::ArbError;

#[derive(Debug, Deserialize)]
struct GraphQlResponse<T> {
    data: Option<T>,
    errors: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
struct PoolMetaRows {
    #[serde(rename = "PoolMeta")]
    pool_meta: Option<Vec<PoolMetaRow>>,
}

#[derive(Debug, Deserialize)]
struct PoolMetaRow {
    id: String,
    protocol: String,
    tokens: serde_json::Value,
    fee: Option<i32>,
    #[serde(rename = "tickSpacing")]
    tick_spacing: Option<i32>,
    #[serde(rename = "poolId")]
    pool_id: Option<String>,
    hooks: Option<String>,
    #[serde(rename = "createdBlock")]
    created_block: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct TokenMetaRows {
    #[serde(rename = "TokenMeta")]
    token_meta: Option<Vec<TokenMetaRow>>,
}

#[derive(Debug, Deserialize)]
struct TokenMetaRow {
    id: Option<String>,
    address: Option<String>,
    decimals: Option<i32>,
}

const DISCOVER_PAGE_SIZE: usize = 2500;
const DISCOVER_MAX_PAGES: usize = 40;
const GRAPHQL_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone, Default)]
pub struct DiscoveryCursor {
    pub last_block: u64,
    pub cursor_block: Option<u64>,
    pub cursor_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DiscoveryResult {
    pub pools: Vec<DiscoveredPool>,
    pub cursor: DiscoveryCursor,
    /// `true` when the indexer tail was reached (no more pages pending).
    pub complete: bool,
}

const POOL_META_FIELDS_FULL: &str = "id protocol tokens fee tickSpacing poolId hooks createdBlock";
const POOL_META_FIELDS_LEGACY: &str = "id protocol tokens fee tickSpacing poolId createdBlock";

fn is_missing_graphql_field_error(err: &anyhow::Error, field: &str) -> bool {
    let msg = err.to_string();
    msg.contains(&format!("field '{field}' not found"))
        || msg.contains(&format!("field \"{field}\" not found"))
}

/// Degrade PoolMeta field selection when Hasura schema lags the indexer.
struct PoolMetaFieldSelector {
    fields: String,
}

impl PoolMetaFieldSelector {
    fn new() -> Self {
        Self {
            fields: POOL_META_FIELDS_FULL.to_string(),
        }
    }

    fn current(&self) -> &str {
        &self.fields
    }

    fn degrade_for_error(&mut self, err: &anyhow::Error) -> bool {
        if self.fields.contains("hooks") && is_missing_graphql_field_error(err, "hooks") {
            self.fields = POOL_META_FIELDS_LEGACY.to_string();
            return true;
        }
        false
    }
}

pub struct HasuraClient {
    url: String,
    secret: Option<String>,
    http: reqwest::Client,
}

impl HasuraClient {
    pub fn new(url: String, secret: Option<String>) -> Result<Self, ArbError> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(GRAPHQL_TIMEOUT_MS))
            .build()
            .map_err(|e| ArbError::HttpClient(format!("hasura reqwest: {e}")))?;
        Ok(Self {
            url,
            secret,
            http,
        })
    }

    async fn query<T: serde::de::DeserializeOwned>(&self, query: &str) -> anyhow::Result<T> {
        let mut req = self
            .http
            .post(&self.url)
            .json(&serde_json::json!({ "query": query }));
        if let Some(secret) = &self.secret {
            req = req.header("x-hasura-admin-secret", secret);
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            anyhow::bail!("hasura HTTP {}", resp.status());
        }
        let body: GraphQlResponse<T> = resp.json().await?;
        if let Some(errors) = body.errors {
            anyhow::bail!("hasura graphql errors: {errors:?}");
        }
        body.data.context("hasura response missing data")
    }

    fn block_cursor_where(
        last_block: u64,
        cursor_block: Option<u64>,
        cursor_id: Option<&str>,
    ) -> String {
        if let (Some(block), Some(id)) = (cursor_block, cursor_id) {
            let id = id.replace('\\', "\\\\").replace('"', "\\\"");
            format!(
                "where: {{ _or: [ {{ createdBlock: {{ _gt: {block} }} }}, {{ _and: [ {{ createdBlock: {{ _eq: {block} }} }}, {{ id: {{ _gt: \"{id}\" }} }} ] }} ] }}"
            )
        } else if last_block > 0 {
            format!("where: {{ createdBlock: {{ _gt: {last_block} }} }}")
        } else {
            String::new()
        }
    }

    async fn query_pool_meta_page(
        &self,
        field_selector: &mut PoolMetaFieldSelector,
        where_clause: &str,
    ) -> anyhow::Result<PoolMetaRows> {
        loop {
            let mut args = vec![format!("limit: {DISCOVER_PAGE_SIZE}")];
            if !where_clause.is_empty() {
                args.push(where_clause.to_string());
            }
            args.push("order_by: [{ createdBlock: asc }, { id: asc }]".to_string());
            let query = format!(
                "{{ PoolMeta({}) {{ {} }} }}",
                args.join(", "),
                field_selector.current()
            );

            match self.query::<PoolMetaRows>(&query).await {
                Ok(page) => return Ok(page),
                Err(err) => {
                    if field_selector.degrade_for_error(&err) {
                        continue;
                    }
                    return Err(err);
                }
            }
        }
    }

    pub async fn discover_pools(
        &self,
        cursor: &DiscoveryCursor,
    ) -> anyhow::Result<DiscoveryResult> {
        let mut combined = Vec::new();
        let mut max_block = cursor.last_block;
        let mut page_cursor_block = cursor.cursor_block;
        let mut page_cursor_id = cursor.cursor_id.clone();
        let mut field_selector = PoolMetaFieldSelector::new();
        let mut hit_page_cap = false;

        for _ in 0..DISCOVER_MAX_PAGES {
            let where_clause = Self::block_cursor_where(
                cursor.last_block,
                page_cursor_block,
                page_cursor_id.as_deref(),
            );
            let page = self
                .query_pool_meta_page(&mut field_selector, &where_clause)
                .await?;
            let rows = page.pool_meta.unwrap_or_default();
            if rows.is_empty() {
                return Ok(DiscoveryResult {
                    pools: combined,
                    cursor: DiscoveryCursor {
                        last_block: max_block,
                        // Preserve tail position — don't skip pools at max_block
                        // that may appear after the previous cursor landed.
                        cursor_block: page_cursor_block,
                        cursor_id: page_cursor_id,
                    },
                    complete: true,
                });
            }

            for row in &rows {
                if let Some(pool) = parse_pool_meta_row(
                    &row.id,
                    &row.protocol,
                    &row.tokens,
                    row.fee,
                    row.tick_spacing,
                    row.pool_id.as_deref(),
                    row.hooks.as_deref(),
                    row.created_block,
                ) {
                    if pool.created_block > max_block {
                        max_block = pool.created_block;
                    }
                    combined.push(pool);
                }
                page_cursor_block = row.created_block.map(|b| b.max(0) as u64);
                page_cursor_id = Some(row.id.clone());
            }

            if rows.len() < DISCOVER_PAGE_SIZE {
                return Ok(DiscoveryResult {
                    pools: combined,
                    cursor: DiscoveryCursor {
                        last_block: max_block,
                        // Preserve tail position so next discovery picks up
                        // pools at max_block that weren't in this batch.
                        cursor_block: page_cursor_block,
                        cursor_id: page_cursor_id,
                    },
                    complete: true,
                });
            }
            hit_page_cap = true;
        }

        Ok(DiscoveryResult {
            pools: combined,
            cursor: DiscoveryCursor {
                last_block: cursor.last_block,
                cursor_block: page_cursor_block,
                cursor_id: page_cursor_id,
            },
            complete: !hit_page_cap,
        })
    }

    pub async fn fetch_token_metas(&self) -> anyhow::Result<Vec<TokenMeta>> {
        let query = "{ TokenMeta(limit: 50000) { id address decimals } }";
        let page: TokenMetaRows = self.query(query).await?;
        let mut out = Vec::new();
        for row in page.token_meta.unwrap_or_default() {
            let addr_str = row.address.or(row.id);
            let Some(addr_str) = addr_str else { continue };
            let Ok(address) = addr_str.parse() else {
                continue;
            };
            out.push(TokenMeta {
                address,
                decimals: row.decimals.unwrap_or(18).clamp(0, 77) as u8,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_selector_degrades_when_hooks_missing() {
        let mut selector = PoolMetaFieldSelector::new();
        assert!(selector.current().contains("hooks"));
        let err = anyhow::anyhow!(r#"field "hooks" not found in type: 'PoolMeta'"#);
        assert!(selector.degrade_for_error(&err));
        assert!(!selector.current().contains("hooks"));
        assert!(selector.current().contains("poolId"));
    }
}
