use anyhow::Context;
use serde::Deserialize;

use crate::error::ArbError;
use crate::services::discovery::{DiscoveredPool, TokenMeta, parse_pool_meta_row};

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
    address: Option<String>,
    protocol: String,
    tokens: serde_json::Value,
    fee: Option<i32>,
    #[serde(rename = "tickSpacing")]
    tick_spacing: Option<i32>,
    #[serde(rename = "poolId")]
    pool_id: Option<String>,
    hooks: Option<String>,
    #[serde(rename = "poolType")]
    pool_type: Option<String>,
    #[serde(rename = "createdBlock")]
    created_block: Option<i64>,
    #[serde(rename = "updatedAtBlock")]
    updated_at_block: Option<i64>,
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

#[derive(Debug, Deserialize)]
struct IndexerProgressRows {
    #[serde(rename = "IndexerProgress")]
    indexer_progress: Option<Vec<IndexerProgressRow>>,
}

#[derive(Debug, Deserialize)]
struct IndexerProgressRow {
    chain_id: Option<i64>,
    #[serde(rename = "chainId")]
    chain_id_camel: Option<i64>,
    last_processed_block: Option<i64>,
    #[serde(rename = "lastProcessedBlock")]
    last_processed_block_camel: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct IndexerMetaRows {
    _meta: Option<Vec<IndexerMetaRow>>,
}

#[derive(Debug, Deserialize)]
struct IndexerMetaRow {
    chain_id: Option<i64>,
    #[serde(rename = "chainId")]
    chain_id_camel: Option<i64>,
    progress_block: Option<i64>,
    #[serde(rename = "progressBlock")]
    progress_block_camel: Option<i64>,
    source_block: Option<i64>,
    #[serde(rename = "sourceBlock")]
    source_block_camel: Option<i64>,
    is_ready: Option<bool>,
    #[serde(rename = "isReady")]
    is_ready_camel: Option<bool>,
}

const DISCOVER_PAGE_SIZE: usize = 2500;
const DISCOVER_MAX_PAGES: usize = 40;
const TOKEN_META_PAGE_SIZE: usize = 10_000;
const TOKEN_META_MAX_PAGES: usize = 60;
const GRAPHQL_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone, Default)]
pub struct DiscoveryCursor {
    pub last_block: u64,
    /// Watermark for `updatedAtBlock` — catches Balancer token-list updates without new `createdBlock`.
    pub last_updated_block: u64,
    pub cursor_block: Option<u64>,
    pub cursor_id: Option<String>,
    pub updated_cursor_block: Option<u64>,
    pub updated_cursor_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DiscoveryResult {
    pub pools: Vec<DiscoveredPool>,
    pub cursor: DiscoveryCursor,
    /// `true` when the indexer tail was reached (no more pages pending).
    pub complete: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct IndexerProgress {
    pub chain_id: u64,
    pub last_processed_block: u64,
    pub source_block: Option<u64>,
    /// Envio `_meta.isReady` — `false` during historical backfill.
    pub is_ready: Option<bool>,
}

const POOL_META_FIELDS_FULL: &str =
    "id address protocol tokens fee tickSpacing poolId hooks poolType createdBlock updatedAtBlock";

fn is_missing_graphql_field_error(err: &anyhow::Error, field: &str) -> bool {
    let msg = err.to_string();
    msg.contains(&format!("field '{field}' not found"))
        || msg.contains(&format!("field \"{field}\" not found"))
}

/// Degrade PoolMeta field selection when Hasura schema lags the indexer.
/// Removes ONE field at a time so earlier removals are never re-added.
struct PoolMetaFieldSelector {
    fields: String,
    supports_updated_at_block: bool,
}

impl PoolMetaFieldSelector {
    fn new() -> Self {
        Self {
            fields: POOL_META_FIELDS_FULL.to_string(),
            supports_updated_at_block: true,
        }
    }

    fn current(&self) -> &str {
        &self.fields
    }

    fn supports_updated_at_block(&self) -> bool {
        self.supports_updated_at_block
    }

    fn degrade_for_error(&mut self, err: &anyhow::Error) -> bool {
        let removable = ["updatedAtBlock", "poolType", "hooks", "address"];
        let Some(field) = removable.iter().find(|f| {
            self.fields.contains(*f) && is_missing_graphql_field_error(err, f)
        }) else {
            return false;
        };
        // Remove the single field with its surrounding whitespace.
        self.fields = self
            .fields
            .replace(&format!(" {field}"), "")
            .replace(&format!("{field} "), "");
        if *field == "updatedAtBlock" {
            self.supports_updated_at_block = false;
        }
        true
    }
}

struct DiscoverPassState {
    pools: Vec<DiscoveredPool>,
    max_watermark: u64,
    page_cursor_block: Option<u64>,
    page_cursor_id: Option<String>,
    complete: bool,
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
            .connect_timeout(std::time::Duration::from_secs(5))
            .pool_max_idle_per_host(4)
            .build()
            .map_err(|e| ArbError::HttpClient(format!("hasura reqwest: {e}")))?;
        Ok(Self { url, secret, http })
    }

    async fn query<T: serde::de::DeserializeOwned>(&self, query: &str) -> anyhow::Result<T> {
        let mut req = self
            .http
            .post(&self.url)
            .json(&serde_json::json!({ "query": query }));
        if let Some(secret) = &self.secret {
            req = req.header("x-hasura-admin-secret", secret);
        }
        let resp = req.send().await?.error_for_status()?;
        let body: GraphQlResponse<T> = resp.json().await?;
        if let Some(errors) = body.errors {
            anyhow::bail!("hasura graphql errors: {errors:?}");
        }
        body.data.context("hasura response missing data")
    }

    fn block_field_cursor_where(
        field: &str,
        last_watermark: u64,
        cursor_block: Option<u64>,
        cursor_id: Option<&str>,
    ) -> String {
        if let (Some(block), Some(id)) = (cursor_block, cursor_id) {
            let id = id.replace('\\', "\\\\").replace('"', "\\\"");
            format!(
                "where: {{ _or: [ {{ {field}: {{ _gt: {block} }} }}, {{ _and: [ {{ {field}: {{ _eq: {block} }} }}, {{ id: {{ _gt: \"{id}\" }} }} ] }} ] }}"
            )
        } else if last_watermark > 0 {
            format!("where: {{ {field}: {{ _gt: {last_watermark} }} }}")
        } else {
            String::new()
        }
    }

    fn id_cursor_where(cursor_id: Option<&str>) -> String {
        match cursor_id {
            Some(id) => {
                let id = id.replace('\\', "\\\\").replace('"', "\\\"");
                format!("where: {{ id: {{ _gt: \"{id}\" }} }}")
            }
            None => String::new(),
        }
    }

    async fn query_pool_meta_page(
        &self,
        field_selector: &mut PoolMetaFieldSelector,
        where_clause: &str,
        order_by: &str,
    ) -> anyhow::Result<PoolMetaRows> {
        loop {
            let mut args = vec![format!("limit: {DISCOVER_PAGE_SIZE}")];
            if !where_clause.is_empty() {
                args.push(where_clause.to_string());
            }
            args.push(format!("order_by: {order_by}"));
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

    fn parse_pool_row(row: &PoolMetaRow) -> Option<DiscoveredPool> {
        parse_pool_meta_row(
            &row.id,
            &row.protocol,
            &row.tokens,
            row.fee,
            row.tick_spacing,
            row.pool_id.as_deref(),
            row.hooks.as_deref(),
            row.pool_type.as_deref(),
            row.created_block,
            row.address.as_deref(),
        )
    }

    async fn discover_by_block_field(
        &self,
        field_selector: &mut PoolMetaFieldSelector,
        block_field: &str,
        watermark: u64,
        page_cursor_block: Option<u64>,
        page_cursor_id: Option<String>,
    ) -> anyhow::Result<DiscoverPassState> {
        let mut pools = Vec::new();
        let mut max_watermark = watermark;
        let mut cursor_block = page_cursor_block;
        let mut cursor_id = page_cursor_id;
        let mut hit_page_cap = false;
        let order_by = format!("[{{ {block_field}: asc }}, {{ id: asc }}]");

        for _ in 0..DISCOVER_MAX_PAGES {
            let where_clause = Self::block_field_cursor_where(
                block_field,
                watermark,
                cursor_block,
                cursor_id.as_deref(),
            );
            let page = self
                .query_pool_meta_page(
                    field_selector,
                    &where_clause,
                    &order_by,
                )
                .await?;
            let rows = page.pool_meta.unwrap_or_default();
            if rows.is_empty() {
                return Ok(DiscoverPassState {
                    pools,
                    max_watermark,
                    page_cursor_block: cursor_block,
                    page_cursor_id: cursor_id,
                    complete: true,
                });
            }

            for row in &rows {
                if let Some(pool) = Self::parse_pool_row(row) {
                    pools.push(pool);
                }
                let row_block = row_block_value(row, block_field);
                if row_block > max_watermark {
                    max_watermark = row_block;
                }
                cursor_block = Some(row_block);
                cursor_id = Some(row.id.clone());
            }

            if rows.len() < DISCOVER_PAGE_SIZE {
                return Ok(DiscoverPassState {
                    pools,
                    max_watermark,
                    page_cursor_block: cursor_block,
                    page_cursor_id: cursor_id,
                    complete: true,
                });
            }
            hit_page_cap = true;
        }

        Ok(DiscoverPassState {
            pools,
            max_watermark,
            page_cursor_block: cursor_block,
            page_cursor_id: cursor_id,
            complete: !hit_page_cap,
        })
    }

    pub async fn discover_pools(
        &self,
        cursor: &DiscoveryCursor,
    ) -> anyhow::Result<DiscoveryResult> {
        let mut field_selector = PoolMetaFieldSelector::new();

        let created = self
            .discover_by_block_field(
                &mut field_selector,
                "createdBlock",
                cursor.last_block,
                cursor.cursor_block,
                cursor.cursor_id.clone(),
            )
            .await?;

        let bootstrap = cursor.last_block == 0 && cursor.last_updated_block == 0;
        let effective_updated_watermark = if cursor.last_updated_block == 0 && cursor.last_block > 0 {
            cursor.last_block
        } else {
            cursor.last_updated_block
        };

        let updated = if bootstrap && created.complete {
            DiscoverPassState {
                pools: Vec::new(),
                max_watermark: created.max_watermark,
                page_cursor_block: None,
                page_cursor_id: None,
                complete: true,
            }
        } else if field_selector.supports_updated_at_block() {
            self.discover_by_block_field(
                &mut field_selector,
                "updatedAtBlock",
                effective_updated_watermark,
                cursor.updated_cursor_block,
                cursor.updated_cursor_id.clone(),
            )
            .await?
        } else {
            DiscoverPassState {
                pools: Vec::new(),
                max_watermark: effective_updated_watermark,
                page_cursor_block: None,
                page_cursor_id: None,
                complete: true,
            }
        };

        let mut combined = created.pools;
        combined.extend(updated.pools);

        let last_block = if created.complete {
            created.max_watermark
        } else {
            cursor.last_block
        };
        let last_updated_block = if updated.complete {
            if bootstrap && created.complete {
                created.max_watermark.max(updated.max_watermark)
            } else {
                updated.max_watermark.max(effective_updated_watermark)
            }
        } else {
            cursor.last_updated_block
        };

        Ok(DiscoveryResult {
            pools: combined,
            cursor: DiscoveryCursor {
                last_block,
                last_updated_block,
                cursor_block: if created.complete {
                    None
                } else {
                    created.page_cursor_block
                },
                cursor_id: if created.complete {
                    None
                } else {
                    created.page_cursor_id
                },
                updated_cursor_block: if updated.complete {
                    None
                } else {
                    updated.page_cursor_block
                },
                updated_cursor_id: if updated.complete {
                    None
                } else {
                    updated.page_cursor_id
                },
            },
            complete: created.complete && updated.complete,
        })
    }

    pub async fn fetch_token_metas(&self) -> anyhow::Result<Vec<TokenMeta>> {
        let mut out = Vec::new();
        let mut cursor_id: Option<String> = None;

        for _ in 0..TOKEN_META_MAX_PAGES {
            let mut args = vec![format!("limit: {TOKEN_META_PAGE_SIZE}")];
            let where_clause = Self::id_cursor_where(cursor_id.as_deref());
            if !where_clause.is_empty() {
                args.push(where_clause);
            }
            args.push("order_by: [{ id: asc }]".to_string());
            let query = format!(
                "{{ TokenMeta({}) {{ id address decimals }} }}",
                args.join(", ")
            );
            let page: TokenMetaRows = self.query(&query).await?;
            let rows = page.token_meta.unwrap_or_default();
            if rows.is_empty() {
                break;
            }

            for row in &rows {
                let addr_str = row.address.as_deref().or(row.id.as_deref());
                let Some(addr_str) = addr_str else { continue };
                let Ok(address) = addr_str.parse() else {
                    continue;
                };
                out.push(TokenMeta {
                    address,
                    decimals: row.decimals.unwrap_or(18).clamp(0, 77) as u8,
                });
            }

            if rows.len() < TOKEN_META_PAGE_SIZE {
                break;
            }
            cursor_id = rows.last().and_then(|r| r.id.clone());
            if cursor_id.is_none() {
                break;
            }
        }

        Ok(out)
    }

    /// Prefer Envio `_meta` (includes chain head); fall back to legacy `IndexerProgress`.
    pub async fn fetch_indexer_progress(
        &self,
        chain_id: u64,
    ) -> anyhow::Result<Option<IndexerProgress>> {
        if let Some(progress) = self.fetch_indexer_meta(chain_id).await? {
            return Ok(Some(progress));
        }
        self.fetch_legacy_indexer_progress(chain_id).await
    }

    async fn fetch_indexer_meta(&self, chain_id: u64) -> anyhow::Result<Option<IndexerProgress>> {
        let query = format!(
            "{{ _meta(where: {{ chainId: {{ _eq: {chain_id} }} }}) {{ chainId progressBlock sourceBlock isReady }} }}"
        );
        let page: IndexerMetaRows = match self.query(&query).await {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };
        let Some(row) = page._meta.and_then(|rows| rows.into_iter().next()) else {
            return Ok(None);
        };
        let cid = row
            .chain_id_camel
            .or(row.chain_id)
            .map(|v| v.max(0) as u64)
            .unwrap_or(chain_id);
        let Some(progress) = row
            .progress_block_camel
            .or(row.progress_block)
            .map(|v| v.max(0) as u64)
            .filter(|v| *v > 0)
        else {
            return Ok(None);
        };
        let source = row
            .source_block_camel
            .or(row.source_block)
            .map(|v| v.max(0) as u64)
            .filter(|v| *v > 0);
        let is_ready = row.is_ready_camel.or(row.is_ready);
        Ok(Some(IndexerProgress {
            chain_id: cid,
            last_processed_block: progress,
            source_block: source,
            is_ready,
        }))
    }

    async fn fetch_legacy_indexer_progress(
        &self,
        chain_id: u64,
    ) -> anyhow::Result<Option<IndexerProgress>> {
        let query = format!(
            "{{ IndexerProgress(where: {{ chainId: {{ _eq: {chain_id} }} }}, limit: 1, order_by: {{ lastProcessedBlock: desc }}) {{ chainId lastProcessedBlock }} }}"
        );
        let page: IndexerProgressRows = self.query(&query).await?;
        let Some(row) = page
            .indexer_progress
            .and_then(|rows| rows.into_iter().next())
        else {
            return Ok(None);
        };
        let cid = row
            .chain_id_camel
            .or(row.chain_id)
            .map(|v| v.max(0) as u64)
            .unwrap_or(chain_id);
        let Some(last) = row
            .last_processed_block_camel
            .or(row.last_processed_block)
            .map(|v| v.max(0) as u64)
            .filter(|v| *v > 0)
        else {
            return Ok(None);
        };
        Ok(Some(IndexerProgress {
            chain_id: cid,
            last_processed_block: last,
            source_block: None,
            is_ready: None,
        }))
    }
}

fn row_block_value(row: &PoolMetaRow, block_field: &str) -> u64 {
    if block_field == "updatedAtBlock" {
        row.updated_at_block.unwrap_or(0).max(0) as u64
    } else {
        row.created_block.unwrap_or(0).max(0) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_selector_degrades_updated_at_block_then_pool_type() {
        let mut selector = PoolMetaFieldSelector::new();
        assert!(selector.supports_updated_at_block());
        let err = anyhow::anyhow!(r#"field "updatedAtBlock" not found in type: 'PoolMeta'"#);
        assert!(selector.degrade_for_error(&err));
        assert!(!selector.supports_updated_at_block());
        assert!(!selector.current().contains("updatedAtBlock"));

        let err = anyhow::anyhow!(r#"field "poolType" not found in type: 'PoolMeta'"#);
        assert!(selector.degrade_for_error(&err));
        assert!(!selector.current().contains("poolType"));
    }

    #[test]
    fn field_selector_degrades_pool_type_then_hooks() {
        let mut selector = PoolMetaFieldSelector::new();
        assert!(selector.current().contains("poolType"));
        let err = anyhow::anyhow!(r#"field "poolType" not found in type: 'PoolMeta'"#);
        assert!(selector.degrade_for_error(&err));
        assert!(!selector.current().contains("poolType"));
        assert!(selector.current().contains("hooks"));

        let err = anyhow::anyhow!(r#"field "hooks" not found in type: 'PoolMeta'"#);
        assert!(selector.degrade_for_error(&err));
        assert!(!selector.current().contains("hooks"));
    }

    #[test]
    fn block_field_cursor_where_formats_created_and_updated() {
        let created = HasuraClient::block_field_cursor_where(
            "createdBlock",
            100,
            Some(100),
            Some("0xabc"),
        );
        assert!(created.contains("createdBlock"));
        assert!(created.contains("0xabc"));

        let updated = HasuraClient::block_field_cursor_where(
            "updatedAtBlock",
            50,
            None,
            None,
        );
        assert!(updated.contains("updatedAtBlock: { _gt: 50 }"));
    }
}
