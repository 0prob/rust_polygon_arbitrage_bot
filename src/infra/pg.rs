use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, ensure};
use tokio::sync::Mutex;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Error as PgError, NoTls, Statement};

use crate::services::discovery::{DiscoveredPool, TokenMeta, parse_pool_meta_row};
use crate::services::pipeline_survival::{ParseStats, record_pg_row};
use alloy::primitives::Address;

const POOL_META_COLUMNS: &str = r#"id, address, protocol::text, tokens, fee, "tickSpacing", "poolId", hooks, "poolType", "createdBlock""#;

fn pool_meta_keyset_sql() -> String {
    format!(
        r#"SELECT {POOL_META_COLUMNS} FROM "PoolMeta"
        WHERE ("createdBlock", id) > ($1, $2)
        ORDER BY "createdBlock", id
        LIMIT $3"#
    )
}

fn pool_meta_incremental_sql() -> String {
    format!(
        r#"
        SELECT {POOL_META_COLUMNS}, "updatedAtBlock", "sortBlock" FROM (
            SELECT {POOL_META_COLUMNS}, NULL::integer AS "updatedAtBlock", "createdBlock" AS "sortBlock"
            FROM "PoolMeta"
            WHERE "createdBlock" > $1
            UNION ALL
            SELECT {POOL_META_COLUMNS}, "updatedAtBlock", "updatedAtBlock" AS "sortBlock"
            FROM "PoolMeta"
            WHERE "updatedAtBlock" > $2 AND "createdBlock" <= $3
        ) AS combined
        ORDER BY "sortBlock" ASC
        "#
    )
}

const INDEXER_META_SQL: &str = r#"SELECT "chainId", "progressBlock", "sourceBlock", "isReady" FROM "_meta" WHERE "chainId" = $1"#;

const INDEXER_LEGACY_SQL: &str = r#"SELECT "lastProcessedBlock" FROM "IndexerProgress" WHERE id = $1 ORDER BY "lastProcessedBlock" DESC LIMIT 1"#;

const TOKEN_METAS_SQL: &str = r#"SELECT id, decimals FROM "TokenMeta""#;

const PG_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const PG_QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Keyset cursor for paginated PoolMeta bootstrap (avoids OFFSET scans).
/// `created_block` matches Envio `PoolMeta."createdBlock"` (`int4`).
#[derive(Debug, Clone, Default)]
pub struct PoolMetaKeyset {
    pub created_block: i32,
    pub id: String,
}

/// Prepared statements tied to one live connection (rebuilt on reconnect).
struct PgSession {
    client: Arc<Client>,
    pool_meta_keyset: Statement,
    pool_meta_incremental: Statement,
    pool_meta_count: Statement,
    indexer_meta: Statement,
    indexer_legacy: Statement,
    token_metas: Statement,
}

/// Direct PostgreSQL client.
pub struct PgClient {
    url: String,
    session: Mutex<Option<Arc<PgSession>>>,
}

impl PgClient {
    #[must_use]
    pub fn new(url: String) -> Self {
        Self {
            url,
            session: Mutex::new(None),
        }
    }

    async fn session(&self) -> anyhow::Result<Arc<PgSession>> {
        let mut guard = self.session.lock().await;
        if let Some(session) = guard.as_ref() {
            if !session.client.is_closed() {
                return Ok(Arc::clone(session));
            }
            crate::warn!("postgres session closed — reconnecting");
        }

        let (client, conn) = tokio::time::timeout(
            PG_CONNECT_TIMEOUT,
            tokio_postgres::connect(&self.url, NoTls),
        )
        .await
        .context("postgres connect timed out")?
        .context("postgres connect failed")?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                crate::warn!("postgres connection error: {e}");
            }
        });

        let client = Arc::new(client);
        let pool_meta_keyset = Self::prepare_with_timeout(&client, &pool_meta_keyset_sql()).await?;
        let pool_meta_incremental =
            Self::prepare_with_timeout(&client, &pool_meta_incremental_sql()).await?;
        const POOL_META_COUNT_SQL: &str = r#"SELECT COUNT(*)::bigint FROM "PoolMeta""#;
        let pool_meta_count = Self::prepare_with_timeout(&client, POOL_META_COUNT_SQL).await?;
        let indexer_meta = Self::prepare_with_timeout(&client, INDEXER_META_SQL).await?;
        let indexer_legacy = Self::prepare_with_timeout(&client, INDEXER_LEGACY_SQL).await?;
        let token_metas = Self::prepare_with_timeout(&client, TOKEN_METAS_SQL).await?;

        let session = Arc::new(PgSession {
            client,
            pool_meta_keyset,
            pool_meta_incremental,
            pool_meta_count,
            indexer_meta,
            indexer_legacy,
            token_metas,
        });
        *guard = Some(Arc::clone(&session));
        Ok(session)
    }

    async fn with_session_retry<T, F, Fut>(&self, op: F) -> anyhow::Result<T>
    where
        F: Fn(Arc<PgSession>) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        let session = self.session().await?;
        match op(Arc::clone(&session)).await {
            Ok(value) => Ok(value),
            Err(error) if is_transient_pg_error(&error) => {
                crate::warn!("postgres transient error — reconnecting: {error:#}");
                *self.session.lock().await = None;
                let session = self.session().await?;
                op(session).await
            }
            Err(error) => Err(error),
        }
    }

    async fn prepare_with_timeout(client: &Client, sql: &str) -> anyhow::Result<Statement> {
        tokio::time::timeout(PG_QUERY_TIMEOUT, client.prepare(sql))
            .await
            .context("postgres prepare timed out")?
            .context("postgres prepare failed")
    }

    async fn query_timeout<T>(
        fut: impl std::future::Future<Output = Result<T, tokio_postgres::Error>>,
    ) -> anyhow::Result<T> {
        tokio::time::timeout(PG_QUERY_TIMEOUT, fut)
            .await
            .context("postgres query timed out")?
            .map_err(anyhow::Error::from)
            .context("postgres query failed")
    }

    async fn query_with_timeout(
        client: &Client,
        statement: &Statement,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> anyhow::Result<Vec<tokio_postgres::Row>> {
        Self::query_timeout(client.query(statement, params)).await
    }

    async fn query_opt_with_timeout(
        client: &Client,
        statement: &Statement,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> anyhow::Result<Option<tokio_postgres::Row>> {
        Self::query_timeout(client.query_opt(statement, params)).await
    }

    /// Fast connectivity probe: connect and count PoolMeta rows.
    pub async fn probe_pool_meta_count(&self) -> anyhow::Result<u64> {
        self.with_session_retry(|session| async move {
            let rows =
                Self::query_with_timeout(&session.client, &session.pool_meta_count, &[]).await?;
            let count: i64 = rows.first().context("probe count row missing")?.get(0);
            Ok(count.max(0) as u64)
        })
        .await
    }

    /// Bootstrap paginated: keyset page after `(created_block, id)`.
    /// Returns (pools, next cursor, has_more, parse_stats).
    pub async fn fetch_pool_meta_page(
        &self,
        after: &PoolMetaKeyset,
        limit: u64,
    ) -> anyhow::Result<(Vec<DiscoveredPool>, PoolMetaKeyset, bool, ParseStats)> {
        let after = after.clone();
        let limit = i64::try_from(limit).context("pool meta page limit overflow")?;
        self.with_session_retry(move |session| {
            let after = after.clone();
            async move {
                let rows = Self::query_with_timeout(
                    &session.client,
                    &session.pool_meta_keyset,
                    &[&after.created_block, &after.id, &limit],
                )
                .await?;

                let has_more = rows.len() == limit as usize;
                let (pools, _, stats) = parse_rows(&rows, "createdBlock", 0);
                let next = rows
                    .last()
                    .map(|row| PoolMetaKeyset {
                        created_block: row.try_get("createdBlock").unwrap_or(after.created_block),
                        id: row.try_get("id").unwrap_or_else(|_| after.id.clone()),
                    })
                    .unwrap_or(after);
                Ok((pools, next, has_more, stats))
            }
        })
        .await
    }

    /// Incremental: new pools + metadata updates in a single round-trip.
    pub async fn fetch_pool_meta_incremental(
        &self,
        cursor: &DiscoveryCursor,
    ) -> anyhow::Result<(Vec<DiscoveredPool>, u64, u64)> {
        ensure!(
            cursor.last_block > 0,
            "pool discovery not bootstrapped — use keyset bootstrap first"
        );

        let updated_wm = if cursor.last_updated_block == 0 {
            cursor.last_block
        } else {
            cursor.last_updated_block
        };

        let last_block = i32::try_from(cursor.last_block).context("cursor last_block overflow")?;
        let updated_wm = i32::try_from(updated_wm).context("cursor updated watermark overflow")?;
        let initial_created = cursor.last_block;
        let initial_updated = cursor.last_updated_block;

        self.with_session_retry(move |session| async move {
            let rows = Self::query_with_timeout(
                &session.client,
                &session.pool_meta_incremental,
                &[&last_block, &updated_wm, &last_block],
            )
            .await?;
            Ok(parse_incremental_rows(
                &rows,
                initial_created,
                initial_updated,
            ))
        })
        .await
    }

    /// Fetch all token metadata in a single query.
    pub async fn fetch_all_token_metas(&self) -> anyhow::Result<Vec<TokenMeta>> {
        self.with_session_retry(|session| async move {
            let rows = Self::query_with_timeout(&session.client, &session.token_metas, &[]).await?;
            Ok(parse_token_meta_rows(&rows))
        })
        .await
    }

    /// Fetch indexer progress: prefer `_meta` table, fall back to `IndexerProgress`.
    pub async fn fetch_indexer_progress(
        &self,
        chain_id: u64,
    ) -> anyhow::Result<Option<IndexerProgress>> {
        let chain_id = i32::try_from(chain_id).context("chain_id overflow")?;
        self.with_session_retry(move |session| async move {
            if let Some(progress) =
                Self::query_meta(&session.client, &session.indexer_meta, chain_id).await?
            {
                return Ok(Some(progress));
            }
            Self::query_legacy_progress(&session.client, &session.indexer_legacy, chain_id).await
        })
        .await
    }

    async fn query_meta(
        client: &Client,
        statement: &Statement,
        chain_id: i32,
    ) -> anyhow::Result<Option<IndexerProgress>> {
        let Some(row) = Self::query_opt_with_timeout(client, statement, &[&chain_id]).await? else {
            return Ok(None);
        };
        let cid: i32 = row.get(0);
        let progress: i32 = row.get(1);
        let source: Option<i32> = row.get(2);
        let is_ready: Option<bool> = row.get(3);

        if progress <= 0 {
            return Ok(None);
        }
        Ok(Some(IndexerProgress {
            chain_id: cid.max(0) as u64,
            last_processed_block: progress.max(0) as u64,
            source_block: source.map(|v| v.max(0) as u64).filter(|v| *v > 0),
            is_ready,
        }))
    }

    async fn query_legacy_progress(
        client: &Client,
        statement: &Statement,
        chain_id: i32,
    ) -> anyhow::Result<Option<IndexerProgress>> {
        let id = chain_id.to_string();
        let Some(row) = Self::query_opt_with_timeout(client, statement, &[&id]).await? else {
            return Ok(None);
        };
        let last: i32 = row.get(0);
        if last <= 0 {
            return Ok(None);
        }
        Ok(Some(IndexerProgress {
            chain_id: chain_id.max(0) as u64,
            last_processed_block: last.max(0) as u64,
            source_block: None,
            is_ready: None,
        }))
    }
}

/// Cursor for pool discovery — tracks watermarks for incremental queries.
#[derive(Debug, Clone, Default)]
pub struct DiscoveryCursor {
    pub last_block: u64,
    pub last_updated_block: u64,
}

#[derive(Debug, Clone)]
pub struct DiscoveryResult {
    pub pools: Vec<DiscoveredPool>,
    pub cursor: DiscoveryCursor,
    pub complete: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct IndexerProgress {
    pub chain_id: u64,
    pub last_processed_block: u64,
    pub source_block: Option<u64>,
    pub is_ready: Option<bool>,
}

fn block_from_row(row: &tokio_postgres::Row, col: &str) -> u64 {
    let v: i32 = row.try_get(col).unwrap_or(0);
    v.max(0) as u64
}

fn parse_rows(
    rows: &[tokio_postgres::Row],
    block_col: &str,
    initial: u64,
) -> (Vec<DiscoveredPool>, u64, ParseStats) {
    let mut pools = Vec::with_capacity(rows.len());
    let mut max_block = initial;
    let mut stats = ParseStats::default();
    for row in rows {
        let b = block_from_row(row, block_col);
        if b > max_block {
            max_block = b;
        }
        let protocol: String = row.try_get("protocol").unwrap_or_default();
        if let Some(pool) = parse_pg_row(row) {
            record_pg_row(&mut stats, &protocol, true);
            pools.push(pool);
        } else {
            record_pg_row(&mut stats, &protocol, false);
        }
    }
    (pools, max_block, stats)
}

fn parse_incremental_rows(
    rows: &[tokio_postgres::Row],
    initial_created: u64,
    initial_updated: u64,
) -> (Vec<DiscoveredPool>, u64, u64) {
    let mut pools = Vec::with_capacity(rows.len());
    let mut max_created = initial_created;
    let mut max_updated = initial_updated;
    for row in rows {
        let created = block_from_row(row, "createdBlock");
        if created > max_created {
            max_created = created;
        }
        if let Ok(updated) = row.try_get::<_, i32>("updatedAtBlock") {
            let updated = updated.max(0) as u64;
            if updated > max_updated {
                max_updated = updated;
            }
        }
        if let Some(pool) = parse_pg_row(row) {
            pools.push(pool);
        }
    }
    (pools, max_created, max_updated.max(max_created))
}

fn is_transient_pg_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<PgError>().is_some_and(|pg| {
            pg.is_closed()
                || matches!(
                    pg.code(),
                    Some(&SqlState::CONNECTION_FAILURE)
                        | Some(&SqlState::CONNECTION_DOES_NOT_EXIST)
                        | Some(&SqlState::ADMIN_SHUTDOWN)
                        | Some(&SqlState::CRASH_SHUTDOWN)
                )
        })
    })
}

fn parse_token_meta_rows(rows: &[tokio_postgres::Row]) -> Vec<TokenMeta> {
    let mut metas = Vec::with_capacity(rows.len());
    for row in rows {
        let id: String = row.get("id");
        let decimals: Option<i32> = row.get("decimals");
        let Ok(address) = id.parse::<Address>() else {
            continue;
        };
        let dec = decimals.unwrap_or(18).clamp(0, 77) as u8;
        metas.push(TokenMeta {
            address,
            decimals: dec,
        });
    }
    metas
}

fn parse_pg_row(row: &tokio_postgres::Row) -> Option<DiscoveredPool> {
    let id: String = row.try_get("id").ok()?;
    let address: Option<String> = row.try_get("address").ok();
    let protocol: String = row.try_get("protocol").ok()?;
    let tokens: Vec<String> = row.try_get("tokens").ok()?;
    let fee: Option<i32> = row.get("fee");
    let tick_spacing: Option<i32> = row.get("tickSpacing");
    let pool_id: Option<String> = row.try_get("poolId").ok().flatten();
    let hooks: Option<String> = row.try_get("hooks").ok().flatten();
    let pool_type: Option<String> = row.try_get("poolType").ok().flatten();
    let created_block: Option<i32> = row.try_get("createdBlock").ok();
    let created_block = created_block.map(i64::from);

    parse_pool_meta_row(
        &id,
        &protocol,
        &tokens,
        fee,
        tick_spacing,
        pool_id.as_deref(),
        hooks.as_deref(),
        pool_type.as_deref(),
        created_block,
        address.as_deref(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incremental_requires_bootstrapped_cursor() {
        let client = PgClient::new(String::new());
        let err = tokio::runtime::Runtime::new()
            .expect("test runtime")
            .block_on(client.fetch_pool_meta_incremental(&DiscoveryCursor::default()))
            .expect_err("incremental fetch should fail without bootstrap");
        assert!(err.to_string().contains("not bootstrapped"));
    }

    #[test]
    fn pool_meta_sql_quotes_created_block_column() {
        let page = pool_meta_keyset_sql();
        assert!(
            page.contains(r#""createdBlock""#),
            "keyset sql missing quoted createdBlock: {page}"
        );
        assert!(page.contains(r#"("createdBlock", id) >"#));
        assert!(!page.contains("OFFSET"));

        let incremental = pool_meta_incremental_sql();
        assert!(
            incremental.contains(r#""createdBlock", "updatedAtBlock""#),
            "incremental sql missing quoted columns: {incremental}"
        );
        assert!(incremental.contains(r#"ORDER BY "sortBlock" ASC"#));
    }
}
