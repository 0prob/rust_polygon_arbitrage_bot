use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, ensure};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures_util::{StreamExt, stream};
use tokio::sync::watch;
use tokio_postgres::AsyncMessage;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Error as PgError, NoTls, Row, types::ToSql};

use crate::services::discovery::{DiscoveredPool, TokenMeta, parse_pool_meta_row};
use crate::services::pipeline_survival::{ParseStats, record_pg_row};
use alloy::primitives::Address;

const POOL_META_COLUMNS: &str = r#"id, address, protocol::text, tokens, fee, "tickSpacing", "poolId", hooks, "poolType", "createdBlock""#;
const POOL_META_VALIDITY_SQL: &str = r#"
        protocol IS NOT NULL
        AND cardinality(tokens) >= 2
        AND (
            protocol <> 'UNISWAP_V4'
            OR (fee IS NOT NULL AND "tickSpacing" IS NOT NULL AND "poolId" IS NOT NULL AND hooks IS NOT NULL)
        )
        AND (
            protocol <> 'BALANCER_V2'
            OR cardinality(tokens) >= 2
        )
    "#;

static POOL_META_KEYSET_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        r#"SELECT {POOL_META_COLUMNS} FROM "PoolMeta"
        WHERE ("createdBlock", id) > ($1, $2)
        AND {POOL_META_VALIDITY_SQL}
        ORDER BY "createdBlock", id
        LIMIT $3"#
    )
});

static POOL_META_INCREMENTAL_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        r#"
        SELECT {POOL_META_COLUMNS}, "updatedAtBlock", "sortBlock" FROM (
            SELECT {POOL_META_COLUMNS}, NULL::integer AS "updatedAtBlock", "createdBlock" AS "sortBlock"
            FROM "PoolMeta"
            WHERE "createdBlock" > $1
            AND {POOL_META_VALIDITY_SQL}
            UNION ALL
            SELECT {POOL_META_COLUMNS}, "updatedAtBlock", "updatedAtBlock" AS "sortBlock"
            FROM "PoolMeta"
            WHERE "updatedAtBlock" > $2 AND "createdBlock" <= $3
            AND {POOL_META_VALIDITY_SQL}
        ) AS combined
        ORDER BY "sortBlock" ASC
        LIMIT $4
        "#
    )
});

const INDEXER_META_SQL: &str = r#"SELECT "chainId", "progressBlock", "sourceBlock", "isReady" FROM "_meta" WHERE "chainId" = $1"#;

const INDEXER_LEGACY_SQL: &str = r#"SELECT "lastProcessedBlock" FROM "IndexerProgress" WHERE id = $1 ORDER BY "lastProcessedBlock" DESC LIMIT 1"#;

const TOKEN_METAS_SQL: &str = r#"SELECT id, decimals FROM "TokenMeta""#;

const POOL_META_COUNT_SQL: &str = r#"SELECT COUNT(*)::bigint FROM "PoolMeta""#;

const PG_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PG_QUERY_TIMEOUT: Duration = Duration::from_secs(15);
/// Cap incremental catch-up rows per query so a long indexer gap cannot OOM the bot.
const POOL_META_INCREMENTAL_LIMIT: i64 = 10_000;
const MAX_POOL_SIZE: usize = 16; // increased from 8: discovery + bootstrap + token + health + spare
const NOTIFY_CHANNEL: &str = "pool_meta_channel";

/// Execute a query against the pool with a cached (per-connection) prepared statement.
async fn pg_query(
    pool: &Pool,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> anyhow::Result<Vec<Row>> {
    let client = pool.get().await.context("pg pool checkout failed")?;
    let stmt = tokio::time::timeout(PG_QUERY_TIMEOUT, client.prepare_cached(sql))
        .await
        .context("pg prepare_cached timed out")?
        .context("pg prepare_cached failed")?;
    tokio::time::timeout(PG_QUERY_TIMEOUT, client.query(&stmt, params))
        .await
        .context("pg query timed out")?
        .context("pg query failed")
}

/// Same as `pg_query` but with transient-error retry.
async fn pg_query_retry(
    pool: &Pool,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> anyhow::Result<Vec<Row>> {
    match pg_query(pool, sql, params).await {
        Ok(rows) => Ok(rows),
        Err(e) if is_transient_pg_error(&e) => {
            crate::warn!("pg transient error — retrying: {e:#}");
            pg_query(pool, sql, params).await
        }
        Err(e) => Err(e),
    }
}

/// Same as `pg_query` but returns at most one row.
async fn pg_query_opt(
    pool: &Pool,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> anyhow::Result<Option<Row>> {
    let client = pool.get().await.context("pg pool checkout failed")?;
    let stmt = tokio::time::timeout(PG_QUERY_TIMEOUT, client.prepare_cached(sql))
        .await
        .context("pg prepare_cached timed out")?
        .context("pg prepare_cached failed")?;
    tokio::time::timeout(PG_QUERY_TIMEOUT, client.query_opt(&stmt, params))
        .await
        .context("pg query_opt timed out")?
        .context("pg query_opt failed")
}

/// Keyset cursor for paginated PoolMeta bootstrap (avoids OFFSET scans).
#[derive(Debug, Clone, Default)]
pub struct PoolMetaKeyset {
    pub created_block: i32,
    pub id: String,
}

/// Connection-pooled PostgreSQL client — multiple concurrent queries without blocking.
pub struct PgClient {
    pool: Pool,
}

impl PgClient {
    pub fn new(url_str: String) -> anyhow::Result<Self> {
        let (user, password, host, port, dbname) =
            parse_pg_url(&url_str).context("invalid postgres connection URL")?;
        let mut pg_config = tokio_postgres::Config::new();
        pg_config.host(&host);
        pg_config.port(port);
        pg_config.dbname(&dbname);
        pg_config.user(&user);
        if !password.is_empty() {
            pg_config.password(&password);
        }
        pg_config.connect_timeout(PG_CONNECT_TIMEOUT);
        // HFT-optimized connection pool: small, fast-recycle, bounded wait.
        let mgr = Manager::from_config(
            pg_config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(mgr)
            .max_size(MAX_POOL_SIZE)
            .runtime(deadpool_postgres::Runtime::Tokio1)
            .build()
            .context("deadpool postgres pool build failed")?;
        Ok(Self { pool })
    }

    pub async fn spawn_notify_listener(
        url_str: &str,
        notify_flag: Arc<AtomicBool>,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let url = url_str.to_string();
        let mut backoff = Duration::from_secs(1);
        loop {
            if *shutdown.borrow() {
                break;
            }
            match Self::run_notify_listener(&url, &notify_flag, &mut shutdown).await {
                Ok(()) => break,
                Err(e) => {
                    crate::warn!(
                        "pg LISTEN disconnected ({e:#}); reconnecting in {}s",
                        backoff.as_secs()
                    );
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                break;
                            }
                        }
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                }
            }
        }
        crate::info!("pg LISTEN listener shut down");
        Ok(())
    }

    async fn run_notify_listener(
        url_str: &str,
        notify_flag: &AtomicBool,
        shutdown: &mut watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let (user, password, host, port, dbname) =
            parse_pg_url(url_str).context("invalid postgres LISTEN connection URL")?;
        let mut config = tokio_postgres::Config::new();
        config.host(&host);
        config.port(port);
        config.dbname(&dbname);
        config.user(&user);
        if !password.is_empty() {
            config.password(&password);
        }
        config.connect_timeout(PG_CONNECT_TIMEOUT);
        let (client, mut connection) = config
            .connect(NoTls)
            .await
            .context("pg LISTEN connect failed")?;

        let mut messages = stream::poll_fn(move |cx| connection.poll_message(cx));
        let listen_sql = format!("LISTEN {NOTIFY_CHANNEL}");
        let subscribe = client.batch_execute(&listen_sql);
        tokio::pin!(subscribe);
        loop {
            tokio::select! {
                result = &mut subscribe => {
                    result.context("pg LISTEN pool_meta_channel failed")?;
                    break;
                }
                message = messages.next() => match message {
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.into()),
                    None => anyhow::bail!("pg LISTEN connection closed during subscribe"),
                },
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
        crate::info!("pg LISTEN subscribed to {NOTIFY_CHANNEL}");

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return Ok(());
                    }
                }
                message = messages.next() => {
                    match message {
                        Some(Ok(AsyncMessage::Notification(notification))) => {
                            if notification.channel() == NOTIFY_CHANNEL {
                                notify_flag.store(true, Ordering::Release);
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            return Err(e.into());
                        }
                        None => {
                            anyhow::bail!("pg LISTEN connection closed");
                        }
                    }
                }
            }
        }
    }

    pub async fn probe_pool_meta_count(&self) -> anyhow::Result<u64> {
        let rows = pg_query(&self.pool, POOL_META_COUNT_SQL, &[]).await?;
        let count: i64 = rows.first().context("probe count row missing")?.get(0);
        Ok(count.max(0) as u64)
    }

    pub async fn fetch_pool_meta_page(
        &self,
        after: &PoolMetaKeyset,
        limit: u64,
    ) -> anyhow::Result<(Vec<DiscoveredPool>, PoolMetaKeyset, bool, ParseStats)> {
        let after = after.clone();
        let limit_i64 = i64::try_from(limit).context("pool meta page limit overflow")?;
        let rows = pg_query_retry(
            &self.pool,
            POOL_META_KEYSET_SQL.as_str(),
            &[&after.created_block, &after.id, &limit_i64],
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

    pub async fn fetch_pool_meta_incremental(
        &self,
        cursor: &DiscoveryCursor,
    ) -> anyhow::Result<(Vec<DiscoveredPool>, u64, u64, bool)> {
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
        let updated_wm_i32 =
            i32::try_from(updated_wm).context("cursor updated watermark overflow")?;
        let initial_created = cursor.last_block;
        let initial_updated = cursor.last_updated_block;
        let rows = pg_query_retry(
            &self.pool,
            POOL_META_INCREMENTAL_SQL.as_str(),
            &[
                &last_block,
                &updated_wm_i32,
                &last_block,
                &POOL_META_INCREMENTAL_LIMIT,
            ],
        )
        .await?;
        let has_more = rows.len() == POOL_META_INCREMENTAL_LIMIT as usize;
        let (pools, max_created, max_updated) =
            parse_incremental_rows(&rows, initial_created, initial_updated);
        Ok((pools, max_created, max_updated, has_more))
    }

    pub async fn fetch_all_token_metas(&self) -> anyhow::Result<Vec<TokenMeta>> {
        let rows = pg_query(&self.pool, TOKEN_METAS_SQL, &[]).await?;
        Ok(parse_token_meta_rows(&rows))
    }

    pub async fn fetch_indexer_progress(
        &self,
        chain_id: u64,
    ) -> anyhow::Result<Option<IndexerProgress>> {
        let chain_id_i32 = i32::try_from(chain_id).context("chain_id overflow")?;
        if let Some(row) = pg_query_opt(&self.pool, INDEXER_META_SQL, &[&chain_id_i32]).await? {
            return Ok(Some(parse_meta_row(row, chain_id_i32)));
        }
        let id = chain_id.to_string();
        let rows = pg_query(&self.pool, INDEXER_LEGACY_SQL, &[&id]).await?;
        Ok(rows.into_iter().next().map(|row| {
            let last: i32 = row.get(0);
            IndexerProgress {
                chain_id,
                last_processed_block: last.max(0) as u64,
                source_block: None,
                is_ready: None,
            }
        }))
    }
}

/// Lightweight PG URL parser (avoids url crate dependency).
fn parse_pg_url(url_str: &str) -> Option<(String, String, String, u16, String)> {
    let rest = url_str
        .strip_prefix("postgres://")
        .or_else(|| url_str.strip_prefix("postgresql://"))?;
    let (userinfo, rest) = rest.split_once('@').unwrap_or(("", rest));
    let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));
    let (hostport, db_and_params) = rest.split_once('/').unwrap_or((rest, ""));
    let dbname = db_and_params.split('?').next().unwrap_or("");
    let (host, port_str) = hostport.rsplit_once(':').unwrap_or((hostport, "5432"));
    let port: u16 = port_str.parse().unwrap_or(5432);
    Some((
        user.to_string(),
        password.to_string(),
        host.to_string(),
        port,
        dbname.to_string(),
    ))
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

fn parse_meta_row(row: Row, _chain_id: i32) -> IndexerProgress {
    let cid: i32 = row.get(0);
    let progress: i32 = row.get(1);
    let source: Option<i32> = row.get(2);
    let is_ready: Option<bool> = row.get(3);
    IndexerProgress {
        chain_id: cid.max(0) as u64,
        last_processed_block: progress.max(0) as u64,
        source_block: source.map(|v| v.max(0) as u64).filter(|v| *v > 0),
        is_ready,
    }
}

fn block_from_row(row: &Row, col: &str) -> u64 {
    let v: i32 = row.try_get(col).unwrap_or(0);
    v.max(0) as u64
}

fn parse_rows(
    rows: &[Row],
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
    rows: &[Row],
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

fn parse_token_meta_rows(rows: &[Row]) -> Vec<TokenMeta> {
    let mut metas = Vec::with_capacity(rows.len());
    for row in rows {
        let id: String = row.get("id");
        let decimals: Option<i32> = row.get("decimals");
        let Ok(address) = id.parse::<Address>() else {
            continue;
        };
        let Some(dec) = valid_token_decimals(decimals) else {
            continue;
        };
        metas.push(TokenMeta {
            address,
            decimals: dec,
        });
    }
    metas
}

fn valid_token_decimals(decimals: Option<i32>) -> Option<u8> {
    let decimals = u8::try_from(decimals?).ok()?;
    (decimals <= crate::core::constants::MAX_SUPPORTED_TOKEN_DECIMALS).then_some(decimals)
}

fn parse_pg_row(row: &Row) -> Option<DiscoveredPool> {
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
        let client = PgClient::new("postgres://localhost:5432/test".into()).expect("test pg url");
        let err = tokio::runtime::Runtime::new()
            .expect("test runtime")
            .block_on(client.fetch_pool_meta_incremental(&DiscoveryCursor::default()))
            .expect_err("incremental fetch should fail without bootstrap");
        assert!(err.to_string().contains("not bootstrapped"));
    }

    #[test]
    fn token_decimal_metadata_rejects_unknown_and_unsupported_values() {
        assert_eq!(valid_token_decimals(None), None);
        assert_eq!(valid_token_decimals(Some(-1)), None);
        assert_eq!(valid_token_decimals(Some(31)), None);
        assert_eq!(valid_token_decimals(Some(77)), None);
        assert_eq!(valid_token_decimals(Some(0)), Some(0));
        assert_eq!(valid_token_decimals(Some(6)), Some(6));
        assert_eq!(valid_token_decimals(Some(18)), Some(18));
    }

    #[test]
    fn pool_meta_sql_generates_keyset_without_offset() {
        let page = POOL_META_KEYSET_SQL.as_str();
        assert!(
            page.contains(r#""createdBlock""#),
            "keyset sql missing quoted createdBlock: {page}"
        );
        assert!(page.contains(r#"("createdBlock", id) >"#));
        assert!(page.contains("cardinality(tokens) >= 2"));
        assert!(page.contains("protocol <> 'UNISWAP_V4'"));
        assert!(!page.contains("OFFSET"));

        let incremental = POOL_META_INCREMENTAL_SQL.as_str();
        assert!(
            incremental.contains(r#""createdBlock", "updatedAtBlock""#),
            "incremental sql missing quoted columns: {incremental}"
        );
        assert!(incremental.contains("cardinality(tokens) >= 2"));
        assert!(incremental.contains(r#"ORDER BY "sortBlock" ASC"#));
        assert!(incremental.contains("LIMIT $4"));
    }
}
