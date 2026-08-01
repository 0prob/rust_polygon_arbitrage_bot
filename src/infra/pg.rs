use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use std::future::poll_fn;

use anyhow::{Context, ensure};
use deadpool_postgres::{
    Manager, ManagerConfig, Pool, PoolError, RecyclingMethod, Runtime, TimeoutType, Timeouts,
};
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
        -- Drop precompile-range / zero tokens (matches is_plausible_contract_address).
        -- Live: 241 bad_shape parse rejects (132 UniV3 + 85 QuickV2 + …) were all
        -- token addresses < 0x10000; filtering here saves bootstrap rows + parse work.
        AND NOT EXISTS (
            SELECT 1 FROM unnest(tokens) AS t
            WHERE length(replace(lower(t), '0x', '')) <> 40
               OR lower(t) < '0x0000000000000000000000000000000000010000'
        )
        AND (
            protocol <> 'UNISWAP_V4'
            OR (
                fee IS NOT NULL
                AND "tickSpacing" IS NOT NULL
                AND "poolId" IS NOT NULL
                AND hooks IS NOT NULL
                AND (
                    lower(hooks) = '0x0000000000000000000000000000000000000000'
                    OR ((('x' || right(lower(hooks), 4))::bit(16)::int & 204) = 0)
                )
            )
        )
        AND (
            protocol <> 'BALANCER_V2'
            OR (
                fee IS NOT NULL
                AND "poolId" IS NOT NULL
                AND "poolType" IN ('weighted', 'stable', 'linear')
            )
        )
        AND (
            protocol <> 'CURVE'
            OR (
                fee IS NOT NULL
                AND fee > 0
                AND "poolType" IN ('stable', 'crypto', 'stable_ng', 'crypto_ng')
            )
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
            WHERE ("createdBlock", id) > ($1, $2)
            AND {POOL_META_VALIDITY_SQL}
            UNION ALL
            SELECT {POOL_META_COLUMNS}, "updatedAtBlock", "updatedAtBlock" AS "sortBlock"
            FROM "PoolMeta"
            WHERE ("updatedAtBlock", id) > ($3, $4) AND "createdBlock" <= $5
            AND {POOL_META_VALIDITY_SQL}
        ) AS combined
        ORDER BY "sortBlock" ASC, id ASC
        LIMIT $6
        "#
    )
});

const INDEXER_META_SQL: &str = r#"SELECT "chainId", "progressBlock", "sourceBlock", "isReady" FROM "_meta" WHERE "chainId" = $1"#;

const INDEXER_LEGACY_SQL: &str = r#"SELECT "lastProcessedBlock" FROM "IndexerProgress" WHERE id = $1 ORDER BY "lastProcessedBlock" DESC LIMIT 1"#;

const TOKEN_METAS_SQL: &str = r#"SELECT id, decimals FROM "TokenMeta""#;

const TOKEN_POOL_FREQUENCY_SQL: &str = r#"
    SELECT lower(t) AS token, COUNT(*)::bigint AS pool_count
    FROM "PoolMeta" p, unnest(p.tokens) AS t
    WHERE cardinality(p.tokens) >= 2
    GROUP BY 1
    ORDER BY pool_count DESC
    LIMIT $1
"#;

const POOL_META_COUNT_SQL: &str = r#"SELECT COUNT(*)::bigint FROM "PoolMeta""#;

const PG_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PG_QUERY_TIMEOUT: Duration = Duration::from_secs(15);
/// Bound `pool.get()` so a saturated pool cannot stall discovery forever.
const PG_POOL_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
/// Detect dead TCP peers faster than libpq's 2h keepalive default.
const PG_KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
const PG_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const PG_KEEPALIVE_RETRIES: u32 = 3;
const PG_TCP_USER_TIMEOUT: Duration = Duration::from_secs(30);
/// Cap incremental catch-up rows per query so a long indexer gap cannot OOM the bot.
const POOL_META_INCREMENTAL_LIMIT: i64 = 10_000;
const MAX_POOL_SIZE: usize = 16; // discovery + bootstrap + token + health + spare
const NOTIFY_CHANNEL: &str = "pool_meta_channel";
const PG_APP_NAME: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

/// Parse a libpq URL / key-value string (tokio-postgres `Config::from_str`) and apply bot defaults.
fn pg_config(url: &str) -> anyhow::Result<tokio_postgres::Config> {
    let mut config: tokio_postgres::Config =
        url.parse().context("invalid postgres connection URL")?;
    // URL query params already applied by `from_str`; fill only unset knobs.
    if config.get_connect_timeout().is_none() {
        config.connect_timeout(PG_CONNECT_TIMEOUT);
    }
    if config.get_application_name().is_none() {
        config.application_name(PG_APP_NAME);
    }
    if config.get_tcp_user_timeout().is_none() {
        config.tcp_user_timeout(PG_TCP_USER_TIMEOUT);
    }
    config.keepalives(true);
    config.keepalives_idle(PG_KEEPALIVE_IDLE);
    if config.get_keepalives_interval().is_none() {
        config.keepalives_interval(PG_KEEPALIVE_INTERVAL);
    }
    if config.get_keepalives_retries().is_none() {
        config.keepalives_retries(PG_KEEPALIVE_RETRIES);
    }
    Ok(config)
}

/// Checkout with deadpool's configured wait/create/recycle timeouts (requires Runtime).
async fn pg_checkout(pool: &Pool) -> anyhow::Result<deadpool_postgres::Client> {
    pool.get().await.map_err(|e| {
        let label = match &e {
            PoolError::Timeout(TimeoutType::Wait) => "pg pool wait timed out",
            PoolError::Timeout(TimeoutType::Create) => "pg pool create timed out",
            PoolError::Timeout(TimeoutType::Recycle) => "pg pool recycle timed out",
            _ => "pg pool checkout failed",
        };
        // Keep PoolError in the chain so transient retry can classify create/recycle.
        anyhow::Error::new(e).context(label)
    })
}

/// Execute a query against the pool with a cached (per-connection) prepared statement.
async fn pg_query(
    pool: &Pool,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> anyhow::Result<Vec<Row>> {
    let client = pg_checkout(pool).await?;
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
    let client = pg_checkout(pool).await?;
    let stmt = tokio::time::timeout(PG_QUERY_TIMEOUT, client.prepare_cached(sql))
        .await
        .context("pg prepare_cached timed out")?
        .context("pg prepare_cached failed")?;
    tokio::time::timeout(PG_QUERY_TIMEOUT, client.query_opt(&stmt, params))
        .await
        .context("pg query_opt timed out")?
        .context("pg query_opt failed")
}

/// Same as `pg_query_opt` but with transient-error retry.
async fn pg_query_opt_retry(
    pool: &Pool,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> anyhow::Result<Option<Row>> {
    match pg_query_opt(pool, sql, params).await {
        Ok(row) => Ok(row),
        Err(e) if is_transient_pg_error(&e) => {
            crate::warn!("pg transient error — retrying: {e:#}");
            pg_query_opt(pool, sql, params).await
        }
        Err(e) => Err(e),
    }
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
        let pg_config = pg_config(&url_str)?;
        // Verified: recycle test-query (deadpool FAQ) — safer than Fast for long-lived indexer TCP.
        let mgr = Manager::from_config(
            pg_config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Verified,
            },
        );
        // Timeouts require Runtime (deadpool PoolBuilder); get() uses these via timeout_get.
        let pool = Pool::builder(mgr)
            .max_size(MAX_POOL_SIZE)
            .runtime(Runtime::Tokio1)
            .timeouts(Timeouts {
                wait: Some(PG_POOL_WAIT_TIMEOUT),
                create: Some(PG_CONNECT_TIMEOUT),
                recycle: Some(PG_QUERY_TIMEOUT),
            })
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
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
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
        let config = pg_config(url_str)?;
        let (client, mut connection) = config
            .connect(NoTls)
            .await
            .context("pg LISTEN connect failed")?;

        // std::future::poll_fn — same as futures_util::stream::poll_fn + next(), no Stream dep.
        let listen_sql = format!("LISTEN {NOTIFY_CHANNEL}");
        let subscribe = client.batch_execute(&listen_sql);
        tokio::pin!(subscribe);
        loop {
            tokio::select! {
                result = &mut subscribe => {
                    result.context("pg LISTEN pool_meta_channel failed")?;
                    break;
                }
                message = poll_fn(|cx| connection.poll_message(cx)) => match message {
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.into()),
                    None => anyhow::bail!("pg LISTEN connection closed during subscribe"),
                },
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
        crate::info!("pg LISTEN subscribed to {NOTIFY_CHANNEL}");

        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                message = poll_fn(|cx| connection.poll_message(cx)) => {
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
    ) -> anyhow::Result<(Vec<DiscoveredPool>, DiscoveryCursor, bool, ParseStats)> {
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
        let rows = pg_query_retry(
            &self.pool,
            POOL_META_INCREMENTAL_SQL.as_str(),
            &[
                &last_block,
                &cursor.last_block_id,
                &updated_wm_i32,
                &cursor.last_updated_id,
                &last_block,
                &POOL_META_INCREMENTAL_LIMIT,
            ],
        )
        .await?;
        let has_more = rows.len() == POOL_META_INCREMENTAL_LIMIT as usize;
        let (pools, next_cursor, stats) = parse_incremental_rows(&rows, cursor);
        Ok((pools, next_cursor, has_more, stats))
    }

    pub async fn fetch_all_token_metas(&self) -> anyhow::Result<Vec<TokenMeta>> {
        let rows = pg_query(&self.pool, TOKEN_METAS_SQL, &[]).await?;
        Ok(parse_token_meta_rows(&rows))
    }

    /// Token addresses ranked by how many valid `PoolMeta` rows reference them.
    pub async fn fetch_token_pool_frequency(
        &self,
        limit: i64,
    ) -> anyhow::Result<Vec<(Address, i64)>> {
        let rows = pg_query(&self.pool, TOKEN_POOL_FREQUENCY_SQL, &[&limit]).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let token: String = row.try_get("token")?;
            let count: i64 = row.try_get("pool_count")?;
            let hex = if token.starts_with("0x") {
                token
            } else {
                format!("0x{token}")
            };
            let addr: Address = hex
                .parse()
                .context("invalid token address in pool frequency")?;
            out.push((addr, count));
        }
        Ok(out)
    }

    pub async fn fetch_indexer_progress(
        &self,
        chain_id: u64,
    ) -> anyhow::Result<Option<IndexerProgress>> {
        let chain_id_i32 = i32::try_from(chain_id).context("chain_id overflow")?;
        if let Some(row) =
            pg_query_opt_retry(&self.pool, INDEXER_META_SQL, &[&chain_id_i32]).await?
        {
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

/// Cursor for pool discovery — tracks watermarks for incremental queries.
#[derive(Debug, Clone, Default)]
pub struct DiscoveryCursor {
    pub last_block: u64,
    pub last_block_id: String,
    pub last_updated_block: u64,
    pub last_updated_id: String,
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
        if let Some(pool) = parse_pg_row(row) {
            record_pg_row(&mut stats, &pool.protocol_label, true);
            pools.push(pool);
        } else {
            let protocol: String = row.try_get("protocol").unwrap_or_default();
            record_pg_row(&mut stats, &protocol, false);
        }
    }
    (pools, max_block, stats)
}

fn parse_incremental_rows(
    rows: &[Row],
    initial: &DiscoveryCursor,
) -> (Vec<DiscoveredPool>, DiscoveryCursor, ParseStats) {
    let mut pools = Vec::with_capacity(rows.len());
    let mut cursor = initial.clone();
    let mut stats = ParseStats::default();
    for row in rows {
        let id: String = row.try_get("id").unwrap_or_default();
        if let Ok(updated) = row.try_get::<_, i32>("updatedAtBlock") {
            let updated = updated.max(0) as u64;
            advance_cursor_pair(
                &mut cursor.last_updated_block,
                &mut cursor.last_updated_id,
                updated,
                &id,
            );
        } else {
            let created = block_from_row(row, "createdBlock");
            advance_cursor_pair(
                &mut cursor.last_block,
                &mut cursor.last_block_id,
                created,
                &id,
            );
        }
        if let Some(pool) = parse_pg_row(row) {
            record_pg_row(&mut stats, &pool.protocol_label, true);
            pools.push(pool);
        } else {
            let protocol: String = row.try_get("protocol").unwrap_or_default();
            record_pg_row(&mut stats, &protocol, false);
        }
    }
    if cursor.last_updated_block < cursor.last_block {
        cursor.last_updated_block = cursor.last_block;
        cursor.last_updated_id.clear();
    }
    (pools, cursor, stats)
}

fn advance_cursor_pair(block: &mut u64, id: &mut String, candidate_block: u64, candidate_id: &str) {
    if candidate_block > *block || (candidate_block == *block && candidate_id > id.as_str()) {
        *block = candidate_block;
        id.clear();
        id.push_str(candidate_id);
    }
}

fn is_transient_pg_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if let Some(pool_err) = cause.downcast_ref::<PoolError>() {
            // Create/recycle timeouts often mean a dead conn being replaced; wait = saturated.
            // Backend errors still classify via the nested PgError below.
            return matches!(
                pool_err,
                PoolError::Timeout(TimeoutType::Create | TimeoutType::Recycle)
            );
        }
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
    fn pg_config_uses_libpq_url_parser() {
        let cfg = pg_config("postgres://alice:p%40ss@127.0.0.1:6543/indexer?connect_timeout=9")
            .expect("parse url");
        assert_eq!(cfg.get_user(), Some("alice"));
        assert_eq!(cfg.get_password(), Some(b"p@ss".as_slice()));
        assert_eq!(cfg.get_dbname(), Some("indexer"));
        assert_eq!(cfg.get_connect_timeout(), Some(&Duration::from_secs(9)));
        assert_eq!(cfg.get_application_name(), Some(PG_APP_NAME));
        assert_eq!(cfg.get_tcp_user_timeout(), Some(&PG_TCP_USER_TIMEOUT));
        assert!(cfg.get_keepalives());
        assert_eq!(cfg.get_keepalives_idle(), PG_KEEPALIVE_IDLE);
        assert_eq!(cfg.get_keepalives_interval(), Some(PG_KEEPALIVE_INTERVAL));
        assert_eq!(cfg.get_keepalives_retries(), Some(PG_KEEPALIVE_RETRIES));
    }

    #[test]
    fn pg_config_preserves_url_keepalive_overrides() {
        let cfg = pg_config("postgres://127.0.0.1/db?keepalives_interval=7&keepalives_retries=9")
            .expect("parse url");
        assert_eq!(cfg.get_keepalives_interval(), Some(Duration::from_secs(7)));
        assert_eq!(cfg.get_keepalives_retries(), Some(9));
    }

    #[test]
    fn pg_config_rejects_garbage() {
        assert!(pg_config("not-a-postgres-url").is_err());
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
        assert!(
            page.contains("0000000000000000000000000000000000010000"),
            "keyset sql must filter precompile-range tokens: {page}"
        );
        assert!(page.contains("protocol <> 'UNISWAP_V4'"));
        assert!(page.contains("protocol <> 'BALANCER_V2'"));
        assert!(page.contains("protocol <> 'CURVE'"));
        assert!(page.contains("right(lower(hooks), 4)"));
        assert!(!page.contains("OFFSET"));

        let incremental = POOL_META_INCREMENTAL_SQL.as_str();
        assert!(
            incremental.contains(r#""createdBlock", "updatedAtBlock""#),
            "incremental sql missing quoted columns: {incremental}"
        );
        assert!(incremental.contains("cardinality(tokens) >= 2"));
        assert!(
            incremental.contains("0000000000000000000000000000000000010000"),
            "incremental sql must filter precompile-range tokens"
        );
        assert!(incremental.contains(r#"("createdBlock", id) > ($1, $2)"#));
        assert!(incremental.contains(r#"("updatedAtBlock", id) > ($3, $4)"#));
        assert!(incremental.contains(r#"ORDER BY "sortBlock" ASC, id ASC"#));
        assert!(incremental.contains("LIMIT $6"));
    }

    #[test]
    fn cursor_pair_advances_within_a_block() {
        let mut block = 100;
        let mut id = "0x01".to_string();
        advance_cursor_pair(&mut block, &mut id, 100, "0x02");
        assert_eq!((block, id.as_str()), (100, "0x02"));
        advance_cursor_pair(&mut block, &mut id, 100, "0x01");
        assert_eq!((block, id.as_str()), (100, "0x02"));
    }
}
