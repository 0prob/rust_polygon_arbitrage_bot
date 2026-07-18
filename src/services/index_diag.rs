use std::sync::atomic::{AtomicU32, Ordering};

/// Why a PostgreSQL `PoolMeta` row failed `parse_pool_meta_row`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexParseReject {
    BadToken,
    BadIdentity,
    UnknownProtocol,
    UnresolvedProtocol,
    BadShape,
    V4Fields,
    BalancerPoolType,
    V4Hooks,
}

/// Post-parse discovery filter (parsed OK but not merged into routing index).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexRoutableSkip {
    NotFetchable,
    BadShape,
    V4Hooks,
    QuickswapV2Disabled,
    UniswapV2Disabled,
    SushiswapV2Disabled,
}

static PARSE_ATTEMPTS: AtomicU32 = AtomicU32::new(0);
static PARSE_OK: AtomicU32 = AtomicU32::new(0);
static REJ_BAD_TOKEN: AtomicU32 = AtomicU32::new(0);
static REJ_BAD_IDENTITY: AtomicU32 = AtomicU32::new(0);
static REJ_UNKNOWN_PROTOCOL: AtomicU32 = AtomicU32::new(0);
static REJ_UNRESOLVED_PROTOCOL: AtomicU32 = AtomicU32::new(0);
static REJ_BAD_SHAPE: AtomicU32 = AtomicU32::new(0);
static REJ_V4_FIELDS: AtomicU32 = AtomicU32::new(0);
static REJ_BALANCER_POOL_TYPE: AtomicU32 = AtomicU32::new(0);
static REJ_V4_HOOKS: AtomicU32 = AtomicU32::new(0);

static SKIP_NOT_FETCHABLE: AtomicU32 = AtomicU32::new(0);
static SKIP_BAD_SHAPE: AtomicU32 = AtomicU32::new(0);
static SKIP_V4_HOOKS: AtomicU32 = AtomicU32::new(0);
static SKIP_QUICK_V2: AtomicU32 = AtomicU32::new(0);
static SKIP_UNI_V2: AtomicU32 = AtomicU32::new(0);
static SKIP_SUSHI_V2: AtomicU32 = AtomicU32::new(0);

static PG_BOOTSTRAP_PAGES: AtomicU32 = AtomicU32::new(0);
static PG_INCREMENTAL_ROWS: AtomicU32 = AtomicU32::new(0);
static DISCOVERY_NOTIFY: AtomicU32 = AtomicU32::new(0);
static DISCOVERY_SKIPPED_TICKS: AtomicU32 = AtomicU32::new(0);
static INDEXER_STALE_GATED: AtomicU32 = AtomicU32::new(0);

pub fn record_index_parse_attempt() {
    PARSE_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
}

pub fn record_index_parse_ok() {
    PARSE_OK.fetch_add(1, Ordering::Relaxed);
}

pub fn record_index_parse_reject(reason: IndexParseReject) {
    let counter = match reason {
        IndexParseReject::BadToken => &REJ_BAD_TOKEN,
        IndexParseReject::BadIdentity => &REJ_BAD_IDENTITY,
        IndexParseReject::UnknownProtocol => &REJ_UNKNOWN_PROTOCOL,
        IndexParseReject::UnresolvedProtocol => &REJ_UNRESOLVED_PROTOCOL,
        IndexParseReject::BadShape => &REJ_BAD_SHAPE,
        IndexParseReject::V4Fields => &REJ_V4_FIELDS,
        IndexParseReject::BalancerPoolType => &REJ_BALANCER_POOL_TYPE,
        IndexParseReject::V4Hooks => &REJ_V4_HOOKS,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn record_index_routable_skip(reason: IndexRoutableSkip) {
    let counter = match reason {
        IndexRoutableSkip::NotFetchable => &SKIP_NOT_FETCHABLE,
        IndexRoutableSkip::BadShape => &SKIP_BAD_SHAPE,
        IndexRoutableSkip::V4Hooks => &SKIP_V4_HOOKS,
        IndexRoutableSkip::QuickswapV2Disabled => &SKIP_QUICK_V2,
        IndexRoutableSkip::UniswapV2Disabled => &SKIP_UNI_V2,
        IndexRoutableSkip::SushiswapV2Disabled => &SKIP_SUSHI_V2,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn record_index_bootstrap_page() {
    PG_BOOTSTRAP_PAGES.fetch_add(1, Ordering::Relaxed);
}

pub fn record_index_incremental_rows(rows: u32) {
    if rows > 0 {
        PG_INCREMENTAL_ROWS.fetch_add(rows, Ordering::Relaxed);
    }
}

pub fn record_index_discovery_notify() {
    DISCOVERY_NOTIFY.fetch_add(1, Ordering::Relaxed);
}

pub fn record_index_discovery_skipped_tick() {
    DISCOVERY_SKIPPED_TICKS.fetch_add(1, Ordering::Relaxed);
}

pub fn record_indexer_stale_gate() {
    INDEXER_STALE_GATED.fetch_add(1, Ordering::Relaxed);
}

pub fn log_index_summary() {
    let attempts = PARSE_ATTEMPTS.load(Ordering::Relaxed);
    let parse_rejects = attempts.saturating_sub(PARSE_OK.load(Ordering::Relaxed));
    let routable_skips = SKIP_NOT_FETCHABLE.load(Ordering::Relaxed)
        + SKIP_BAD_SHAPE.load(Ordering::Relaxed)
        + SKIP_V4_HOOKS.load(Ordering::Relaxed)
        + SKIP_QUICK_V2.load(Ordering::Relaxed)
        + SKIP_UNI_V2.load(Ordering::Relaxed)
        + SKIP_SUSHI_V2.load(Ordering::Relaxed);
    if attempts == 0 && routable_skips == 0 && PG_INCREMENTAL_ROWS.load(Ordering::Relaxed) == 0 {
        return;
    }
    crate::info!(
        "index: parse_ok={} parse_reject={} bad_token={} bad_id={} unknown_proto={} unresolved={} \
         bad_shape={} v4_fields={} balancer_type={} v4_hooks={} routable_skip={} skip_fetch={} \
         skip_shape={} skip_v4_hooks={} skip_quick_v2={} skip_uni_v2={} skip_sushi_v2={} \
         pg_pages={} pg_incr_rows={} notify={} disc_skip_ticks={} stale_gated={}",
        PARSE_OK.load(Ordering::Relaxed),
        parse_rejects,
        REJ_BAD_TOKEN.load(Ordering::Relaxed),
        REJ_BAD_IDENTITY.load(Ordering::Relaxed),
        REJ_UNKNOWN_PROTOCOL.load(Ordering::Relaxed),
        REJ_UNRESOLVED_PROTOCOL.load(Ordering::Relaxed),
        REJ_BAD_SHAPE.load(Ordering::Relaxed),
        REJ_V4_FIELDS.load(Ordering::Relaxed),
        REJ_BALANCER_POOL_TYPE.load(Ordering::Relaxed),
        REJ_V4_HOOKS.load(Ordering::Relaxed),
        routable_skips,
        SKIP_NOT_FETCHABLE.load(Ordering::Relaxed),
        SKIP_BAD_SHAPE.load(Ordering::Relaxed),
        SKIP_V4_HOOKS.load(Ordering::Relaxed),
        SKIP_QUICK_V2.load(Ordering::Relaxed),
        SKIP_UNI_V2.load(Ordering::Relaxed),
        SKIP_SUSHI_V2.load(Ordering::Relaxed),
        PG_BOOTSTRAP_PAGES.load(Ordering::Relaxed),
        PG_INCREMENTAL_ROWS.load(Ordering::Relaxed),
        DISCOVERY_NOTIFY.load(Ordering::Relaxed),
        DISCOVERY_SKIPPED_TICKS.load(Ordering::Relaxed),
        INDEXER_STALE_GATED.load(Ordering::Relaxed),
    );
}
