//! Human-in-the-loop oracle feed tooling (audit / propose / verify).
//!
//! ```text
//! oracle_feeds audit [--top N]
//! oracle_feeds propose [--top N] [--out FILE] [--verify] [--include-non-usd]
//! oracle_feeds verify --file FILE
//! ```

use std::io::{self, Write};
use std::path::PathBuf;

fn cli_line(out: &mut impl Write, s: &str) {
    let _ = out.write_all(s.as_bytes());
    let _ = out.write_all(b"\n");
}

fn cli_stdout(s: &str) {
    cli_line(&mut io::stdout(), s);
}

fn cli_stderr(s: &str) {
    cli_line(&mut io::stderr(), s);
}

use alloy::primitives::Address;
use anyhow::Context;
use reqwest::Client;
use rpbot::config::AppConfig;
use rpbot::infra::http::{HttpClientOpts, build};
use rpbot::infra::pg::PgClient;
use rpbot::services::oracle::CURATED_POLYGON_TOKEN_HINTS;
use rpbot::services::oracle::price_oracle::PriceOracle;
use rpbot::services::oracle::{
    build_audit_report, default_runtime_demand_path, format_config_pyth_feeds,
    load_runtime_demand_snapshot, parse_proposed_pyth_feed_lines, parse_runtime_demand_from_log,
    propose_curated_unmapped_pyth_feeds, propose_pyth_feed_lines, verify_proposed_pyth_feeds,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rpbot::config::load_dotenv();
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 || matches!(args[1].as_str(), "-h" | "--help") {
        print_usage();
        return Ok(());
    }
    let cmd = args[1].as_str();
    let top = arg_usize(&args, "--top").unwrap_or(50);
    let out_path = arg_path(&args, "--out");
    let file_path = arg_path(&args, "--file");
    let verify = args.iter().any(|a| a == "--verify");
    let curated_only = args.iter().any(|a| a == "--curated-only");
    let include_non_usd = args.iter().any(|a| a == "--include-non-usd");

    let config = AppConfig::load()?;
    let http = build_http();
    let oracle = build_oracle(&config, http.clone());
    register_configured_feeds(&oracle, &config.oracle);

    match cmd {
        "audit" => {
            let runtime_file = arg_path(&args, "--runtime-file");
            let runtime_log = arg_path(&args, "--runtime-log");
            run_audit(
                &config,
                &oracle,
                top,
                runtime_file.as_deref(),
                runtime_log.as_deref(),
            )
            .await?
        }
        "propose" => {
            run_propose(
                &config,
                &oracle,
                &http,
                top,
                out_path.as_deref(),
                verify,
                curated_only,
                include_non_usd,
            )
            .await?
        }
        "verify" => {
            let path = file_path.context("--file required for verify")?;
            run_verify(&oracle, &path).await?
        }
        _ => {
            anyhow::bail!("unknown command: {cmd}");
        }
    }
    Ok(())
}

fn print_usage() {
    cli_stderr(
        "oracle_feeds — Polygon oracle feed audit (human-in-the-loop)\n\
         \n\
         Commands:\n\
           audit   Unmapped tokens: PG pool frequency + runtime demand (--top N)\n\
                   --runtime-file JSON snapshot (default: target/run-logs/oracle-demand.json)\n\
                   --runtime-log  Parse latest demand lines from an rpbot .log file\n\
           propose Suggest oracle.pyth_feeds lines via Hermes (--curated-only, --out FILE, --verify)\n\
           verify  Live-check a proposal file (oracle_live_test-style)\n\
         \n\
         Curated hints (manual review):",
    );
    for (label, addr, query) in CURATED_POLYGON_TOKEN_HINTS {
        cli_stderr(&format!("  {label}: {addr} (search: {query})"));
    }
}

fn build_http() -> Client {
    build(HttpClientOpts {
        timeout: std::time::Duration::from_secs(15),
        pool_max_idle_per_host: 4,
        max_redirects: 5,
    })
    .expect("http client")
}

fn build_oracle(config: &AppConfig, http: Client) -> PriceOracle {
    PriceOracle::new(
        http,
        config.oracle.pyth_hermes_url.clone(),
        config.oracle.cache_ttl_ms,
    )
}

fn register_configured_feeds(oracle: &PriceOracle, oracle_cfg: &rpbot::config::OracleConfig) {
    for pair in oracle_cfg.pyth_feeds.split(',').filter(|s| !s.is_empty()) {
        let Some((token_str, feed_id)) = pair.split_once('=') else {
            continue;
        };
        let Ok(token) = token_str.trim().parse::<Address>() else {
            continue;
        };
        oracle.register_pyth_feed(token, feed_id.trim().to_string());
    }
    for pair in oracle_cfg
        .chainlink_feeds
        .split(',')
        .filter(|s| !s.is_empty())
    {
        let Some((token_str, feed_str)) = pair.split_once('=') else {
            continue;
        };
        let Ok(token) = token_str.trim().parse::<Address>() else {
            continue;
        };
        let Ok(feed) = feed_str.trim().parse::<Address>() else {
            continue;
        };
        oracle.register_chainlink_feed(token, feed);
    }
}

async fn load_pool_frequency(
    config: &AppConfig,
    top: usize,
) -> anyhow::Result<Vec<(Address, i64)>> {
    let pg = PgClient::new(config.pg_url.clone())?;
    pg.fetch_token_pool_frequency(i64::try_from(top).unwrap_or(50))
        .await
}

fn load_runtime_demand(
    runtime_file: Option<&std::path::Path>,
    runtime_log: Option<&std::path::Path>,
) -> anyhow::Result<rustc_hash::FxHashMap<Address, u64>> {
    if let Some(path) = runtime_log {
        return parse_runtime_demand_from_log(path);
    }
    let path = runtime_file
        .map(std::path::PathBuf::from)
        .unwrap_or_else(default_runtime_demand_path);
    if path.exists() {
        load_runtime_demand_snapshot(path.as_path())
    } else {
        Ok(rustc_hash::FxHashMap::default())
    }
}

async fn run_audit(
    config: &AppConfig,
    oracle: &PriceOracle,
    top: usize,
    runtime_file: Option<&std::path::Path>,
    runtime_log: Option<&std::path::Path>,
) -> anyhow::Result<()> {
    let freq = load_pool_frequency(config, top.saturating_mul(2)).await?;
    let runtime = load_runtime_demand(runtime_file, runtime_log)?;
    let report = build_audit_report(oracle, &freq, &runtime);
    let runtime_addrs = runtime.len();
    cli_stdout(&format!(
        "oracle audit: scanned={} mapped={} unmapped={} runtime_addrs={}",
        report.scanned_tokens,
        report.mapped_count,
        report.rows.len(),
        runtime_addrs
    ));
    for row in report.rows.iter().take(top) {
        let label = row.label.unwrap_or("-");
        cli_stdout(&format!(
            "  {label:10} {} pools={} runtime={} demand={}",
            row.address, row.pool_hits, row.cycle_hits, row.demand_score,
        ));
    }
    Ok(())
}

async fn run_propose(
    config: &AppConfig,
    oracle: &PriceOracle,
    http: &Client,
    top: usize,
    out: Option<&std::path::Path>,
    verify: bool,
    curated_only: bool,
    include_non_usd: bool,
) -> anyhow::Result<()> {
    let proposals = if curated_only {
        propose_curated_unmapped_pyth_feeds(
            http,
            &config.oracle.pyth_hermes_url,
            oracle,
            include_non_usd,
        )
        .await?
    } else {
        let freq = load_pool_frequency(config, top.saturating_mul(2)).await?;
        let runtime = load_runtime_demand(None, None).unwrap_or_default();
        let report = build_audit_report(oracle, &freq, &runtime);
        let rows: Vec<_> = report.rows.into_iter().take(top).collect();
        propose_pyth_feed_lines(
            http,
            &config.oracle.pyth_hermes_url,
            &rows,
            include_non_usd,
        )
        .await?
    };
    if proposals.is_empty() {
        cli_stdout("propose: no candidates (use --include-non-usd for RR / non-USD review lines)");
        return Ok(());
    }
    let mut text = String::from(
        "# proposed oracle.pyth_feeds — verify before merge (oracle_feeds verify --file)\n",
    );
    for p in &proposals {
        let comment = p.comment.as_deref().unwrap_or("");
        text.push_str(&format!("{}={} # {comment}\n", p.token, p.feed_id));
    }
    if verify {
        verify_proposed_pyth_feeds(oracle, &proposals).await?;
        cli_stdout(&format!(
            "propose: live verify OK for {} feeds",
            proposals.len()
        ));
    }
    if let Some(path) = out {
        std::fs::write(path, &text)?;
        cli_stdout(&format!("propose: wrote {}", path.display()));
    } else {
        let _ = io::stdout().write_all(text.as_bytes());
    }
    cli_stdout(&format!(
        "\nconfig snippet (comma-separated):\n{}",
        format_config_pyth_feeds(&proposals)
    ));
    Ok(())
}

async fn run_verify(oracle: &PriceOracle, path: &std::path::Path) -> anyhow::Result<()> {
    let raw = std::fs::read_to_string(path)?;
    let proposals = parse_proposed_pyth_feed_lines(&raw)?;
    let verified = verify_proposed_pyth_feeds(oracle, &proposals).await?;
    for v in &verified {
        cli_stdout(&format!(
            "OK {} feed={} usd={:.6}",
            v.token, v.feed_id, v.token_usd
        ));
    }
    cli_stdout(&format!("verify: {} feeds passed", verified.len()));
    Ok(())
}

fn arg_usize(args: &[String], flag: &str) -> Option<usize> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
}

fn arg_path(args: &[String], flag: &str) -> Option<PathBuf> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
}
