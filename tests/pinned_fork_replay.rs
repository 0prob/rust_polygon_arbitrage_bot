use std::process::Command;

use alloy::providers::{DynProvider, Provider, ProviderBuilder};

fn polygon_rpc_url() -> String {
    std::env::var("POLYGON_RPC_URL")
        .unwrap_or_else(|_| "https://polygon-bor-rpc.publicnode.com".to_string())
}

fn provider(rpc: &str) -> anyhow::Result<DynProvider> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()?;
    Ok(ProviderBuilder::new()
        .connect_reqwest(client, rpc.parse()?)
        .erased())
}

fn latest_block_number(rpc: &str) -> anyhow::Result<u64> {
    let provider = provider(rpc)?;
    let rt = tokio::runtime::Runtime::new()?;
    let block = rt.block_on(async { provider.get_block_number().await })?;
    Ok(block)
}

fn run_forge(rpc: &str, block: u64, match_contract: &str) -> anyhow::Result<std::process::Output> {
    Command::new("forge")
        .arg("test")
        .arg("--match-contract")
        .arg(match_contract)
        .arg("--fork-url")
        .arg(rpc)
        .arg("--fork-block-number")
        .arg(block.to_string())
        .arg("--root")
        .arg("/home/x/arb/sol")
        .current_dir("/home/x/arb/sol")
        .output()
        .map_err(Into::into)
}

#[test]
#[ignore = "manual fork replay gate"]
fn pinned_block_replay_harness_runs_foundry_executor_surfaces() -> anyhow::Result<()> {
    let rpc = polygon_rpc_url();
    let block = latest_block_number(&rpc)?;

    let aave = run_forge(&rpc, block, "ArbExecutorAaveForkTest")?;
    if !aave.status.success() {
        anyhow::bail!(
            "ArbExecutorAaveForkTest failed at block {block}:\n{}\n{}",
            String::from_utf8_lossy(&aave.stdout),
            String::from_utf8_lossy(&aave.stderr)
        );
    }

    let atomic = run_forge(&rpc, block, "ArbExecutorAtomicTest")?;
    if !atomic.status.success() {
        anyhow::bail!(
            "ArbExecutorAtomicTest failed at block {block}:\n{}\n{}",
            String::from_utf8_lossy(&atomic.stdout),
            String::from_utf8_lossy(&atomic.stderr)
        );
    }

    Ok(())
}
