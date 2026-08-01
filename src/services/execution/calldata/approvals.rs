use alloy::primitives::{Address, Bytes, U256};
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, IArbExecutor, IERC20};

fn exec_call(target: Address, data: Vec<u8>) -> ExecutorCall {
    ExecutorCall {
        target,
        value: U256::ZERO,
        data: Bytes::from(data),
    }
}

/// Approve `spender` to pull `amount` of `token` from the executor during route execution.
///
/// Uses a direct ERC-20 `approve` on the token contract (executor is `msg.sender` inside
/// `EXECUTE_CALLS`). Avoids Huff `approveIfNeeded`, which reverts on some Polygon tokens
/// despite direct `approve` succeeding.
pub(crate) fn encode_approve_if_needed(
    token: Address,
    spender: Address,
    amount: U256,
) -> ExecutorCall {
    exec_call(token, IERC20::approveCall { spender, amount }.abi_encode())
}

/// Prefund a pool with `token`.
///
/// - Exact path: ERC-20 `transfer(pool, amount)` from the executor (msg.sender).
/// - `transferAll` path: call **`executor.transferAll(token, pool)`** — `executor` must
///   be the ArbExecutor address, never the swap output recipient / next pool.
pub(crate) fn encode_token_transfer(
    executor: Address,
    token: Address,
    pool: Address,
    amount: U256,
    use_transfer_all: bool,
) -> ExecutorCall {
    if use_transfer_all {
        encode_transfer_all(executor, token, pool)
    } else {
        exec_call(
            token,
            IERC20::transferCall { to: pool, amount }.abi_encode(),
        )
    }
}

pub(crate) fn encode_transfer_all(executor: Address, token: Address, to: Address) -> ExecutorCall {
    exec_call(
        executor,
        IArbExecutor::transferAllCall { token, to }.abi_encode(),
    )
}
