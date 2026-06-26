use alloy::primitives::{Address, U256};
use alloy::sol_types::SolCall;

use crate::abis::{ExecutorCall, IArbExecutor, IERC20};

/// Approve `spender` to pull `amount` of `token` from the executor during route execution.
///
/// Uses a direct ERC-20 `approve` on the token contract (executor is `msg.sender` inside
/// `EXECUTE_CALLS`). Avoids Huff `approveIfNeeded`, which reverts on some Polygon tokens
/// despite direct `approve` succeeding.
pub(crate) fn encode_approve_if_needed(
    _executor: Address,
    token: Address,
    spender: Address,
    amount: U256,
) -> ExecutorCall {
    let call = IERC20::approveCall { spender, amount };
    ExecutorCall {
        target: token,
        value: U256::ZERO,
        data: call.abi_encode().into(),
    }
}

pub(crate) fn encode_transfer_all(executor: Address, token: Address, to: Address) -> ExecutorCall {
    let call = IArbExecutor::transferAllCall { token, to };
    ExecutorCall {
        target: executor,
        value: U256::ZERO,
        data: call.abi_encode().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_approve_if_needed_encodes_correctly() {
        let executor = Address::repeat_byte(0x01);
        let token = Address::repeat_byte(0x02);
        let spender = Address::repeat_byte(0x03);
        let amount = U256::from(1000);

        let call = encode_approve_if_needed(executor, token, spender, amount);

        assert_eq!(call.target, token, "Approval target should be token contract");
        assert_eq!(call.value, U256::ZERO, "Approval should have no ETH value");
        assert!(!call.data.is_empty(), "Approval data should not be empty");
    }

    #[test]
    fn test_encode_approve_if_needed_different_amounts() {
        let executor = Address::repeat_byte(0x01);
        let token = Address::repeat_byte(0x02);
        let spender = Address::repeat_byte(0x03);

        let call1 = encode_approve_if_needed(executor, token, spender, U256::from(1000));
        let call2 = encode_approve_if_needed(executor, token, spender, U256::from(2000));

        assert_ne!(
            call1.data, call2.data,
            "Different amounts should produce different encoded data"
        );
    }

    #[test]
    fn test_encode_transfer_all_encodes_correctly() {
        let executor = Address::repeat_byte(0x01);
        let token = Address::repeat_byte(0x02);
        let to = Address::repeat_byte(0x03);

        let call = encode_transfer_all(executor, token, to);

        assert_eq!(call.target, executor, "Transfer target should be executor");
        assert_eq!(call.value, U256::ZERO, "Transfer should have no ETH value");
        assert!(!call.data.is_empty(), "Transfer data should not be empty");
    }
}
