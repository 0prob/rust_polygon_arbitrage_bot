use alloy::primitives::{Address, U256};

use crate::abis::{ExecutorCall, IArbExecutor};
use alloy::sol_types::SolCall;

#[allow(dead_code)]
pub(crate) fn encode_approve_if_needed(
    executor: Address,
    token: Address,
    spender: Address,
    amount: U256,
) -> ExecutorCall {
    let call = IArbExecutor::approveIfNeededCall {
        token,
        spender,
        amount,
    };
    ExecutorCall {
        target: executor,
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

        assert_eq!(call.target, executor, "Approval target should be executor");
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
