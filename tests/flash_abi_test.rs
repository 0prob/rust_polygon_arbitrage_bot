//! Flash-loan ABI selector alignment: Rust bindings vs ArbExecutor.sol / official protocol interfaces.
use alloy::sol_types::SolCall;

use rpbot::abis::{
    IAaveV3Pool, IArbExecutor, IBalancerVault, IFlashLoanRecipient, IFlashLoanSimpleReceiver,
};

const EXECUTE_ARB: [u8; 4] = [0x5f, 0xa6, 0x51, 0x9d];
const EXECUTE_ARB_DIRECT: [u8; 4] = [0x36, 0x87, 0x64, 0xc8];
const EXECUTE_ARB_WITH_AAVE: [u8; 4] = [0x54, 0x02, 0x85, 0xec];
const EXECUTE_ARB_WITH_DODO: [u8; 4] = [0x73, 0x5e, 0xc4, 0xd7];
const AAVE_FLASH_LOAN_SIMPLE: [u8; 4] = [0x42, 0xb0, 0xb7, 0x7c];
const BALANCER_FLASH_LOAN: [u8; 4] = [0x5c, 0x38, 0x44, 0x9e];
const BALANCER_QUERY_BATCH_SWAP: [u8; 4] = [0xf8, 0x4d, 0x06, 0x6e];
const AAVE_EXECUTE_OPERATION: [u8; 4] = [0x1b, 0x11, 0xd0, 0xff];
const BALANCER_RECEIVE_FLASH_LOAN: [u8; 4] = [0xf0, 0x4f, 0x27, 0x07];

#[test]
fn arb_executor_entrypoint_selectors_match_huff() {
    assert_eq!(IArbExecutor::executeArbCall::SELECTOR, EXECUTE_ARB);
    assert_eq!(
        IArbExecutor::executeArbDirectCall::SELECTOR,
        EXECUTE_ARB_DIRECT
    );
    assert_eq!(
        IArbExecutor::executeArbWithAaveCall::SELECTOR,
        EXECUTE_ARB_WITH_AAVE
    );
    assert_eq!(
        IArbExecutor::executeArbWithDodoCall::SELECTOR,
        EXECUTE_ARB_WITH_DODO
    );
}

#[test]
fn protocol_flash_loan_selectors_match_official_interfaces() {
    assert_eq!(
        IAaveV3Pool::flashLoanSimpleCall::SELECTOR,
        AAVE_FLASH_LOAN_SIMPLE
    );
    assert_eq!(IBalancerVault::flashLoanCall::SELECTOR, BALANCER_FLASH_LOAN);
    assert_eq!(
        IBalancerVault::queryBatchSwapCall::SELECTOR,
        BALANCER_QUERY_BATCH_SWAP
    );
}

#[test]
fn flash_callback_selectors_match_executor_interfaces() {
    assert_eq!(
        IFlashLoanSimpleReceiver::executeOperationCall::SELECTOR,
        AAVE_EXECUTE_OPERATION
    );
    assert_eq!(
        IFlashLoanRecipient::receiveFlashLoanCall::SELECTOR,
        BALANCER_RECEIVE_FLASH_LOAN
    );
}
