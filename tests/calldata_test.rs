use alloy::primitives::{Address, U256};
use alloy::sol_types::SolCall;

use rpbot::abis::{ExecutorCall, IArbExecutor};
use rpbot::core::types::FlashLoanSource;
use rpbot::services::execution::calldata::{
    ExecutorEntrypoint, build_arb_calldata, build_packed_route_payload, compute_route_hash,
    pack_executor_calls,
};

#[test]
fn packed_route_matches_executor_envelope() {
    let flash_token = "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"
        .parse::<Address>()
        .expect("WMATIC address should parse");
    let flash_amount = U256::from(50500000000000000000u128);
    let min_profit = U256::from(100000000000000000u128);
    let deadline = U256::from(9999999999u64);

    let calls = vec![ExecutorCall {
        target: flash_token,
        value: U256::ZERO,
        data: "0x70a082310000000000000000000000000000000000000000000000000000000000000001"
            .parse()
            .expect("test calldata should parse"),
    }];

    let packed_calls = pack_executor_calls(&calls).expect("test calls should pack");
    let route_hash = compute_route_hash(&packed_calls);
    let (route_payload, route_hash_from_builder) = build_packed_route_payload(
        flash_token,
        flash_amount,
        flash_token,
        min_profit,
        deadline,
        &calls,
    )
    .expect("test route payload should build");

    assert_eq!(route_hash, route_hash_from_builder);

    let aave_calldata = IArbExecutor::executeArbWithAaveCall {
        packedRoute: route_payload.clone(),
    }
    .abi_encode();
    assert!(!route_payload.is_empty());
    assert!(!aave_calldata.is_empty());
}

#[test]
fn executor_entrypoints_map_to_expected_selectors() {
    let executor = Address::from([0x11; 20]);
    let token = Address::from([0x22; 20]);
    let calls = vec![ExecutorCall {
        target: token,
        value: U256::ZERO,
        data: vec![0x01].into(),
    }];

    for (source, entrypoint, expected_selector) in [
        (
            FlashLoanSource::Balancer,
            ExecutorEntrypoint::BalancerFlash,
            IArbExecutor::executeArbCall::SELECTOR,
        ),
        (
            FlashLoanSource::AaveV3,
            ExecutorEntrypoint::AaveFlash,
            IArbExecutor::executeArbWithAaveCall::SELECTOR,
        ),
        (
            FlashLoanSource::Direct,
            ExecutorEntrypoint::Direct,
            IArbExecutor::executeArbDirectCall::SELECTOR,
        ),
    ] {
        let built = build_arb_calldata(
            executor,
            token,
            token,
            U256::from(1_000u64),
            U256::from(1u64),
            U256::from(9_999_999_999u64),
            calls.clone(),
            entrypoint,
        )
        .expect("calldata should build");
        assert_eq!(&built.data[..4], expected_selector);
    }
}
