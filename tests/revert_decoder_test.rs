use alloy::hex;
use alloy::primitives::{Address, U256};
use rpbot::services::execution::revert_decoder::{DecodedRevert, decode_revert};

fn sel(hex: &str) -> [u8; 4] {
    let bytes = hex::decode(hex).expect("valid selector hex");
    bytes.try_into().expect("selector is four bytes")
}

type RevertCase = ([u8; 4], fn(DecodedRevert) -> bool);

#[test]
fn decodes_all_executor_custom_errors() {
    let cases: &[RevertCase] = &[
        (sel("82b42900"), |r| {
            matches!(r, DecodedRevert::Unauthorized)
        }),
        (sel("1ab7da6b"), |r| {
            matches!(r, DecodedRevert::DeadlineExpired)
        }),
        (sel("ea60ab1d"), |r| matches!(r, DecodedRevert::EmptyRoute)),
        (sel("f5dedbff"), |r| {
            matches!(r, DecodedRevert::TooManyCalls)
        }),
        (sel("946302fe"), |r| {
            matches!(r, DecodedRevert::FlashLoanRequired)
        }),
        (sel("c858adff"), |r| {
            matches!(r, DecodedRevert::InvalidRouteHash)
        }),
        (sel("fc305329"), |r| {
            matches!(r, DecodedRevert::FlashLoanOnly)
        }),
        (sel("add4adc0"), |r| {
            matches!(r, DecodedRevert::InvalidFlashLoanContext)
        }),
        (sel("c21d53e8"), |r| {
            matches!(r, DecodedRevert::CallbackOnly)
        }),
        (sel("936198e9"), |r| {
            matches!(r, DecodedRevert::InvalidCallbackSource)
        }),
        (sel("d92e233d"), |r| matches!(r, DecodedRevert::ZeroAddress)),
        (sel("6be92da6"), |r| {
            matches!(r, DecodedRevert::BalancerVaultReentrancy)
        }),
    ];
    for (selector, pred) in cases {
        let mut data = selector.to_vec();
        data.extend_from_slice(&[0u8; 32]);
        let decoded = decode_revert(&data).expect("known selector should decode");
        assert!(pred(decoded), "selector 0x{selector:02x?}");
    }
}

#[test]
fn decodes_parameterized_errors() {
    let token = Address::from([0x11; 20]);
    let spender = Address::from([0x22; 20]);
    let mut approve = sel("1b6c83ab").to_vec();
    approve.extend_from_slice(&[0u8; 12]);
    approve.extend_from_slice(token.as_slice());
    approve.extend_from_slice(&[0u8; 12]);
    approve.extend_from_slice(spender.as_slice());
    let decoded = decode_revert(&approve).expect("ApproveFailed should decode");
    assert!(matches!(
        decoded,
        DecodedRevert::ApproveFailed { token: t, spender: s } if t == token && s == spender
    ));

    let mut profit = sel("4e88422a").to_vec();
    profit.extend_from_slice(&U256::from(90u64).to_be_bytes::<32>());
    profit.extend_from_slice(&U256::from(100u64).to_be_bytes::<32>());
    let decoded = decode_revert(&profit).expect("InsufficientProfit should decode");
    assert!(matches!(
        decoded,
        DecodedRevert::InsufficientProfit { final_balance, required }
            if final_balance == U256::from(90u64) && required == U256::from(100u64)
    ));
}

#[test]
fn decodes_external_call_failed_from_live_revert() {
    let data = hex::decode(
        "0f4345730000000000000000000000000000000000000000000000000000000000000040\
         0000000000000000000000000000000000000000000000000000000000000001\
         0000000000000000000000000000000000000000000000000000000000000060\
         0000000000000000000000000000000000000000000000000000000000000000",
    )
    .expect("hex");
    let decoded = decode_revert(&data).expect("ExternalCallFailed should decode");
    // ABI-encoded data has first word as offset (0x40=64) pointing past the
    // available 128-byte payload — the ABI decoder returns None and the Huff
    // fallback rejects the index (64 >= MAX_ROUTE_CALLS=12). Falls to catch-all.
    assert!(matches!(
        decoded,
        DecodedRevert::ExternalCallFailed { index: 0, .. }
    ));
}

#[test]
fn rejects_abi_offset_misread_as_hop_index() {
    let mut data = vec![0x0f, 0x43, 0x45, 0x73];
    data.extend_from_slice(&U256::from(0x60u64).to_be_bytes::<32>());
    data.extend_from_slice(&U256::from(0x600000000000u64).to_be_bytes::<32>());
    data.extend_from_slice(&U256::from(0x600000000000u64).to_be_bytes::<32>());
    data.extend_from_slice(&[0u8; 32]);
    let decoded = decode_revert(&data).expect("should decode fallback");
    assert!(matches!(
        decoded,
        DecodedRevert::ExternalCallFailed {
            index: 0,
            target,
            ..
        } if target == Address::ZERO
    ));
}

#[test]
fn decodes_huff_external_call_failed_layout() {
    let target = Address::from([0xab; 20]);
    let mut data = vec![0x0f, 0x43, 0x45, 0x73];
    data.extend_from_slice(&U256::from(4u64).to_be_bytes::<32>());
    data.extend_from_slice(&[0u8; 12]);
    data.extend_from_slice(target.as_slice());
    data.extend_from_slice(&U256::from(0x60u64).to_be_bytes::<32>());
    data.extend_from_slice(&U256::ZERO.to_be_bytes::<32>());
    let decoded = decode_revert(&data).expect("huff ExternalCallFailed should decode");
    assert!(matches!(
        decoded,
        DecodedRevert::ExternalCallFailed {
            index: 4,
            target: t,
            ..
        } if t == target
    ));
}
