//! Golden calldata parity tests against the TypeScript bot fixtures
//! (`/home/x/arb/t/src/services/execution/calldata/index.test.ts`).
//! Regenerate goldens: `bun -e '...'` in the TS repo (see module docs in calldata.rs).

use std::str::FromStr;

use alloy::hex;
use alloy::primitives::{Address, Bytes, FixedBytes, U256};
use rpbot::abis::ExecutorCall;
use rpbot::core::types::{Edge, PoolState, ProtocolType, V3PoolState, V4PoolState};
use rpbot::pipeline::arena::StateArena;
use rpbot::pipeline::types::PoolMeta;
use rpbot::services::execution::calldata::{
    RouteEncodeConfig, build_arb_calldata, build_calldata_hops, compute_route_hash, encode_route,
};

const SLIPPAGE_BPS: u64 = 50;
const DEADLINE: u64 = 1_700_000_000;

fn addr(hex: &str) -> Address {
    hex.parse().expect("address")
}

fn fixture_reserve() -> U256 {
    U256::from(1000u128) * U256::from(10u128).pow(U256::from(18))
}

fn register_v2_pool(arena: &mut StateArena, pool: Address) -> rpbot::core::types::PoolIndex {
    arena.register_pool(
        pool,
        rpbot::core::types::PoolState::V2(rpbot::core::types::V2PoolState {
            reserve0: fixture_reserve(),
            reserve1: fixture_reserve() * U256::from(2u8),
            fee: U256::from(30u32),
            fee_denominator: U256::from(1000u32),
        }),
    )
}

fn test_encode_cfg() -> RouteEncodeConfig {
    RouteEncodeConfig {
        slippage_bps: SLIPPAGE_BPS,
        deadline: U256::from(DEADLINE),
    }
}

#[test]
fn route_hash_matches_ts_simple_fixture() {
    let calls = vec![
        ExecutorCall {
            target: addr("0x0000000000000000000000000000000000000a01"),
            value: U256::ZERO,
            data: Bytes::from(vec![0x12, 0x34]),
        },
        ExecutorCall {
            target: addr("0x0000000000000000000000000000000000000b01"),
            value: U256::ZERO,
            data: Bytes::from(vec![0xab, 0xcd]),
        },
    ];
    let hash = compute_route_hash(&calls);
    assert_eq!(
        hash,
        FixedBytes::from_str("0x9a2901980faa45a2623f968a47d5faa76292977c38696882681e824f9e965ae7")
            .unwrap()
    );
}

#[test]
fn multi_hop_v2_matches_ts_fixture() {
    let executor = addr("0x0000000000000000000000000000000000000001");
    let pool_a = addr("0x0000000000000000000000000000000000000a01");
    let pool_b = addr("0x0000000000000000000000000000000000000b01");
    let token_a = addr("0x000000000000000000000000000000000000aaaa");
    let token_b = addr("0x000000000000000000000000000000000000bbbb");
    let token_c = addr("0x000000000000000000000000000000000000cccc");

    let mut arena = StateArena::new();
    let t0 = arena.register_token(token_a);
    let t1 = arena.register_token(token_b);
    let t2 = arena.register_token(token_c);
    let p0 = register_v2_pool(&mut arena, pool_a);
    let p1 = register_v2_pool(&mut arena, pool_b);

    let edges = [
        Edge {
            pool_index: p0,
            token_in: t0,
            token_out: t1,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        },
        Edge {
            pool_index: p1,
            token_in: t1,
            token_out: t2,
            token_in_idx: 0,
            token_out_idx: 1,
            protocol: ProtocolType::UniswapV2,
            fee_bps: 30,
            zero_for_one: true,
        },
    ];

    let amount_in = U256::from(10u128).pow(U256::from(18));
    let mid = U256::from(9u128) * U256::from(10u128).pow(U256::from(17));
    let amount_out = U256::from(8u128) * U256::from(10u128).pow(U256::from(17));

    let hops =
        build_calldata_hops(&arena, &edges, &[amount_in, mid, amount_out], &[]).expect("hops");
    let calls = encode_route(
        &arena,
        &hops,
        executor,
        RouteEncodeConfig {
            slippage_bps: SLIPPAGE_BPS,
            deadline: U256::from(DEADLINE),
        },
    )
    .expect("encode");

    assert_eq!(calls.len(), 4);
    assert_eq!(
        compute_route_hash(&calls),
        FixedBytes::from_str("0xa4f4e5469baf454115007ea3daa1f3976426699f87d76553c340d220014134fe")
            .unwrap()
    );

    // Hop 2+ uses executor transferAll (selector 0x4b14e003).
    assert_eq!(calls[2].target, executor);
    assert_eq!(&calls[2].data[..4], &[0x4b, 0x14, 0xe0, 0x03]);

    let golden = [
        "0xa9059cbb0000000000000000000000000000000000000000000000000000000000000a010000000000000000000000000000000000000000000000000de0b6b3a7640000",
        "0x022c0d9f00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001b81ab837e65fca7000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000000",
        "0x4b14e003000000000000000000000000000000000000000000000000000000000000bbbb0000000000000000000000000000000000000000000000000000000000000b01",
        "0x022c0d9f00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001863256d67f33308000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000000",
    ];
    for (call, hex) in calls.iter().zip(golden) {
        assert_eq!(format!("0x{}", hex::encode(&call.data)), hex);
    }
}

#[test]
fn execute_arb_wrapper_matches_ts_fixture() {
    let executor = addr("0x0000000000000000000000000000000000000001");
    let token_a = addr("0x000000000000000000000000000000000000aaaa");
    let pool_a = addr("0x0000000000000000000000000000000000000a01");

    let calls = vec![ExecutorCall {
        target: pool_a,
        value: U256::ZERO,
        data: Bytes::from(vec![0x12, 0x34]),
    }];

    let built = build_arb_calldata(
        executor,
        token_a,
        token_a,
        U256::from(10u128).pow(U256::from(18)),
        U256::from(1u64),
        U256::from(DEADLINE),
        calls.clone(),
        false,
    );

    assert_eq!(
        built.route_hash,
        compute_route_hash(&calls),
        "route hash must match embedded params"
    );
    assert_eq!(
        built.route_hash,
        FixedBytes::from_str("0x8ca8c8cf55d982c486e88ae81497c71982377540eecd087a7a7d42cc31e1c58e")
            .unwrap()
    );
    assert_eq!(&built.data[..4], &[0x49, 0x1e, 0x69, 0xd3]);
    assert_eq!(built.to, executor);
}

#[test]
fn v4_single_hop_matches_ts_fixture() {
    let executor = addr("0x0000000000000000000000000000000000000001");
    let pool = addr("0x0000000000000000000000000000000000000a01");
    let token_a = addr("0x000000000000000000000000000000000000aaaa");
    let token_b = addr("0x000000000000000000000000000000000000bbbb");

    let mut arena = StateArena::new();
    let t0 = arena.register_token(token_a);
    let t1 = arena.register_token(token_b);
    let sqrt = U256::from(1u128) << 96;
    let p = arena.register_pool(
        pool,
        PoolState::V4(V4PoolState {
            sqrt_price_x96: sqrt,
            tick: 0,
            liquidity: 1_000_000_000_000,
            fee: U256::from(3000u32),
            tick_spacing: 60,
            ticks: Box::new([]),
        }),
    );

    let edge = Edge {
        pool_index: p,
        token_in: t0,
        token_out: t1,
        token_in_idx: 0,
        token_out_idx: 1,
        protocol: ProtocolType::UniswapV4,
        fee_bps: 30,
        zero_for_one: true,
    };
    let amount_in = U256::from(10u128).pow(U256::from(18));
    let hops = build_calldata_hops(
        &arena,
        &[edge],
        &[amount_in, amount_in / U256::from(2u8)],
        &[],
    )
    .expect("hops");
    let calls = encode_route(&arena, &hops, executor, test_encode_cfg()).expect("encode");

    assert_eq!(calls.len(), 2);
    assert_eq!(
        compute_route_hash(&calls),
        FixedBytes::from_str("0x73a19c09f7ea5ea710d373e0b2dd249b878d2a9a6773c3837ee7e25874babeaf")
            .unwrap()
    );

    let golden = [
        "0xfad6b994000000000000000000000000000000000000000000000000000000000000aaaa00000000000000000000000067366782805870060151383f4bbff9dab53e5cd60000000000000000000000000000000000000000000000000de0b6b3a7640000",
        "0x8154831900000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000aaaa000000000000000000000000000000000000000000000000000000000000bbbb0000000000000000000000000000000000000000000000000000000000000bb8000000000000000000000000000000000000000000000000000000000000003c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001fffffffffffffffffffffffffffffffffffffffffffffffff21f494c589c000000000000000000000000000000000000000000000000000000000001000276a4",
    ];
    for (call, hex) in calls.iter().zip(golden) {
        assert_eq!(
            format!("0x{}", hex::encode(&call.data)).to_ascii_lowercase(),
            hex
        );
    }
}

#[test]
fn kyber_single_hop_matches_ts_fixture() {
    let executor = addr("0x0000000000000000000000000000000000000001");
    let pool = addr("0x0000000000000000000000000000000000000a01");
    let token_a = addr("0x000000000000000000000000000000000000aaaa");
    let token_b = addr("0x000000000000000000000000000000000000bbbb");

    let mut arena = StateArena::new();
    let t0 = arena.register_token(token_a);
    let t1 = arena.register_token(token_b);
    let sqrt = U256::from(1u128) << 96;
    let p = arena.register_pool(
        pool,
        PoolState::V3(V3PoolState {
            sqrt_price_x96: sqrt,
            tick: 0,
            liquidity: 10u128.pow(20),
            fee: U256::from(300u32),
            tick_spacing: 60,
            ticks: Box::new([]),
        }),
    );

    let edge = Edge {
        pool_index: p,
        token_in: t0,
        token_out: t1,
        token_in_idx: 0,
        token_out_idx: 1,
        protocol: ProtocolType::UniswapV3,
        fee_bps: 30,
        zero_for_one: true,
    };
    let metas = [PoolMeta {
        pool_index: p,
        protocol: ProtocolType::UniswapV3,
        tokens: vec![t0, t1],
        fee_bps: 30,
        token0: t0,
        token1: t1,
        bpt_index: None,
        pool_id: None,
        protocol_label: Some("KYBERSWAP_ELASTIC".into()),
        router: None,
        hooks: None,
        tick_spacing: None,
    }];
    let amount_in = U256::from(10u128).pow(U256::from(18));
    let hops = build_calldata_hops(
        &arena,
        &[edge],
        &[amount_in, amount_in / U256::from(2u8)],
        &metas,
    )
    .expect("hops");
    let calls = encode_route(&arena, &hops, executor, test_encode_cfg()).expect("encode");

    assert_eq!(calls.len(), 1);
    assert_eq!(
        compute_route_hash(&calls),
        FixedBytes::from_str("0x8b62e55d56892faa0176d4ed40be8a84151153266a7ff1ec88e6ab3d0250b5c4")
            .unwrap()
    );

    let golden = "0x24b31a0c0000000000000000000000000000000000000000000000000000000000000001fffffffffffffffffffffffffffffffffffffffffffffffff21f494c589c000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000fdd51ef22020b910eb2ec30b00000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000aaaa000000000000000000000000000000000000000000000000000000000000bbbb0000000000000000000000000000000000000000000000000000000000000bb8";
    assert_eq!(
        format!("0x{}", hex::encode(&calls[0].data)).to_ascii_lowercase(),
        golden
    );
}
