//! Decode custom error reversions from the ArbExecutor contract.
//! Selectors from ArbExecutor.huff — see `../sol/src/ArbExecutor.huff`.

use std::fmt::Write;

use alloy::primitives::{Address, U256};

/// All custom error selectors from the ArbExecutor contract.
const SEL_UNAUTHORIZED: [u8; 4] = [0x82, 0xb4, 0x29, 0x00];
const SEL_DEADLINE_EXPIRED: [u8; 4] = [0x1a, 0xb7, 0xda, 0x6b];
const SEL_EMPTY_ROUTE: [u8; 4] = [0xea, 0x60, 0xab, 0x1d];
const SEL_TOO_MANY_CALLS: [u8; 4] = [0xf5, 0xde, 0xdb, 0xff];
const SEL_FLASH_LOAN_REQUIRED: [u8; 4] = [0x94, 0x63, 0x02, 0xfe];
const SEL_INVALID_ROUTE_HASH: [u8; 4] = [0xc8, 0x58, 0xad, 0xff];
const SEL_FLASH_LOAN_ONLY: [u8; 4] = [0xfc, 0x30, 0x53, 0x29];
const SEL_INVALID_FLASH_LOAN_CTX: [u8; 4] = [0xad, 0xd4, 0xad, 0xc0];
const SEL_CALLBACK_ONLY: [u8; 4] = [0xc2, 0x1d, 0x53, 0xe8];
const SEL_UNSUPPORTED_PROTOCOL: [u8; 4] = [0xf8, 0x50, 0x44, 0x2b];
const SEL_INVALID_POOL_CALLER: [u8; 4] = [0xf2, 0x06, 0x25, 0x59];
const SEL_EXTERNAL_CALL_FAILED: [u8; 4] = [0x0f, 0x43, 0x45, 0x73];
const SEL_INSUFFICIENT_PROFIT: [u8; 4] = [0x4e, 0x88, 0x42, 0x2a];
const SEL_TRANSFER_FAILED: [u8; 4] = [0xbf, 0x18, 0x2b, 0xe8];
const SEL_APPROVE_FAILED: [u8; 4] = [0x1b, 0x6c, 0x83, 0xab];
const SEL_ZERO_ADDRESS: [u8; 4] = [0xd9, 0x2e, 0x23, 0x3d];
const SEL_INVALID_CALLBACK_SOURCE: [u8; 4] = [0x93, 0x61, 0x98, 0xe9];
const SEL_BALANCER_VAULT_REENTRANCY: [u8; 4] = [0x6b, 0xe9, 0x2d, 0xa6];

#[derive(Debug, Clone)]
pub enum DecodedRevert {
    Unauthorized,
    DeadlineExpired,
    EmptyRoute,
    TooManyCalls,
    FlashLoanRequired,
    InvalidRouteHash,
    FlashLoanOnly,
    InvalidFlashLoanContext,
    CallbackOnly,
    UnsupportedProtocol(u8),
    InvalidPoolCaller {
        expected: Address,
        actual: Address,
    },
    ExternalCallFailed {
        index: u64,
        target: Address,
        reason: String,
    },
    InsufficientProfit {
        final_balance: U256,
        required: U256,
    },
    TransferFailed {
        token: Address,
        to: Address,
        amount: U256,
    },
    ApproveFailed {
        token: Address,
        spender: Address,
    },
    ZeroAddress,
    InvalidCallbackSource,
    BalancerVaultReentrancy,
    Unknown([u8; 4], String),
}

#[must_use]
pub fn decode_revert(data: &[u8]) -> Option<DecodedRevert> {
    if data.len() < 4 {
        return None;
    }
    let sel: [u8; 4] = data[0..4]
        .try_into()
        .expect("revert selector length was checked above");
    let payload = &data[4..];

    match sel {
        SEL_UNAUTHORIZED => Some(DecodedRevert::Unauthorized),
        SEL_DEADLINE_EXPIRED => Some(DecodedRevert::DeadlineExpired),
        SEL_EMPTY_ROUTE => Some(DecodedRevert::EmptyRoute),
        SEL_TOO_MANY_CALLS => Some(DecodedRevert::TooManyCalls),
        SEL_FLASH_LOAN_REQUIRED => Some(DecodedRevert::FlashLoanRequired),
        SEL_INVALID_ROUTE_HASH => Some(DecodedRevert::InvalidRouteHash),
        SEL_FLASH_LOAN_ONLY => Some(DecodedRevert::FlashLoanOnly),
        SEL_INVALID_FLASH_LOAN_CTX => Some(DecodedRevert::InvalidFlashLoanContext),
        SEL_CALLBACK_ONLY => Some(DecodedRevert::CallbackOnly),
        SEL_ZERO_ADDRESS => Some(DecodedRevert::ZeroAddress),
        SEL_INVALID_CALLBACK_SOURCE => Some(DecodedRevert::InvalidCallbackSource),
        SEL_BALANCER_VAULT_REENTRANCY => Some(DecodedRevert::BalancerVaultReentrancy),
        SEL_UNSUPPORTED_PROTOCOL => {
            if payload.len() >= 32 {
                let id = U256::from_be_slice(&payload[0..32]).try_into().unwrap_or(0);
                Some(DecodedRevert::UnsupportedProtocol(id))
            } else {
                Some(DecodedRevert::UnsupportedProtocol(0))
            }
        }
        SEL_INVALID_POOL_CALLER => decode_invalid_pool_caller(payload),
        SEL_EXTERNAL_CALL_FAILED => decode_external_call_failed(payload),
        SEL_INSUFFICIENT_PROFIT => decode_insufficient_profit(payload),
        SEL_TRANSFER_FAILED => decode_transfer_failed(payload),
        SEL_APPROVE_FAILED => decode_approve_failed(payload),
        _ => {
            let hex = data
                .iter()
                .fold(String::with_capacity(data.len() * 2), |mut s, b| {
                    let _ = write!(s, "{b:02x}");
                    s
                });
            Some(DecodedRevert::Unknown(sel, hex))
        }
    }
}

fn decode_invalid_pool_caller(data: &[u8]) -> Option<DecodedRevert> {
    if data.len() < 64 {
        return Some(DecodedRevert::InvalidPoolCaller {
            expected: Address::ZERO,
            actual: Address::ZERO,
        });
    }
    let expected = Address::from_slice(&data[12..32]);
    let actual = Address::from_slice(&data[44..64]);
    Some(DecodedRevert::InvalidPoolCaller { expected, actual })
}

fn word_u256(data: &[u8], word: usize) -> Option<U256> {
    let start = word.checked_mul(32)?;
    let end = start.checked_add(32)?;
    Some(U256::from_be_slice(data.get(start..end)?))
}

fn decode_abi_string(data: &[u8], offset: usize) -> String {
    if offset.saturating_add(32) > data.len() {
        return String::new();
    }
    let len = word_u256(data, offset / 32)
        .and_then(|v| usize::try_from(v).ok())
        .unwrap_or(0);
    let body_start = offset.saturating_add(32);
    let end = body_start.saturating_add(len);
    if end <= data.len() {
        String::from_utf8_lossy(&data[body_start..end]).to_string()
    } else {
        String::new()
    }
}

fn decode_external_call_failed_abi(payload: &[u8]) -> Option<DecodedRevert> {
    if payload.len() < 96 {
        return None;
    }
    let head = word_u256(payload, 0)?;
    let off = usize::try_from(head).ok()?;
    if off + 96 > payload.len() {
        return None;
    }
    let index = u64::try_from(U256::from_be_slice(&payload[off..off + 32])).unwrap_or(0);
    let target = Address::from_slice(&payload[off + 32 + 12..off + 64]);
    let reason_rel = usize::try_from(U256::from_be_slice(&payload[off + 64..off + 96])).ok()?;
    let reason = decode_abi_string(payload, off.saturating_add(reason_rel));
    sanitize_external_call_failed(index, target, reason)
}

fn sanitize_external_call_failed(
    index: u64,
    target: Address,
    reason: String,
) -> Option<DecodedRevert> {
    if index >= crate::pipeline::route_calls::MAX_ROUTE_CALLS as u64 {
        return None;
    }
    let target = if crate::services::discovery::is_plausible_contract_address(target) {
        target
    } else {
        Address::ZERO
    };
    Some(DecodedRevert::ExternalCallFailed {
        index,
        target,
        reason,
    })
}

fn decode_huff_external_call_failed_payload(payload: &[u8]) -> Option<DecodedRevert> {
    // ArbExecutor.huff execute_call_failed payload (selector stripped): index@0, target@32.
    if payload.len() < 64 {
        return None;
    }
    let index = u64::try_from(U256::from_be_slice(&payload[0..32])).unwrap_or(0);
    let target = Address::from_slice(&payload[44..64]);
    let mut reason = String::new();
    if payload.len() >= 128 {
        let rd_size = usize::try_from(U256::from_be_slice(&payload[96..128])).unwrap_or(0);
        if rd_size > 0 && payload.len() >= 128usize.saturating_add(rd_size) {
            let rd = &payload[128..128 + rd_size];
            reason = decode_revert(rd)
                .map(|r| r.to_string())
                .unwrap_or_else(|| format!("0x{}", hex_preview(rd, 64)));
        }
    }
    sanitize_external_call_failed(index, target, reason)
}

fn hex_preview(data: &[u8], max: usize) -> String {
    data.iter()
        .take(max)
        .fold(String::with_capacity(max * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

fn decode_external_call_failed(payload: &[u8]) -> Option<DecodedRevert> {
    if payload.is_empty() {
        return Some(DecodedRevert::ExternalCallFailed {
            index: 0,
            target: Address::ZERO,
            reason: String::new(),
        });
    }
    if let Some(decoded) = decode_external_call_failed_abi(payload) {
        return Some(decoded);
    }
    if let Some(decoded) = decode_huff_external_call_failed_payload(payload) {
        return Some(decoded);
    }
    let reason = if payload.len() > 128 {
        format!("undecodable payload ({} bytes)", payload.len())
    } else {
        format!("0x{}", hex_preview(payload, payload.len()))
    };
    Some(DecodedRevert::ExternalCallFailed {
        index: 0,
        target: Address::ZERO,
        reason,
    })
}

fn decode_insufficient_profit(data: &[u8]) -> Option<DecodedRevert> {
    if data.len() < 64 {
        return None;
    }
    let final_balance = U256::from_be_slice(&data[0..32]);
    let required = U256::from_be_slice(&data[32..64]);
    Some(DecodedRevert::InsufficientProfit {
        final_balance,
        required,
    })
}

fn decode_transfer_failed(data: &[u8]) -> Option<DecodedRevert> {
    if data.len() < 96 {
        return None;
    }
    let token = Address::from_slice(&data[12..32]);
    let to = Address::from_slice(&data[44..64]);
    let amount = U256::from_be_slice(&data[64..96]);
    Some(DecodedRevert::TransferFailed { token, to, amount })
}

fn decode_approve_failed(data: &[u8]) -> Option<DecodedRevert> {
    if data.len() < 64 {
        return None;
    }
    let token = Address::from_slice(&data[12..32]);
    let spender = Address::from_slice(&data[44..64]);
    Some(DecodedRevert::ApproveFailed { token, spender })
}

impl std::fmt::Display for DecodedRevert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodedRevert::Unauthorized => write!(f, "Unauthorized: caller is not owner"),
            DecodedRevert::DeadlineExpired => {
                write!(f, "DeadlineExpired: block.timestamp > deadline")
            }
            DecodedRevert::EmptyRoute => write!(f, "EmptyRoute: calls array is empty"),
            DecodedRevert::TooManyCalls => write!(f, "TooManyCalls: more than 12 calls"),
            DecodedRevert::FlashLoanRequired => write!(f, "FlashLoanRequired: flashAmount is zero"),
            DecodedRevert::InvalidRouteHash => {
                write!(f, "InvalidRouteHash: keccak(calls) != routeHash")
            }
            DecodedRevert::FlashLoanOnly => {
                write!(f, "FlashLoanOnly: caller is not the flash loan provider")
            }
            DecodedRevert::InvalidFlashLoanContext => write!(
                f,
                "InvalidFlashLoanContext: wrong phase or mismatched context"
            ),
            DecodedRevert::CallbackOnly => {
                write!(f, "CallbackOnly: caller is not the expected DEX pool")
            }
            DecodedRevert::UnsupportedProtocol(id) => {
                write!(f, "UnsupportedProtocol: protocol ID {id}")
            }
            DecodedRevert::InvalidPoolCaller { expected, actual } => {
                write!(f, "InvalidPoolCaller: expected {expected}, got {actual}")
            }
            DecodedRevert::ExternalCallFailed {
                index,
                target,
                reason,
            } => {
                write!(
                    f,
                    "ExternalCallFailed: hop {index}, target {target}, reason: {reason}"
                )
            }
            DecodedRevert::InsufficientProfit {
                final_balance,
                required,
            } => {
                write!(
                    f,
                    "InsufficientProfit: final={final_balance}, required={required}, shortfall={}",
                    required.saturating_sub(*final_balance)
                )
            }
            DecodedRevert::TransferFailed { token, to, amount } => {
                write!(f, "TransferFailed: token={token}, to={to}, amount={amount}")
            }
            DecodedRevert::ApproveFailed { token, spender } => {
                write!(f, "ApproveFailed: token={token}, spender={spender}")
            }
            DecodedRevert::ZeroAddress => write!(f, "ZeroAddress: parameter is address(0)"),
            DecodedRevert::InvalidCallbackSource => write!(
                f,
                "InvalidCallbackSource: factory lookup returned address(0)"
            ),
            DecodedRevert::BalancerVaultReentrancy => write!(
                f,
                "BalancerVaultReentrancy: vault calls forbidden inside Balancer flash-loan callback"
            ),
            DecodedRevert::Unknown(sel, hex) => {
                write!(f, "Unknown: selector=0x{sel:02x?}, data={hex}")
            }
        }
    }
}
