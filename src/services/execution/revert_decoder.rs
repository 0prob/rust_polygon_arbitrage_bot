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
/// Aave V3 `ReserveInactive()` — bubbled through Huff `aave_call_failed`, not an ArbExecutor error.
const SEL_AAVE_RESERVE_INACTIVE: [u8; 4] = [0x90, 0xcd, 0x6f, 0x24];
/// Standard Solidity `revert(string)` — produced by `require(cond, "msg")`.
const SEL_ERROR_STRING: [u8; 4] = [0x08, 0xc3, 0x79, 0xa0];
/// Solidity `Panic(uint256)` — produced by `assert()`, division by zero, etc.
const SEL_PANIC: [u8; 4] = [0x4e, 0x48, 0x7b, 0x71];

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
    AaveReserveInactive,
    Unknown([u8; 4], String),
}

#[must_use]
pub fn decode_revert(data: &[u8]) -> Option<DecodedRevert> {
    if data.len() < 4 {
        return None;
    }
    let Ok(sel) = data[0..4].try_into() else {
        return None;
    };
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
        SEL_AAVE_RESERVE_INACTIVE => Some(DecodedRevert::AaveReserveInactive),
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
        SEL_ERROR_STRING => {
            let str_off = word_u256(payload, 0)
                .and_then(|v| usize::try_from(v).ok())
                .unwrap_or(32);
            Some(DecodedRevert::ExternalCallFailed {
                index: 0,
                target: Address::ZERO,
                reason: decode_abi_string(payload, str_off),
            })
        }
        SEL_PANIC => {
            let code = U256::from_be_slice(payload.get(..32).unwrap_or(&[]));
            Some(DecodedRevert::ExternalCallFailed {
                index: 0,
                target: Address::ZERO,
                reason: format!("Panic({code})"),
            })
        }
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
        if rd_size == 0 {
            // V4/router often reverts with empty returndata; surface that explicitly.
            let trailing = payload.len().saturating_sub(128);
            reason = if trailing > 0 {
                format!(
                    "empty nested revert (rd_size=0, trailing={}B 0x{})",
                    trailing,
                    hex_preview(&payload[128..], 32)
                )
            } else {
                "empty nested revert (rd_size=0)".to_string()
            };
        } else if payload.len() >= 128usize.saturating_add(rd_size) {
            let rd = &payload[128..128 + rd_size];
            // Prefer Error(string) so ERC20 balance messages are not misread as
            // nested ExternalCallFailed (Huff layout collides with ABI string heads).
            reason = decode_error_string_prefer(rd)
                .or_else(|| decode_revert(rd).map(|r| r.to_string()))
                .unwrap_or_else(|| format!("0x{}", hex_preview(rd, 64)));
        } else {
            reason = format!(
                "truncated nested revert (rd_size={rd_size}, payload={}B)",
                payload.len()
            );
        }
    } else if payload.len() > 64 {
        reason = format!("short huff payload ({}B)", payload.len());
    }
    sanitize_external_call_failed(index, target, reason)
}

/// Prefer Solidity `Error(string)` when nested reason bytes are a standard revert string.
/// Prevents misreading `"ERC20: transfer amount exceeds balance"` as a nested ExternalCallFailed.
fn decode_error_string_prefer(data: &[u8]) -> Option<String> {
    const ERROR_STRING: [u8; 4] = [0x08, 0xc3, 0x79, 0xa0];
    if data.len() < 4 || data[0..4] != ERROR_STRING {
        return None;
    }
    let payload = &data[4..];
    // Error(string) ABI: word0 = offset to (len, bytes); decode_abi_string wants the len word.
    let str_off = word_u256(payload, 0).and_then(|v| usize::try_from(v).ok())?;
    let s = decode_abi_string(payload, str_off);
    if s.is_empty() { None } else { Some(s) }
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
    if let Some(decoded) = decode_huff_external_call_failed_payload(payload) {
        return Some(decoded);
    }
    if let Some(decoded) = decode_external_call_failed_abi(payload) {
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
            DecodedRevert::AaveReserveInactive => {
                write!(
                    f,
                    "AaveReserveInactive: token reserve not active for flash loan"
                )
            }
            DecodedRevert::Unknown(sel, hex) => {
                write!(f, "Unknown: selector=0x{sel:02x?}, data={hex}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;

    #[test]
    fn huff_external_call_failure_wins_over_abi_shape_collision() {
        let target = address!("c8024d73455853224df33bcda42b82502a964b9e");
        let mut payload = [0u8; 128];
        payload[31] = 3;
        payload[44..64].copy_from_slice(target.as_slice());

        assert!(matches!(
            decode_external_call_failed(&payload),
            Some(DecodedRevert::ExternalCallFailed {
                index: 3,
                target: decoded_target,
                ..
            }) if decoded_target == target
        ));
    }

    #[test]
    fn empty_nested_revert_is_labeled_when_rd_size_zero() {
        let target = address!("c8024d73455853224df33bcda42b82502a964b9e");
        let mut payload = [0u8; 128];
        payload[31] = 1;
        payload[44..64].copy_from_slice(target.as_slice());
        let decoded = decode_external_call_failed(&payload).expect("decode");
        match decoded {
            DecodedRevert::ExternalCallFailed {
                index,
                target: t,
                reason,
            } => {
                assert_eq!(index, 1);
                assert_eq!(t, target);
                assert_eq!(reason, "empty nested revert (rd_size=0)");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn nested_error_string_is_not_misread_as_external_call_failed() {
        let token = address!("a571963278014b5b3a686778747fdf8ad4dfbb94");
        let msg = b"ERC20: transfer amount exceeds balance";
        // Error(string) ABI: selector + offset(32) + len(32) + padded string
        let mut inner = Vec::with_capacity(4 + 64 + 64);
        inner.extend_from_slice(&[0x08, 0xc3, 0x79, 0xa0]);
        inner.extend_from_slice(&U256::from(32u64).to_be_bytes::<32>());
        inner.extend_from_slice(&U256::from(msg.len() as u64).to_be_bytes::<32>());
        inner.extend_from_slice(msg);
        inner.resize(4 + 64 + ((msg.len() + 31) / 32) * 32, 0);

        // Huff ExternalCallFailed body: index, target, offset(0x60), rd_size, rd
        let mut payload = vec![0u8; 128 + inner.len()];
        payload[31] = 0; // index 0
        payload[44..64].copy_from_slice(token.as_slice());
        payload[64..96].copy_from_slice(&U256::from(0x60u64).to_be_bytes::<32>());
        payload[96..128].copy_from_slice(&U256::from(inner.len() as u64).to_be_bytes::<32>());
        payload[128..].copy_from_slice(&inner);

        let decoded = decode_external_call_failed(&payload).expect("decode");
        match decoded {
            DecodedRevert::ExternalCallFailed {
                index,
                target,
                reason,
            } => {
                assert_eq!(index, 0);
                assert_eq!(target, token);
                assert!(
                    reason.contains("transfer amount exceeds balance"),
                    "reason={reason}"
                );
                assert!(
                    !reason.contains("ExternalCallFailed"),
                    "must not nest phantom ExternalCallFailed: {reason}"
                );
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
