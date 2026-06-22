use alloy::primitives::{Address, U256};
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;

use crate::abis::IERC20;

/// Net ERC-20 profit credited to `executor` for `profit_token` from receipt logs.
pub fn parse_transfer_profit(
    logs: &[Log],
    executor: Address,
    profit_token: Option<Address>,
) -> U256 {
    let mut net = U256::ZERO;
    for log in logs {
        if let Some(token) = profit_token
            && log.address() != token
        {
            continue;
        }
        let Ok(decoded) = IERC20::Transfer::decode_log(&log.inner) else {
            continue;
        };
        if decoded.to == executor {
            net = net.saturating_add(decoded.value);
        } else if decoded.from == executor {
            net = net.saturating_sub(decoded.value);
        }
    }
    net
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Log as PrimitiveLog, address};
    use alloy::rpc::types::Log;

    use super::*;

    fn transfer_log(token: Address, from: Address, to: Address, value: U256) -> Log {
        Log {
            inner: PrimitiveLog {
                address: token,
                data: IERC20::Transfer { from, to, value }.encode_log_data(),
            },
            ..Default::default()
        }
    }

    #[test]
    fn nets_inflows_and_outflows_to_executor() {
        let executor = address!("0x00000000000000000000000000000000000000ee");
        let token = address!("0x0000000000000000000000000000000000000001");
        let other = address!("0x0000000000000000000000000000000000000002");
        let logs = vec![
            transfer_log(token, other, executor, U256::from(100u64)),
            transfer_log(token, executor, other, U256::from(30u64)),
            transfer_log(other, other, executor, U256::from(999u64)),
        ];
        assert_eq!(
            parse_transfer_profit(&logs, executor, Some(token)),
            U256::from(70u64)
        );
    }
}
