use alloy::primitives::{FixedBytes, keccak256};

use crate::abis::ExecutorCall;

/// Compute a deterministic hash of the packed route payload.
#[must_use]
pub fn compute_route_hash(route_blob: &[u8]) -> FixedBytes<32> {
    keccak256(route_blob)
}

pub fn pack_executor_calls(calls: &[ExecutorCall]) -> anyhow::Result<Vec<u8>> {
    if calls.is_empty() {
        anyhow::bail!("route must contain at least one executor call");
    }
    if !crate::pipeline::route_calls::packed_calls_fit_executor(calls.len()) {
        anyhow::bail!(
            "route has {} calls, maximum is {}",
            calls.len(),
            crate::pipeline::route_calls::MAX_EXECUTOR_CALLS
        );
    }
    if calls.iter().any(|call| {
        call.target == alloy::primitives::Address::ZERO
            || !crate::services::discovery::is_plausible_contract_address(call.target)
    }) {
        anyhow::bail!("executor call target must be a plausible contract address");
    }
    let mut out = Vec::with_capacity(
        32 + calls
            .iter()
            .map(|call| 32 + 32 + 32 + call.data.len())
            .sum::<usize>(),
    );
    out.extend_from_slice(&alloy::primitives::U256::from(calls.len()).to_be_bytes::<32>());
    for call in calls {
        out.extend_from_slice(&[0u8; 12]);
        out.extend_from_slice(call.target.as_slice());
        out.extend_from_slice(&call.value.to_be_bytes::<32>());
        out.extend_from_slice(&alloy::primitives::U256::from(call.data.len()).to_be_bytes::<32>());
        out.extend_from_slice(call.data.as_ref());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_route_hash_empty() {
        let hash = compute_route_hash(&[]);
        assert!(!hash.is_zero());
    }

    #[test]
    fn rejects_empty_and_zero_target_routes() {
        assert!(pack_executor_calls(&[]).is_err());
        let calls = [ExecutorCall {
            target: alloy::primitives::Address::ZERO,
            value: alloy::primitives::U256::ZERO,
            data: Default::default(),
        }];
        assert!(pack_executor_calls(&calls).is_err());
    }

    #[test]
    fn packed_calls_use_unpadded_data_length_layout() {
        let call = ExecutorCall {
            target: alloy::primitives::Address::from([0x11; 20]),
            value: alloy::primitives::U256::from(7u8),
            data: vec![0xaa, 0xbb, 0xcc].into(),
        };
        let packed = pack_executor_calls(&[call]).expect("call should pack");
        assert_eq!(packed.len(), 32 + 32 + 32 + 32 + 3);
        assert_eq!(
            &packed[0..32],
            &alloy::primitives::U256::ONE.to_be_bytes::<32>()
        );
        assert_eq!(&packed[32 + 12..32 + 32], &[0x11; 20]);
        assert_eq!(
            &packed[64..96],
            &alloy::primitives::U256::from(7u8).to_be_bytes::<32>()
        );
        assert_eq!(
            &packed[96..128],
            &alloy::primitives::U256::from(3u8).to_be_bytes::<32>()
        );
        assert_eq!(&packed[128..], &[0xaa, 0xbb, 0xcc]);
    }
}
