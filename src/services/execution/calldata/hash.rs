use alloy::dyn_abi::DynSolValue;
use alloy::primitives::{FixedBytes, keccak256};

use crate::abis::ExecutorCall;

/// Compute a deterministic hash of executor calls for route verification
pub fn compute_route_hash(calls: &[ExecutorCall]) -> FixedBytes<32> {
    let values: Vec<DynSolValue> = calls
        .iter()
        .map(|c| {
            DynSolValue::Tuple(vec![
                DynSolValue::Address(c.target),
                DynSolValue::Uint(c.value, 256),
                DynSolValue::Bytes(c.data.to_vec()),
            ])
        })
        .collect();
    keccak256(DynSolValue::Array(values).abi_encode())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{Address, U256};

    #[test]
    fn test_compute_route_hash_deterministic() {
        let call1 = ExecutorCall {
            target: Address::repeat_byte(0x01),
            value: U256::from(100),
            data: vec![1, 2, 3].into(),
        };
        let call2 = ExecutorCall {
            target: Address::repeat_byte(0x02),
            value: U256::from(200),
            data: vec![4, 5, 6].into(),
        };
        let hash1 = compute_route_hash(&[call1.clone(), call2.clone()]);
        let hash2 = compute_route_hash(&[call1, call2]);
        assert_eq!(hash1, hash2, "Route hash should be deterministic");
    }

    #[test]
    fn test_compute_route_hash_different_calls() {
        let call1 = ExecutorCall {
            target: Address::repeat_byte(0x01),
            value: U256::from(100),
            data: vec![1, 2, 3].into(),
        };
        let call2 = ExecutorCall {
            target: Address::repeat_byte(0x02),
            value: U256::from(200),
            data: vec![4, 5, 6].into(),
        };
        let call3 = ExecutorCall {
            target: Address::repeat_byte(0x03),
            value: U256::from(300),
            data: vec![7, 8, 9].into(),
        };

        let hash1 = compute_route_hash(&[call1.clone(), call2.clone()]);
        let hash2 = compute_route_hash(&[call1, call2, call3]);
        assert_ne!(
            hash1, hash2,
            "Different calls should produce different hashes"
        );
    }
}
