//! Query consistency shared by the Data API and sockets transports.

use std::collections::BTreeMap;

use serde_json::Value;

/// Build a sparse scan vector from `(partition, uuid, sequence)` tokens.
///
/// A partition must wait for its highest sequence, independent of token order.
/// Different UUIDs describe different histories and cannot safely be merged.
pub fn scan_vector(tokens: impl IntoIterator<Item = (u64, u64, u64)>) -> Option<Value> {
    let mut partitions = BTreeMap::<u64, (u64, u64)>::new();
    for (partition, uuid, sequence) in tokens {
        if uuid == 0 {
            return None;
        }
        let entry = partitions.entry(partition).or_insert((uuid, sequence));
        if entry.0 != uuid {
            return None;
        }
        entry.1 = entry.1.max(sequence);
    }
    if partitions.is_empty() {
        return None;
    }
    Some(Value::Object(
        partitions
            .into_iter()
            .map(|(partition, (uuid, sequence))| {
                (
                    partition.to_string(),
                    serde_json::json!([sequence, uuid.to_string()]),
                )
            })
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_partitions_keep_the_highest_sequence_in_any_order() {
        let expected = serde_json::json!({"5": [100, "9"], "6": [7, "10"]});
        for tokens in [
            [(5, 9, 100), (6, 10, 7), (5, 9, 90)],
            [(5, 9, 90), (6, 10, 7), (5, 9, 100)],
        ] {
            assert_eq!(scan_vector(tokens), Some(expected.clone()));
        }
    }

    #[test]
    fn incompatible_histories_and_missing_tokens_are_refused() {
        assert!(scan_vector([]).is_none());
        assert!(scan_vector([(5, 0, 100)]).is_none());
        assert!(scan_vector([(5, 9, 100), (5, 10, 90)]).is_none());
        assert!(scan_vector([(5, 10, 90), (5, 9, 100)]).is_none());
    }
}
