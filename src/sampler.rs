use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use anyhow::anyhow;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BinaryArray;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::FixedSizeBinaryBuilder;
use arrow::array::StringArray;
use arrow_select::concat::concat;

#[derive(Debug, Clone)]
pub struct BatchSample {
    pub len: usize,
    pub uuid: FixedSizeBinaryArray,
    pub url: StringArray,
    pub bytes: Option<BinaryArray>,
    pub additional_columns: HashMap<String, ArrayRef>,
}

impl BatchSample {
    pub fn from_vec(
        urls: Vec<String>,
        additional_columns: Option<HashMap<String, ArrayRef>>,
    ) -> Self {
        let len = urls.len();
        let mut uuid_builder = FixedSizeBinaryBuilder::new(16); // 16 bytes for UUID v7
        for _ in 0..len {
            uuid_builder
                .append_value(&uuid::Uuid::now_v7().as_bytes())
                .unwrap();
        }
        let uuid_array = uuid_builder.finish();
        let url_builder = StringArray::from(urls);

        BatchSample {
            len,
            uuid: uuid_array,
            url: url_builder,
            bytes: None,
            additional_columns: additional_columns.unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShardSample {
    pub original_filepath: String,
    pub shard_id: uuid::Uuid,
    pub samples: Vec<BatchSample>,
}

impl ShardSample {
    pub fn get_valid_bytes_count(&self) -> usize {
        self.samples
            .iter()
            .map(|s| s.bytes.as_ref().map(|b| s.len - b.null_count()))
            .sum::<Option<usize>>()
            .unwrap_or(0)
    }

    pub fn len(&self) -> usize {
        self.samples.iter().map(|s| s.len).sum()
    }
}

pub fn merge_batch_samples(batch_samples: Vec<BatchSample>) -> Result<BatchSample> {
    if batch_samples.is_empty() {
        return Err(anyhow!("Cannot merge empty batch samples"));
    }

    let uuids: Vec<&dyn Array> = batch_samples
        .iter()
        .map(|s| &s.uuid as &dyn Array)
        .collect();
    let uuid_binding = concat(&uuids)?;
    let merged_uuid = uuid_binding
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("Merged ID should be UInt64Array");
    let urls: Vec<&dyn Array> = batch_samples.iter().map(|s| &s.url as &dyn Array).collect();
    let url_binding = concat(&urls)?;
    let merged_url = url_binding
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Merged URL should be StringArray");
    let has_bytes = batch_samples.iter().any(|s| s.bytes.is_some());
    let merged_bytes = match has_bytes {
        true => {
            let bytes: Vec<&dyn Array> = batch_samples
                .iter()
                .filter_map(|s| s.bytes.as_ref().map(|b| b as &dyn Array))
                .collect();
            let byte_binding = concat(&bytes)?;
            Some(
                byte_binding
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("Merged bytes should be BinaryArray")
                    .clone(),
            )
        }
        false => None,
    };

    let mut additional_columns = HashMap::new();
    if !batch_samples.is_empty() {
        let first_keys = batch_samples[0].additional_columns.keys();
        for key in first_keys {
            let columns_for_key: Vec<ArrayRef> = batch_samples
                .iter()
                .map(|s| {
                    s.additional_columns
                        .get(key)
                        .cloned()
                        .unwrap_or_else(|| Arc::new(StringArray::new_null(s.len)))
                })
                .collect();
            let columns_for_key_refs: Vec<&dyn Array> =
                columns_for_key.iter().map(|c| c.as_ref()).collect();
            let merged_column = concat(&columns_for_key_refs)?;
            additional_columns.insert(key.clone(), merged_column);
        }
    }

    Ok(BatchSample {
        len: merged_uuid.len(),
        uuid: merged_uuid.clone(),
        url: merged_url.clone(),
        bytes: merged_bytes,
        additional_columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryBuilder, FixedSizeBinaryBuilder, Int32Array, StringBuilder};
    use std::sync::Arc;

    fn create_test_batch_sample(
        urls: Vec<&str>,
        bytes_data: Option<Vec<Vec<u8>>>,
        additional_data: Option<HashMap<String, ArrayRef>>,
    ) -> BatchSample {
        let len = urls.len();

        // Create UUID array
        let mut uuid_builder = FixedSizeBinaryBuilder::new(16);
        for _ in 0..len {
            uuid_builder
                .append_value(&uuid::Uuid::now_v7().as_bytes())
                .unwrap();
        }
        let uuid_array = uuid_builder.finish();

        // Create URL array
        let mut url_builder = StringBuilder::new();
        for url in urls {
            url_builder.append_value(url);
        }
        let url_array = url_builder.finish();

        // Create bytes array if provided
        let bytes_array = bytes_data.map(|bytes| {
            let mut bytes_builder = BinaryBuilder::new();
            for byte_vec in bytes {
                bytes_builder.append_value(&byte_vec);
            }
            bytes_builder.finish()
        });

        BatchSample {
            len,
            uuid: uuid_array,
            url: url_array,
            bytes: bytes_array,
            additional_columns: additional_data.unwrap_or_default(),
        }
    }

    #[test]
    fn test_merge_batch_samples_basic() {
        let batch1 = create_test_batch_sample(
            vec!["http://example.com/1", "http://example.com/2"],
            None,
            None,
        );

        let batch2 = create_test_batch_sample(vec!["http://example.com/3"], None, None);

        let merged = merge_batch_samples(vec![batch1, batch2]).unwrap();

        assert_eq!(merged.len, 3);
        assert_eq!(merged.url.len(), 3);
        assert_eq!(merged.url.value(0), "http://example.com/1");
        assert_eq!(merged.url.value(1), "http://example.com/2");
        assert_eq!(merged.url.value(2), "http://example.com/3");
    }

    #[test]
    fn test_merge_batch_samples_with_bytes() {
        let bytes1 = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let bytes2 = vec![vec![7, 8, 9]];

        let batch1 = create_test_batch_sample(
            vec!["http://example.com/1", "http://example.com/2"],
            Some(bytes1),
            None,
        );

        let batch2 = create_test_batch_sample(vec!["http://example.com/3"], Some(bytes2), None);

        let merged = merge_batch_samples(vec![batch1, batch2]).unwrap();

        assert_eq!(merged.len, 3);
        assert!(merged.bytes.is_some());

        let bytes_array = merged.bytes.unwrap();
        assert_eq!(bytes_array.value(0), &[1, 2, 3]);
        assert_eq!(bytes_array.value(1), &[4, 5, 6]);
        assert_eq!(bytes_array.value(2), &[7, 8, 9]);
    }

    #[test]
    fn test_merge_batch_samples_with_additional_columns() {
        // Create additional columns for batch1
        let mut additional1 = HashMap::new();
        let id_array1 = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        additional1.insert("id".to_string(), id_array1);

        // Create additional columns for batch2
        let mut additional2 = HashMap::new();
        let id_array2 = Arc::new(Int32Array::from(vec![3])) as ArrayRef;
        additional2.insert("id".to_string(), id_array2);

        let batch1 = create_test_batch_sample(
            vec!["http://example.com/1", "http://example.com/2"],
            None,
            Some(additional1),
        );

        let batch2 =
            create_test_batch_sample(vec!["http://example.com/3"], None, Some(additional2));

        let merged = merge_batch_samples(vec![batch1, batch2]).unwrap();

        assert_eq!(merged.len, 3);
        assert!(merged.additional_columns.contains_key("id"));

        let id_column = merged.additional_columns.get("id").unwrap();
        let id_array = id_column.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);
        assert_eq!(id_array.value(2), 3);
    }

    #[test]
    fn test_merge_batch_samples_empty_input() {
        let result = merge_batch_samples(vec![]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot merge empty batch samples")
        );
    }

    #[test]
    fn test_merge_batch_samples_single_batch() {
        let batch = create_test_batch_sample(
            vec!["http://example.com/1", "http://example.com/2"],
            None,
            None,
        );

        let original_len = batch.len;
        let merged = merge_batch_samples(vec![batch]).unwrap();

        assert_eq!(merged.len, original_len);
        assert_eq!(merged.url.len(), 2);
    }
}
