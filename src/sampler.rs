use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use anyhow::anyhow;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BinaryArray;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::StringArray;
use arrow::datatypes::DataType;
use base64::{Engine as _, engine::general_purpose};
use chrono::DateTime;
use serde::Serialize;
use serde_json::Map;
use serde_json::Number;
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub enum InputSampleColumnData {
    String(String),
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    // Float16(f16),
    Float32(f32),
    Float64(f64),
    Date32(i32),
    Date64(i64),
    Binary(Vec<u8>),
    FixedSizeBinary(Vec<u8>),
}
impl Into<Value> for InputSampleColumnData {
    fn into(self) -> Value {
        match self {
            InputSampleColumnData::String(s) => Value::String(s),
            InputSampleColumnData::Boolean(b) => Value::Bool(b),
            InputSampleColumnData::Int8(i) => Value::Number(i.into()),
            InputSampleColumnData::Int16(i) => Value::Number(i.into()),
            InputSampleColumnData::Int32(i) => Value::Number(i.into()),
            InputSampleColumnData::Int64(i) => Value::Number(i.into()),
            InputSampleColumnData::UInt8(u) => Value::Number(u.into()),
            InputSampleColumnData::UInt16(u) => Value::Number(u.into()),
            InputSampleColumnData::UInt32(u) => Value::Number(u.into()),
            InputSampleColumnData::UInt64(u) => Value::Number(u.into()),
            // InputSampleColumnData::Float16(f) => Value::Number(f.into()),
            InputSampleColumnData::Float32(f) => Number::from_f64(f as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            InputSampleColumnData::Float64(f) => Number::from_f64(f)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            InputSampleColumnData::Date32(d) => Value::Number(d.into()),
            InputSampleColumnData::Date64(d) => Value::Number(d.into()),
            InputSampleColumnData::Binary(b) => Value::String(general_purpose::STANDARD.encode(b)),
            InputSampleColumnData::FixedSizeBinary(b) => {
                Value::String(general_purpose::STANDARD.encode(b))
            }
        }
    }
}

pub fn get_ith_element_from_array(
    array: &ArrayRef,
    index: usize,
) -> anyhow::Result<Option<InputSampleColumnData>> {
    match array.data_type() {
        DataType::Utf8 => {
            let string_array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("Expected StringArray for Utf8 data type"))?;
            let value = match string_array.is_valid(index) {
                true => Some(InputSampleColumnData::String(
                    string_array.value(index).to_string(),
                )),
                false => None,
            };
            Ok(value)
        }
        DataType::Boolean => {
            let bool_array = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| anyhow!("Expected BooleanArray for Boolean data type"))?;
            let value = match bool_array.is_valid(index) {
                true => Some(InputSampleColumnData::Boolean(bool_array.value(index))),
                false => None,
            };
            Ok(value)
        }
        DataType::Int8 => {
            let int8_array = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| anyhow!("Expected Int8Array for Int8 data type"))?;
            let value = match int8_array.is_valid(index) {
                true => Some(InputSampleColumnData::Int8(int8_array.value(index))),
                false => None,
            };
            Ok(value)
        }
        DataType::Int16 => {
            let int16_array = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| anyhow!("Expected Int16Array for Int16 data type"))?;
            let value = match int16_array.is_valid(index) {
                true => Some(InputSampleColumnData::Int16(int16_array.value(index))),
                false => None,
            };
            Ok(value)
        }
        DataType::Int32 => {
            let int32_array = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| anyhow!("Expected Int32Array for Int32 data type"))?;
            let value = match int32_array.is_valid(index) {
                true => Some(InputSampleColumnData::Int32(int32_array.value(index))),
                false => None,
            };
            Ok(value)
        }
        DataType::Int64 => {
            let int64_array = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| anyhow!("Expected Int64Array for Int64 data type"))?;
            let value = match int64_array.is_valid(index) {
                true => Some(InputSampleColumnData::Int64(int64_array.value(index))),
                false => None,
            };
            Ok(value)
        }
        DataType::UInt8 => {
            let uint8_array = array
                .as_any()
                .downcast_ref::<arrow::array::UInt8Array>()
                .ok_or_else(|| anyhow!("Expected UInt8Array for UInt8 data type"))?;
            let value = match uint8_array.is_valid(index) {
                true => Some(InputSampleColumnData::UInt8(uint8_array.value(index))),
                false => None,
            };
            Ok(value)
        }
        DataType::UInt16 => {
            let uint16_array = array
                .as_any()
                .downcast_ref::<arrow::array::UInt16Array>()
                .ok_or_else(|| anyhow!("Expected UInt16Array for UInt16 data type"))?;
            let value = match uint16_array.is_valid(index) {
                true => Some(InputSampleColumnData::UInt16(uint16_array.value(index))),
                false => None,
            };
            Ok(value)
        }
        DataType::UInt32 => {
            let uint32_array = array
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .ok_or_else(|| anyhow!("Expected UInt32Array for UInt32 data type"))?;
            let value = match uint32_array.is_valid(index) {
                true => Some(InputSampleColumnData::UInt32(uint32_array.value(index))),
                false => None,
            };
            Ok(value)
        }
        DataType::UInt64 => {
            let uint64_array = array
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .ok_or_else(|| anyhow!("Expected UInt64Array for UInt64 data type"))?;
            let value = match uint64_array.is_valid(index) {
                true => Some(InputSampleColumnData::UInt64(uint64_array.value(index))),
                false => None,
            };
            Ok(value)
        }
        DataType::Float32 => {
            let float32_array = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| anyhow!("Expected Float32Array for Float32 data type"))?;
            let value = match float32_array.is_valid(index) {
                true => Some(InputSampleColumnData::Float32(float32_array.value(index))),
                false => None,
            };
            Ok(value)
        }
        DataType::Float64 => {
            let float64_array = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| anyhow!("Expected Float64Array for Float64 data type"))?;
            let value = match float64_array.is_valid(index) {
                true => Some(InputSampleColumnData::Float64(float64_array.value(index))),
                false => None,
            };
            Ok(value)
        }
        DataType::Date32 => {
            let date32_array = array
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| anyhow!("Expected Date32Array for Date32 data type"))?;
            let value = match date32_array.is_valid(index) {
                true => Some(InputSampleColumnData::Date32(date32_array.value(index))),
                false => None,
            };
            Ok(value)
        }
        DataType::Date64 => {
            let date64_array = array
                .as_any()
                .downcast_ref::<arrow::array::Date64Array>()
                .ok_or_else(|| anyhow!("Expected Date64Array for Date64 data type"))?;
            let value = match date64_array.is_valid(index) {
                true => Some(InputSampleColumnData::Date64(date64_array.value(index))),
                false => None,
            };
            Ok(value)
        }
        _ => Err(anyhow!("Unsupported data type: {}", array.data_type()).into()),
    }
}

#[derive(Debug, Clone)]
pub struct InputSample {
    pub id: usize,
    pub original_filepath: String,
    pub url: String,
    pub caption: Option<String>,
    pub additional_columns: HashMap<String, InputSampleColumnData>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub enum SampleStatus {
    Success,
    Failure(String),
}

#[derive(Debug, Clone)]
pub struct BatchSample {
    pub len: usize,
    pub uuid: FixedSizeBinaryArray,
    pub url: StringArray,
    pub caption: Option<StringArray>,
    pub bytes: Option<BinaryArray>,
    pub additional_columns: HashMap<String, ArrayRef>,
}

use arrow_select::concat::concat;

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
    let has_caption = batch_samples.iter().any(|s| s.caption.is_some());
    let merged_caption = match has_caption {
        true => {
            let captions: Vec<&dyn Array> = batch_samples
                .iter()
                .filter_map(|s| s.caption.as_ref().map(|c| c as &dyn Array))
                .collect();
            let caption_binding = concat(&captions)?;
            Some(
                caption_binding
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Merged caption should be StringArray")
                    .clone(),
            )
        }
        false => None,
    };
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
        caption: merged_caption,
        bytes: merged_bytes,
        additional_columns,
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputSample {
    pub id: usize,
    pub url: String,
    pub caption: Option<String>,
    pub original_filepath: String,
    pub download_data: Vec<u8>,
    pub download_mime_type: Option<String>,
    pub download_timestamp: Option<DateTime<chrono::Utc>>,
    pub additional_columns: HashMap<String, InputSampleColumnData>,
    pub status: SampleStatus,
}

impl OutputSample {
    pub fn to_json_file(&self) -> Result<(String, Vec<u8>)> {
        let mut json_map = Map::new();
        json_map.insert("_id".to_string(), Value::Number(self.id.into()));
        json_map.insert(
            "_original_filepath".to_string(),
            Value::String(self.original_filepath.clone()),
        );
        if let Some(mime_type) = &self.download_mime_type {
            json_map.insert(
                "_download_mime_type".to_string(),
                Value::String(mime_type.clone()),
            );
        }
        if let Some(timestamp) = &self.download_timestamp {
            json_map.insert(
                "_download_timestamp".to_string(),
                Value::String(timestamp.to_rfc3339()),
            );
        }
        for (key, value) in &self.additional_columns {
            json_map.insert(key.clone(), value.clone().into());
        }
        let json_string = serde_json::to_string(&json_map)?;
        let json_path = format!("{}.json", self.id);
        Ok((json_path, json_string.into_bytes()))
    }

    pub fn to_image_file(&self) -> Result<(String, Vec<u8>)> {
        let extension = match &self.download_mime_type {
            Some(mime_type) => match mime_type.as_str() {
                "image/jpeg" => "jpg",
                "image/png" => "png",
                _ => "jpg", // Fallback for unknown types
            },
            None => "jpg", // Fallback if no mime type is provided
        };
        let image_path = format!("{}.{}", self.id, extension);
        Ok((image_path, self.download_data.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryBuilder, FixedSizeBinaryBuilder, Int32Array, StringBuilder};
    use std::sync::Arc;

    fn create_test_batch_sample(
        urls: Vec<&str>,
        captions: Option<Vec<&str>>,
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

        // Create caption array if provided
        let caption_array = captions.map(|caps| {
            let mut caption_builder = StringBuilder::new();
            for cap in caps {
                caption_builder.append_value(cap);
            }
            caption_builder.finish()
        });

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
            caption: caption_array,
            bytes: bytes_array,
            additional_columns: additional_data.unwrap_or_default(),
        }
    }

    #[test]
    fn test_merge_batch_samples_basic() {
        let batch1 = create_test_batch_sample(
            vec!["http://example.com/1", "http://example.com/2"],
            Some(vec!["caption1", "caption2"]),
            None,
            None,
        );

        let batch2 = create_test_batch_sample(
            vec!["http://example.com/3"],
            Some(vec!["caption3"]),
            None,
            None,
        );

        let merged = merge_batch_samples(vec![batch1, batch2]).unwrap();

        assert_eq!(merged.len, 3);
        assert_eq!(merged.url.len(), 3);
        assert_eq!(merged.url.value(0), "http://example.com/1");
        assert_eq!(merged.url.value(1), "http://example.com/2");
        assert_eq!(merged.url.value(2), "http://example.com/3");

        assert!(merged.caption.is_some());
        let caption_array = merged.caption.unwrap();
        assert_eq!(caption_array.value(0), "caption1");
        assert_eq!(caption_array.value(1), "caption2");
        assert_eq!(caption_array.value(2), "caption3");
    }

    #[test]
    fn test_merge_batch_samples_with_bytes() {
        let bytes1 = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let bytes2 = vec![vec![7, 8, 9]];

        let batch1 = create_test_batch_sample(
            vec!["http://example.com/1", "http://example.com/2"],
            None,
            Some(bytes1),
            None,
        );

        let batch2 =
            create_test_batch_sample(vec!["http://example.com/3"], None, Some(bytes2), None);

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
            None,
            Some(additional1),
        );

        let batch2 =
            create_test_batch_sample(vec!["http://example.com/3"], None, None, Some(additional2));

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
    fn test_merge_batch_samples_mixed_captions() {
        // First batch has captions, second doesn't
        let batch1 = create_test_batch_sample(
            vec!["http://example.com/1"],
            Some(vec!["caption1"]),
            None,
            None,
        );

        let batch2 = create_test_batch_sample(
            vec!["http://example.com/2"],
            None, // No captions
            None,
            None,
        );

        let merged = merge_batch_samples(vec![batch1, batch2]).unwrap();

        assert_eq!(merged.len, 2);
        // Should have captions because at least one batch has them
        assert!(merged.caption.is_some());
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
            Some(vec!["caption1", "caption2"]),
            None,
            None,
        );

        let original_len = batch.len;
        let merged = merge_batch_samples(vec![batch]).unwrap();

        assert_eq!(merged.len, original_len);
        assert_eq!(merged.url.len(), 2);
        assert!(merged.caption.is_some());
    }
}
