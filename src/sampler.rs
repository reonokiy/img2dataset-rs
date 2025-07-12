use std::collections::HashMap;

use anyhow::Result;
use anyhow::anyhow;
use arrow::array::Array;
use arrow::array::ArrayRef;
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
