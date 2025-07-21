use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use anyhow::anyhow;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BinaryArray;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::FixedSizeBinaryBuilder;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct BatchSample {
    original_filepath: String,
    shard_id: uuid::Uuid,
    uuid: FixedSizeBinaryArray,
    url: StringArray,
    bytes: Vec<Option<Bytes>>,
    additional_columns: HashMap<String, ArrayRef>,
}

impl BatchSample {
    pub fn generate(
        filepath: String,
        url: StringArray,
        additional_columns: HashMap<String, ArrayRef>,
    ) -> Result<Self> {
        let shard_id = uuid::Uuid::now_v7();
        let len = url.len();
        let mut uuid_builder = FixedSizeBinaryBuilder::new(16);
        for _ in 0..url.len() {
            uuid_builder.append_value(uuid::Uuid::now_v7()).unwrap();
        }
        let uuid_array = uuid_builder.finish();
        for (_, value) in additional_columns.iter() {
            if value.len() != url.len() {
                return Err(anyhow!(
                    "Additional columns must have the same length as the URL array"
                ));
            }
        }
        Ok(BatchSample {
            original_filepath: filepath,
            shard_id: shard_id,
            uuid: uuid_array,
            url: url,
            bytes: vec![None; len],
            additional_columns: additional_columns,
        })
    }

    pub fn get_valid_bytes_count(&self) -> usize {
        let mut count = 0;
        for value in self.bytes.iter() {
            if value.is_some() {
                count += 1;
            }
        }
        count
    }

    pub fn id(&self) -> uuid::Uuid {
        self.shard_id
    }

    pub fn url(&self) -> &StringArray {
        &self.url
    }

    pub fn put_bytes(&mut self, bytes: Vec<Option<Bytes>>) {
        self.bytes = bytes;
    }

    pub fn bytes(&self) -> &[Option<Bytes>] {
        &self.bytes
    }

    pub fn len(&self) -> usize {
        self.url.len()
    }

    pub fn filepath(&self) -> &str {
        &self.original_filepath
    }

    pub fn schema(&self) -> Arc<Schema> {
        let mut schema_vec = vec![
            Field::new("_id", DataType::FixedSizeBinary(16), false),
            Field::new("_url", DataType::Utf8, false),
            Field::new("_filepath", DataType::Utf8, false),
            Field::new("_data", DataType::Binary, true),
        ];
        for (key, value) in self.additional_columns.iter() {
            schema_vec.push(Field::new(key, value.data_type().clone(), true));
        }
        Arc::new(Schema::new(schema_vec))
    }

    pub fn array(&self) -> Vec<ArrayRef> {
        let filepath_array = StringArray::from(
            std::iter::repeat(self.original_filepath.clone())
                .take(self.len())
                .collect::<Vec<String>>(),
        );
        let bytes_array = BinaryArray::from_iter(self.bytes.iter());

        let mut array_vec: Vec<ArrayRef> = vec![
            Arc::new(self.uuid.clone()),
            Arc::new(self.url.clone()),
            Arc::new(filepath_array),
            Arc::new(bytes_array),
        ];
        for (_, value) in self.additional_columns.iter() {
            array_vec.push(value.clone());
        }

        array_vec
    }

    pub fn batch(&self) -> Result<RecordBatch> {
        let schema = self.schema();
        let array = self.array();
        RecordBatch::try_new(schema, array).map_err(|e| anyhow::anyhow!(e))
    }
}
