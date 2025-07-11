use anyhow::anyhow;
use arrow::array::{Array, ArrayRef, StringArray};
use async_stream::stream;
use csv_async::AsyncReader;
use futures::{Stream, StreamExt};
use opendal::{Operator, services::Fs};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::LogicalType;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, error::Error};
use tokio::io::{AsyncBufReadExt, BufReader};
#[derive(Debug, Clone)]
pub struct ArrayData {
    pub index: usize,
    pub reference: ArrayRef,
}

#[derive(Debug, Clone)]
pub enum SampleData {
    Array(ArrayData),
    String(String),
}

#[derive(Debug, Clone)]
pub struct Sample {
    pub url: String,
    pub caption: Option<String>,
    pub additional_columns: HashMap<String, SampleData>,
}

#[derive(Debug, Clone, Copy)]
pub enum InputFormat {
    Txt,
    Csv,
    Parquet,
}

pub struct Reader {
    path: String,
    input_format: InputFormat,
    url_col: String,
    caption_col: Option<String>,
    save_additional_columns: bool,
    number_of_items_per_shard: usize,
    max_shard_size: usize,
    shard_count: usize,
    start_shard: usize,
    end_shard: Option<usize>,
    operator: Arc<Operator>,
}

type SampleStream =
    Pin<Box<dyn Stream<Item = Result<Sample, Box<dyn Error + Send + Sync>>> + Send>>;

impl Reader {
    pub fn new(
        path: &str,
        input_format: InputFormat,
        url_col: &str,
        caption_col: Option<&str>,
        save_additional_columns: bool,
        number_of_items_per_shard: usize,
        shard_count: usize,
        start_shard: usize,
        end_shard: Option<usize>,
    ) -> Result<Self, Box<dyn Error>> {
        let mut builder = Fs::default();
        builder.root("/");
        let op = Operator::new(builder)?.finish();

        // Simplified for now, will implement properly later
        let max_shard_size = 1024 * 1024 * 1024; // 1GB

        Ok(Self {
            path: path.to_string(),
            input_format,
            url_col: url_col.to_string(),
            caption_col: caption_col.map(|s| s.to_string()),
            save_additional_columns,
            number_of_items_per_shard,
            max_shard_size,
            shard_count,
            start_shard,
            end_shard,
            operator: Arc::new(op),
        })
    }

    pub fn shards(
        &self,
    ) -> impl Stream<Item = Result<Vec<Sample>, Box<dyn Error + Send + Sync>>> + '_ {
        stream! {
            let sample_stream = self.get_sample_stream().await.unwrap(); // This unwrap is bad, fix it
            let mut shard = Vec::with_capacity(self.number_of_items_per_shard);
            let mut current_shard_size = 0;

            for await sample in sample_stream {
                let sample = sample?;
                let sample_size = sample.url.len() + sample.caption.as_deref().unwrap_or("").len(); // Approximate size
                if shard.len() >= self.number_of_items_per_shard || current_shard_size + sample_size > self.max_shard_size {
                    yield Ok(shard);
                    shard = Vec::with_capacity(self.number_of_items_per_shard);
                    current_shard_size = 0;
                }
                current_shard_size += sample_size;
                shard.push(sample);
            }
            if !shard.is_empty() {
                yield Ok(shard);
            }
        }
    }

    async fn get_sample_stream(&self) -> Result<SampleStream, Box<dyn Error + Send + Sync>> {
        let op = self.operator.clone();
        let path = self.path.clone();
        let url_col = self.url_col.clone();
        let caption_col = self.caption_col.clone();
        let save_additional_columns = self.save_additional_columns;

        let stream: SampleStream = match self.input_format {
            InputFormat::Txt => {
                let stream = read_txt_stream(op, path).await?;
                Box::pin(stream)
            }
            InputFormat::Csv => {
                let stream =
                    read_csv_stream(op, path, url_col, caption_col, save_additional_columns)
                        .await?;
                Box::pin(stream)
            }
            InputFormat::Parquet => {
                let stream =
                    read_parquet_stream(op, path, url_col, caption_col, save_additional_columns)
                        .await?;
                Box::pin(stream)
            }
        };
        Ok(stream)
    }
}

async fn read_txt_stream(
    op: Arc<Operator>,
    path: String,
) -> Result<
    impl Stream<Item = Result<Sample, Box<dyn Error + Send + Sync>>>,
    Box<dyn Error + Send + Sync>,
> {
    let reader = op.reader(&path).await?;
    let buf_reader = BufReader::new(reader);
    let stream = stream! {
        let mut lines = buf_reader.lines();
        while let Some(new_line) = lines.next_line().await? {
            // assuming each line is a URL
            if new_line.trim().is_empty() {
                continue; // skip empty lines
            }
            let url = new_line.trim().to_string();
            yield Ok(Sample {
                url,
                caption: None,
                additional_columns: HashMap::new(),
            });
        }
    };
    Ok(stream)
}

async fn read_csv_stream(
    op: Arc<Operator>,
    path: String,
    input_url_col: String,
    input_caption_col: Option<String>,
    save_additional_columns: bool,
) -> Result<
    impl Stream<Item = Result<Sample, Box<dyn Error + Send + Sync>>>,
    Box<dyn Error + Send + Sync>,
> {
    let reader = op.reader(&path).await?;
    let mut csv_reader = AsyncReader::from_reader(reader);
    let headers = csv_reader.headers().await?.clone();

    // Create an indexed map of additional column names to save
    let additional_column_names: HashMap<String, usize> = headers
        .iter()
        .enumerate()
        .filter_map(|(i, h)| {
            if save_additional_columns
                && (h != input_url_col
                    && (input_caption_col.is_none() || h != input_caption_col.as_deref().unwrap()))
            {
                Some((h.to_string(), i))
            } else {
                None
            }
        })
        .collect();
    let url_col_idx = headers
        .iter()
        .position(|h| h == input_url_col)
        .ok_or_else(|| anyhow!("URL column '{}' not found in headers", input_url_col))?;
    let caption_col_idx = match input_caption_col.as_deref() {
        Some(col) => Some(
            headers
                .iter()
                .position(|h| h == col)
                .ok_or_else(|| anyhow!("Caption column '{}' not found in headers", col))?,
        ),
        None => None,
    };

    let stream = stream! {
        let mut records = csv_reader.into_records();
        while let Some(result) = records.next().await {
            match result {
                Ok(record) => {
                    let url = match record.get(url_col_idx) {
                        Some(url) => url.to_string(),
                        None => {
                            yield Err(anyhow!("Missing URL value in record where headers are: {:?}", headers).into());
                            continue;
                        }
                    };

                    let caption = caption_col_idx
                        .and_then(|idx| record.get(idx).map(|s| s.to_string()));

                    let mut additional_columns = HashMap::new();
                    for (name, idx) in additional_column_names.iter() {
                        if let Some(value) = record.get(*idx) {
                            additional_columns.insert(
                                name.clone(),
                                SampleData::String(
                                    value.to_string(),
                                )
                            );
                        }
                    }

                    yield Ok(Sample {
                        url,
                        caption,
                        additional_columns,
                    });
                },
                Err(e) => {
                    yield Err(e.into());
                }
            }
        }
    };

    Ok(stream)
}

async fn read_parquet_stream(
    op: Arc<Operator>,
    path: String,
    url_col: String,
    caption_col: Option<String>,
    save_additional_columns: bool,
) -> Result<
    impl Stream<Item = Result<Sample, Box<dyn Error + Send + Sync>>>,
    Box<dyn Error + Send + Sync>,
> {
    // Step 1: Read the Parquet file and extract the columns to ids index
    let reader = op.reader(&path).await?;
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let file_metadata = builder.metadata().file_metadata();
    let schema_desc = file_metadata.schema_descr();

    let columns = schema_desc.columns();
    let mut col_name_to_idx: HashMap<String, usize> = HashMap::new();
    for (i, field) in schema_desc.columns().iter().enumerate() {
        col_name_to_idx.insert(field.name().to_string(), i);
    }

    // Step 2: Get the wanted indices for the columns
    let mut wanted_indices: Vec<usize> = Vec::new();
    let mut additional_col_names: Vec<String> = Vec::new();

    // a. Handle url_col
    let url_col_idx = *col_name_to_idx
        .get(&url_col)
        .ok_or_else(|| anyhow!("URL column '{}' not found in Parquet file", url_col))?;
    let url_col_desc = &columns[url_col_idx];
    if url_col_desc.logical_type() != Some(LogicalType::String) {
        return Err(anyhow!("URL column '{}' must be of type String", url_col).into());
    }
    wanted_indices.push(url_col_idx);

    // b. Handle caption_col (optional but required if specified)
    let caption_col_idx = if let Some(col_name) = caption_col.clone() {
        let idx = *col_name_to_idx
            .get(&col_name)
            .ok_or_else(|| anyhow!("Caption column '{}' not found in Parquet file", col_name))?;
        wanted_indices.push(idx);
        let caption_col_desc = &columns[idx];
        if caption_col_desc.logical_type() != Some(LogicalType::String) {
            return Err(anyhow!("Caption column '{}' must be of type String", col_name).into());
        }
        Some(idx)
    } else {
        None
    };

    // c. If save_additional_columns is true, collect additional columns
    if save_additional_columns {
        for (name, &idx) in &col_name_to_idx {
            if idx != url_col_idx && caption_col_idx.map_or(true, |c_idx| idx != c_idx) {
                wanted_indices.push(idx);
                additional_col_names.push(name.clone());
            }
        }
    }

    // Step 3: Create the projection mask and build the reader
    let projection = ProjectionMask::roots(schema_desc, wanted_indices);
    let mut arrow_reader = builder.with_projection(projection).build()?;

    // Step 4: Find the indices of the URL, caption, and additional columns
    let projected_schema = arrow_reader.schema();
    let url_idx_proj = projected_schema
        .index_of(&url_col)
        .map_err(|_| anyhow!("URL column '{}' not found in projected schema", url_col))?;
    let caption_idx_proj = caption_col.and_then(|name| projected_schema.index_of(&name).ok());
    let additional_col_indices: HashMap<String, usize> = additional_col_names
        .clone()
        .into_iter()
        .map(|name| {
            let idx = projected_schema.index_of(&name).unwrap();
            (name, idx)
        })
        .collect();

    let stream = stream! {
        let mut batch_num = 0;
        while let Some(result) = arrow_reader.next().await {
            log::info!("Processing batch {}", batch_num);
            let record_batch = match result {
                Ok(batch) => batch,
                Err(e) => {
                    yield Err(Box::new(e) as Box<dyn Error + Send + Sync>);
                    continue;
                }
            };

            // prepare the arrays for URL, caption, and additional columns
            let url_array = match record_batch.column(url_idx_proj).as_any().downcast_ref::<StringArray>() {
                Some(arr) => arr,
                None => {
                    yield Err(anyhow!("URL column is not of type String").into());
                    continue;
                }
            };
            let caption_array = match caption_idx_proj {
                Some(idx) => record_batch.column(idx).as_any().downcast_ref::<StringArray>(),
                None => None,
            };
            let additional_arrays: Vec<(&str, ArrayRef)> = additional_col_names.iter().map(|name| {
                let idx = additional_col_indices.get(name).unwrap();
                let array = record_batch.column(*idx);
                (name.as_str(), array.clone())
            }).collect();

            for i in 0..record_batch.num_rows() {
                let url = url_array.value(i).to_string();
                let caption = caption_array.as_ref().map(|arr| arr.value(i).to_string());

                let mut additional_columns = HashMap::new();
                for (name, array) in &additional_arrays {
                    additional_columns.insert(
                        name.to_string(),
                        SampleData::Array(ArrayData {
                            index: i,
                            reference: array.slice(i, 1),
                        }),
                    );
                }

                yield Ok(Sample {
                    url,
                    caption,
                    additional_columns,
                });
            }
            batch_num += 1;
        }
    };

    Ok(stream)
}
