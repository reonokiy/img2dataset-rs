use crate::sampler::{BatchSample, ShardSample};
// use crate::sampler::{InputSample, InputSampleColumnData, get_ith_element_from_array};
use anyhow::anyhow;
use arrow::array::FixedSizeBinaryBuilder;
use arrow::array::StringArray;
// use arrow::array::{Array, ArrayRef, StringArray};
use async_stream::stream;
use clap::ValueEnum;
// use csv_async::AsyncReader;
use futures::{Stream, StreamExt, TryStreamExt};
use opendal::services::Huggingface;
use opendal::services::S3;
use opendal::{Operator, services::Fs};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use tokio::sync::Mutex;
// use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, error::Error};
// use tokio::io::{AsyncBufReadExt, BufReader};
// use tokio::sync::{Mutex, RwLock};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing::instrument;

#[derive(Debug, Clone, ValueEnum)]
pub enum ReaderBackend {
    Fs,
    S3,
    HuggingFace,
}

#[derive(Debug, Clone)]
pub struct ReaderOptions {
    pub format: InputFormat,
    pub backend: ReaderBackend,
    pub root: String,
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    pub s3_endpoint: Option<String>,
    pub huggingface_token: Option<String>,
    pub huggingface_repo_id: Option<String>,
    pub huggingface_revision: Option<String>,
    pub url_column_name: String,
    pub save_additional_columns: bool,
    pub batch_size: usize,
    pub batch_per_shard: usize,
    pub opendal_buffer_size: usize,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum InputFormat {
    Txt,
    Csv,
    Parquet,
}

impl InputFormat {
    pub fn extension(&self) -> &str {
        match self {
            InputFormat::Txt => "txt",
            InputFormat::Csv => "csv",
            InputFormat::Parquet => "parquet",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FileReadState {
    NotStarted,
    Reading,
    Completed,
    Error,
}

#[derive(Debug, Clone)]
pub struct Reader {
    op: Operator,
    options: ReaderOptions,
    read_state: Arc<Mutex<HashMap<String, FileReadState>>>,
}

// type SampleStream =
//     Pin<Box<dyn Stream<Item = Result<InputSample, Box<dyn Error + Send + Sync>>> + Send>>;

impl Reader {
    pub fn new(options: ReaderOptions) -> anyhow::Result<Self> {
        let op = match options.backend {
            ReaderBackend::Fs => {
                let builder = Fs::default().root(&options.root);
                Operator::new(builder)?.finish()
            }
            ReaderBackend::S3 => {
                let mut builder = S3::default().root(&options.root);
                if let Some(bucket) = &options.s3_bucket {
                    builder = builder.bucket(bucket);
                }
                if let Some(region) = &options.s3_region {
                    builder = builder.region(region);
                }
                if let Some(access_key) = &options.s3_access_key {
                    builder = builder.access_key_id(access_key);
                }
                if let Some(secret_key) = &options.s3_secret_key {
                    builder = builder.secret_access_key(secret_key);
                }
                if let Some(endpoint) = &options.s3_endpoint {
                    builder = builder.endpoint(endpoint);
                }
                Operator::new(builder)?.finish()
            }
            ReaderBackend::HuggingFace => {
                let mut builder = Huggingface::default()
                    .root(&options.root)
                    .repo_type("dataset");
                if let Some(token) = &options.huggingface_token {
                    builder = builder.token(token);
                }
                if let Some(repo) = &options.huggingface_repo_id {
                    builder = builder.repo_id(repo);
                }
                if let Some(revision) = &options.huggingface_revision {
                    builder = builder.revision(revision);
                }
                Operator::new(builder)?.finish()
            }
        };

        Ok(Self {
            op,
            options,
            read_state: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn shard_read(
        &self,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<ShardSample>>> {
        let file_lister = self.op.lister_with("").recursive(true).await?;
        let config = self.options.clone();
        let op = self.op.clone();

        let stream_of_streams = stream! {
            let mut file_lister = file_lister;
            while let Some(Ok(file)) = file_lister.next().await {
                if file.name().ends_with(config.format.extension()) {
                    {
                        let filepath = file.path();
                        let mut state = self.read_state.lock().await;
                        if !state.contains_key(filepath) || state.get(filepath) == Some(&FileReadState::NotStarted) {
                            state.insert(filepath.to_string(), FileReadState::Reading);
                        }
                    }

                    let path = file.path().to_string();
                    let reader = op.reader_with(&path.clone()).chunk(self.options.opendal_buffer_size).await?;
                    let batch = match config.format {
                        InputFormat::Txt => {
                            todo!()
                        }
                        InputFormat::Csv => {
                            todo!()
                        }
                        InputFormat::Parquet => {
                            read_parquet_shard(path.clone(), reader, config.clone()).await
                        }
                    };

                    {
                        let mut state = self.read_state.lock().await;
                        match &batch {
                            Ok(_) => {
                                state.insert(path.clone(), FileReadState::Completed);
                            }
                            Err(e) => {
                                tracing::error!("Error reading shard from {}: {}", path, e);
                                state.insert(path.clone(), FileReadState::Error);
                            }
                        }
                    }

                    yield batch;
                }
            }
        };

        let flattened_stream = stream_of_streams.try_flatten();
        Ok(Box::pin(flattened_stream))
    }
}
//     pub async fn stream_samples(&self) -> Result<SampleStream, Box<dyn Error + Send + Sync>> {
//         let file_lister = self.op.lister_with("").recursive(true).await?;
//         let config = self.options.clone();
//         let op = self.op.clone();

//         let stream_of_streams = stream! {
//             let mut file_lister = file_lister;
//             while let Some(file) = file_lister.next().await {
//                 tracing::debug!("Listing file: {:?}", file);
//                 match file {
//                     Ok(file) => {
//                         if file.name().ends_with(config.format.extension()) {
//                             let path = file.path().to_string();
//                             let reader = op.reader_with(&path.clone()).chunk(16 * 1024 * 1024).await?;
//                             let config_clone = config.clone();

//                             let stream_result = match config_clone.format {
//                                 InputFormat::Txt => {
//                                     read_txt_stream(
//                                         path.clone(),
//                                         reader,
//                                         config_clone,
//                                     ).await
//                                 }
//                                 InputFormat::Csv => {
//                                     read_csv_stream(
//                                         path.clone(),
//                                         reader,
//                                         config_clone,
//                                     )
//                                     .await

//                                 }
//                                 InputFormat::Parquet => {
//                                     read_parquet_stream(
//                                         path.clone(),
//                                         reader,
//                                         config_clone,
//                                     )
//                                     .await
//                                 }
//                             };
//                             yield stream_result;
//                         }
//                     }
//                     Err(e) => {
//                         tracing::error!("Error listing file: {}", e);
//                         yield Err(e.into());
//                     }
//                 }
//             }
//         };

//         let flattened_stream = stream_of_streams.try_flatten();
//         Ok(Box::pin(flattened_stream))
//     }

//     pub async fn stream_samples_from(
//         &self,
//         skip_samples: usize,
//     ) -> Result<SampleStream, Box<dyn Error + Send + Sync>> {
//         let base_stream = self.stream_samples().await?;

//         let filtered_stream = base_stream
//             .enumerate()
//             .filter_map(move |(index, sample_result)| {
//                 async move {
//                     if index < skip_samples {
//                         None // Skip this sample
//                     } else {
//                         match sample_result {
//                             Ok(mut sample) => {
//                                 sample.id = index - skip_samples; // Adjust ID
//                                 Some(Ok(sample))
//                             }
//                             Err(e) => Some(Err(e)),
//                         }
//                     }
//                 }
//             });

//         Ok(Box::pin(filtered_stream))
//     }
// }

// async fn read_txt_stream(
//     filepath: String,
//     reader: opendal::Reader,
//     _options: ReaderOptions,
// ) -> Result<SampleStream, Box<dyn Error + Send + Sync>> {
//     let buf_reader = BufReader::new(reader.into_futures_async_read(..).await?.compat());
//     let mut lines = buf_reader.lines();
//     let mut id = 0;
//     let stream = stream! {
//         while let Some(new_line_result) = lines.next_line().await.transpose() {
//             let new_line = new_line_result.unwrap();
//             tracing::debug!("Reading line: {:?}", new_line);
//             // assuming each line is a URL
//             if new_line.trim().is_empty() {
//                 continue; // skip empty lines
//             }
//             let url = new_line.trim().to_string();
//             yield Ok(InputSample {
//                 id: id,
//                 original_filepath: filepath.clone(),
//                 url,
//                 caption: None,
//                 additional_columns: HashMap::new(),
//             });
//             id += 1;
//         }
//     };
//     Ok(Box::pin(stream))
// }

// async fn read_csv_stream(
//     filepath: String,
//     reader: opendal::Reader,
//     options: ReaderOptions,
// ) -> Result<SampleStream, Box<dyn Error + Send + Sync>> {
//     let mut csv_reader =
//         AsyncReader::from_reader(reader.into_futures_async_read(..).await?.compat());
//     let headers = csv_reader.headers().await?.clone();

//     let additional_column_names: HashMap<String, usize> = headers
//         .iter()
//         .enumerate()
//         .filter_map(|(i, h)| {
//             if options.save_additional_columns
//                 && (h != options.input_url_column_name
//                     && (options.input_caption_column_name.is_none()
//                         || h != options.input_caption_column_name.as_deref().unwrap()))
//             {
//                 Some((h.to_string(), i))
//             } else {
//                 None
//             }
//         })
//         .collect();
//     let url_col_idx = headers
//         .iter()
//         .position(|h| h == options.input_url_column_name)
//         .ok_or_else(|| {
//             anyhow!(
//                 "URL column '{}' not found in headers",
//                 options.input_url_column_name
//             )
//         })?;
//     let caption_col_idx = match options.input_caption_column_name.as_deref() {
//         Some(col) => Some(
//             headers
//                 .iter()
//                 .position(|h| h == col)
//                 .ok_or_else(|| anyhow!("Caption column '{}' not found in headers", col))?,
//         ),
//         None => None,
//     };

//     let mut id = 0;
//     let stream = stream! {
//         let mut records = csv_reader.into_records();
//         while let Some(result) = records.next().await {
//             match result {
//                 Ok(record) => {
//                     let url = match record.get(url_col_idx) {
//                         Some(url) => url.to_string(),
//                         None => {
//                             yield Err(anyhow!("Missing URL value in record where headers are: {:?}", headers).into());
//                             continue;
//                         }
//                     };

//                     let caption = caption_col_idx
//                         .and_then(|idx| record.get(idx).map(|s| s.to_string()));

//                     let mut additional_columns = HashMap::new();
//                     for (name, idx) in additional_column_names.iter() {
//                         if let Some(value) = record.get(*idx) {
//                             additional_columns.insert(
//                                 name.clone(),
//                                 InputSampleColumnData::String(
//                                     value.to_string(),
//                                 )
//                             );
//                         }
//                     }

//                     yield Ok(InputSample {
//                         id: id,
//                         original_filepath: filepath.clone(),
//                         url,
//                         caption,
//                         additional_columns,
//                     });
//                     id += 1;
//                 },
//                 Err(e) => {
//                     yield Err(e.into());
//                 }
//             }
//         }
//     };
//     Ok(Box::pin(stream))
// }

// async fn read_parquet_stream(
//     filepath: String,
//     reader: opendal::Reader,
//     options: ReaderOptions,
// ) -> Result<SampleStream, Box<dyn Error + Send + Sync>> {
//     let builder =
//         ParquetRecordBatchStreamBuilder::new(reader.into_futures_async_read(..).await?.compat())
//             .await
//             .map_err(|e| anyhow!("Failed to create Parquet reader: {}", e))?;

//     let schema = builder.schema().clone();
//     let mut col_name_to_idx: HashMap<String, usize> = HashMap::new();
//     for (i, field) in schema.fields().iter().enumerate() {
//         col_name_to_idx.insert(field.name().to_string(), i);
//     }

//     // Get the wanted indices for the columns
//     let mut wanted_indices: Vec<usize> = Vec::new();
//     let mut additional_col_names: Vec<String> = Vec::new();

//     // a. Handle url_col
//     let url_col_idx = *col_name_to_idx
//         .get(&options.input_url_column_name)
//         .ok_or_else(|| {
//             anyhow!(
//                 "URL column '{}' not found in Parquet file",
//                 options.input_url_column_name
//             )
//         })?;
//     if !matches!(
//         schema.field(url_col_idx).data_type(),
//         arrow::datatypes::DataType::Utf8
//     ) {
//         return Err(anyhow!(
//             "URL column '{}' must be of type String",
//             options.input_url_column_name
//         )
//         .into());
//     }
//     wanted_indices.push(url_col_idx);

//     // b. Handle caption_col (optional but required if specified)
//     let caption_col_idx = if let Some(col_name) = options.input_caption_column_name.as_deref() {
//         let idx = *col_name_to_idx
//             .get(col_name)
//             .ok_or_else(|| anyhow!("Caption column '{}' not found in Parquet file", col_name))?;
//         if !matches!(
//             schema.field(idx).data_type(),
//             arrow::datatypes::DataType::Utf8
//         ) {
//             return Err(anyhow!("Caption column '{}' must be of type String", col_name).into());
//         }
//         wanted_indices.push(idx);
//         Some(idx)
//     } else {
//         None
//     };

//     // c. If save_additional_columns is true, collect additional columns
//     if options.save_additional_columns {
//         for (name, &idx) in &col_name_to_idx {
//             if idx != url_col_idx && caption_col_idx.map_or(true, |c_idx| idx != c_idx) {
//                 wanted_indices.push(idx);
//                 additional_col_names.push(name.clone());
//             }
//         }
//     }

//     let projection = ProjectionMask::roots(builder.parquet_schema(), wanted_indices);
//     let mut record_batch_reader = builder.with_projection(projection).build()?;

//     // Find the indices of the URL, caption, and additional columns in the projected schema
//     let projected_schema = record_batch_reader.schema();
//     let url_idx_proj = projected_schema
//         .index_of(&options.input_url_column_name)
//         .map_err(|_| {
//             anyhow!(
//                 "URL column '{}' not found in projected schema",
//                 options.input_url_column_name
//             )
//         })?;
//     let caption_idx_proj = options
//         .input_caption_column_name
//         .as_deref()
//         .and_then(|name| projected_schema.index_of(name).ok());
//     let additional_col_indices: HashMap<String, usize> = additional_col_names
//         .into_iter()
//         .map(|name| {
//             let idx = projected_schema.index_of(&name).unwrap();
//             (name, idx)
//         })
//         .collect();

//     let mut id = 0;
//     let stream = stream! {
//         while let Some(result) = record_batch_reader.next().await {
//             let record_batch = match result {
//                 Ok(batch) => batch,
//                 Err(e) => {
//                     yield Err(Box::new(e) as Box<dyn Error + Send + Sync>);
//                     continue;
//                 }
//             };

//             // prepare the arrays for URL, caption, and additional columns
//             let url_array = match record_batch.column(url_idx_proj).as_any().downcast_ref::<StringArray>() {
//                 Some(arr) => arr,
//                 None => {
//                     yield Err(anyhow!("URL column is not of type String").into());
//                     continue;
//                 }
//             };
//             let caption_array = match caption_idx_proj {
//                 Some(idx) => record_batch.column(idx).as_any().downcast_ref::<StringArray>(),
//                 None => None,
//             };
//             let additional_arrays: Vec<(&str, ArrayRef)> = additional_col_indices.iter().map(|(name, &idx)| {
//                 let array = record_batch.column(idx);
//                 (name.as_str(), array.clone())
//             }).collect();

//             for i in 0..record_batch.num_rows() {
//                 let url = url_array.value(i).to_string();
//                 let caption = caption_array.as_ref().map(|arr| arr.value(i).to_string());

//                 let mut additional_columns = HashMap::new();
//                 for (name, array) in &additional_arrays {
//                     let data = match get_ith_element_from_array(array, i) {
//                         Ok(Some(data)) => data,
//                         Ok(None) => continue, // Skip if the value is None
//                         Err(e) => {
//                             yield Err(e.into());
//                             continue;
//                         }
//                     };

//                     additional_columns.insert(
//                         name.to_string(),
//                         data,
//                     );
//                 }

//                 yield Ok(InputSample {
//                     id: id,
//                     original_filepath: filepath.clone(),
//                     url,
//                     caption,
//                     additional_columns,
//                 });
//                 id += 1;
//             }
//         }
//     };

//     Ok(Box::pin(stream))
// }

#[instrument(skip(reader))]
async fn read_parquet_shard(
    filepath: String,
    reader: opendal::Reader,
    options: ReaderOptions,
) -> anyhow::Result<impl Stream<Item = anyhow::Result<ShardSample>>> {
    let builder =
        ParquetRecordBatchStreamBuilder::new(reader.into_futures_async_read(..).await?.compat())
            .await
            .map_err(|e| anyhow!("Failed to create Parquet reader: {}", e))?;
    let mut record_batch_reader = builder.with_batch_size(options.batch_size).build()?;
    let schema = record_batch_reader.schema();
    let additional_col_names: Vec<String> = if options.save_additional_columns {
        schema
            .fields()
            .iter()
            .filter_map(|field| {
                if *field.name() != options.url_column_name {
                    Some(field.name().to_string())
                } else {
                    None
                }
            })
            .collect()
    } else {
        vec![]
    };

    let stream = stream! {
        let mut shard_sample = ShardSample {
            original_filepath: filepath,
            shard_id: uuid::Uuid::now_v7(),
            samples: Vec::new(),
        };
        while let Some(Ok(batch)) = record_batch_reader.next().await {
            let mut uuid_builder = FixedSizeBinaryBuilder::new(16); // 16 bytes for UUID v7
            for _ in 0..batch.num_rows() {
                uuid_builder.append_value(&uuid::Uuid::now_v7().as_bytes())?;
            }
            let uuid = uuid_builder.finish();
            let url = match batch.column_by_name(&options.url_column_name)
            .expect("URL column should exist")
            .as_any().downcast_ref::<StringArray>() {
                Some(arr) => arr,
                None => {
                    yield Err(anyhow!("URL column is not of type String"));
                    continue;
                }
            };

            let mut additional_columns = HashMap::new();
            for name in &additional_col_names {
                let array = batch.column_by_name(name)
                    .ok_or_else(|| anyhow!("Column '{}' not found in batch", name))?;
                additional_columns.insert(
                    name.clone(),
                    array.clone(),
                );
            }

            if shard_sample.samples.len() >= options.batch_per_shard {
                shard_sample.shard_id = uuid::Uuid::now_v7();
                yield Ok(shard_sample.clone());
                shard_sample.samples.clear();
            } else {
                shard_sample.samples.push(BatchSample {
                    len: batch.num_rows(),
                    uuid: uuid.clone(),
                    url: url.clone(),
                    bytes: None,
                    additional_columns: additional_columns,
                });
            }
        }
        if !shard_sample.samples.is_empty() {
            shard_sample.shard_id = uuid::Uuid::now_v7();
            yield Ok(shard_sample);
        }
    };

    Ok(stream)
}
