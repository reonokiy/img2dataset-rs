use crate::sampler::{BatchSample, ShardSample};
use anyhow::anyhow;
use arrow::array::FixedSizeBinaryBuilder;
use arrow::array::StringArray;
use async_stream::stream;
use clap::ValueEnum;
use futures::{Stream, StreamExt, TryStreamExt};
use opendal::services::Huggingface;
use opendal::services::S3;
use opendal::{Operator, services::Fs};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
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
