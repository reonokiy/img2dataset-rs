use crate::sampler::{InputSample, InputSampleColumnData, get_ith_element_from_array};
use anyhow::anyhow;
use arrow::array::{Array, ArrayRef, StringArray};
use async_stream::stream;
use clap::ValueEnum;
use csv_async::AsyncReader;
use futures::{Stream, StreamExt, TryStreamExt};
use opendal::services::Huggingface;
use opendal::services::S3;
use opendal::{Operator, services::Fs};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, error::Error};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::compat::FuturesAsyncReadCompatExt;

#[derive(Debug, Clone, ValueEnum)]
pub enum ReaderBackend {
    Fs,
    S3,
    HuggingFace,
}

#[derive(Debug, Clone)]
pub struct ReaderConfig {
    pub input_url_column_name: String,
    pub input_caption_column_name: Option<String>,
    pub save_additional_columns: bool,
    pub input_format: InputFormat,
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

pub struct Reader {
    op: Arc<Operator>,
    reader_config: ReaderConfig,
}

type SampleStream =
    Pin<Box<dyn Stream<Item = Result<InputSample, Box<dyn Error + Send + Sync>>> + Send>>;

impl Reader {
    pub fn new(
        backend: ReaderBackend,
        base_path: &str,
        reader_config: ReaderConfig,
    ) -> Result<Self, Box<dyn Error>> {
        let op = match backend {
            ReaderBackend::Fs => Operator::new(Fs::default().root(base_path))?.finish(),
            ReaderBackend::S3 => Operator::new(S3::default().root(base_path))?.finish(),
            ReaderBackend::HuggingFace => {
                Operator::new(Huggingface::default().root(base_path))?.finish()
            }
        };

        Ok(Self {
            op: Arc::new(op),
            reader_config,
        })
    }

    pub async fn stream_samples(&self) -> Result<SampleStream, Box<dyn Error + Send + Sync>> {
        let file_lister = self.op.lister_with("").recursive(true).await?;
        let config = self.reader_config.clone();
        let op = self.op.clone();

        let stream_of_streams = stream! {
            let mut file_lister = file_lister;
            while let Some(file) = file_lister.next().await {
                log::debug!("Listing file: {:?}", file);
                match file {
                    Ok(file) => {
                        if file.name().ends_with(config.input_format.extension()) {
                            let path = file.path().to_string();
                            let reader = op.reader_with(&path.clone()).await?;
                            let config_clone = config.clone();

                            let stream_result = match config_clone.input_format {
                                InputFormat::Txt => {
                                    read_txt_stream(
                                        path.clone(),
                                        reader,
                                        config_clone,
                                    ).await
                                }
                                InputFormat::Csv => {
                                    read_csv_stream(
                                        path.clone(),
                                        reader,
                                        config_clone,
                                    )
                                    .await

                                }
                                InputFormat::Parquet => {
                                    read_parquet_stream(
                                        path.clone(),
                                        reader,
                                        config_clone,
                                    )
                                    .await
                                }
                            };
                            yield stream_result;
                        }
                    }
                    Err(e) => {
                        log::error!("Error listing file: {}", e);
                        yield Err(e.into());
                    }
                }
            }
        };

        let flattened_stream = stream_of_streams.try_flatten();
        Ok(Box::pin(flattened_stream))
    }
}

async fn read_txt_stream(
    filepath: String,
    reader: opendal::Reader,
    config: ReaderConfig,
) -> Result<SampleStream, Box<dyn Error + Send + Sync>> {
    let buf_reader = BufReader::new(reader.into_futures_async_read(..).await?.compat());
    let mut lines = buf_reader.lines();
    let mut id = 0;
    let stream = stream! {
        while let Some(new_line_result) = lines.next_line().await.transpose() {
            let new_line = new_line_result.unwrap();
            log::debug!("Reading line: {:?}", new_line);
            // assuming each line is a URL
            if new_line.trim().is_empty() {
                continue; // skip empty lines
            }
            let url = new_line.trim().to_string();
            yield Ok(InputSample {
                id: id,
                original_filepath: filepath.clone(),
                url,
                caption: None,
                additional_columns: HashMap::new(),
            });
            id += 1;
        }
    };
    Ok(Box::pin(stream))
}

async fn read_csv_stream(
    filepath: String,
    reader: opendal::Reader,
    config: ReaderConfig,
) -> Result<SampleStream, Box<dyn Error + Send + Sync>> {
    let mut csv_reader =
        AsyncReader::from_reader(reader.into_futures_async_read(..).await?.compat());
    let headers = csv_reader.headers().await?.clone();

    let additional_column_names: HashMap<String, usize> = headers
        .iter()
        .enumerate()
        .filter_map(|(i, h)| {
            if config.save_additional_columns
                && (h != config.input_url_column_name
                    && (config.input_caption_column_name.is_none()
                        || h != config.input_caption_column_name.as_deref().unwrap()))
            {
                Some((h.to_string(), i))
            } else {
                None
            }
        })
        .collect();
    let url_col_idx = headers
        .iter()
        .position(|h| h == config.input_url_column_name)
        .ok_or_else(|| {
            anyhow!(
                "URL column '{}' not found in headers",
                config.input_url_column_name
            )
        })?;
    let caption_col_idx = match config.input_caption_column_name.as_deref() {
        Some(col) => Some(
            headers
                .iter()
                .position(|h| h == col)
                .ok_or_else(|| anyhow!("Caption column '{}' not found in headers", col))?,
        ),
        None => None,
    };

    let mut id = 0;
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
                                InputSampleColumnData::String(
                                    value.to_string(),
                                )
                            );
                        }
                    }

                    yield Ok(InputSample {
                        id: id,
                        original_filepath: filepath.clone(),
                        url,
                        caption,
                        additional_columns,
                    });
                    id += 1;
                },
                Err(e) => {
                    yield Err(e.into());
                }
            }
        }
    };
    Ok(Box::pin(stream))
}

async fn read_parquet_stream(
    filepath: String,
    reader: opendal::Reader,
    config: ReaderConfig,
) -> Result<SampleStream, Box<dyn Error + Send + Sync>> {
    let builder =
        ParquetRecordBatchStreamBuilder::new(reader.into_futures_async_read(..).await?.compat())
            .await
            .map_err(|e| anyhow!("Failed to create Parquet reader: {}", e))?;

    let schema = builder.schema().clone();
    let mut col_name_to_idx: HashMap<String, usize> = HashMap::new();
    for (i, field) in schema.fields().iter().enumerate() {
        col_name_to_idx.insert(field.name().to_string(), i);
    }

    // Get the wanted indices for the columns
    let mut wanted_indices: Vec<usize> = Vec::new();
    let mut additional_col_names: Vec<String> = Vec::new();

    // a. Handle url_col
    let url_col_idx = *col_name_to_idx
        .get(&config.input_url_column_name)
        .ok_or_else(|| {
            anyhow!(
                "URL column '{}' not found in Parquet file",
                config.input_url_column_name
            )
        })?;
    if !matches!(
        schema.field(url_col_idx).data_type(),
        arrow::datatypes::DataType::Utf8
    ) {
        return Err(anyhow!(
            "URL column '{}' must be of type String",
            config.input_url_column_name
        )
        .into());
    }
    wanted_indices.push(url_col_idx);

    // b. Handle caption_col (optional but required if specified)
    let caption_col_idx = if let Some(col_name) = config.input_caption_column_name.as_deref() {
        let idx = *col_name_to_idx
            .get(col_name)
            .ok_or_else(|| anyhow!("Caption column '{}' not found in Parquet file", col_name))?;
        if !matches!(
            schema.field(idx).data_type(),
            arrow::datatypes::DataType::Utf8
        ) {
            return Err(anyhow!("Caption column '{}' must be of type String", col_name).into());
        }
        wanted_indices.push(idx);
        Some(idx)
    } else {
        None
    };

    // c. If save_additional_columns is true, collect additional columns
    if config.save_additional_columns {
        for (name, &idx) in &col_name_to_idx {
            if idx != url_col_idx && caption_col_idx.map_or(true, |c_idx| idx != c_idx) {
                wanted_indices.push(idx);
                additional_col_names.push(name.clone());
            }
        }
    }

    let projection = ProjectionMask::roots(builder.parquet_schema(), wanted_indices);
    let mut record_batch_reader = builder.with_projection(projection).build()?;

    // Find the indices of the URL, caption, and additional columns in the projected schema
    let projected_schema = record_batch_reader.schema();
    let url_idx_proj = projected_schema
        .index_of(&config.input_url_column_name)
        .map_err(|_| {
            anyhow!(
                "URL column '{}' not found in projected schema",
                config.input_url_column_name
            )
        })?;
    let caption_idx_proj = config
        .input_caption_column_name
        .as_deref()
        .and_then(|name| projected_schema.index_of(name).ok());
    let additional_col_indices: HashMap<String, usize> = additional_col_names
        .into_iter()
        .map(|name| {
            let idx = projected_schema.index_of(&name).unwrap();
            (name, idx)
        })
        .collect();

    let mut id = 0;
    let stream = stream! {
        while let Some(result) = record_batch_reader.next().await {
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
            let additional_arrays: Vec<(&str, ArrayRef)> = additional_col_indices.iter().map(|(name, &idx)| {
                let array = record_batch.column(idx);
                (name.as_str(), array.clone())
            }).collect();

            for i in 0..record_batch.num_rows() {
                let url = url_array.value(i).to_string();
                let caption = caption_array.as_ref().map(|arr| arr.value(i).to_string());

                let mut additional_columns = HashMap::new();
                for (name, array) in &additional_arrays {
                    let data = match get_ith_element_from_array(array, i) {
                        Ok(Some(data)) => data,
                        Ok(None) => continue, // Skip if the value is None
                        Err(e) => {
                            yield Err(e.into());
                            continue;
                        }
                    };

                    additional_columns.insert(
                        name.to_string(),
                        data,
                    );
                }

                yield Ok(InputSample {
                    id: id,
                    original_filepath: filepath.clone(),
                    url,
                    caption,
                    additional_columns,
                });
                id += 1;
            }
        }
    };

    Ok(Box::pin(stream))
}
