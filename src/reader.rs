use anyhow::anyhow;
use async_stream::stream;
use bytes::{Bytes, buf};
use csv_async::{AsyncReader, AsyncReaderBuilder};
use futures::{Stream, StreamExt};
use opendal::{Operator, services::Fs};
use parquet::{
    arrow::arrow_reader::{self, ParquetRecordBatchStreamBuilder},
    file::{reader::FileReader, serialized_reader::SerializedFileReader},
};
use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Debug, Clone)]
pub struct ColumnMap {
    pub input_url_col: String,
    pub input_caption_col: Option<String>,
    pub input_additional_columns: Vec<String>,
    pub output_additional_columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Sample {
    pub url: String,
    pub caption: Option<String>,
    pub additional_columns: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum InputFormat {
    Txt,
    Csv,
    Tsv,
    Parquet,
}

pub struct Reader {
    url_list: String,
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
        url_list: &str,
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
            url_list: url_list.to_string(),
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
        let url_list = self.url_list.clone();

        let stream = match self.input_format {
            InputFormat::Txt => {
                let stream = read_txt_stream(op, url_list).await?;
                Box::pin(stream)
            }
            InputFormat::Csv | InputFormat::Tsv => {
                let delimiter = if let InputFormat::Csv = self.input_format {
                    b','
                } else {
                    b'\t'
                };
                let stream = read_csv_stream(
                    op,
                    url_list,
                    delimiter,
                    self.url_col.clone(),
                    self.caption_col.clone(),
                    self.save_additional_columns,
                )
                .await?;
                Box::pin(stream)
            }
            InputFormat::Parquet => {
                let samples =
                    read_parquet(op, &url_list, &self.url_col, self.caption_col.as_deref()).await?;
                Box::pin(futures::stream::iter(samples.into_iter().map(Ok)))
            }
        };
        Ok(stream)
    }
}

async fn read_txt_stream(
    op: Arc<Operator>,
    url_list: String,
) -> Result<
    impl Stream<Item = Result<Sample, Box<dyn Error + Send + Sync>>>,
    Box<dyn Error + Send + Sync>,
> {
    let reader = op.reader(&url_list).await?;
    let buf_reader = BufReader::new(reader);
    let mut lines = buf_reader.lines();
    let stream = stream! {
        while let Some(new_line) = lines.next_line().await? {
            let url = new_line.trim().to_string();
            yield Ok(Sample {
                url,
                caption: None,
                additional_columns: Vec::new(), // TODO: implement this
            });
        }
    };
    Ok(stream)
}

async fn read_csv_stream(
    op: Arc<Operator>,
    url_list: String,
    delimiter: u8,
    url_col: String,
    caption_col: Option<String>,
    save_additional_columns: bool,
) -> Result<
    impl Stream<Item = Result<Sample, Box<dyn Error + Send + Sync>>>,
    Box<dyn Error + Send + Sync>,
> {
    let reader = op.reader(&url_list).await?;
    let mut csv_reader = AsyncReader::from_reader(reader);

    let headers = csv_reader.headers().await?.clone();
    let url_col_idx = headers
        .iter()
        .position(|h| h == url_col)
        .ok_or_else(|| anyhow!("URL column '{}' not found", url_col))?;
    let caption_col_idx = caption_col
        .as_ref()
        .and_then(|col| headers.iter().position(|h| h == col));
    let additional_col_indices = if save_additional_columns {
        headers
            .iter()
            .enumerate()
            .filter_map(|(i, h)| {
                if i != url_col_idx && caption_col_idx.map_or(true, |c| c != i) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
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

                    let additional_columns = additional_col_indices
                        .iter()
                        .filter_map(|&i| record.get(i).map(|s| s.to_string()))
                        .collect();

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

async fn read_parquet(
    op: Arc<Operator>,
    url_list: &str,
    url_col: &str,
    caption_col: Option<&str>,
) -> Result<Vec<Sample>, Box<dyn Error + Send + Sync>> {
    let reader = op.reader(url_list).await?;
    let buf_reader = BufReader::new(reader);
    let builder = ParquetRecordBatchStreamBuilder::new(buf_reader);
    let arrow_reader = builder.build().await?;
    let mut samples = Vec::new();
    for record_batch in arrow_reader {
        let record_batch = record_batch?;
        let url_array = record_batch
            .column_by_name(url_col)
            .ok_or("URL column not found")?;
        let url_array = url_array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or("URL column is not a string array")?;

        let caption_array = caption_col.and_then(|name| record_batch.column_by_name(name));
        let caption_array =
            caption_array.and_then(|arr| arr.as_any().downcast_ref::<arrow::array::StringArray>());

        for i in 0..record_batch.num_rows() {
            let url = url_array.value(i).to_string();
            let caption = caption_array.map(|arr| arr.value(i).to_string());
            samples.push(Sample {
                url,
                caption,
                additional_columns: Vec::new(), // TODO: implement this
            });
        }
    }
    Ok(samples)
}
