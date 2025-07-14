use crate::sampler::{ShardSample, merge_batch_samples};
use anyhow::Result;
use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use clap::ValueEnum;
use opendal::Operator;
use opendal::services::{Fs, S3};
use parquet::arrow::AsyncArrowWriter;
use std::str::FromStr;
use std::sync::Arc;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use tracing::instrument;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    Files,
    Webdataset,
    Parquet,
    // Tfrecord,
    // Dummy,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum OutputBackend {
    Fs,
    S3,
    B2,
}

impl FromStr for OutputFormat {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "files" => Ok(OutputFormat::Files),
            "webdataset" => Ok(OutputFormat::Webdataset),
            "parquet" => Ok(OutputFormat::Parquet),
            _ => Err(anyhow::anyhow!("Unsupported output format: {}", s)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WriterOptions {
    pub root: String,
    pub format: OutputFormat,
    pub backend: OutputBackend,
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    pub s3_endpoint: Option<String>,
    pub b2_bucket: Option<String>,
    pub b2_bucket_id: Option<String>,
    pub b2_application_key_id: Option<String>,
    pub b2_application_key: Option<String>,
    pub shard_prefix: String,
}

#[derive(Debug, Clone)]
pub struct Writer {
    op: Operator,
    options: WriterOptions,
}

impl Writer {
    pub fn new(options: WriterOptions) -> Result<Self> {
        let op = match &options.backend {
            OutputBackend::Fs => {
                let builder = Fs::default().root(&options.root);
                Operator::new(builder)?.finish()
            }
            OutputBackend::S3 => {
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
            OutputBackend::B2 => {
                let mut builder = opendal::services::B2::default().root(&options.root);
                if let Some(bucket) = &options.b2_bucket {
                    builder = builder.bucket(bucket);
                }
                if let Some(bucket_id) = &options.b2_bucket_id {
                    builder = builder.bucket_id(bucket_id);
                }
                if let Some(application_key_id) = &options.b2_application_key_id {
                    builder = builder.application_key_id(application_key_id);
                }
                if let Some(application_key) = &options.b2_application_key {
                    builder = builder.application_key(application_key);
                }
                Operator::new(builder)?.finish()
            }
        };

        Ok(Self { op, options })
    }

    #[instrument]
    pub async fn shard_write(&self, samples: ShardSample) -> Result<()> {
        match self.options.format {
            OutputFormat::Parquet => self.write_shard_parquet(samples).await,
            _ => Err(anyhow::anyhow!(
                "Unsupported output format for shard writing."
            )),
        }
    }

    // pub async fn write_streaming_samples<S>(&self, samples: S, shard_id: &mut usize) -> Result<()>
    // where
    //     S: Stream<Item = OutputSample> + Send + Unpin,
    // {
    //     match self.options.format {
    //         OutputFormat::Files => self.write_streaming_samples_to_files(samples).await,
    //         OutputFormat::Webdataset => {
    //             self.write_streaming_samples_to_tar(samples, *shard_id)
    //                 .await
    //         }
    //         OutputFormat::Parquet => {
    //             // Implement Parquet writing logic here
    //             Err(anyhow::anyhow!(
    //                 "Parquet output format is not yet implemented."
    //             ))
    //         }
    //     }
    // }

    fn get_shard_name(&self, shard_id: uuid::Uuid) -> String {
        let extension = match self.options.format {
            OutputFormat::Files => "jsonl",
            OutputFormat::Webdataset => "tar",
            OutputFormat::Parquet => "parquet",
        };
        format!(
            "{}{}.{}",
            self.options.shard_prefix,
            shard_id.to_string(),
            extension
        )
    }

    pub fn output_format(&self) -> OutputFormat {
        self.options.format
    }

    // pub async fn write_streaming_samples_to_files<S>(&self, samples: S) -> Result<()>
    // where
    //     S: Stream<Item = OutputSample> + Send,
    // {
    //     samples
    //         .map(Ok)
    //         .try_for_each_concurrent(self.options.writer_thread_count, |sample| async move {
    //             match (sample.to_image_file(), sample.to_json_file()) {
    //                 (Ok((image_path, image_data)), Ok((json_path, json_data))) => {
    //                     let image_write = self.op.write(&image_path, image_data);
    //                     let json_write = self.op.write(&json_path, json_data);

    //                     if let Err(e) = futures::try_join!(image_write, json_write) {
    //                         tracing::error!("Failed to write sample files: {}", e);
    //                         return Err(e.into());
    //                     } else {
    //                         tracing::info!(
    //                             "Successfully wrote sample files: {} and {}",
    //                             image_path,
    //                             json_path
    //                         );
    //                         return Ok(());
    //                     }
    //                 }
    //                 _ => {
    //                     let err_msg = "Failed to prepare sample data for writing.";
    //                     tracing::error!("{}", err_msg);
    //                     return Err(anyhow::anyhow!(err_msg));
    //                 }
    //             }
    //         })
    //         .await?;
    //     Ok(())
    // }

    // pub async fn write_streaming_samples_to_tar<S>(
    //     &self,
    //     mut samples: S,
    //     shard_id: usize,
    // ) -> Result<()>
    // where
    //     S: Stream<Item = OutputSample> + Send + Unpin,
    // {
    //     let filepath = self.get_shard_name(shard_id);
    //     let writer = self
    //         .op
    //         .writer_with(&filepath)
    //         .chunk(16 * 1024 * 1024) // 16 MB chunk size
    //         .await?;
    //     let mut tar_writer = async_tar::Builder::new(writer.into_futures_async_write());

    //     while let Some(sample) = samples.next().await {
    //         let (json_path, json_data) = sample.to_json_file()?;
    //         let (image_path, image_data) = sample.to_image_file()?;
    //         let mut json_header = async_tar::Header::new_gnu();
    //         json_header.set_path(json_path.clone())?;
    //         json_header.set_size(json_data.len() as u64);
    //         json_header.set_mode(0o644);
    //         json_header.set_cksum();
    //         let mut image_header = async_tar::Header::new_gnu();
    //         image_header.set_path(image_path.clone())?;
    //         image_header.set_size(image_data.len() as u64);
    //         image_header.set_mode(0o644);
    //         image_header.set_cksum();
    //         tar_writer
    //             .append_data(&mut json_header, json_path, json_data.as_slice())
    //             .await?;
    //         tar_writer
    //             .append_data(&mut image_header, image_path, image_data.as_slice())
    //             .await?;
    //     }

    //     tar_writer.finish().await?;
    //     tar_writer.into_inner().await?.close().await?;

    //     Ok(())
    // }

    pub async fn write_shard_parquet(&self, samples: ShardSample) -> Result<()> {
        let merged_samples = merge_batch_samples(samples.samples)?;
        let mut schema_vec = vec![
            Field::new("_id", DataType::FixedSizeBinary(16), false),
            Field::new("_url", DataType::Utf8, false),
        ];
        let mut array_vec: Vec<Arc<dyn Array>> =
            vec![Arc::new(merged_samples.uuid), Arc::new(merged_samples.url)];
        if let Some(bytes) = merged_samples.bytes {
            schema_vec.push(Field::new("bytes", DataType::Binary, true));
            array_vec.push(Arc::new(bytes));
        }
        for (key, value) in merged_samples.additional_columns.iter() {
            schema_vec.push(Field::new(key, value.data_type().clone(), true));
            array_vec.push(value.clone());
        }
        let schema = Arc::new(Schema::new(schema_vec));
        let batch = RecordBatch::try_new(schema.clone(), array_vec)?;
        let writer = self
            .op
            .writer_with(&self.get_shard_name(samples.shard_id))
            .await?;
        let mut writer = AsyncArrowWriter::try_new(
            writer.into_futures_async_write().compat_write(),
            schema.clone(),
            None,
        )?;
        writer.write(&batch).await?;
        writer.close().await?;

        Ok(())
    }
}
