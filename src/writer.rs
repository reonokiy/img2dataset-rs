use crate::sampler::BatchSample;
use anyhow::Result;
use clap::ValueEnum;
use opendal::Operator;
use opendal::services::{B2, Fs, S3};
use parquet::arrow::AsyncArrowWriter;
use std::str::FromStr;
use tokio_util::compat::FuturesAsyncWriteCompatExt;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    Files,
    Webdataset,
    Parquet,
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
    pub writer_buffer: usize,
}

impl WriterOptions {
    pub fn get_operator(&self) -> Result<Operator> {
        let op = match &self.backend {
            OutputBackend::Fs => {
                let builder = Fs::default().root(&self.root);
                Operator::new(builder)?.finish()
            }
            OutputBackend::S3 => {
                let mut builder = S3::default().root(&self.root);
                if let Some(bucket) = &self.s3_bucket {
                    builder = builder.bucket(bucket);
                }
                if let Some(region) = &self.s3_region {
                    builder = builder.region(region);
                }
                if let Some(access_key) = &self.s3_access_key {
                    builder = builder.access_key_id(access_key);
                }
                if let Some(secret_key) = &self.s3_secret_key {
                    builder = builder.secret_access_key(secret_key);
                }
                if let Some(endpoint) = &self.s3_endpoint {
                    builder = builder.endpoint(endpoint);
                }
                Operator::new(builder)?.finish()
            }
            OutputBackend::B2 => {
                let mut builder = B2::default().root(&self.root);
                if let Some(bucket) = &self.b2_bucket {
                    builder = builder.bucket(bucket);
                }
                if let Some(bucket_id) = &self.b2_bucket_id {
                    builder = builder.bucket_id(bucket_id);
                }
                if let Some(application_key_id) = &self.b2_application_key_id {
                    builder = builder.application_key_id(application_key_id);
                }
                if let Some(application_key) = &self.b2_application_key {
                    builder = builder.application_key(application_key);
                }
                Operator::new(builder)?.finish()
            }
        };
        Ok(op)
    }
}

#[derive(Debug, Clone)]
pub struct Writer {
    op: Operator,
    options: WriterOptions,
}

impl Writer {
    pub fn new(options: WriterOptions) -> Result<Self> {
        let op = options.get_operator()?;
        Ok(Self { op, options })
    }

    pub async fn batch_write(&self, samples: BatchSample) -> Result<()> {
        match self.options.format {
            OutputFormat::Parquet => self.write_shard_parquet(samples).await,
            _ => todo!(),
        }
    }

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

    pub async fn write_shard_parquet(&self, samples: BatchSample) -> Result<()> {
        let schema = samples.schema();
        let batch = samples.batch()?;
        let writer = self
            .op
            .writer_with(&self.get_shard_name(samples.id()))
            .chunk(self.options.writer_buffer)
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
