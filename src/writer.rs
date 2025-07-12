use crate::sampler::OutputSample;
use crate::state::State;
use anyhow::Result;
use clap::ValueEnum;
use futures::{AsyncWriteExt, Stream, StreamExt, TryStreamExt};
use opendal::Operator;
use opendal::services::{Fs, S3};
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;

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
    pub base_path: String,
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    pub s3_endpoint: Option<String>,
    pub output_backend: OutputBackend,
    pub output_format: OutputFormat,
    pub write_concurrent_limit: usize,
    pub number_sample_per_shard: usize,
    pub webdataset_shard_bits_num: usize,
    pub webdataset_shard_prefix: String,
}

/// The `Writer` is responsible for writing samples to the output destination.
#[derive(Debug, Clone)]
pub struct Writer {
    op: Operator,
    output_format: OutputFormat,
    write_concurrent_limit: usize,
    // for webdataset
    webdataset_shard_bits_num: usize,
    webdataset_shard_prefix: String,
}

static WRITER_SHARD_COUNTER: AtomicUsize = AtomicUsize::new(0);

impl Writer {
    pub fn new(options: WriterOptions) -> Result<Self> {
        let op = match options.output_backend {
            OutputBackend::Fs => {
                let builder = Fs::default().root(&options.base_path);
                Operator::new(builder)?.finish()
            }
            OutputBackend::S3 => {
                let mut builder = S3::default().root(&options.base_path);
                if let Some(bucket) = options.s3_bucket {
                    builder = builder.bucket(&bucket);
                }
                if let Some(region) = options.s3_region {
                    builder = builder.region(&region);
                }
                if let Some(access_key) = options.s3_access_key {
                    builder = builder.access_key_id(&access_key);
                }
                if let Some(secret_key) = options.s3_secret_key {
                    builder = builder.secret_access_key(&secret_key);
                }
                if let Some(endpoint) = options.s3_endpoint {
                    builder = builder.endpoint(&endpoint);
                }
                Operator::new(builder)?.finish()
            }
        };

        Ok(Self {
            op,
            output_format: options.output_format,
            write_concurrent_limit: options.write_concurrent_limit,
            webdataset_shard_bits_num: options.webdataset_shard_bits_num,
            webdataset_shard_prefix: options.webdataset_shard_prefix,
        })
    }

    pub fn output_format(&self) -> OutputFormat {
        self.output_format
    }

    pub async fn write_streaming_samples_to_files<S>(&self, samples: S, state: &State) -> Result<()>
    where
        S: Stream<Item = OutputSample> + Send,
    {
        samples
            .map(Ok)
            .try_for_each_concurrent(self.write_concurrent_limit, |sample| {
                let state = state;
                async move {
                    match (sample.to_image_file(), sample.to_json_file()) {
                        (Ok((image_path, image_data)), Ok((json_path, json_data))) => {
                            let image_write = self.op.write(&image_path, image_data);
                            let json_write = self.op.write(&json_path, json_data);

                            if let Err(e) = futures::try_join!(image_write, json_write) {
                                state.set_download_state(&sample.url, false).await?;
                                log::error!("Failed to write sample files: {}", e);
                                return Err(e.into());
                            } else {
                                state.set_download_state(&sample.url, true).await?;
                            }
                        }
                        _ => {
                            state.set_download_state(&sample.url, false).await?;
                            let err_msg = "Failed to prepare sample data for writing.";
                            log::error!("{}", err_msg);
                            return Err(anyhow::anyhow!(err_msg));
                        }
                    }
                    Ok(())
                }
            })
            .await?;
        Ok(())
    }

    fn construct_webdataset_shard_name(&self) -> String {
        let shard_id = WRITER_SHARD_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        format!(
            "{}-{:0>5}.tar",
            self.webdataset_shard_prefix,
            shard_id & ((1 << self.webdataset_shard_bits_num) - 1)
        )
    }

    pub async fn write_streaming_samples_to_tar<S>(
        &self,
        mut samples: S,
        state: &State,
    ) -> Result<()>
    where
        S: Stream<Item = OutputSample> + Send + Unpin,
    {
        let filepath = self.construct_webdataset_shard_name();
        let writer = self
            .op
            .writer_with(&filepath)
            .chunk(16 * 1024 * 1024) // 16 MB chunk size
            .await?;
        let mut tar_writer = async_tar::Builder::new(writer.into_futures_async_write());

        while let Some(sample) = samples.next().await {
            state.set_download_state(&sample.url, false).await?;
            let (image_path, image_data) = sample.to_image_file()?;
            let (json_path, json_data) = sample.to_json_file()?;
            tar_writer
                .append_data(
                    &mut async_tar::Header::new_gnu(),
                    image_path,
                    image_data.as_slice(),
                )
                .await?;
            tar_writer
                .append_data(
                    &mut async_tar::Header::new_gnu(),
                    json_path,
                    json_data.as_slice(),
                )
                .await?;
            state.set_download_state(&sample.url, true).await?;
        }

        tar_writer.finish().await?;
        tar_writer.into_inner().await?.close().await?;

        Ok(())
    }
}
