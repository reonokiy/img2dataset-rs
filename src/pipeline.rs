use anyhow::Result;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::time::Instant;

use futures::stream::StreamExt;

use crate::{
    downloader::{Downloader, DownloaderOptions},
    reader::{Reader, ReaderOptions},
    resizer::{Resizer, ResizerOptions},
    writer::{Writer, WriterOptions},
};

pub struct PipelineOptions {
    pub reader_options: ReaderOptions,
    pub downloader_options: DownloaderOptions,
    pub resizer_options: ResizerOptions,
    pub writer_options: WriterOptions,
    pub concurrency: usize,
}

#[derive(Debug, Clone)]
pub struct PipelineStatus {
    pub read_shards: Arc<AtomicU64>,
    pub downloaded_shards: Arc<AtomicU64>,
    pub error_shards: Arc<AtomicU64>,
    pub read_samples: Arc<AtomicU64>,
    pub downloaded_samples: Arc<AtomicU64>,
    pub written_samples: Arc<AtomicU64>,
}

impl PipelineStatus {
    pub fn new() -> Self {
        Self {
            read_shards: Arc::new(AtomicU64::new(0)),
            downloaded_shards: Arc::new(AtomicU64::new(0)),
            error_shards: Arc::new(AtomicU64::new(0)),
            read_samples: Arc::new(AtomicU64::new(0)),
            downloaded_samples: Arc::new(AtomicU64::new(0)),
            written_samples: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn increment_read_shards(&mut self) {
        self.read_shards.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_downloaded_shards(&mut self) {
        self.downloaded_shards.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_error_shards(&mut self) {
        self.error_shards.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_read_samples(&mut self) {
        self.read_samples.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_downloaded_samples(&mut self) {
        self.downloaded_samples.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_written_samples(&mut self) {
        self.written_samples.fetch_add(1, Ordering::Relaxed);
    }
}

pub async fn pipeline(options: PipelineOptions) -> Result<()> {
    let status = PipelineStatus::new();
    let reader = Reader::new(options.reader_options)?;
    let downloader = Arc::new(Downloader::new(options.downloader_options));
    let resizer = Arc::new(Resizer::new(options.resizer_options));
    let writer = Arc::new(Writer::new(options.writer_options)?);
    let shards = reader.shard_read().await?;
    shards
        .for_each_concurrent(options.concurrency, |shard| {
            let mut status = status.clone();
            let downloader = downloader.clone();
            let resizer = resizer.clone();
            let writer = writer.clone();

            async move {
                let start_time = Instant::now();
                let read_shard = match shard {
                    Ok(shard) => {
                        tracing::info!("Shard {} read successfully", shard.shard_id);
                        shard
                    }
                    Err(e) => {
                        tracing::error!("Error reading shard: {}", e);
                        return;
                    }
                };

                let downloaded_shard = downloader.shard_download(read_shard.clone()).await;
                tracing::info!("Shard {} downloaded successfully", read_shard.shard_id);
                let downloaded_count = downloaded_shard.get_valid_bytes_count();
                let all_count = read_shard.len();
                tracing::info!(
                    "Shard {} downloaded {} samples, all {} samples, success rate: {:.2}%",
                    read_shard.shard_id,
                    downloaded_count,
                    all_count,
                    downloaded_count as f64 / all_count as f64 * 100.0
                );
                let resized_shard = resizer.shard_resize(downloaded_shard.clone()).await;
                let write_result = match resized_shard {
                    Ok(resized_shard) => {
                        tracing::info!("Shard {} resized successfully", resized_shard.shard_id);
                        writer.shard_write(resized_shard).await
                    }
                    Err(e) => {
                        tracing::error!("Error resizing shard: {}", e);
                        writer.shard_write(downloaded_shard).await
                    }
                };
                let end_time = Instant::now();
                let duration = end_time.duration_since(start_time);
                let process_rate = read_shard.len() as f64 / duration.as_secs_f64();
                tracing::info!(
                    "Shard {} finished in {:?}, process rate: {:.2} samples/s",
                    read_shard.shard_id,
                    duration,
                    process_rate
                );
                match write_result {
                    Ok(_) => {
                        tracing::info!("Shard {} written successfully", read_shard.shard_id);
                        status.increment_written_samples()
                    }
                    Err(e) => {
                        tracing::error!("Error writing shard: {}", e);
                        status.increment_error_shards();
                    }
                }
            }
        })
        .await;

    Ok(())
}
