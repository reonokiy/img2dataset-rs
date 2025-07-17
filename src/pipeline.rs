use anyhow::Result;
use std::sync::Arc;
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

pub async fn pipeline(options: PipelineOptions) -> Result<()> {
    let reader = Reader::new(options.reader_options)?;
    let downloader = Arc::new(Downloader::new(options.downloader_options));
    let resizer = Arc::new(Resizer::new(options.resizer_options));
    let writer = Arc::new(Writer::new(options.writer_options)?);
    let shards = reader.shard_read().await?;
    shards
        .for_each_concurrent(options.concurrency, |shard| {
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
                    }
                    Err(e) => {
                        tracing::error!("Error writing shard: {}", e);
                    }
                }
            }
        })
        .await;

    Ok(())
}
