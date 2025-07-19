use anyhow::Result;
use std::sync::Arc;
use tokio::{sync::mpsc, task::JoinSet, time::Instant};
use tokio_stream::wrappers::ReceiverStream;

use futures::stream::StreamExt;

use crate::{
    downloader::{Downloader, DownloaderOptions},
    reader::{Reader, ReaderOptions},
    resizer::{Resizer, ResizerOptions},
    sampler::BatchSample,
    writer::{Writer, WriterOptions},
};

pub struct PipelineOptions {
    pub reader_options: ReaderOptions,
    pub downloader_options: DownloaderOptions,
    pub resizer_options: ResizerOptions,
    pub writer_options: WriterOptions,
    pub downloader_concurrency: usize,
    pub resizer_concurrency: usize,
    pub writer_concurrency: usize,
    pub reader_buffer_size: usize,
    pub downloader_buffer_size: usize,
    pub resizer_buffer_size: usize,
}

pub async fn pipeline(options: PipelineOptions) -> Result<()> {
    let reader = Reader::new(options.reader_options)?;
    let downloader = Arc::new(Downloader::new(options.downloader_options));
    let resizer = Arc::new(Resizer::new(options.resizer_options));
    let writer = Arc::new(Writer::new(options.writer_options)?);

    let (tx_read, rx_read) = mpsc::channel::<BatchSample>(options.reader_buffer_size);
    let (tx_download, rx_download) = mpsc::channel::<BatchSample>(options.downloader_buffer_size);
    let (tx_resize, rx_resize) = mpsc::channel::<BatchSample>(options.resizer_buffer_size);

    let mut tasks = JoinSet::new();

    // Throughput monitoring counters
    let writer_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let resizer_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let downloader_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let reader_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Download success/failure counters
    let download_success_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let download_failure_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Start throughput monitoring task
    let writer_counter_monitor = writer_counter.clone();
    let resizer_counter_monitor = resizer_counter.clone();
    let downloader_counter_monitor = downloader_counter.clone();
    let reader_counter_monitor = reader_counter.clone();
    let download_success_monitor = download_success_counter.clone();
    let download_failure_monitor = download_failure_counter.clone();

    tasks.spawn(async move {
        let start_time = Instant::now();
        let mut last_log_time = start_time;
        let mut last_reader_count = 0u64;
        let mut last_downloader_count = 0u64;
        let mut last_resizer_count = 0u64;
        let mut last_writer_count = 0u64;

        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

            let now = Instant::now();
            let total_elapsed = now.duration_since(start_time).as_secs_f64();
            let interval_elapsed = now.duration_since(last_log_time).as_secs_f64();

            let reader_count = reader_counter_monitor.load(std::sync::atomic::Ordering::Relaxed);
            let downloader_count =
                downloader_counter_monitor.load(std::sync::atomic::Ordering::Relaxed);
            let resizer_count = resizer_counter_monitor.load(std::sync::atomic::Ordering::Relaxed);
            let writer_count = writer_counter_monitor.load(std::sync::atomic::Ordering::Relaxed);

            // Download success rate statistics
            let download_success_count =
                download_success_monitor.load(std::sync::atomic::Ordering::Relaxed);
            let download_failure_count =
                download_failure_monitor.load(std::sync::atomic::Ordering::Relaxed);
            let download_total = download_success_count + download_failure_count;
            let download_success_rate = if download_total > 0 {
                (download_success_count as f64 / download_total as f64) * 100.0
            } else {
                0.0
            };

            // Calculate total throughput rates
            let reader_throughput = reader_count as f64 / total_elapsed;
            let downloader_throughput = downloader_count as f64 / total_elapsed;
            let resizer_throughput = resizer_count as f64 / total_elapsed;
            let writer_throughput = writer_count as f64 / total_elapsed;

            // Calculate interval throughput rates (speed in this period)
            let reader_interval_count = reader_count - last_reader_count;
            let downloader_interval_count = downloader_count - last_downloader_count;
            let resizer_interval_count = resizer_count - last_resizer_count;
            let writer_interval_count = writer_count - last_writer_count;

            let reader_interval_speed = reader_interval_count as f64 / interval_elapsed;
            let downloader_interval_speed = downloader_interval_count as f64 / interval_elapsed;
            let resizer_interval_speed = resizer_interval_count as f64 / interval_elapsed;
            let writer_interval_speed = writer_interval_count as f64 / interval_elapsed;

            // Log each component's throughput
            tracing::info!(
                "Reader - Total: {} batches ({:.2}/s) | Interval: {} batches ({:.2}/s)",
                reader_count,
                reader_throughput,
                reader_interval_count,
                reader_interval_speed
            );

            tracing::info!(
                "Downloader - Total: {} batches ({:.2}/s) | Interval: {} batches ({:.2}/s) | Success Rate: {:.1}% ({}/{})",
                downloader_count,
                downloader_throughput,
                downloader_interval_count,
                downloader_interval_speed,
                download_success_rate,
                download_success_count,
                download_total
            );

            tracing::info!(
                "Resizer - Total: {} batches ({:.2}/s) | Interval: {} batches ({:.2}/s)",
                resizer_count,
                resizer_throughput,
                resizer_interval_count,
                resizer_interval_speed
            );

            tracing::info!(
                "Writer - Total: {} batches ({:.2}/s) | Interval: {} batches ({:.2}/s)",
                writer_count,
                writer_throughput,
                writer_interval_count,
                writer_interval_speed
            );

            // Update last counts for next interval
            last_reader_count = reader_count;
            last_downloader_count = downloader_count;
            last_resizer_count = resizer_count;
            last_writer_count = writer_count;

            last_log_time = now;
        }
    });

    let writer_handle = writer.clone();
    let writer_counter_clone = writer_counter.clone();
    tasks.spawn(async move {
        tracing::info!("Writer task group started.");

        let stream = ReceiverStream::new(rx_resize);
        stream
            .for_each_concurrent(options.writer_concurrency, |resized_shard| {
                let value = writer_handle.clone();
                let counter = writer_counter_clone.clone();
                async move {
                    if let Err(e) = value.batch_write(resized_shard).await {
                        tracing::error!("Error writing shard: {}", e);
                    } else {
                        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            })
            .await;
        tracing::info!("Writer task group finished.");
    });

    let resizer_handle = resizer.clone();
    let tx_resize_clone = tx_resize.clone();
    let resizer_counter_clone = resizer_counter.clone();
    tasks.spawn(async move {
        tracing::info!("Resizer task group started.");

        let stream = ReceiverStream::new(rx_download);
        stream
            .for_each_concurrent(options.resizer_concurrency, |downloaded_shard| {
                let resizer = resizer_handle.clone();
                let tx_resize = tx_resize_clone.clone();
                let counter = resizer_counter_clone.clone();

                async move {
                    let resized_shard_result = resizer.batch_resize(downloaded_shard.clone()).await;
                    let shard_to_write = match resized_shard_result {
                        Ok(resized) => {
                            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            resized
                        }
                        Err(e) => {
                            tracing::error!("Error resizing shard, writing original: {}", e);
                            downloaded_shard
                        }
                    };
                    if tx_resize.send(shard_to_write).await.is_err() {
                        tracing::warn!("Writer channel closed, could not send resized shard.");
                    }
                }
            })
            .await;

        tracing::info!("Resizer task group finished.");
    });
    drop(tx_resize);

    let downloader_handle = downloader.clone();
    let tx_download_clone = tx_download.clone();
    let downloader_counter_clone = downloader_counter.clone();
    let download_success_counter_clone = download_success_counter.clone();
    let download_failure_counter_clone = download_failure_counter.clone();
    tasks.spawn(async move {
        tracing::info!("Downloader task group started.");
        let stream = ReceiverStream::new(rx_read);
        stream
            .for_each_concurrent(options.downloader_concurrency, |read_shard| {
                let downloader = downloader_handle.clone();
                let tx_download = tx_download_clone.clone();
                let counter = downloader_counter_clone.clone();
                let success_counter = download_success_counter_clone.clone();
                let failure_counter = download_failure_counter_clone.clone();

                async move {
                    let downloaded = downloader.batch_download(read_shard).await;
                    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    let success_count = downloaded.get_valid_bytes_count() as u64;
                    let failure_count = downloaded.len() as u64 - success_count;
                    success_counter.fetch_add(success_count, std::sync::atomic::Ordering::Relaxed);
                    failure_counter.fetch_add(failure_count, std::sync::atomic::Ordering::Relaxed);

                    if tx_download.send(downloaded).await.is_err() {
                        tracing::warn!("Resize channel closed, could not send downloaded shard.");
                    }
                }
            })
            .await;

        tracing::info!("Downloader task group finished.");
    });
    drop(tx_download);

    let reader_counter_clone = reader_counter.clone();
    tasks.spawn(async move {
        tracing::info!("Reader task started.");

        let shards_stream = match reader.batch_read().await {
            Ok(stream) => stream,
            Err(e) => {
                tracing::error!("Failed to initialize shard stream: {}", e);
                return;
            }
        };

        shards_stream
            .for_each(|shard_result| {
                let tx_read = tx_read.clone();
                let counter = reader_counter_clone.clone();
                async move {
                    match shard_result {
                        Ok(shard) => {
                            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if tx_read.send(shard).await.is_err() {
                                tracing::warn!(
                                    "Downstream channel closed. Reader is stopping early."
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to read a specific shard, skipping it: {}", e);
                        }
                    }
                }
            })
            .await;

        tracing::info!("Reader task finished: all shards have been sent to the pipeline.");
    });

    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res {
            tracing::error!("A task panicked: {}", e);
        }
    }

    // Final throughput summary
    let final_reader_count = reader_counter.load(std::sync::atomic::Ordering::Relaxed);
    let final_downloader_count = downloader_counter.load(std::sync::atomic::Ordering::Relaxed);
    let final_resizer_count = resizer_counter.load(std::sync::atomic::Ordering::Relaxed);
    let final_writer_count = writer_counter.load(std::sync::atomic::Ordering::Relaxed);

    // Final download success rate
    let final_download_success_count =
        download_success_counter.load(std::sync::atomic::Ordering::Relaxed);
    let final_download_failure_count =
        download_failure_counter.load(std::sync::atomic::Ordering::Relaxed);
    let final_download_total = final_download_success_count + final_download_failure_count;
    let final_download_success_rate = if final_download_total > 0 {
        (final_download_success_count as f64 / final_download_total as f64) * 100.0
    } else {
        0.0
    };

    tracing::info!("Pipeline finished successfully.");
    tracing::info!(
        "Pipeline Final Summary - Reader: {} batches, Downloader: {} batches, Resizer: {} batches, Writer: {} batches | Download Success Rate: {:.1}% ({}/{})",
        final_reader_count,
        final_downloader_count,
        final_resizer_count,
        final_writer_count,
        final_download_success_rate,
        final_download_success_count,
        final_download_total
    );

    Ok(())
}
