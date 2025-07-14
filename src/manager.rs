//! Dynamic Image Processing Pipeline Manager
//!
//! This module provides a comprehensive pipeline management system for image processing
//! using a producer-consumer architecture with dynamic worker scaling.
//!
//! ## Architecture
//!
//! The pipeline consists of four main components in sequence:
//! - **Reader**: Reads data batches from input sources (Parquet files, etc.)
//! - **DownloaderManager**: Manages a dynamic pool of image downloading workers
//! - **ResizerManager**: Manages a dynamic pool of image resizing workers
//! - **WriterManager**: Manages a dynamic pool of output writing workers
//!
//! ## Dynamic Scaling
//!
//! Workers are automatically scaled based on:
//! - Queue load (scale up when queues are full, scale down when empty)
//! - System memory usage (scale down when memory is constrained)
//! - Configurable min/max worker limits
//!
//! ## Usage
//!
//! ```rust
//! let config = PipelineConfig::new(reader_options, resizer_options, writer_options);
//! let manager = PipelineManager::new(config);
//! manager.run().await?;
//! ```

use anyhow::Result;
use futures::StreamExt;
use std::sync::Arc;
use sysinfo::System;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};

use crate::downloader::{Downloader, DownloaderOptions};
use crate::reader::{Reader, ReaderOptions};
use crate::resizer::{Resizer, ResizerOptions};
use crate::sampler::ShardSample;
use crate::writer::{Writer, WriterOptions};

/// Pipeline manager configuration
#[derive(Debug, Clone)]
pub struct PipelineOptions {
    pub reader_options: ReaderOptions,
    pub downloader_options: DownloaderOptions,
    pub resizer_options: ResizerOptions,
    pub writer_options: WriterOptions,
    pub buffer_size: usize,
    pub min_downloaders: usize,
    pub max_downloaders: usize,
    pub min_resizers: usize,
    pub max_resizers: usize,
    pub min_writers: usize,
    pub max_writers: usize,
    pub memory_threshold: f32,
    pub scale_up_threshold: f32,
    pub scale_down_threshold: f32,
    pub manager_check_interval_ms: u64,
}

/// System resource monitoring utility
///
/// Provides methods to monitor system memory and CPU usage for
/// dynamic scaling decisions in the pipeline managers.
struct SystemMonitor {
    system: System,
}

impl SystemMonitor {
    fn new() -> Self {
        Self {
            system: System::new_all(),
        }
    }

    /// Gets current memory usage as a ratio (0.0 to 1.0)
    ///
    /// Returns the ratio of used memory to total memory
    fn get_memory_usage(&mut self) -> f32 {
        self.system.refresh_memory();
        let used = self.system.used_memory();
        let total = self.system.total_memory();
        used as f32 / total as f32
    }

    /// Gets current CPU usage as a ratio (0.0 to 1.0)
    ///
    /// Returns the global CPU usage across all cores
    fn get_cpu_usage(&mut self) -> f32 {
        self.system.refresh_cpu_all();
        self.system.global_cpu_usage() / 100.0
    }
}

/// Dynamic resizer worker manager
///
/// Manages a pool of resizer workers that process image data in parallel.
/// Automatically scales the number of workers based on queue load and system memory usage.
struct ResizerManager {
    workers: Vec<(JoinHandle<()>, oneshot::Sender<()>)>,
    input_rx: Arc<Mutex<mpsc::Receiver<ShardSample>>>,
    output_tx: mpsc::Sender<ShardSample>,
    resizer_options: ResizerOptions,
    next_worker_id: usize,
    options: PipelineOptions,
}

impl ResizerManager {
    fn new(
        input_rx: Arc<Mutex<mpsc::Receiver<ShardSample>>>,
        output_tx: mpsc::Sender<ShardSample>,
        options: PipelineOptions,
    ) -> Self {
        Self {
            workers: Vec::new(),
            input_rx,
            output_tx,
            resizer_options: options.resizer_options.clone(),
            next_worker_id: 0,
            options,
        }
    }

    /// Main management loop that monitors and scales resizer workers
    ///
    /// This method continuously monitors:
    /// - Queue load and scaling needs
    /// - Memory usage for resource management
    /// - Worker health and completion status
    async fn run(&mut self, mut system_monitor: SystemMonitor) {
        tracing::info!(
            "Starting resizer manager with {} initial workers",
            self.options.min_resizers
        );

        // Start minimum number of resizers
        for _ in 0..self.options.min_resizers {
            self.spawn_resizer();
        }

        loop {
            sleep(Duration::from_millis(
                self.options.manager_check_interval_ms,
            ))
            .await;

            // Clean up completed tasks
            self.workers.retain(|(handle, _)| !handle.is_finished());

            // Check if output channel is closed
            if self.output_tx.is_closed() {
                tracing::info!("Output channel closed, shutting down all resizers");
                self.shutdown_all().await;
                break;
            }

            // Calculate queue usage and system resource utilization
            let queue_len = self.options.buffer_size - self.output_tx.capacity();
            let queue_ratio = queue_len as f32 / self.options.buffer_size as f32;
            let memory_usage = system_monitor.get_memory_usage();

            tracing::info!(
                "Resizer manager - Workers: {}, Queue: {:.1}%, Memory: {:.1}%",
                self.workers.len(),
                queue_ratio * 100.0,
                memory_usage * 100.0
            );

            // Dynamic scaling logic
            if memory_usage > self.options.memory_threshold
                && self.workers.len() > self.options.min_resizers
            {
                tracing::warn!("High memory usage, scaling down resizers");
                self.shutdown_one_resizer();
            } else if queue_ratio > self.options.scale_up_threshold
                && self.workers.len() < self.options.max_resizers
            {
                tracing::info!("High queue load, scaling up resizers");
                self.spawn_resizer();
            } else if queue_ratio < self.options.scale_down_threshold
                && self.workers.len() > self.options.min_resizers
            {
                tracing::info!("Low queue load, scaling down resizers");
                self.shutdown_one_resizer();
            }
        }
        tracing::info!("Resizer manager shutdown complete");
    }

    /// Spawns a new resizer worker
    fn spawn_resizer(&mut self) {
        let id = self.next_worker_id;
        self.next_worker_id += 1;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(resizer_worker(
            id,
            self.resizer_options.clone(),
            Arc::clone(&self.input_rx),
            self.output_tx.clone(),
            shutdown_rx,
        ));

        self.workers.push((handle, shutdown_tx));
        tracing::info!("Spawned new resizer worker: {}", id);
    }

    /// Gracefully shuts down one resizer worker
    fn shutdown_one_resizer(&mut self) {
        if let Some((_handle, shutdown_tx)) = self.workers.pop() {
            tracing::info!("Shutting down one resizer worker");
            let _ = shutdown_tx.send(());
        }
    }

    /// Shuts down all resizer workers
    async fn shutdown_all(&mut self) {
        tracing::info!("Shutting down all resizer workers");
        while let Some((handle, shutdown_tx)) = self.workers.pop() {
            let _ = shutdown_tx.send(());
            let _ = handle.await;
        }
    }
}

/// Dynamic downloader worker manager
///
/// Manages a pool of downloader workers that download images from URLs in parallel.
/// Automatically scales the number of workers based on queue load and system memory usage.
struct DownloaderManager {
    workers: Vec<(JoinHandle<()>, oneshot::Sender<()>)>,
    input_rx: Arc<Mutex<mpsc::Receiver<ShardSample>>>,
    output_tx: mpsc::Sender<ShardSample>,
    downloader_options: DownloaderOptions,
    next_worker_id: usize,
    options: PipelineOptions,
}

impl DownloaderManager {
    fn new(
        input_rx: Arc<Mutex<mpsc::Receiver<ShardSample>>>,
        output_tx: mpsc::Sender<ShardSample>,
        options: PipelineOptions,
    ) -> Self {
        Self {
            workers: Vec::new(),
            input_rx,
            output_tx,
            downloader_options: options.downloader_options.clone(),
            next_worker_id: 0,
            options,
        }
    }

    /// Main management loop that monitors and scales downloader workers
    ///
    /// This method continuously monitors:
    /// - Queue load and scaling needs
    /// - Memory usage for resource management
    /// - Worker health and completion status
    async fn run(&mut self, mut system_monitor: SystemMonitor) {
        tracing::info!(
            "Starting downloader manager with {} initial workers",
            self.options.min_downloaders
        );

        // Start minimum number of downloaders
        for _ in 0..self.options.min_downloaders {
            self.spawn_downloader();
        }

        loop {
            sleep(Duration::from_millis(
                self.options.manager_check_interval_ms,
            ))
            .await;

            // Clean up completed tasks
            self.workers.retain(|(handle, _)| !handle.is_finished());

            // Check if output channel is closed
            if self.output_tx.is_closed() {
                tracing::info!("Output channel closed, shutting down all downloaders");
                self.shutdown_all().await;
                break;
            }

            // Calculate queue usage and system resource utilization
            let queue_len = self.options.buffer_size - self.output_tx.capacity();
            let queue_ratio = queue_len as f32 / self.options.buffer_size as f32;
            let memory_usage = system_monitor.get_memory_usage();

            tracing::info!(
                "Downloader manager - Workers: {}, Queue: {:.1}%, Memory: {:.1}%",
                self.workers.len(),
                queue_ratio * 100.0,
                memory_usage * 100.0
            );

            // Dynamic scaling logic
            if memory_usage > self.options.memory_threshold
                && self.workers.len() > self.options.min_downloaders
            {
                tracing::warn!("High memory usage, scaling down downloaders");
                self.shutdown_one_downloader();
            } else if queue_ratio > self.options.scale_up_threshold
                && self.workers.len() < self.options.max_downloaders
            {
                tracing::info!("High queue load, scaling up downloaders");
                self.spawn_downloader();
            } else if queue_ratio < self.options.scale_down_threshold
                && self.workers.len() > self.options.min_downloaders
            {
                tracing::info!("Low queue load, scaling down downloaders");
                self.shutdown_one_downloader();
            }
        }
        tracing::info!("Downloader manager shutdown complete");
    }

    /// Spawns a new downloader worker
    fn spawn_downloader(&mut self) {
        let id = self.next_worker_id;
        self.next_worker_id += 1;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(downloader_worker(
            id,
            self.downloader_options.clone(),
            Arc::clone(&self.input_rx),
            self.output_tx.clone(),
            shutdown_rx,
        ));

        self.workers.push((handle, shutdown_tx));
        tracing::info!("Spawned new downloader worker: {}", id);
    }

    /// Gracefully shuts down one downloader worker
    fn shutdown_one_downloader(&mut self) {
        if let Some((_handle, shutdown_tx)) = self.workers.pop() {
            tracing::info!("Shutting down one downloader worker");
            let _ = shutdown_tx.send(());
        }
    }

    /// Shuts down all downloader workers
    async fn shutdown_all(&mut self) {
        tracing::info!("Shutting down all downloader workers");
        while let Some((handle, shutdown_tx)) = self.workers.pop() {
            let _ = shutdown_tx.send(());
            let _ = handle.await;
        }
    }
}

/// Dynamic writer worker manager
///
/// Manages a pool of writer workers that write processed data in parallel.
/// Automatically scales the number of workers based on queue load and system memory usage.
struct WriterManager {
    workers: Vec<(JoinHandle<Result<()>>, oneshot::Sender<()>)>,
    input_rx: Arc<Mutex<mpsc::Receiver<ShardSample>>>,
    writer_options: WriterOptions,
    next_worker_id: usize,
    options: PipelineOptions,
}

impl WriterManager {
    fn new(input_rx: Arc<Mutex<mpsc::Receiver<ShardSample>>>, options: PipelineOptions) -> Self {
        Self {
            workers: Vec::new(),
            input_rx,
            writer_options: options.writer_options.clone(),
            next_worker_id: 0,
            options,
        }
    }

    /// Main management loop that monitors and scales writer workers
    ///
    /// This method continuously monitors:
    /// - Queue load and scaling needs
    /// - Memory usage for resource management
    /// - Worker health and completion status
    async fn run(&mut self, mut system_monitor: SystemMonitor) {
        tracing::info!(
            "Starting writer manager with {} initial workers",
            self.options.min_writers
        );

        // Start minimum number of writers
        for _ in 0..self.options.min_writers {
            self.spawn_writer();
        }

        loop {
            sleep(Duration::from_millis(
                self.options.manager_check_interval_ms,
            ))
            .await;

            // Clean up completed tasks
            self.workers.retain(|(handle, _)| !handle.is_finished());

            // Check if input channel is closed
            let input_closed = {
                let guard = self.input_rx.lock().await;
                guard.is_closed()
            };

            if input_closed && self.workers.is_empty() {
                tracing::info!(
                    "Input channel closed and no workers remaining, shutting down writer manager"
                );
                break;
            }

            // Calculate queue usage and system resource utilization
            let queue_len = {
                let guard = self.input_rx.lock().await;
                guard.len()
            };
            let queue_ratio = queue_len as f32 / self.options.buffer_size as f32;
            let memory_usage = system_monitor.get_memory_usage();

            tracing::info!(
                "Writer manager - Workers: {}, Queue: {:.1}%, Memory: {:.1}%",
                self.workers.len(),
                queue_ratio * 100.0,
                memory_usage * 100.0
            );

            // Dynamic scaling logic
            if memory_usage > self.options.memory_threshold
                && self.workers.len() > self.options.min_writers
            {
                tracing::warn!("High memory usage, scaling down writers");
                self.shutdown_one_writer();
            } else if queue_ratio > self.options.scale_up_threshold
                && self.workers.len() < self.options.max_writers
            {
                tracing::info!("High queue load, scaling up writers");
                self.spawn_writer();
            } else if queue_ratio < self.options.scale_down_threshold
                && self.workers.len() > self.options.min_writers
            {
                tracing::info!("Low queue load, scaling down writers");
                self.shutdown_one_writer();
            }
        }
        tracing::info!("Writer manager shutdown complete");
    }

    /// Spawns a new writer worker
    fn spawn_writer(&mut self) {
        let id = self.next_worker_id;
        self.next_worker_id += 1;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(writer_worker(
            id,
            self.writer_options.clone(),
            Arc::clone(&self.input_rx),
            shutdown_rx,
        ));

        self.workers.push((handle, shutdown_tx));
        tracing::info!("Spawned new writer worker: {}", id);
    }

    /// Gracefully shuts down one writer worker
    fn shutdown_one_writer(&mut self) {
        if let Some((_handle, shutdown_tx)) = self.workers.pop() {
            tracing::info!("Shutting down one writer worker");
            let _ = shutdown_tx.send(());
        }
    }

    /// Shuts down all writer workers
    async fn shutdown_all(&mut self) {
        tracing::info!("Shutting down all writer workers");
        while let Some((handle, shutdown_tx)) = self.workers.pop() {
            let _ = shutdown_tx.send(());
            let _ = handle.await;
        }
    }
}

/// Reader worker thread
async fn reader_worker(options: ReaderOptions, output_tx: mpsc::Sender<ShardSample>) -> Result<()> {
    tracing::info!("Starting reader worker");
    let reader = Reader::new(options)?;
    let shard_stream = reader.shard_read().await?;
    tokio::pin!(shard_stream);

    while let Some(shard_result) = shard_stream.next().await {
        match shard_result {
            Ok(shard_sample) => {
                tracing::debug!("Reader sending shard {}", shard_sample.shard_id);
                if output_tx.send(shard_sample).await.is_err() {
                    tracing::warn!("Reader output channel closed");
                    break;
                }
            }
            Err(e) => {
                tracing::error!("Reader error: {}", e);
                return Err(e);
            }
        }
    }

    tracing::info!("Reader worker completed");
    Ok(())
}

/// Resizer worker thread
async fn resizer_worker(
    id: usize,
    options: ResizerOptions,
    input_rx: Arc<Mutex<mpsc::Receiver<ShardSample>>>,
    output_tx: mpsc::Sender<ShardSample>,
    mut shutdown_signal: oneshot::Receiver<()>,
) {
    tracing::info!("Starting resizer worker {}", id);
    let resizer = Resizer::new(options);

    loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                tracing::info!("Resizer worker {} received shutdown signal", id);
                break;
            }

            maybe_sample = async {
                let mut guard = input_rx.lock().await;
                guard.recv().await
            } => match maybe_sample {
                Some(shard_sample) => {
                    tracing::debug!("Resizer {} processing shard {}", id, shard_sample.shard_id);
                    match resizer.shard_resize(shard_sample).await {
                        Ok(resized_sample) => {
                            if output_tx.send(resized_sample).await.is_err() {
                                tracing::warn!("Resizer {} output channel closed", id);
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::error!("Resizer {} failed to process shard: {}", id, e);
                        }
                    }
                }
                None => {
                    tracing::info!("Resizer worker {} input channel closed", id);
                    break;
                }
            },
        }
    }
    tracing::info!("Resizer worker {} stopped", id);
}

/// Downloader worker thread
async fn downloader_worker(
    id: usize,
    options: DownloaderOptions,
    input_rx: Arc<Mutex<mpsc::Receiver<ShardSample>>>,
    output_tx: mpsc::Sender<ShardSample>,
    mut shutdown_signal: oneshot::Receiver<()>,
) {
    tracing::info!("Starting downloader worker {}", id);
    let downloader = Downloader::new(options);

    loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                tracing::info!("Downloader worker {} received shutdown signal", id);
                break;
            }

            maybe_sample = async {
                let mut guard = input_rx.lock().await;
                guard.recv().await
            } => match maybe_sample {
                Some(shard_sample) => {
                    tracing::debug!("Downloader {} processing shard {}", id, shard_sample.shard_id);
                    let downloaded_shard = downloader.shard_download(shard_sample).await;
                    if output_tx.send(downloaded_shard).await.is_err() {
                        tracing::warn!("Downloader {} output channel closed", id);
                        break;
                    }
                }
                None => {
                    tracing::info!("Downloader worker {} input channel closed", id);
                    break;
                }
            },
        }
    }
    tracing::info!("Downloader worker {} stopped", id);
}

/// Writer worker thread
async fn writer_worker(
    id: usize,
    options: WriterOptions,
    input_rx: Arc<Mutex<mpsc::Receiver<ShardSample>>>,
    mut shutdown_signal: oneshot::Receiver<()>,
) -> Result<()> {
    tracing::info!("Starting writer worker {}", id);
    let writer = Writer::new(options)?;

    loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                tracing::info!("Writer worker {} received shutdown signal", id);
                break;
            }

            maybe_sample = async {
                let mut guard = input_rx.lock().await;
                guard.recv().await
            } => match maybe_sample {
                Some(shard_sample) => {
                    tracing::debug!("Writer {} processing shard {}", id, shard_sample.shard_id);
                    if let Err(e) = writer.shard_write(shard_sample).await {
                        tracing::error!("Writer {} failed to write shard: {}", id, e);
                        return Err(e);
                    }
                }
                None => {
                    tracing::info!("Writer worker {} input channel closed", id);
                    break;
                }
            },
        }
    }
    tracing::info!("Writer worker {} stopped", id);
    Ok(())
}

/// Main Pipeline manager
///
/// Orchestrates the entire image processing pipeline using a producer-consumer model.
///
/// The pipeline consists of:
/// 1. Reader: Reads data batches from input sources
/// 2. DownloaderManager: Manages dynamic pool of image downloading workers
/// 3. ResizerManager: Manages dynamic pool of image resizing workers
/// 4. WriterManager: Manages dynamic pool of output writing workers
///
/// Features:
/// - Dynamic scaling based on queue load and system resources
/// - Graceful shutdown and error handling
/// - Resource monitoring and adaptive behavior
pub struct PipelineManager {
    options: PipelineOptions,
}

impl PipelineManager {
    /// Creates a new PipelineManager with the given configuration
    pub fn new(options: PipelineOptions) -> Self {
        Self { options }
    }

    /// Runs the complete image processing pipeline
    ///
    /// This method:
    /// 1. Sets up communication channels between components
    /// 2. Starts the reader, downloader manager, resizer manager, and writer manager
    /// 3. Monitors all components for completion or errors
    /// 4. Ensures graceful shutdown of all resources
    pub async fn run(&self) -> Result<()> {
        tracing::info!("Starting image processing pipeline");

        // Create channels
        let (reader_tx, reader_rx) = mpsc::channel::<ShardSample>(self.options.buffer_size);
        let (downloader_tx, downloader_rx) = mpsc::channel::<ShardSample>(self.options.buffer_size);
        let (resizer_tx, resizer_rx) = mpsc::channel::<ShardSample>(self.options.buffer_size);

        let shared_reader_rx = Arc::new(Mutex::new(reader_rx));
        let shared_downloader_rx = Arc::new(Mutex::new(downloader_rx));
        let shared_resizer_rx = Arc::new(Mutex::new(resizer_rx));

        // Start reader
        let reader_options = self.options.reader_options.clone();
        let reader_handle =
            tokio::spawn(async move { reader_worker(reader_options, reader_tx).await });

        // Start system monitoring
        let system_monitor1 = SystemMonitor::new();
        let system_monitor2 = SystemMonitor::new();
        let system_monitor3 = SystemMonitor::new();

        // Start downloader manager
        let mut downloader_manager =
            DownloaderManager::new(shared_reader_rx, downloader_tx, self.options.clone());
        let downloader_manager_handle = tokio::spawn(async move {
            downloader_manager.run(system_monitor1).await;
        });

        // Start resizer manager
        let mut resizer_manager =
            ResizerManager::new(shared_downloader_rx, resizer_tx, self.options.clone());
        let resizer_manager_handle = tokio::spawn(async move {
            resizer_manager.run(system_monitor2).await;
        });

        // Start writer manager
        let mut writer_manager = WriterManager::new(shared_resizer_rx, self.options.clone());
        let writer_manager_handle = tokio::spawn(async move {
            writer_manager.run(system_monitor3).await;
        });

        // Wait for all tasks to complete
        if let Err(e) = reader_handle.await {
            tracing::error!("Reader failed: {:?}", e);
        }

        if let Err(e) = downloader_manager_handle.await {
            tracing::error!("Downloader manager failed: {:?}", e);
        }

        if let Err(e) = resizer_manager_handle.await {
            tracing::error!("Resizer manager failed: {:?}", e);
        }

        if let Err(e) = writer_manager_handle.await {
            tracing::error!("Writer manager failed: {:?}", e);
        }

        tracing::info!("Pipeline completed successfully");
        Ok(())
    }
}
