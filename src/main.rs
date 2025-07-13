use crate::downloader::Downloader;
use crate::reader::{InputFormat, Reader, ReaderBackend, ReaderOptions};
use crate::resizer::{ResizeMode, Resizer};
use crate::sampler::{OutputSample, SampleStatus};
use crate::writer::{OutputBackend, OutputFormat, Writer, WriterOptions};
use anyhow::Result;
use anyhow::anyhow;
use clap::{Parser, Subcommand, ValueEnum};
use futures::stream::StreamExt;
use metrics::{counter, gauge, histogram};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

pub mod downloader;
mod observability;
pub mod reader;
pub mod resizer;
mod sampler;
mod status_line;
pub mod writer;

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the main image downloading and processing pipeline
    Run(Args),
    /// A subcommand for debugging purposes
    Debug(DebugCommand),
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Parser, Debug)]
struct DebugCommand {
    #[clap(subcommand)]
    command: DebugSubcommand,
}

#[derive(Subcommand, Debug)]
enum DebugSubcommand {
    Read(DebugReadArgs),
}

#[derive(Parser, Debug)]
struct DebugReadArgs {
    #[clap(long, default_value = "url")]
    reader_url_column_name: String,
    #[clap(long)]
    reader_caption_column_name: Option<String>,
    #[clap(long)]
    reader_save_additional_columns: bool,
    #[clap(long, default_value_t = 100)]
    thread_count: usize,
    #[clap(long, default_value_t = 10)]
    number_of_items_per_shard: usize,
    #[clap(long, default_value_t = 0)]
    shard_id_start: usize,
    #[clap(long, default_value = "png")]
    image_format: String,
    #[clap(value_enum, long, default_value_t = ResizeMode::Border)]
    resize_mode: ResizeMode,
    #[clap(long, default_value_t = 256)]
    image_size: u32,
    #[clap(long)]
    resize_only_if_bigger: bool,
    #[clap(long, default_value_t = u64::MAX)]
    max_items_to_download: u64,
    #[clap(long, default_value_t = 60)]
    timeout: u64,
    #[clap(long, default_value_t = 3)]
    retries: u32,
    #[clap(long)]
    user_agent_token: Option<String>,
    #[clap(long)]
    disallowed_header_directives: Option<Vec<String>>,
    #[clap(value_enum, long, default_value_t = LogLevel::Info)]
    log_level: LogLevel,
    #[clap(long, default_value_t = 0)]
    resume_from_shard_number: usize,
    #[clap(long, default_value_t = 0)]
    resume_from_sample_number: usize,
    #[clap(long, default_value = ".")]
    state_db_path: PathBuf,
    #[clap(long, default_value = "100")]
    writer_concurrent_limit: usize,
    #[clap(long, default_value = "10")]
    writer_webdataset_shard_bits_num: usize,
    #[clap(long, default_value = "shard")]
    writer_webdataset_shard_prefix: String,
    #[clap(long, default_value = "")]
    input_root: String,
    #[clap(value_enum, long, default_value_t = ReaderBackend::Fs)]
    input_backend: ReaderBackend,
    #[clap(value_enum, long, default_value_t = InputFormat::Parquet)]
    input_format: InputFormat,

    // Input Backend Hugging Face options
    #[clap(long)]
    input_hf_access_token: Option<String>,
    #[clap(long)]
    input_hf_dataset_repo_id: Option<String>,
    #[clap(long)]
    input_hf_revision: Option<String>,

    // Input Backend S3 options
    #[clap(long)]
    input_s3_bucket: Option<String>,
    #[clap(long)]
    input_s3_region: Option<String>,
    #[clap(long)]
    input_s3_access_key: Option<String>,
    #[clap(long)]
    input_s3_secret_key: Option<String>,
    #[clap(long)]
    input_s3_endpoint: Option<String>,

    // Output options
    #[clap(long, default_value = "")]
    output_root: String,
    #[clap(value_enum, long, default_value_t = OutputBackend::Fs)]
    output_backend: OutputBackend,
    #[clap(value_enum, long, default_value_t = OutputFormat::Files)]
    output_format: OutputFormat,
    #[clap(long)]
    output_s3_bucket: Option<String>,
    #[clap(long)]
    output_s3_region: Option<String>,
    #[clap(long)]
    output_s3_access_key: Option<String>,
    #[clap(long)]
    output_s3_secret_key: Option<String>,
    #[clap(long)]
    output_s3_endpoint: Option<String>,
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_value = "url")]
    reader_url_column_name: String,
    #[clap(long)]
    reader_caption_column_name: Option<String>,
    #[clap(long, default_value_t = true)]
    reader_save_additional_columns: bool,
    #[clap(long, default_value_t = 100)]
    downloader_thread_count: usize,
    #[clap(long, default_value_t = 1000)]
    number_of_items_per_shard: usize,
    #[clap(long, default_value_t = 0)]
    shard_id_start: usize,
    #[clap(long, default_value = "jpg")]
    image_format: String,
    #[clap(value_enum, long, default_value_t = ResizeMode::Border)]
    resize_mode: ResizeMode,
    #[clap(long, default_value_t = 256)]
    image_size: u32,
    #[clap(long)]
    resize_only_if_bigger: bool,
    #[clap(long, default_value_t = u64::MAX)]
    max_items_to_download: u64,
    #[clap(long, default_value_t = 60)]
    timeout: u64,
    #[clap(long, default_value_t = 0)]
    retries: u32,
    #[clap(long)]
    user_agent_token: Option<String>,
    #[clap(long)]
    disallowed_header_directives: Option<Vec<String>>,
    #[clap(value_enum, long, default_value_t = LogLevel::Info)]
    log_level: LogLevel,
    #[clap(long, default_value_t = 0)]
    resume_from_shard_number: usize,
    #[clap(long, default_value_t = 0)]
    resume_from_sample_number: usize,
    #[clap(long, default_value = ".")]
    state_db_path: PathBuf,
    #[clap(long, default_value = "10")]
    writer_thread_count: usize,
    #[clap(long, default_value = "10")]
    writer_webdataset_shard_bits_num: usize,
    #[clap(long, default_value = "shard")]
    writer_webdataset_shard_prefix: String,
    #[clap(long, default_value = ".")]
    input_root: String,
    #[clap(value_enum, long, default_value_t = ReaderBackend::Fs)]
    input_backend: ReaderBackend,
    #[clap(value_enum, long, default_value_t = InputFormat::Parquet)]
    input_format: InputFormat,

    // Input Backend Hugging Face options
    #[clap(long)]
    input_hf_access_token: Option<String>,
    #[clap(long)]
    input_hf_dataset_repo_id: Option<String>,
    #[clap(long)]
    input_hf_revision: Option<String>,

    // Input Backend S3 options
    #[clap(long)]
    input_s3_bucket: Option<String>,
    #[clap(long)]
    input_s3_region: Option<String>,
    #[clap(long)]
    input_s3_access_key: Option<String>,
    #[clap(long)]
    input_s3_secret_key: Option<String>,
    #[clap(long)]
    input_s3_endpoint: Option<String>,

    // Output options
    #[clap(long, default_value = ".")]
    output_root: String,
    #[clap(value_enum, long, default_value_t = OutputBackend::Fs)]
    output_backend: OutputBackend,
    #[clap(value_enum, long, default_value_t = OutputFormat::Files)]
    output_format: OutputFormat,
    #[clap(long)]
    output_s3_bucket: Option<String>,
    #[clap(long)]
    output_s3_region: Option<String>,
    #[clap(long)]
    output_s3_access_key: Option<String>,
    #[clap(long)]
    output_s3_secret_key: Option<String>,
    #[clap(long)]
    output_s3_endpoint: Option<String>,
    #[clap(long)]
    output_b2_bucket: Option<String>,
    #[clap(long)]
    output_b2_bucket_id: Option<String>,
    #[clap(long)]
    output_b2_application_key_id: Option<String>,
    #[clap(long)]
    output_b2_application_key: Option<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl From<LogLevel> for tracing::Level {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Error => tracing::Level::ERROR,
            LogLevel::Warn => tracing::Level::WARN,
            LogLevel::Info => tracing::Level::INFO,
            LogLevel::Debug => tracing::Level::DEBUG,
            LogLevel::Trace => tracing::Level::TRACE,
        }
    }
}

async fn debug_read(args: DebugReadArgs) -> Result<()> {
    // Initialize observability with specified log level
    observability::install_observability_with_config(false, args.log_level.into())?;

    log::info!("Starting debug read mode");
    counter!("debug_reads_total").increment(1);
    let reader_options = ReaderOptions {
        format: args.input_format,
        backend: args.input_backend,
        root: args.input_root,
        huggingface_token: args.input_hf_access_token,
        huggingface_repo_id: args.input_hf_dataset_repo_id,
        huggingface_revision: args.input_hf_revision,
        s3_bucket: args.input_s3_bucket,
        s3_region: args.input_s3_region,
        s3_access_key: args.input_s3_access_key,
        s3_secret_key: args.input_s3_secret_key,
        s3_endpoint: args.input_s3_endpoint,
        input_url_column_name: args.reader_url_column_name,
        input_caption_column_name: args.reader_caption_column_name,
        save_additional_columns: args.reader_save_additional_columns,
    };

    let reader = Reader::new(reader_options)
        .map_err(|e| anyhow!("Failed to create reader: {}", e.to_string()))?;
    let mut stream = reader
        .stream_samples()
        .await
        .map_err(|e| anyhow!("Failed to stream samples: {}", e.to_string()))?;

    let mut count = 0;
    let debug_start = Instant::now();
    while let Some(sample) = stream.next().await {
        count += 1;
        match sample {
            Ok(s) => {
                counter!("debug_samples_read_total").increment(1);
                log::debug!("Processing sample {}: {:?}", count, s);
            }
            Err(e) => {
                counter!("debug_samples_error_total").increment(1);
                log::error!("Error reading sample: {}", e);
            }
        }
    }

    let debug_duration = debug_start.elapsed();
    log::info!(
        "Debug read completed: {} samples processed in {:.2}s",
        count,
        debug_duration.as_secs_f64()
    );
    histogram!("debug_read_duration_seconds").record(debug_duration.as_secs_f64());
    gauge!("debug_samples_processed").set(count as f64);

    Ok(())
}

async fn main_run(args: Args) -> Result<()> {
    // Install observability with log redirection for status line and specified log level
    observability::install_observability_with_config(true, args.log_level.into())?;

    // Setup status line (always enabled)
    let (stats, stats_tx) = {
        let (stats, tx) = status_line::create_stats_updater();
        (stats, tx)
    };

    // Start status line in background
    let status_stats = stats.clone();
    tokio::spawn(async move {
        status_line::run_status_line(status_stats).await;
    });

    // Record application start metrics
    counter!("app_starts_total").increment(1);
    gauge!("configured_downloader_threads").set(args.downloader_thread_count as f64);
    gauge!("configured_items_per_shard").set(args.number_of_items_per_shard as f64);
    gauge!("configured_timeout_seconds").set(args.timeout as f64);

    log::info!(
        "Starting img2dataset-rs with {} downloader threads, {} items per shard",
        args.downloader_thread_count,
        args.number_of_items_per_shard
    );

    if args.resume_from_shard_number > 0 || args.resume_from_sample_number > 0 {
        log::info!(
            "Resume mode enabled: starting from shard {}, sample {}",
            args.resume_from_shard_number,
            args.resume_from_sample_number
        );
        counter!("app_resume_starts_total").increment(1);
    }

    let reader_options = ReaderOptions {
        input_url_column_name: args.reader_url_column_name,
        input_caption_column_name: args.reader_caption_column_name,
        format: args.input_format,
        backend: args.input_backend,
        root: args.input_root,
        s3_bucket: args.input_s3_bucket,
        s3_region: args.input_s3_region,
        s3_access_key: args.input_s3_access_key,
        s3_secret_key: args.input_s3_secret_key,
        s3_endpoint: args.input_s3_endpoint,
        huggingface_token: args.input_hf_access_token,
        huggingface_repo_id: args.input_hf_dataset_repo_id,
        huggingface_revision: args.input_hf_revision,
        save_additional_columns: args.reader_save_additional_columns,
    };
    let reader = Reader::new(reader_options).map_err(|e| anyhow!("{}", e))?;

    let writer_options = WriterOptions {
        root: args.output_root,
        format: args.output_format,
        backend: args.output_backend,
        s3_bucket: args.output_s3_bucket,
        s3_region: args.output_s3_region,
        s3_access_key: args.output_s3_access_key,
        s3_secret_key: args.output_s3_secret_key,
        s3_endpoint: args.output_s3_endpoint,
        b2_bucket: args.output_b2_bucket,
        b2_bucket_id: args.output_b2_bucket_id,
        b2_application_key_id: args.output_b2_application_key_id,
        b2_application_key: args.output_b2_application_key,
        writer_thread_count: args.downloader_thread_count,
        webdataset_shard_bits_num: args.writer_webdataset_shard_bits_num,
        webdataset_shard_prefix: args.writer_webdataset_shard_prefix,
    };
    let writer = Arc::new(Writer::new(writer_options)?);

    // Start from the specified shard number for resume functionality
    for shard_id in args.resume_from_shard_number.. {
        let shard_start_time = Instant::now();
        log::info!("Processing shard {} started", shard_id);
        gauge!("current_shard_id").set(shard_id as f64);

        // Update status line with current shard
        let _ = stats_tx.send(status_line::StatsUpdate::ShardChanged(shard_id as u64));

        let downloader = Downloader::new(
            args.timeout,
            args.user_agent_token.clone(),
            args.disallowed_header_directives.clone(),
            args.retries,
        );
        let resizer = Resizer::new(
            args.resize_mode,
            args.image_size,
            args.resize_only_if_bigger,
            &args.image_format,
        );

        // Skip samples only for the first resumed shard
        let skip_samples = if shard_id == args.resume_from_shard_number {
            if args.resume_from_sample_number > 0 {
                log::info!(
                    "Resuming from shard {} and skipping {} samples",
                    shard_id,
                    args.resume_from_sample_number
                );
            }
            args.resume_from_sample_number
        } else {
            0
        };

        let stream_samples = reader
            .stream_samples_from(skip_samples)
            .await
            .map_err(|e| anyhow!("{}", e))?;

        // Clone stats_tx for use in closures
        let stats_tx_for_memory = stats_tx.clone();
        let stats_tx_for_download_start = stats_tx.clone();
        let stats_tx_for_download_result = stats_tx.clone();

        let mut shard_id_counter = shard_id;
        let writer_clone = writer.clone();

        let samples = stream_samples
            .map(move |maybe_sample| {
                // Track memory usage with process stats
                if let Ok(usage) = std::fs::read_to_string("/proc/self/status") {
                    if let Some(line) = usage.lines().find(|l| l.starts_with("VmRSS:")) {
                        if let Some(kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(kb) = kb_str.parse::<u64>() {
                                let mb = (kb as f64) / 1024.0;
                                metrics::gauge!("memory_usage_bytes").set((kb * 1024) as f64);

                                // Update status line with memory usage
                                let _ = stats_tx_for_memory
                                    .send(status_line::StatsUpdate::MemoryUsage(mb));
                            }
                        }
                    }
                }
                maybe_sample
            })
            .filter_map(|result| async move {
                match result {
                    Ok(sample) => {
                        counter!("input_samples_total").increment(1);
                        counter!("samples_read_success_total").increment(1);
                        log::debug!("Read sample ID {}: {}", sample.id, sample.url);
                        Some(sample)
                    }
                    Err(e) => {
                        counter!("samples_read_error_total").increment(1);
                        log::error!("Failed to read sample: {}", e);
                        None
                    }
                }
            })
            .map(move |sample| {
                let download_start = Instant::now();
                let downloader = downloader.clone();
                let stats_tx_for_start = stats_tx_for_download_start.clone();

                async move {
                    // Notify status line that download started
                    let _ = stats_tx_for_start.send(status_line::StatsUpdate::DownloadStarted);

                    let result = downloader.download(sample).await;
                    let download_duration = download_start.elapsed();
                    histogram!("download_duration_seconds").record(download_duration.as_secs_f64());
                    result
                }
            })
            .buffer_unordered(args.downloader_thread_count)
            .map(move |sample| {
                match &sample.status {
                    SampleStatus::Success => {
                        counter!("download_success_total").increment(1);
                        histogram!("download_size_bytes").record(sample.download_data.len() as f64);
                        log::debug!(
                            "Downloaded sample ID {}: {} bytes",
                            sample.id,
                            sample.download_data.len()
                        );

                        // Update status line
                        let _ = stats_tx_for_download_result
                            .send(status_line::StatsUpdate::DownloadSuccess);
                    }
                    SampleStatus::Failure(reason) => {
                        counter!("download_failure_total").increment(1);

                        // Categorize failure reasons
                        if reason.contains("timeout") {
                            counter!("download_timeout_total").increment(1);
                        } else if reason.contains("404") || reason.contains("not found") {
                            counter!("download_not_found_total").increment(1);
                        } else if reason.contains("403") || reason.contains("forbidden") {
                            counter!("download_forbidden_total").increment(1);
                        } else if reason.contains("network") || reason.contains("connection") {
                            counter!("download_network_error_total").increment(1);
                        } else {
                            counter!("download_other_error_total").increment(1);
                        }

                        log::warn!("Download failed for sample ID {}: {}", sample.id, reason);

                        // Update status line
                        let _ = stats_tx_for_download_result
                            .send(status_line::StatsUpdate::DownloadFailed(reason.clone()));
                    }
                }
                sample
            })
            .map(move |sample| {
                let resize_start = Instant::now();
                let resizer = resizer.clone();
                let resize_result = resizer.resize(sample);
                let resize_duration = resize_start.elapsed();
                histogram!("resize_duration_seconds").record(resize_duration.as_secs_f64());

                match resize_result {
                    Ok(resized_sample) => {
                        counter!("resize_success_total").increment(1);
                        log::debug!("Resized sample ID {} successfully", resized_sample.id);
                        resized_sample
                    }
                    Err(e) => {
                        counter!("resize_failure_total").increment(1);
                        log::error!("Resize failed for sample: {}", e);
                        OutputSample {
                            id: 0,
                            url: "".to_string(),
                            caption: None,
                            original_filepath: "".to_string(),
                            download_data: Vec::new(),
                            download_mime_type: None,
                            download_timestamp: None,
                            additional_columns: Default::default(),
                            status: SampleStatus::Failure(format!("Resize error: {}", e)),
                        }
                    }
                }
            })
            .filter(|sample| {
                let success = sample.status == SampleStatus::Success;
                async move { success }
            });

        // Write samples directly using the writer
        let write_start = Instant::now();
        match writer_clone
            .write_streaming_samples(Box::pin(samples), &mut shard_id_counter)
            .await
        {
            Ok(_) => {
                let write_duration = write_start.elapsed();
                histogram!("write_duration_seconds").record(write_duration.as_secs_f64());
                counter!("shard_written_success_total").increment(1);
                log::info!(
                    "Successfully wrote shard {} in {:.2}s",
                    shard_id,
                    write_duration.as_secs_f64()
                );
            }
            Err(e) => {
                counter!("shard_written_failure_total").increment(1);
                log::error!("Failed to write shard {}: {}", shard_id, e);
            }
        }

        let shard_duration = shard_start_time.elapsed();
        histogram!("shard_processing_duration_seconds").record(shard_duration.as_secs_f64());
        log::info!(
            "Completed shard {} in {:.2}s",
            shard_id,
            shard_duration.as_secs_f64()
        );
    }

    log::info!("Processing completed successfully");
    counter!("app_completion_total").increment(1);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run(args) => main_run(args).await,
        Command::Debug(debug_command) => match debug_command.command {
            DebugSubcommand::Read(args) => Ok(debug_read(args).await?),
        },
    }
}
