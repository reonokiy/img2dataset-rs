use crate::downloader::DownloaderOptions;
use crate::manager::{PipelineManager, PipelineOptions};
use crate::observability::{ObservabilityManager, ObservabilityOptions};
use crate::reader::{InputFormat, Reader, ReaderBackend, ReaderOptions};
use crate::resizer::{ImageFormat, ResizeMode, ResizerOptions};
use crate::writer::{OutputBackend, OutputFormat, WriterOptions};
use anyhow::Result;
use anyhow::anyhow;
use clap::{Parser, Subcommand};
use futures::stream::StreamExt;
use std::time::Instant;

pub mod downloader;
pub mod manager;
mod observability;
pub mod reader;
pub mod resizer;
mod sampler;
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
    Read(Args),
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    // Reader options
    #[clap(long, default_value = "url")]
    reader_url_column_name: String,
    #[clap(long, default_value_t = true)]
    reader_save_additional_columns: bool,
    #[clap(long)]
    reader_root: String,
    #[clap(value_enum, long, default_value_t = ReaderBackend::Fs)]
    reader_backend: ReaderBackend,
    #[clap(value_enum, long, default_value_t = InputFormat::Parquet)]
    reader_format: InputFormat,
    #[clap(long)]
    reader_s3_bucket: Option<String>,
    #[clap(long)]
    reader_s3_region: Option<String>,
    #[clap(long)]
    reader_s3_access_key: Option<String>,
    #[clap(long)]
    reader_s3_secret_key: Option<String>,
    #[clap(long)]
    reader_s3_endpoint: Option<String>,
    #[clap(long)]
    reader_hf_access_token: Option<String>,
    #[clap(long)]
    reader_hf_dataset_repo_id: Option<String>,
    #[clap(long)]
    reader_hf_revision: Option<String>,
    #[clap(long, default_value_t = 1024)]
    reader_batch_size: usize,
    #[clap(long, default_value_t = 8)]
    reader_batch_per_shard: usize,
    #[clap(long, default_value_t = 16 * 1024 * 1024)]
    reader_opendal_buffer_size: usize,

    // Resizer options
    #[clap(long, default_value_t = false)]
    reencode: bool,
    #[clap(value_enum, long, default_value_t = ImageFormat::Jpeg)]
    reencode_image_format: ImageFormat,
    #[clap(long, default_value_t = 95)]
    reencode_image_quality: u8,
    #[clap(value_enum, long, default_value_t = ResizeMode::No)]
    resize_mode: ResizeMode,
    #[clap(long, default_value_t = 512)]
    resize_image_height: u32,
    #[clap(long, default_value_t = 512)]
    resize_image_width: u32,
    #[clap(long, default_value_t = false)]
    resize_only_if_bigger: bool,
    #[clap(long, default_value_t = 1)]
    min_resizers: usize,
    #[clap(long, default_value_t = 10)]
    max_resizers: usize,

    // Writer options
    #[clap(long)]
    writer_root: String,
    #[clap(value_enum, long, default_value_t = OutputBackend::Fs)]
    writer_backend: OutputBackend,
    #[clap(value_enum, long, default_value_t = OutputFormat::Files)]
    writer_format: OutputFormat,
    #[clap(long)]
    writer_s3_bucket: Option<String>,
    #[clap(long)]
    writer_s3_region: Option<String>,
    #[clap(long)]
    writer_s3_access_key: Option<String>,
    #[clap(long)]
    writer_s3_secret_key: Option<String>,
    #[clap(long)]
    writer_s3_endpoint: Option<String>,
    #[clap(long)]
    writer_b2_bucket: Option<String>,
    #[clap(long)]
    writer_b2_bucket_id: Option<String>,
    #[clap(long)]
    writer_b2_application_key_id: Option<String>,
    #[clap(long)]
    writer_b2_application_key: Option<String>,
    #[clap(long, default_value = "")]
    writer_shard_prefix: String,
    #[clap(long, default_value_t = 1)]
    min_writers: usize,
    #[clap(long, default_value_t = 10)]
    max_writers: usize,

    // Downloader options
    #[clap(long, default_values_t = vec!["noai".to_string(), "noimageai".to_string(), "noindex".to_string(), "noimageindex".to_string()])]
    disallowed_header_directives: Vec<String>,
    #[clap(long, default_value_t = 64)]
    downloader_thread_count: usize,
    #[clap(long, default_value_t = 10)]
    timeout: u64,
    #[clap(long, default_value_t = 1)]
    retries: u32,
    #[clap(
        long,
        default_value = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:72.0) Gecko/20100101 Firefox/72.0 (compatible; +https://github.com/reonokiy/img2dataset-rs)"
    )]
    user_agent: String,
    #[clap(long, default_value_t = 1)]
    minimum_downloaders: usize,
    #[clap(long, default_value_t = 10)]
    maximum_downloaders: usize,

    // Pipeline options
    #[clap(long, default_value_t = 1024)]
    buffer_size: usize,
    #[clap(long, default_value_t = 0.9)]
    memory_threshold: f32,
    #[clap(long, default_value_t = 0.8)]
    scale_up_threshold: f32,
    #[clap(long, default_value_t = 0.2)]
    scale_down_threshold: f32,
    #[clap(long, default_value_t = 1000)]
    manager_check_interval_ms: u64,

    // Observability options
    #[clap(value_enum, long, default_value = "info")]
    log_level: tracing::Level,
}

async fn debug_read(args: Args) -> Result<()> {
    let observability_options = ObservabilityOptions {
        log_level: args.log_level.into(),
    };
    let mut observability_manager = ObservabilityManager::new(observability_options);

    tracing::info!("Starting debug read mode");
    let reader_options = ReaderOptions {
        format: args.reader_format,
        backend: args.reader_backend,
        root: args.reader_root,
        huggingface_token: args.reader_hf_access_token,
        huggingface_repo_id: args.reader_hf_dataset_repo_id,
        huggingface_revision: args.reader_hf_revision,
        s3_bucket: args.reader_s3_bucket,
        s3_region: args.reader_s3_region,
        s3_access_key: args.reader_s3_access_key,
        s3_secret_key: args.reader_s3_secret_key,
        s3_endpoint: args.reader_s3_endpoint,
        url_column_name: args.reader_url_column_name,
        save_additional_columns: args.reader_save_additional_columns,
        batch_size: args.reader_batch_size,
        batch_per_shard: args.reader_batch_per_shard,
        opendal_buffer_size: args.reader_opendal_buffer_size,
    };

    let reader = Reader::new(reader_options)
        .map_err(|e| anyhow!("Failed to create reader: {}", e.to_string()))?;
    let mut stream = reader
        .shard_read()
        .await
        .map_err(|e| anyhow!("Failed to stream samples: {}", e.to_string()))?;

    let mut count = 0;
    let debug_start = Instant::now();
    while let Some(sample) = stream.next().await {
        count += 1;
        match sample {
            Ok(s) => {
                tracing::debug!("Processing sample {}: {:?}", count, s);
            }
            Err(e) => {
                tracing::error!("Error reading sample: {}", e);
            }
        }
    }

    let debug_duration = debug_start.elapsed();
    tracing::info!(
        "Debug read completed: {} samples processed in {:.2}s",
        count,
        debug_duration.as_secs_f64()
    );

    observability_manager.shutdown().await?;
    Ok(())
}

async fn main_run(args: Args) -> Result<()> {
    let observability_options = ObservabilityOptions {
        log_level: args.log_level.into(),
    };
    let mut observability_manager = ObservabilityManager::new(observability_options);

    let reader_options = ReaderOptions {
        url_column_name: args.reader_url_column_name,
        format: args.reader_format,
        backend: args.reader_backend,
        root: args.reader_root,
        s3_bucket: args.reader_s3_bucket,
        s3_region: args.reader_s3_region,
        s3_access_key: args.reader_s3_access_key,
        s3_secret_key: args.reader_s3_secret_key,
        s3_endpoint: args.reader_s3_endpoint,
        huggingface_token: args.reader_hf_access_token,
        huggingface_repo_id: args.reader_hf_dataset_repo_id,
        huggingface_revision: args.reader_hf_revision,
        save_additional_columns: args.reader_save_additional_columns,
        batch_size: args.reader_batch_size,
        batch_per_shard: args.reader_batch_per_shard,
        opendal_buffer_size: args.reader_opendal_buffer_size,
    };

    let downloader_options = DownloaderOptions {
        thread: args.downloader_thread_count,
        timeout: args.timeout,
        retries: args.retries,
        user_agent: args.user_agent,
        disallowed_header_directives: args.disallowed_header_directives,
    };

    let resizer_options = ResizerOptions {
        reencode: args.reencode,
        resize_mode: args.resize_mode,
        target_image_width: args.resize_image_height,
        target_image_height: args.resize_image_height,
        resize_only_if_bigger: args.resize_only_if_bigger,
        target_image_format: args.reencode_image_format,
        target_image_quality: 95, // High quality JPEG
    };

    let writer_options = WriterOptions {
        root: args.writer_root,
        format: args.writer_format,
        backend: args.writer_backend,
        s3_bucket: args.writer_s3_bucket,
        s3_region: args.writer_s3_region,
        s3_access_key: args.writer_s3_access_key,
        s3_secret_key: args.writer_s3_secret_key,
        s3_endpoint: args.writer_s3_endpoint,
        b2_bucket: args.writer_b2_bucket,
        b2_bucket_id: args.writer_b2_bucket_id,
        b2_application_key_id: args.writer_b2_application_key_id,
        b2_application_key: args.writer_b2_application_key,
        shard_prefix: args.writer_shard_prefix,
    };

    let pipeline_options = PipelineOptions {
        reader_options,
        downloader_options,
        resizer_options,
        writer_options,
        buffer_size: args.buffer_size,
        min_resizers: args.min_resizers,
        max_resizers: args.max_resizers,
        min_writers: args.min_writers,
        max_writers: args.max_writers,
        min_downloaders: args.downloader_thread_count,
        max_downloaders: args.downloader_thread_count,
        memory_threshold: args.memory_threshold,
        scale_up_threshold: args.scale_up_threshold,
        scale_down_threshold: args.scale_down_threshold,
        manager_check_interval_ms: args.manager_check_interval_ms,
    };

    tracing::info!("Starting pipeline manager");
    let pipeline_manager = PipelineManager::new(pipeline_options);
    pipeline_manager.run().await?;
    tracing::info!("Pipeline manager completed");

    observability_manager.shutdown().await?;
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
