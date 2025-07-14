use crate::manager::{PipelineManager, PipelineOptions};
use crate::observability::{ObservabilityManager, ObservabilityOptions};
use crate::reader::{InputFormat, Reader, ReaderBackend, ReaderOptions};
use crate::resizer::{ImageFormat, ResizeMode, ResizerOptions};
use crate::writer::{OutputBackend, OutputFormat, WriterOptions};
use anyhow::Result;
use anyhow::anyhow;
use clap::{Parser, Subcommand};
use futures::stream::StreamExt;
use std::path::PathBuf;
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
    #[clap(value_enum, long, default_value_t = ImageFormat::Jpeg)]
    image_format: ImageFormat,
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

    // Pipeline options
    #[clap(long, default_value_t = 1024)]
    buffer_size: usize,
    #[clap(long, default_value_t = 1)]
    min_resizers: usize,
    #[clap(long, default_value_t = 10)]
    max_resizers: usize,
    #[clap(long, default_value_t = 1)]
    min_writers: usize,
    #[clap(long, default_value_t = 10)]
    max_writers: usize,
    #[clap(long, default_value_t = 0.9)]
    memory_threshold: f32,
    #[clap(long, default_value_t = 0.2)]
    scale_up_threshold: f32,
    #[clap(long, default_value_t = 0.8)]
    scale_down_threshold: f32,
    #[clap(long, default_value_t = 5000)]
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
        batch_per_shard: args.number_of_items_per_shard,
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

    tracing::info!(
        "Starting img2dataset-rs with {} downloader threads, {} items per shard",
        args.downloader_thread_count,
        args.number_of_items_per_shard
    );

    if args.resume_from_shard_number > 0 || args.resume_from_sample_number > 0 {
        tracing::info!(
            "Resume mode enabled: starting from shard {}, sample {}",
            args.resume_from_shard_number,
            args.resume_from_sample_number
        );
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
        batch_per_shard: args.number_of_items_per_shard,
    };

    let resizer_options = ResizerOptions {
        resize_mode: args.resize_mode,
        target_image_width: args.image_size,
        target_image_height: args.image_size,
        resize_only_if_bigger: args.resize_only_if_bigger,
        target_image_format: args.image_format,
        target_image_quality: 95, // High quality JPEG
    };

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
        writer_thread_count: args.writer_thread_count,
        webdataset_shard_bits_num: args.writer_webdataset_shard_bits_num,
        webdataset_shard_prefix: args.writer_webdataset_shard_prefix,
    };

    let pipeline_options = PipelineOptions {
        reader_options,
        resizer_options,
        writer_options,
        buffer_size: args.buffer_size,
        min_resizers: args.min_resizers,
        max_resizers: args.max_resizers,
        min_writers: args.min_writers,
        max_writers: args.max_writers,
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
