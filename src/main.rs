use crate::downloader::Downloader;
use crate::reader::{InputFormat, Reader, ReaderBackend, ReaderOptions};
use crate::resizer::{ResizeMode, Resizer};
use crate::sampler::OutputSample;
use crate::state::State;
use crate::writer::{OutputBackend, OutputFormat, Writer, WriterOptions};
use anyhow::Result;
use anyhow::anyhow;
use clap::{Parser, Subcommand};
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

pub mod downloader;
pub mod reader;
pub mod resizer;
mod sampler;
mod state;
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
    #[clap(long)]
    resume: bool,
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
    #[clap(long, default_value_t = 10)]
    timeout: u64,
    #[clap(long, default_value_t = 0)]
    retries: u32,
    #[clap(long)]
    user_agent_token: Option<String>,
    #[clap(long)]
    disallowed_header_directives: Option<Vec<String>>,
    #[clap(long)]
    resume: bool,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match cli.command {
        Command::Run(args) => run_downloader(args).await?,
        Command::Debug(debug_cmd) => match debug_cmd.command {
            DebugSubcommand::Read(debug_args) => {
                debug_read(debug_args).await?;
            }
        },
    }

    Ok(())
}

async fn debug_read(args: DebugReadArgs) -> Result<()> {
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
    while let Some(sample) = stream.next().await {
        count += 1;
        match sample {
            Ok(s) => {
                println!("Processing sample {}: {:?}", count, s);
            }
            Err(e) => log::error!("Error reading sample: {}", e),
        }
    }

    Ok(())
}

async fn run_downloader(args: Args) -> Result<()> {
    log::info!("Starting downloader with args: {:?}", args);

    let state = Arc::new(State::new(Path::new(&args.state_db_path)).await?);

    let reader_options = ReaderOptions {
        root: args.input_root,
        format: args.input_format,
        backend: args.input_backend,
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

    let writer_options = WriterOptions {
        root: args.output_root,
        format: args.output_format,
        backend: args.output_backend,
        s3_bucket: args.output_s3_bucket,
        s3_region: args.output_s3_region,
        s3_access_key: args.output_s3_access_key,
        s3_secret_key: args.output_s3_secret_key,
        s3_endpoint: args.output_s3_endpoint,
        writer_thread_count: args.writer_thread_count,
        webdataset_shard_prefix: args.writer_webdataset_shard_prefix,
        webdataset_shard_bits_num: args.writer_webdataset_shard_bits_num,
    };
    let writer = Arc::new(Writer::new(writer_options)?);

    let resizer = Arc::new(Resizer::new(
        args.resize_mode,
        args.image_size,
        args.resize_only_if_bigger,
        &args.image_format,
    ));

    let downloader = Arc::new(Downloader::new(
        args.timeout,
        args.user_agent_token,
        args.disallowed_header_directives,
        args.retries,
    ));

    let sample_stream = reader
        .stream_samples()
        .await
        .map_err(|e| anyhow!("Failed to stream samples: {}", e.to_string()))?;

    let downloader_thread_count = args.downloader_thread_count;
    let number_of_items_per_shard = args.number_of_items_per_shard;

    let download_stream = sample_stream
        .map_err(|e| anyhow!(e.to_string()))
        .map_ok(move |sample| {
            let downloader = downloader.clone();
            async move { downloader.download(sample).await }
        })
        .try_buffer_unordered(downloader_thread_count);

    let processing_stream: Pin<Box<dyn Stream<Item = Result<OutputSample>> + Send>> =
        if args.resize_mode != ResizeMode::No {
            let resizer = resizer.clone();
            Box::pin(download_stream.and_then(move |mut downloaded_sample| {
                let resizer = resizer.clone();
                async move {
                    downloaded_sample.download_data =
                        resizer.resize(&downloaded_sample.download_data)?;
                    Ok(downloaded_sample)
                }
            }))
        } else {
            log::info!("No resizing will be performed.");
            Box::pin(download_stream)
        };

    let final_stream = Box::pin(processing_stream.filter_map(|res| async {
        match res {
            Ok(sample) => Some(sample),
            Err(e) => {
                log::error!("An error occurred during processing: {}", e);
                None
            }
        }
    }));

    let chunk_stream = final_stream.chunks(number_of_items_per_shard);

    chunk_stream
        .enumerate()
        .for_each_concurrent(Some(args.writer_thread_count), |(i, chunk)| {
            let writer = writer.clone();
            let state = state.clone();
            let shard_id = args.shard_id_start + i;
            async move {
                log::info!("Processing chunk {} of {} samples", shard_id, chunk.len());
                let result = match writer.output_format() {
                    OutputFormat::Files => {
                        writer
                            .write_streaming_samples_to_files(stream::iter(chunk), &state)
                            .await
                    }
                    OutputFormat::Webdataset => {
                        writer
                            .write_streaming_samples_to_tar(stream::iter(chunk), shard_id, &state)
                            .await
                    }
                    OutputFormat::Parquet => {
                        log::warn!("Parquet output format is not yet implemented.");
                        Ok(())
                    }
                };
                if let Err(e) = result {
                    log::error!("Failed to write chunk {}: {}", shard_id, e);
                }
            }
        })
        .await;

    Ok(())
}
