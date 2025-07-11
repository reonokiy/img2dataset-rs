use crate::downloader::Downloader;
use crate::reader::{InputFormat, Reader, ReaderBackend, ReaderConfig, Sample};
use crate::resizer::{ResizeMode, Resizer};
use crate::state::State;
use crate::writer::{OutputFormat, Writer};
use anyhow::Result;
use anyhow::anyhow;
use clap::{ArgGroup, Parser, Subcommand};
use futures::stream::StreamExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub mod downloader;
pub mod reader;
pub mod resizer;
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
#[clap(group(
    ArgGroup::new("input_source")
        .required(true)
        .args(["input_file", "input_directory", "input_hf_dataset"]),
))]
struct DebugReadArgs {
    #[clap(long)]
    input_file: Option<String>,
    #[clap(long)]
    input_directory: Option<String>,
    #[clap(long)]
    input_hf_dataset: Option<String>,
    #[clap(value_enum, long, default_value_t = InputFormat::Parquet)]
    input_format: InputFormat,
    #[clap(long, default_value = "url")]
    url_col: String,
    #[clap(long)]
    caption_col: Option<String>,
    #[clap(long)]
    save_additional_columns: bool,
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(group(
    ArgGroup::new("input_source")
        .required(true)
        .args(["input_file", "input_directory", "input_hf_dataset"]),
))]
struct Args {
    #[clap(long)]
    input_file: Option<String>,
    #[clap(long)]
    input_directory: Option<String>,
    #[clap(long)]
    input_hf_dataset: Option<String>,
    #[clap(long, default_value = "url")]
    url_col: String,
    #[clap(long)]
    caption_col: Option<String>,
    #[clap(long)]
    save_additional_columns: bool,
    #[clap(long, default_value = "output")]
    output_folder: String,
    #[clap(long, default_value_t = 100)]
    thread_count: usize,
    #[clap(long, default_value_t = 10000)]
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
    #[clap(value_enum, long, default_value_t = OutputFormat::Files)]
    output_format: OutputFormat,
    #[clap(value_enum, long, default_value_t = InputFormat::Parquet)]
    input_format: InputFormat,
    #[clap(long, default_value_t = 10_000_000)]
    max_items_to_download: usize,
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
    db_path: PathBuf,
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
    let (backend, path) = if let Some(dir) = args.input_directory {
        (ReaderBackend::Fs, dir)
    } else if let Some(file) = args.input_file {
        (ReaderBackend::Fs, file)
    } else if let Some(dataset) = args.input_hf_dataset {
        (ReaderBackend::HuggingFace, dataset)
    } else {
        unreachable!() // Clap group should prevent this
    };

    let reader_config = ReaderConfig {
        input_url_column_name: args.url_col,
        input_caption_column_name: args.caption_col,
        save_additional_columns: args.save_additional_columns,
        input_format: args.input_format,
    };

    let reader = Reader::new(backend, &path, reader_config)
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

    let state = Arc::new(State::new(Path::new(&args.db_path)).await?);
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

    let (backend, path) = if let Some(dir) = args.input_directory {
        (ReaderBackend::Fs, dir)
    } else if let Some(file) = args.input_file {
        // Assuming url_list is a file path for now, can be adjusted
        (ReaderBackend::Fs, file)
    } else if let Some(dataset) = args.input_hf_dataset {
        (ReaderBackend::HuggingFace, dataset)
    } else {
        unreachable!() // Clap group should prevent this
    };

    let reader_config = ReaderConfig {
        input_url_column_name: args.url_col.clone(),
        input_caption_column_name: args.caption_col.clone(),
        save_additional_columns: args.save_additional_columns,
        input_format: args.input_format,
    };

    let reader = Reader::new(backend, &path, reader_config)
        .map_err(|e| anyhow!("Failed to create reader: {}", e.to_string()))?;
    let sample_stream = reader
        .stream_samples()
        .await
        .map_err(|e| anyhow!("Failed to stream samples: {}", e.to_string()))?;

    let thread_count = args.thread_count;
    let shard_id_start = args.shard_id_start;
    let number_of_items_per_shard = args.number_of_items_per_shard;
    let resume = args.resume;

    let (tx, mut rx) = tokio::sync::mpsc::channel(thread_count);

    let writer = Arc::new(Writer::new(
        &args.output_folder,
        args.output_format,
        shard_id_start,
        args.save_additional_columns,
        &args.image_format,
    ));

    let writer_handle = tokio::spawn({
        let writer = writer.clone();
        let shard_size = number_of_items_per_shard;
        async move {
            let mut shard_id = shard_id_start;
            let mut buffer: Vec<(Sample, Vec<u8>, String)> = Vec::new();
            while let Some((sample, resized_image, mime_type)) = rx.recv().await {
                buffer.push((sample, resized_image, mime_type));
                if buffer.len() >= shard_size {
                    if let Err(e) = writer.write_shard(shard_id, buffer.clone()) {
                        log::error!("Error writing shard {}: {}", shard_id, e);
                    }
                    buffer.clear();
                    shard_id += 1;
                }
            }
            // Write any remaining items in the buffer
            if !buffer.is_empty() {
                if let Err(e) = writer.write_shard(shard_id, buffer.clone()) {
                    log::error!("Error writing final shard {}: {}", shard_id, e);
                }
            }
        }
    });

    sample_stream
        .for_each_concurrent(Some(thread_count), |sample_result| {
            let downloader = downloader.clone();
            let resizer = resizer.clone();
            let state = state.clone();
            let tx = tx.clone();

            async move {
                match sample_result {
                    Ok(sample) => {
                        if resume {
                            if let Ok(Some(true)) = state.get_download_state(&sample.url).await {
                                return; // Already succeeded, skip
                            }
                        }

                        let url = sample.url.clone();
                        let download_result = downloader.download(&url).await;
                        let success = download_result.is_ok();

                        // Update state immediately after download attempt
                        if let Err(e) = state.set_download_state(&url, success).await {
                            log::error!("Failed to set download state for {}: {}", url, e);
                        }

                        if let Ok((image_data, mime_type)) = download_result {
                            if let Ok(resized_image) = resizer.resize(&image_data) {
                                if tx.send((sample, resized_image, mime_type)).await.is_err() {
                                    log::error!("Failed to send sample to writer");
                                }
                            } else {
                                log::warn!("Error resizing {}", url);
                            }
                        } else {
                            log::warn!("Error downloading {}", url);
                        }
                    }
                    Err(e) => log::error!("Error reading sample: {}", e),
                }
            }
        })
        .await;

    drop(tx); // Close the channel
    writer_handle.await?; // Wait for the writer to finish

    Ok(())
}
