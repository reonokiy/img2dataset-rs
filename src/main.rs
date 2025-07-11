use crate::downloader::Downloader;
use crate::reader::{InputFormat, Reader};
use crate::resizer::{ResizeMode, Resizer};
use crate::writer::{OutputFormat, Writer};
use anyhow::Result;
use clap::Parser;
use futures::stream::StreamExt;
use std::sync::Arc;

pub mod downloader;
pub mod reader;
pub mod resizer;
pub mod writer;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_value = "")]
    url_list: String,
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
    #[clap(long, default_value_t = 0)]
    start_shard: usize,
    #[clap(long)]
    end_shard: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

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

    let writer = Arc::new(Writer::new(
        &args.output_folder,
        args.output_format,
        args.shard_id_start,
        args.save_additional_columns,
        &args.image_format,
    ));

    let reader = Reader::new(
        &args.url_list,
        args.input_format,
        &args.url_col,
        args.caption_col.as_deref(),
        args.save_additional_columns,
        args.number_of_items_per_shard,
        0, // shard_count is not used anymore
        args.start_shard,
        args.end_shard,
    )
    .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let shard_stream = reader.shards();

    shard_stream
        .enumerate()
        .for_each_concurrent(Some(args.thread_count), |(i, shard_result)| {
            let downloader = downloader.clone();
            let resizer = resizer.clone();
            let writer = writer.clone();
            let shard_id = args.shard_id_start + i;

            async move {
                match shard_result {
                    Ok(shard) => {
                        let download_futures: Vec<_> = shard
                            .into_iter()
                            .map(|sample| {
                                let downloader = downloader.clone();
                                let resizer = resizer.clone();
                                tokio::spawn(async move {
                                    let url = sample.url.clone();
                                    match downloader.download(&url).await {
                                        Ok((image_data, mime_type)) => {
                                            match resizer.resize(&image_data) {
                                                Ok(resized_image) => {
                                                    Some((sample, resized_image, mime_type))
                                                }
                                                Err(e) => {
                                                    log::warn!("Error resizing {}: {}", url, e);
                                                    None
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            log::warn!("Error downloading {}: {}", url, e);
                                            None
                                        }
                                    }
                                })
                            })
                            .collect();

                        let results = futures::future::join_all(download_futures).await;
                        let mut shard_data = Vec::new();
                        for result in results {
                            if let Ok(Some(data)) = result {
                                shard_data.push(data);
                            }
                        }

                        if !shard_data.is_empty() {
                            if let Err(e) = writer.write_shard(shard_id, shard_data) {
                                log::error!("Error writing shard {}: {}", shard_id, e);
                            }
                        }
                    }
                    Err(e) => log::error!("Error reading shard: {}", e),
                }
            }
        })
        .await;

    Ok(())
}
