use crate::downloader::Downloader;
use crate::resizer::{ResizeMode, Resizer};
use crate::writer::{OutputFormat, Writer};
use clap::Parser;
use futures::{Stream, StreamExt, stream};
use opendal::Operator;
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    file::{reader::FileReader, serialized_reader::SerializedFileReader},
};
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::pin::Pin;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub mod downloader;
pub mod reader;
pub mod resizer;
pub mod writer;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_value = "")]
    url_list: String,
    #[clap(long, default_value = "")]
    url_col: String,
    #[clap(long, default_value = "")]
    caption_col: Option<String>,
    #[clap(long, default_value = "false")]
    save_additional_columns: bool,
    #[clap(long, default_value = "files")]
    output_folder: String,
    #[clap(long, default_value_t = 10000)]
    thread_count: usize,
    #[clap(long, default_value_t = 256)]
    number_of_items_per_shard: usize,
    #[clap(long, default_value = "1G")]
    max_shard_size: String,
    #[clap(long, default_value_t = 0)]
    shard_id_start: usize,
    #[clap(long, default_value = "png")]
    image_format: String,
    #[clap(long, default_value = "resize")]
    resize_mode: String,
    #[clap(long, default_value_t = 256)]
    resize_only_if_bigger: bool,
    #[clap(long, default_value_t = 256)]
    image_size: u32,
    #[clap(long, default_value = "parquet")]
    output_format: String,
    #[clap(long, default_value = "parquet")]
    input_format: String,
    #[clap(long, default_value_t = 10_000_000)]
    max_items_to_download: usize,
    #[clap(long, default_value_t = 0)]
    timeout: u64,
    #[clap(long)]
    enable_wandb: bool,
    #[clap(long, default_value = "img2dataset")]
    wandb_project: String,
    #[clap(long, default_value = "")]
    wandb_entity: String,
    #[clap(long, default_value = "")]
    wandb_run_name: String,
    #[clap(long, default_value = "")]
    s3_endpoint_url: String,
    #[clap(long, default_value = "")]
    s3_access_key_id: String,
    #[clap(long, default_value = "")]
    s3_secret_access_key: String,
    #[clap(long, default_value = "")]
    s3_region: String,
    #[clap(long, default_value_t = 0)]
    shard_count: usize,
    #[clap(long, default_value_t = 0)]
    start_shard: usize,
    #[clap(long)]
    end_shard: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let resize_mode_enum = match args.resize_mode.as_str() {
        "no" => ResizeMode::No,
        "keep_ratio" => ResizeMode::KeepRatio,
        "center_crop" => ResizeMode::CenterCrop,
        "border" => ResizeMode::Border,
        _ => panic!("Invalid resize mode"),
    };

    let output_format_enum = match args.output_format.as_str() {
        "files" => OutputFormat::Files,
        "webdataset" => OutputFormat::Webdataset,
        "parquet" => OutputFormat::Parquet,
        _ => panic!("Invalid output format"),
    };

    let input_format_enum = match args.input_format.as_str() {
        "txt" => reader::InputFormat::Txt,
        "csv" => reader::InputFormat::Csv,
        "tsv" => reader::InputFormat::Tsv,
        "parquet" => reader::InputFormat::Parquet,
        _ => panic!("Invalid input format"),
    };

    let resizer_instance = Arc::new(Resizer::new(
        resize_mode_enum,
        args.image_size,
        args.resize_only_if_bigger,
        &args.image_format,
    )?);

    let downloader_instance = Arc::new(Downloader::new(args.timeout));
    let writer_instance = Arc::new(Writer::new(
        args.output_folder.clone(),
        output_format_enum,
        args.shard_id_start,
        args.save_additional_columns,
        args.image_format.clone(),
    )?);

    let reader_instance = reader::Reader::new(
        &args.url_list,
        input_format_enum,
        args.url_col.clone(),
        args.caption_col.clone(),
        args.save_additional_columns,
        args.number_of_items_per_shard,
        args.max_shard_size.clone(),
        args.shard_count,
        args.start_shard,
        args.end_shard,
    )?;

    let mut shard_stream = Box::pin(reader_instance.shards());

    let mut shard_id = args.shard_id_start;
    while let Some(shard) = shard_stream.next().await {
        let shard = shard?;
        let tasks: Vec<_> = shard
            .into_iter()
            .map(|sample| {
                let resizer = resizer_instance.clone();
                let downloader = downloader_instance.clone();
                tokio::spawn(async move {
                    let (image_data, mime_type) = match downloader.download(&sample.url).await {
                        Ok(data) => data,
                        Err(e) => {
                            println!("Error downloading {}: {}", sample.url, e);
                            return None;
                        }
                    };
                    let resized_image = match resizer.resize(image_data) {
                        Ok(data) => data,
                        Err(e) => {
                            println!("Error resizing {}: {}", sample.url, e);
                            return None;
                        }
                    };
                    Some((sample, resized_image, mime_type))
                })
            })
            .collect();

        let results = futures::future::join_all(tasks).await;
        let mut shard_data = Vec::new();
        for result in results {
            if let Ok(Some(data)) = result {
                shard_data.push(data);
            }
        }

        if !shard_data.is_empty() {
            writer_instance.write_shard(shard_id, shard_data)?;
            shard_id += 1;
        }
    }

    Ok(())
}
