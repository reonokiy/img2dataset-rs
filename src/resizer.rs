use crate::sampler::{BatchSample, ShardSample};
use anyhow::{Result, anyhow};
use arrow::array::BinaryArray;
use clap::ValueEnum;
use futures::future::try_join_all;
use image::{DynamicImage, GenericImageView, imageops::FilterType};
use rayon::prelude::*;
use std::io::Cursor;
use std::str::FromStr;
use tokio::task::spawn_blocking;

/// Defines the available resizing modes.
#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ResizeMode {
    /// Resizes the image to fit within the target dimensions while
    /// preserving the aspect ratio, and adds a border if necessary.
    Border,

    /// No resizing, return the image as is.
    No,
}

impl FromStr for ResizeMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "border" => Ok(ResizeMode::Border),
            "no" => Ok(ResizeMode::No),
            _ => Err(anyhow!("Unsupported resize mode: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ImageFormat {
    Jpeg,
}

impl Into<image::ImageFormat> for ImageFormat {
    fn into(self) -> image::ImageFormat {
        match self {
            ImageFormat::Jpeg => image::ImageFormat::Jpeg,
        }
    }
}

/// The `Resizer` is responsible for resizing images.
#[derive(Clone)]
pub struct Resizer {
    options: ResizerOptions,
}

#[derive(Debug, Clone)]
pub struct ResizerOptions {
    pub resize_mode: ResizeMode,
    pub target_image_width: u32,
    pub target_image_height: u32,
    pub resize_only_if_bigger: bool,
    pub target_image_format: ImageFormat,
    pub target_image_quality: u8, // JPEG quality (0-100)
}

impl Resizer {
    pub fn new(options: ResizerOptions) -> Self {
        Self { options }
    }

    pub fn single_resize(&self, bytes: Vec<u8>) -> Result<Vec<u8>> {
        let img = image::load_from_memory(&bytes)?;

        let resized_img = match self.options.resize_mode {
            ResizeMode::No => img,
            ResizeMode::Border => self.resize_border(img),
        };

        let mut buf = Cursor::new(Vec::new());
        match self.options.target_image_format {
            ImageFormat::Jpeg => {
                let mut encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(
                    &mut buf,
                    self.options.target_image_quality,
                );
                encoder.encode_image(&resized_img)?;
            }
        }

        Ok(buf.into_inner())
    }

    pub async fn shard_resize(&self, sample: ShardSample) -> Result<ShardSample> {
        let resize_futures = sample
            .samples
            .iter()
            .map(|sample| self.batch_resize(sample.clone()));
        let resize_batchs = try_join_all(resize_futures).await?;
        Ok(ShardSample {
            shard_id: sample.shard_id,
            samples: resize_batchs,
        })
    }

    pub async fn batch_resize(&self, samples: BatchSample) -> Result<BatchSample> {
        let resizer = self.clone();
        let handler = spawn_blocking(move || {
            samples
                .bytes
                .par_iter()
                .enumerate()
                .map(|(i, data)| -> Result<(usize, Vec<u8>)> {
                    let bytes = data.value_data();
                    let img = resizer.single_resize(bytes.to_vec())?;
                    Ok((i, img))
                })
                .collect::<Vec<_>>()
        });

        let resized_data = handler.await?;
        let mut resized_bytes = vec![Vec::new(); samples.len];
        for result in resized_data.iter() {
            if let Ok((i, img)) = result {
                resized_bytes[*i] = img.clone();
            }
        }
        let bytes = BinaryArray::from_vec(
            resized_bytes
                .iter()
                .map(|b| b.as_slice())
                .collect::<Vec<_>>(),
        );

        Ok(BatchSample {
            len: samples.len,
            uuid: samples.uuid,
            url: samples.url,
            caption: samples.caption,
            bytes: Some(bytes),
            additional_columns: samples.additional_columns,
        })
    }

    /// Resizes an image using the "border" mode.
    /// This mode resizes the image to fit within the target dimensions while
    /// preserving the aspect ratio, and adds a border if necessary.
    fn resize_border(&self, img: DynamicImage) -> DynamicImage {
        let (width, height) = img.dimensions();
        let (target_width, target_height) = (
            self.options.target_image_width,
            self.options.target_image_height,
        );

        if width == target_width && height == target_height {
            return img;
        }

        let resized = img.resize(target_width, target_height, FilterType::Lanczos3);

        // The current implementation just resizes to the target size.
        // A more complete implementation would handle aspect ratio preservation
        // and border addition.
        resized
    }
}
