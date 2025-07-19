use crate::sampler::BatchSample;
use anyhow::{Result, anyhow};
use bytes::Bytes;
use clap::ValueEnum;
use image::{DynamicImage, GenericImageView, imageops::FilterType};
use rayon::prelude::*;
use std::io::Cursor;
use std::str::FromStr;
use tokio::task::spawn_blocking;
use tracing::instrument;

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
    pub reencode: bool,
    pub resize_mode: ResizeMode,
    pub target_image_width: u32,
    pub target_image_height: u32,
    pub resize_only_if_bigger: bool,
    pub target_image_format: ImageFormat,
    pub target_image_quality: u8, // JPEG quality (0-100)
}

impl Resizer {
    #[instrument]
    pub fn new(options: ResizerOptions) -> Self {
        Self { options }
    }

    #[instrument(skip(self))]
    pub fn single_resize(&self, bytes: Bytes) -> Result<Bytes> {
        if !self.options.reencode && self.options.resize_mode == ResizeMode::No {
            return Ok(bytes);
        }

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

        Ok(Bytes::from(buf.into_inner()))
    }

    pub async fn batch_resize(&self, sample: BatchSample) -> Result<BatchSample> {
        let resizer = self.clone();
        let mut sample_clone = sample.clone();
        let handler = spawn_blocking(move || {
            sample
                .bytes()
                .par_iter()
                .map(|data| -> Option<Bytes> {
                    match data {
                        Some(data) => resizer.single_resize(data.clone()).ok(),
                        None => None,
                    }
                })
                .collect::<Vec<_>>()
        });

        let resized_data = handler.await?;

        sample_clone.put_bytes(resized_data);
        Ok(sample_clone)
    }

    /// Resizes an image using the "border" mode.
    /// This mode resizes the image to fit within the target dimensions while
    /// preserving the aspect ratio, and adds a border if necessary.
    #[instrument(skip(self))]
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
