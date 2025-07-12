use anyhow::{Result, anyhow};
use clap::ValueEnum;
use image::{DynamicImage, GenericImageView, ImageFormat, imageops::FilterType};
use std::io::Cursor;
use std::str::FromStr;

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

/// The `Resizer` is responsible for resizing images.
#[derive(Clone)]
pub struct Resizer {
    image_size: u32,
    resize_mode: ResizeMode,
    resize_only_if_bigger: bool,
    image_format: String,
}

impl Resizer {
    /// Creates a new `Resizer`.
    pub fn new(
        resize_mode: ResizeMode,
        image_size: u32,
        resize_only_if_bigger: bool,
        image_format: &str,
    ) -> Self {
        Self {
            resize_mode,
            image_size,
            resize_only_if_bigger,
            image_format: image_format.to_string(),
        }
    }

    /// Resizes an image.
    pub fn resize(&self, image_data: &[u8]) -> Result<Vec<u8>> {
        match self.resize_mode {
            ResizeMode::No => Ok(image_data.to_vec()),
            ResizeMode::Border => {
                let img = image::load_from_memory(image_data)?;
                let (width, height) = img.dimensions();

                if self.resize_only_if_bigger
                    && width <= self.image_size
                    && height <= self.image_size
                {
                    return Ok(image_data.to_vec());
                }

                let resized_img = self.resize_border(img);

                let mut buf = Cursor::new(Vec::new());
                resized_img.write_to(&mut buf, ImageFormat::Jpeg)?;
                Ok(buf.into_inner())
            }
        }
    }

    /// Resizes an image using the "border" mode.
    /// This mode resizes the image to fit within the target dimensions while
    /// preserving the aspect ratio, and adds a border if necessary.
    fn resize_border(&self, img: DynamicImage) -> DynamicImage {
        let (width, height) = img.dimensions();
        let (target_width, target_height) = (self.image_size, self.image_size);

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
