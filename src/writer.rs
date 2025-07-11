use anyhow::Result;
use arrow::{
    array::{BinaryBuilder, StringBuilder, UInt64Builder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use std::fs::{self, File};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tar::Builder;

#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Files,
    Webdataset,
    Parquet,
    // Tfrecord,
    // Dummy,
}

impl FromStr for OutputFormat {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "files" => Ok(OutputFormat::Files),
            "webdataset" => Ok(OutputFormat::Webdataset),
            "parquet" => Ok(OutputFormat::Parquet),
            _ => Err(anyhow::anyhow!("Unsupported output format: {}", s)),
        }
    }
}

/// The `Writer` is responsible for writing samples to the output destination.
pub struct Writer {
    output_folder: PathBuf,
    output_format: OutputFormat,
}

impl Writer {
    /// Creates a new `Writer`.
    pub fn new(output_folder: &str, output_format: OutputFormat) -> Self {
        Self {
            output_folder: PathBuf::from(output_folder),
            output_format,
        }
    }

    /// Writes a shard of samples to the output destination.
    pub fn write_shard(
        &mut self,
        shard_id: u64,
        samples: Vec<(i64, Vec<u8>, Option<String>, String)>,
    ) -> Result<()> {
        match self.output_format {
            OutputFormat::Files => self.write_files(shard_id, samples),
            OutputFormat::Webdataset => self.write_webdataset(shard_id, samples),
            OutputFormat::Parquet => self.write_parquet(shard_id, samples),
        }
    }

    fn write_files(
        &self,
        shard_id: u64,
        samples: Vec<(i64, Vec<u8>, Option<String>, String)>,
    ) -> Result<()> {
        let shard_folder = self.output_folder.join(format!("{:0>5}", shard_id));
        fs::create_dir_all(&shard_folder)?;

        for (id, image_data, caption, mime_type) in samples {
            let extension = mime_type.split('/').last().unwrap_or("jpg");
            let image_path = shard_folder.join(format!("{}.{}", id, extension));
            fs::write(image_path, image_data)?;

            if let Some(caption) = caption {
                let caption_path = shard_folder.join(format!("{}.txt", id));
                fs::write(caption_path, caption)?;
            }
        }
        Ok(())
    }

    fn write_webdataset(
        &self,
        shard_id: u64,
        samples: Vec<(i64, Vec<u8>, Option<String>, String)>,
    ) -> Result<()> {
        let shard_path = self
            .output_folder
            .join(format!("{:0>5}.tar", shard_id));
        let file = File::create(shard_path)?;
        let mut tar_builder = Builder::new(file);

        for (id, image_data, caption, mime_type) in samples {
            let extension = mime_type.split('/').last().unwrap_or("jpg");
            let mut header = tar::Header::new_gnu();
            header.set_size(image_data.len() as u64);
            header.set_cksum();
            tar_builder.append_data(
                &mut header,
                format!("{}.{}", id, extension),
                image_data.as_slice(),
            )?;

            if let Some(caption) = caption {
                let mut header = tar::Header::new_gnu();
                header.set_size(caption.len() as u64);
                header.set_cksum();
                tar_builder.append_data(
                    &mut header,
                    format!("{}.txt", id),
                    caption.as_bytes(),
                )?;
            }
        }
        Ok(())
    }

    fn write_parquet(
        &self,
        shard_id: u64,
        samples: Vec<(i64, Vec<u8>, Option<String>, String)>,
    ) -> Result<()> {
        let shard_path = self
            .output_folder
            .join(format!("{:0>5}.parquet", shard_id));
        let file = File::create(shard_path)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("image", DataType::Binary, true),
            Field::new("caption", DataType::Utf8, true),
            Field::new("mime_type", DataType::Utf8, true),
        ]));

        let mut id_builder = UInt64Builder::new();
        let mut image_builder = BinaryBuilder::new();
        let mut caption_builder = StringBuilder::new();
        let mut mime_type_builder = StringBuilder::new();

        for (id, image_data, caption, mime_type) in samples {
            id_builder.append_value(id as u64);
            image_builder.append_value(image_data);
            if let Some(caption) = caption {
                caption_builder.append_value(caption);
            } else {
                caption_builder.append_null();
            }
            mime_type_builder.append_value(mime_type);
        }

        let id_array = id_builder.finish();
        let image_array = image_builder.finish();
        let caption_array = caption_builder.finish();
        let mime_type_array = mime_type_builder.finish();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(image_array),
                Arc::new(caption_array),
                Arc::new(mime_type_array),
            ],
        )?;

        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(())
    }
}
