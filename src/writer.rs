use crate::reader::Sample;
use anyhow::Result;
use arrow::array::{Array, BinaryArray, StringArray};
use arrow::record_batch::RecordBatch;
use clap::ValueEnum;
use parquet::arrow::ArrowWriter;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, ValueEnum)]
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
#[derive(Debug, Clone)]
pub struct Writer {
    output_folder: PathBuf,
    output_format: OutputFormat,
    shard_id_start: usize,
    save_additional_columns: bool,
    image_format: String,
}

impl Writer {
    pub fn new(
        output_folder: &str,
        output_format: OutputFormat,
        shard_id_start: usize,
        save_additional_columns: bool,
        image_format: &str,
    ) -> Self {
        Self {
            output_folder: PathBuf::from(output_folder),
            output_format,
            shard_id_start,
            save_additional_columns,
            image_format: image_format.to_string(),
        }
    }

    pub fn write_shard(&self, shard_id: usize, data: Vec<(Sample, Vec<u8>, String)>) -> Result<()> {
        let shard_name = format!("{:0>5}", shard_id);
        std::fs::create_dir_all(&self.output_folder)?;
        match self.output_format {
            OutputFormat::Files => self.write_files(&shard_name, &data)?,
            OutputFormat::Webdataset => self.write_webdataset(&shard_name, &data)?,
            OutputFormat::Parquet => self.write_parquet(&shard_name, &data)?,
        }
        Ok(())
    }

    fn write_files(&self, shard_name: &str, data: &[(Sample, Vec<u8>, String)]) -> Result<()> {
        let shard_folder = self.output_folder.join(shard_name);
        fs::create_dir_all(&shard_folder)?;

        for (i, (sample, image_data, _)) in data.iter().enumerate() {
            let image_path = shard_folder.join(format!("{:0>5}.{}", i, self.image_format));
            fs::write(image_path, image_data)?;

            let caption_path = shard_folder.join(format!("{:0>5}.txt", i));
            if let Some(caption) = &sample.caption {
                fs::write(caption_path, caption)?;
            }

            if self.save_additional_columns {
                let json_path = shard_folder.join(format!("{:0>5}.json", i));
                let json_data = serde_json::to_string(&sample.additional_columns)?;
                fs::write(json_path, json_data)?;
            }
        }
        Ok(())
    }

    fn write_webdataset(&self, shard_name: &str, data: &[(Sample, Vec<u8>, String)]) -> Result<()> {
        let tar_path = self.output_folder.join(format!("{}.tar", shard_name));
        let file = fs::File::create(tar_path)?;
        let mut tar_builder = tar::Builder::new(file);

        for (i, (sample, image_data, _)) in data.iter().enumerate() {
            let key = format!("{:0>5}", i);

            let mut header = tar::Header::new_gnu();
            header.set_size(image_data.len() as u64);
            header.set_cksum();
            tar_builder.append_data(
                &mut header,
                format!("{}.{}", key, self.image_format),
                image_data.as_slice(),
            )?;

            if let Some(caption) = &sample.caption {
                let caption_data = caption.as_bytes();
                let mut header = tar::Header::new_gnu();
                header.set_size(caption_data.len() as u64);
                header.set_cksum();
                tar_builder.append_data(&mut header, format!("{}.txt", key), caption_data)?;
            }

            if self.save_additional_columns {
                let json_data = serde_json::to_string(&sample.additional_columns)?;
                let json_bytes = json_data.as_bytes();
                let mut header = tar::Header::new_gnu();
                header.set_size(json_bytes.len() as u64);
                header.set_cksum();
                tar_builder.append_data(&mut header, format!("{}.json", key), json_bytes)?;
            }
        }
        tar_builder.finish()?;
        Ok(())
    }

    fn write_parquet(&self, shard_name: &str, data: &[(Sample, Vec<u8>, String)]) -> Result<()> {
        let file_path = self.output_folder.join(format!("{}.parquet", shard_name));
        let file = fs::File::create(file_path)?;

        let url_array: StringArray = data.iter().map(|(s, _, _)| Some(s.url.as_str())).collect();
        let caption_array: StringArray =
            data.iter().map(|(s, _, _)| s.caption.as_deref()).collect();
        let image_array: BinaryArray = data
            .iter()
            .map(|(_, img, _)| Some(img.as_slice()))
            .collect();

        let mut columns: Vec<(&str, Arc<dyn Array>)> = vec![
            ("url", Arc::new(url_array)),
            ("caption", Arc::new(caption_array)),
            ("image", Arc::new(image_array)),
        ];

        if self.save_additional_columns {
            // This part is tricky because additional_columns can have different types.
            // For simplicity, we'll serialize them to JSON strings.
            let additional_columns_json: StringArray = data
                .iter()
                .map(|(s, _, _)| serde_json::to_string(&s.additional_columns).unwrap_or_default())
                .map(|s| Some(s))
                .collect();
            columns.push(("additional_columns", Arc::new(additional_columns_json)));
        }

        let batch = RecordBatch::try_from_iter(columns)?;
        let schema = batch.schema();

        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(())
    }
}
