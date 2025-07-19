use crate::writer::WriterOptions;
use anyhow::Result;
use async_stream::stream;
use chrono::Utc;

use futures::{Stream, StreamExt};
use opendal::{Operator, services::Fs};
use parquet::arrow::async_writer::AsyncFileWriter;
use std::{collections::HashSet, sync::Arc};
use tokio::io::{BufReader, BufWriter};
use tokio_util::compat::{FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};
use tracing::{error, info};

#[derive(Debug, Clone)]
pub struct SyncOptions {
    pub root: String,
    pub skip_recent_seconds: i64,
    pub sync_and_delete: bool,
    pub reader_chunk_size: usize,
    pub writer_chunk_size: usize,
    pub reader_concurrent: usize,
    pub writer_concurrent: usize,
    pub verify_content_length: bool,
    pub concurrency: usize,
}

#[derive(Debug, Clone)]
pub struct Synchronizer {
    iop: Operator,
    oop: Operator,
    sync_options: Arc<SyncOptions>,
    writer_options: Arc<WriterOptions>,
}

impl Synchronizer {
    pub fn new(sync_options: SyncOptions, writer_options: WriterOptions) -> Result<Self> {
        let iop = writer_options.get_operator()?;
        let oop = Operator::new(Fs::default().root(&sync_options.root))?.finish();

        Ok(Self {
            iop,
            oop,
            sync_options: Arc::new(sync_options),
            writer_options: Arc::new(writer_options),
        })
    }

    pub async fn list_to_sync(&self) -> Result<impl Stream<Item = String>> {
        // scan existing files in output root
        let mut files = HashSet::new();
        let mut olister = self.oop.lister_with("").recursive(true).await?;
        let mut ilister = self.iop.lister_with("").recursive(true).await?;
        while let Some(Ok(entry)) = olister.next().await {
            files.insert(entry.path().to_string());
        }

        let stream = stream! {
            while let Some(Ok(entry)) = ilister.next().await {
                let metadata = entry.metadata();
                if metadata.is_dir() {
                    continue;
                }
                if let Some(last_modified) = metadata.last_modified() {
                    if last_modified.timestamp()
                        > Utc::now().timestamp() - self.sync_options.skip_recent_seconds
                    {
                        continue;
                    }
                }
                if !files.contains(entry.path()) {
                    yield entry.path().to_string();
                }
            }
        };
        Ok(stream)
    }

    pub async fn single_sync(&self, path: &str) -> Result<()> {
        let reader = self
            .iop
            .reader_with(path)
            .chunk(self.sync_options.reader_chunk_size)
            .concurrent(self.sync_options.reader_concurrent)
            .await?;
        let tmp_path = format!("{path}.tmp");
        let writer = self
            .oop
            .writer_with(&tmp_path)
            .chunk(self.sync_options.writer_chunk_size)
            .concurrent(self.sync_options.writer_concurrent)
            .await?;
        let mut tokio_reader = BufReader::new(reader.into_futures_async_read(..).await?.compat());
        let mut tokio_writer = BufWriter::new(writer.into_futures_async_write().compat_write());
        tokio::io::copy_buf(&mut tokio_reader, &mut tokio_writer).await?;
        tokio_writer.complete().await?;
        self.oop.rename(&tmp_path, path).await?;
        if self.sync_options.sync_and_delete {
            match self.single_delete(path).await {
                Ok(()) => {}
                Err(e) => {
                    error!("Failed to delete {}: {}", path, e);
                }
            }
        }
        Ok(())
    }

    pub async fn single_delete(&self, path: &str) -> Result<()> {
        let istat = self.iop.stat(path).await?;
        let ostat = self.oop.stat(path).await?;
        if self.sync_options.verify_content_length {
            if istat.content_length() != ostat.content_length() {
                return Err(anyhow::anyhow!("Content length mismatch for {}", path));
            }
        }
        self.iop.delete(path).await?;
        Ok(())
    }

    pub async fn sync(&self) -> Result<()> {
        let stream = self.list_to_sync().await?;
        let sync_options = self.sync_options.clone();

        stream
            .for_each_concurrent(sync_options.concurrency, |path| {
                let self_clone = self.clone();

                async move {
                    if let Err(e) = self_clone.single_sync(&path).await {
                        error!("Failed to sync {}: {}", path, e);
                    } else {
                        info!("Successfully synced {}", path);
                    }
                }
            })
            .await;

        Ok(())
    }
}
