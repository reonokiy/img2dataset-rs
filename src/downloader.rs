use arrow::array::BinaryBuilder;
use futures::stream::{self, StreamExt};
use opentelemetry::{KeyValue, global};
use reqwest::{Client, Response};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_tracing::TracingMiddleware;
use std::time::Duration;
use std::{error::Error, sync::Arc};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::sampler::{BatchSample, ShardSample};

/// The `Downloader` is responsible for downloading images from URLs.
#[derive(Clone)]
pub struct Downloader {
    client: ClientWithMiddleware,
    options: DownloaderOptions,
}

#[derive(Debug, Clone)]
pub struct DownloaderOptions {
    pub thread: usize,
    pub timeout: u64,
    pub user_agent: String,
    pub disallowed_header_directives: Vec<String>,
    pub retries: u32,
}

impl Downloader {
    pub fn new(options: DownloaderOptions) -> Self {
        let reqwest_client = Client::builder()
            .user_agent(options.user_agent.clone())
            .timeout(Duration::from_secs(options.timeout))
            .build()
            .expect("Failed to create HTTP client");
        let client = ClientBuilder::new(reqwest_client)
            // Insert the tracing middleware
            .with(TracingMiddleware::default())
            .build();

        Self { client, options }
    }

    pub async fn shard_download(&self, shard: ShardSample) -> ShardSample {
        let downloaded_bytes = stream::iter(shard.samples.iter())
            .then(|sample| self.batch_download(sample.clone()))
            .collect()
            .await;
        ShardSample {
            original_filepath: shard.original_filepath,
            shard_id: shard.shard_id,
            samples: downloaded_bytes,
        }
    }

    pub async fn batch_download(&self, inputs: BatchSample) -> BatchSample {
        let mut blobs: Vec<Vec<u8>> = vec![vec![]; inputs.len];
        let mut set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(self.options.thread));

        for (i, url) in inputs.url.iter().enumerate() {
            let permit_semaphore = Arc::clone(&semaphore);
            let downloader = self.clone();
            let url = url.map(|u| u.to_string());
            set.spawn(async move {
                let _permit = permit_semaphore
                    .acquire()
                    .await
                    .expect("Failed to acquire semaphore");
                match url {
                    Some(url) => {
                        let result = downloader.single_download(url).await;
                        match result {
                            Ok(data) => (i, Some(data)),
                            Err(e) => (i, None),
                        }
                    }
                    None => (i, None),
                }
            });
        }

        while let Some(Ok((i, blob))) = set.join_next().await {
            match blob {
                Some(data) => blobs[i] = data,
                None => (),
            }
        }

        let mut array_builder = BinaryBuilder::new();
        for blob in blobs {
            if blob.is_empty() {
                array_builder.append_null();
            } else {
                array_builder.append_value(blob.as_slice());
            }
        }
        let data = array_builder.finish();

        BatchSample {
            len: inputs.len,
            uuid: inputs.uuid,
            url: inputs.url,
            bytes: Some(data),
            additional_columns: inputs.additional_columns,
        }
    }

    async fn single_download(&self, url: String) -> anyhow::Result<Vec<u8>> {
        let meter = global::meter(module_path!());
        let download_counter = meter
            .u64_counter("download.start")
            .with_description("Counter for download start events")
            .build();

        let response = match self.client.get(&url).send().await {
            Ok(resp) => resp,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Failed to download {}: {:?}",
                    url,
                    e.source()
                ));
            }
        };
        download_counter.add(
            1,
            &[
                KeyValue::new("url", url.clone()),
                KeyValue::new("user_agent", self.options.user_agent.clone()),
                KeyValue::new("status", response.status().to_string()),
                KeyValue::new(
                    "content_length",
                    response.content_length().unwrap_or(0).to_string(),
                ),
            ],
        );
        if self.is_disallowed(&response) {
            tracing::debug!("Download {} disallowed by X-Robots-Tag", url);
            return Err(anyhow::anyhow!(
                "Failed to download {}: Disallowed by X-Robots-Tag",
                url
            ));
        }
        if !response.status().is_success() {
            tracing::debug!("Failed to download {}: {}", url, response.status());
            return Err(anyhow::anyhow!(
                "Failed to download {}: {}",
                url,
                response.status()
            ));
        }

        Ok(response.bytes().await?.to_vec())
    }

    fn is_disallowed(&self, response: &Response) -> bool {
        if self.options.disallowed_header_directives.is_empty() {
            return false;
        }

        for value in response.headers().get_all("X-Robots-Tag") {
            if let Ok(value_str) = value.to_str() {
                let parts: Vec<&str> = value_str.split(':').collect();
                let (ua_token, directives_str) = if parts.len() == 2 {
                    (Some(parts[0].trim().to_lowercase()), parts[1])
                } else {
                    (None, parts[0])
                };

                if ua_token.is_none() || ua_token == Some(self.options.user_agent.clone()) {
                    let directives = directives_str.split(',').map(|d| d.trim().to_lowercase());
                    for directive in directives {
                        if self
                            .options
                            .disallowed_header_directives
                            .contains(&directive)
                        {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }
}
