use arrow::array::BinaryArray;
use reqwest::{Client, Response};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::sampler::{BatchSample, InputSample, OutputSample, SampleStatus};

/// The `Downloader` is responsible for downloading images from URLs.
#[derive(Clone)]
pub struct Downloader {
    client: Client,
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
    /// Creates a new `Downloader`.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The timeout for the HTTP client in seconds.
    /// * `user_agent_token` - An optional custom User-Agent token.
    /// * `disallowed_header_directives` - Optional list of disallowed X-Robots-Tag directives.
    /// * `retries` - The number of times to retry the download on failure.
    pub fn new(options: DownloaderOptions) -> Self {
        let client = Client::builder()
            .user_agent(options.user_agent.clone())
            .timeout(Duration::from_secs(options.timeout))
            .build()
            .expect("Failed to create HTTP client");

        Self { client, options }
    }

    pub async fn download_batch(&self, inputs: BatchSample) -> anyhow::Result<BatchSample> {
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
                            Err(e) => {
                                log::warn!("{}", e.to_string());
                                (i, None)
                            }
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

        let data = BinaryArray::from_vec(blobs.iter().map(|b| b.as_slice()).collect::<Vec<_>>());

        Ok(BatchSample {
            len: inputs.len,
            uuid: inputs.uuid,
            url: inputs.url,
            caption: inputs.caption,
            bytes: Some(data),
            additional_columns: inputs.additional_columns,
        })
    }

    /// Downloads an image from a URL.
    ///
    /// # Arguments
    ///
    /// * `input` - The input sample containing the URL to download.
    ///
    /// # Returns
    ///
    /// An `OutputSample` with either success or failure status.
    pub async fn download(&self, input: InputSample) -> OutputSample {
        let mut last_err = None;
        for i in 0..=self.options.retries {
            match self.client.get(input.url.clone()).send().await {
                Ok(response) => {
                    if self.is_disallowed(&response) {
                        return OutputSample {
                            id: input.id,
                            original_filepath: input.original_filepath,
                            download_data: Vec::new(),
                            download_mime_type: None,
                            download_timestamp: Some(chrono::Utc::now()),
                            url: input.url,
                            caption: input.caption,
                            additional_columns: input.additional_columns,
                            status: SampleStatus::Failure("X-Robots-Tag disallowed".to_string()),
                        };
                    }
                    let mime_type = response
                        .headers()
                        .get("Content-Type")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());
                    match response.bytes().await {
                        Ok(data) => {
                            let data_vec = data.to_vec();
                            log::info!("Successfully downloaded {}", input.url);
                            return OutputSample {
                                id: input.id,
                                original_filepath: input.original_filepath,
                                download_data: data_vec,
                                download_mime_type: mime_type.map(|s| s.to_string()),
                                download_timestamp: Some(chrono::Utc::now()),
                                url: input.url,
                                caption: input.caption,
                                additional_columns: input.additional_columns,
                                status: SampleStatus::Success,
                            };
                        }
                        Err(e) => {
                            log::warn!(
                                "[Try {}] Failed to read bytes from {}: {}",
                                i,
                                input.url,
                                e
                            );
                            last_err = Some(e);
                        }
                    }
                }
                Err(e) => {
                    log::warn!("[Try {}] Failed to download {}: {}", i, input.url, e);
                    last_err = Some(e);
                }
            }
        }

        // Return failure sample if all retries failed
        OutputSample {
            id: input.id,
            original_filepath: input.original_filepath,
            download_data: Vec::new(),
            download_mime_type: None,
            download_timestamp: Some(chrono::Utc::now()),
            url: input.url,
            caption: input.caption,
            additional_columns: input.additional_columns,
            status: SampleStatus::Failure(format!(
                "Download failed after {} retries: {}",
                self.options.retries,
                last_err
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "Unknown error".to_string())
            )),
        }
    }

    async fn single_download(&self, url: String) -> anyhow::Result<Vec<u8>> {
        let response = self.client.get(&url).send().await?;
        if self.is_disallowed(&response) {
            return Err(anyhow::anyhow!(
                "Failed to download {}: Disallowed by X-Robots-Tag",
                url
            ));
        }
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to download {}: Unsuccessful status code {}",
                url,
                response.status()
            ));
        }

        Ok(response.bytes().await?.to_vec())
    }

    /// Checks if the response is disallowed by X-Robots-Tag headers.
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
