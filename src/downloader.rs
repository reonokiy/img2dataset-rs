use bytes::Bytes;
use futures::stream::{self, StreamExt};
use reqwest::{Client, Response};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_tracing::TracingMiddleware;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

use crate::sampler::BatchSample;

/// The `Downloader` is responsible for downloading images from URLs.
#[derive(Clone)]
pub struct Downloader {
    client: ClientWithMiddleware,
    options: DownloaderOptions,
    semaphore: Arc<Semaphore>,
}

#[derive(Debug, Clone)]
pub struct DownloaderOptions {
    pub max_concurrent_downloads: usize,
    pub timeout: u64,
    pub user_agent: String,
    pub disallowed_header_directives: Vec<String>,
    pub retries: u32,
}

impl Downloader {
    pub fn new(options: DownloaderOptions) -> Self {
        let semaphore = Arc::new(Semaphore::new(options.max_concurrent_downloads));
        let reqwest_client = Client::builder()
            .user_agent(options.user_agent.clone())
            .timeout(Duration::from_secs(options.timeout))
            .connect_timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(100)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(30))
            .tcp_nodelay(true)
            .http2_adaptive_window(true)
            .build()
            .expect("Failed to create HTTP client");
        let client = ClientBuilder::new(reqwest_client)
            .with(TracingMiddleware::default())
            .build();

        Self {
            client,
            options,
            semaphore,
        }
    }

    pub async fn batch_download(&self, mut inputs: BatchSample) -> BatchSample {
        let len = inputs.len();
        let urls: Vec<Option<String>> = inputs
            .url()
            .iter()
            .map(|u| u.map(|s| s.to_string()))
            .collect();

        let results: Vec<Option<Bytes>> = stream::iter(urls.into_iter().enumerate())
            .map(|(i, url)| {
                let downloader = self.clone();
                async move {
                    match url {
                        Some(url_str) => {
                            let _permit = downloader.semaphore.acquire().await.unwrap();
                            match downloader.single_download(url_str).await {
                                Ok(data) => (i, Some(data)),
                                Err(_) => (i, None),
                            }
                        }
                        None => (i, None),
                    }
                }
            })
            .buffer_unordered(len)
            .fold(vec![None; len], |mut acc, (i, result)| async move {
                acc[i] = result;
                acc
            })
            .await;

        inputs.put_bytes(results);
        inputs
    }

    async fn single_download(&self, url: String) -> anyhow::Result<Bytes> {
        let mut last_err = None;
        let max_attempts = self.options.retries + 1;

        for attempt in 0..max_attempts {
            match self.try_download(&url).await {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    if attempt + 1 < max_attempts {
                        tracing::debug!(
                            "Retry {}/{} for {}: {}",
                            attempt + 1,
                            max_attempts,
                            url,
                            e
                        );
                    }
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap())
    }

    async fn try_download(&self, url: &str) -> anyhow::Result<Bytes> {
        let response = match self.client.get(url).send().await {
            Ok(resp) => resp,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Failed to download {}: {:?}",
                    url,
                    e.source()
                ));
            }
        };

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

        Ok(response.bytes().await?)
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
