use anyhow::{Result, anyhow};
use reqwest::{Client, Response};
use std::collections::HashSet;
use std::time::Duration;

use crate::sampler::{InputSample, OutputSample};

/// The `Downloader` is responsible for downloading images from URLs.
#[derive(Clone)]
pub struct Downloader {
    client: Client,
    user_agent_token: Option<String>,
    disallowed_header_directives: HashSet<String>,
    retries: u32,
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
    pub fn new(
        timeout: u64,
        user_agent_token: Option<String>,
        disallowed_header_directives: Option<Vec<String>>,
        retries: u32,
    ) -> Self {
        let user_agent_string =
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
                .to_string()
                + &user_agent_token
                    .as_ref()
                    .map(|token| {
                        format!(
                            " (compatible; {}; +https://github.com/reonokiy/img2dataset-rs)",
                            token
                        )
                    })
                    .unwrap_or_default();

        let client = Client::builder()
            .user_agent(user_agent_string)
            .timeout(Duration::from_secs(timeout))
            .build()
            .unwrap();

        let directives = disallowed_header_directives
            .unwrap_or_else(|| vec!["noai".to_string(), "noimageai".to_string()])
            .into_iter()
            .collect();

        Self {
            client,
            user_agent_token,
            disallowed_header_directives: directives,
            retries,
        }
    }

    /// Downloads an image from a URL.
    ///
    /// # Arguments
    ///
    /// * `url` - The URL of the image to download.
    ///
    /// # Returns
    ///
    /// A `Result` containing the image data as a `Vec<u8>` and the MIME type as a `String`.
    pub async fn download(&self, input: InputSample) -> Result<OutputSample> {
        let mut last_err = None;
        for i in 0..=self.retries {
            match self.client.get(input.url.clone()).send().await {
                Ok(response) => {
                    if self.is_disallowed(&response) {
                        return Err(anyhow!("X-Robots-Tag disallowed"));
                    }
                    let mime_type = response
                        .headers()
                        .get("Content-Type")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());
                    let data = response.bytes().await?.to_vec();
                    log::info!("Successfully downloaded {}", input.url);
                    return Ok({
                        OutputSample {
                            id: input.id,
                            original_filepath: input.original_filepath,
                            download_data: data,
                            download_mime_type: mime_type.map(|s| s.to_string()),
                            download_timestamp: Some(chrono::Utc::now()),
                            url: input.url,
                            caption: input.caption,
                            additional_columns: input.additional_columns,
                        }
                    });
                }
                Err(e) => {
                    log::warn!("[Try {}] Failed to download {}: {}", i, input.url, e);
                    last_err = Some(e);
                }
            }
        }
        Err(anyhow!(last_err.unwrap()))
    }

    /// Checks if the response is disallowed by X-Robots-Tag headers.
    fn is_disallowed(&self, response: &Response) -> bool {
        if self.disallowed_header_directives.is_empty() {
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

                if ua_token.is_none() || ua_token == self.user_agent_token {
                    let directives = directives_str.split(',').map(|d| d.trim().to_lowercase());
                    for directive in directives {
                        if self.disallowed_header_directives.contains(&directive) {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }
}
