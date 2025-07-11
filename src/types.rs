use anyhow::Result;
use sha2::{Digest, Sha256};

pub struct ProcessedSample {
    pub sample_id: usize,
    pub url: String,
    pub caption: Option<String>,
    pub image_data: Result<Vec<u8>>,
    pub hash: Option<String>,
}

pub fn compute_hash(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}
