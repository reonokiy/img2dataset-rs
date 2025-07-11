#[derive(Debug, Clone)]
pub struct Sample {
    pub id: i64,
    pub url: String,
    pub caption: Option<String>,
}
