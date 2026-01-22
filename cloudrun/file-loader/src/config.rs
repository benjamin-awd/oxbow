/// Configuration loaded from environment variables
pub struct Config {
    pub source_bucket: String,
    pub source_prefix: String,
    pub delta_table_uri: String,
    pub state_file_uri: String,
    pub poll_interval: u64,
    pub download_concurrency: usize,
    pub batch_size: usize,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            source_bucket: std::env::var("SOURCE_BUCKET")
                .expect("SOURCE_BUCKET environment variable required"),
            source_prefix: std::env::var("SOURCE_PREFIX").unwrap_or_default(),
            delta_table_uri: std::env::var("DELTA_TABLE_URI")
                .expect("DELTA_TABLE_URI environment variable required"),
            state_file_uri: std::env::var("STATE_FILE_URI")
                .expect("STATE_FILE_URI environment variable required"),
            poll_interval: std::env::var("POLL_INTERVAL_SECS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10),
            download_concurrency: std::env::var("DOWNLOAD_CONCURRENCY")
                .unwrap_or_else(|_| "50".to_string())
                .parse()
                .unwrap_or(50),
            batch_size: std::env::var("BATCH_SIZE")
                .unwrap_or_else(|_| "4096".to_string())
                .parse()
                .unwrap_or(4096),
        }
    }
}
