use tracing::warn;

pub struct Config {
    pub source_bucket: String,
    pub source_prefix: String,
    pub delta_table_uri: String,
    pub state_file_uri: String,
    pub poll_interval: u64,
    pub download_concurrency: usize,
    pub batch_size: usize,
    pub schema_evolution: bool,
    pub schema_sample_bytes: usize,
    /// Timeout in seconds for downloading and processing a single file (default: 300s / 5 min)
    pub file_timeout_secs: u64,
    /// Maximum number of consecutive failures for a file before marking it as permanently failed (default: 3)
    pub max_file_retries: usize,
}

fn parse_env_or<T: std::str::FromStr>(key: &str, default: T) -> T
where
    T::Err: std::fmt::Display,
{
    std::env::var(key)
        .ok()
        .and_then(|v| {
            v.parse()
                .map_err(|e| {
                    warn!("{key}={v:?} failed to parse: {e}, using default");
                    e
                })
                .ok()
        })
        .unwrap_or(default)
}

impl Config {
    pub fn from_env() -> Self {
        let delta_table_uri = std::env::var("DELTA_TABLE_URI")
            .expect("DELTA_TABLE_URI environment variable required");
        let state_file_uri = format!("{delta_table_uri}/_file_loader_state.json");

        Self {
            source_bucket: std::env::var("SOURCE_BUCKET")
                .expect("SOURCE_BUCKET environment variable required"),
            source_prefix: std::env::var("SOURCE_PREFIX").unwrap_or_default(),
            delta_table_uri,
            state_file_uri,
            poll_interval: parse_env_or("POLL_INTERVAL_SECS", 10),
            download_concurrency: parse_env_or("DOWNLOAD_CONCURRENCY", 50),
            batch_size: parse_env_or("BATCH_SIZE", 4096),
            schema_evolution: std::env::var("SCHEMA_EVOLUTION").is_ok(),
            schema_sample_bytes: parse_env_or("SCHEMA_SAMPLE_BYTES", 64 * 1024),
            file_timeout_secs: parse_env_or("FILE_TIMEOUT_SECS", 300),
            max_file_retries: parse_env_or("MAX_FILE_RETRIES", 3),
        }
    }
}
