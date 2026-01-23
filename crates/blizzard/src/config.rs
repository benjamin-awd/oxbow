use std::fmt;
use tracing::warn;

pub struct Config {
    pub source_bucket: String,
    pub source_prefix: String,
    pub delta_table_uri: String,
    /// Table name derived from the last path segment of delta_table_uri.
    /// Used when creating a new Delta table to set the name in metadata.
    pub delta_table_name: String,
    pub state_file_uri: String,
    pub poll_interval: u64,
    pub download_concurrency: usize,
    pub batch_size: usize,
    /// Timeout in seconds for downloading and processing a single file (default: 300s)
    pub file_timeout_secs: u64,
    /// Maximum number of consecutive failures for a file before marking it as permanently failed (default: 3)
    pub max_file_retries: usize,
    /// Number of hours to look back when listing files (default: None = full listing).
    /// When set, only lists files from the last N hours using date/hour partitioned prefixes.
    pub listing_lookback_hours: Option<u64>,
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

        // Derive table name from the last path segment of the URI
        // e.g., "gs://bucket/delta/my_table" -> "my_table"
        let delta_table_name = delta_table_uri
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .expect("DELTA_TABLE_URI must have a path segment for table name")
            .to_string();

        Self {
            source_bucket: std::env::var("SOURCE_BUCKET")
                .expect("SOURCE_BUCKET environment variable required"),
            source_prefix: std::env::var("SOURCE_PREFIX")
                .expect("SOURCE_PREFIX environment variable required"),
            delta_table_uri,
            delta_table_name,
            state_file_uri,
            poll_interval: parse_env_or("POLL_INTERVAL_SECS", 10),
            download_concurrency: parse_env_or("DOWNLOAD_CONCURRENCY", 50),
            batch_size: parse_env_or("BATCH_SIZE", 4096),
            file_timeout_secs: parse_env_or("FILE_TIMEOUT_SECS", 300),
            max_file_retries: parse_env_or("MAX_FILE_RETRIES", 3),
            listing_lookback_hours: std::env::var("LISTING_LOOKBACK_HOURS")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|&h| h > 0),
        }
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let lookback = match self.listing_lookback_hours {
            Some(h) => format!("{}h", h),
            None => "full".to_string(),
        };
        write!(
            f,
            "bucket={}, prefix={}, table={}, table_name={}, state={}, interval={}s, \
             concurrency={}, batch_size={}, file_timeout={}s, max_retries={}, listing_lookback={}",
            self.source_bucket,
            self.source_prefix,
            self.delta_table_uri,
            self.delta_table_name,
            self.state_file_uri,
            self.poll_interval,
            self.download_concurrency,
            self.batch_size,
            self.file_timeout_secs,
            self.max_file_retries,
            lookback
        )
    }
}
