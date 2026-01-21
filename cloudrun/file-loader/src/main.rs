///
/// The file-loader Cloud Run service polls a GCS bucket for JSONL files and appends
/// records to Delta tables. Polls every 10 seconds for new files.
///
/// Supported file formats:
/// - .json, .jsonl (newline-delimited JSON)
/// - .ndjson (newline-delimited JSON)
/// - .json.gz, .jsonl.gz, .ndjson.gz (gzip compressed)
///
/// Environment variables:
/// - SOURCE_BUCKET: GCS bucket to poll for files
/// - SOURCE_PREFIX: Optional prefix within the bucket (default: "")
/// - DELTA_TABLE_URI: Destination Delta table (e.g., gs://bucket/table)
/// - POLL_INTERVAL_SECS: Polling interval in seconds (default: 10)
/// - STATE_FILE_URI: GCS URI for state file tracking processed files
///
use axum::{Router, routing::get};
use deltalake::ObjectStore;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::json::ReaderBuilder;
use flate2::read::GzDecoder;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
use tracing::log::*;
use url::Url;

/// State file tracking which files have been processed
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ProcessedState {
    processed_files: HashSet<String>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    deltalake::gcp::register_handlers(None);

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    info!("Starting file-loader polling service");

    // Get configuration from environment
    let source_bucket =
        std::env::var("SOURCE_BUCKET").expect("SOURCE_BUCKET environment variable required");
    let source_prefix = std::env::var("SOURCE_PREFIX").unwrap_or_default();
    let delta_table_uri =
        std::env::var("DELTA_TABLE_URI").expect("DELTA_TABLE_URI environment variable required");
    let state_file_uri =
        std::env::var("STATE_FILE_URI").expect("STATE_FILE_URI environment variable required");
    let poll_interval: u64 = std::env::var("POLL_INTERVAL_SECS")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap_or(10);

    info!(
        "Configuration: bucket={}, prefix={}, table={}, state={}, interval={}s",
        source_bucket, source_prefix, delta_table_uri, state_file_uri, poll_interval
    );

    // Start health check server in background (Cloud Run requires HTTP responsiveness)
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("0.0.0.0:{}", port);
    tokio::spawn(async move {
        let app = Router::new().route("/health", get(|| async { "ok" }));
        let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
        info!("Health server listening on {}", addr);
        axum::serve(listener, app).await.unwrap();
    });

    // Main polling loop
    loop {
        if let Err(e) = poll_and_process(
            &source_bucket,
            &source_prefix,
            &delta_table_uri,
            &state_file_uri,
        )
        .await
        {
            error!("Error during poll cycle: {:?}", e);
        }

        tokio::time::sleep(Duration::from_secs(poll_interval)).await;
    }
}

/// Poll for new files and process them
async fn poll_and_process(
    bucket: &str,
    prefix: &str,
    delta_table_uri: &str,
    state_file_uri: &str,
) -> Result<(), anyhow::Error> {
    use deltalake::logstore::{StorageConfig, logstore_for};

    debug!("Polling gs://{}/{} for new files", bucket, prefix);

    // Get object store for the source bucket
    let bucket_url = Url::parse(&format!("gs://{}", bucket))?;
    let store = logstore_for(&bucket_url, StorageConfig::default())?;
    let object_store = store.object_store(None);

    // Load current state
    let mut state = load_state(state_file_uri).await?;
    let initial_count = state.processed_files.len();

    // List all objects with the prefix
    let list_prefix = if prefix.is_empty() {
        None
    } else {
        Some(deltalake::Path::from(prefix))
    };

    let objects: Vec<_> = object_store
        .list(list_prefix.as_ref())
        .try_collect()
        .await?;

    // Filter to supported files that haven't been processed
    let new_files: Vec<_> = objects
        .into_iter()
        .filter(|obj| {
            let name = obj.location.as_ref();
            is_supported_json_file(name) && !state.processed_files.contains(name)
        })
        .collect();

    if new_files.is_empty() {
        debug!("No new files to process");
        return Ok(());
    }

    info!("Found {} new files to process", new_files.len());

    // Open the Delta table once for all files
    let mut table = oxbow::lock::open_table(delta_table_uri).await?;
    let arrow_schema = table.snapshot()?.snapshot().arrow_schema();

    // Process each new file
    for obj in new_files {
        let file_path = obj.location.as_ref();
        let file_uri = format!("gs://{}/{}", bucket, file_path);
        let is_compressed = is_gzip_compressed(file_path);

        info!(
            "Processing: {} ({} bytes, compressed: {})",
            file_uri, obj.size, is_compressed
        );

        match process_file(
            &object_store,
            &obj.location,
            arrow_schema.clone(),
            &mut table,
            is_compressed,
        )
        .await
        {
            Ok(records) => {
                info!(
                    "Successfully processed {} records from {}",
                    records, file_path
                );
                state.processed_files.insert(file_path.to_string());
            }
            Err(e) => {
                error!("Failed to process {}: {:?}", file_path, e);
                // Continue with other files, don't fail the whole batch
            }
        }
    }

    // Save state if we processed any files
    if state.processed_files.len() > initial_count {
        save_state(state_file_uri, &state).await?;
        info!(
            "Updated state file with {} total processed files",
            state.processed_files.len()
        );
    }

    Ok(())
}

/// Process a single file and append to Delta table
async fn process_file(
    store: &Arc<dyn ObjectStore>,
    path: &deltalake::Path,
    arrow_schema: Arc<deltalake::arrow::datatypes::Schema>,
    table: &mut deltalake::DeltaTable,
    is_compressed: bool,
) -> Result<usize, anyhow::Error> {
    // Read the file content
    let compressed_data = store.get(path).await?.bytes().await?;

    // Decompress if needed
    let data: Vec<u8> = if is_compressed {
        debug!(
            "Decompressing {} ({} compressed bytes)",
            path,
            compressed_data.len()
        );
        let mut decoder = GzDecoder::new(&compressed_data[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        debug!("Decompressed to {} bytes", decompressed.len());
        decompressed
    } else {
        compressed_data.to_vec()
    };

    // Parse JSONL and create record batches
    let reader = ReaderBuilder::new(arrow_schema).build(std::io::Cursor::new(data))?;

    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;

    if batches.is_empty() {
        return Ok(0);
    }

    let total_records: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Write batches to Delta table
    let version = write_batches_to_table(&batches, table).await?;
    debug!("Wrote to Delta table version {}", version);

    Ok(total_records)
}

/// Load processed state from GCS
async fn load_state(state_file_uri: &str) -> Result<ProcessedState, anyhow::Error> {
    use deltalake::logstore::{StorageConfig, logstore_for};

    let url = Url::parse(state_file_uri)?;
    let store = logstore_for(&url, StorageConfig::default())?;
    let path = deltalake::Path::from(url.path());

    match store.object_store(None).get(&path).await {
        Ok(result) => {
            let bytes = result.bytes().await?;
            let state: ProcessedState = serde_json::from_slice(&bytes)?;
            debug!(
                "Loaded state with {} processed files",
                state.processed_files.len()
            );
            Ok(state)
        }
        Err(deltalake::ObjectStoreError::NotFound { .. }) => {
            debug!("No existing state file, starting fresh");
            Ok(ProcessedState::default())
        }
        Err(e) => Err(e.into()),
    }
}

/// Save processed state to GCS
async fn save_state(state_file_uri: &str, state: &ProcessedState) -> Result<(), anyhow::Error> {
    use deltalake::logstore::{StorageConfig, logstore_for};

    let url = Url::parse(state_file_uri)?;
    let store = logstore_for(&url, StorageConfig::default())?;
    let path = deltalake::Path::from(url.path());

    let bytes = serde_json::to_vec_pretty(state)?;
    store.object_store(None).put(&path, bytes.into()).await?;

    Ok(())
}

/// Check if a filename is a supported JSON file format
fn is_supported_json_file(name: &str) -> bool {
    let lower = name.to_lowercase();
    lower.ends_with(".json")
        || lower.ends_with(".jsonl")
        || lower.ends_with(".ndjson")
        || lower.ends_with(".json.gz")
        || lower.ends_with(".jsonl.gz")
        || lower.ends_with(".ndjson.gz")
}

/// Check if a file is gzip compressed based on extension
fn is_gzip_compressed(name: &str) -> bool {
    name.to_lowercase().ends_with(".gz")
}

/// Write a slice of RecordBatch to the given DeltaTable and return the new version
async fn write_batches_to_table(
    batches: &[RecordBatch],
    table: &mut deltalake::DeltaTable,
) -> Result<i64, anyhow::Error> {
    use deltalake::writer::{DeltaWriter, record_batch::RecordBatchWriter};

    let mut writer = RecordBatchWriter::for_table(table)?;

    for batch in batches {
        writer.write(batch.clone()).await?;
    }

    let version = writer.flush_and_commit(table).await?;
    debug!("Successfully wrote v{version} to Delta table");
    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_supported_json_file() {
        // Supported formats
        assert!(is_supported_json_file("data.json"));
        assert!(is_supported_json_file("data.jsonl"));
        assert!(is_supported_json_file("data.ndjson"));
        assert!(is_supported_json_file("data.json.gz"));
        assert!(is_supported_json_file("data.jsonl.gz"));
        assert!(is_supported_json_file("data.ndjson.gz"));
        assert!(is_supported_json_file("path/to/data.NDJSON.GZ")); // case insensitive

        // Unsupported formats
        assert!(!is_supported_json_file("data.parquet"));
        assert!(!is_supported_json_file("data.csv"));
        assert!(!is_supported_json_file("data.txt"));
        assert!(!is_supported_json_file("data.gz")); // just .gz without json
    }

    #[test]
    fn test_is_gzip_compressed() {
        assert!(is_gzip_compressed("data.ndjson.gz"));
        assert!(is_gzip_compressed("data.json.gz"));
        assert!(is_gzip_compressed("data.GZ")); // case insensitive

        assert!(!is_gzip_compressed("data.ndjson"));
        assert!(!is_gzip_compressed("data.json"));
    }

    #[test]
    fn test_state_serialization() {
        let mut state = ProcessedState::default();
        state.processed_files.insert("file1.ndjson".to_string());
        state.processed_files.insert("file2.ndjson.gz".to_string());

        let json = serde_json::to_string(&state).unwrap();
        let restored: ProcessedState = serde_json::from_str(&json).unwrap();

        assert_eq!(state.processed_files, restored.processed_files);
    }
}
