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
use async_compression::tokio::bufread::GzipDecoder;
use axum::{Router, routing::get};
use deltalake::ObjectStore;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::json::reader::{Decoder, ReaderBuilder};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
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

/// Poll for new files and process them in batches
async fn poll_and_process(
    bucket: &str,
    prefix: &str,
    delta_table_uri: &str,
    state_file_uri: &str,
) -> Result<(), anyhow::Error> {
    use deltalake::logstore::{StorageConfig, logstore_for};

    const BATCH_SIZE: usize = 100; // Process 100 files per batch

    debug!("Polling gs://{}/{} for new files", bucket, prefix);

    // Get object store for the source bucket
    let bucket_url = Url::parse(&format!("gs://{}", bucket))?;
    let store = logstore_for(&bucket_url, StorageConfig::default())?;
    let object_store = store.object_store(None);

    // Load current state
    let mut state = load_state(state_file_uri).await?;

    // List objects with prefix, filtering as we go
    let list_prefix = if prefix.is_empty() {
        None
    } else {
        Some(deltalake::Path::from(prefix))
    };

    // Stream the listing and filter to new files only
    let mut listing = object_store.list(list_prefix.as_ref());
    let mut new_files = Vec::with_capacity(BATCH_SIZE);
    let mut total_listed = 0usize;
    let mut total_processed = 0usize;

    // Open the Delta table once
    let mut table = oxbow::lock::open_table(delta_table_uri).await?;
    let arrow_schema = table.snapshot()?.snapshot().arrow_schema();

    let concurrency = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    // Stream through listing and process in batches
    while let Some(result) = listing.next().await {
        let obj = result?;
        total_listed += 1;

        let name = obj.location.as_ref();
        if is_supported_json_file(name) && !state.processed_files.contains(name) {
            new_files.push(obj);
        }

        // Process batch when full
        if new_files.len() >= BATCH_SIZE {
            let batch_num = total_processed / BATCH_SIZE + 1;
            info!(
                "Processing batch {} ({} files, {} listed so far)",
                batch_num,
                new_files.len(),
                total_listed
            );

            let processed = process_file_batch(
                &new_files,
                &object_store,
                &arrow_schema,
                bucket,
                &mut table,
                &mut state,
                concurrency,
            )
            .await?;

            total_processed += processed;
            save_state(state_file_uri, &state).await?;
            new_files.clear();
        }
    }

    // Process remaining files
    if !new_files.is_empty() {
        info!(
            "Processing final batch ({} files, {} total listed)",
            new_files.len(),
            total_listed
        );

        let processed = process_file_batch(
            &new_files,
            &object_store,
            &arrow_schema,
            bucket,
            &mut table,
            &mut state,
            concurrency,
        )
        .await?;

        total_processed += processed;
        save_state(state_file_uri, &state).await?;
    }

    if total_processed > 0 {
        info!(
            "Completed: processed {} files out of {} listed",
            total_processed, total_listed
        );
    } else {
        debug!("No new files to process ({} listed)", total_listed);
    }

    Ok(())
}

/// Process a batch of files concurrently
async fn process_file_batch(
    files: &[deltalake::ObjectMeta],
    object_store: &Arc<dyn ObjectStore>,
    arrow_schema: &Arc<deltalake::arrow::datatypes::Schema>,
    _bucket: &str,
    table: &mut deltalake::DeltaTable,
    state: &mut ProcessedState,
    concurrency: usize,
) -> Result<usize, anyhow::Error> {
    // Read and parse files concurrently
    let results: Vec<_> = futures::stream::iter(files)
        .map(|obj| {
            let store = object_store.clone();
            let schema = arrow_schema.clone();
            let location = obj.location.clone();
            let size = obj.size;
            async move {
                let file_path = location.as_ref().to_string();
                let is_compressed = is_gzip_compressed(&file_path);

                debug!(
                    "Processing: {} ({} bytes, compressed: {})",
                    file_path, size, is_compressed
                );

                let result = read_and_parse_file(&store, &location, schema, is_compressed).await;
                (file_path, result)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    // Collect successful batches and track processed files
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut processed_paths: Vec<String> = Vec::new();

    for (file_path, result) in results {
        match result {
            Ok((batches, records)) => {
                info!("Parsed {} records from {}", records, file_path);
                all_batches.extend(batches);
                processed_paths.push(file_path);
            }
            Err(e) => {
                error!("Failed to process {}: {:?}", file_path, e);
            }
        }
    }

    // Write all batches to Delta table in one commit
    let files_processed = processed_paths.len();
    if !all_batches.is_empty() {
        let total_records: usize = all_batches.iter().map(|b| b.num_rows()).sum();
        let version = write_batches_to_table(all_batches, table).await?;
        info!(
            "Wrote {} records from {} files to Delta table v{}",
            total_records, files_processed, version
        );

        // Update state with processed files
        for path in processed_paths {
            state.processed_files.insert(path);
        }
    }

    Ok(files_processed)
}

/// Read and parse a single file using streaming decompression and JSON parsing
async fn read_and_parse_file(
    store: &Arc<dyn ObjectStore>,
    path: &deltalake::Path,
    arrow_schema: Arc<deltalake::arrow::datatypes::Schema>,
    is_compressed: bool,
) -> Result<(Vec<RecordBatch>, usize), anyhow::Error> {
    // Load compressed file content into memory (small, ~1MB)
    let data_bytes = store.get(path).await?.bytes().await?;

    // Create incremental JSON decoder
    let mut decoder = ReaderBuilder::new(arrow_schema)
        .with_batch_size(10_000)
        .build_decoder()?;

    // Stream decompress and parse
    let batches = if is_compressed {
        let reader = BufReader::new(GzipDecoder::new(BufReader::new(&data_bytes[..])));
        deserialize_stream(reader, &mut decoder).await?
    } else {
        let reader = BufReader::new(&data_bytes[..]);
        deserialize_stream(reader, &mut decoder).await?
    };

    let total_records: usize = batches.iter().map(|b| b.num_rows()).sum();
    Ok((batches, total_records))
}

/// Deserialize bytes from an async reader into RecordBatches
async fn deserialize_stream(
    mut reader: impl tokio::io::AsyncBufRead + Unpin,
    decoder: &mut Decoder,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let mut batches = Vec::new();

    loop {
        let buf = reader.fill_buf().await?;

        if buf.is_empty() {
            // End of stream - flush any remaining data
            if let Some(batch) = decoder.flush()? {
                if batch.num_rows() > 0 {
                    batches.push(batch);
                }
            }
            break;
        }

        let mut have_read = decoder.decode(buf)?;

        // If decoder couldn't make progress, flush to free internal buffer space
        if have_read == 0 && !buf.is_empty() {
            if let Some(batch) = decoder.flush()? {
                batches.push(batch);
            }
            // Try decode again after flush
            have_read = decoder.decode(buf)?;
            if have_read == 0 {
                // Still can't progress - skip one byte to avoid infinite loop
                warn!("Decoder stuck, skipping byte");
                reader.consume(1);
                continue;
            }
        }

        reader.consume(have_read);

        // Flush when decoder has complete records
        if !decoder.has_partial_record() {
            if let Some(batch) = decoder.flush()? {
                batches.push(batch);
            }
        }
    }

    Ok(batches)
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

/// Write RecordBatches to the given DeltaTable and return the new version
async fn write_batches_to_table(
    batches: Vec<RecordBatch>,
    table: &mut deltalake::DeltaTable,
) -> Result<i64, anyhow::Error> {
    use deltalake::writer::{DeltaWriter, record_batch::RecordBatchWriter};

    let mut writer = RecordBatchWriter::for_table(table)?;

    for batch in batches {
        writer.write(batch).await?;
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
