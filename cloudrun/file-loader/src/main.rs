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
/// - DOWNLOAD_CONCURRENCY: Concurrent file downloads (default: 50)
/// - BATCH_SIZE: Records per batch when parsing JSON (default: 4096)
///
use async_compression::tokio::bufread::GzipDecoder;
use axum::{Router, routing::get};
use deltalake::ObjectMeta;
use deltalake::ObjectStore;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::json::reader::{Decoder, ReaderBuilder};
use deltalake::parquet::arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter};
use deltalake::parquet::basic::Compression;
use deltalake::parquet::file::properties::WriterProperties;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::io::StreamReader;
use tracing::log::*;
use url::Url;
use uuid::Uuid;

/// State file tracking which files have been processed
/// Uses serde(default) for backward compatibility with old state files
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ProcessedState {
    #[serde(default)]
    processed_files: HashSet<String>,
}

/// Configuration loaded from environment
struct Config {
    source_bucket: String,
    source_prefix: String,
    delta_table_uri: String,
    state_file_uri: String,
    poll_interval: u64,
    download_concurrency: usize,
    batch_size: usize,
}

impl Config {
    fn from_env() -> Self {
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

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    deltalake::gcp::register_handlers(None);

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    info!("Starting file-loader polling service");

    // Get configuration from environment
    let config = Config::from_env();

    info!(
        "Configuration: bucket={}, prefix={}, table={}, state={}, interval={}s, concurrency={}, batch_size={}",
        config.source_bucket, config.source_prefix, config.delta_table_uri,
        config.state_file_uri, config.poll_interval, config.download_concurrency, config.batch_size
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
        if let Err(e) = poll_and_process(&config).await {
            error!("Error during poll cycle: {:?}", e);
        }

        tokio::time::sleep(Duration::from_secs(config.poll_interval)).await;
    }
}

/// Poll for new files and process them in batches
async fn poll_and_process(config: &Config) -> Result<(), anyhow::Error> {
    use deltalake::logstore::{StorageConfig, logstore_for};

    const FILE_BATCH_SIZE: usize = 100; // Process 100 files per batch

    debug!(
        "Polling gs://{}/{} for new files",
        config.source_bucket, config.source_prefix
    );

    // Get object store for the source bucket (still needed for listing metadata)
    let bucket_url = Url::parse(&format!("gs://{}", config.source_bucket))?;
    let store = logstore_for(&bucket_url, StorageConfig::default())?;
    let object_store = store.object_store(None);

    // Load current state
    let mut state = load_state(&config.state_file_uri).await?;

    // List objects with prefix, filtering as we go
    let list_prefix = if config.source_prefix.is_empty() {
        None
    } else {
        Some(deltalake::Path::from(config.source_prefix.as_str()))
    };

    // Stream the listing and filter to new files only
    let mut listing = object_store.list(list_prefix.as_ref());
    let mut new_files = Vec::with_capacity(FILE_BATCH_SIZE);
    let mut total_listed = 0usize;
    let mut total_processed = 0usize;

    // Open the Delta table once
    let mut table = oxbow::lock::open_table(&config.delta_table_uri).await?;
    let arrow_schema = table.snapshot()?.snapshot().arrow_schema();

    // Stream through listing and process in batches
    while let Some(result) = listing.next().await {
        let obj = result?;
        total_listed += 1;

        let name = obj.location.as_ref();
        if is_supported_json_file(name) && !state.processed_files.contains(name) {
            new_files.push(obj);
        }

        // Process batch when full
        if new_files.len() >= FILE_BATCH_SIZE {
            let batch_num = total_processed / FILE_BATCH_SIZE + 1;
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
                &mut table,
                &mut state,
                config,
            )
            .await?;

            total_processed += processed;
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
            &mut table,
            &mut state,
            config,
        )
        .await?;

        total_processed += processed;
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

/// Buffering writer that accumulates batches and commits based on rolling policy
struct BatchBufferingWriter {
    writer: AsyncArrowWriter<ParquetObjectWriter>,
    path: deltalake::Path,
    records_written: usize,
}

impl BatchBufferingWriter {
    async fn new(
        object_store: Arc<dyn ObjectStore>,
        schema: Arc<deltalake::arrow::datatypes::Schema>,
    ) -> Result<Self, anyhow::Error> {
        let uuid = Uuid::new_v4();
        let filename = format!("part-{}.snappy.parquet", uuid);
        let path = deltalake::Path::from(filename);

        let sink = ParquetObjectWriter::new(object_store, path.clone());
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let writer = AsyncArrowWriter::try_new(sink, schema, Some(props))?;

        Ok(Self {
            writer,
            path,
            records_written: 0,
        })
    }

    async fn write(&mut self, batch: &RecordBatch) -> Result<(), anyhow::Error> {
        self.records_written += batch.num_rows();
        self.writer.write(batch).await?;
        Ok(())
    }

    async fn close(self, object_store: &Arc<dyn ObjectStore>) -> Result<ObjectMeta, anyhow::Error> {
        self.writer.close().await?;
        let meta = object_store.head(&self.path).await?;
        Ok(ObjectMeta {
            location: self.path,
            last_modified: meta.last_modified,
            size: meta.size,
            e_tag: meta.e_tag,
            version: meta.version,
        })
    }

    fn records(&self) -> usize {
        self.records_written
    }
}

/// Rolling policy configuration
const COMMIT_RECORD_THRESHOLD: usize = 500_000; // Commit every 500k records

/// Commit a parquet file to Delta and save state
async fn commit_to_delta(
    file_meta: ObjectMeta,
    table: &mut deltalake::DeltaTable,
    state: &mut ProcessedState,
    pending_paths: &mut Vec<String>,
    state_file_uri: &str,
) -> Result<i64, anyhow::Error> {
    let actions = oxbow::add_actions_for(&[file_meta]);
    let version = oxbow::commit_to_table(&actions, table).await?;

    // Reload table state
    table.load().await?;

    // Mark files as processed and save state
    for path in pending_paths.drain(..) {
        state.processed_files.insert(path);
    }
    save_state(state_file_uri, state).await?;

    Ok(version)
}

/// Process a batch of files with streaming writes and rolling commits
async fn process_file_batch(
    files: &[deltalake::ObjectMeta],
    object_store: &Arc<dyn ObjectStore>,
    arrow_schema: &Arc<deltalake::arrow::datatypes::Schema>,
    table: &mut deltalake::DeltaTable,
    state: &mut ProcessedState,
    config: &Config,
) -> Result<usize, anyhow::Error> {
    let table_store = table.object_store();

    // Create initial buffering writer
    let mut writer = BatchBufferingWriter::new(table_store.clone(), arrow_schema.clone()).await?;
    let mut pending_paths: Vec<String> = Vec::new(); // Paths waiting for next commit
    let mut files_processed: usize = 0;
    let mut total_records: usize = 0;

    // Stream file downloads and parse results as they complete
    let mut stream = futures::stream::iter(files)
        .map(|obj| {
            let store = object_store.clone();
            let schema = arrow_schema.clone();
            let location = obj.location.clone();
            let size = obj.size;
            let batch_size = config.batch_size;
            async move {
                let file_path = location.as_ref().to_string();
                let is_compressed = is_gzip_compressed(&file_path);

                debug!(
                    "Processing: {} ({} bytes, compressed: {})",
                    file_path, size, is_compressed
                );

                // Stream from GCS using object_store
                let result =
                    stream_and_parse_file(&store, &location, schema, is_compressed, batch_size)
                        .await;
                (file_path, result)
            }
        })
        .buffer_unordered(config.download_concurrency);

    // Process results as they arrive (streaming, not collecting)
    while let Some((file_path, result)) = stream.next().await {
        match result {
            Ok((batches, records)) => {
                info!("Parsed {} records from {}", records, file_path);

                // Write batches to parquet as they arrive
                for batch in batches {
                    writer.write(&batch).await?;
                }

                pending_paths.push(file_path);
                files_processed += 1;
                total_records += records;

                // Rolling commit: flush and commit when we hit the record threshold
                if writer.records() >= COMMIT_RECORD_THRESHOLD {
                    let records_in_file = writer.records();
                    let file_meta = writer.close(&table_store).await?;

                    // Commit immediately (Arroyo-style)
                    let version = commit_to_delta(
                        file_meta.clone(),
                        table,
                        state,
                        &mut pending_paths,
                        &config.state_file_uri,
                    )
                    .await?;

                    info!(
                        "Committed parquet file {} ({} bytes, {} records) to Delta v{}",
                        file_meta.location, file_meta.size, records_in_file, version
                    );

                    // Start a new writer for the next batch of records
                    writer =
                        BatchBufferingWriter::new(table_store.clone(), arrow_schema.clone())
                            .await?;
                }
            }
            Err(e) => {
                error!("Failed to process {}: {:?}", file_path, e);
            }
        }
    }

    // Close and commit final writer if it has any records
    if writer.records() > 0 {
        let records_in_file = writer.records();
        let file_meta = writer.close(&table_store).await?;

        let version = commit_to_delta(
            file_meta.clone(),
            table,
            state,
            &mut pending_paths,
            &config.state_file_uri,
        )
        .await?;

        info!(
            "Committed final parquet file {} ({} bytes, {} records) to Delta v{}",
            file_meta.location, file_meta.size, records_in_file, version
        );
    }

    if files_processed > 0 {
        info!(
            "Batch complete: {} files, {} total records",
            files_processed, total_records
        );
    }

    Ok(files_processed)
}

/// Stream and parse a file from GCS with lazy reading
/// - Uses object_store's streaming API (bytes fetched on demand)
/// - Uses configurable batch size for accumulating records
async fn stream_and_parse_file(
    store: &Arc<dyn ObjectStore>,
    path: &deltalake::Path,
    arrow_schema: Arc<deltalake::arrow::datatypes::Schema>,
    is_compressed: bool,
    batch_size: usize,
) -> Result<(Vec<RecordBatch>, usize), anyhow::Error> {
    // Get file as a stream of bytes
    let get_result = store.get(path).await?;

    // Convert to a stream of bytes, then to AsyncRead
    let byte_stream = get_result
        .into_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let stream_reader = StreamReader::new(byte_stream);

    // Create incremental JSON decoder with configurable batch size
    let mut decoder = ReaderBuilder::new(arrow_schema)
        .with_batch_size(batch_size)
        .build_decoder()?;

    // Stream decompress and parse
    let batches = if is_compressed {
        let buf_reader = BufReader::new(GzipDecoder::new(BufReader::new(stream_reader)));
        deserialize_stream(buf_reader, &mut decoder).await?
    } else {
        let buf_reader = BufReader::new(stream_reader);
        deserialize_stream(buf_reader, &mut decoder).await?
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
