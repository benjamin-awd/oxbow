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
mod config;
mod read;
mod state;
mod write;

use axum::{Router, routing::get};
use deltalake::ObjectStore;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tracing::log::*;
use url::Url;

use config::Config;
use read::{is_gzip_compressed, is_supported_json_file, stream_and_parse_file};
use state::{ProcessedState, load_state};
use write::{BatchBufferingWriter, COMMIT_RECORD_THRESHOLD, commit_to_delta};

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
        config.source_bucket,
        config.source_prefix,
        config.delta_table_uri,
        config.state_file_uri,
        config.poll_interval,
        config.download_concurrency,
        config.batch_size
    );

    // Start health check server in background (Cloud Run requires HTTP responsiveness)
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("0.0.0.0:{port}");
    tokio::spawn(async move {
        let app = Router::new().route("/health", get(|| async { "ok" }));
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind health server to {addr}: {e}");
                return;
            }
        };
        info!("Health server listening on {addr}");
        if let Err(e) = axum::serve(listener, app).await {
            error!("Health server error: {e}");
        }
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
    let list_prefix = (!config.source_prefix.is_empty())
        .then(|| deltalake::Path::from(config.source_prefix.as_str()));

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
    let mut writer =
        BatchBufferingWriter::new(Arc::clone(&table_store), Arc::clone(arrow_schema)).await?;
    let mut pending_paths: Vec<String> = Vec::new(); // Paths waiting for next commit
    let mut files_processed: usize = 0;
    let mut total_records: usize = 0;

    // Stream file downloads and parse results as they complete
    let mut stream = futures::stream::iter(files)
        .map(|obj| {
            let store = Arc::clone(object_store);
            let schema = Arc::clone(arrow_schema);
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
                    let file_meta = writer.close().await?;

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
                    writer = BatchBufferingWriter::new(
                        Arc::clone(&table_store),
                        Arc::clone(arrow_schema),
                    )
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
        let file_meta = writer.close().await?;

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
