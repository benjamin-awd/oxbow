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
/// - DOWNLOAD_CONCURRENCY: Concurrent file downloads (default: 50)
/// - BATCH_SIZE: Records per batch when parsing JSON (default: 4096)
/// - LISTING_LOOKBACK_HOURS: Only list files from last N hours (default: full listing)
///
/// State is stored at gs://{DELTA_TABLE_URI}/_file_loader_state.json
///
use blizzard::config::Config;
use blizzard::error::{DeltaTableSnafu, Error, ObjectStoreError, Result, UrlParseSnafu};
use blizzard::health;
use blizzard::read::{
    augment_with_source_file, is_gzip_compressed, is_supported_json_file, sample_schema_from_file,
    stream_and_parse_file,
};
use blizzard::schema;
use blizzard::state::{ProcessedState, load_state, query_processed_files, save_state};
use blizzard::write::commit_writer;

use chrono::{Duration as ChronoDuration, Utc};
use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::kernel::schema::{DataType, StructField};
use deltalake::logstore::{StorageConfig, logstore_for};
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use deltalake::writer::{DeltaWriter, record_batch::RecordBatchWriter};
use deltalake::{DeltaTableError, ObjectStore};
use futures::StreamExt;
use snafu::ResultExt;
use std::sync::Arc;
use std::time::Duration;
use tracing::log::*;
use url::Url;

/// Number of files to process in a single Delta commit
const FILE_BATCH_SIZE: usize = 100;

/// Max age in seconds before health check considers service unhealthy
const HEALTH_MAX_AGE_SECS: u64 = 300;

/// Number of bytes to sample when inferring schema from a JSON file
const SCHEMA_SAMPLE_BYTES: usize = 64 * 1024;

/// How long to keep processed file entries in state (4 hours, gives buffer beyond typical lookback)
const STATE_PRUNE_AGE_SECS: u64 = 4 * 60 * 60;

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    deltalake::gcp::register_handlers(None);

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    info!("Starting file-loader polling service");

    let config = Config::from_env();

    info!("Configuration: {config}");

    // Start health check server with heartbeat tracking
    let heartbeat = health::Heartbeat::new(HEALTH_MAX_AGE_SECS);
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("0.0.0.0:{port}");
    health::spawn_health_server(addr, heartbeat.clone());

    // Load state once at startup
    let mut state = load_state(&config.state_file_uri).await?;
    let mut state_initialized = !state.processed_files.is_empty();

    loop {
        match poll_and_process(&config, &mut state, &mut state_initialized).await {
            Ok(_) => {}
            Err(e) => {
                if e.is_fatal() {
                    error!("Fatal error during poll cycle: {:?}", e);
                } else if e.is_transient() {
                    warn!("Transient error during poll cycle (will retry): {:?}", e);
                } else {
                    error!("Error during poll cycle: {:?}", e);
                }
            }
        }

        // Update heartbeat after each cycle
        heartbeat.touch();

        tokio::time::sleep(Duration::from_secs(config.poll_interval)).await;
    }
}

async fn poll_and_process(
    config: &Config,
    state: &mut ProcessedState,
    state_initialized: &mut bool,
) -> Result<()> {
    let bucket_url =
        Url::parse(&format!("gs://{}", config.source_bucket)).context(UrlParseSnafu {
            url: format!("gs://{}", config.source_bucket),
        })?;
    let store = logstore_for(&bucket_url, StorageConfig::default()).context(DeltaTableSnafu)?;
    let object_store = store.object_store(None);

    // Determine listing strategy: time-bounded or full
    let (files_to_check, listing_mode) = if let Some(hours) = config.listing_lookback_hours {
        debug!(
            "Listing gs://{}/{} with {}h lookback",
            config.source_bucket, config.source_prefix, hours
        );
        let files =
            list_files_time_bounded(&object_store, &config.source_prefix, hours, state).await?;
        (files, format!("{}h lookback", hours))
    } else {
        debug!(
            "Listing gs://{}/{} (full)",
            config.source_bucket, config.source_prefix
        );
        let files = list_files_full(&object_store, &config.source_prefix, state).await?;
        (files, "full".to_string())
    };

    let total_listed = files_to_check.len();
    debug!("Listed {} files ({})", total_listed, listing_mode);

    if files_to_check.is_empty() {
        debug!("No files to check");
        return Ok(());
    }

    // Get first valid file for table creation if needed
    let first_file = files_to_check.first().cloned();

    // Try to open the Delta table, creating it if it doesn't exist
    let mut table = match oxbow::lock::open_table(&config.delta_table_uri).await {
        Ok(table) => table,
        Err(DeltaTableError::NotATable(_)) => {
            let sample_file = match &first_file {
                Some(f) => f,
                None => {
                    debug!("No valid files found to create table from");
                    return Ok(());
                }
            };
            info!(
                "Delta table does not exist at {}, creating from first JSON file",
                config.delta_table_uri
            );
            create_table_from_json(&config.delta_table_uri, &object_store, sample_file, config)
                .await?
        }
        Err(e) => return Err(Error::DeltaTable { source: e }),
    };

    let arrow_schema = table
        .snapshot()
        .context(DeltaTableSnafu)?
        .snapshot()
        .arrow_schema();

    // On first poll (or after restart), populate state from Delta table
    // This ensures atomicity: Delta is the source of truth
    if !*state_initialized {
        info!("Initializing processed files cache from Delta table...");
        let delta_processed = query_processed_files(&table).await?;
        if !delta_processed.is_empty() {
            info!(
                "Populated {} processed files from Delta table",
                delta_processed.len()
            );
            state.populate_from_delta(delta_processed);
            save_state(&config.state_file_uri, state).await?;
        }
        *state_initialized = true;
    }

    // Filter to files not yet processed (using state cache)
    let files_to_process: Vec<_> = files_to_check
        .into_iter()
        .filter(|obj| !state.is_processed(obj.location.as_ref()))
        .collect();

    if files_to_process.is_empty() {
        debug!("No new files to process ({} listed)", total_listed);
        return Ok(());
    }

    // Process files in batches
    let mut total_processed = 0usize;

    for (batch_idx, batch) in files_to_process.chunks(FILE_BATCH_SIZE).enumerate() {
        if files_to_process.len() > FILE_BATCH_SIZE {
            info!("Processing batch {} ({} files)", batch_idx + 1, batch.len());
        } else {
            info!("Processing {} files", batch.len());
        }

        let processed = process_file_batch(
            batch,
            &object_store,
            &arrow_schema,
            &mut table,
            state,
            config,
        )
        .await?;
        total_processed += processed;
    }

    // Prune old entries from state to keep it bounded
    state.prune_old_entries(STATE_PRUNE_AGE_SECS);

    if total_processed > 0 {
        info!(
            "Completed: processed {} files out of {} listed",
            total_processed, total_listed
        );
    }

    Ok(())
}

/// List files using time-bounded prefixes (last N hours).
/// Only lists files from hour-partitioned paths within the lookback window.
async fn list_files_time_bounded(
    object_store: &Arc<dyn ObjectStore>,
    source_prefix: &str,
    lookback_hours: u64,
    state: &ProcessedState,
) -> Result<Vec<deltalake::ObjectMeta>> {
    let prefixes = generate_hour_prefixes(source_prefix, lookback_hours);
    let mut all_files = Vec::new();

    for prefix in prefixes {
        let path = deltalake::Path::from(prefix.as_str());
        let mut listing = object_store.list(Some(&path));

        while let Some(result) = listing.next().await {
            let obj = result.map_err(|e| ObjectStoreError::from_source(e, &prefix))?;
            let name = obj.location.as_ref();

            if is_supported_json_file(name) && !state.is_permanently_failed(name) {
                all_files.push(obj);
            }
        }
    }

    Ok(all_files)
}

/// List all files under the source prefix (full listing).
async fn list_files_full(
    object_store: &Arc<dyn ObjectStore>,
    source_prefix: &str,
    state: &ProcessedState,
) -> Result<Vec<deltalake::ObjectMeta>> {
    let path = (!source_prefix.is_empty()).then(|| deltalake::Path::from(source_prefix));
    let mut listing = object_store.list(path.as_ref());
    let mut files = Vec::new();

    while let Some(result) = listing.next().await {
        let obj = result.map_err(|e| ObjectStoreError::from_source(e, source_prefix))?;
        let name = obj.location.as_ref();

        if is_supported_json_file(name) && !state.is_permanently_failed(name) {
            files.push(obj);
        }
    }

    Ok(files)
}

/// Generates hour-partitioned prefixes for the last N hours.
/// Returns prefixes in the format: `{base_prefix}/date=YYYY-MM-DD/hour=HH/`
fn generate_hour_prefixes(base_prefix: &str, lookback_hours: u64) -> Vec<String> {
    let now = Utc::now();
    let mut prefixes = Vec::with_capacity(lookback_hours as usize);

    for i in 0..lookback_hours {
        let time = now - ChronoDuration::hours(i as i64);
        let date = time.format("%Y-%m-%d");
        let hour = time.format("%H");

        let prefix = if base_prefix.is_empty() {
            format!("date={}/hour={}/", date, hour)
        } else {
            let base = base_prefix.trim_end_matches('/');
            format!("{}/date={}/hour={}/", base, date, hour)
        };
        prefixes.push(prefix);
    }

    prefixes
}

/// Create a new Delta table by inferring schema from a JSON file.
async fn create_table_from_json(
    table_uri: &str,
    object_store: &Arc<dyn ObjectStore>,
    sample_file: &deltalake::ObjectMeta,
    config: &Config,
) -> Result<deltalake::DeltaTable> {
    let file_path = sample_file.location.as_ref();
    let is_compressed = is_gzip_compressed(file_path);

    info!("Inferring schema from {} to create Delta table", file_path);

    let arrow_schema = sample_schema_from_file(
        object_store,
        &sample_file.location,
        is_compressed,
        SCHEMA_SAMPLE_BYTES,
    )
    .await?;

    let mut columns: Vec<StructField> = Vec::new();
    for field in arrow_schema.fields() {
        let coerced = oxbow::coerce_field(field.clone());
        if let Ok(delta_type) = DataType::try_from_arrow(coerced.data_type()) {
            columns.push(StructField::new(field.name().to_string(), delta_type, true));
        } else {
            warn!(
                "Could not convert field {} to Delta type, skipping",
                field.name()
            );
        }
    }

    columns.push(schema::source_file_field());

    info!(
        "Creating Delta table with {} columns inferred from {}",
        columns.len(),
        file_path
    );

    let table_url = Url::parse(table_uri).context(UrlParseSnafu { url: table_uri })?;
    let store = logstore_for(&table_url, StorageConfig::default()).context(DeltaTableSnafu)?;

    let table = CreateBuilder::new()
        .with_log_store(store)
        .with_columns(columns)
        .with_table_name(&config.delta_table_name)
        .with_save_mode(SaveMode::Ignore)
        .await
        .context(DeltaTableSnafu)?;

    info!(
        "Created Delta table '{}' at {}",
        config.delta_table_name, table_uri
    );

    Ok(table)
}

async fn process_file_batch(
    files: &[deltalake::ObjectMeta],
    object_store: &Arc<dyn ObjectStore>,
    arrow_schema: &Arc<deltalake::arrow::datatypes::Schema>,
    table: &mut deltalake::DeltaTable,
    state: &mut ProcessedState,
    config: &Config,
) -> Result<usize> {
    let mut processed_paths: Vec<String> = Vec::new();
    let mut total_records: usize = 0;

    let file_timeout = Duration::from_secs(config.file_timeout_secs);
    let max_retries = config.max_file_retries;

    let mut writer = RecordBatchWriter::for_table(table)
        .context(DeltaTableSnafu)?
        .with_commit_properties(oxbow::default_commit_properties());

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

                let result = tokio::time::timeout(
                    file_timeout,
                    stream_and_parse_file(&store, &location, schema, is_compressed, batch_size),
                )
                .await;

                (file_path, result)
            }
        })
        .buffer_unordered(config.download_concurrency);

    let mut has_batches = false;

    while let Some((file_path, result)) = stream.next().await {
        match result {
            Ok(Ok((batches, records))) => {
                info!("Parsed {} records from {}", records, file_path);
                let mut file_success = true;
                for batch in batches {
                    match augment_with_source_file(batch, &file_path) {
                        Ok(augmented) => {
                            if let Err(e) = writer.write(augmented).await {
                                error!("Failed to write batch to Delta writer: {:?}", e);
                                let write_error = Error::DeltaTable { source: e };
                                state.record_failure(&file_path, &write_error, max_retries);
                                file_success = false;
                                break;
                            }
                            has_batches = true;
                        }
                        Err(e) => {
                            error!("Failed to augment batch with source file: {:?}", e);
                            state.record_failure(&file_path, &e, max_retries);
                            file_success = false;
                            break;
                        }
                    }
                }
                if file_success {
                    state.clear_failure(&file_path);
                    processed_paths.push(file_path);
                    total_records += records;
                }
            }
            Ok(Err(e)) => {
                state.record_failure(&file_path, &e, max_retries);
            }
            Err(_) => {
                let timeout_error = Error::Timeout {
                    duration_secs: file_timeout.as_secs(),
                };
                error!("File processing timed out: {}", file_path);
                state.record_failure(&file_path, &timeout_error, max_retries);
            }
        }
    }

    let files_processed = processed_paths.len();

    if has_batches {
        *table = commit_writer(table.clone(), writer).await?;

        // Mark files as processed in state (after successful Delta commit)
        state.mark_processed_batch(&processed_paths);

        // Save state
        save_state(&config.state_file_uri, state).await?;

        info!(
            "Committed {} files ({} records) to Delta v{:?}",
            files_processed,
            total_records,
            table.version(),
        );
    }

    Ok(files_processed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_hour_prefixes_with_base_prefix() {
        let prefixes = generate_hour_prefixes("region=us/subject=ticks/version=v1", 2);

        assert_eq!(prefixes.len(), 2);
        for prefix in &prefixes {
            assert!(prefix.starts_with("region=us/subject=ticks/version=v1/date="));
            assert!(prefix.contains("/hour="));
            assert!(prefix.ends_with('/'));
        }
    }

    #[test]
    fn test_generate_hour_prefixes_empty_base() {
        let prefixes = generate_hour_prefixes("", 1);

        assert_eq!(prefixes.len(), 1);
        assert!(prefixes[0].starts_with("date="));
        assert!(prefixes[0].contains("/hour="));
    }
}
