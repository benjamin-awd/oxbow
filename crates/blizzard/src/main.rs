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
use blizzard::schema::{self, try_detect_schema_evolution, with_source_file_column};
use blizzard::state::{ProcessedState, load_state, query_processed_files, save_state};
use blizzard::store::ObjectStoreResultExt;
use blizzard::write::{commit_writer, commit_writer_with_schema_evolution};

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

/// Prefix where processed files are moved to
const ARCHIVE_PREFIX: &str = "processed/";

/// Number of files to process in a single Delta commit
const FILE_BATCH_SIZE: usize = 100;

/// Max age in seconds before health check considers service unhealthy
const HEALTH_MAX_AGE_SECS: u64 = 300;

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    deltalake::gcp::register_handlers(None);

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    info!("Starting file-loader polling service");

    let config = Config::from_env();

    info!(
        "Configuration: bucket={}, prefix={}, table={}, table_name={}, state={}, interval={}s, concurrency={}, batch_size={}, schema_evolution={}, schema_sample_bytes={}, file_timeout={}s, max_retries={}",
        config.source_bucket,
        config.source_prefix,
        config.delta_table_uri,
        config.delta_table_name,
        config.state_file_uri,
        config.poll_interval,
        config.download_concurrency,
        config.batch_size,
        config.schema_evolution,
        config.schema_sample_bytes,
        config.file_timeout_secs,
        config.max_file_retries
    );

    // Start health check server with heartbeat tracking
    let heartbeat = health::Heartbeat::new(HEALTH_MAX_AGE_SECS);
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("0.0.0.0:{port}");
    health::spawn_health_server(addr, heartbeat.clone());

    loop {
        if let Err(e) = poll_and_process(&config).await {
            if e.is_fatal() {
                error!("Fatal error during poll cycle: {:?}", e);
            } else if e.is_transient() {
                warn!("Transient error during poll cycle (will retry): {:?}", e);
            } else {
                error!("Error during poll cycle: {:?}", e);
            }
        }

        // Update heartbeat after each cycle
        heartbeat.touch();

        tokio::time::sleep(Duration::from_secs(config.poll_interval)).await;
    }
}

async fn poll_and_process(config: &Config) -> Result<()> {
    debug!(
        "Polling gs://{}/{} for new files",
        config.source_bucket, config.source_prefix
    );

    let bucket_url =
        Url::parse(&format!("gs://{}", config.source_bucket)).context(UrlParseSnafu {
            url: format!("gs://{}", config.source_bucket),
        })?;
    let store = logstore_for(&bucket_url, StorageConfig::default()).context(DeltaTableSnafu)?;
    let object_store = store.object_store(None);

    let mut state = load_state(&config.state_file_uri).await?;

    let list_prefix = (!config.source_prefix.is_empty())
        .then(|| deltalake::Path::from(config.source_prefix.as_str()));

    // Stream listing and process in chunks to avoid loading all file metadata into memory
    let mut listing = object_store.list(list_prefix.as_ref());

    // First, peek at the listing to get the first valid file for table creation
    let first_file = peek_first_valid_file(&mut listing, &state).await?;

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

    // Query Delta table for files that have already been processed
    // This provides atomic state tracking - if a crash occurs after Delta commit,
    // we won't reprocess files because they're already tracked in the table
    let delta_processed_files = query_processed_files(&table).await?;
    if !delta_processed_files.is_empty() {
        info!(
            "Found {} previously processed files in Delta table",
            delta_processed_files.len()
        );
    }

    // Process files in batches as we stream the listing
    let mut batch: Vec<deltalake::ObjectMeta> = Vec::with_capacity(FILE_BATCH_SIZE);
    let mut total_processed = 0usize;
    let mut total_listed = 0usize;
    let mut batch_num = 0usize;

    // Add the first file to the batch if it wasn't already processed
    if let Some(f) = first_file {
        total_listed += 1;
        if !delta_processed_files.contains(f.location.as_ref()) {
            batch.push(f);
        }
    }

    // Continue streaming the rest of the listing
    while let Some(result) = listing.next().await {
        let obj = result.map_err(|e| {
            ObjectStoreError::from_source(e, &format!("gs://{}", config.source_bucket))
        })?;
        total_listed += 1;

        let name = obj.location.as_ref();

        // Skip archived files
        if name.starts_with(ARCHIVE_PREFIX) {
            continue;
        }

        // Collect supported JSON files that haven't permanently failed and aren't already processed
        if is_supported_json_file(name)
            && !state.is_permanently_failed(name)
            && !delta_processed_files.contains(name)
        {
            batch.push(obj);
        }

        // Process batch when it reaches FILE_BATCH_SIZE
        if batch.len() >= FILE_BATCH_SIZE {
            batch_num += 1;
            info!("Processing batch {} ({} files)", batch_num, batch.len());
            let processed =
                process_file_batch(&batch, &object_store, &arrow_schema, &mut table, &mut state, config).await?;
            total_processed += processed;
            batch.clear();
        }
    }

    // Process remaining files in the final batch
    if !batch.is_empty() {
        if batch_num > 0 {
            batch_num += 1;
            info!("Processing batch {} ({} files)", batch_num, batch.len());
        } else {
            info!("Processing {} files", batch.len());
        }
        let processed =
            process_file_batch(&batch, &object_store, &arrow_schema, &mut table, &mut state, config).await?;
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

/// Peek at the listing stream to get the first valid file for table creation.
/// This consumes only the first matching file from the stream.
async fn peek_first_valid_file(
    listing: &mut futures::stream::BoxStream<
        'static,
        std::result::Result<deltalake::ObjectMeta, deltalake::ObjectStoreError>,
    >,
    state: &ProcessedState,
) -> Result<Option<deltalake::ObjectMeta>> {
    while let Some(result) = listing.next().await {
        let obj = result.map_err(|e| ObjectStoreError::from_source(e, "listing"))?;
        let name = obj.location.as_ref();

        // Skip archived files
        if name.starts_with(ARCHIVE_PREFIX) {
            continue;
        }

        // Return first supported JSON file that hasn't permanently failed
        if is_supported_json_file(name) && !state.is_permanently_failed(name) {
            return Ok(Some(obj));
        }
    }
    Ok(None)
}

/// Create a new Delta table by inferring schema from a JSON file.
///
/// This function samples the schema from the first JSON file and creates a Delta table
/// with the inferred schema plus the `_source_file` column for tracking.
async fn create_table_from_json(
    table_uri: &str,
    object_store: &Arc<dyn ObjectStore>,
    sample_file: &deltalake::ObjectMeta,
    config: &Config,
) -> Result<deltalake::DeltaTable> {
    let file_path = sample_file.location.as_ref();
    let is_compressed = is_gzip_compressed(file_path);

    info!("Inferring schema from {} to create Delta table", file_path);

    // Sample schema from JSON file
    let arrow_schema = sample_schema_from_file(
        object_store,
        &sample_file.location,
        is_compressed,
        config.schema_sample_bytes,
    )
    .await?;

    // Convert Arrow fields to Delta StructFields
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

    // Add _source_file column for atomic state tracking
    columns.push(schema::source_file_field());

    info!(
        "Creating Delta table with {} columns inferred from {}",
        columns.len(),
        file_path
    );

    // Create the table
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
    let mut new_fields: Vec<StructField> = Vec::new();

    // Check if _source_file column needs to be added to the table schema
    let table_schema = table.snapshot().context(DeltaTableSnafu)?.schema();
    if table_schema.index_of(schema::SOURCE_FILE_COLUMN).is_none() {
        info!(
            "Adding {} column to table schema for atomic state tracking",
            schema::SOURCE_FILE_COLUMN
        );
        new_fields.push(schema::source_file_field());
    }

    let parsing_schema = if config.schema_evolution
        && let Some(first_file) = files.first()
    {
        let (schema, evolved_fields) = try_detect_schema_evolution(
            object_store,
            &first_file.location,
            table,
            arrow_schema,
            config.schema_sample_bytes,
        )
        .await;
        new_fields.extend(evolved_fields);
        schema
    } else {
        Arc::clone(arrow_schema)
    };

    let file_timeout = Duration::from_secs(config.file_timeout_secs);
    let max_retries = config.max_file_retries;
    let needs_schema_evolution = !new_fields.is_empty();

    // Build the writer schema: parsing_schema + _source_file column
    let writer_schema = Arc::new(with_source_file_column(&parsing_schema));

    // Create writer with appropriate schema - use try_new for evolved schema,
    // for_table for non-evolution (simpler, handles storage options automatically)
    let mut writer = if needs_schema_evolution {
        RecordBatchWriter::try_new(
            &config.delta_table_uri,
            writer_schema,
            None, // no partition columns
            Some(std::collections::HashMap::new()),
        )
        .context(DeltaTableSnafu)?
        .with_commit_properties(oxbow::default_commit_properties())
    } else {
        RecordBatchWriter::for_table(table)
            .context(DeltaTableSnafu)?
            .with_commit_properties(oxbow::default_commit_properties())
    };

    let mut stream = futures::stream::iter(files)
        .map(|obj| {
            let store = Arc::clone(object_store);
            let schema = Arc::clone(&parsing_schema);
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

                // Apply timeout to the file download and parsing
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
                            // Stream directly to writer (works for both evolution and non-evolution)
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
        let updated_table = if needs_schema_evolution {
            // Schema evolution: flush writer and commit with metadata action
            commit_writer_with_schema_evolution(table.clone(), writer, &new_fields).await?
        } else {
            // No evolution: simple commit
            commit_writer(table.clone(), writer).await?
        };
        *table = updated_table;

        // Save state file for failure tracking (processed files are now tracked in Delta)
        save_state(&config.state_file_uri, state).await?;

        info!(
            "Committed {} files ({} records) to Delta v{:?}{}",
            files_processed,
            total_records,
            table.version(),
            if !new_fields.is_empty() {
                format!(" (schema evolved with {} new fields)", new_fields.len())
            } else {
                String::new()
            }
        );

        archive_processed_files(object_store, &processed_paths).await;
    }

    Ok(files_processed)
}

async fn archive_processed_files(object_store: &Arc<dyn ObjectStore>, paths: &[String]) {
    let mut archived = 0;
    for path in paths {
        match archive_file(object_store, path, ARCHIVE_PREFIX).await {
            Ok(()) => archived += 1,
            Err(Error::ObjectStore {
                source: ObjectStoreError::NotFound { .. },
            }) => {
                debug!("File {} already gone during archive, treating as success", path);
                archived += 1;
            }
            Err(e) => {
                warn!("Failed to archive {}: {:?}", path, e);
            }
        }
    }
    if archived > 0 {
        info!("Archived {} files to {}", archived, ARCHIVE_PREFIX);
    }
}

async fn archive_file(
    object_store: &Arc<dyn ObjectStore>,
    source_path: &str,
    archive_prefix: &str,
) -> Result<()> {
    let archive_path = compute_archive_path(source_path, archive_prefix);

    let from = deltalake::Path::from(source_path);
    let to = deltalake::Path::from(archive_path.as_str());

    object_store
        .copy(&from, &to)
        .await
        .with_path_context(source_path)?;
    object_store
        .delete(&from)
        .await
        .with_path_context(source_path)?;

    debug!("Archived {} -> {}", source_path, archive_path);
    Ok(())
}

/// Computes the archive path for a source file.
/// Preserves the full source path to avoid collisions between different source prefixes.
fn compute_archive_path(source_path: &str, archive_prefix: &str) -> String {
    format!("{}{}", archive_prefix, source_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_archive_path_preserves_full_path_structure() {
        let source_path = "region=asia-northeast1/auth_type=public/subject=orderbooks/version=v1/2024/01/file.json";
        let archive_prefix = "processed/";

        let archive_path = compute_archive_path(source_path, archive_prefix);

        // The archive path should preserve the full path structure to avoid collisions
        assert_eq!(
            archive_path,
            "processed/region=asia-northeast1/auth_type=public/subject=orderbooks/version=v1/2024/01/file.json",
            "Archive path should preserve full source path to avoid collisions between different source prefixes"
        );
    }
}
