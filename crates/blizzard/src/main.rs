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
mod config;
mod error;
mod health;
mod read;
mod schema;
mod state;

use deltalake::ObjectStore;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tracing::log::*;
use url::Url;

use config::Config;
use error::{
    DeltaTableSnafu, Error, ObjectStoreError, Result, SchemaInferenceSnafu, UrlParseSnafu,
};
use read::{
    is_gzip_compressed, is_supported_json_file, sample_schema_from_file, stream_and_parse_file,
};
use schema::{create_merged_arrow_schema, create_metadata_action, merge_schemas, needs_evolution};
use state::{ProcessedState, load_state, save_state};

/// Prefix where processed files are moved to
const ARCHIVE_PREFIX: &str = "processed/";

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
        "Configuration: bucket={}, prefix={}, table={}, state={}, interval={}s, concurrency={}, batch_size={}, schema_evolution={}, schema_sample_bytes={}, file_timeout={}s, max_retries={}",
        config.source_bucket,
        config.source_prefix,
        config.delta_table_uri,
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
    // Consider unhealthy if no heartbeat for 5 minutes
    let heartbeat = health::Heartbeat::new(300);
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
    use deltalake::logstore::{StorageConfig, logstore_for};
    use snafu::ResultExt;

    const FILE_BATCH_SIZE: usize = 100;

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

    let mut listing = object_store.list(list_prefix.as_ref());
    let mut new_files = Vec::with_capacity(FILE_BATCH_SIZE);
    let mut total_listed = 0usize;
    let mut total_processed = 0usize;

    // Open delta table
    let mut table = oxbow::lock::open_table(&config.delta_table_uri)
        .await
        .context(DeltaTableSnafu)?;
    let arrow_schema = table
        .snapshot()
        .context(DeltaTableSnafu)?
        .snapshot()
        .arrow_schema();

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

        if is_supported_json_file(name) && !state.should_skip(name) {
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

async fn process_file_batch(
    files: &[deltalake::ObjectMeta],
    object_store: &Arc<dyn ObjectStore>,
    arrow_schema: &Arc<deltalake::arrow::datatypes::Schema>,
    table: &mut deltalake::DeltaTable,
    state: &mut ProcessedState,
    config: &Config,
) -> Result<usize> {
    use deltalake::kernel::StructField;
    use snafu::ResultExt;

    let mut all_batches: Vec<deltalake::arrow::array::RecordBatch> = Vec::new();
    let mut processed_paths: Vec<String> = Vec::new();
    let mut total_records: usize = 0;
    let mut new_fields: Vec<StructField> = Vec::new();

    let parsing_schema: Arc<deltalake::arrow::datatypes::Schema> = if config.schema_evolution {
        // Sample schema from first file to detect new fields
        if let Some(first_file) = files.first() {
            let file_path = first_file.location.as_ref();
            let is_compressed = is_gzip_compressed(file_path);

            match sample_schema_from_file(
                object_store,
                &first_file.location,
                is_compressed,
                config.schema_sample_bytes,
            )
            .await
            {
                Ok(file_schema) => {
                    let table_schema = table.snapshot().context(DeltaTableSnafu)?.schema();
                    if needs_evolution(&table_schema, &file_schema) {
                        new_fields = merge_schemas(&table_schema, &file_schema);
                        let merged = create_merged_arrow_schema(arrow_schema, &file_schema);
                        info!(
                            "Schema evolution detected: {} new fields from {}",
                            new_fields.len(),
                            file_path
                        );
                        Arc::new(merged)
                    } else {
                        Arc::clone(arrow_schema)
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to sample schema from {}: {:?}, using table schema",
                        file_path, e
                    );
                    Arc::clone(arrow_schema)
                }
            }
        } else {
            Arc::clone(arrow_schema)
        }
    } else {
        Arc::clone(arrow_schema)
    };

    let file_timeout = Duration::from_secs(config.file_timeout_secs);
    let max_retries = config.max_file_retries;

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

    while let Some((file_path, result)) = stream.next().await {
        match result {
            Ok(Ok((batches, records))) => {
                info!("Parsed {} records from {}", records, file_path);
                all_batches.extend(batches);
                state.clear_failure(&file_path);
                processed_paths.push(file_path);
                total_records += records;
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

    if !all_batches.is_empty() {
        let batch_iter = all_batches.into_iter().map(Ok);

        let updated_table = if !new_fields.is_empty() {
            commit_with_schema_evolution(table.clone(), batch_iter, &new_fields).await?
        } else {
            oxbow::write::append_batches(table.clone(), batch_iter)
                .await
                .context(DeltaTableSnafu)?
        };
        *table = updated_table;

        state
            .processed_files
            .extend(processed_paths.iter().cloned());
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

        let mut archived = 0;
        for path in &processed_paths {
            match archive_file(object_store, path, &config.source_prefix, ARCHIVE_PREFIX).await {
                Ok(()) => archived += 1,
                Err(Error::ObjectStore {
                    source: ObjectStoreError::NotFound { .. },
                }) => {
                    // File already archived or deleted - treat as success
                    debug!(
                        "File {} already gone during archive, treating as success",
                        path
                    );
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

    Ok(files_processed)
}

async fn commit_with_schema_evolution(
    mut table: deltalake::DeltaTable,
    batches: impl IntoIterator<
        Item = std::result::Result<
            deltalake::arrow::array::RecordBatch,
            deltalake::arrow::error::ArrowError,
        >,
    >,
    new_fields: &[deltalake::kernel::StructField],
) -> Result<deltalake::DeltaTable> {
    use deltalake::kernel::transaction::CommitBuilder;
    use deltalake::protocol::DeltaOperation;
    use deltalake::writer::{DeltaWriter, record_batch::RecordBatchWriter};
    use snafu::ResultExt;

    let metadata_action = create_metadata_action(&table, new_fields)?;

    let mut writer = RecordBatchWriter::for_table(&table)
        .context(DeltaTableSnafu)?
        .with_commit_properties(oxbow::default_commit_properties());

    for batch in batches {
        let batch = batch.context(SchemaInferenceSnafu)?;
        writer.write(batch).await.context(DeltaTableSnafu)?;
    }

    // Flush to get add actions without committing
    let add_actions = writer.flush().await.context(DeltaTableSnafu)?;

    // Combine metadata + add actions in single atomic commit
    let mut all_actions = vec![metadata_action];
    all_actions.extend(add_actions.into_iter().map(deltalake::kernel::Action::Add));

    let snapshot = table.snapshot().context(DeltaTableSnafu)?;
    let commit = CommitBuilder::from(oxbow::default_commit_properties())
        .with_actions(all_actions)
        .build(
            Some(snapshot),
            table.log_store(),
            DeltaOperation::Write {
                mode: deltalake::protocol::SaveMode::Append,
                partition_by: None,
                predicate: None,
            },
        );

    let post_commit = commit.await.context(DeltaTableSnafu)?;

    // Reload table with new version
    table
        .load_version(post_commit.version())
        .await
        .context(DeltaTableSnafu)?;

    Ok(table)
}

async fn archive_file(
    object_store: &Arc<dyn ObjectStore>,
    source_path: &str,
    source_prefix: &str,
    archive_prefix: &str,
) -> Result<()> {
    let archive_path = if source_path.starts_with(source_prefix) {
        format!("{}{}", archive_prefix, &source_path[source_prefix.len()..])
    } else {
        format!("{}{}", archive_prefix, source_path)
    };

    let from = deltalake::Path::from(source_path);
    let to = deltalake::Path::from(archive_path.as_str());

    object_store
        .copy(&from, &to)
        .await
        .map_err(|e| ObjectStoreError::from_source(e, source_path))?;
    object_store
        .delete(&from)
        .await
        .map_err(|e| ObjectStoreError::from_source(e, source_path))?;

    debug!("Archived {} -> {}", source_path, archive_path);
    Ok(())
}
