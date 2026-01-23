use deltalake::DeltaTable;
use deltalake::arrow::array::Array;
use deltalake::datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::log::*;

use crate::error::{
    DataFusionSnafu, DeltaTableSnafu, Error, ErrorCategory, ObjectStoreError, Result,
    StateSerializationSnafu,
};
use crate::schema::SOURCE_FILE_COLUMN;
use crate::store::{ObjectStoreResultExt, object_store_for_uri};

/// Tracks failure information for a file
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileFailure {
    /// Number of times this file has failed
    pub retry_count: usize,
    /// Last error message
    pub last_error: String,
    /// Error category for structured tracking
    #[serde(default)]
    pub error_category: Option<ErrorCategory>,
    /// Whether the error is transient (retryable)
    #[serde(default)]
    pub is_transient: Option<bool>,
}

/// State file tracking file failures (processed files are tracked in Delta via _source_file column)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProcessedState {
    /// Files that have failed processing with their retry counts
    #[serde(default)]
    pub failed_files: HashMap<String, FileFailure>,
    /// Files that have permanently failed (exceeded max retries)
    #[serde(default)]
    pub permanently_failed: HashSet<String>,
}

impl ProcessedState {
    /// Record a file failure, returns true if the file should be retried
    pub fn record_failure(&mut self, path: &str, error: &Error, max_retries: usize) -> bool {
        self.record_failure_impl(
            path,
            &format!("{:?}", error),
            Some(ErrorCategory::from(error)),
            Some(error.is_transient()),
            max_retries,
        )
    }

    fn record_failure_impl(
        &mut self,
        path: &str,
        error_msg: &str,
        error_category: Option<ErrorCategory>,
        is_transient: Option<bool>,
        max_retries: usize,
    ) -> bool {
        let entry = self.failed_files.entry(path.to_string()).or_default();
        entry.retry_count += 1;
        entry.last_error = error_msg.to_string();
        entry.error_category = error_category;
        entry.is_transient = is_transient;

        if entry.retry_count >= max_retries {
            warn!(
                "File {} has failed {} times (category: {:?}, transient: {:?}), marking as permanently failed: {}",
                path, entry.retry_count, error_category, is_transient, error_msg
            );
            self.permanently_failed.insert(path.to_string());
            self.failed_files.remove(path);
            false
        } else {
            warn!(
                "File {} failed (attempt {}/{}, category: {:?}, transient: {:?}): {}",
                path, entry.retry_count, max_retries, error_category, is_transient, error_msg
            );
            true
        }
    }

    /// Clear failure record for a successfully processed file
    pub fn clear_failure(&mut self, path: &str) {
        self.failed_files.remove(path);
    }

    /// Check if a file should be skipped due to permanent failure
    /// Note: Successfully processed files are tracked via Delta's _source_file column
    pub fn is_permanently_failed(&self, path: &str) -> bool {
        self.permanently_failed.contains(path)
    }
}

/// Query the Delta table to get all previously processed source files
///
/// Uses DataFusion to execute `SELECT DISTINCT _source_file FROM table`.
/// Returns an empty set if the column doesn't exist (table hasn't been migrated yet).
pub async fn query_processed_files(table: &DeltaTable) -> Result<HashSet<String>> {
    let ctx = SessionContext::new();
    ctx.register_table("source_table", Arc::new(table.clone()))
        .context(DataFusionSnafu)?;

    // Check if the _source_file column exists in the schema
    let schema = table.snapshot().context(DeltaTableSnafu)?.schema();
    if schema.index_of(SOURCE_FILE_COLUMN).is_none() {
        debug!(
            "Column {} not found in table schema, returning empty set",
            SOURCE_FILE_COLUMN
        );
        return Ok(HashSet::new());
    }

    let df = ctx
        .sql(&format!(
            "SELECT DISTINCT {} FROM source_table",
            SOURCE_FILE_COLUMN
        ))
        .await
        .context(DataFusionSnafu)?;

    let batches = df.collect().await.context(DataFusionSnafu)?;

    let mut processed_files = HashSet::new();
    for batch in batches {
        if let Some(array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<deltalake::arrow::array::StringArray>()
        {
            for i in 0..array.len() {
                if !array.is_null(i) {
                    processed_files.insert(array.value(i).to_string());
                }
            }
        }
    }

    debug!(
        "Found {} previously processed files in Delta table",
        processed_files.len()
    );
    Ok(processed_files)
}

pub async fn load_state(state_file_uri: &str) -> Result<ProcessedState> {
    let (store, path) = object_store_for_uri(state_file_uri).await?;

    match store.get(&path).await {
        Ok(result) => {
            let bytes = result.bytes().await.with_path_context(state_file_uri)?;
            let state: ProcessedState =
                serde_json::from_slice(&bytes).context(StateSerializationSnafu)?;
            debug!(
                "Loaded state with {} failed files, {} permanently failed",
                state.failed_files.len(),
                state.permanently_failed.len()
            );
            Ok(state)
        }
        Err(deltalake::ObjectStoreError::NotFound { .. }) => {
            debug!("No existing state file, starting fresh");
            Ok(ProcessedState::default())
        }
        Err(e) => Err(ObjectStoreError::from_source(e, state_file_uri).into()),
    }
}

pub async fn save_state(state_file_uri: &str, state: &ProcessedState) -> Result<()> {
    let (store, path) = object_store_for_uri(state_file_uri).await?;

    let bytes = serde_json::to_vec_pretty(state).context(StateSerializationSnafu)?;
    store
        .put(&path, bytes.into())
        .await
        .with_path_context(state_file_uri)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    impl ProcessedState {
        /// Test helper: record a failure with a string error message
        fn record_failure_str(&mut self, path: &str, error: &str, max_retries: usize) -> bool {
            self.record_failure_impl(path, error, None, None, max_retries)
        }
    }

    #[test]
    fn test_state_serialization() {
        let mut state = ProcessedState::default();
        state.permanently_failed.insert("file1.ndjson".to_string());
        state
            .permanently_failed
            .insert("file2.ndjson.gz".to_string());

        let json = serde_json::to_string(&state).unwrap();
        let restored: ProcessedState = serde_json::from_str(&json).unwrap();

        assert_eq!(state.permanently_failed, restored.permanently_failed);
    }

    #[test]
    fn test_failure_tracking() {
        let mut state = ProcessedState::default();
        let max_retries = 3;

        // First failure - should retry
        assert!(state.record_failure_str("file1.json", "error 1", max_retries));
        assert!(!state.is_permanently_failed("file1.json"));
        assert_eq!(state.failed_files.get("file1.json").unwrap().retry_count, 1);

        // Second failure - should retry
        assert!(state.record_failure_str("file1.json", "error 2", max_retries));
        assert!(!state.is_permanently_failed("file1.json"));
        assert_eq!(state.failed_files.get("file1.json").unwrap().retry_count, 2);

        // Third failure - should be permanently failed
        assert!(!state.record_failure_str("file1.json", "error 3", max_retries));
        assert!(state.is_permanently_failed("file1.json"));
        assert!(state.permanently_failed.contains("file1.json"));
        assert!(!state.failed_files.contains_key("file1.json"));
    }

    #[test]
    fn test_clear_failure_on_success() {
        let mut state = ProcessedState::default();

        // Record a failure
        state.record_failure_str("file1.json", "error", 3);
        assert!(state.failed_files.contains_key("file1.json"));

        // Clear on success
        state.clear_failure("file1.json");
        assert!(!state.failed_files.contains_key("file1.json"));
    }

    #[test]
    fn test_is_permanently_failed() {
        let mut state = ProcessedState::default();

        // New file - not permanently failed
        assert!(!state.is_permanently_failed("file1.json"));

        // Permanently failed file
        state.permanently_failed.insert("file2.json".to_string());
        assert!(state.is_permanently_failed("file2.json"));
    }

    #[test]
    fn test_backward_compatibility() {
        // Old state format with processed_files should still deserialize (ignored)
        let old_json = r#"{"processed_files":["file1.json"]}"#;
        let state: ProcessedState = serde_json::from_str(old_json).unwrap();

        // processed_files is now ignored, only failure tracking matters
        assert!(state.failed_files.is_empty());
        assert!(state.permanently_failed.is_empty());
    }
}
