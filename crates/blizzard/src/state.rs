use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::log::*;
use url::Url;

/// Tracks failure information for a file
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileFailure {
    /// Number of times this file has failed
    pub retry_count: usize,
    /// Last error message
    pub last_error: String,
}

/// State file tracking which files have been processed
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProcessedState {
    #[serde(default)]
    pub processed_files: HashSet<String>,
    /// Files that have failed processing with their retry counts
    #[serde(default)]
    pub failed_files: HashMap<String, FileFailure>,
    /// Files that have permanently failed (exceeded max retries)
    #[serde(default)]
    pub permanently_failed: HashSet<String>,
}

impl ProcessedState {
    /// Record a file failure, returns true if the file should be retried
    pub fn record_failure(&mut self, path: &str, error: &str, max_retries: usize) -> bool {
        let entry = self.failed_files.entry(path.to_string()).or_default();
        entry.retry_count += 1;
        entry.last_error = error.to_string();

        if entry.retry_count >= max_retries {
            warn!(
                "File {} has failed {} times, marking as permanently failed: {}",
                path, entry.retry_count, error
            );
            self.permanently_failed.insert(path.to_string());
            self.failed_files.remove(path);
            false
        } else {
            warn!(
                "File {} failed (attempt {}/{}): {}",
                path, entry.retry_count, max_retries, error
            );
            true
        }
    }

    /// Clear failure record for a successfully processed file
    pub fn clear_failure(&mut self, path: &str) {
        self.failed_files.remove(path);
    }

    /// Check if a file should be skipped (already processed or permanently failed)
    pub fn should_skip(&self, path: &str) -> bool {
        self.processed_files.contains(path) || self.permanently_failed.contains(path)
    }
}

pub async fn load_state(state_file_uri: &str) -> Result<ProcessedState, anyhow::Error> {
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

pub async fn save_state(state_file_uri: &str, state: &ProcessedState) -> Result<(), anyhow::Error> {
    use deltalake::logstore::{StorageConfig, logstore_for};

    let url = Url::parse(state_file_uri)?;
    let store = logstore_for(&url, StorageConfig::default())?;
    let path = deltalake::Path::from(url.path());

    let bytes = serde_json::to_vec_pretty(state)?;
    store.object_store(None).put(&path, bytes.into()).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_serialization() {
        let mut state = ProcessedState::default();
        state.processed_files.insert("file1.ndjson".to_string());
        state.processed_files.insert("file2.ndjson.gz".to_string());

        let json = serde_json::to_string(&state).unwrap();
        let restored: ProcessedState = serde_json::from_str(&json).unwrap();

        assert_eq!(state.processed_files, restored.processed_files);
    }

    #[test]
    fn test_failure_tracking() {
        let mut state = ProcessedState::default();
        let max_retries = 3;

        // First failure - should retry
        assert!(state.record_failure("file1.json", "error 1", max_retries));
        assert!(!state.should_skip("file1.json"));
        assert_eq!(state.failed_files.get("file1.json").unwrap().retry_count, 1);

        // Second failure - should retry
        assert!(state.record_failure("file1.json", "error 2", max_retries));
        assert!(!state.should_skip("file1.json"));
        assert_eq!(state.failed_files.get("file1.json").unwrap().retry_count, 2);

        // Third failure - should be permanently failed
        assert!(!state.record_failure("file1.json", "error 3", max_retries));
        assert!(state.should_skip("file1.json"));
        assert!(state.permanently_failed.contains("file1.json"));
        assert!(!state.failed_files.contains_key("file1.json"));
    }

    #[test]
    fn test_clear_failure_on_success() {
        let mut state = ProcessedState::default();

        // Record a failure
        state.record_failure("file1.json", "error", 3);
        assert!(state.failed_files.contains_key("file1.json"));

        // Clear on success
        state.clear_failure("file1.json");
        assert!(!state.failed_files.contains_key("file1.json"));
    }

    #[test]
    fn test_should_skip() {
        let mut state = ProcessedState::default();

        // New file - shouldn't skip
        assert!(!state.should_skip("file1.json"));

        // Processed file - should skip
        state.processed_files.insert("file2.json".to_string());
        assert!(state.should_skip("file2.json"));

        // Permanently failed file - should skip
        state.permanently_failed.insert("file3.json".to_string());
        assert!(state.should_skip("file3.json"));
    }

    #[test]
    fn test_backward_compatibility() {
        // Old state format without new fields should still deserialize
        let old_json = r#"{"processed_files":["file1.json"]}"#;
        let state: ProcessedState = serde_json::from_str(old_json).unwrap();

        assert!(state.processed_files.contains("file1.json"));
        assert!(state.failed_files.is_empty());
        assert!(state.permanently_failed.is_empty());
    }
}
