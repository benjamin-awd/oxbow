use chrono::{Duration, NaiveDate, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use tracing::log::*;
use url::Url;

/// Regex to extract date in YYYY-MM-DD format from file paths
static DATE_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"/(\d{4}-\d{2}-\d{2})/").unwrap());

/// Extract date from path patterns like `/2025-01-22/`
fn extract_date_from_path(path: &str) -> Option<NaiveDate> {
    DATE_REGEX
        .captures(path)
        .and_then(|caps| caps.get(1))
        .and_then(|m| NaiveDate::parse_from_str(m.as_str(), "%Y-%m-%d").ok())
}

/// Statistics from pruning operation
#[derive(Debug, Default)]
pub struct PruneStats {
    pub processed_pruned: usize,
    pub failed_pruned: usize,
}

/// Tracks failure information for a file
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileFailure {
    /// Number of times this file has failed
    pub retry_count: usize,
    /// Last error message
    pub last_error: String,
}

/// State file tracking which files have been processed
/// Uses serde(default) for backward compatibility with old state files
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

    /// Prune entries older than the retention period.
    /// Entries without parseable dates are retained (safe default).
    /// Returns statistics about what was pruned.
    pub fn prune_old_entries(&mut self, retention_days: u64) -> PruneStats {
        if retention_days == 0 {
            return PruneStats::default();
        }

        let cutoff = Utc::now().date_naive() - Duration::days(retention_days as i64);
        let mut stats = PruneStats::default();

        let processed_before = self.processed_files.len();
        self.processed_files.retain(|path| {
            extract_date_from_path(path)
                .map(|date| date >= cutoff)
                .unwrap_or(true) // Keep entries without parseable dates
        });
        stats.processed_pruned = processed_before - self.processed_files.len();

        let failed_before = self.permanently_failed.len();
        self.permanently_failed.retain(|path| {
            extract_date_from_path(path)
                .map(|date| date >= cutoff)
                .unwrap_or(true) // Keep entries without parseable dates
        });
        stats.failed_pruned = failed_before - self.permanently_failed.len();

        stats
    }
}

/// Load processed state from GCS
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

/// Save processed state to GCS
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

    #[test]
    fn test_extract_date_from_path() {
        // Valid paths with dates
        assert_eq!(
            extract_date_from_path("region=us/2025-01-22/hour=10/file.ndjson.gz"),
            Some(NaiveDate::from_ymd_opt(2025, 1, 22).unwrap())
        );
        assert_eq!(
            extract_date_from_path("/2024-12-31/data.json"),
            Some(NaiveDate::from_ymd_opt(2024, 12, 31).unwrap())
        );
        assert_eq!(
            extract_date_from_path("prefix/2023-06-15/suffix/file.ndjson"),
            Some(NaiveDate::from_ymd_opt(2023, 6, 15).unwrap())
        );

        // Invalid or missing dates
        assert_eq!(extract_date_from_path("no-date-here.json"), None);
        assert_eq!(extract_date_from_path("2025-01-22.json"), None); // No slashes
        assert_eq!(extract_date_from_path("/2025-13-01/invalid.json"), None); // Invalid month
        assert_eq!(extract_date_from_path("/2025-01-32/invalid.json"), None); // Invalid day
        assert_eq!(extract_date_from_path(""), None);
    }

    #[test]
    fn test_prune_old_entries() {
        let mut state = ProcessedState::default();
        let today = Utc::now().date_naive();
        let yesterday = today - Duration::days(1);
        let old_date = today - Duration::days(10);

        // Format dates as path components
        let recent_path = format!("region=us/{}/file1.ndjson", yesterday.format("%Y-%m-%d"));
        let old_path = format!("region=us/{}/file2.ndjson", old_date.format("%Y-%m-%d"));
        let unparseable_path = "no-date-file.json".to_string();

        // Add entries
        state.processed_files.insert(recent_path.clone());
        state.processed_files.insert(old_path.clone());
        state.processed_files.insert(unparseable_path.clone());

        state.permanently_failed.insert(format!(
            "region=eu/{}/failed.ndjson",
            old_date.format("%Y-%m-%d")
        ));

        // Prune with 7-day retention
        let stats = state.prune_old_entries(7);

        // Recent entry should be retained
        assert!(state.processed_files.contains(&recent_path));

        // Old entry should be pruned
        assert!(!state.processed_files.contains(&old_path));

        // Unparseable entry should be retained (safe default)
        assert!(state.processed_files.contains(&unparseable_path));

        // Old failed entry should be pruned
        assert!(state.permanently_failed.is_empty());

        // Stats should be accurate
        assert_eq!(stats.processed_pruned, 1);
        assert_eq!(stats.failed_pruned, 1);
    }

    #[test]
    fn test_prune_disabled_when_zero() {
        let mut state = ProcessedState::default();
        let old_date = Utc::now().date_naive() - Duration::days(100);
        let old_path = format!("region=us/{}/file.ndjson", old_date.format("%Y-%m-%d"));

        state.processed_files.insert(old_path.clone());
        state.permanently_failed.insert(old_path.clone());

        // Prune with 0 retention (disabled)
        let stats = state.prune_old_entries(0);

        // Nothing should be pruned
        assert!(state.processed_files.contains(&old_path));
        assert!(state.permanently_failed.contains(&old_path));
        assert_eq!(stats.processed_pruned, 0);
        assert_eq!(stats.failed_pruned, 0);
    }

    #[test]
    fn test_backward_compatibility_with_pruning() {
        // Old state format should still work after pruning is added
        let old_json = r#"{"processed_files":["region=us/2025-01-15/file.json"]}"#;
        let mut state: ProcessedState = serde_json::from_str(old_json).unwrap();

        // Pruning should work on deserialized state
        let _ = state.prune_old_entries(7);

        // State should still be serializable
        let json = serde_json::to_string(&state).unwrap();
        let _: ProcessedState = serde_json::from_str(&json).unwrap();
    }
}
