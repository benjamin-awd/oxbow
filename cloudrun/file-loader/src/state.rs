use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tracing::log::*;
use url::Url;

/// State file tracking which files have been processed
/// Uses serde(default) for backward compatibility with old state files
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProcessedState {
    #[serde(default)]
    pub processed_files: HashSet<String>,
}

/// Load processed state from GCS
pub async fn load_state(state_file_uri: &str) -> Result<ProcessedState, anyhow::Error> {
    use deltalake::logstore::{logstore_for, StorageConfig};

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
    use deltalake::logstore::{logstore_for, StorageConfig};

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
}
