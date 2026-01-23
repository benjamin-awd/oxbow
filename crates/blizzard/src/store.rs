//! Object store utilities for consistent store setup and error handling.

use crate::error::{DeltaTableSnafu, ObjectStoreError, Result, UrlParseSnafu};
use deltalake::ObjectStore;
use deltalake::logstore::{StorageConfig, logstore_for};
use snafu::ResultExt;
use std::sync::Arc;
use url::Url;

/// Creates an object store from a URI string.
///
/// This is the canonical way to create an object store in blizzard,
/// ensuring consistent URL parsing and error handling.
pub async fn object_store_for_uri(uri: &str) -> Result<(Arc<dyn ObjectStore>, deltalake::Path)> {
    let url = Url::parse(uri).context(UrlParseSnafu { url: uri })?;
    // Create store from just the bucket root to avoid path duplication
    let root_url = Url::parse(&format!(
        "{}://{}/",
        url.scheme(),
        url.host_str().unwrap_or("")
    ))
    .context(UrlParseSnafu { url: uri })?;
    let store = logstore_for(&root_url, StorageConfig::default()).context(DeltaTableSnafu)?;
    let path = deltalake::Path::from(url.path());
    Ok((store.object_store(None), path))
}

/// Extension trait for adding path context to ObjectStore errors.
pub trait ObjectStoreResultExt<T> {
    /// Converts an ObjectStoreError to our error type with path context.
    fn with_path_context(self, path: &str) -> Result<T>;
}

impl<T> ObjectStoreResultExt<T> for std::result::Result<T, deltalake::ObjectStoreError> {
    fn with_path_context(self, path: &str) -> Result<T> {
        self.map_err(|e| ObjectStoreError::from_source(e, path).into())
    }
}
