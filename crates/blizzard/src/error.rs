use serde::{Deserialize, Serialize};
use snafu::Snafu;

/// Object store related errors
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ObjectStoreError {
    #[snafu(display("Object not found: {path}"))]
    NotFound {
        path: String,
        source: deltalake::ObjectStoreError,
    },

    #[snafu(display("Permission denied: {path}"))]
    PermissionDenied {
        path: String,
        source: deltalake::ObjectStoreError,
    },

    #[snafu(display("Object store error: {path}"))]
    Other {
        path: String,
        source: deltalake::ObjectStoreError,
    },
}

impl ObjectStoreError {
    pub fn from_source(err: deltalake::ObjectStoreError, path: &str) -> Self {
        match &err {
            deltalake::ObjectStoreError::NotFound { .. } => Self::NotFound {
                path: path.to_string(),
                source: err,
            },
            deltalake::ObjectStoreError::PermissionDenied { .. } => Self::PermissionDenied {
                path: path.to_string(),
                source: err,
            },
            _ => Self::Other {
                path: path.to_string(),
                source: err,
            },
        }
    }
}

/// JSON parsing and schema inference errors
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ParsingError {
    #[snafu(display("JSON decoder stuck: {preview}"))]
    DecoderStuck { preview: String },

    #[snafu(display("Schema inference failed"))]
    SchemaInference {
        source: deltalake::arrow::error::ArrowError,
    },
}

/// Configuration and serialization errors
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ConfigError {
    #[snafu(display("Invalid URL: {url}"))]
    UrlParse {
        url: String,
        source: url::ParseError,
    },

    #[snafu(display("State serialization error"))]
    StateSerialization { source: serde_json::Error },
}

/// Top-level error type for the crate
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("{source}"), context(false))]
    ObjectStore { source: ObjectStoreError },

    #[snafu(display("{source}"), context(false))]
    Parsing { source: ParsingError },

    #[snafu(display("{source}"), context(false))]
    Config { source: ConfigError },

    #[snafu(display("Delta table error: {source}"))]
    DeltaTable { source: deltalake::DeltaTableError },

    #[snafu(display("I/O error: {source}"))]
    Io { source: std::io::Error },

    #[snafu(display("Operation timed out after {duration_secs}s"))]
    Timeout { duration_secs: u64 },

    #[snafu(display("Arrow error: {source}"))]
    Arrow {
        source: deltalake::arrow::error::ArrowError,
    },

    #[snafu(display("DataFusion error: {source}"))]
    DataFusion {
        source: deltalake::datafusion::error::DataFusionError,
    },
}

impl Error {
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::ObjectStore {
                source: ObjectStoreError::Other { .. }
            } | Self::Timeout { .. }
                | Self::Io { .. }
        )
    }

    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::ObjectStore {
                source: ObjectStoreError::PermissionDenied { .. }
            } | Self::Parsing {
                source: ParsingError::DecoderStuck { .. }
            } | Self::Arrow { .. }
                | Self::DataFusion { .. }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCategory {
    ObjectStore,
    Parsing,
    DeltaTable,
    Io,
    Configuration,
    Timeout,
    Arrow,
    DataFusion,
}

impl From<&Error> for ErrorCategory {
    fn from(err: &Error) -> Self {
        match err {
            Error::ObjectStore { .. } => Self::ObjectStore,
            Error::Parsing { .. } => Self::Parsing,
            Error::DeltaTable { .. } => Self::DeltaTable,
            Error::Io { .. } => Self::Io,
            Error::Config { .. } => Self::Configuration,
            Error::Timeout { .. } => Self::Timeout,
            Error::Arrow { .. } => Self::Arrow,
            Error::DataFusion { .. } => Self::DataFusion,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
