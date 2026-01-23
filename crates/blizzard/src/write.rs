//! Delta table write and commit operations.

use crate::error::{DeltaTableSnafu, Result};

use deltalake::writer::{DeltaWriter, record_batch::RecordBatchWriter};
use snafu::ResultExt;

/// Commit a writer that already has data written to it.
pub async fn commit_writer(
    mut table: deltalake::DeltaTable,
    mut writer: RecordBatchWriter,
) -> Result<deltalake::DeltaTable> {
    writer
        .flush_and_commit(&mut table)
        .await
        .context(DeltaTableSnafu)?;
    Ok(table)
}
