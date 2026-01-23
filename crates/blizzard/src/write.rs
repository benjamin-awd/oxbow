//! Delta table write and commit operations.

use crate::error::{DeltaTableSnafu, Result};
use crate::schema::create_metadata_action;

use deltalake::kernel::schema::StructField;
use deltalake::kernel::transaction::CommitBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::{DeltaWriter, record_batch::RecordBatchWriter};
use snafu::ResultExt;

/// Commit a writer that already has data written to it (no schema evolution).
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

/// Commit a writer with schema evolution - flush and commit with metadata action.
pub async fn commit_writer_with_schema_evolution(
    mut table: deltalake::DeltaTable,
    mut writer: RecordBatchWriter,
    new_fields: &[StructField],
) -> Result<deltalake::DeltaTable> {
    let metadata_action = create_metadata_action(&table, new_fields)?;

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
                mode: SaveMode::Append,
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
