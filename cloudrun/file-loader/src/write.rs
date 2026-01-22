use crate::state::{save_state, ProcessedState};
use deltalake::arrow::array::RecordBatch;
use deltalake::parquet::arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter};
use deltalake::parquet::basic::Compression;
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::{ObjectMeta, ObjectStore};
use std::sync::Arc;
use uuid::Uuid;

/// Rolling policy configuration
pub const COMMIT_RECORD_THRESHOLD: usize = 500_000; // Commit every 500k records

/// Buffering writer that accumulates batches and commits based on rolling policy
pub struct BatchBufferingWriter {
    writer: AsyncArrowWriter<ParquetObjectWriter>,
    path: deltalake::Path,
    records_written: usize,
}

impl BatchBufferingWriter {
    pub async fn new(
        object_store: Arc<dyn ObjectStore>,
        schema: Arc<deltalake::arrow::datatypes::Schema>,
    ) -> Result<Self, anyhow::Error> {
        let uuid = Uuid::new_v4();
        let filename = format!("part-{}.snappy.parquet", uuid);
        let path = deltalake::Path::from(filename);

        let sink = ParquetObjectWriter::new(object_store, path.clone());
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let writer = AsyncArrowWriter::try_new(sink, schema, Some(props))?;

        Ok(Self {
            writer,
            path,
            records_written: 0,
        })
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<(), anyhow::Error> {
        self.records_written += batch.num_rows();
        self.writer.write(batch).await?;
        Ok(())
    }

    pub async fn close(self, object_store: &Arc<dyn ObjectStore>) -> Result<ObjectMeta, anyhow::Error> {
        self.writer.close().await?;
        let meta = object_store.head(&self.path).await?;
        Ok(ObjectMeta {
            location: self.path,
            last_modified: meta.last_modified,
            size: meta.size,
            e_tag: meta.e_tag,
            version: meta.version,
        })
    }

    pub fn records(&self) -> usize {
        self.records_written
    }
}

/// Commit a parquet file to Delta and save state
pub async fn commit_to_delta(
    file_meta: ObjectMeta,
    table: &mut deltalake::DeltaTable,
    state: &mut ProcessedState,
    pending_paths: &mut Vec<String>,
    state_file_uri: &str,
) -> Result<i64, anyhow::Error> {
    let actions = oxbow::add_actions_for(&[file_meta]);
    let version = oxbow::commit_to_table(&actions, table).await?;

    // Reload table state
    table.load().await?;

    // Mark files as processed and save state
    for path in pending_paths.drain(..) {
        state.processed_files.insert(path);
    }
    save_state(state_file_uri, state).await?;

    Ok(version)
}
