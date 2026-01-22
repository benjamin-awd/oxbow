//! Integration tests for _source_file column atomic state tracking
//!
//! These tests verify that:
//! 1. The _source_file column is properly added to batches
//! 2. The query_processed_files function correctly retrieves processed files from Delta
//! 3. Files that are already in Delta are not reprocessed

use blizzard::read::augment_with_source_file;
use blizzard::schema::{SOURCE_FILE_COLUMN, source_file_field};
use blizzard::state::query_processed_files;
use deltalake::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::logstore::{StorageConfig, logstore_for};
use deltalake::operations::create::CreateBuilder;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use std::sync::Arc;
use tempfile::TempDir;
use url::Url;

/// Creates a simple test RecordBatch without the _source_file column
fn create_test_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true), // Match table schema - nullable
    ]);

    let id_array = Arc::new(Int64Array::from(ids.to_vec()));
    let name_array = Arc::new(StringArray::from(
        names.iter().map(|s| *s).collect::<Vec<_>>(),
    ));

    RecordBatch::try_new(Arc::new(schema), vec![id_array, name_array]).unwrap()
}

/// Helper to create a logstore from a temp directory
fn create_temp_logstore(temp_dir: &TempDir) -> deltalake::logstore::LogStoreRef {
    let path = temp_dir.path().to_str().unwrap();
    let url = Url::parse(&format!("file://{}", path)).unwrap();
    logstore_for(&url, StorageConfig::default()).unwrap()
}

#[test]
fn test_augment_with_source_file_adds_column() {
    let batch = create_test_batch(&[1, 2, 3], &["a", "b", "c"]);
    let source_file = "test/path/file1.json";

    let augmented = augment_with_source_file(batch, source_file).unwrap();

    // Verify the _source_file column was added
    assert_eq!(augmented.num_columns(), 3);
    assert!(
        augmented
            .schema()
            .field_with_name(SOURCE_FILE_COLUMN)
            .is_ok()
    );

    // Verify all rows have the correct source file value
    let source_col = augmented
        .column_by_name(SOURCE_FILE_COLUMN)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(source_col.len(), 3);
    for i in 0..3 {
        assert_eq!(source_col.value(i), source_file);
    }
}

#[test]
fn test_augment_preserves_original_data() {
    let batch = create_test_batch(&[10, 20], &["foo", "bar"]);
    let augmented = augment_with_source_file(batch, "source.json").unwrap();

    // Verify original columns are preserved
    let id_col = augmented
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id_col.value(0), 10);
    assert_eq!(id_col.value(1), 20);

    let name_col = augmented
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name_col.value(0), "foo");
    assert_eq!(name_col.value(1), "bar");
}

#[test]
fn test_source_file_field_creates_correct_struct_field() {
    let field = source_file_field();
    assert_eq!(field.name, SOURCE_FILE_COLUMN);
    assert_eq!(field.data_type, deltalake::kernel::DataType::STRING);
    assert!(!field.is_nullable());
}

#[tokio::test]
async fn test_query_processed_files_empty_table() {
    let temp_dir = TempDir::new().unwrap();
    let store = create_temp_logstore(&temp_dir);

    // Create a Delta table with the _source_file column but no data
    let table = CreateBuilder::new()
        .with_log_store(store)
        .with_column("id", deltalake::kernel::DataType::LONG, false, None)
        .with_column(
            SOURCE_FILE_COLUMN,
            deltalake::kernel::DataType::STRING,
            false,
            None,
        )
        .await
        .unwrap();

    let processed = query_processed_files(&table).await.unwrap();
    assert!(processed.is_empty());
}

#[tokio::test]
async fn test_query_processed_files_returns_distinct_sources() {
    let temp_dir = TempDir::new().unwrap();
    let store = create_temp_logstore(&temp_dir);

    // Create a Delta table with _source_file column
    let mut table = CreateBuilder::new()
        .with_log_store(store)
        .with_column("id", deltalake::kernel::DataType::LONG, false, None)
        .with_column("name", deltalake::kernel::DataType::STRING, true, None)
        .with_column(
            SOURCE_FILE_COLUMN,
            deltalake::kernel::DataType::STRING,
            false,
            None,
        )
        .await
        .unwrap();

    // Write some test data with different source files
    let batch1 = create_test_batch(&[1, 2], &["a", "b"]);
    let augmented1 = augment_with_source_file(batch1, "file1.json").unwrap();

    let batch2 = create_test_batch(&[3, 4], &["c", "d"]);
    let augmented2 = augment_with_source_file(batch2, "file2.json").unwrap();

    // Write another batch from file1 to test DISTINCT works
    let batch3 = create_test_batch(&[5], &["e"]);
    let augmented3 = augment_with_source_file(batch3, "file1.json").unwrap();

    let mut writer = RecordBatchWriter::for_table(&table).unwrap();
    writer.write(augmented1).await.unwrap();
    writer.write(augmented2).await.unwrap();
    writer.write(augmented3).await.unwrap();
    writer.flush_and_commit(&mut table).await.unwrap();

    // Query processed files
    let processed = query_processed_files(&table).await.unwrap();

    // Should have exactly 2 distinct source files
    assert_eq!(processed.len(), 2);
    assert!(processed.contains("file1.json"));
    assert!(processed.contains("file2.json"));
}

#[tokio::test]
async fn test_query_processed_files_table_without_column() {
    let temp_dir = TempDir::new().unwrap();
    let store = create_temp_logstore(&temp_dir);

    // Create a Delta table WITHOUT the _source_file column
    let table = CreateBuilder::new()
        .with_log_store(store)
        .with_column("id", deltalake::kernel::DataType::LONG, false, None)
        .await
        .unwrap();

    // Should return empty set when column doesn't exist
    let processed = query_processed_files(&table).await.unwrap();
    assert!(processed.is_empty());
}
