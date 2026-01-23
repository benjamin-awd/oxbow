//! Integration tests for Delta table creation from JSON files
//!
//! These tests verify that:
//! 1. Schema can be correctly inferred from JSON files
//! 2. Arrow schema is correctly converted to Delta schema
//! 3. Tables are created with the correct columns including _source_file

use blizzard::read::{infer_schema_from_file, is_gzip_compressed};
use blizzard::schema::{SOURCE_FILE_COLUMN, source_file_field};
use deltalake::Path;
use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::kernel::schema::{DataType, StructField};
use deltalake::logstore::{StorageConfig, logstore_for};
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use url::Url;

/// Helper to create a logstore and object store from a temp directory
fn create_temp_stores(
    temp_dir: &TempDir,
) -> (
    deltalake::logstore::LogStoreRef,
    Arc<dyn deltalake::ObjectStore>,
) {
    let path = temp_dir.path().to_str().unwrap();
    let url = Url::parse(&format!("file://{}", path)).unwrap();
    let store = logstore_for(&url, StorageConfig::default()).unwrap();
    let object_store = store.object_store(None);
    (store, object_store)
}

/// Write a JSON file to the object store
async fn write_json_file(
    object_store: &Arc<dyn deltalake::ObjectStore>,
    path: &str,
    content: &[u8],
) {
    use deltalake::ObjectStore;
    let location = Path::from(path);
    object_store
        .put(&location, content.to_vec().into())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_create_table_from_json_schema() {
    let temp_dir = TempDir::new().unwrap();
    let (log_store, object_store) = create_temp_stores(&temp_dir);

    // Write a sample JSON file
    let json_content = br#"{"id": 1, "name": "test", "value": 42.5, "active": true}
{"id": 2, "name": "another", "value": 10.0, "active": false}
"#;
    write_json_file(&object_store, "data/sample.json", json_content).await;

    // Sample schema from the JSON file (same as create_table_from_json does)
    let file_path = "data/sample.json";
    let is_compressed = is_gzip_compressed(file_path);
    let location = Path::from(file_path);

    let arrow_schema = infer_schema_from_file(&object_store, &location, is_compressed)
        .await
        .unwrap();

    // Convert Arrow fields to Delta StructFields (same logic as create_table_from_json)
    let mut columns: Vec<StructField> = Vec::new();
    for field in arrow_schema.fields() {
        let coerced = oxbow::coerce_field(field.clone());
        if let Ok(delta_type) = DataType::try_from_arrow(coerced.data_type()) {
            columns.push(StructField::new(field.name().to_string(), delta_type, true));
        }
    }

    // Add _source_file column
    columns.push(source_file_field());

    // Create the table
    let table = CreateBuilder::new()
        .with_log_store(log_store)
        .with_columns(columns)
        .with_save_mode(SaveMode::Ignore)
        .await
        .unwrap();

    // Verify the table schema
    let schema = table.snapshot().unwrap().schema();

    // Should have 5 columns: id, name, value, active, _source_file
    assert_eq!(schema.fields().len(), 5);

    // Verify each field exists
    assert!(schema.index_of("id").is_some());
    assert!(schema.index_of("name").is_some());
    assert!(schema.index_of("value").is_some());
    assert!(schema.index_of("active").is_some());
    assert!(schema.index_of(SOURCE_FILE_COLUMN).is_some());

    // Verify _source_file field properties
    let source_idx = schema.index_of(SOURCE_FILE_COLUMN).unwrap();
    let source_field = schema.fields().nth(source_idx).unwrap();
    assert_eq!(source_field.data_type, DataType::STRING);
    assert!(!source_field.is_nullable());
}

#[tokio::test]
async fn test_create_table_from_nested_json_schema() {
    let temp_dir = TempDir::new().unwrap();
    let (log_store, object_store) = create_temp_stores(&temp_dir);

    // Write a JSON file with nested structure
    let json_content = br#"{"user": {"id": 1, "name": "test"}, "timestamp": "2024-01-01T00:00:00Z"}
{"user": {"id": 2, "name": "another"}, "timestamp": "2024-01-02T00:00:00Z"}
"#;
    write_json_file(&object_store, "nested.json", json_content).await;

    let location = Path::from("nested.json");
    let arrow_schema = infer_schema_from_file(&object_store, &location, false)
        .await
        .unwrap();

    let mut columns: Vec<StructField> = Vec::new();
    for field in arrow_schema.fields() {
        let coerced = oxbow::coerce_field(field.clone());
        if let Ok(delta_type) = DataType::try_from_arrow(coerced.data_type()) {
            columns.push(StructField::new(field.name().to_string(), delta_type, true));
        }
    }
    columns.push(source_file_field());

    let table = CreateBuilder::new()
        .with_log_store(log_store)
        .with_columns(columns)
        .with_save_mode(SaveMode::Ignore)
        .await
        .unwrap();

    let schema = table.snapshot().unwrap().schema();

    // Should have user (struct), timestamp, and _source_file
    assert_eq!(schema.fields().len(), 3);
    assert!(schema.index_of("user").is_some());
    assert!(schema.index_of("timestamp").is_some());
    assert!(schema.index_of(SOURCE_FILE_COLUMN).is_some());

    // Verify the nested struct
    let user_idx = schema.index_of("user").unwrap();
    let user_field = schema.fields().nth(user_idx).unwrap();
    if let DataType::Struct(inner) = &user_field.data_type {
        assert!(inner.index_of("id").is_some());
        assert!(inner.index_of("name").is_some());
    } else {
        panic!("Expected user to be a struct type");
    }
}

#[tokio::test]
async fn test_create_table_from_gzipped_json() {
    use async_compression::tokio::write::GzipEncoder;

    let temp_dir = TempDir::new().unwrap();
    let (log_store, object_store) = create_temp_stores(&temp_dir);

    // Create gzipped JSON content
    let json_content = r#"{"id": 1, "message": "hello"}
{"id": 2, "message": "world"}
"#;

    let mut encoder = GzipEncoder::new(Vec::new());
    encoder.write_all(json_content.as_bytes()).await.unwrap();
    encoder.shutdown().await.unwrap();
    let compressed = encoder.into_inner();

    // Write compressed file
    use deltalake::ObjectStore;
    let location = Path::from("data.json.gz");
    object_store
        .put(&location, compressed.into())
        .await
        .unwrap();

    // Verify is_gzip_compressed works
    assert!(is_gzip_compressed("data.json.gz"));

    let arrow_schema = infer_schema_from_file(&object_store, &location, true)
        .await
        .unwrap();

    let mut columns: Vec<StructField> = Vec::new();
    for field in arrow_schema.fields() {
        let coerced = oxbow::coerce_field(field.clone());
        if let Ok(delta_type) = DataType::try_from_arrow(coerced.data_type()) {
            columns.push(StructField::new(field.name().to_string(), delta_type, true));
        }
    }
    columns.push(source_file_field());

    let table = CreateBuilder::new()
        .with_log_store(log_store)
        .with_columns(columns)
        .with_save_mode(SaveMode::Ignore)
        .await
        .unwrap();

    let schema = table.snapshot().unwrap().schema();

    // Should have id, message, and _source_file
    assert_eq!(schema.fields().len(), 3);
    assert!(schema.index_of("id").is_some());
    assert!(schema.index_of("message").is_some());
    assert!(schema.index_of(SOURCE_FILE_COLUMN).is_some());
}

#[tokio::test]
async fn test_source_file_column_is_not_nullable() {
    // Verify that source_file_field creates a non-nullable field
    // This is important for data integrity - every record must have a source
    let field = source_file_field();
    assert!(!field.is_nullable());
    assert_eq!(field.name, SOURCE_FILE_COLUMN);
    assert_eq!(field.data_type, DataType::STRING);
}

#[tokio::test]
async fn test_create_table_with_name_stores_in_metadata() {
    let temp_dir = TempDir::new().unwrap();
    let (log_store, _object_store) = create_temp_stores(&temp_dir);

    let table_name = "my_test_table";

    let table = CreateBuilder::new()
        .with_log_store(log_store)
        .with_table_name(table_name)
        .with_column("id", DataType::LONG, false, None)
        .with_column(SOURCE_FILE_COLUMN, DataType::STRING, false, None)
        .with_save_mode(SaveMode::Ignore)
        .await
        .unwrap();

    // Verify the table name is stored in metadata
    let snapshot = table.snapshot().unwrap();
    let metadata = snapshot.metadata();
    assert_eq!(metadata.name(), Some(table_name));
}

#[tokio::test]
async fn test_existing_table_preserves_name_on_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_str().unwrap();
    let table_url = Url::parse(&format!("file://{}", path)).unwrap();

    // Create a table with a specific name
    let original_name = "original_table_name";
    {
        let log_store = logstore_for(&table_url, StorageConfig::default()).unwrap();

        let _table = CreateBuilder::new()
            .with_log_store(log_store)
            .with_table_name(original_name)
            .with_column("id", DataType::LONG, false, None)
            .with_save_mode(SaveMode::Ignore)
            .await
            .unwrap();
    }

    // Reopen the table (simulating what happens when table already exists)
    let reopened = deltalake::open_table(table_url.clone()).await.unwrap();

    // Verify the original name is preserved
    let snapshot = reopened.snapshot().unwrap();
    let metadata = snapshot.metadata();
    assert_eq!(metadata.name(), Some(original_name));
}

#[tokio::test]
async fn test_open_existing_table_does_not_error() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_str().unwrap();
    let table_url = Url::parse(&format!("file://{}", path)).unwrap();

    // Create a table first
    {
        let log_store = logstore_for(&table_url, StorageConfig::default()).unwrap();

        let _table = CreateBuilder::new()
            .with_log_store(log_store)
            .with_column("id", DataType::LONG, false, None)
            .with_save_mode(SaveMode::Ignore)
            .await
            .unwrap();
    }

    // Opening an existing table should succeed
    let result = deltalake::open_table(table_url).await;
    assert!(result.is_ok());

    // Verify we can read the schema
    let table = result.unwrap();
    let schema = table.snapshot().unwrap().schema();
    assert!(schema.index_of("id").is_some());
}
