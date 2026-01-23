//! Schema evolution support for JSON file loading
//!
//! This module provides functionality to detect new fields in JSON files
//! and evolve the Delta table schema to accommodate them.

use crate::error::{DeltaTableSnafu, Result, SchemaInferenceSnafu};
use deltalake::DeltaTable;
use deltalake::arrow::datatypes::{Field, Schema as ArrowSchema};
use deltalake::arrow::json::reader::infer_json_schema_from_seekable;
use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::kernel::models;
use deltalake::kernel::schema::{DataType, Schema, StructField};
use deltalake::kernel::{Action, MetadataExt};
use snafu::ResultExt;
use std::io::Cursor;
use std::sync::Arc;
use tracing::log::*;

/// Column name for tracking source file origin
pub const SOURCE_FILE_COLUMN: &str = "_source_file";

/// Creates a StructField for the source file column
pub fn source_file_field() -> StructField {
    StructField::new(SOURCE_FILE_COLUMN.to_string(), DataType::STRING, false)
}

/// Infer an Arrow schema from a JSON sample
pub fn infer_schema_from_json_sample(sample: &[u8]) -> Result<ArrowSchema> {
    let cursor = Cursor::new(sample);
    let (schema, _) =
        infer_json_schema_from_seekable(cursor, None).context(SchemaInferenceSnafu)?;
    Ok(schema)
}

/// Detect new fields that need to be added to the table schema.
/// Returns `Some(fields)` if evolution is needed, `None` otherwise.
pub fn detect_new_fields(
    table_schema: &Schema,
    file_schema: &ArrowSchema,
) -> Option<Vec<StructField>> {
    let new_fields: Vec<StructField> = file_schema
        .fields()
        .iter()
        .filter(|field| table_schema.index_of(field.name()).is_none())
        .filter_map(|field| {
            let name = field.name();
            let coerced = oxbow::coerce_field(field.clone());
            match DataType::try_from_arrow(coerced.data_type()) {
                Ok(delta_type) => {
                    debug!("Found new field for schema evolution: {name}");
                    Some(StructField::new(name.to_string(), delta_type, true))
                }
                Err(_) => {
                    warn!("Could not convert field {name} to Delta type, skipping");
                    None
                }
            }
        })
        .collect();

    (!new_fields.is_empty()).then_some(new_fields)
}

/// Create a merged Arrow schema combining table schema with new fields from file schema.
/// New fields are made nullable for backwards compatibility.
pub fn create_merged_arrow_schema(
    table_schema: &ArrowSchema,
    file_schema: &ArrowSchema,
) -> ArrowSchema {
    let mut fields: Vec<Arc<Field>> = table_schema.fields().iter().cloned().collect();

    for field in file_schema.fields() {
        let name = field.name();
        if table_schema.field_with_name(name).is_err() {
            let coerced = oxbow::coerce_field(field.clone());
            let nullable_field = Arc::new(Field::new(
                coerced.name(),
                coerced.data_type().clone(),
                true,
            ));
            fields.push(nullable_field);
        }
    }

    ArrowSchema::new_with_metadata(fields, table_schema.metadata.clone())
}

/// Create a metadata action for schema evolution with the given new fields.
pub fn create_metadata_action(table: &DeltaTable, new_fields: &[StructField]) -> Result<Action> {
    let snapshot = table.snapshot().context(DeltaTableSnafu)?;
    let table_schema = snapshot.schema();
    let table_metadata = snapshot.metadata();

    let mut all_fields: Vec<StructField> = table_schema.fields().cloned().collect();
    all_fields.extend(new_fields.iter().cloned());

    let new_schema = Schema::try_new(all_fields)
        .map_err(deltalake::DeltaTableError::from)
        .context(DeltaTableSnafu)?;

    let mut action = models::new_metadata(
        &new_schema,
        table_metadata.partition_columns().clone(),
        table_metadata.configuration().clone(),
    )
    .map_err(deltalake::DeltaTableError::from)
    .context(DeltaTableSnafu)?;

    if let Some(name) = table_metadata.name() {
        action = action
            .with_name(name.into())
            .map_err(deltalake::DeltaTableError::from)
            .context(DeltaTableSnafu)?;
    }
    if let Some(description) = table_metadata.description() {
        action = action
            .with_description(description.into())
            .map_err(deltalake::DeltaTableError::from)
            .context(DeltaTableSnafu)?;
    }

    info!(
        "Created schema evolution metadata action with {} new fields",
        new_fields.len()
    );

    Ok(Action::Metadata(action))
}

/// Build an Arrow schema with the `_source_file` column appended.
/// This is the schema used for writing batches after augmenting with source file info.
pub fn with_source_file_column(schema: &ArrowSchema) -> ArrowSchema {
    use deltalake::arrow::datatypes::DataType as ArrowDataType;

    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(
        SOURCE_FILE_COLUMN,
        ArrowDataType::Utf8,
        false,
    )));
    ArrowSchema::new(fields)
}

/// High-level function to handle schema evolution.
/// Returns the new fields and merged Arrow schema if evolution is needed.
pub fn evolve_schema(
    table: &DeltaTable,
    current_arrow_schema: &ArrowSchema,
    file_schema: &ArrowSchema,
) -> Result<Option<(Vec<StructField>, Arc<ArrowSchema>)>> {
    let table_schema = table.snapshot().context(DeltaTableSnafu)?.schema();

    if let Some(new_fields) = detect_new_fields(&table_schema, file_schema) {
        let merged = create_merged_arrow_schema(current_arrow_schema, file_schema);
        info!("Schema evolution detected: {} new fields", new_fields.len());
        Ok(Some((new_fields, Arc::new(merged))))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::kernel::DataType;

    fn sample_json() -> &'static [u8] {
        br#"{"id": 1, "name": "test", "value": 42.5}
{"id": 2, "name": "another", "value": 10.0}
"#
    }

    #[test]
    fn test_infer_schema_from_json_sample() {
        let schema = infer_schema_from_json_sample(sample_json()).unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("value").is_ok());
    }

    #[test]
    fn test_detect_new_fields_with_new_field() {
        let table_schema = Schema::try_new(vec![StructField::new(
            "id".to_string(),
            DataType::LONG,
            true,
        )])
        .unwrap();

        let file_schema = infer_schema_from_json_sample(sample_json()).unwrap();

        let new_fields = detect_new_fields(&table_schema, &file_schema);
        assert!(new_fields.is_some());
        let new_fields = new_fields.unwrap();
        assert_eq!(new_fields.len(), 2);
        let field_names: Vec<&str> = new_fields.iter().map(|f| f.name.as_str()).collect();
        assert!(field_names.contains(&"name"));
        assert!(field_names.contains(&"value"));
    }

    #[test]
    fn test_detect_new_fields_no_new_fields() {
        let table_schema = Schema::try_new(vec![
            StructField::new("id".to_string(), DataType::LONG, true),
            StructField::new("name".to_string(), DataType::STRING, true),
            StructField::new("value".to_string(), DataType::DOUBLE, true),
        ])
        .unwrap();

        let file_schema = infer_schema_from_json_sample(sample_json()).unwrap();

        let new_fields = detect_new_fields(&table_schema, &file_schema);
        assert!(new_fields.is_none());
    }

    #[test]
    fn test_create_merged_arrow_schema() {
        use deltalake::arrow::datatypes::DataType as ArrowDataType;

        let table_schema = ArrowSchema::new(vec![Field::new("id", ArrowDataType::Int64, true)]);

        let file_schema = infer_schema_from_json_sample(sample_json()).unwrap();

        let merged = create_merged_arrow_schema(&table_schema, &file_schema);

        assert_eq!(merged.fields().len(), 3);
        assert!(merged.field_with_name("id").is_ok());
        assert!(merged.field_with_name("name").is_ok());
        assert!(merged.field_with_name("value").is_ok());
    }
}
