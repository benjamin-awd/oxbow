//! Schema evolution support for JSON file loading
//!
//! This module provides functionality to detect new fields in JSON files
//! and evolve the Delta table schema to accommodate them.

use deltalake::DeltaTable;
use deltalake::arrow::datatypes::{Field, Schema as ArrowSchema};
use deltalake::arrow::json::reader::infer_json_schema_from_seekable;
use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::kernel::models;
use deltalake::kernel::schema::{Schema, StructField};
use deltalake::kernel::{Action, MetadataExt};
use std::io::Cursor;
use std::sync::Arc;
use tracing::log::*;

/// Infer Arrow schema from a JSON sample buffer
pub fn infer_schema_from_json_sample(sample: &[u8]) -> Result<ArrowSchema, anyhow::Error> {
    let cursor = Cursor::new(sample);
    let (schema, _) = infer_json_schema_from_seekable(cursor, None)?;
    Ok(schema)
}

/// Check if file schema contains fields not in table schema
pub fn needs_evolution(table_schema: &Schema, file_schema: &ArrowSchema) -> bool {
    for field in file_schema.fields() {
        if table_schema.index_of(field.name()).is_none() {
            return true;
        }
    }
    false
}

/// Find new fields in file schema that don't exist in table schema
pub fn merge_schemas(table_schema: &Schema, file_schema: &ArrowSchema) -> Vec<StructField> {
    let mut new_fields = Vec::new();

    for field in file_schema.fields() {
        let name = field.name();
        if table_schema.index_of(name).is_none() {
            debug!("Found new field for schema evolution: {name}");
            let coerced = oxbow::coerce_field(field.clone());
            if let Ok(delta_type) = deltalake::kernel::DataType::try_from_arrow(coerced.data_type())
            {
                new_fields.push(StructField::new(name.to_string(), delta_type, true));
            } else {
                warn!("Could not convert field {name} to Delta type, skipping");
            }
        }
    }

    new_fields
}

/// Create a merged Arrow schema that includes both table and new file fields
pub fn create_merged_arrow_schema(
    table_schema: &ArrowSchema,
    file_schema: &ArrowSchema,
) -> ArrowSchema {
    let mut fields: Vec<Arc<Field>> = table_schema.fields().iter().cloned().collect();

    for field in file_schema.fields() {
        let name = field.name();
        if table_schema.field_with_name(name).is_err() {
            let coerced = oxbow::coerce_field(field.clone());
            // Make new fields nullable for backwards compatibility
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

/// Create a Delta Metadata action for schema evolution
pub fn create_metadata_action(
    table: &DeltaTable,
    new_fields: &[StructField],
) -> Result<Action, anyhow::Error> {
    let snapshot = table.snapshot()?;
    let table_schema = snapshot.schema();
    let table_metadata = snapshot.metadata();

    // Build new schema with existing + new fields
    let mut all_fields: Vec<StructField> = table_schema.fields().cloned().collect();
    all_fields.extend(new_fields.iter().cloned());

    let new_schema = Schema::try_new(all_fields)?;

    let mut action = models::new_metadata(
        &new_schema,
        table_metadata.partition_columns().clone(),
        table_metadata.configuration().clone(),
    )?;

    if let Some(name) = table_metadata.name() {
        action = action.with_name(name.into())?;
    }
    if let Some(description) = table_metadata.description() {
        action = action.with_description(description.into())?;
    }

    info!(
        "Created schema evolution metadata action with {} new fields",
        new_fields.len()
    );

    Ok(Action::Metadata(action))
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
    fn test_needs_evolution_with_new_field() {
        let table_schema = Schema::try_new(vec![StructField::new(
            "id".to_string(),
            DataType::LONG,
            true,
        )])
        .unwrap();

        let file_schema = infer_schema_from_json_sample(sample_json()).unwrap();

        assert!(needs_evolution(&table_schema, &file_schema));
    }

    #[test]
    fn test_needs_evolution_no_new_fields() {
        let table_schema = Schema::try_new(vec![
            StructField::new("id".to_string(), DataType::LONG, true),
            StructField::new("name".to_string(), DataType::STRING, true),
            StructField::new("value".to_string(), DataType::DOUBLE, true),
        ])
        .unwrap();

        let file_schema = infer_schema_from_json_sample(sample_json()).unwrap();

        assert!(!needs_evolution(&table_schema, &file_schema));
    }

    #[test]
    fn test_merge_schemas() {
        let table_schema = Schema::try_new(vec![StructField::new(
            "id".to_string(),
            DataType::LONG,
            true,
        )])
        .unwrap();

        let file_schema = infer_schema_from_json_sample(sample_json()).unwrap();

        let new_fields = merge_schemas(&table_schema, &file_schema);

        assert_eq!(new_fields.len(), 2);
        let field_names: Vec<&str> = new_fields.iter().map(|f| f.name.as_str()).collect();
        assert!(field_names.contains(&"name"));
        assert!(field_names.contains(&"value"));
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
