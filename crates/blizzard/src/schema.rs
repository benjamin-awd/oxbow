//! Schema utilities for JSON file loading

use crate::error::{Result, SchemaInferenceSnafu};
use deltalake::arrow::datatypes::{Field, Schema as ArrowSchema};
use deltalake::arrow::json::reader::infer_json_schema_from_seekable;
use deltalake::kernel::schema::{DataType, StructField};
use snafu::ResultExt;
use std::io::Cursor;
use std::sync::Arc;

/// Column name for tracking source file origin
pub const SOURCE_FILE_COLUMN: &str = "_source_file";

/// Creates a StructField for the source file column
pub fn source_file_field() -> StructField {
    StructField::new(SOURCE_FILE_COLUMN.to_string(), DataType::STRING, false)
}

/// Infer an Arrow schema from a JSON sample.
///
/// All inferred fields are made nullable since JSON data may have missing fields
/// or nulls that weren't present in the sample used for inference.
pub fn infer_schema_from_json_sample(sample: &[u8]) -> Result<ArrowSchema> {
    let cursor = Cursor::new(sample);
    let (schema, _) =
        infer_json_schema_from_seekable(cursor, None).context(SchemaInferenceSnafu)?;

    // Make all fields nullable to handle missing/null values in subsequent records
    let nullable_fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| Arc::new(make_field_nullable(f)))
        .collect();

    Ok(ArrowSchema::new(nullable_fields))
}

/// Recursively make a field and all its nested fields nullable
fn make_field_nullable(field: &Field) -> Field {
    use deltalake::arrow::datatypes::DataType as ArrowDataType;

    let new_data_type = match field.data_type() {
        ArrowDataType::Struct(fields) => {
            let nullable_fields: Vec<Arc<Field>> = fields
                .iter()
                .map(|f| Arc::new(make_field_nullable(f)))
                .collect();
            ArrowDataType::Struct(nullable_fields.into())
        }
        ArrowDataType::List(inner) => ArrowDataType::List(Arc::new(make_field_nullable(inner))),
        ArrowDataType::LargeList(inner) => {
            ArrowDataType::LargeList(Arc::new(make_field_nullable(inner)))
        }
        ArrowDataType::Map(inner, sorted) => {
            ArrowDataType::Map(Arc::new(make_field_nullable(inner)), *sorted)
        }
        other => other.clone(),
    };

    Field::new(field.name(), new_data_type, true)
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

/// Remove the `_source_file` column from a schema.
/// Used to get the schema for parsing source JSON files, which shouldn't include
/// the metadata column we add after parsing.
pub fn without_source_file_column(schema: &ArrowSchema) -> ArrowSchema {
    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .filter(|f| f.name() != SOURCE_FILE_COLUMN)
        .cloned()
        .collect();
    ArrowSchema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
