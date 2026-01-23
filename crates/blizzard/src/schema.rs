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

/// Infer an Arrow schema from a JSON sample
pub fn infer_schema_from_json_sample(sample: &[u8]) -> Result<ArrowSchema> {
    let cursor = Cursor::new(sample);
    let (schema, _) =
        infer_json_schema_from_seekable(cursor, None).context(SchemaInferenceSnafu)?;
    Ok(schema)
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
