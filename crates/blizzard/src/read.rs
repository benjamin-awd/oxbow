use async_compression::tokio::bufread::GzipDecoder;
use deltalake::ObjectStore;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use deltalake::arrow::json::reader::{Decoder, ReaderBuilder};
use futures::TryStreamExt;
use snafu::ResultExt;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;

use crate::error::{IoSnafu, ObjectStoreError, ParsingError, Result, SchemaInferenceSnafu};
use crate::schema::infer_schema_from_json_sample;

pub async fn stream_and_parse_file(
    store: &Arc<dyn ObjectStore>,
    path: &deltalake::Path,
    arrow_schema: Arc<deltalake::arrow::datatypes::Schema>,
    is_compressed: bool,
    batch_size: usize,
) -> Result<(Vec<RecordBatch>, usize)> {
    let get_result = store
        .get(path)
        .await
        .map_err(|e| ObjectStoreError::from_source(e, path.as_ref()))?;

    let byte_stream = get_result
        .into_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let stream_reader = StreamReader::new(byte_stream);

    let mut decoder = ReaderBuilder::new(arrow_schema)
        .with_batch_size(batch_size)
        .build_decoder()
        .context(SchemaInferenceSnafu)?;

    let batches = if is_compressed {
        let buf_reader = BufReader::new(GzipDecoder::new(BufReader::new(stream_reader)));
        deserialize_stream(buf_reader, &mut decoder).await?
    } else {
        let buf_reader = BufReader::new(stream_reader);
        deserialize_stream(buf_reader, &mut decoder).await?
    };

    let total_records: usize = batches.iter().map(|b| b.num_rows()).sum();
    Ok((batches, total_records))
}

/// Sample schema from a JSON file by reading and inferring from the first N bytes
///
/// This avoids reading the entire file when we only need to detect schema changes.
/// For compressed files, decompresses the sampled bytes before inference.
pub async fn sample_schema_from_file(
    store: &Arc<dyn ObjectStore>,
    path: &deltalake::Path,
    is_compressed: bool,
    sample_bytes: usize,
) -> Result<ArrowSchema> {
    let get_result = store
        .get(path)
        .await
        .map_err(|e| ObjectStoreError::from_source(e, path.as_ref()))?;
    let byte_stream = get_result
        .into_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let stream_reader = StreamReader::new(byte_stream);

    let sample = if is_compressed {
        let mut buf_reader = BufReader::new(GzipDecoder::new(BufReader::new(stream_reader)));
        let mut decompressed = vec![0u8; sample_bytes];
        let bytes_read = buf_reader.read(&mut decompressed).await.context(IoSnafu)?;
        decompressed.truncate(bytes_read);
        decompressed
    } else {
        let mut buf_reader = BufReader::new(stream_reader);
        let mut buffer = vec![0u8; sample_bytes];
        let bytes_read = buf_reader.read(&mut buffer).await.context(IoSnafu)?;
        buffer.truncate(bytes_read);
        buffer
    };

    infer_schema_from_json_sample(&sample)
}

async fn deserialize_stream(
    mut reader: impl tokio::io::AsyncBufRead + Unpin,
    decoder: &mut Decoder,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();

    loop {
        let buf = reader.fill_buf().await.context(IoSnafu)?;

        if buf.is_empty() {
            // End of stream - flush any remaining data
            if let Some(batch) = decoder.flush().context(SchemaInferenceSnafu)? {
                if batch.num_rows() > 0 {
                    batches.push(batch);
                }
            }
            break;
        }

        let mut have_read = decoder.decode(buf).context(SchemaInferenceSnafu)?;

        // If decoder couldn't make progress, flush to free internal buffer space
        if have_read == 0 && !buf.is_empty() {
            if let Some(batch) = decoder.flush().context(SchemaInferenceSnafu)? {
                batches.push(batch);
            }
            // Try decode again after flush
            have_read = decoder.decode(buf).context(SchemaInferenceSnafu)?;
            if have_read == 0 {
                // Fail fast rather than silently dropping bytes which causes data loss
                let preview_len = buf.len().min(100);
                let preview = String::from_utf8_lossy(&buf[..preview_len]);
                return Err(ParsingError::DecoderStuck {
                    preview: format!(
                        "unable to parse data after flush. Data preview (first {} bytes): {:?}",
                        preview_len, preview
                    ),
                }
                .into());
            }
        }

        reader.consume(have_read);

        // Flush when decoder has complete records
        if !decoder.has_partial_record() {
            if let Some(batch) = decoder.flush().context(SchemaInferenceSnafu)? {
                batches.push(batch);
            }
        }
    }

    Ok(batches)
}

fn ends_with_ignore_case(s: &str, suffix: &str) -> bool {
    s.len() >= suffix.len() && s[s.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
}

pub fn is_supported_json_file(name: &str) -> bool {
    const EXTENSIONS: &[&str] = &[
        ".json",
        ".jsonl",
        ".ndjson",
        ".json.gz",
        ".jsonl.gz",
        ".ndjson.gz",
    ];
    EXTENSIONS
        .iter()
        .any(|ext| ends_with_ignore_case(name, ext))
}

pub fn is_gzip_compressed(name: &str) -> bool {
    ends_with_ignore_case(name, ".gz")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_supported_json_file() {
        // Supported formats
        assert!(is_supported_json_file("data.json"));
        assert!(is_supported_json_file("data.jsonl"));
        assert!(is_supported_json_file("data.ndjson"));
        assert!(is_supported_json_file("data.json.gz"));
        assert!(is_supported_json_file("data.jsonl.gz"));
        assert!(is_supported_json_file("data.ndjson.gz"));
        assert!(is_supported_json_file("path/to/data.NDJSON.GZ")); // case insensitive

        // Unsupported formats
        assert!(!is_supported_json_file("data.parquet"));
        assert!(!is_supported_json_file("data.csv"));
        assert!(!is_supported_json_file("data.txt"));
        assert!(!is_supported_json_file("data.gz")); // just .gz without json
    }

    #[test]
    fn test_is_gzip_compressed() {
        assert!(is_gzip_compressed("data.ndjson.gz"));
        assert!(is_gzip_compressed("data.json.gz"));
        assert!(is_gzip_compressed("data.GZ")); // case insensitive

        assert!(!is_gzip_compressed("data.ndjson"));
        assert!(!is_gzip_compressed("data.json"));
    }
}
