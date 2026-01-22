use async_compression::tokio::bufread::GzipDecoder;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::json::reader::{Decoder, ReaderBuilder};
use deltalake::ObjectStore;
use futures::TryStreamExt;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::io::StreamReader;
use tracing::log::*;

/// Stream and parse a file from GCS with lazy reading
/// - Uses object_store's streaming API (bytes fetched on demand)
/// - Uses configurable batch size for accumulating records
pub async fn stream_and_parse_file(
    store: &Arc<dyn ObjectStore>,
    path: &deltalake::Path,
    arrow_schema: Arc<deltalake::arrow::datatypes::Schema>,
    is_compressed: bool,
    batch_size: usize,
) -> Result<(Vec<RecordBatch>, usize), anyhow::Error> {
    // Get file as a stream of bytes
    let get_result = store.get(path).await?;

    // Convert to a stream of bytes, then to AsyncRead
    let byte_stream = get_result
        .into_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let stream_reader = StreamReader::new(byte_stream);

    // Create incremental JSON decoder with configurable batch size
    let mut decoder = ReaderBuilder::new(arrow_schema)
        .with_batch_size(batch_size)
        .build_decoder()?;

    // Stream decompress and parse
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

/// Deserialize bytes from an async reader into RecordBatches
async fn deserialize_stream(
    mut reader: impl tokio::io::AsyncBufRead + Unpin,
    decoder: &mut Decoder,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let mut batches = Vec::new();

    loop {
        let buf = reader.fill_buf().await?;

        if buf.is_empty() {
            // End of stream - flush any remaining data
            if let Some(batch) = decoder.flush()? {
                if batch.num_rows() > 0 {
                    batches.push(batch);
                }
            }
            break;
        }

        let mut have_read = decoder.decode(buf)?;

        // If decoder couldn't make progress, flush to free internal buffer space
        if have_read == 0 && !buf.is_empty() {
            if let Some(batch) = decoder.flush()? {
                batches.push(batch);
            }
            // Try decode again after flush
            have_read = decoder.decode(buf)?;
            if have_read == 0 {
                // Still can't progress - skip one byte to avoid infinite loop
                warn!("Decoder stuck, skipping byte");
                reader.consume(1);
                continue;
            }
        }

        reader.consume(have_read);

        // Flush when decoder has complete records
        if !decoder.has_partial_record() {
            if let Some(batch) = decoder.flush()? {
                batches.push(batch);
            }
        }
    }

    Ok(batches)
}

/// Check if a filename is a supported JSON file format
pub fn is_supported_json_file(name: &str) -> bool {
    let lower = name.to_lowercase();
    lower.ends_with(".json")
        || lower.ends_with(".jsonl")
        || lower.ends_with(".ndjson")
        || lower.ends_with(".json.gz")
        || lower.ends_with(".jsonl.gz")
        || lower.ends_with(".ndjson.gz")
}

/// Check if a file is gzip compressed based on extension
pub fn is_gzip_compressed(name: &str) -> bool {
    name.to_lowercase().ends_with(".gz")
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
