//! Benchmark for file-loader throughput
//!
//! Run with: cargo bench -p file-loader-cloudrun
//!
//! This benchmarks the core file parsing logic - streaming JSON to Arrow RecordBatches.
//!
use async_compression::tokio::bufread::GzipDecoder;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::json::reader::{Decoder, ReaderBuilder};
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader};

/// Generate test JSONL data with specified number of records
fn generate_jsonl_data(num_records: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(num_records * 100);
    for i in 0..num_records {
        let record = format!(
            r#"{{"id":{},"name":"user_{}","email":"user{}@example.com","score":{},"active":true,"timestamp":"2024-01-15T10:30:00Z"}}"#,
            i,
            i,
            i,
            i % 100
        );
        data.extend_from_slice(record.as_bytes());
        data.push(b'\n');
    }
    data
}

/// Generate gzip-compressed JSONL data
fn generate_gzip_jsonl_data(num_records: usize) -> Vec<u8> {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

    let raw = generate_jsonl_data(num_records);
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(&raw).unwrap();
    encoder.finish().unwrap()
}

/// Arrow schema for our test data
fn test_schema() -> Arc<deltalake::arrow::datatypes::Schema> {
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("timestamp", DataType::Utf8, false),
    ]))
}

/// Deserialize bytes from an async reader into RecordBatches
async fn deserialize_stream(
    mut reader: impl AsyncBufRead + Unpin,
    decoder: &mut Decoder,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let mut batches = Vec::new();

    loop {
        let buf = reader.fill_buf().await?;

        if buf.is_empty() {
            if let Some(batch) = decoder.flush()? {
                if batch.num_rows() > 0 {
                    batches.push(batch);
                }
            }
            break;
        }

        let have_read = decoder.decode(buf)?;
        reader.consume(have_read);

        if !decoder.has_partial_record() {
            if let Some(batch) = decoder.flush()? {
                batches.push(batch);
            }
        }
    }

    Ok(batches)
}

/// Parse data (simulating what happens after fetching from GCS)
async fn parse_data(
    data: Vec<u8>,
    arrow_schema: Arc<deltalake::arrow::datatypes::Schema>,
    is_compressed: bool,
) -> Result<(Vec<RecordBatch>, usize), anyhow::Error> {
    let cursor = Cursor::new(data);

    let buffered: Box<dyn AsyncBufRead + Unpin + Send> = if is_compressed {
        Box::new(BufReader::new(GzipDecoder::new(BufReader::new(cursor))))
    } else {
        Box::new(BufReader::new(cursor))
    };

    let mut decoder = ReaderBuilder::new(arrow_schema)
        .with_batch_size(10_000)
        .build_decoder()?;

    let batches = deserialize_stream(buffered, &mut decoder).await?;
    let total_records: usize = batches.iter().map(|b| b.num_rows()).sum();

    Ok((batches, total_records))
}

#[derive(Debug, Clone)]
struct BenchmarkResult {
    scenario: String,
    num_files: usize,
    records_per_file: usize,
    total_records: usize,
    raw_data_bytes: usize,
    compressed_bytes: Option<usize>,
    duration: Duration,
    records_per_sec: f64,
    mb_per_sec: f64,
}

impl std::fmt::Display for BenchmarkResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== {} ===", self.scenario)?;
        writeln!(
            f,
            "Files: {}, Records/file: {}",
            self.num_files, self.records_per_file
        )?;
        writeln!(f, "Total records: {}", self.total_records)?;
        writeln!(
            f,
            "Raw data: {:.2} MB",
            self.raw_data_bytes as f64 / 1_000_000.0
        )?;
        if let Some(compressed) = self.compressed_bytes {
            writeln!(
                f,
                "Compressed: {:.2} MB ({:.1}% of raw)",
                compressed as f64 / 1_000_000.0,
                (compressed as f64 / self.raw_data_bytes as f64) * 100.0
            )?;
        }
        writeln!(f, "Duration: {:.2?}", self.duration)?;
        writeln!(f, "Throughput: {:.0} records/sec", self.records_per_sec)?;
        writeln!(f, "Throughput: {:.2} MB/sec (raw)", self.mb_per_sec)?;
        Ok(())
    }
}

async fn run_benchmark(
    scenario: &str,
    num_files: usize,
    records_per_file: usize,
    compressed: bool,
    concurrency: usize,
) -> Result<BenchmarkResult, anyhow::Error> {
    // Generate test data
    let raw_data = generate_jsonl_data(records_per_file);
    let raw_size = raw_data.len();

    let file_data = if compressed {
        generate_gzip_jsonl_data(records_per_file)
    } else {
        raw_data.clone()
    };
    let compressed_size = if compressed {
        Some(file_data.len())
    } else {
        None
    };

    let schema = test_schema();

    // Benchmark: parse all files concurrently
    let start = Instant::now();

    let results: Vec<_> = futures::stream::iter(0..num_files)
        .map(|_| {
            let data = file_data.clone();
            let schema = schema.clone();
            async move { parse_data(data, schema, compressed).await }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let duration = start.elapsed();

    // Calculate totals
    let mut total_records_parsed = 0usize;
    for result in results {
        let (_batches, count) = result?;
        total_records_parsed += count;
    }

    let raw_data_bytes = raw_size * num_files;
    let records_per_sec = total_records_parsed as f64 / duration.as_secs_f64();
    let mb_per_sec = (raw_data_bytes as f64 / 1_000_000.0) / duration.as_secs_f64();

    Ok(BenchmarkResult {
        scenario: scenario.to_string(),
        num_files,
        records_per_file,
        total_records: total_records_parsed,
        raw_data_bytes,
        compressed_bytes: compressed_size.map(|s| s * num_files),
        duration,
        records_per_sec,
        mb_per_sec,
    })
}

use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    println!("File Loader Benchmark");
    println!("=====================\n");
    println!("This benchmarks the core JSON-to-Arrow parsing logic.\n");

    let concurrency = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    println!("CPU cores available: {}\n", concurrency);

    // Warmup
    println!("Warming up...\n");
    let _ = run_benchmark("warmup", 2, 1000, false, concurrency).await?;

    let scenarios = vec![
        // (name, files, records_per_file, compressed)
        ("Small files (1K records each)", 10, 1_000, false),
        ("Medium files (10K records each)", 10, 10_000, false),
        ("Large files (100K records each)", 10, 100_000, false),
        ("Many small files", 100, 1_000, false),
        ("Compressed small files", 10, 1_000, true),
        ("Compressed large files", 10, 100_000, true),
        ("Single large file (1M records)", 1, 1_000_000, false),
        ("High concurrency (100 files)", 100, 10_000, false),
    ];

    let mut results = Vec::new();

    for (name, files, records, compressed) in scenarios {
        print!("Running: {}... ", name);
        std::io::Write::flush(&mut std::io::stdout())?;

        match run_benchmark(name, files, records, compressed, concurrency).await {
            Ok(result) => {
                println!("done ({:.2?})", result.duration);
                results.push(result);
            }
            Err(e) => {
                println!("FAILED: {:?}", e);
            }
        }
    }

    println!("\n\n========== RESULTS ==========\n");
    for result in &results {
        println!("{}", result);
    }

    // Summary table
    println!("========== SUMMARY ==========");
    println!("{:<40} {:>15} {:>12}", "Scenario", "Records/sec", "MB/sec");
    println!("{:-<40} {:->15} {:->12}", "", "", "");
    for result in &results {
        println!(
            "{:<40} {:>15.0} {:>12.2}",
            result.scenario, result.records_per_sec, result.mb_per_sec
        );
    }

    Ok(())
}
