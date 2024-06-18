mod compression;
mod elasticsearch;
mod stats;
mod storage;
mod utils;

use indicatif::ProgressStyle;
use std::fs::File;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::rc::Rc;

use ::elasticsearch::auth::Credentials;
use anyhow::Context;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, info_span, warn, Instrument};

use crate::elasticsearch::{ElasticDistribution, ElasticsearchClient};
use clap::Parser;
use clap_num::number_range;
use futures_util::StreamExt;
use serde_json::{Map, Value};

use tracing_indicatif::span_ext::IndicatifSpanExt;
use tracing_indicatif::IndicatifLayer;

use crate::compression::Compression;
use crate::storage::StorageBackend;
use stats::BatchStats;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use url::Url;

fn valid_batch_size(s: &str) -> Result<u16, String> {
    number_range(s, 1, 10_000)
}

fn valid_batches_per_file(s: &str) -> Result<u16, String> {
    number_range(s, 1, 1_000)
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Elasticsearch cluster to dump
    elasticsearch_url: Url,

    /// Location to write results.
    /// Can be a file://, s3://, gs:// or az:// URL.
    output_location: Url,

    /// Index to dump
    #[arg(short, long)]
    index: String,

    /// Number of concurrent requests to use
    #[arg(short, long)]
    concurrency: NonZeroUsize,

    /// Limit the total number of records returned
    #[arg(short, long)]
    limit: Option<NonZeroU32>,

    /// Number of records in each batch
    #[arg(short, long, value_parser = valid_batch_size)]
    batch_size: u16,

    /// Number of batches to write per file
    #[arg(long, value_parser = valid_batches_per_file)]
    batches_per_file: u16,

    /// A file path containing a query to execute while dumping
    #[arg(short, long)]
    query: Option<PathBuf>,

    /// Specific fields to fetch
    #[arg(short, long)]
    field: Option<Vec<String>>,

    /// Compress the output files
    #[arg(value_enum, long, default_value_t = Compression::Zstd)]
    compression: Compression,

    /// Max chunks to concurrently upload *per task*
    #[arg(long)]
    concurrent_uploads: Option<NonZeroUsize>,

    /// Size of each uploaded
    #[arg(long, default_value = "15MB")]
    upload_size: byte_unit::Byte,

    /// Distribution of the cluster
    #[arg(short, long)]
    distribution: Option<ElasticDistribution>,

    /// Distribution of the cluster
    #[arg(long, default_value = ".env")]
    env_file: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    let footer_style =
        ProgressStyle::with_template("...and {pending_progress_bars} more not shown above.")?;
    let indicatif_layer = IndicatifLayer::default().with_max_progress_bars(40, Some(footer_style));

    let filter_builder = EnvFilter::builder();
    let filter = filter_builder
        .try_from_env()
        .unwrap_or_else(|_| filter_builder.parse_lossy("RUST_LOG=warn,esdump_rs=info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(indicatif_layer.get_stderr_writer()))
        .with(indicatif_layer)
        .init();

    if let Err(e) = dotenv::from_filename(args.env_file) {
        warn!("Error reading env file: {}", e);
    };

    let creds = Credentials::Basic(
        std::env::var("ES_DUMP_USERNAME").context("ES_DUMP_USERNAME env var not set")?,
        std::env::var("ES_DUMP_PASSWORD").context("ES_DUMP_PASSWORD env var not set")?,
    );

    let mut query: Map<String, Value> = match args.query {
        None => Map::new(),
        Some(query_file) => {
            let query_file = File::open(query_file).context("Error reading query file")?;
            serde_json::from_reader(query_file).context("Error parsing query file JSON")?
        }
    };

    if let Some(fields) = args.field {
        // Convert to a vec of Value objects
        let fields: Vec<_> = fields.into_iter().map(Value::String).collect();
        query.insert("_source".to_string(), Value::Array(fields));
    };

    info!(
        "Dumping index {} {} to {}",
        args.index, args.elasticsearch_url, args.output_location
    );
    info!(
        "Using {:?} concurrent uploads, with {} concurrent fetchers",
        args.concurrent_uploads,
        args.concurrency.get()
    );
    info!("Using query {}", serde_json::to_string_pretty(&query)?);
    let storage = StorageBackend::from_url(
        &args.output_location,
        args.upload_size,
        args.concurrent_uploads,
    )?;
    let client = Rc::new(storage);

    let es_client = Arc::new(
        ElasticsearchClient::new(
            args.elasticsearch_url,
            creds,
            &args.index,
            query,
            args.distribution,
        )
        .await
        .context("Error creating ES client")?,
    );

    let index_range = es_client
        .get_index_document_range()
        .await
        .context("Error getting document index range")?;
    let task_range = match args.limit {
        None => index_range,
        Some(limit) => 0..index_range.end.min(limit.get()),
    };
    let split_ranges = utils::split_range(task_range, args.batch_size);
    let batches: Vec<_> = split_ranges
        .chunks(args.batches_per_file as usize)
        .collect();
    info!(
        "Got {batch_count} batches to process in {chunk_count} chunks",
        batch_count = split_ranges.len(),
        chunk_count = batches.len()
    );

    let header_span = info_span!("fetch_batches");
    header_span.pb_set_style(&ProgressStyle::with_template(
        "[{elapsed} {percent}%] {wide_bar} {pos}/{len} [ETA {eta}]",
    )?);
    header_span.pb_set_length(split_ranges.len() as u64);

    let header_span_enter = header_span.enter();

    let futures = batches.into_iter().enumerate().map(|(idx, ranges)| {
        let es_client = es_client.clone();

        let parent_span = tracing::Span::current();
        let range_count = ranges.len();
        let min_row = ranges[0].start;
        let max_row = ranges[range_count - 1].end;
        let overall_range = min_row..max_row;
        let suffix = uuid::Uuid::new_v7(uuid::timestamp::Timestamp::now(uuid::NoContext));
        let client = client.clone();

        async move {
            let upload_file_name = format!("{idx:0>5}-{suffix}.jsonl");
            let (fs_url, mut upload) = client
                .create_streaming_upload(&upload_file_name, args.compression)
                .await.with_context(|| format!("Error creating streaming upload for file {upload_file_name}"))?;
            let mut compressed_buffer = Vec::with_capacity(1024 * 1024 * 10);
            let mut total_batch_stats = BatchStats::default();

            for (range_idx, range) in ranges.iter().enumerate() {
                let storage_time = Instant::now();
                client.wait_for_capacity(&mut upload).await?;
                total_batch_stats.storage += storage_time.elapsed();

                let batch_idx = range_idx + 1;
                let (batch_stats, mut batch_compressed_buffer) = es_client
                    .fetch_single_batch(range, args.compression, compressed_buffer)
                    .instrument(info_span!("fetch_batch",
                        batch = batch_idx,
                        batches=range_count,
                        ids=?overall_range,
                        chunk=idx,
                    ))
                    .await.with_context(|| format!("Error uploading batch {batch_idx} for chunk {idx} to file {upload_file_name}"))?;

                // Add metrics to the total stats
                total_batch_stats += batch_stats;

                // Write the contents to the storage backend. Should be quick, but it may copy the
                // buffer, and so it's good to time.
                let buffers_time = Instant::now();

                upload.write(&batch_compressed_buffer);
                // Clear the buffer, removing all values, ready for the next iteration
                batch_compressed_buffer.clear();
                // Reset compressed_buffer for the next iteration
                compressed_buffer = batch_compressed_buffer;

                total_batch_stats.buffers += buffers_time.elapsed();

                parent_span.pb_inc(1);
            }

            let storage_time = Instant::now();
            upload.finish().await.with_context(|| format!("Error finishing streaming upload for file {upload_file_name}"))?;
            total_batch_stats.storage += storage_time.elapsed();

            Ok::<_, anyhow::Error>((idx, total_batch_stats, fs_url))
        }
    });

    let mut total_timings = BatchStats::default();
    let mut stream = futures_util::stream::iter(futures).buffer_unordered(args.concurrency.get());
    while let Some(result) = stream.next().await {
        let (chunk, timings, url) = result?;
        info!("Chunk {chunk} uploaded to {url}. Timings: {timings}");
        total_timings += timings;
    }
    drop(header_span_enter);
    drop(header_span);
    info!("Completed! Total timings: {total_timings}");
    Ok(())
}
