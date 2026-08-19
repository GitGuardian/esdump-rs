mod compression;
mod elasticsearch;
mod paging;
mod stats;
mod storage;

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

    /// Retained for compatibility. Paging is sequential within an index (each page's
    /// cursor comes from the previous page), so this no longer controls request
    /// concurrency; use --concurrent-uploads to bound in-flight uploads.
    #[arg(short, long)]
    concurrency: Option<NonZeroUsize>,

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
    info!("Using {:?} concurrent uploads", args.concurrent_uploads);
    if args.concurrency.is_some() {
        warn!("--concurrency is ignored: paging within an index is sequential by construction");
    }
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

    let document_count = es_client
        .get_index_document_count()
        .await
        .context("Error getting document count")?;
    let target_count = match args.limit {
        None => document_count,
        Some(limit) => document_count.min(limit.get()),
    };
    let pages_per_file = args.batches_per_file as usize;
    info!(
        "Got {target_count} records to fetch in pages of {batch_size}, {pages_per_file} pages per file",
        target_count = target_count,
        batch_size = args.batch_size,
        pages_per_file = pages_per_file,
    );

    let header_span = info_span!("fetch_batches");
    header_span.pb_set_style(&ProgressStyle::with_template(
        "[{elapsed} {percent}%] {wide_bar} {pos}/{len} [ETA {eta}]",
    )?);
    header_span.pb_set_length(target_count as u64);

    let header_span_enter = header_span.enter();

    // Paging is sequential by necessity: each page's cursor is the previous page's last sort
    // value, so a page cannot start before its predecessor has returned. Parallelism comes
    // from running one process per index, and from --concurrent-uploads within a file.
    let mut total_timings = BatchStats::default();
    let mut compressed_buffer = Vec::with_capacity(1024 * 1024 * 10);
    let mut search_after: Option<Vec<Value>> = None;
    let mut fetched: u32 = 0;
    let mut file_idx: usize = 0;
    let mut exhausted = false;

    while !exhausted && fetched < target_count {
        let suffix = uuid::Uuid::new_v7(uuid::timestamp::Timestamp::now(uuid::NoContext));
        let upload_file_name = format!("{file_idx:0>5}-{suffix}.jsonl");
        let (fs_url, mut upload) = client
            .create_streaming_upload(&upload_file_name, args.compression)
            .await
            .with_context(|| {
                format!("Error creating streaming upload for file {upload_file_name}")
            })?;
        let mut file_stats = BatchStats::default();

        for page_idx in 0..pages_per_file {
            let size = paging::page_size(args.batch_size, fetched, target_count);

            let storage_time = Instant::now();
            client.wait_for_capacity(&mut upload).await?;
            file_stats.storage += storage_time.elapsed();

            let page = es_client
                .fetch_page(
                    search_after.as_ref(),
                    size,
                    args.compression,
                    compressed_buffer,
                )
                .instrument(info_span!("fetch_page",
                    page = page_idx + 1,
                    pages = pages_per_file,
                    fetched = fetched,
                    file = file_idx,
                ))
                .await
                .with_context(|| {
                    format!("Error fetching page {page_idx} for file {upload_file_name}")
                })?;

            let mut page_buffer = page.buffer;
            file_stats += page.stats;
            fetched += page.hits as u32;

            let buffers_time = Instant::now();
            upload.write(&page_buffer);
            // Clear the buffer, removing all values, ready for the next iteration
            page_buffer.clear();
            compressed_buffer = page_buffer;
            file_stats.buffers += buffers_time.elapsed();

            header_span.pb_inc(page.hits as u64);

            match paging::next_cursor(page.hits, size, page.last_sort) {
                Some(cursor) => search_after = Some(cursor),
                None => {
                    exhausted = true;
                    break;
                }
            }

            if fetched >= target_count {
                break;
            }
        }

        let storage_time = Instant::now();
        upload.finish().await.with_context(|| {
            format!("Error finishing streaming upload for file {upload_file_name}")
        })?;
        file_stats.storage += storage_time.elapsed();

        info!("File {file_idx} uploaded to {fs_url}. Timings: {file_stats}");
        total_timings += file_stats;
        file_idx += 1;
    }

    info!("Fetched {fetched} records into {file_idx} files");

    drop(header_span_enter);
    drop(header_span);
    info!("Completed! Total timings: {total_timings}");
    Ok(())
}
