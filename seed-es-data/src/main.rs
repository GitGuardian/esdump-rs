use clap::Parser;
use elasticsearch::auth::Credentials;
use elasticsearch::cert::CertificateValidation;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::http::StatusCode;
use elasticsearch::indices::{IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts, IndicesRefreshParts};
use elasticsearch::{BulkParts, CountParts, Elasticsearch, IndexParts, SearchParts};
use futures_util::StreamExt;
use indicatif::ProgressBar;
use serde_json::{json, Value};
use std::sync::Arc;
use url::Url;
use uuid::Uuid;
use itertools::Itertools;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    es_url: Url,
    index_name: String,

    #[arg(long, default_value = "100000")]
    count: usize,

    #[arg(long, default_value = "100")]
    chunk_size: usize,

    #[arg(long, default_value = "100")]
    concurrency: usize,

    #[arg(long, default_value = "5KB")]
    text_size: byte_unit::Byte,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    dotenv::from_filename("test.env")?;

    let username = "admin".to_string();
    let password = dotenv::var("OPENSEARCH_INITIAL_ADMIN_PASSWORD")?;

    let creds = Credentials::Basic(username, password);
    let conn_pool = SingleNodeConnectionPool::new(args.es_url);
    let transport = TransportBuilder::new(conn_pool)
        .auth(creds)
        .cert_validation(CertificateValidation::None)
        .disable_proxy()
        .build()?;
    let client = Arc::new(Elasticsearch::new(transport));

    if client
        .indices()
        .exists(IndicesExistsParts::Index(&[&args.index_name]))
        .send()
        .await?
        .status_code()
        != StatusCode::NOT_FOUND
    {
        client
            .indices()
            .delete(IndicesDeleteParts::Index(&[&args.index_name]))
            .send()
            .await?.error_for_status_code()?;
    };

    client
        .indices()
        .create(IndicesCreateParts::Index(&args.index_name))
        .send()
        .await?.error_for_status_code()?;

    let data = (0..args.count).map(|idx| {
        let uuid = Uuid::now_v7();
        pub static ALPHANUMERIC_WITH_SPACE: &str =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";
        let rand_string =
            random_string::generate(args.text_size.as_u64() as usize, ALPHANUMERIC_WITH_SPACE);

        let numbers: Vec<_> = (idx..idx + 10).collect();
        let document = json!({
            "meta": {
                "item_id": idx,
                "uuid": uuid,
            },
            "numbers": numbers,
            "content": rand_string
        });
        serde_json::to_string(&document).unwrap()
    }).into_iter().chunks(args.chunk_size);



    let futures = data.into_iter().map(|chunks| {
        let items: Vec<_> = chunks.collect();
        let client = client.clone();
        let index = BulkParts::Index(&args.index_name);

        let action = serde_json::to_string(&json!({"create": {}})).unwrap();

        async move {
            let body_size: usize = items.iter().map(|r| r.len()).sum();

            let items: Vec<_> = std::iter::repeat(action).interleave_shortest(items.into_iter()).collect();

            client.bulk(index).body(items).send().await?.error_for_status_code()?;
            Ok::<_, anyhow::Error>(body_size)
        }
    });

    let mut stream = futures_util::stream::iter(futures).buffer_unordered(args.concurrency);

    let pbar = ProgressBar::new((args.count / args.chunk_size) as u64);

    let mut total_bytes = 0;
    while let Some(result) = stream.next().await {
        total_bytes += result?;
        pbar.inc(1);
    }

    pbar.finish();

    let res = client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[&args.index_name]))
        .send()
        .await?;
    println!("Refresh: {:#}", res.json::<Value>().await?);

    let search_result = client
        .search(SearchParts::Index(&[&args.index_name]))
        .body(json!({
            "query" : {
                "match_all" : {}
            }
        }))
        .size(1)
        .send()
        .await?;

    println!(
        "Random document: {:#}",
        search_result.json::<Value>().await?
    );

    let result = client
        .count(CountParts::Index(&[&args.index_name]))
        .send()
        .await?;
    println!("Index count: {:#}", result.json::<Value>().await?);

    println!(
        "Inserted {} of data",
        human_bytes::human_bytes(total_bytes as f64)
    );

    Ok(())
}
