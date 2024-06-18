use elasticsearch::auth::Credentials;
use elasticsearch::cert::CertificateValidation;
use elasticsearch::{
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    CountParts, Elasticsearch, OpenPointInTimeParts, SearchParts,
};
use std::collections::HashMap;
use std::default::Default;
use std::fmt::Debug;
use std::io::Write;

use crate::compression::Compression;
use anyhow::{bail, Context};
use elasticsearch::http::headers::HeaderMap;
use elasticsearch::http::Method;
use std::ops::Range;
use std::time::{Duration, Instant};

use serde_json::value::RawValue;
use serde_json::{json, Map, Value};
use tracing::{debug, error, info, instrument};
use url::Url;

use crate::stats::BatchStats;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Deserialize, Serialize, Clone, clap::ValueEnum, Debug)]
#[serde(rename_all = "lowercase")]
pub enum ElasticDistribution {
    Elasticsearch,
    Opensearch,
}

#[derive(Deserialize, Debug)]
pub struct ClusterInfo {
    pub version: ClusterVersion,
}

#[derive(Deserialize, Debug)]
pub struct ClusterVersion {
    pub distribution: ElasticDistribution,
}

#[derive(Deserialize, Debug)]
pub struct Hits<'a> {
    #[serde(borrow)]
    pub hits: Vec<&'a RawValue>,
}

#[derive(Deserialize, Debug)]
pub struct SearchResult<'a> {
    #[serde(borrow)]
    pub hits: Hits<'a>,
    pub took: u32,
    pub pit_id: String,
}

#[derive(Deserialize, Debug)]
pub struct IndexCountResponse {
    pub count: u32,
}

#[derive(Serialize)]
pub struct OutputRecord<'a> {
    id: &'a str,
    fields: &'a RawValue,
}

#[derive(Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct PointInTimeResponse {
    pub id: String,
}

pub type PointInTimeID = String;

pub struct ElasticsearchClient {
    client: Elasticsearch,
    index: String,
    query: Map<String, Value>,

    // The docs very explicitly call out that the point-in-time ID can change between calls.
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html
    // However, this doesn't seem to actually happen in practice: it seems stable.
    // However, to ensure that we account for this we wrap the point-in-time ID in a RWLock
    // and update it where required.
    // When we take the RWLock we keep the reader alive for the duration of the request, which
    // has the nice property of ensuring all requests finish (and new ones are blocked) whilst we
    // wait to update the point-in-time ID.
    // This should keep things consistent. However, the documentation is light on the specifics
    // around what happens when an ID change does happen in the case of concurrent requests using
    // the same ID: do the same requests get the same new ID returned, or different ones?
    point_in_time: RwLock<PointInTimeID>,
}

impl ElasticsearchClient {
    pub async fn new(
        url: Url,
        creds: Credentials,
        index: &str,
        query: Map<String, Value>,
        distribution: Option<ElasticDistribution>,
    ) -> anyhow::Result<Self> {
        let conn_pool = SingleNodeConnectionPool::new(url);
        let transport = TransportBuilder::new(conn_pool)
            .auth(creds)
            .cert_validation(CertificateValidation::None)
            .disable_proxy()
            .build()?;
        let client = Elasticsearch::new(transport);

        let distribution = match distribution {
            Some(d) => d,
            None => {
                debug!("Detecting distribution");
                let cluster_info_response = client
                    .send::<(), ()>(Method::Get, "/", HeaderMap::new(), None, None, None)
                    .await?;
                let cluster_info: ClusterInfo = cluster_info_response.json().await?;
                info!(
                    "Distribution detected: {:?}",
                    cluster_info.version.distribution
                );
                cluster_info.version.distribution
            }
        };

        let pit_id: PointInTimeID = match distribution {
            ElasticDistribution::Elasticsearch => {
                let pit_resp = client
                    .open_point_in_time(OpenPointInTimeParts::Index(&[index]))
                    .keep_alive("2m")
                    .send()
                    .await?;
                let point_in_time_response: PointInTimeResponse = match pit_resp.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        bail!("Could not get point-in-time ID ({e}). Use the --distribution flag to specify opensearch or elasticsearch")
                    }
                };
                point_in_time_response.id
            }
            ElasticDistribution::Opensearch => {
                // POST /my-index-1/_search/point_in_time?keep_alive=100m
                let pit_resp = client
                    .send::<(), _>(
                        Method::Post,
                        &format!("/{index}/_search/point_in_time"),
                        HeaderMap::new(),
                        Some(&HashMap::from([("keep_alive", "2m")])),
                        None,
                        None,
                    )
                    .await?;
                let point_in_time_response: Value = pit_resp.json().await?;

                match point_in_time_response.get("pit_id") {
                    None => {
                        error!("Invalid PIT response: {point_in_time_response}");
                        bail!("Could not get point-in-time ID. Use the --distribution flag to specify opensearch or elasticsearch")
                    }
                    Some(v) => v.as_str().unwrap().to_string(),
                }
            }
        };

        debug!("Got point-in-time ID: {:?}", pit_id);

        Ok(ElasticsearchClient {
            client,
            query,
            index: index.to_string(),
            point_in_time: RwLock::new(pit_id),
        })
    }

    pub async fn get_index_document_range(&self) -> anyhow::Result<Range<u32>> {
        let resp = self
            .client
            .count(CountParts::Index(&[&self.index]))
            .send()
            .await?;
        let count_response: IndexCountResponse = resp.json().await?;
        info!("Counted {count} records", count = count_response.count);
        let document_range: Range<u32> = 0..count_response.count;
        Ok(document_range)
    }

    async fn update_point_in_time(&self, new_point_in_time: PointInTimeID) {
        let mut locked = self.point_in_time.write().await;
        info!("Point in time changed to {new_point_in_time}");
        *locked = new_point_in_time;
    }

    #[instrument(skip_all, level = "debug")]
    pub async fn fetch_single_batch(
        &self,
        id_range: &Range<u32>,
        compression: Compression,
        output_buffer: Vec<u8>,
    ) -> anyhow::Result<(BatchStats, Vec<u8>)> {
        let start_idx: u32 = id_range.start - 1;

        let mut query = self.query.clone();
        query.insert("search_after".to_string(), json!([start_idx]));
        query.insert("query".to_string(), json!({"match_all": {}}));

        debug!("ID Range: {:?}, Query: {:?}", id_range, query);

        let es_start_time = Instant::now();

        let (response, used_point_in_time) = {
            let mut attempt = 0;
            loop {
                attempt += 1;

                // Point-in-time ID could change between loops 🤷‍
                let point_in_time = self.point_in_time.read().await;
                query.insert(
                    "pit".to_string(),
                    json!({"id": *point_in_time, "keep_alive": "2m"}),
                );

                let resp_result = self
                    .client
                    .search(SearchParts::None)
                    .track_total_hits(false)
                    // .header(ACCEPT_ENCODING, HeaderValue::from_static("gzip"))
                    // Can use different formats here:
                    // https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-rest-format.html
                    // .header(ACCEPT, HeaderValue::from_static("application/json"))
                    .track_scores(false)
                    .size(id_range.len() as i64)
                    .allow_partial_search_results(false)
                    .sort(&["_doc"])
                    .body(&query)
                    .send()
                    .await;
                match resp_result {
                    Ok(r) => {
                        break (r, point_in_time.clone());
                    }
                    Err(e) => {
                        if attempt > 3 || e.is_json() {
                            // JSON errors are not really recoverable - just bail
                            return Err(e.into());
                        }
                        error!(
                            "Attempt {attempt} for {id_range:?} failed",
                            attempt = attempt,
                            id_range = id_range
                        );
                        // Backoff for 1 second. Not great, not bad.
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        };
        let response_body = response.text().await.context("Error reading content")?;

        let elasticsearch_time = es_start_time.elapsed();
        let response_bytes = response_body.as_bytes().len() as u64;

        let (mut stats, compressed_bytes, new_point_in_time) = self
            .compress_batch(compression, response_body, output_buffer)
            .await
            .context("Error compressing")?;

        if used_point_in_time != new_point_in_time {
            self.update_point_in_time(new_point_in_time).await;
        }

        stats.elasticsearch = elasticsearch_time;
        stats.elasticsearch_bytes = response_bytes;

        Ok((stats, compressed_bytes))
    }

    #[instrument(skip_all, level = "debug")]
    async fn compress_batch(
        &self,
        compression: Compression,
        buffer: String,
        output_buffer: Vec<u8>,
    ) -> anyhow::Result<(BatchStats, Vec<u8>, PointInTimeID)> {
        let result = tokio::task::spawn_blocking(move || {
            let json_parse_start = Instant::now();
            let results: SearchResult = serde_json::from_str(&buffer).with_context(|| {
                let end_item = buffer.len().min(500);
                format!(
                    "Error deserializing response. First 500 bytes: {}",
                    &buffer[0..end_item]
                )
            })?;
            let json_duration = json_parse_start.elapsed();

            let compression_start = Instant::now();
            let mut encoder = compression.get_encoder(output_buffer);
            for hit in results.hits.hits {
                encoder.write_all(hit.get().as_bytes())?;
                encoder.write_all(&[b'\n'])?;
            }
            let compressed_bytes = encoder.finish()?;
            let compression_duration = compression_start.elapsed();

            let stats = BatchStats {
                json: json_duration,
                compression: compression_duration,
                compressed_bytes: compressed_bytes.len() as u64,
                ..Default::default()
            };

            Ok::<_, anyhow::Error>((stats, compressed_bytes, results.pit_id))
        })
        .await??;
        Ok(result)
    }
}
