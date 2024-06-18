use anyhow::bail;
use object_store::aws::AmazonS3Builder;
use object_store::azure::{MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::PathPart;
use object_store::{path::Path, ObjectStore, WriteMultipart};
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use tracing::{debug};

use crate::compression::Compression;
use url::Url;

#[derive(Debug)]
pub struct StorageBackend {
    url: Url,
    store: Box<dyn ObjectStore>,
    path: Path,
    buffer_size: byte_unit::Byte,
    concurrent_uploads: Option<NonZeroUsize>,
}

impl Display for StorageBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.url.as_str())
    }
}

fn parse_url(url: &Url) -> anyhow::Result<(Box<dyn ObjectStore>, Path)> {
    // See https://github.com/apache/arrow-rs/pull/5912
    let (store, path) = match (url.scheme(), url.host_str()) {
        ("file", None) => (Box::new(LocalFileSystem::new()) as _, url.path()),
        ("memory", None) => (Box::new(InMemory::new()) as _, url.path()),
        ("s3" | "s3a", Some(_)) => (
            Box::new(
                AmazonS3Builder::from_env()
                    .with_url(url.to_string())
                    .build()?,
            ) as _,
            url.path(),
        ),
        ("gs", Some(_)) => (
            Box::new(
                GoogleCloudStorageBuilder::from_env()
                    .with_url(url.to_string())
                    .build()?,
            ) as _,
            url.path(),
        ),
        ("az" | "adl" | "azure" | "abfs" | "abfss", Some(_)) => (
            Box::new(
                MicrosoftAzureBuilder::from_env()
                    .with_url(url.to_string())
                    .build()?,
            ) as _,
            url.path(),
        ),
        _ => bail!("Unknown storage target {url}."),
    };

    Ok((store, Path::parse(path)?))
}

impl StorageBackend {
    pub fn from_url(
        url: &Url,
        buffer_size: byte_unit::Byte,
        concurrent_uploads: Option<NonZeroUsize>,
    ) -> anyhow::Result<StorageBackend> {
        let (store, path) = parse_url(url)?;
        Ok(StorageBackend {
            url: url.clone(),
            store,
            path,
            buffer_size,
            concurrent_uploads,
        })
    }

    pub async fn create_streaming_upload(
        &self,
        file_name: &str,
        compression: Compression,
    ) -> anyhow::Result<(String, WriteMultipart)> {
        let file_name_path = PathPart::from(format!("{file_name}.{}", compression.extension()));
        let key_path = Path::from_iter(self.path.parts().chain([file_name_path]));
        debug!("Created multipart upload for {key_path:?}");
        let upload = self.store.put_multipart(&key_path).await?;

        let buffer_size = self.buffer_size.as_u64() as usize;

        let writer = WriteMultipart::new_with_chunk_size(upload, buffer_size);
        Ok(("to-do".to_string(), writer))
    }

    pub async fn wait_for_capacity(&self, upload: &mut WriteMultipart) -> anyhow::Result<()> {
        if let Some(concurrency) = self.concurrent_uploads {
            upload.wait_for_capacity(concurrency.get()).await?;
        }
        Ok(())
    }
}

// pub struct S3Client {
//     pub store: AmazonS3,
//     pub bucket: String,
//     pub prefix: String,
//     pub concurrent_uploads: Option<usize>,
// }
//
// impl S3Client {
//     pub async fn new(s3_url: Url, concurrent_uploads: Option<usize>) -> anyhow::Result<Self> {
//         let s3_bucket = s3_url.domain().expect("No bucket name given");
//         let url_path = s3_url.path();
//         let s3_prefix = &url_path[1..];
//         let trimmed_prefix = s3_prefix.strip_suffix('/').unwrap_or(s3_prefix);
//
//         let bucket_region =
//             std::env::var("AWS_BUCKET_REGION").context("No AWS_BUCKET_REGION variable set")?;
//
//         let aws_config = aws_config::defaults(BehaviorVersion::latest())
//             .region(Region::new(bucket_region.to_string()))
//             .load()
//             .await;
//
//         let credential_provider = aws_config
//             .credentials_provider()
//             .context("Error loading AWS credentials")?;
//         let auth = Arc::new(AWSAuthentication {
//             credential_provider,
//         });
//
//         let store = AmazonS3Builder::new()
//             .with_region(bucket_region)
//             .with_bucket_name(s3_bucket)
//             .with_credentials(auth)
//             .build()
//             .unwrap();
//
//         Ok(S3Client {
//             store,
//             bucket: s3_bucket.to_string(),
//             prefix: trimmed_prefix.to_string(),
//             concurrent_uploads,
//         })
//     }
// }
//
// #[async_trait]
// impl StorageBackend for S3Client {
//     async fn create_streaming_upload(
//         &self,
//         file_name: &str,
//         compression: Compression,
//     ) -> anyhow::Result<(String, WriteMultipart)> {

//     }

// }
//
// #[derive(Debug)]
// pub struct AWSAuthentication {
//     credential_provider: SharedCredentialsProvider,
// }
//
// #[async_trait]
// impl CredentialProvider for AWSAuthentication {
//     type Credential = AwsCredential;
//
//     async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
//         let creds = self
//             .credential_provider
//             .provide_credentials()
//             .await
//             .map_err(|e| object_store::Error::Generic {
//                 store: "s3",
//                 source: Box::new(e),
//             })?;
//         Ok(Arc::from(AwsCredential {
//             key_id: creds.access_key_id().to_string(),
//             secret_key: creds.secret_access_key().to_string(),
//             token: creds.session_token().map(|t| t.to_string()),
//         }))
//     }
// }
