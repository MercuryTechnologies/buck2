/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! S3-based log storage, as an alternative to Manifold for OSS deployments.
//!
//! This module provides a drop-in replacement for `ManifoldClient` that uploads
//! logs to Amazon S3 instead of Meta's internal Manifold service.
//!
//! # Configuration
//!
//! Set the following environment variables:
//! - `BUCK2_LOG_BUCKET_NAME`: S3 bucket name (required for uploads to work)
//! - Standard AWS credentials (via AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, IAM role,
//!   or ~/.aws/credentials). Region is determined from standard AWS config.
//!
//! # S3 Object Lifecycle
//!
//! TTL is implemented via S3 object expiration tags. Configure your bucket with
//! a lifecycle rule that expires objects based on the `expiration-days` tag:
//!
//! ```json
//! {
//!     "Rules": [{
//!         "ID": "ExpireByTag",
//!         "Status": "Enabled",
//!         "Filter": { "Tag": { "Key": "managed-by", "Value": "buck2" } },
//!         "Expiration": { "ExpiredObjectDeleteMarker": true },
//!         "NoncurrentVersionExpiration": { "NoncurrentDays": 1 }
//!     }]
//! }
//! ```
//!
//! Or use S3 Intelligent-Tiering with archive policies.

use std::collections::HashMap;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::types::CompletedPart;
use aws_sdk_s3::Client as AwsS3Client;
use buck2_core::buck2_env;
use buck2_fs::paths::abs_path::AbsPath;
use bytes::Bytes;
use dupe::Dupe;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::chunked_uploader::ChunkedUploader;

/// TTL configuration for uploaded objects.
#[derive(Copy, Clone, Dupe)]
pub struct Ttl {
    duration: Duration,
}

impl Ttl {
    pub fn from_secs(ttl: u64) -> Self {
        Self {
            duration: Duration::from_secs(ttl),
        }
    }

    pub fn from_days(days: u64) -> Self {
        let secs = days * 24 * 60 * 60;
        Self {
            duration: Duration::from_secs(secs),
        }
    }

    pub fn as_secs(&self) -> u64 {
        self.duration.as_secs()
    }

    pub fn as_days(&self) -> u64 {
        self.duration.as_secs() / 86400
    }
}

impl Default for Ttl {
    fn default() -> Self {
        Self::from_secs(164 * 86_400) // 164 days
    }
}

/// Bucket configuration, mapping logical bucket names to S3 paths.
#[derive(Clone, Copy)]
pub struct Bucket {
    pub name: &'static str,
}

impl Bucket {
    pub const EVENT_LOGS: Bucket = Bucket {
        name: "buck2_logs",
    };

    pub const RAGE_DUMPS: Bucket = Bucket {
        name: "buck2_rage_dumps",
    };

    pub const RE_LOGS: Bucket = Bucket {
        name: "buck2_re_logs",
    };

    pub const INSTALLER_LOGS: Bucket = Bucket {
        name: "buck2_installer_logs",
    };
}

#[derive(Debug, buck2_error::Error)]
#[buck2(tag = Environment)]
pub enum S3Error {
    #[error("S3 bucket name not configured (set BUCK2_LOG_BUCKET_NAME)")]
    BucketNotConfigured,

    #[error("Failed to create S3 multipart upload for `{path}`: {source}")]
    CreateMultipartUpload { path: String, source: String },

    #[error("Failed to upload part {part_number} for `{path}`: {source}")]
    UploadPart {
        path: String,
        part_number: i32,
        source: String,
    },

    #[error("Failed to complete multipart upload for `{path}`: {source}")]
    CompleteMultipartUpload { path: String, source: String },

    #[error("Failed to abort multipart upload for `{path}`: {source}")]
    AbortMultipartUpload { path: String, source: String },

    #[error("Failed to put object `{path}`: {source}")]
    PutObject { path: String, source: String },

    #[error("Failed to get object `{path}`: {source}")]
    GetObject { path: String, source: String },

    #[error("Failed to write downloaded object to `{path}`: {source}")]
    WriteDownload { path: String, source: String },

    #[error("Multipart upload not started - call write() first")]
    MultipartNotStarted,

    #[error("Multipart upload already completed or aborted")]
    MultipartAlreadyFinished,

    #[error("Missing ETag in upload part response for part {0}")]
    MissingETag(i32),
}

/// S3 client for uploading build logs.
///
/// This is a drop-in replacement for `ManifoldClient` that uses S3 instead.
pub struct S3Client {
    client: AwsS3Client,
    bucket_name: Option<String>,
}

impl S3Client {
    /// Create a new S3 client.
    ///
    /// Returns a client that will silently skip uploads if `BUCK2_LOG_BUCKET_NAME`
    /// is not set, matching the behavior of `ManifoldClient` in OSS builds.
    pub async fn new() -> buck2_error::Result<Self> {
        let bucket_name = buck2_env!("BUCK2_LOG_BUCKET_NAME", applicability = internal)?;
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let client = AwsS3Client::new(&config);

        Ok(Self {
            client,
            bucket_name: bucket_name.map(|s| s.to_owned()),
        })
    }

    /// Check if uploads are enabled.
    pub fn is_enabled(&self) -> bool {
        self.bucket_name.is_some()
    }

    fn bucket(&self) -> Option<&str> {
        self.bucket_name.as_deref()
    }

    fn make_key(&self, bucket: Bucket, path: &str) -> String {
        format!("{}/{}", bucket.name, path)
    }

    fn make_tags(&self, ttl: Ttl) -> String {
        // S3 object tagging for lifecycle rules
        let expiration_days = ttl.as_days();
        format!("managed-by=buck2&expiration-days={}", expiration_days)
    }

    fn make_metadata(&self, ttl: Ttl) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let expires_at = now.as_secs() + ttl.as_secs();
        metadata.insert("expires-at".to_owned(), expires_at.to_string());
        metadata.insert("uploaded-by".to_owned(), "buck2".to_owned());

        // Add hostname for debugging (like Manifold's worker field)
        if let Ok(hostname) = hostname::get() {
            metadata.insert(
                "uploader-host".to_owned(),
                hostname.to_string_lossy().into_owned(),
            );
        }
        metadata
    }

    /// Write a complete object to S3.
    ///
    /// For small objects that don't need streaming/chunked upload.
    pub async fn write(
        &self,
        bucket: Bucket,
        path: &str,
        buf: Bytes,
        ttl: Ttl,
    ) -> buck2_error::Result<()> {
        let bucket_name = match self.bucket() {
            None => return Ok(()),
            Some(b) => b,
        };

        let key = self.make_key(bucket, path);

        self.client
            .put_object()
            .bucket(bucket_name)
            .key(&key)
            .body(ByteStream::from(buf))
            .tagging(self.make_tags(ttl))
            .set_metadata(Some(self.make_metadata(ttl)))
            .send()
            .await
            .map_err(|e| S3Error::PutObject {
                path: key,
                source: e.to_string(),
            })?;

        Ok(())
    }
    /// Start a chunked upload session using S3 multipart upload.
    ///
    /// Unlike the Manifold version, you MUST call `finish()` on the returned
    /// uploader when done, or `abort()` to cancel.
    pub async fn start_chunked_upload(
        &self,
        bucket: Bucket,
        path: &str,
        ttl: Ttl,
    ) -> buck2_error::Result<S3ChunkedUploader> {
        let bucket_name = match self.bucket() {
            None => {
                return Ok(S3ChunkedUploader {
                    client: self.client.clone(),
                    bucket_name: None,
                    key: String::new(),
                    upload_id: None,
                    parts: Vec::new(),
                    part_number: 1,
                    position: 0,
                    ttl,
                    state: MultipartState::Disabled,
                });
            }
            Some(b) => b.to_owned(),
        };

        let key = self.make_key(bucket, path);

        Ok(S3ChunkedUploader {
            client: self.client.clone(),
            bucket_name: Some(bucket_name),
            key,
            upload_id: None,
            parts: Vec::new(),
            part_number: 1,
            position: 0,
            ttl,
            state: MultipartState::NotStarted,
        })
    }

    /// Download an object from S3 to a local file.
    ///
    /// Downloads the object at `bucket/path` to `local_path`.
    pub async fn download(
        &self,
        bucket: Bucket,
        path: &str,
        local_path: &AbsPath,
    ) -> buck2_error::Result<()> {
        let bucket_name = match self.bucket() {
            None => {
                return Err(S3Error::BucketNotConfigured.into());
            }
            Some(b) => b,
        };

        let key = self.make_key(bucket, path);

        let resp = self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(&key)
            .send()
            .await
            .map_err(|e| S3Error::GetObject {
                path: key.clone(),
                source: e.to_string(),
            })?;

        let path_str = local_path.display().to_string();

        let mut file = File::create(local_path).await.map_err(|e| S3Error::WriteDownload {
            path: path_str.clone(),
            source: e.to_string(),
        })?;

        let mut stream = resp.body.into_async_read();
        tokio::io::copy(&mut stream, &mut file)
            .await
            .map_err(|e| S3Error::WriteDownload {
                path: path_str.clone(),
                source: e.to_string(),
            })?;

        file.flush().await.map_err(|e| S3Error::WriteDownload {
            path: path_str,
            source: e.to_string(),
        })?;

        Ok(())
    }

    /// Create an S3Client with a specific bucket name (for downloads).
    ///
    /// Unlike `new()` which reads from environment, this allows specifying
    /// the bucket directly (useful when bucket is known from config).
    pub async fn with_bucket(bucket_name: String) -> buck2_error::Result<Self> {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let client = AwsS3Client::new(&config);

        Ok(Self {
            client,
            bucket_name: Some(bucket_name),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MultipartState {
    /// Uploads are disabled (no bucket configured)
    Disabled,
    /// Multipart upload not yet initiated
    NotStarted,
    /// Multipart upload is active
    InProgress,
    /// Multipart upload has been completed or aborted
    Finished,
}

/// Chunked uploader using S3 multipart upload.
///
/// # Important
///
/// You MUST call either `finish()` or `abort()` when done uploading.
/// Dropping without finishing will leave an incomplete multipart upload
/// in S3, which will incur storage costs until it expires or is manually
/// cleaned up.
///
/// # Example
///
/// ```ignore
/// let mut uploader = client.start_chunked_upload(bucket, path, ttl).await?;
/// uploader.write(chunk1).await?;
/// uploader.write(chunk2).await?;
/// uploader.finish().await?;
/// ```
pub struct S3ChunkedUploader {
    client: AwsS3Client,
    bucket_name: Option<String>,
    key: String,
    upload_id: Option<String>,
    parts: Vec<CompletedPart>,
    part_number: i32,
    position: u64,
    ttl: Ttl,
    state: MultipartState,
}

impl S3ChunkedUploader {
    /// Write a chunk of data (internal implementation).
    ///
    /// The first call initiates the multipart upload. Subsequent calls
    /// upload additional parts.
    ///
    /// # Note on part sizes
    ///
    /// S3 requires parts to be at least 5MB (except for the last part).
    /// If you're uploading smaller chunks, consider buffering them or
    /// using `S3Client::write()` for small objects instead.
    async fn write_impl(&mut self, chunk: Bytes) -> buck2_error::Result<()> {
        let bucket_name = match &self.bucket_name {
            None => return Ok(()),
            Some(b) => b.clone(),
        };

        if self.state == MultipartState::Finished {
            return Err(S3Error::MultipartAlreadyFinished.into());
        }

        if chunk.is_empty() {
            return Ok(());
        }

        // Lazily initiate multipart upload on first write
        if self.state == MultipartState::NotStarted {
            let create_resp = self
                .client
                .create_multipart_upload()
                .bucket(&bucket_name)
                .key(&self.key)
                .tagging(format!(
                    "managed-by=buck2&expiration-days={}",
                    self.ttl.as_days()
                ))
                .send()
                .await
                .map_err(|e| S3Error::CreateMultipartUpload {
                    path: self.key.clone(),
                    source: e.to_string(),
                })?;

            self.upload_id = create_resp.upload_id().map(|s| s.to_owned());
            self.state = MultipartState::InProgress;
        }

        let upload_id = self
            .upload_id
            .as_ref()
            .ok_or(S3Error::MultipartNotStarted)?;

        let chunk_len = chunk.len() as u64;

        let upload_resp = self
            .client
            .upload_part()
            .bucket(&bucket_name)
            .key(&self.key)
            .upload_id(upload_id)
            .part_number(self.part_number)
            .body(ByteStream::from(chunk))
            .send()
            .await
            .map_err(|e| S3Error::UploadPart {
                path: self.key.clone(),
                part_number: self.part_number,
                source: e.to_string(),
            })?;

        let etag = upload_resp
            .e_tag()
            .ok_or(S3Error::MissingETag(self.part_number))?;

        self.parts.push(
            CompletedPart::builder()
                .e_tag(etag)
                .part_number(self.part_number)
                .build(),
        );

        self.part_number += 1;
        self.position += chunk_len;

        Ok(())
    }

    /// Complete the multipart upload (internal implementation).
    ///
    /// This MUST be called after all chunks have been written.
    /// Failing to call this will leave an incomplete upload in S3.
    async fn finish_impl(&mut self) -> buck2_error::Result<()> {
        let bucket_name = match &self.bucket_name {
            None => return Ok(()),
            Some(b) => b.clone(),
        };

        match self.state {
            MultipartState::Disabled => return Ok(()),
            MultipartState::NotStarted => {
                // Nothing was written, nothing to complete
                self.state = MultipartState::Finished;
                return Ok(());
            }
            MultipartState::Finished => {
                return Err(S3Error::MultipartAlreadyFinished.into());
            }
            MultipartState::InProgress => {}
        }

        let upload_id = self
            .upload_id
            .as_ref()
            .ok_or(S3Error::MultipartNotStarted)?;

        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(std::mem::take(&mut self.parts)))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(&bucket_name)
            .key(&self.key)
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await
            .map_err(|e| S3Error::CompleteMultipartUpload {
                path: self.key.clone(),
                source: e.to_string(),
            })?;

        self.state = MultipartState::Finished;
        Ok(())
    }

    /// Abort the multipart upload.
    ///
    /// Call this if you need to cancel an in-progress upload.
    /// This cleans up any parts that have been uploaded.
    pub async fn abort(&mut self) -> buck2_error::Result<()> {
        let bucket_name = match &self.bucket_name {
            None => return Ok(()),
            Some(b) => b.clone(),
        };

        if self.state != MultipartState::InProgress {
            return Ok(());
        }

        let upload_id = match &self.upload_id {
            None => return Ok(()),
            Some(id) => id.clone(),
        };

        self.client
            .abort_multipart_upload()
            .bucket(&bucket_name)
            .key(&self.key)
            .upload_id(&upload_id)
            .send()
            .await
            .map_err(|e| S3Error::AbortMultipartUpload {
                path: self.key.clone(),
                source: e.to_string(),
            })?;

        self.state = MultipartState::Finished;
        Ok(())
    }
}

#[async_trait]
impl ChunkedUploader for S3ChunkedUploader {
    async fn write(&mut self, chunk: Bytes) -> buck2_error::Result<()> {
        self.write_impl(chunk).await
    }

    fn position(&self) -> u64 {
        self.position
    }

    async fn finish(&mut self) -> buck2_error::Result<()> {
        self.finish_impl().await
    }
}

/// Warn on drop if multipart upload wasn't finished.
///
/// This is a safety net - callers should always call finish() or abort().
impl Drop for S3ChunkedUploader {
    fn drop(&mut self) {
        if self.state == MultipartState::InProgress {
            tracing::warn!(
                "S3ChunkedUploader dropped without calling finish() or abort() for key `{}`. \
                 This leaves an incomplete multipart upload in S3 that will incur storage costs. \
                 Use `aws s3api list-multipart-uploads` to find and clean up orphaned uploads.",
                self.key
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_days_to_secs() {
        assert_eq!(Ttl::from_days(1).duration.as_secs(), 86400);
        assert_eq!(Ttl::from_days(3).duration.as_secs(), 86400 * 3);
    }

    #[test]
    fn test_make_key() {
        // Can't fully test without AWS credentials, but we can test key generation
        let key = format!("{}/{}", Bucket::EVENT_LOGS.name, "test/path");
        assert_eq!(key, "buck2_logs/test/path");
    }
}
