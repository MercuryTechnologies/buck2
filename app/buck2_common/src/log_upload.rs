/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Unified log upload abstraction that works with both Manifold (Meta internal)
//! and S3 (OSS deployments).
//!
//! # Usage
//!
//! For most use cases, use `LogUploadClient` and `ChunkedUploader`:
//!
//! ```ignore
//! use buck2_common::log_upload::{LogUploadClient, ChunkedUploader, Bucket, Ttl};
//!
//! let client = LogUploadClient::new().await?;
//! let mut uploader = client.start_chunked_upload(Bucket::EVENT_LOGS, "path", ttl).await?;
//! uploader.write(chunk).await?;
//! uploader.finish().await?;
//! ```
//!
//! # Backend Selection
//!
//! - **fbcode builds**: Uses Manifold (Meta's internal blob storage)
//! - **OSS builds**: Uses S3 if `BUCK2_LOG_BUCKET_NAME` is set, otherwise no-op

#[cfg(fbcode_build)]
use std::sync::Arc;

// Re-export common types
pub use crate::chunked_uploader::ChunkedUploader;
pub use crate::manifold::Bucket;
pub use crate::manifold::Ttl;

/// Unified client for log uploads.
///
/// This provides a common interface that works with both Manifold (fbcode)
/// and S3 (OSS). The backend is selected at compile time.
pub struct LogUploadClient {
    #[cfg(fbcode_build)]
    inner: Arc<crate::manifold::ManifoldClient>,
    #[cfg(not(fbcode_build))]
    inner: crate::s3::S3Client,
}

impl LogUploadClient {
    /// Create a new client.
    ///
    /// For fbcode builds, this creates a Manifold client.
    /// For OSS builds, this creates an S3 client (which requires
    /// `BUCK2_LOG_BUCKET_NAME` to be set for uploads to work).
    pub async fn new() -> buck2_error::Result<Self> {
        Ok(Self {
            #[cfg(fbcode_build)]
            inner: Arc::new(crate::manifold::ManifoldClient::new().await?),
            #[cfg(not(fbcode_build))]
            inner: crate::s3::S3Client::new().await?,
        })
    }

    /// Check if uploads are enabled.
    pub fn is_enabled(&self) -> bool {
        #[cfg(fbcode_build)]
        {
            true // Manifold is always available in fbcode
        }
        #[cfg(not(fbcode_build))]
        {
            self.inner.is_enabled()
        }
    }

    /// Start a chunked upload session.
    ///
    /// Returns a boxed `ChunkedUploader` that can be used to stream data.
    /// You MUST call `finish()` on the uploader when done.
    pub async fn start_chunked_upload(
        &self,
        bucket: Bucket,
        path: &str,
        ttl: Ttl,
    ) -> buck2_error::Result<Box<dyn ChunkedUploader>> {
        #[cfg(fbcode_build)]
        {
            Ok(Box::new(self.inner.start_chunked_upload(bucket, path, ttl)))
        }
        #[cfg(not(fbcode_build))]
        {
            let s3_bucket = crate::s3::Bucket { name: bucket.name };
            let s3_ttl = crate::s3::Ttl::from_secs(ttl.as_secs());
            Ok(Box::new(
                self.inner
                    .start_chunked_upload(s3_bucket, path, s3_ttl)
                    .await?,
            ))
        }
    }
}
