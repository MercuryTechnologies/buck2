/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Trait for chunked uploaders that can stream data to a remote storage backend.

use async_trait::async_trait;
use bytes::Bytes;

/// Trait for chunked uploaders that can stream data to a remote storage backend.
///
/// This trait abstracts over the differences between Manifold (append-based, implicit
/// completion) and S3 (multipart upload, explicit completion).
///
/// # Contract
///
/// - `write()` can be called multiple times to upload chunks of data
/// - `finish()` MUST be called after all writes to ensure data is persisted
/// - `position()` returns the total bytes written so far
///
/// # Example
///
/// ```ignore
/// let mut uploader = client.start_chunked_upload(bucket, path, ttl).await?;
/// uploader.write(chunk1.into()).await?;
/// uploader.write(chunk2.into()).await?;
/// uploader.finish().await?;
/// ```
#[async_trait]
pub trait ChunkedUploader: Send {
    /// Write a chunk of data.
    async fn write(&mut self, chunk: Bytes) -> buck2_error::Result<()>;

    /// Get the current upload position (total bytes written).
    fn position(&self) -> u64;

    /// Complete the upload.
    ///
    /// This MUST be called after all chunks have been written. For Manifold this
    /// is a no-op (appends are immediately persisted), but for S3 this completes
    /// the multipart upload.
    async fn finish(&mut self) -> buck2_error::Result<()>;
}
