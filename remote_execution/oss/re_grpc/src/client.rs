/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::collections::HashMap;
use std::collections::HashSet;
use std::env::VarError;
use std::io;
use std::io::Cursor;
use std::io::SeekFrom;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use async_compression::tokio::bufread::BrotliDecoder;
use async_compression::tokio::bufread::BrotliEncoder;
use async_compression::tokio::bufread::DeflateDecoder;
use async_compression::tokio::bufread::DeflateEncoder;
use async_compression::tokio::bufread::ZstdDecoder;
use async_compression::tokio::bufread::ZstdEncoder;
use buck2_credential_helper::CredentialHelper;
use buck2_credential_helper::CredentialHelperSettings;
use buck2_credential_helper::Credentials;
use buck2_events::dispatch::span_async;
use buck2_re_configuration::Buck2OssReConfiguration;
use buck2_re_configuration::CASdMode;
use buck2_re_configuration::CopyPolicy;
use buck2_re_configuration::HttpHeader;
use dupe::Dupe;
use futures::Stream;
use futures::future::BoxFuture;
use futures::future::Future;
use futures::future::FutureExt;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::stream::TryStreamExt;
use gazebo::prelude::*;
use hyper_util::client::legacy::connect::HttpConnector;
use lru::LruCache;
use prost::Message;
use re_grpc_proto::build::bazel::remote::execution::v2::Action;
use re_grpc_proto::build::bazel::remote::execution::v2::ActionResult;
use re_grpc_proto::build::bazel::remote::execution::v2::BatchReadBlobsRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::BatchReadBlobsResponse;
use re_grpc_proto::build::bazel::remote::execution::v2::BatchUpdateBlobsRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::BatchUpdateBlobsResponse;
use re_grpc_proto::build::bazel::remote::execution::v2::CacheCapabilities;
use re_grpc_proto::build::bazel::remote::execution::v2::Digest;
use re_grpc_proto::build::bazel::remote::execution::v2::ExecuteOperationMetadata;
use re_grpc_proto::build::bazel::remote::execution::v2::ExecuteRequest as GExecuteRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::ExecuteResponse as GExecuteResponse;
use re_grpc_proto::build::bazel::remote::execution::v2::ExecutedActionMetadata;
use re_grpc_proto::build::bazel::remote::execution::v2::ExecutionCapabilities;
use re_grpc_proto::build::bazel::remote::execution::v2::ExecutionPolicy;
use re_grpc_proto::build::bazel::remote::execution::v2::FindMissingBlobsRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::FindMissingBlobsResponse;
use re_grpc_proto::build::bazel::remote::execution::v2::GetActionResultRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::GetCapabilitiesRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::OutputDirectory;
use re_grpc_proto::build::bazel::remote::execution::v2::OutputFile;
use re_grpc_proto::build::bazel::remote::execution::v2::OutputSymlink;
use re_grpc_proto::build::bazel::remote::execution::v2::PriorityCapabilities;
use re_grpc_proto::build::bazel::remote::execution::v2::RequestMetadata;
use re_grpc_proto::build::bazel::remote::execution::v2::ResultsCachePolicy;
use re_grpc_proto::build::bazel::remote::execution::v2::SpliceBlobRequest as GSpliceBlobRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::SpliceBlobResponse as GSpliceBlobResponse;
use re_grpc_proto::build::bazel::remote::execution::v2::SplitBlobRequest as GSplitBlobRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::SplitBlobResponse as GSplitBlobResponse;
use re_grpc_proto::build::bazel::remote::execution::v2::ToolDetails;
use re_grpc_proto::build::bazel::remote::execution::v2::UpdateActionResultRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::WaitExecutionRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::action_cache_client::ActionCacheClient;
use re_grpc_proto::build::bazel::remote::execution::v2::batch_update_blobs_request::Request;
use re_grpc_proto::build::bazel::remote::execution::v2::capabilities_client::CapabilitiesClient;
use re_grpc_proto::build::bazel::remote::execution::v2::chunking_function;
use re_grpc_proto::build::bazel::remote::execution::v2::compressor;
use re_grpc_proto::build::bazel::remote::execution::v2::content_addressable_storage_client::ContentAddressableStorageClient;
use re_grpc_proto::build::bazel::remote::execution::v2::digest_function;
use re_grpc_proto::build::bazel::remote::execution::v2::execution_client::ExecutionClient;
use re_grpc_proto::build::bazel::remote::execution::v2::execution_stage;
use re_grpc_proto::build::bazel::semver::SemVer;
use re_grpc_proto::google::bytestream::QueryWriteStatusRequest;
use re_grpc_proto::google::bytestream::ReadRequest;
use re_grpc_proto::google::bytestream::ReadResponse;
use re_grpc_proto::google::bytestream::WriteRequest;
use re_grpc_proto::google::bytestream::WriteResponse;
use re_grpc_proto::google::bytestream::byte_stream_client::ByteStreamClient;
use re_grpc_proto::google::longrunning::Operation;
use re_grpc_proto::google::longrunning::operation::Result as OpResult;
use re_grpc_proto::google::rpc::Code;
use re_grpc_proto::google::rpc::Status;
use regex::Regex;
use sha1::Sha1;
use sha2::Digest as _;
use sha2::Sha256;
use tokio::fs::OpenOptions;
use tokio::io::AsyncBufRead;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio_util::io::StreamReader;
use tonic::codegen::InterceptedService;
use tonic::metadata;
use tonic::metadata::MetadataKey;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::Certificate;
use tonic::transport::Channel;
use tonic::transport::Identity;
use tonic::transport::Uri;
use tonic::transport::channel::ClientTlsConfig;
use uuid::Uuid;

use crate::casd_autostart::CheckedConnector;
use crate::casd_autostart::DaemonAddress;
use crate::error::*;
use crate::metadata::*;
use crate::request::*;
use crate::response::*;
use crate::shared_cache::SharedCacheCounters;
use crate::shared_cache::SharedCasCache;
use crate::stats::CountingConnector;
use crate::unix_socket::UnixConnector;

const DEFAULT_MAX_TOTAL_BATCH_SIZE: usize = 4 * 1000 * 1000;
const DEFAULT_REMOTE_CACHE_COMPRESSION_THRESHOLD: usize = 100;
const DEFAULT_RETRIES: usize = 5;
const GRPC_RETRY_INITIAL_DELAY_MILLIS: u64 = 100;
const DEFAULT_RETRY_MAX_DELAY_MILLIS: u64 = 5000;
const GRPC_RETRY_JITTER: f64 = 0.1;
const DEFAULT_GRPC_KEEPALIVE_TIME_SECS: u64 = 60;
const DEFAULT_GRPC_KEEPALIVE_TIMEOUT_SECS: u64 = 20;
const DEFAULT_GRPC_KEEPALIVE_WHILE_IDLE: bool = false;
const DEFAULT_GRPC_REQUEST_TIMEOUT_SECS: u64 = 60;
const DEFAULT_BYTESTREAM_PROGRESS_TIMEOUT_SECS: u64 = 60;
/// BuildBuddy merges an Execute onto a pending execution of the same action digest for 10
/// minutes after that execution's Execute while no executor has claimed it (v2.310.0
/// enterprise/server/remote_execution/action_merger/action_merger.go:27, 288-294), so only an
/// Execute sent later than that starts a fresh execution.
const DEFAULT_QUEUED_OPERATION_TIMEOUT_SECS: u64 = 15 * 60;
/// Ten of the 60 s progress updates BuildBuddy's executor sends for a task it is running
/// (v2.310.0 enterprise/server/remote_execution/executor/executor.go:58), missed in a row.
const DEFAULT_STALLED_OPERATION_TIMEOUT_SECS: u64 = 10 * 60;
const DEFAULT_FAST_CDC_2020_AVG_CHUNK_SIZE: u64 = 512 * 1024;
// Match Bazel's default gRPC remote-execution fanout: roughly 100 requests per
// connection, with at most 100 connections unless explicitly overridden.
const DEFAULT_EXECUTION_CONCURRENCY_LIMIT: usize = 400;
const DEFAULT_ENGINE_REQUESTS_PER_CONNECTION: usize = 100;
const DEFAULT_MAX_ENGINE_CONNECTION_COUNT: usize = 100;
const DEFAULT_REQUEST_METADATA_TOOL_NAME: &str = "buck2";
const REQUEST_METADATA_HEADER: &str = "build.bazel.remote.execution.v2.requestmetadata-bin";

fn tdigest_to(tdigest: TDigest) -> Digest {
    Digest {
        hash: tdigest.hash,
        size_bytes: tdigest.size_in_bytes,
    }
}

fn tdigest_from(digest: Digest) -> TDigest {
    TDigest {
        hash: digest.hash,
        size_in_bytes: digest.size_bytes,
        ..Default::default()
    }
}

fn tstatus_ok() -> TStatus {
    TStatus {
        code: TCode::OK,
        message: "".to_owned(),
        ..Default::default()
    }
}

#[allow(clippy::large_enum_variant)]
enum BlobHashVerifier {
    Sha1(Sha1),
    Sha256(Sha256),
    Blake3(blake3::Hasher),
}

impl BlobHashVerifier {
    fn name(&self) -> &'static str {
        match self {
            Self::Sha1(_) => "SHA1",
            Self::Sha256(_) => "SHA256",
            Self::Blake3(_) => "BLAKE3",
        }
    }

    fn update(&mut self, data: &[u8]) {
        match self {
            Self::Sha1(hasher) => hasher.update(data),
            Self::Sha256(hasher) => hasher.update(data),
            Self::Blake3(hasher) => {
                hasher.update(data);
            }
        }
    }

    fn finalize_hex(self) -> String {
        match self {
            Self::Sha1(hasher) => format!("{:x}", hasher.finalize()),
            Self::Sha256(hasher) => format!("{:x}", hasher.finalize()),
            Self::Blake3(hasher) => blake3::Hasher::finalize(&hasher).to_hex().to_string(),
        }
    }
}

struct BlobHashValidators {
    expected_hash: String,
    verifiers: Vec<BlobHashVerifier>,
}

impl BlobHashValidators {
    fn new(
        expected_hash: &str,
        selected_digest_function: Option<digest_function::Value>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !expected_hash.is_empty(),
            "Digest hash is empty and cannot be validated"
        );
        anyhow::ensure!(
            expected_hash.bytes().all(|b| b.is_ascii_hexdigit()),
            "Digest hash contains non-hex characters: `{expected_hash}`"
        );

        let expected_hash = expected_hash.to_ascii_lowercase();
        let verifiers = if let Some(digest_function) = selected_digest_function {
            match digest_function {
                digest_function::Value::Sha1 => {
                    anyhow::ensure!(
                        expected_hash.len() == 40,
                        "Digest hash length mismatch for configured SHA1: `{expected_hash}`"
                    );
                    vec![BlobHashVerifier::Sha1(Sha1::new())]
                }
                digest_function::Value::Sha256 => {
                    anyhow::ensure!(
                        expected_hash.len() == 64,
                        "Digest hash length mismatch for configured SHA256: `{expected_hash}`"
                    );
                    vec![BlobHashVerifier::Sha256(Sha256::new())]
                }
                digest_function::Value::Blake3 => {
                    anyhow::ensure!(
                        expected_hash.len() == 64,
                        "Digest hash length mismatch for configured BLAKE3: `{expected_hash}`"
                    );
                    vec![BlobHashVerifier::Blake3(blake3::Hasher::new())]
                }
                _ => {
                    anyhow::bail!(
                        "Configured digest function {:?} is not supported for download hash validation",
                        digest_function
                    )
                }
            }
        } else {
            match expected_hash.len() {
                40 => vec![BlobHashVerifier::Sha1(Sha1::new())],
                // Could be either SHA256 or BLAKE3. Validate against both.
                64 => vec![
                    BlobHashVerifier::Sha256(Sha256::new()),
                    BlobHashVerifier::Blake3(blake3::Hasher::new()),
                ],
                n => {
                    anyhow::bail!(
                        "Unsupported digest hash length `{n}` for `{expected_hash}`; cannot validate downloaded blob hash"
                    )
                }
            }
        };

        Ok(Self {
            expected_hash,
            verifiers,
        })
    }

    fn update(&mut self, data: &[u8]) {
        for verifier in &mut self.verifiers {
            verifier.update(data);
        }
    }

    fn finish(self, digest: &TDigest) -> anyhow::Result<()> {
        let mut tried = Vec::with_capacity(self.verifiers.len());
        for verifier in self.verifiers {
            let name = verifier.name();
            tried.push(name);
            if verifier.finalize_hex() == self.expected_hash {
                return Ok(());
            }
        }
        anyhow::bail!(
            "Downloaded blob hash mismatch for `{digest}` after validating with [{}]",
            tried.join(", ")
        );
    }
}

fn validate_downloaded_blob_size(digest: &TDigest, actual_size: usize) -> anyhow::Result<()> {
    let expected_size = usize::try_from(digest.size_in_bytes)
        .with_context(|| format!("Invalid negative digest size for `{digest}`"))?;
    anyhow::ensure!(
        actual_size == expected_size,
        "Downloaded blob size mismatch for `{digest}`: expected {expected_size} bytes, got {actual_size} bytes"
    );
    Ok(())
}

fn validate_downloaded_blob_hash(
    digest: &TDigest,
    data: &[u8],
    selected_digest_function: Option<digest_function::Value>,
) -> anyhow::Result<()> {
    let mut validators = BlobHashValidators::new(&digest.hash, selected_digest_function)?;
    validators.update(data);
    validators.finish(digest)
}

fn validate_downloaded_blob(
    digest: &TDigest,
    data: &[u8],
    selected_digest_function: Option<digest_function::Value>,
) -> anyhow::Result<()> {
    validate_downloaded_blob_size(digest, data.len())?;
    validate_downloaded_blob_hash(digest, data, selected_digest_function)
}

fn should_validate_upload_hash(digest: &TDigest) -> bool {
    matches!(digest.hash.len(), 40 | 64) && digest.hash.bytes().all(|b| b.is_ascii_hexdigit())
}

fn validate_upload_blob(
    digest: &TDigest,
    data: &[u8],
    selected_digest_function: Option<digest_function::Value>,
) -> anyhow::Result<()> {
    validate_downloaded_blob_size(digest, data.len())?;
    if should_validate_upload_hash(digest) {
        validate_downloaded_blob_hash(digest, data, selected_digest_function)?;
    }
    Ok(())
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct RetryInfoDetail {
    #[prost(message, optional, tag = "1")]
    retry_delay: Option<::prost_types::Duration>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct PreconditionFailureDetail {
    #[prost(message, repeated, tag = "1")]
    violations: Vec<PreconditionFailureViolation>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct PreconditionFailureViolation {
    #[prost(string, tag = "1")]
    r#type: String,
    #[prost(string, tag = "2")]
    subject: String,
    #[prost(string, tag = "3")]
    description: String,
}

const RETRY_INFO_TYPE_URL: &str = "type.googleapis.com/google.rpc.RetryInfo";
const PRECONDITION_FAILURE_TYPE_URL: &str = "type.googleapis.com/google.rpc.PreconditionFailure";

fn check_status(status: Status) -> Result<(), REClientError> {
    if status.code == 0 {
        return Ok(());
    }

    Err(re_client_error_from_rpc_status(&status))
}

fn grpc_duration_to_duration(duration: &::prost_types::Duration) -> Option<Duration> {
    if duration.seconds < 0 || duration.nanos < 0 {
        return None;
    }

    Some(Duration::new(
        u64::try_from(duration.seconds).ok()?,
        u32::try_from(duration.nanos).ok()?,
    ))
}

fn retry_delay_from_rpc_status(status: &Status) -> Option<Duration> {
    status.details.iter().find_map(|detail| {
        if detail.type_url != RETRY_INFO_TYPE_URL {
            return None;
        }

        let retry_info = RetryInfoDetail::decode(detail.value.as_slice()).ok()?;
        retry_info
            .retry_delay
            .as_ref()
            .and_then(grpc_duration_to_duration)
    })
}

fn capped_retry_delay_from_rpc_status(
    status: &Status,
    retry_max_delay: Duration,
) -> Option<Duration> {
    retry_delay_from_rpc_status(status).map(|delay| std::cmp::min(delay, retry_max_delay))
}

async fn sleep_for_execute_retry_info(
    status: &Status,
    retry_max_delay: Duration,
    operation_name: Option<&str>,
) {
    let Some(delay) = capped_retry_delay_from_rpc_status(status, retry_max_delay) else {
        return;
    };

    tracing::debug!(
        operation_name = operation_name.unwrap_or(""),
        delay_ms = delay.as_millis(),
        "Delaying Execute retry per RetryInfo"
    );
    tokio::time::sleep(delay).await;
}

fn precondition_failures(status: &Status) -> impl Iterator<Item = PreconditionFailureDetail> + '_ {
    status.details.iter().filter_map(|detail| {
        if detail.type_url != PRECONDITION_FAILURE_TYPE_URL {
            return None;
        }

        PreconditionFailureDetail::decode(detail.value.as_slice()).ok()
    })
}

fn rpc_status_has_missing_precondition(status: &Status) -> bool {
    status.code == Code::FailedPrecondition as i32
        && precondition_failures(status).any(|failure| {
            !failure.violations.is_empty()
                && failure
                    .violations
                    .iter()
                    .all(|violation| violation.r#type.eq_ignore_ascii_case("MISSING"))
        })
}

fn message_indicates_quota(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("quota") || message.contains("rate limit")
}

fn group_for_rpc_status(status: &Status) -> TCodeReasonGroup {
    if status.code == Code::ResourceExhausted as i32 && message_indicates_quota(&status.message) {
        TCodeReasonGroup::USER_QUOTA
    } else {
        TCodeReasonGroup::UNKNOWN
    }
}

fn format_rpc_status_message(status: &Status) -> String {
    let mut details = Vec::new();
    if let Some(retry_delay) = retry_delay_from_rpc_status(status) {
        details.push(format!(
            "RetryInfo(retry_delay_ms={})",
            retry_delay.as_millis()
        ));
    }

    for failure in precondition_failures(status) {
        for violation in failure.violations {
            details.push(format!(
                "PreconditionFailure(type={}, subject={}, description={})",
                violation.r#type, violation.subject, violation.description
            ));
        }
    }

    for detail in &status.details {
        if detail.type_url != RETRY_INFO_TYPE_URL
            && detail.type_url != PRECONDITION_FAILURE_TYPE_URL
        {
            details.push(format!("detail_type={}", detail.type_url));
        }
    }

    if details.is_empty() {
        status.message.clone()
    } else if status.message.is_empty() {
        details.join("; ")
    } else {
        format!("{} ({})", status.message, details.join("; "))
    }
}

fn re_client_error_from_rpc_status(status: &Status) -> REClientError {
    REClientError {
        code: TCode(status.code),
        message: format_rpc_status_message(status),
        group: group_for_rpc_status(status),
    }
}

fn tcode_is_retryable(code: TCode) -> bool {
    matches!(
        code,
        TCode::UNKNOWN
            | TCode::DEADLINE_EXCEEDED
            | TCode::ABORTED
            | TCode::INTERNAL
            | TCode::UNAVAILABLE
            | TCode::RESOURCE_EXHAUSTED
    )
}

fn tcode_from_grpc_code(code: tonic::Code) -> TCode {
    match code {
        tonic::Code::Ok => TCode::OK,
        tonic::Code::Cancelled => TCode::CANCELLED,
        tonic::Code::Unknown => TCode::UNKNOWN,
        tonic::Code::InvalidArgument => TCode::INVALID_ARGUMENT,
        tonic::Code::DeadlineExceeded => TCode::DEADLINE_EXCEEDED,
        tonic::Code::NotFound => TCode::NOT_FOUND,
        tonic::Code::AlreadyExists => TCode::ALREADY_EXISTS,
        tonic::Code::PermissionDenied => TCode::PERMISSION_DENIED,
        tonic::Code::ResourceExhausted => TCode::RESOURCE_EXHAUSTED,
        tonic::Code::FailedPrecondition => TCode::FAILED_PRECONDITION,
        tonic::Code::Aborted => TCode::ABORTED,
        tonic::Code::OutOfRange => TCode::OUT_OF_RANGE,
        tonic::Code::Unimplemented => TCode::UNIMPLEMENTED,
        tonic::Code::Internal => TCode::INTERNAL,
        tonic::Code::Unavailable => TCode::UNAVAILABLE,
        tonic::Code::DataLoss => TCode::DATA_LOSS,
        tonic::Code::Unauthenticated => TCode::UNAUTHENTICATED,
    }
}

fn tonic_status_rpc_status(status: &tonic::Status) -> Option<Status> {
    if status.details().is_empty() {
        return None;
    }

    Status::decode(status.details()).ok()
}

/// A Status that the client's own connection produced as it went away, as opposed to one the
/// server sent, which has no source. hyper fails a request with Canceled when the connection it
/// was queued on closes under it (hyper 1.10.1 src/client/dispatch.rs:223) or is no longer ready
/// for it (src/client/conn/http2.rs:196), which tonic reports as CANCELLED (tonic 0.14.6
/// src/status.rs:440-442). A stream in flight when the connection closes with GOAWAY fails with
/// h2's error, which tonic reports by the GOAWAY's reason (src/status.rs:384-412), as
/// RESOURCE_EXHAUSTED for the ENHANCE_YOUR_CALM that h2 sends itself after too many resets.
fn tonic_status_lost_with_connection(status: &tonic::Status) -> bool {
    let mut source = std::error::Error::source(status);
    while let Some(error) = source {
        if error
            .downcast_ref::<hyper::Error>()
            .is_some_and(hyper::Error::is_canceled)
            || error
                .downcast_ref::<h2::Error>()
                .is_some_and(h2::Error::is_go_away)
        {
            return true;
        }
        source = error.source();
    }
    false
}

fn tonic_status_indicates_broken_connection(status: &tonic::Status) -> bool {
    if tonic_status_lost_with_connection(status) {
        return true;
    }

    if !matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::Unknown
            | tonic::Code::Internal
            | tonic::Code::DeadlineExceeded
    ) {
        return false;
    }

    let message = status.message().to_ascii_lowercase();
    message.contains("transport error")
        || message.contains("connection reset")
        || message.contains("connection refused")
        || message.contains("connection closed")
        || message.contains("connection error")
        || message.contains("broken pipe")
        || message.contains("goaway")
        || message.contains("http2")
        || message.contains("io error")
}

fn re_client_error_from_tonic_status(status: &tonic::Status) -> REClientError {
    let rpc_status = tonic_status_rpc_status(status);
    let mut message = status.message().to_owned();
    let mut group = if tonic_status_indicates_broken_connection(status) {
        TCodeReasonGroup::RE_CONNECTION
    } else {
        TCodeReasonGroup::UNKNOWN
    };

    if let Some(rpc_status) = &rpc_status {
        let rpc_group = group_for_rpc_status(rpc_status);
        if group == TCodeReasonGroup::UNKNOWN {
            group = rpc_group;
        }
        let rpc_message = format_rpc_status_message(rpc_status);
        if !rpc_message.is_empty() {
            message = rpc_message;
        }
    } else if status.code() == tonic::Code::ResourceExhausted && message_indicates_quota(&message) {
        group = TCodeReasonGroup::USER_QUOTA;
    }

    REClientError {
        code: tcode_from_grpc_code(status.code()),
        message,
        group,
    }
}

fn tonic_status_from_io_error(error: &io::Error) -> Option<&tonic::Status> {
    let source = error.get_ref()?;
    if let Some(status) = source.downcast_ref::<tonic::Status>() {
        return Some(status);
    }

    let mut current = source.source();
    while let Some(source) = current {
        if let Some(status) = source.downcast_ref::<tonic::Status>() {
            return Some(status);
        }
        current = source.source();
    }
    None
}

fn normalize_grpc_error(err: anyhow::Error) -> anyhow::Error {
    if err.downcast_ref::<REClientError>().is_some() {
        return err;
    }

    let re_client_error = err
        .downcast_ref::<tonic::Status>()
        .map(re_client_error_from_tonic_status)
        .or_else(|| {
            err.downcast_ref::<io::Error>()
                .and_then(tonic_status_from_io_error)
                .map(re_client_error_from_tonic_status)
        });
    match re_client_error {
        Some(re_client_error) => anyhow::Error::from(re_client_error),
        None => err,
    }
}

fn error_tcode(err: &anyhow::Error) -> Option<TCode> {
    err.downcast_ref::<REClientError>()
        .map(|status| status.code)
        .or_else(|| {
            err.downcast_ref::<tonic::Status>()
                .map(|status| tcode_from_grpc_code(status.code()))
        })
        .or_else(|| {
            err.downcast_ref::<io::Error>()
                .and_then(tonic_status_from_io_error)
                .map(|status| tcode_from_grpc_code(status.code()))
        })
}

fn grpc_error_retry_delay(err: &anyhow::Error) -> Option<Duration> {
    err.downcast_ref::<tonic::Status>()
        .and_then(tonic_status_rpc_status)
        .and_then(|status| retry_delay_from_rpc_status(&status))
        .or_else(|| {
            err.downcast_ref::<io::Error>()
                .and_then(tonic_status_from_io_error)
                .and_then(tonic_status_rpc_status)
                .and_then(|status| retry_delay_from_rpc_status(&status))
        })
}

fn is_broken_connection_error(err: &anyhow::Error) -> bool {
    if err
        .downcast_ref::<REClientError>()
        .is_some_and(|error| error.group == TCodeReasonGroup::RE_CONNECTION)
    {
        return true;
    }

    if err
        .downcast_ref::<tonic::Status>()
        .is_some_and(tonic_status_indicates_broken_connection)
    {
        return true;
    }

    if let Some(io_error) = err.downcast_ref::<io::Error>() {
        if matches!(
            io_error.kind(),
            io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::TimedOut
        ) {
            return true;
        }

        if tonic_status_from_io_error(io_error)
            .is_some_and(tonic_status_indicates_broken_connection)
        {
            return true;
        }
    }

    // The CANCELLED the connection produces was accepted above, by its source. A CANCELLED the
    // server sends is its answer, whatever its message says.
    if error_tcode(err) == Some(TCode::CANCELLED) {
        return false;
    }

    let message = format!("{err:#}").to_ascii_lowercase();
    message.contains("connection reset")
        || message.contains("connection refused")
        || message.contains("connection closed")
        || message.contains("broken pipe")
        || message.contains("transport error")
}

/// CANCELLED is retried only when it came from the connection: buck2 cancelling a request drops
/// its future, which leaves no Status to look at, and a CANCELLED from the server is its answer.
fn is_retryable_grpc_error(err: &anyhow::Error) -> bool {
    grpc_error_retry_delay(err).is_some()
        || error_tcode(err).is_some_and(tcode_is_retryable)
        || (error_tcode(err) == Some(TCode::CANCELLED) && is_broken_connection_error(err))
}

fn is_operation_not_found(err: &anyhow::Error) -> bool {
    error_tcode(err) == Some(TCode::NOT_FOUND)
}

/// WaitExecution has used its own retries by now. NOT_FOUND says the server lost the operation,
/// and a connection that keeps breaking may have taken the operation's stream with it. Execute
/// with the same action digest returns the result if the first run finished, or joins it if it is
/// still running. Any other error is the server's answer about the operation.
fn should_retry_execute_after_wait_execution_error(err: &anyhow::Error) -> bool {
    is_operation_not_found(err) || is_broken_connection_error(err)
}

fn should_retry_execute_after_operation_stream_error(err: &anyhow::Error) -> bool {
    is_operation_not_found(err)
}

fn should_retry_execute_after_operation_error(status: &Status) -> bool {
    if rpc_status_has_missing_precondition(status) {
        return false;
    }
    retry_delay_from_rpc_status(status).is_some() || tcode_is_retryable(TCode(status.code))
}

fn should_retry_execute_after_execute_response_status(status: &Status) -> bool {
    if rpc_status_has_missing_precondition(status) {
        return false;
    }
    retry_delay_from_rpc_status(status).is_some()
        || (TCode(status.code) != TCode::DEADLINE_EXCEEDED
            && tcode_is_retryable(TCode(status.code)))
}

fn can_retry_execute(retry_attempts: usize, retries: usize) -> bool {
    retry_attempts < retries
}

/// A number in [0, 1].
fn random_unit() -> f64 {
    let random_bytes = Uuid::new_v4().into_bytes();
    u16::from_be_bytes([random_bytes[0], random_bytes[1]]) as f64 / u16::MAX as f64
}

fn jittered_retry_delay(base_delay: Duration) -> Duration {
    let jitter_ratio = GRPC_RETRY_JITTER * ((2.0 * random_unit()) - 1.0);
    Duration::try_from_secs_f64(base_delay.as_secs_f64() * (1.0 + jitter_ratio))
        .unwrap_or(base_delay)
}

async fn retry_grpc_request<T, Fut, F>(
    retries: usize,
    retry_max_delay: Duration,
    request: F,
) -> anyhow::Result<T>
where
    Fut: Future<Output = anyhow::Result<T>>,
    F: FnMut() -> Fut,
{
    retry_grpc_request_with_recovery(retries, retry_max_delay, request, |_| async { false }).await
}

/// What a failed attempt asks of the client before the next one.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Recovery {
    None,
    Reconnect,
    RefreshCredentials,
}

fn recovery_for_error(err: &anyhow::Error) -> Recovery {
    // Before the broken-connection test: a proxy's 401 arrives as INTERNAL, which that test
    // would otherwise be the one to look at.
    if error_rejects_credentials(err) {
        Recovery::RefreshCredentials
    } else if is_broken_connection_error(err) {
        Recovery::Reconnect
    } else {
        Recovery::None
    }
}

fn error_rejects_credentials(err: &anyhow::Error) -> bool {
    error_tcode(err) == Some(TCode::UNAUTHENTICATED)
        || err
            .downcast_ref::<tonic::Status>()
            .is_some_and(buck2_credential_helper::status_rejects_credentials)
        || err
            .downcast_ref::<io::Error>()
            .and_then(tonic_status_from_io_error)
            .is_some_and(buck2_credential_helper::status_rejects_credentials)
}

/// tonic ends a request that outlives its `grpc-timeout` on the client's side with
/// CANCELLED "Timeout expired" (tonic 0.14.6 transport/service/grpc_timeout.rs, status.rs
/// `TimeoutExpired`): the server never answered, which for a read says nothing about the answer.
fn is_client_timeout(err: &anyhow::Error) -> bool {
    let timed_out = |status: &tonic::Status| {
        status.code() == tonic::Code::Cancelled && status.message() == "Timeout expired"
    };
    err.downcast_ref::<tonic::Status>().is_some_and(timed_out)
        || err
            .downcast_ref::<io::Error>()
            .and_then(tonic_status_from_io_error)
            .is_some_and(timed_out)
}

/// A read that timed out on the client's side, after its retries, as the DEADLINE_EXCEEDED it
/// amounts to, so a caller that skips an unavailable cache treats it as one (buck2_execute_impl
/// executors/action_cache.rs, `is_remote_cache_unavailable`).
fn client_timeout_as_deadline_exceeded(err: anyhow::Error) -> anyhow::Error {
    if !is_client_timeout(&err) {
        return err;
    }
    anyhow::Error::from(REClientError {
        code: TCode::DEADLINE_EXCEEDED,
        message: format!("the request timed out on the client's side: {err:#}"),
        group: TCodeReasonGroup::UNKNOWN,
    })
}

/// `recover` is asked to reconnect after a broken connection, as before, and to refresh
/// credentials after UNAUTHENTICATED. A refresh it confirms is followed by one attempt at once,
/// outside the retry budget and the code's retry policy, and only once per call: a remote that
/// rejects the refreshed credentials too fails the call instead of running the helper in a loop.
async fn retry_grpc_request_with_recovery<T, Fut, F, RFut, R>(
    retries: usize,
    retry_max_delay: Duration,
    request: F,
    recover: R,
) -> anyhow::Result<T>
where
    Fut: Future<Output = anyhow::Result<T>>,
    F: FnMut() -> Fut,
    RFut: Future<Output = bool>,
    R: FnMut(Recovery) -> RFut,
{
    retry_grpc_request_with_policy(retries, retry_max_delay, request, recover, false).await
}

/// `retry_client_timeouts` is for idempotent reads only. An Execute that timed out on the
/// client's side may still be running, and executing it again would run it twice.
async fn retry_grpc_request_with_policy<T, Fut, F, RFut, R>(
    retries: usize,
    retry_max_delay: Duration,
    mut request: F,
    mut recover: R,
    retry_client_timeouts: bool,
) -> anyhow::Result<T>
where
    Fut: Future<Output = anyhow::Result<T>>,
    F: FnMut() -> Fut,
    RFut: Future<Output = bool>,
    R: FnMut(Recovery) -> RFut,
{
    let mut retry_attempt = 0usize;
    let mut next_delay = Duration::from_millis(GRPC_RETRY_INITIAL_DELAY_MILLIS);
    let mut credentials_refreshed = false;

    loop {
        match request().await {
            Ok(response) => return Ok(response),
            Err(err) => {
                let recovery = recovery_for_error(&err);
                if recovery == Recovery::RefreshCredentials
                    && !credentials_refreshed
                    && recover(recovery).await
                {
                    credentials_refreshed = true;
                    continue;
                }

                // Refused credentials that could not be refreshed would be refused again: a proxy's
                // 401 reads as INTERNAL, which the retry policy would otherwise repeat.
                let client_timeout = retry_client_timeouts && is_client_timeout(&err);
                if retry_attempt >= retries
                    || !(is_retryable_grpc_error(&err) || client_timeout)
                    || recovery == Recovery::RefreshCredentials
                {
                    if client_timeout {
                        return Err(client_timeout_as_deadline_exceeded(err));
                    }
                    return Err(normalize_grpc_error(err));
                }

                // A read that timed out is treated as a broken connection too. On 2026-10-03 one
                // HTTP/2 connection to the action cache answered nothing for 96 s after the
                // execution connection broke, while CAS and Execute recovered within 40 s, so a
                // retry on the same connection would only have waited again. Reconnects are rate
                // limited per client (RECONNECT_MIN_INTERVAL), so many reads timing out together
                // redial once a second at most.
                if recovery == Recovery::Reconnect || client_timeout {
                    recover(Recovery::Reconnect).await;
                }

                let delay = grpc_error_retry_delay(&err)
                    .map(|delay| std::cmp::min(delay, retry_max_delay))
                    .unwrap_or_else(|| jittered_retry_delay(next_delay));
                tracing::debug!(
                    retry_attempt = retry_attempt + 1,
                    retries,
                    delay_ms = delay.as_millis(),
                    "Retrying transient gRPC failure"
                );
                tokio::time::sleep(delay).await;
                retry_attempt += 1;
                next_delay = std::cmp::min(next_delay.saturating_mul(2), retry_max_delay);
            }
        }
    }
}

async fn retry_grpc_request_with_client_reconnect<T, Fut, F>(
    grpc_clients: Arc<GRPCClients>,
    kind: GrpcClientKind,
    retries: usize,
    retry_max_delay: Duration,
    request: F,
) -> anyhow::Result<T>
where
    Fut: Future<Output = anyhow::Result<T>>,
    F: FnMut() -> Fut,
{
    retry_grpc_request_with_recovery(retries, retry_max_delay, request, |recovery| {
        let grpc_clients = grpc_clients.clone();
        async move { grpc_clients.recover(kind, recovery).await }
    })
    .await
}

/// `retry_grpc_request_with_client_reconnect` for an idempotent call, which also retries a
/// request that timed out on the client's side: a read, GetActionResult or FindMissingBlobs, or a
/// CAS write, whose content address makes a second write of the same blob a no-op.
async fn retry_idempotent_with_client_reconnect<T, Fut, F>(
    grpc_clients: Arc<GRPCClients>,
    kind: GrpcClientKind,
    retries: usize,
    retry_max_delay: Duration,
    request: F,
) -> anyhow::Result<T>
where
    Fut: Future<Output = anyhow::Result<T>>,
    F: FnMut() -> Fut,
{
    retry_grpc_request_with_policy(
        retries,
        retry_max_delay,
        request,
        |recovery| {
            let grpc_clients = grpc_clients.clone();
            async move { grpc_clients.recover(kind, recovery).await }
        },
        true,
    )
    .await
}

/// `retry_grpc_request_with_policy` for an ActionCache call. Each attempt goes out on the next
/// member of the pool, so a retry lands on another connection, and a reconnect redials the
/// member the failed attempt used.
async fn retry_action_cache_request<T, Fut, F>(
    grpc_clients: Arc<GRPCClients>,
    retries: usize,
    retry_max_delay: Duration,
    retry_client_timeouts: bool,
    request: F,
) -> anyhow::Result<T>
where
    Fut: Future<Output = anyhow::Result<T>>,
    F: Fn(ActionCacheClient<GrpcService>) -> Fut,
{
    let member = AtomicUsize::new(0);
    retry_grpc_request_with_policy(
        retries,
        retry_max_delay,
        || async {
            let (index, client) = grpc_clients.action_cache_client().await?;
            member.store(index, Ordering::Relaxed);
            request(client).await
        },
        |recovery| {
            let kind = GrpcClientKind::ActionCache {
                member: member.load(Ordering::Relaxed),
            };
            let grpc_clients = grpc_clients.clone();
            async move { grpc_clients.recover(kind, recovery).await }
        },
        retry_client_timeouts,
    )
    .await
}

async fn execute_stream(
    grpc_clients: Arc<GRPCClients>,
    metadata: RemoteExecutionMetadata,
    use_fbcode_metadata: bool,
    request_metadata_tool_name: &str,
    request: GExecuteRequest,
    retries: usize,
    retry_max_delay: Duration,
) -> anyhow::Result<tonic::Streaming<Operation>> {
    let mut start = remote_request_start(
        "Execution",
        "Execute",
        &metadata,
        request.action_digest.as_ref().map(grpc_digest_string),
    );
    start.skip_cache_lookup = Some(request.skip_cache_lookup);
    if let Some(priority) = request
        .execution_policy
        .as_ref()
        .map(|policy| policy.priority)
    {
        start
            .details
            .insert("priority".to_owned(), priority.to_string());
    }
    remote_request_span(
        start,
        retry_grpc_request_with_client_reconnect(
            grpc_clients.clone(),
            GrpcClientKind::Execution,
            retries,
            retry_max_delay,
            || {
                let grpc_clients = grpc_clients.clone();
                let metadata = metadata.clone();
                let request = request.clone();
                async move {
                    let mut client = grpc_clients.execution_client().await?;
                    Ok(client
                        .execute(with_re_metadata(
                            request,
                            &metadata,
                            use_fbcode_metadata,
                            request_metadata_tool_name,
                        ))
                        .await?
                        .into_inner())
                }
            },
        ),
    )
    .await
}

async fn wait_execution_stream(
    grpc_clients: Arc<GRPCClients>,
    metadata: RemoteExecutionMetadata,
    use_fbcode_metadata: bool,
    request_metadata_tool_name: &str,
    operation_name: String,
    retries: usize,
    retry_max_delay: Duration,
) -> anyhow::Result<tonic::Streaming<Operation>> {
    let mut start = remote_request_start("Execution", "WaitExecution", &metadata, None);
    start
        .details
        .insert("operation_name".to_owned(), operation_name.clone());
    remote_request_span(
        start,
        retry_grpc_request_with_client_reconnect(
            grpc_clients.clone(),
            GrpcClientKind::Execution,
            retries,
            retry_max_delay,
            || {
                let grpc_clients = grpc_clients.clone();
                let metadata = metadata.clone();
                let operation_name = operation_name.clone();
                async move {
                    let mut client = grpc_clients.execution_client().await?;
                    Ok(client
                        .wait_execution(with_re_metadata(
                            WaitExecutionRequest {
                                name: operation_name,
                            },
                            &metadata,
                            use_fbcode_metadata,
                            request_metadata_tool_name,
                        ))
                        .await?
                        .into_inner())
                }
            },
        ),
    )
    .await
}

/// How long a finished operation's stream has to end once its done Operation is in. BuildBuddy
/// writes the trailers as soon as it has sent the done Operation, so they are already in flight;
/// a server that holds the stream open longer keeps one of its concurrent streams busy meanwhile.
const OPERATION_STREAM_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Reads the rest of a stream whose done Operation is in, so it closes with the server's
/// END_STREAM. Dropped instead, it is reset, and h2 remembers a reset stream for 1 s and only 50
/// at a time (h2 0.4.15 src/proto/mod.rs:34-41): the trailers that arrive after that count
/// against a per-connection limit of 1024, the next one closes the connection with GOAWAY
/// ENHANCE_YOUR_CALM, and every action with a stream on it fails (streams.rs:1641-1660).
fn drain_finished_operation_stream(mut stream: tonic::Streaming<Operation>) {
    tokio::spawn(async move {
        let drained = tokio::time::timeout(OPERATION_STREAM_DRAIN_TIMEOUT, async {
            while let Ok(Some(_)) = stream.message().await {}
        })
        .await;
        if drained.is_err() {
            tracing::debug!(
                timeout_secs = OPERATION_STREAM_DRAIN_TIMEOUT.as_secs(),
                "A finished operation's stream did not end in time; resetting it"
            );
        }
    });
}

/// Puts a resumption or a new Execute of a remote action on the console, which the build event
/// stream carries too, so it shows on the Build URL rather than only in the daemon's log.
fn warn_re_execution_retry(message: String) {
    tracing::warn!("{message}");
    if let Some(dispatcher) = buck2_events::dispatch::get_dispatcher_opt() {
        dispatcher.console_warning(message);
    }
}

fn execute_request_action(request: &GExecuteRequest) -> String {
    request
        .action_digest
        .as_ref()
        .map(grpc_digest_string)
        .unwrap_or_default()
}

/// An action's operation stream, between two polls of the stream `execute_with_progress` returns.
struct OperationStream {
    stream: tonic::Streaming<Operation>,
    operation_name: Option<String>,
    execute_retry_attempts: usize,
    /// Resumptions with WaitExecution in a row that the operation said nothing new after.
    stalled_resumes: usize,
    /// The stream is a resumption that has sent nothing yet past its first message, which is the
    /// operation's current status again (REAPI remote_execution.proto, `WaitExecution`).
    resumed: bool,
    queued: QueuedDeadline,
    stalled: StallDeadline,
    /// Operations of the action that stayed QUEUED and were executed again, still read.
    superseded: Vec<SupersededOperation>,
    /// The Execute that every later Execute of the action repeats: the original, or, once the
    /// action has stalled, the one for its uncached Action.
    execute_request: GExecuteRequest,
}

/// When an action is executed again because its operation is still QUEUED.
///
/// BuildBuddy sends one QUEUED Operation on an Execute stream and then only what is published
/// for the execution, which starts when an executor claims it (v2.310.0
/// enterprise/server/remote_execution/execution_server/execution_server.go:1297-1313), and
/// nothing at all meanwhile: no queue position, no heartbeat. An execution whose task the
/// scheduler has lost therefore looks like one waiting in a long queue, and its stream, kept
/// alive by HTTP/2 pings, never ends. The clock is the only way out of that wait.
struct QueuedDeadline {
    timeout: Duration,
    /// Times the action was executed again because an operation stayed QUEUED, each of which
    /// doubles the next period, so a long legitimate queue gets few duplicates.
    reexecutes: u32,
    /// None once an executor has claimed the operation (`operation_claimed`): a claimed
    /// operation is the executor's lease to keep, and an action may run for as long as it needs.
    at: Option<tokio::time::Instant>,
    /// The period with its jitter, which is how long the operation stayed QUEUED when `at` comes.
    wait: Duration,
    /// Whether the current operation has said QUEUED, after which its CACHE_CHECK is a claim.
    seen_queued: bool,
}

impl QueuedDeadline {
    fn new(timeout: Duration) -> Self {
        let mut deadline = Self {
            timeout,
            reexecutes: 0,
            at: None,
            wait: Duration::ZERO,
            seen_queued: false,
        };
        deadline.start();
        deadline
    }

    /// Starts the clock of a new operation, which an Execute has just created. tonic returns the
    /// stream once the response headers are in, which BuildBuddy sends with the first QUEUED
    /// Operation (execution_server.go:1302), after it has recorded the execution for merging
    /// (execution_server.go:1144, action_merger.go:288-294). So the merge record is older than
    /// the clock, and a timeout longer than its TTL outlives it.
    fn start(&mut self) {
        self.seen_queued = false;
        self.rearm();
    }

    /// Starts the clock again for the current operation. The period gets up to a quarter more at
    /// random, because the actions a build enqueues together would otherwise reach their
    /// deadlines together and send a burst of Executes to a scheduler that may be what is in
    /// trouble. The jitter only lengthens the period, so it still outlives the merge record.
    fn rearm(&mut self) {
        let period = self.period();
        self.wait = period.saturating_add(
            Duration::try_from_secs_f64(period.as_secs_f64() / 4.0 * random_unit())
                .unwrap_or_default(),
        );
        self.at = if self.timeout.is_zero() {
            None
        } else {
            tokio::time::Instant::now().checked_add(self.wait)
        };
    }

    fn period(&self) -> Duration {
        self.timeout.saturating_mul(1 << self.reexecutes.min(16))
    }
}

/// An operation that stayed QUEUED past its deadline, whose action was executed again. Its stream
/// stays open, so a legitimately queued operation keeps its place in the queue: the first
/// operation of the action that an executor claims, or that finishes with the action's result, is
/// the one the action waits for.
struct SupersededOperation {
    stream: tonic::Streaming<Operation>,
    operation_name: Option<String>,
    seen_queued: bool,
}

/// What ends a wait on an action's operation streams.
enum OperationStreamEvent {
    /// The current operation's stream has a message, has ended, or has failed.
    Next(Result<Option<Operation>, tonic::Status>),
    /// A superseded operation has been claimed, or has finished with the action's result.
    Superseded(SupersededOperation, Operation),
    /// The current operation is still QUEUED at its deadline, or, once claimed, has made no
    /// progress by its stall deadline.
    Deadline,
}

/// Whether an operation's stage says an executor has claimed it, which stops its QUEUED clock.
/// CACHE_CHECK comes before QUEUED in REAPI's order, and a server may send it before it queues the
/// operation. BuildBuddy's executor sends it when it claims the task, after the app's QUEUED, and
/// gets a runner during it before it publishes EXECUTING with the image pull (v2.310.0
/// enterprise/server/remote_execution/executor/executor.go:275-291, 348,
/// operation/operation.go:87-90). By then the merge record lives only as long as the executor's
/// lease, so an Execute sent then would queue a duplicate.
fn operation_claimed(stage: i32, seen_queued: &mut bool) -> bool {
    match execution_stage::Value::try_from(stage) {
        Ok(execution_stage::Value::Queued) => {
            *seen_queued = true;
            false
        }
        Ok(execution_stage::Value::CacheCheck) => *seen_queued,
        Ok(execution_stage::Value::Executing | execution_stage::Value::Completed) => true,
        _ => false,
    }
}

fn operation_stage(operation: &Operation) -> i32 {
    operation
        .metadata
        .as_ref()
        .and_then(|metadata| ExecuteOperationMetadata::decode(&metadata.value[..]).ok())
        .map_or(0, |metadata| metadata.stage)
}

/// Whether a finished operation has the action's result, cached or not, whatever the command's
/// exit code. Any other end says nothing about the current operation, which may still succeed.
fn operation_succeeded(operation: &Operation) -> bool {
    match &operation.result {
        Some(OpResult::Response(any)) => GExecuteResponse::decode(&any.value[..])
            .is_ok_and(|response| response.status.is_none_or(|status| status.code == 0)),
        _ => false,
    }
}

/// Waits for the current operation's stream, for the superseded ones and for the deadline. A
/// superseded stream that ends, fails, or finishes without the action's result is dropped, not
/// resumed.
async fn next_operation_event(
    stream: &mut tonic::Streaming<Operation>,
    superseded: &mut Vec<SupersededOperation>,
    deadline: Option<tokio::time::Instant>,
) -> OperationStreamEvent {
    let mut deadline = deadline.map(|at| Box::pin(tokio::time::sleep_until(at)));
    std::future::poll_fn(|cx| {
        if let std::task::Poll::Ready(next) = stream.poll_next_unpin(cx) {
            return std::task::Poll::Ready(OperationStreamEvent::Next(next.transpose()));
        }
        let mut i = 0;
        while i < superseded.len() {
            match superseded[i].stream.poll_next_unpin(cx) {
                std::task::Poll::Pending => i += 1,
                std::task::Poll::Ready(Some(Ok(operation))) => {
                    if !operation.name.is_empty() {
                        superseded[i].operation_name = Some(operation.name.clone());
                    }
                    if operation.done {
                        let first = superseded.swap_remove(i);
                        if operation_succeeded(&operation) {
                            return std::task::Poll::Ready(OperationStreamEvent::Superseded(
                                first, operation,
                            ));
                        }
                        tracing::debug!(
                            operation_name = first.operation_name.as_deref().unwrap_or(""),
                            "An operation that stayed QUEUED finished without a result; waiting for the later Execute"
                        );
                        drain_finished_operation_stream(first.stream);
                    } else if operation_claimed(
                        operation_stage(&operation),
                        &mut superseded[i].seen_queued,
                    ) {
                        return std::task::Poll::Ready(OperationStreamEvent::Superseded(
                            superseded.swap_remove(i),
                            operation,
                        ));
                    }
                }
                std::task::Poll::Ready(_) => {
                    superseded.swap_remove(i);
                }
            }
        }
        match deadline.as_mut().map(|sleep| sleep.as_mut().poll(cx)) {
            Some(std::task::Poll::Ready(())) => {
                std::task::Poll::Ready(OperationStreamEvent::Deadline)
            }
            _ => std::task::Poll::Pending,
        }
    })
    .await
}

/// Puts an operation that stayed QUEUED past its deadline on the console, with the re-Execute
/// (`attempt` of `retries`) it causes, or that none is left.
fn warn_stayed_queued(
    request: &GExecuteRequest,
    operation_name: Option<&str>,
    waited: Duration,
    attempt: Option<usize>,
    retries: usize,
) {
    let operation_name = operation_name.unwrap_or("");
    let waited = waited.as_secs();
    warn_re_execution_retry(match attempt {
        Some(attempt) => format!(
            "Executing RE action {} again (re-Execute {attempt}/{retries}): operation `{operation_name}` stayed QUEUED for {waited}s",
            execute_request_action(request),
        ),
        None => format!(
            "RE operation `{operation_name}` of action {} stayed QUEUED for {waited}s and its re-Executes are spent ({retries}/{retries}); still waiting for it",
            execute_request_action(request),
        ),
    });
}

/// When an action is executed once more because its claimed operation went quiet.
///
/// An executor that is running a task tells the server so: BuildBuddy's republishes the task's
/// state every `executor.task_progress_publish_interval`, 60 s by default, each time with a new
/// timestamp in the operation's partial execution metadata (v2.310.0
/// enterprise/server/remote_execution/executor/executor.go:58, 446-457,
/// operation/operation.go:62-84), and the app forwards every update to the operation's streams
/// (execution_server/execution_server.go:1611-1615). A claimed operation whose stream says nothing
/// new for many such intervals has lost its executor or its updates. On 2026-10-03 one did both:
/// its first requester's Execute was cancelled during dispatch, the app then marked the execution
/// failed on that same cancelled context, so the failure was never published
/// (execution_server.go:1155-1160, 1373-1376), and every request merged onto the execution waited
/// on a stream nothing would finish. A resumption replays the last status published, EXECUTING,
/// and nothing after it, so it waits again (execution_server.go:1286-1293).
struct StallDeadline {
    timeout: Duration,
    /// None until the current operation is claimed, and after the clock is turned off.
    at: Option<tokio::time::Instant>,
    /// The metadata of the last claimed Operation, which a resumption repeats first and is
    /// therefore not progress.
    last_metadata: Option<Vec<u8>>,
    /// Whether the action has already been executed again after a stall.
    reexecuted: bool,
}

impl StallDeadline {
    fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            at: None,
            last_metadata: None,
            reexecuted: false,
        }
    }

    /// Stops the clock for a new operation, which an Execute has just created or which replaces
    /// the current one, until it is claimed.
    fn reset(&mut self) {
        self.at = None;
        self.last_metadata = None;
    }

    /// Restarts the clock when a claimed operation says something new.
    fn observe(&mut self, metadata: Vec<u8>) {
        if self.at.is_some() && self.last_metadata.as_ref() == Some(&metadata) {
            return;
        }
        self.last_metadata = Some(metadata);
        self.at = if self.timeout.is_zero() {
            None
        } else {
            tokio::time::Instant::now().checked_add(self.timeout)
        };
    }
}

/// A resumption of a claimed operation that did not answer before the operation's stall
/// deadline.
#[derive(Debug)]
struct ResumptionStalled;

impl std::fmt::Display for ResumptionStalled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the resumption of a claimed operation made no progress before its deadline")
    }
}

impl std::error::Error for ResumptionStalled {}

/// The connection and settings a stalled action's re-Execute is sent with.
struct StalledReexecute<'a> {
    grpc_clients: Arc<GRPCClients>,
    metadata: &'a RemoteExecutionMetadata,
    use_fbcode_metadata: bool,
    request_metadata_tool_name: &'a str,
    retries: usize,
    retry_max_delay: Duration,
    grpc_request_timeout: Duration,
}

impl StalledReexecute<'_> {
    /// Executes the action of `request`, whose operation has made no progress for the stall
    /// timeout, once more, as an Action with `do_not_cache` set. BuildBuddy merges an Execute
    /// onto a pending execution of the same action digest, keyed on the digest alone
    /// (action_merger/action_merger.go:68-81), for as long as the merge record lives: 10 minutes
    /// from the first Execute, and while an executor holds a lease, 4 lease periods from its last
    /// renewal (action_merger.go:27-34, 112-133;
    /// enterprise/server/scheduling/scheduler_server/scheduler_server.go:2329). A plain re-Execute
    /// could join the stalled execution. An Action with `do_not_cache` is never merged
    /// (action_merger.go:260-263), and its digest differs, so it is not looked up under the
    /// stalled one either. Its result is not written to the action cache
    /// (execution_server.go:1693-1697), which costs one cache entry for one action. A second
    /// stall fails the action.
    async fn execute(
        &self,
        request: &GExecuteRequest,
        operation_name: Option<&str>,
        stalled: &mut StallDeadline,
    ) -> anyhow::Result<(tonic::Streaming<Operation>, GExecuteRequest)> {
        let operation_name = operation_name.unwrap_or("");
        let action = execute_request_action(request);
        let waited = stalled.timeout.as_secs();
        if stalled.reexecuted {
            let message = format!(
                "RE operation `{operation_name}` of action {action} made no progress for {waited}s, after the action had already been executed again once because an earlier operation made none; failing the action instead of waiting on it"
            );
            warn_re_execution_retry(message.clone());
            return Err(REClientError {
                code: TCode::DEADLINE_EXCEEDED,
                message,
                group: TCodeReasonGroup::UNKNOWN,
            }
            .into());
        }
        stalled.reexecuted = true;
        let uncached = self.uncached_request(request).await.with_context(|| {
            format!(
                "RE operation `{operation_name}` of action {action} made no progress for {waited}s, and its Action could not be sent again with do_not_cache"
            )
        })?;
        warn_re_execution_retry(format!(
            "Executing RE action {action} again as action {} with do_not_cache set: its operation `{operation_name}` made no progress for {waited}s. The server does not merge this Execute into the stalled operation, and does not cache its result",
            execute_request_action(&uncached),
        ));
        let stream = execute_stream(
            self.grpc_clients.clone(),
            self.metadata.clone(),
            self.use_fbcode_metadata,
            self.request_metadata_tool_name,
            uncached.clone(),
            self.retries,
            self.retry_max_delay,
        )
        .await
        .with_context(|| {
            format!(
                "RE operation `{operation_name}` of action {action} made no progress for {waited}s, and the Execute of its uncached Action failed"
            )
        })?;
        Ok((stream, uncached))
    }

    /// `request` for its Action with `do_not_cache` set, which is uploaded under its own digest.
    /// An Action that already has it was never merged, and is executed as it is.
    async fn uncached_request(&self, request: &GExecuteRequest) -> anyhow::Result<GExecuteRequest> {
        let action_digest = request
            .action_digest
            .clone()
            .context("The Execute request has no action digest")?;
        let read = BatchReadBlobsRequest {
            instance_name: request.instance_name.clone(),
            digests: vec![action_digest.clone()],
            acceptable_compressors: vec![compressor::Value::Identity as i32],
            digest_function: request.digest_function,
            ..Default::default()
        };
        let read = retry_grpc_request_with_client_reconnect(
            self.grpc_clients.clone(),
            GrpcClientKind::Cas,
            self.retries,
            self.retry_max_delay,
            || {
                let grpc_clients = self.grpc_clients.clone();
                let read = read.clone();
                async move {
                    Ok(grpc_clients
                        .cas_client()
                        .await?
                        .batch_read_blobs(with_re_metadata_timeout(
                            read,
                            self.metadata.clone(),
                            self.use_fbcode_metadata,
                            self.request_metadata_tool_name,
                            self.grpc_request_timeout,
                        ))
                        .await?
                        .into_inner())
                }
            },
        )
        .await?;
        let blob = read
            .responses
            .into_iter()
            .find(|blob| blob.digest.as_ref() == Some(&action_digest))
            .context("The CAS did not return the Action")?;
        if let Some(status) = blob.status.as_ref().filter(|status| status.code != 0) {
            return Err(anyhow::Error::from(re_client_error_from_rpc_status(status))
                .context("The CAS could not read the Action"));
        }
        let mut action = Action::decode(&blob.data[..]).context("The Action does not decode")?;
        if action.do_not_cache {
            return Ok(request.clone());
        }
        action.do_not_cache = true;
        let data = action.encode_to_vec();
        let digest = tdigest_to(digest_blob(
            &data,
            digest_function_from_grpc(request.digest_function)
                .unwrap_or(digest_function::Value::Sha256),
        )?);
        let update = BatchUpdateBlobsRequest {
            instance_name: request.instance_name.clone(),
            requests: vec![Request {
                digest: Some(digest.clone()),
                data,
                compressor: compressor::Value::Identity as i32,
            }],
            digest_function: request.digest_function,
            ..Default::default()
        };
        let update = retry_grpc_request_with_client_reconnect(
            self.grpc_clients.clone(),
            GrpcClientKind::Cas,
            self.retries,
            self.retry_max_delay,
            || {
                let grpc_clients = self.grpc_clients.clone();
                let update = update.clone();
                async move {
                    Ok(grpc_clients
                        .cas_client()
                        .await?
                        .batch_update_blobs(with_re_metadata_timeout(
                            update,
                            self.metadata.clone(),
                            self.use_fbcode_metadata,
                            self.request_metadata_tool_name,
                            self.grpc_request_timeout,
                        ))
                        .await?
                        .into_inner())
                }
            },
        )
        .await?;
        if let Some(status) = update
            .responses
            .iter()
            .filter_map(|response| response.status.as_ref())
            .find(|status| status.code != 0)
        {
            return Err(anyhow::Error::from(re_client_error_from_rpc_status(status))
                .context("The CAS could not store the uncached Action"));
        }
        Ok(GExecuteRequest {
            action_digest: Some(digest),
            ..request.clone()
        })
    }
}

/// Resumes an operation with WaitExecution, or executes its action again when WaitExecution says
/// the operation or its connection is lost, or when the operation is still QUEUED at the deadline
/// before the resumption answers. BuildBuddy answers a WaitExecution with the last status
/// published for the execution (execution_server.go:1292), nothing is published until an
/// executor claims it, and grpc-go sends the response headers with the first message, so the
/// resumption of a queued operation does not even return its stream until then.
async fn resume_or_retry_execute(
    grpc_clients: Arc<GRPCClients>,
    metadata: RemoteExecutionMetadata,
    use_fbcode_metadata: bool,
    request_metadata_tool_name: &str,
    operation_name: String,
    execute_request: GExecuteRequest,
    mut execute_retry_attempts: usize,
    retries: usize,
    retry_max_delay: Duration,
    wait_failure_context: String,
    queued: &mut QueuedDeadline,
    stalled: &mut StallDeadline,
) -> anyhow::Result<(tonic::Streaming<Operation>, Option<String>, usize)> {
    loop {
        // Only the WaitExecution runs against the clock. An Execute cut off by it might already
        // have queued a task on the server that no attempt counts.
        let wait = wait_execution_stream(
            grpc_clients.clone(),
            metadata.clone(),
            use_fbcode_metadata,
            request_metadata_tool_name,
            operation_name.clone(),
            retries,
            retry_max_delay,
        );
        let waited = match (queued.at, stalled.at) {
            (Some(at), _) => tokio::time::timeout_at(at, wait).await.ok(),
            // A claimed operation's resumption is held to the operation's stall deadline, whose
            // re-Execute the caller sends.
            (None, Some(at)) => match tokio::time::timeout_at(at, wait).await {
                Ok(waited) => Some(waited),
                Err(_) => return Err(anyhow::Error::new(ResumptionStalled)),
            },
            (None, None) => Some(wait.await),
        };
        let failure_context = match waited {
            Some(Ok(stream)) => return Ok((stream, Some(operation_name), execute_retry_attempts)),
            Some(Err(wait_err)) if should_retry_execute_after_wait_execution_error(&wait_err) => {
                if !can_retry_execute(execute_retry_attempts, retries) {
                    return Err(wait_err.context(wait_failure_context).context(format!(
                        "RE operation `{operation_name}` was lost after retry limit"
                    )));
                }
                execute_retry_attempts += 1;
                warn_re_execution_retry(format!(
                    "Executing RE action {} again (re-Execute {execute_retry_attempts}/{retries}): WaitExecution of operation `{operation_name}` failed: {wait_err:#}",
                    execute_request_action(&execute_request),
                ));
                format!("RE operation `{operation_name}` was lost and Execute retry failed")
            }
            Some(Err(wait_err)) => return Err(wait_err.context(wait_failure_context)),
            None if can_retry_execute(execute_retry_attempts, retries) => {
                execute_retry_attempts += 1;
                warn_stayed_queued(
                    &execute_request,
                    Some(&operation_name),
                    queued.wait,
                    Some(execute_retry_attempts),
                    retries,
                );
                queued.reexecutes += 1;
                match execute_stream(
                    grpc_clients.clone(),
                    metadata.clone(),
                    use_fbcode_metadata,
                    request_metadata_tool_name,
                    execute_request.clone(),
                    retries,
                    retry_max_delay,
                )
                .await
                {
                    Ok(stream) => {
                        queued.start();
                        stalled.reset();
                        return Ok((stream, None, execute_retry_attempts));
                    }
                    // The operation may still be queued legitimately, so, as on its own stream,
                    // it is waited for rather than failed, and the attempt is spent.
                    Err(err) => {
                        warn_re_execution_retry(format!(
                            "RE action {} could not be executed again, so its operation `{operation_name}` is still waited for: {err:#}",
                            execute_request_action(&execute_request),
                        ));
                        queued.rearm();
                        continue;
                    }
                }
            }
            None => {
                warn_stayed_queued(
                    &execute_request,
                    Some(&operation_name),
                    queued.wait,
                    None,
                    retries,
                );
                queued.at = None;
                continue;
            }
        };
        let stream = execute_stream(
            grpc_clients.clone(),
            metadata.clone(),
            use_fbcode_metadata,
            request_metadata_tool_name,
            execute_request.clone(),
            retries,
            retry_max_delay,
        )
        .await
        .context(failure_context)?;
        queued.start();
        stalled.reset();
        return Ok((stream, None, execute_retry_attempts));
    }
}

/// Counts a resumption of an operation stream that said nothing new since the previous one, and
/// waits before it, twice as long each time. The first resumption is the protocol at work, the
/// rest are retries, so after `retries + 1` of them in a row this returns false: a server or proxy
/// that keeps opening the stream and dropping it is not going to finish the operation.
async fn pause_before_resume(
    stalled_resumes: &mut usize,
    retries: usize,
    retry_max_delay: Duration,
) -> bool {
    if *stalled_resumes > retries {
        return false;
    }
    *stalled_resumes += 1;
    let doublings = (*stalled_resumes - 1).min(16) as u32;
    let delay = Duration::from_millis(GRPC_RETRY_INITIAL_DELAY_MILLIS)
        .saturating_mul(1 << doublings)
        .min(retry_max_delay);
    tokio::time::sleep(jittered_retry_delay(delay)).await;
    true
}

enum BystreamWritePlan {
    Write(Vec<WriteRequest>),
    AlreadyCommitted(i64),
}

fn total_bystream_write_size(segments: &[WriteRequest]) -> i64 {
    segments
        .last()
        .map(|segment| segment.write_offset + segment.data.len() as i64)
        .unwrap_or(0)
}

fn trim_bystream_write_segments(
    segments: Vec<WriteRequest>,
    committed_size: i64,
) -> Vec<WriteRequest> {
    if committed_size <= 0 {
        return segments;
    }

    let mut resumed = Vec::with_capacity(segments.len());
    for mut segment in segments {
        let start = segment.write_offset;
        let end = start + segment.data.len() as i64;

        if end <= committed_size {
            continue;
        }

        if start < committed_size {
            let skip = (committed_size - start) as usize;
            segment.data = segment.data[skip..].to_vec();
            segment.write_offset = committed_size;
        }

        resumed.push(segment);
    }

    resumed
}

fn ttimestamp_to(ts: TTimestamp) -> ::prost_types::Timestamp {
    ::prost_types::Timestamp {
        seconds: ts.seconds,
        nanos: ts.nanos,
    }
}

fn ttimestamp_from(ts: Option<::prost_types::Timestamp>) -> TTimestamp {
    match ts {
        Some(timestamp) => TTimestamp {
            seconds: timestamp.seconds,
            nanos: timestamp.nanos,
            ..Default::default()
        },
        None => TTimestamp::unix_epoch(),
    }
}

async fn create_tls_config(settings: &GrpcTlsSettings) -> anyhow::Result<ClientTlsConfig> {
    let config = match settings.tls_ca_certs.as_ref() {
        Some(tls_ca_certs) => {
            let tls_ca_certs =
                substitute_env_vars(tls_ca_certs).context("Invalid `tls_ca_certs`")?;
            let data = tokio::fs::read(&tls_ca_certs)
                .await
                .with_context(|| format!("Error reading `{tls_ca_certs}`"))?;
            ClientTlsConfig::new().ca_certificate(Certificate::from_pem(data))
        }
        None => {
            // We set the `tls-webpki-roots` feature so we'll get that default.
            ClientTlsConfig::new().with_enabled_roots()
        }
    };

    let config = match settings.tls_client_cert.as_ref() {
        Some(tls_client_cert) => {
            let tls_client_cert =
                substitute_env_vars(tls_client_cert).context("Invalid `tls_client_cert`")?;
            let data = tokio::fs::read(&tls_client_cert)
                .await
                .with_context(|| format!("Error reading `{tls_client_cert}`"))?;
            config.identity(Identity::from_pem(&data, &data))
        }
        None => config,
    };

    Ok(config)
}

pub(crate) fn prepare_uri(uri: Uri, tls_override: Option<bool>) -> anyhow::Result<(Uri, bool)> {
    // Now do some awkward things with the protocol. Why do we do all this? The reason is
    // because we'd like our configuration to not be super confusing. We don't want to e.g.
    // allow setting the address to `https://foobar`; instead we infer TLS from the source
    // scheme and only accept schemes that are valid in GRPC naming.

    // This is the GRPC spec for naming: https://github.com/grpc/grpc/blob/master/doc/naming.md
    // Many people (including Bazel), use grpc:// and grpcs://, so we tolerate both.
    // We also accept http:// and https:// for convenience.

    let tls = match uri.scheme_str() {
        Some("grpc") => false,
        Some("grpcs") => true,
        Some("http") => false,
        Some("https") => true,
        Some("dns") | Some("ipv4") | Some("ipv6") | None => true,
        Some(scheme) => {
            return Err(anyhow::anyhow!(
                "Invalid URI scheme: `{}` for `{}` (expected one of grpc, grpcs, http, https, dns, ipv4, ipv6, or no scheme)",
                scheme,
                uri,
            ));
        }
    };

    // `[buck2_re_client] tls` wins over the scheme. Upstream buck2 reads TLS from that key
    // alone, so the config nsc and others write for it says `grpc://host:port` and `tls = true`.
    let tls = tls_override.unwrap_or(tls);

    // And now, let's put back a proper scheme for Tonic to be happy with. First, because
    // Tonic will blow up if we don't. Second, so we get port inference.
    let mut parts = uri.into_parts();
    parts.scheme = Some(if tls {
        http::uri::Scheme::HTTPS
    } else {
        http::uri::Scheme::HTTP
    });

    // Is this API actually designed to be unusable? If you've got a scheme, you must
    // have a path_and_query. I'm sure there's a good reason, so we abide:
    if parts.path_and_query.is_none() {
        parts.path_and_query = Some(http::uri::PathAndQuery::from_static("/"));
    }

    Ok((Uri::from_parts(parts)?, tls))
}

/// Contains information queried from the Remote Execution Capabilities service.
pub struct RECapabilities {
    /// Whether these capabilities came from the remote server.
    capabilities_queried: bool,
    /// Largest size of a message before being uploaded using bytestream service.
    /// 0 indicates no limit beyond constraint of underlying transport (which is unknown).
    max_total_batch_size: usize,
    /// Largest CAS blob the server accepts for uploads, if advertised.
    max_cas_blob_size_bytes: Option<i64>,
    /// Compressors supported by the "compressed-blobs" bytestream resources.
    supported_compressors: Vec<Compressor>,
    /// Compressors supported for BatchUpdateBlobs inlined data.
    supported_batch_update_compressors: Vec<Compressor>,
    /// Digest functions supported by the remote cache/execution capabilities.
    supported_digest_functions: Vec<digest_function::Value>,
    /// Digest functions supported by the remote cache capabilities.
    cache_digest_functions: Vec<digest_function::Value>,
    /// Digest functions supported by the remote execution capabilities.
    execution_digest_functions: Vec<digest_function::Value>,
    /// Supported nonzero execution priority ranges.
    execution_priority_ranges: Vec<PriorityRange>,
    /// Whether the action cache accepts updates, if advertised by the server.
    action_cache_update_enabled: Option<bool>,
    /// Whether remote execution is enabled, if advertised by the server.
    execution_enabled: Option<bool>,
    /// Whether the server supports CAS SplitBlob.
    blob_split_supported: bool,
    /// Whether the server supports CAS SpliceBlob.
    blob_splice_supported: bool,
    /// FastCDC 2020 chunking parameters advertised by the remote cache.
    fast_cdc_2020: Option<FastCdc2020Config>,
}

/// Contains runtime options for the remote execution client as set under `buck2_re_client`
pub struct RERuntimeOpts {
    /// Use the Meta version of the request metadata
    use_fbcode_metadata: bool,
    /// Tool name to report in RequestMetadata.tool_details.
    request_metadata_tool_name: String,
    /// Maximum number of concurrent upload requests.
    max_concurrent_uploads_per_action: Option<usize>,
    /// Time that digests are assumed to live in CAS after being touched.
    cas_ttl_secs: i64,
    /// Maximum number of digests per `FindMissingBlobs` RPC.
    find_missing_blobs_batch_size: usize,
    /// Whether to chunk large remote-cache blobs using FastCDC 2020 and SpliceBlob.
    remote_cache_chunking: bool,
    /// Minimum blob size for remote cache compression.
    remote_cache_compression_threshold: usize,
    /// Number of retries to apply for transient gRPC errors.
    retries: usize,
    /// Maximum delay between retry attempts.
    retry_max_delay_ms: u64,
    /// Per-attempt timeout for unary gRPC requests.
    grpc_request_timeout: Duration,
    /// Maximum idle time for a ByteStream download before the stream is retried.
    bytestream_progress_timeout: Duration,
    /// Time an operation may stay QUEUED before its action is executed again; zero is never.
    queued_operation_timeout: Duration,
    /// Time a claimed operation may make no progress before its action is executed again, once;
    /// zero is never.
    stalled_operation_timeout: Duration,
    /// Digest function selected from user config and capabilities for download hash validation.
    download_hash_digest_function: Option<digest_function::Value>,
    /// Digest functions selected from daemon config for RE request fields.
    request_digest_function_config: DigestFunctionConfig,
}

impl RERuntimeOpts {
    fn download_hash_digest_function_for_hash(&self, hash: &str) -> Option<digest_function::Value> {
        self.request_digest_function_config
            .for_hash(hash)
            .or(self.download_hash_digest_function)
    }
}

struct InstanceName(Option<String>);

impl InstanceName {
    fn as_str(&self) -> &str {
        match &self.0 {
            Some(instance_name) => instance_name,
            None => "",
        }
    }

    fn as_resource_prefix(&self) -> String {
        match &self.0 {
            Some(instance_name) => format!("{instance_name}/"),
            None => "".to_owned(),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum Compressor {
    Zstd,
    Deflate,
    Brotli,
}

impl Compressor {
    fn from_grpc(val: i32) -> Option<Self> {
        if val == compressor::Value::Zstd as i32 {
            Some(Self::Zstd)
        } else if val == compressor::Value::Deflate as i32 {
            Some(Self::Deflate)
        } else if val == compressor::Value::Brotli as i32 {
            Some(Self::Brotli)
        } else {
            None
        }
    }

    fn as_grpc(self) -> i32 {
        match self {
            Self::Zstd => compressor::Value::Zstd as i32,
            Self::Deflate => compressor::Value::Deflate as i32,
            Self::Brotli => compressor::Value::Brotli as i32,
        }
    }

    /// The compressor name used in compressed-blob resource paths
    fn name(self) -> &'static str {
        match self {
            Self::Zstd => "zstd",
            Self::Deflate => "deflate",
            Self::Brotli => "brotli",
        }
    }
}

fn grpc_digest_size_bytes(digest: &Digest) -> u64 {
    u64::try_from(digest.size_bytes).unwrap_or_default()
}

fn grpc_digest_string(digest: &Digest) -> String {
    format!("{}/{}", digest.hash, digest.size_bytes)
}

fn remote_request_start(
    service: &str,
    method: &str,
    metadata: &RemoteExecutionMetadata,
    action_digest: Option<String>,
) -> buck2_data::RemoteRequestStart {
    buck2_data::RemoteRequestStart {
        service: service.to_owned(),
        method: method.to_owned(),
        action_digest,
        action_id: metadata.action_id.clone(),
        target: metadata.target_id.clone(),
        action_mnemonic: metadata.action_mnemonic.clone(),
        use_case: metadata.use_case_id.clone(),
        ..Default::default()
    }
}

fn remote_request_end<T>(result: &anyhow::Result<T>) -> buck2_data::RemoteRequestEnd {
    match result {
        Ok(_) => buck2_data::RemoteRequestEnd {
            success: true,
            ..Default::default()
        },
        Err(error) => buck2_data::RemoteRequestEnd {
            success: false,
            error: Some(error.to_string()),
            re_error_code: error
                .downcast_ref::<tonic::Status>()
                .map(|status| format!("{:?}", status.code())),
            ..Default::default()
        },
    }
}

async fn remote_request_span<T>(
    start: buck2_data::RemoteRequestStart,
    fut: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    span_async(start, async move {
        let result = fut.await;
        let end = remote_request_end(&result);
        (result, end)
    })
    .await
}

fn request_stats_for_grpc_digests<'a>(digests: impl IntoIterator<Item = &'a Digest>) -> (u64, u64) {
    digests
        .into_iter()
        .fold((0u64, 0u64), |(count, bytes), digest| {
            (
                count.saturating_add(1),
                bytes.saturating_add(grpc_digest_size_bytes(digest)),
            )
        })
}

fn select_preferred_compressor(compressors: &[Compressor]) -> Option<Compressor> {
    if compressors.contains(&Compressor::Zstd) {
        Some(Compressor::Zstd)
    } else if compressors.contains(&Compressor::Brotli) {
        Some(Compressor::Brotli)
    } else if compressors.contains(&Compressor::Deflate) {
        Some(Compressor::Deflate)
    } else {
        None
    }
}

fn compressor_names(compressors: &[Compressor]) -> String {
    if compressors.is_empty() {
        return "<none>".to_owned();
    }

    compressors
        .iter()
        .map(|compressor| compressor.name())
        .collect::<Vec<_>>()
        .join(",")
}

fn compression_for_blob(
    compressor: Option<Compressor>,
    size_in_bytes: i64,
    threshold: usize,
) -> Option<Compressor> {
    let size = usize::try_from(size_in_bytes).ok()?;
    if size == 0 || size < threshold {
        None
    } else {
        compressor
    }
}

async fn compress_data(data: Vec<u8>, compressor: Compressor) -> anyhow::Result<Vec<u8>> {
    async fn read_all(mut reader: impl AsyncRead + Unpin) -> anyhow::Result<Vec<u8>> {
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await?;
        Ok(data)
    }

    match compressor {
        Compressor::Zstd => read_all(ZstdEncoder::new(Cursor::new(data))).await,
        Compressor::Deflate => read_all(DeflateEncoder::new(Cursor::new(data))).await,
        Compressor::Brotli => read_all(BrotliEncoder::new(Cursor::new(data))).await,
    }
}

async fn decompress_data(data: Vec<u8>, compressor: Compressor) -> anyhow::Result<Vec<u8>> {
    async fn read_all(mut reader: impl AsyncRead + Unpin) -> anyhow::Result<Vec<u8>> {
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await?;
        Ok(data)
    }

    match compressor {
        Compressor::Zstd => {
            let mut decoder = ZstdDecoder::new(Cursor::new(data));
            decoder.multiple_members(true);
            read_all(decoder).await
        }
        Compressor::Deflate => read_all(DeflateDecoder::new(Cursor::new(data))).await,
        Compressor::Brotli => read_all(BrotliDecoder::new(Cursor::new(data))).await,
    }
}

fn digest_function_from_grpc(val: i32) -> Option<digest_function::Value> {
    let value = digest_function::Value::try_from(val).ok()?;
    if value == digest_function::Value::Unknown {
        None
    } else {
        Some(value)
    }
}

fn parse_configured_digest_function(value: &str) -> Option<digest_function::Value> {
    match value.trim().to_ascii_uppercase().as_str() {
        "SHA1" => Some(digest_function::Value::Sha1),
        "SHA256" => Some(digest_function::Value::Sha256),
        // RE API only has BLAKE3 (not keyed); map config tokens to that capability.
        "BLAKE3" | "BLAKE3-KEYED" => Some(digest_function::Value::Blake3),
        _ => None,
    }
}

fn digest_function_name(value: digest_function::Value) -> &'static str {
    match value {
        digest_function::Value::Md5 => "MD5",
        digest_function::Value::Murmur3 => "MURMUR3",
        digest_function::Value::Sha1 => "SHA1",
        digest_function::Value::Sha256 => "SHA256",
        digest_function::Value::Sha384 => "SHA384",
        digest_function::Value::Sha512 => "SHA512",
        digest_function::Value::Vso => "VSO",
        digest_function::Value::Sha256tree => "SHA256TREE",
        digest_function::Value::Blake3 => "BLAKE3",
        digest_function::Value::Unknown => "UNKNOWN",
    }
}

fn digest_function_names(digest_functions: &[digest_function::Value]) -> String {
    if digest_functions.is_empty() {
        return "<unknown>".to_owned();
    }

    digest_functions
        .iter()
        .map(|digest_function| digest_function_name(*digest_function))
        .collect::<Vec<_>>()
        .join(",")
}

fn cache_digest_functions_from_capabilities(
    cache_capabilities: Option<&CacheCapabilities>,
) -> (Vec<digest_function::Value>, bool) {
    let Some(cache_capabilities) = cache_capabilities else {
        return (Vec::new(), false);
    };

    let mut cache_digest_functions = cache_capabilities
        .digest_functions
        .iter()
        .copied()
        .filter_map(digest_function_from_grpc)
        .collect::<Vec<_>>();
    cache_digest_functions.sort_unstable();
    cache_digest_functions.dedup();

    if cache_digest_functions.is_empty() {
        (vec![digest_function::Value::Sha256], true)
    } else {
        (cache_digest_functions, false)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PriorityRange {
    min_priority: i32,
    max_priority: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FastCdc2020Config {
    avg_chunk_size_bytes: u64,
    seed: u32,
}

#[derive(Debug)]
struct LocalChunkCache {
    root: PathBuf,
}

impl LocalChunkCache {
    fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn path_for(&self, digest: &TDigest) -> Option<PathBuf> {
        if digest.size_in_bytes < 0 || digest.hash.is_empty() {
            return None;
        }
        if !digest.hash.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }

        let hash = digest.hash.to_ascii_lowercase();
        let prefix_len = hash.len().min(2);
        let prefix = &hash[..prefix_len];
        Some(
            self.root
                .join(hash.len().to_string())
                .join(prefix)
                .join(format!("{}-{}", hash, digest.size_in_bytes)),
        )
    }

    async fn read(
        &self,
        digest: &TDigest,
        selected_digest_function: Option<digest_function::Value>,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let Some(path) = self.path_for(digest) else {
            return Ok(None);
        };

        match tokio::fs::read(&path).await {
            Ok(blob) => match validate_downloaded_blob(digest, &blob, selected_digest_function) {
                Ok(()) => Ok(Some(blob)),
                Err(error) => {
                    tracing::debug!(
                        path = %path.display(),
                        digest = %digest,
                        %error,
                        "Ignoring corrupt local FastCDC chunk cache entry"
                    );
                    drop(tokio::fs::remove_file(&path).await);
                    Ok(None)
                }
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => {
                tracing::debug!(
                    path = %path.display(),
                    digest = %digest,
                    %error,
                    "Failed to read local FastCDC chunk cache entry"
                );
                Ok(None)
            }
        }
    }

    async fn write(
        &self,
        digest: &TDigest,
        blob: &[u8],
        selected_digest_function: Option<digest_function::Value>,
    ) {
        let Some(path) = self.path_for(digest) else {
            return;
        };
        if let Err(error) = validate_downloaded_blob(digest, blob, selected_digest_function) {
            tracing::debug!(
                path = %path.display(),
                digest = %digest,
                %error,
                "Skipping invalid local FastCDC chunk cache write"
            );
            return;
        }

        let Some(parent) = path.parent() else {
            return;
        };
        if let Err(error) = tokio::fs::create_dir_all(parent).await {
            tracing::debug!(
                path = %parent.display(),
                digest = %digest,
                %error,
                "Failed to create local FastCDC chunk cache directory"
            );
            return;
        }

        let tmp_path = parent.join(format!(".{}.tmp", Uuid::new_v4()));
        if let Err(error) = tokio::fs::write(&tmp_path, blob).await {
            tracing::debug!(
                path = %tmp_path.display(),
                digest = %digest,
                %error,
                "Failed to write local FastCDC chunk cache entry"
            );
            drop(tokio::fs::remove_file(&tmp_path).await);
            return;
        }

        if let Err(error) = tokio::fs::rename(&tmp_path, &path).await {
            tracing::debug!(
                path = %path.display(),
                digest = %digest,
                %error,
                "Failed to commit local FastCDC chunk cache entry"
            );
            drop(tokio::fs::remove_file(&tmp_path).await);
        }
    }
}

impl FastCdc2020Config {
    fn min_chunk_size_bytes(&self) -> u64 {
        self.avg_chunk_size_bytes / 4
    }

    fn max_chunk_size_bytes(&self) -> u64 {
        self.avg_chunk_size_bytes * 4
    }

    fn chunking_threshold_bytes(&self) -> u64 {
        self.max_chunk_size_bytes()
    }

    fn chunking_function(&self) -> TChunkingFunction {
        TChunkingFunction::FastCdc2020
    }

    fn normalization_level(&self) -> fastcdc::v2020::Normalization {
        fastcdc::v2020::Normalization::Level2
    }
}

fn fast_cdc_2020_config_from_capabilities(
    cache_capabilities: Option<&CacheCapabilities>,
) -> Option<FastCdc2020Config> {
    let params = cache_capabilities?.fast_cdc_2020_params.as_ref()?;
    let configured_avg = params.avg_chunk_size_bytes;
    let avg_chunk_size_bytes =
        if (1024..=1024 * 1024).contains(&configured_avg) && configured_avg.is_power_of_two() {
            configured_avg
        } else {
            DEFAULT_FAST_CDC_2020_AVG_CHUNK_SIZE
        };

    Some(FastCdc2020Config {
        avg_chunk_size_bytes,
        seed: params.seed,
    })
}

fn preferred_split_blob_chunking_function(
    capabilities: &RECapabilities,
    requested: TChunkingFunction,
) -> TChunkingFunction {
    if requested != TChunkingFunction::Unknown {
        return requested;
    }

    capabilities.fast_cdc_2020.as_ref().map_or(
        TChunkingFunction::Unknown,
        FastCdc2020Config::chunking_function,
    )
}

fn validate_chunking_function_supported(
    capabilities: &RECapabilities,
    chunking_function: TChunkingFunction,
) -> anyhow::Result<()> {
    match chunking_function {
        TChunkingFunction::Unknown => Ok(()),
        TChunkingFunction::FastCdc2020 if capabilities.fast_cdc_2020.is_some() => Ok(()),
        TChunkingFunction::FastCdc2020 => Err(anyhow::anyhow!(
            "FastCDC 2020 chunking is not supported by the remote server"
        )),
        TChunkingFunction::RepMaxCdc => Err(anyhow::anyhow!(
            "RepMaxCDC chunking is not supported by this client"
        )),
    }
}

fn validate_remote_cache_chunking_enabled(
    remote_cache_chunking: bool,
    capabilities: &RECapabilities,
) -> anyhow::Result<()> {
    if !remote_cache_chunking {
        return Ok(());
    }

    anyhow::ensure!(
        capabilities.capabilities_queried,
        "`remote_cache_chunking` requires RE server capabilities to be enabled"
    );
    validate_blob_split_supported(capabilities.blob_split_supported)?;
    validate_blob_splice_supported(capabilities.blob_splice_supported)?;
    anyhow::ensure!(
        capabilities.fast_cdc_2020.is_some(),
        "`remote_cache_chunking` requires FastCDC 2020 parameters from the remote server"
    );

    Ok(())
}

fn digest_blob(data: &[u8], digest_function: digest_function::Value) -> anyhow::Result<TDigest> {
    let hash = match digest_function {
        digest_function::Value::Sha1 => format!("{:x}", Sha1::digest(data)),
        digest_function::Value::Sha256 => format!("{:x}", Sha256::digest(data)),
        digest_function::Value::Blake3 => blake3::hash(data).to_hex().to_string(),
        _ => {
            anyhow::bail!(
                "Digest function {} is not supported for FastCDC chunk digests",
                digest_function_name(digest_function)
            )
        }
    };

    Ok(TDigest {
        hash,
        size_in_bytes: i64::try_from(data.len()).context("Blob is too large to digest")?,
        ..Default::default()
    })
}

fn chunk_inlined_blob_fast_cdc_2020(
    blob: &InlinedBlobWithDigest,
    config: &FastCdc2020Config,
    digest_function: digest_function::Value,
) -> anyhow::Result<Vec<InlinedBlobWithDigest>> {
    let min_size = usize::try_from(config.min_chunk_size_bytes())
        .context("FastCDC minimum chunk size does not fit usize")?;
    let avg_size = usize::try_from(config.avg_chunk_size_bytes)
        .context("FastCDC average chunk size does not fit usize")?;
    let max_size = usize::try_from(config.max_chunk_size_bytes())
        .context("FastCDC maximum chunk size does not fit usize")?;

    let chunker = fastcdc::v2020::FastCDC::with_level_and_seed(
        &blob.blob,
        min_size,
        avg_size,
        max_size,
        config.normalization_level(),
        u64::from(config.seed),
    );

    let mut chunks = Vec::new();
    for chunk in chunker {
        let end = chunk
            .offset
            .checked_add(chunk.length)
            .context("FastCDC chunk range overflowed")?;
        let data = blob
            .blob
            .get(chunk.offset..end)
            .context("FastCDC chunk range was outside the blob")?
            .to_vec();
        chunks.push(InlinedBlobWithDigest {
            digest: digest_blob(&data, digest_function)?,
            blob: data,
            ..Default::default()
        });
    }

    Ok(chunks)
}

fn chunk_file_fast_cdc_2020(
    path: &str,
    config: &FastCdc2020Config,
    digest_function: digest_function::Value,
) -> anyhow::Result<Vec<InlinedBlobWithDigest>> {
    let min_size = usize::try_from(config.min_chunk_size_bytes())
        .context("FastCDC minimum chunk size does not fit usize")?;
    let avg_size = usize::try_from(config.avg_chunk_size_bytes)
        .context("FastCDC average chunk size does not fit usize")?;
    let max_size = usize::try_from(config.max_chunk_size_bytes())
        .context("FastCDC maximum chunk size does not fit usize")?;
    let file = std::fs::File::open(path).with_context(|| format!("Opening `{path}` failed"))?;
    let chunker = fastcdc::v2020::StreamCDC::with_level_and_seed(
        file,
        min_size,
        avg_size,
        max_size,
        config.normalization_level(),
        u64::from(config.seed),
    );

    let mut chunks = Vec::new();
    for chunk in chunker {
        let chunk = chunk.with_context(|| format!("Reading FastCDC chunk from `{path}` failed"))?;
        chunks.push(InlinedBlobWithDigest {
            digest: digest_blob(&chunk.data, digest_function)?,
            blob: chunk.data,
            ..Default::default()
        });
    }

    Ok(chunks)
}

fn priority_ranges(capabilities: &PriorityCapabilities) -> Vec<PriorityRange> {
    capabilities
        .priorities
        .iter()
        .map(|range| PriorityRange {
            min_priority: range.min_priority,
            max_priority: range.max_priority,
        })
        .collect()
}

fn priority_range_names(ranges: &[PriorityRange]) -> String {
    if ranges.is_empty() {
        return "<unknown>".to_owned();
    }

    ranges
        .iter()
        .map(|range| format!("{}-{}", range.min_priority, range.max_priority))
        .collect::<Vec<_>>()
        .join(",")
}

fn validate_priority_in_range(
    priority: i32,
    option_name: &str,
    ranges: &[PriorityRange],
) -> anyhow::Result<()> {
    if priority == 0 {
        return Ok(());
    }

    if ranges
        .iter()
        .any(|range| range.min_priority <= priority && priority <= range.max_priority)
    {
        return Ok(());
    }

    Err(anyhow::anyhow!(
        "`{option_name}` {priority} is outside of server supported range {}",
        priority_range_names(ranges)
    ))
}

fn supports_hash_validation(digest_function: digest_function::Value) -> bool {
    matches!(
        digest_function,
        digest_function::Value::Sha1
            | digest_function::Value::Sha256
            | digest_function::Value::Blake3
    )
}

fn select_download_hash_digest_function(
    configured_digest_algorithms: &[String],
    supported_digest_functions: &[digest_function::Value],
) -> anyhow::Result<Option<digest_function::Value>> {
    let mut configured = vec![];
    for configured_algorithm in configured_digest_algorithms {
        match parse_configured_digest_function(configured_algorithm) {
            Some(digest_function) if supports_hash_validation(digest_function) => {
                configured.push(digest_function);
            }
            _ => {
                tracing::debug!(
                    "Ignoring unsupported digest_algorithms entry for download validation: `{}`",
                    configured_algorithm
                );
            }
        }
    }
    let mut configured_dedup = vec![];
    for digest_function in configured {
        if !configured_dedup.contains(&digest_function) {
            configured_dedup.push(digest_function);
        }
    }
    let configured = configured_dedup;

    let mut supported = supported_digest_functions
        .iter()
        .copied()
        .filter(|digest_function| supports_hash_validation(*digest_function))
        .collect::<Vec<_>>();
    supported.sort_unstable();
    supported.dedup();

    if !configured.is_empty() {
        if supported.is_empty() {
            return Ok(unique_digest_function(configured.iter().copied()));
        }
        let compatible = configured
            .iter()
            .copied()
            .filter(|configured_digest_function| supported.contains(configured_digest_function))
            .collect::<Vec<_>>();
        if !compatible.is_empty() {
            return Ok(unique_digest_function(compatible.into_iter()));
        }
        return Err(anyhow::anyhow!(
            "Configured digest_algorithms are incompatible with RE server capabilities. configured={}, server={}",
            digest_function_names(&configured),
            digest_function_names(&supported)
        ));
    }

    if supported.len() == 1 {
        Ok(supported.first().copied())
    } else {
        Ok(None)
    }
}

fn configured_digest_functions(
    configured_digest_algorithms: &[String],
) -> Vec<digest_function::Value> {
    let mut digest_functions = Vec::new();
    for configured_algorithm in configured_digest_algorithms {
        let Some(digest_function) = parse_configured_digest_function(configured_algorithm) else {
            tracing::debug!(
                "Ignoring unsupported digest_algorithms entry for RE capabilities validation: `{}`",
                configured_algorithm
            );
            continue;
        };
        if !digest_functions.contains(&digest_function) {
            digest_functions.push(digest_function);
        }
    }
    digest_functions
}

fn validate_digest_functions_supported(
    configured_digest_functions: &[digest_function::Value],
    supported_digest_functions: &[digest_function::Value],
    capability_name: &str,
) -> anyhow::Result<()> {
    let unsupported = configured_digest_functions
        .iter()
        .copied()
        .filter(|digest_function| !supported_digest_functions.contains(digest_function))
        .collect::<Vec<_>>();

    if unsupported.is_empty() {
        return Ok(());
    }

    Err(anyhow::anyhow!(
        "Configured digest_algorithms {} are incompatible with remote {capability_name} capabilities. Server supported functions are: {}",
        digest_function_names(&unsupported),
        digest_function_names(supported_digest_functions),
    ))
}

fn validate_digest_function_capabilities(
    configured_digest_algorithms: &[String],
    capabilities: &RECapabilities,
) -> anyhow::Result<()> {
    if !capabilities.capabilities_queried {
        return Ok(());
    }

    let configured_digest_functions = configured_digest_functions(configured_digest_algorithms);
    if configured_digest_functions.is_empty() {
        return Ok(());
    }

    validate_digest_functions_supported(
        &configured_digest_functions,
        &capabilities.cache_digest_functions,
        "cache",
    )?;

    if capabilities.execution_enabled == Some(true) {
        validate_digest_functions_supported(
            &configured_digest_functions,
            &capabilities.execution_digest_functions,
            "execution",
        )?;
    }

    Ok(())
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
struct DigestFunctionConfig {
    digest160: Option<digest_function::Value>,
    digest256: Option<digest_function::Value>,
}

impl DigestFunctionConfig {
    fn from_configured_algorithms(configured_digest_algorithms: &[String]) -> Self {
        let configured_digest_functions = configured_digest_functions(configured_digest_algorithms);
        let digest160 = unique_digest_function(
            configured_digest_functions
                .iter()
                .copied()
                .filter(|digest_function| *digest_function == digest_function::Value::Sha1),
        );
        let digest256 = unique_digest_function(configured_digest_functions.iter().copied().filter(
            |digest_function| {
                matches!(
                    digest_function,
                    digest_function::Value::Sha256 | digest_function::Value::Blake3
                )
            },
        ));

        Self {
            digest160,
            digest256,
        }
    }

    fn for_hash(self, hash: &str) -> Option<digest_function::Value> {
        match hash.len() {
            40 => self.digest160,
            64 => self.digest256,
            _ => None,
        }
    }

    fn for_digest(self, digest: &Digest) -> Option<digest_function::Value> {
        self.for_hash(&digest.hash)
    }

    fn for_common_digest_function(self, digests: &[Digest]) -> Option<digest_function::Value> {
        let mut common = None;
        for digest in digests {
            let digest_function = self.for_digest(digest)?;
            if common.is_some_and(|common| common != digest_function) {
                return None;
            }
            common = Some(digest_function);
        }
        common
    }
}

fn digest_function_to_grpc(digest_function: Option<digest_function::Value>) -> i32 {
    digest_function
        .map(|digest_function| digest_function as i32)
        .unwrap_or_default()
}

fn chunking_function_to_grpc(chunking_function: TChunkingFunction) -> i32 {
    (match chunking_function {
        TChunkingFunction::Unknown => chunking_function::Value::Unknown,
        TChunkingFunction::FastCdc2020 => chunking_function::Value::FastCdc2020,
        TChunkingFunction::RepMaxCdc => chunking_function::Value::RepMaxCdc,
    }) as i32
}

fn chunking_function_from_grpc(chunking_function: i32) -> TChunkingFunction {
    match chunking_function::Value::try_from(chunking_function).ok() {
        Some(chunking_function::Value::FastCdc2020) => TChunkingFunction::FastCdc2020,
        Some(chunking_function::Value::RepMaxCdc) => TChunkingFunction::RepMaxCdc,
        Some(chunking_function::Value::Unknown) | None => TChunkingFunction::Unknown,
    }
}

fn digest_function_resource_segment(
    digest_function: Option<digest_function::Value>,
) -> Option<&'static str> {
    match digest_function {
        Some(digest_function::Value::Blake3) => Some("blake3"),
        _ => None,
    }
}

fn unique_digest_function(
    digest_functions: impl Iterator<Item = digest_function::Value>,
) -> Option<digest_function::Value> {
    let mut unique = None;
    for digest_function in digest_functions {
        if unique.is_some_and(|unique| unique != digest_function) {
            return None;
        }
        unique = Some(digest_function);
    }
    unique
}

fn validate_remote_execution_enabled(execution_enabled: Option<bool>) -> anyhow::Result<()> {
    match execution_enabled {
        Some(false) => Err(anyhow::anyhow!(concat!(
            "Remote execution is not supported by the remote server or the ",
            "current account is not authorized to use remote execution"
        ))),
        Some(true) | None => Ok(()),
    }
}

fn validate_blob_split_supported(blob_split_supported: bool) -> anyhow::Result<()> {
    if blob_split_supported {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "CAS SplitBlob is not supported by the remote server"
        ))
    }
}

fn validate_blob_splice_supported(blob_splice_supported: bool) -> anyhow::Result<()> {
    if blob_splice_supported {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "CAS SpliceBlob is not supported by the remote server"
        ))
    }
}

fn action_cache_update_enabled_from_capabilities(
    cache_capabilities: Option<&CacheCapabilities>,
) -> bool {
    cache_capabilities
        .and_then(|cache_cap| cache_cap.action_cache_update_capabilities.as_ref())
        .is_some_and(|capabilities| capabilities.update_enabled)
}

fn execution_enabled_from_capabilities(
    execution_capabilities: Option<&ExecutionCapabilities>,
) -> bool {
    execution_capabilities.is_some_and(|capabilities| capabilities.exec_enabled)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ApiVersion {
    major: i32,
    minor: i32,
    patch: i32,
    prerelease: String,
}

impl ApiVersion {
    fn client_low() -> Self {
        Self::new(2, 0, 0, "")
    }

    fn client_high() -> Self {
        Self::new(2, 11, 0, "")
    }

    fn new(major: i32, minor: i32, patch: i32, prerelease: &str) -> Self {
        Self {
            major,
            minor,
            patch,
            prerelease: prerelease.to_owned(),
        }
    }

    fn from_semver(semver: Option<&SemVer>) -> Self {
        let Some(semver) = semver else {
            return Self::new(0, 0, 0, "");
        };

        Self {
            major: semver.major,
            minor: semver.minor,
            patch: semver.patch,
            prerelease: semver.prerelease.clone(),
        }
    }
}

impl std::fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.prerelease.is_empty() {
            return f.write_str(&self.prerelease);
        }
        if self.patch != 0 {
            write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
        } else {
            write!(f, "{}.{}", self.major, self.minor)
        }
    }
}

impl Ord for ApiVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.prerelease.is_empty(), other.prerelease.is_empty()) {
            (false, true) => return std::cmp::Ordering::Less,
            (true, false) => return std::cmp::Ordering::Greater,
            (false, false) => return self.prerelease.cmp(&other.prerelease),
            (true, true) => {}
        }

        self.major
            .cmp(&other.major)
            .then_with(|| self.minor.cmp(&other.minor))
            .then_with(|| self.patch.cmp(&other.patch))
    }
}

impl PartialOrd for ApiVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn highest_supported_api_version(
    server_low: &ApiVersion,
    server_high: &ApiVersion,
) -> Option<ApiVersion> {
    let client_low = ApiVersion::client_low();
    let client_high = ApiVersion::client_high();
    let highest_low = std::cmp::max(client_low, server_low.clone());
    let lowest_high = std::cmp::min(client_high, server_high.clone());

    if highest_low <= lowest_high {
        Some(lowest_high)
    } else {
        None
    }
}

fn validate_re_api_versions(
    low_api_version: Option<&SemVer>,
    high_api_version: Option<&SemVer>,
    deprecated_api_version: Option<&SemVer>,
) -> anyhow::Result<Option<String>> {
    let server_low = ApiVersion::from_semver(low_api_version);
    let server_high = ApiVersion::from_semver(high_api_version);

    if highest_supported_api_version(&server_low, &server_high).is_some() {
        return Ok(None);
    }

    if let Some(deprecated_api_version) = deprecated_api_version {
        let deprecated = ApiVersion::from_semver(Some(deprecated_api_version));
        if let Some(highest) = highest_supported_api_version(&deprecated, &server_high) {
            return Ok(Some(format!(
                "The highest RE API version Buck2 supports {highest} is deprecated by the server. \
                Please upgrade to the server's recommended version: {server_low} to {server_high}."
            )));
        }
    }

    Err(anyhow::anyhow!(
        "The client supported RE API versions, {} to {}, are not supported by the server, {} to {}. Please switch to a different server or upgrade Buck2.",
        ApiVersion::client_low(),
        ApiVersion::client_high(),
        server_low,
        server_high,
    ))
}

fn request_metadata_tool_name_from_options(
    opts: &Buck2OssReConfiguration,
) -> anyhow::Result<String> {
    let request_metadata_tool_name = opts
        .request_metadata_tool_name
        .clone()
        .unwrap_or_else(|| DEFAULT_REQUEST_METADATA_TOOL_NAME.to_owned());
    anyhow::ensure!(
        !request_metadata_tool_name.is_empty(),
        "`request_metadata_tool_name` must not be empty"
    );
    Ok(request_metadata_tool_name)
}

pub struct REClientBuilder;

impl REClientBuilder {
    pub async fn build_and_connect(opts: &Buck2OssReConfiguration) -> anyhow::Result<REClient> {
        let request_metadata_tool_name = request_metadata_tool_name_from_options(opts)?;
        let tls_config = Arc::new(tokio::sync::OnceCell::new());
        let channel_settings = GrpcChannelSettings::from_options(opts);
        let credential_helper = CredentialHelperSettings::from_options(
            opts.credential_helper.as_deref(),
            opts.credential_helper_timeout_secs,
            opts.credential_helper_cache_secs,
        )
        .map(|settings| settings.build())
        .transpose()?
        .map(Arc::new);

        // CAS traffic goes to the machine-local CAS daemon when one is configured; it is a Unix
        // socket or a loopback port and never uses TLS. The daemon passes misses and uploads
        // through to `cas_address`.
        let configured_cache_dir = opts
            .cas_shared_cache
            .as_deref()
            .map(|path| {
                substitute_env_vars(path)
                    .context("Invalid `cas_shared_cache`")
                    .map(PathBuf::from)
            })
            .transpose()?;
        // The daemon's working directory is `buck-out/<isolation dir>` (buck2_fs's cwd.rs moves it
        // there), so a clone into it tells whether blobs can be reflinked into buck-out. When the
        // probe fails, the daemon is left out as well: nothing would clone its blobs.
        let casd_refused = configured_cache_dir.is_some();
        let shared_cache_dir = match configured_cache_dir {
            Some(dir) => {
                let buck_out = std::env::current_dir()
                    .context("Error reading the daemon's working directory")?;
                crate::shared_cache::shared_cache_dir_if_reflink(dir, &buck_out)
            }
            None => None,
        };
        let casd_refused = casd_refused && shared_cache_dir.is_none();
        let daemon_address = if casd_refused {
            None
        } else if shared_cache_dir.is_some() || opts.cas_shared_cache_address.is_some() {
            Some(DaemonAddress::resolve(
                opts.cas_shared_cache_address.as_ref(),
                shared_cache_dir.as_deref(),
                substitute_env_vars,
            )?)
        } else {
            None
        };
        anyhow::ensure!(
            daemon_address.is_none() || !opts.remote_cache_chunking,
            "`remote_cache_chunking` cannot be combined with `cas_shared_cache`: buck2-casd does \
             not implement SplitBlob or SpliceBlob"
        );
        // buck2-casd reaches the remote CAS with its own client, which has no credential helper,
        // so the helper's credentials would stop at the daemon. Checked against the configured
        // address, before the daemon is asked for its upstream: a refusal below clears
        // `daemon_address`, but the configuration is wrong whenever the daemon would be used.
        anyhow::ensure!(
            daemon_address.is_none() || credential_helper.is_none(),
            "`credential_helper` cannot be combined with `cas_shared_cache`: buck2-casd does not \
             call the helper for its upstream requests"
        );
        // The channels below connect eagerly, so the daemon has to answer first.
        let cas_daemon = match (&daemon_address, &shared_cache_dir) {
            (Some(address), Some(dir)) if opts.cas_shared_cache_autostart.unwrap_or(true) => {
                let daemon = Arc::new(CasDaemonLauncher {
                    opts: opts.clone(),
                    address: address.clone(),
                    dir: dir.clone(),
                });
                daemon
                    .ensure_running()
                    .await
                    .context("Error auto-starting buck2-casd")?;
                Some(daemon)
            }
            _ => None,
        };
        // The daemon answering may have been started for another CAS, even right after this
        // client spawned one, and using it would send every blob there. If so, the client goes
        // to its own CAS directly, and leaves the directory alone too.
        let refused = match &daemon_address {
            Some(address) => !crate::casd_autostart::serves_this_cas(opts, address).await?,
            None => false,
        };
        let (daemon_address, shared_cache_dir, cas_daemon) = if refused {
            (None, None, None)
        } else {
            (daemon_address, shared_cache_dir, cas_daemon)
        };
        // Asked again before each connection to the daemon, for as long as the client lives.
        let upstream_check = match &daemon_address {
            Some(address) => {
                crate::casd_autostart::UpstreamCheck::new(opts, address)?.map(Arc::new)
            }
            None => None,
        };
        let cas_address = match &daemon_address {
            Some(address) => Some(address.pool_address()),
            None => opts.cas_address.clone(),
        };
        let shared_cache = match shared_cache_dir {
            Some(dir) if !matches!(opts.cas_shared_cache_mode, Some(CASdMode::Remote)) => Some(
                SharedCasCache::new(
                    dir,
                    opts.cas_shared_cache_copy_policy
                        .unwrap_or(CopyPolicy::Hybrid),
                )
                .context("Error opening the shared CAS cache directory")?,
            ),
            _ => None,
        };

        let cas_connector = GrpcChannelConnector::new(
            cas_address.clone(),
            channel_settings.clone(),
            tls_config.clone(),
            credential_helper.clone(),
        )
        .plaintext(daemon_address.is_some())
        .upstream_check(upstream_check.clone());
        let execution_connector = GrpcChannelConnector::new(
            opts.engine_address.clone(),
            channel_settings.clone(),
            tls_config.clone(),
            credential_helper.clone(),
        );
        let action_cache_connector = GrpcChannelConnector::new(
            opts.action_cache_address.clone(),
            channel_settings.clone(),
            tls_config.clone(),
            credential_helper.clone(),
        );
        let bytestream_connector = GrpcChannelConnector::new(
            cas_address,
            channel_settings.clone(),
            tls_config.clone(),
            credential_helper.clone(),
        )
        .plaintext(daemon_address.is_some())
        .upstream_check(upstream_check);
        let capabilities_connector = GrpcChannelConnector::new(
            opts.engine_address.clone(),
            channel_settings,
            tls_config.clone(),
            credential_helper.clone(),
        );

        let (cas, bytestream, capabilities) = futures::future::join3(
            cas_connector.connect(),
            bytestream_connector.connect(),
            capabilities_connector.connect(),
        )
        .await;

        let execution_connection_count = configured_connection_count(
            opts.engine_connection_count,
            opts.execution_concurrency_limit,
        );
        let execution_channels = futures::future::try_join_all(
            (0..execution_connection_count).map(|_| execution_connector.connect()),
        )
        .await
        .context("Error creating Execution clients")?;
        // Sized like the Execution pool, since a remote action makes one lookup and one Execute.
        // The pool is for a connection that stops answering, which then holds 1/N of the lookups
        // rather than all of them: on 2026-10-03 the single one stalled for 96 s, and all 343
        // lookups in flight on it timed out.
        let action_cache_connection_count = configured_connection_count(
            opts.action_cache_connection_count,
            opts.execution_concurrency_limit,
        );
        let action_cache_channels = futures::future::try_join_all(
            (0..action_cache_connection_count).map(|_| action_cache_connector.connect()),
        )
        .await
        .context("Error creating ActionCache clients")?;

        let interceptor = InjectHeadersInterceptor::new(&opts.http_headers)?;

        let capabilities_client = ResettableGrpcClient::new(
            capabilities.context("Error creating Capabilities client")?,
            capabilities_connector,
            interceptor.dupe(),
            opts.max_decoding_message_size
                .unwrap_or(DEFAULT_MAX_TOTAL_BATCH_SIZE * 2),
            build_capabilities_client,
        );

        let instance_name = InstanceName(opts.instance_name.clone());
        let retries = opts.retries.unwrap_or(DEFAULT_RETRIES);
        let retry_max_delay_ms = opts
            .retry_max_delay_ms
            .unwrap_or(DEFAULT_RETRY_MAX_DELAY_MILLIS);
        let grpc_request_timeout = Duration::from_secs(
            opts.grpc_request_timeout_secs
                .unwrap_or(DEFAULT_GRPC_REQUEST_TIMEOUT_SECS),
        );
        let bytestream_progress_timeout = Duration::from_secs(
            opts.bytestream_progress_timeout_secs
                .unwrap_or(DEFAULT_BYTESTREAM_PROGRESS_TIMEOUT_SECS),
        );
        let queued_operation_timeout = Duration::from_secs(
            opts.queued_operation_timeout_secs
                .unwrap_or(DEFAULT_QUEUED_OPERATION_TIMEOUT_SECS),
        );
        let stalled_operation_timeout = Duration::from_secs(
            opts.stalled_operation_timeout_secs
                .unwrap_or(DEFAULT_STALLED_OPERATION_TIMEOUT_SECS),
        );

        let capabilities = if opts.capabilities.unwrap_or(true) {
            Self::fetch_rbe_capabilities(
                &capabilities_client,
                &instance_name,
                opts.max_total_batch_size,
                retries,
                retry_max_delay_ms,
                grpc_request_timeout,
            )
            .await?
        } else {
            RECapabilities {
                capabilities_queried: false,
                max_total_batch_size: DEFAULT_MAX_TOTAL_BATCH_SIZE,
                max_cas_blob_size_bytes: None,
                supported_compressors: Vec::new(),
                supported_batch_update_compressors: Vec::new(),
                supported_digest_functions: Vec::new(),
                cache_digest_functions: Vec::new(),
                execution_digest_functions: Vec::new(),
                execution_priority_ranges: Vec::new(),
                action_cache_update_enabled: None,
                execution_enabled: None,
                blob_split_supported: false,
                blob_splice_supported: false,
                fast_cdc_2020: None,
            }
        };

        validate_digest_function_capabilities(&opts.digest_algorithms, &capabilities)?;
        validate_remote_cache_chunking_enabled(opts.remote_cache_chunking, &capabilities)?;

        let download_hash_digest_function = select_download_hash_digest_function(
            &opts.digest_algorithms,
            &capabilities.supported_digest_functions,
        )?;
        let request_digest_function_config =
            DigestFunctionConfig::from_configured_algorithms(&opts.digest_algorithms);
        let local_chunk_cache = opts
            .remote_cache_chunk_cache_dir
            .as_deref()
            .map(|path| {
                let path =
                    substitute_env_vars(path).context("Invalid `remote_cache_chunk_cache_dir`")?;
                anyhow::ensure!(
                    !path.is_empty(),
                    "`remote_cache_chunk_cache_dir` must not be empty"
                );
                Ok(LocalChunkCache::new(PathBuf::from(path)))
            })
            .transpose()?;

        let max_decoding_msg_size = opts
            .max_decoding_message_size
            .unwrap_or(capabilities.max_total_batch_size * 2);

        if max_decoding_msg_size < capabilities.max_total_batch_size {
            return Err(anyhow::anyhow!(
                "Attribute `max_decoding_message_size` must always be equal or higher to `max_total_batch_size`"
            ));
        }

        // Choose compressors for ByteStream and inlined batch uploads.
        let bystream_compressor = select_preferred_compressor(&capabilities.supported_compressors);
        // Capabilities come from the engine, but batch uploads go to buck2-casd when one is
        // configured, and it rejects compressed batch updates. Its own upstream client picks
        // compressors from the remote's capabilities, so the network leg keeps its compression.
        let batch_update_compressor = if daemon_address.is_some() {
            None
        } else {
            select_preferred_compressor(&capabilities.supported_batch_update_compressors)
        };
        let remote_cache_compression_threshold = opts
            .remote_cache_compression_threshold
            .unwrap_or(DEFAULT_REMOTE_CACHE_COMPRESSION_THRESHOLD);

        tracing::info!(
            max_total_batch_size = capabilities.max_total_batch_size,
            max_cas_blob_size_bytes = ?capabilities.max_cas_blob_size_bytes,
            supported_digest_functions = %digest_function_names(&capabilities.supported_digest_functions),
            execution_priority_ranges = %priority_range_names(&capabilities.execution_priority_ranges),
            selected_download_hash_digest_function = %download_hash_digest_function
                .map(digest_function_name)
                .unwrap_or("<auto>"),
            supported_compressors = %compressor_names(&capabilities.supported_compressors),
            selected_bystream_compressor = %bystream_compressor
                .map(|compressor| compressor.name())
                .unwrap_or("<none>"),
            supported_batch_update_compressors = %compressor_names(
                &capabilities.supported_batch_update_compressors
            ),
            selected_batch_update_compressor = %batch_update_compressor
                .map(|compressor| compressor.name())
                .unwrap_or("<none>"),
            remote_cache_compression_threshold,
            action_cache_update_enabled = ?capabilities.action_cache_update_enabled,
            execution_enabled = ?capabilities.execution_enabled,
            blob_split_supported = capabilities.blob_split_supported,
            blob_splice_supported = capabilities.blob_splice_supported,
            fast_cdc_2020_avg_chunk_size_bytes = ?capabilities.fast_cdc_2020
                .as_ref()
                .map(|config| config.avg_chunk_size_bytes),
            fast_cdc_2020_seed = ?capabilities.fast_cdc_2020
                .as_ref()
                .map(|config| config.seed),
            local_fast_cdc_chunk_cache_dir = ?local_chunk_cache
                .as_ref()
                .map(|cache| cache.root.display().to_string()),
            execution_connection_count = execution_connection_count,
            action_cache_connection_count = action_cache_connection_count,
            "RE server capabilities"
        );

        let grpc_clients = GRPCClients {
            cas_client: ResettableGrpcClient::new(
                cas.context("Error creating CAS client")?,
                cas_connector,
                interceptor.dupe(),
                max_decoding_msg_size,
                build_cas_client,
            ),
            execution_client: ResettableGrpcClientPool::new(
                execution_channels,
                execution_connector,
                interceptor.dupe(),
                max_decoding_msg_size,
                build_execution_client,
            ),
            action_cache_client: ResettableGrpcClientPool::new(
                action_cache_channels,
                action_cache_connector,
                interceptor.dupe(),
                max_decoding_msg_size,
                build_action_cache_client,
            ),
            bytestream_client: ResettableGrpcClient::new(
                bytestream.context("Error creating Bytestream client")?,
                bytestream_connector,
                interceptor.dupe(),
                max_decoding_msg_size,
                build_bytestream_client,
            ),
            credential_helper,
            cas_daemon,
        };

        Ok(REClient::new(
            RERuntimeOpts {
                use_fbcode_metadata: opts.use_fbcode_metadata,
                request_metadata_tool_name,
                max_concurrent_uploads_per_action: opts.max_concurrent_uploads_per_action,
                // NOTE: This is an arbitrary number because RBE does not return information
                // on the TTL of the remote blob.
                cas_ttl_secs: opts.cas_ttl_secs.unwrap_or(3 * 60 * 60),
                find_missing_blobs_batch_size: opts
                    .find_missing_blobs_batch_size
                    .unwrap_or(100)
                    .max(1),
                remote_cache_chunking: opts.remote_cache_chunking,
                remote_cache_compression_threshold,
                retries,
                retry_max_delay_ms,
                grpc_request_timeout,
                bytestream_progress_timeout,
                queued_operation_timeout,
                stalled_operation_timeout,
                download_hash_digest_function,
                request_digest_function_config,
            },
            grpc_clients,
            capabilities,
            instance_name,
            bystream_compressor,
            batch_update_compressor,
            local_chunk_cache,
            shared_cache,
        ))
    }

    async fn fetch_rbe_capabilities(
        client: &ResettableGrpcClient<CapabilitiesClient<GrpcService>>,
        instance_name: &InstanceName,
        max_total_batch_size: Option<usize>,
        retries: usize,
        retry_max_delay_ms: u64,
        grpc_request_timeout: Duration,
    ) -> anyhow::Result<RECapabilities> {
        // TODO use more of the capabilities of the remote build executor

        let resp = retry_grpc_request(retries, Duration::from_millis(retry_max_delay_ms), || {
            let mut request = tonic::Request::new(GetCapabilitiesRequest {
                instance_name: instance_name.as_str().to_owned(),
            });
            request.set_timeout(grpc_request_timeout);
            async move {
                let mut client = client.client().await?;
                Ok(client.get_capabilities(request).await?.into_inner())
            }
        })
        .await
        .context("Failed to query capabilities of remote")?;

        if let Some(warning) = validate_re_api_versions(
            resp.low_api_version.as_ref(),
            resp.high_api_version.as_ref(),
            resp.deprecated_api_version.as_ref(),
        )? {
            tracing::warn!("{}", warning);
        }

        let supported_compressors = if let Some(cache_cap) = &resp.cache_capabilities {
            cache_cap
                .supported_compressors
                .iter()
                .copied()
                .filter_map(Compressor::from_grpc)
                .collect()
        } else {
            Vec::new()
        };
        let supported_batch_update_compressors = if let Some(cache_cap) = &resp.cache_capabilities {
            cache_cap
                .supported_batch_update_compressors
                .iter()
                .copied()
                .filter_map(Compressor::from_grpc)
                .collect()
        } else {
            Vec::new()
        };

        let (cache_digest_functions, assumed_sha256_cache_digest_function) =
            cache_digest_functions_from_capabilities(resp.cache_capabilities.as_ref());
        if assumed_sha256_cache_digest_function {
            tracing::warn!(
                "Remote cache capabilities did not advertise digest functions; assuming SHA256. \
                Configure `[buck2] digest_algorithms` only when the remote cache advertises \
                matching digest function support."
            );
        }

        let mut execution_digest_functions = resp
            .execution_capabilities
            .as_ref()
            .map(|exec_cap| {
                if exec_cap.digest_functions.is_empty() {
                    digest_function_from_grpc(exec_cap.digest_function)
                        .into_iter()
                        .collect()
                } else {
                    exec_cap
                        .digest_functions
                        .iter()
                        .copied()
                        .filter_map(digest_function_from_grpc)
                        .collect::<Vec<_>>()
                }
            })
            .unwrap_or_default();
        execution_digest_functions.sort_unstable();
        execution_digest_functions.dedup();

        let mut supported_digest_functions = cache_digest_functions.clone();
        if supported_digest_functions.is_empty() {
            supported_digest_functions.extend(execution_digest_functions.iter().copied());
        }
        supported_digest_functions.sort_unstable();
        supported_digest_functions.dedup();

        let max_total_batch_size_from_capabilities: Option<usize> =
            resp.cache_capabilities.as_ref().and_then(|cache_cap| {
                let size = cache_cap.max_batch_total_size_bytes as usize;
                // A value of 0 means no limit is set
                if size != 0 { Some(size) } else { None }
            });

        let max_total_batch_size =
            match (max_total_batch_size_from_capabilities, max_total_batch_size) {
                (Some(cap), Some(config)) => std::cmp::min(cap, config),
                (Some(cap), None) => cap,
                (None, Some(config)) => config,
                (None, None) => DEFAULT_MAX_TOTAL_BATCH_SIZE,
            };

        Ok(RECapabilities {
            capabilities_queried: true,
            max_total_batch_size,
            max_cas_blob_size_bytes: resp.cache_capabilities.as_ref().and_then(|cache_cap| {
                let size = cache_cap.max_cas_blob_size_bytes;
                if size > 0 { Some(size) } else { None }
            }),
            supported_compressors,
            supported_batch_update_compressors,
            supported_digest_functions,
            cache_digest_functions,
            execution_digest_functions,
            execution_priority_ranges: resp
                .execution_capabilities
                .as_ref()
                .and_then(|exec_cap| exec_cap.execution_priority_capabilities.as_ref())
                .map(priority_ranges)
                .unwrap_or_default(),
            action_cache_update_enabled: Some(action_cache_update_enabled_from_capabilities(
                resp.cache_capabilities.as_ref(),
            )),
            execution_enabled: Some(execution_enabled_from_capabilities(
                resp.execution_capabilities.as_ref(),
            )),
            blob_split_supported: resp
                .cache_capabilities
                .as_ref()
                .is_some_and(|cache_cap| cache_cap.split_blob_support),
            blob_splice_supported: resp
                .cache_capabilities
                .as_ref()
                .is_some_and(|cache_cap| cache_cap.splice_blob_support),
            fast_cdc_2020: fast_cdc_2020_config_from_capabilities(resp.cache_capabilities.as_ref()),
        })
    }
}

#[derive(Clone, Dupe)]
struct InjectHeadersInterceptor {
    /// Static headers from the configuration.
    headers: Arc<Vec<(MetadataKey<metadata::Ascii>, MetadataValue<metadata::Ascii>)>>,
    /// Headers from the credential helper, which take precedence over the static ones.
    credentials: Option<Arc<Credentials>>,
}

impl InjectHeadersInterceptor {
    pub fn new(headers: &[HttpHeader]) -> anyhow::Result<Self> {
        let headers = headers
            .iter()
            .map(|h| {
                // This means we can't have `$` in a header key or value, which isn't great. On the
                // flip side, env vars are good for things like credentials, which those headers
                // are likely to contain. In time, we should allow escaping.
                let key = substitute_env_vars(&h.key)?;
                let value = substitute_env_vars(&h.value)?;

                let key = MetadataKey::<metadata::Ascii>::from_bytes(key.as_bytes())
                    .with_context(|| format!("Invalid key in header: `{key}: {value}`"))?;

                let value = MetadataValue::try_from(&value)
                    .with_context(|| format!("Invalid value in header: `{key}: {value}`"))?;

                anyhow::Ok((key, value))
            })
            .collect::<Result<_, _>>()
            .context("Error converting headers")?;

        Ok(Self {
            headers: Arc::new(headers),
            credentials: None,
        })
    }

    /// An interceptor that also injects the headers of `credentials`, if any.
    fn with_credentials(&self, credentials: Option<Arc<Credentials>>) -> Self {
        Self {
            headers: self.headers.dupe(),
            credentials,
        }
    }
}

impl Interceptor for InjectHeadersInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        for (k, v) in self.headers.iter() {
            request.metadata_mut().insert(k.clone(), v.clone());
        }
        if let Some(credentials) = &self.credentials {
            let metadata = request.metadata_mut();
            // A header from the helper replaces a static header of the same name, but a helper
            // header with several values keeps all of them.
            for (k, _) in credentials.headers() {
                metadata.remove(k.clone());
            }
            for (k, v) in credentials.headers() {
                metadata.append(k.clone(), v.clone());
            }
        }
        Ok(request)
    }
}

type GrpcService = InterceptedService<Channel, InjectHeadersInterceptor>;

#[derive(Clone)]
struct GrpcTlsSettings {
    /// `[buck2_re_client] tls`; overrides what the address scheme implies.
    tls: Option<bool>,
    tls_ca_certs: Option<String>,
    tls_client_cert: Option<String>,
}

#[derive(Clone)]
struct GrpcChannelSettings {
    tls: GrpcTlsSettings,
    grpc_keepalive_time_secs: u64,
    grpc_keepalive_timeout_secs: u64,
    grpc_keepalive_while_idle: bool,
    tcp_keepalive_secs: Option<u64>,
}

impl GrpcChannelSettings {
    fn from_options(opts: &Buck2OssReConfiguration) -> Self {
        Self {
            tls: GrpcTlsSettings {
                tls: opts.tls,
                tls_ca_certs: opts.tls_ca_certs.clone(),
                tls_client_cert: opts.tls_client_cert.clone(),
            },
            grpc_keepalive_time_secs: opts
                .grpc_keepalive_time_secs
                .unwrap_or(DEFAULT_GRPC_KEEPALIVE_TIME_SECS),
            grpc_keepalive_timeout_secs: opts
                .grpc_keepalive_timeout_secs
                .unwrap_or(DEFAULT_GRPC_KEEPALIVE_TIMEOUT_SECS),
            grpc_keepalive_while_idle: opts
                .grpc_keepalive_while_idle
                .unwrap_or(DEFAULT_GRPC_KEEPALIVE_WHILE_IDLE),
            tcp_keepalive_secs: opts.tcp_keepalive_secs,
        }
    }
}

#[derive(Clone)]
struct GrpcChannelConnector {
    address: Option<String>,
    settings: GrpcChannelSettings,
    tls_config: Arc<tokio::sync::OnceCell<ClientTlsConfig>>,
    credential_helper: Option<Arc<CredentialHelper>>,
    /// Never use TLS, whatever `[buck2_re_client] tls` says. Set for the machine-local CAS
    /// daemon, a Unix socket or a loopback port, which must stay reachable when the remote
    /// services require TLS.
    plaintext: bool,
    /// Set for the machine-local CAS daemon: what each new connection to it checks first.
    upstream_check: Option<Arc<crate::casd_autostart::UpstreamCheck>>,
}

impl GrpcChannelConnector {
    fn new(
        address: Option<String>,
        settings: GrpcChannelSettings,
        tls_config: Arc<tokio::sync::OnceCell<ClientTlsConfig>>,
        credential_helper: Option<Arc<CredentialHelper>>,
    ) -> Self {
        Self {
            address,
            settings,
            tls_config,
            credential_helper,
            plaintext: false,
            upstream_check: None,
        }
    }

    fn plaintext(self, plaintext: bool) -> Self {
        Self { plaintext, ..self }
    }

    fn upstream_check(
        self,
        upstream_check: Option<Arc<crate::casd_autostart::UpstreamCheck>>,
    ) -> Self {
        Self {
            upstream_check,
            ..self
        }
    }

    fn address(&self) -> anyhow::Result<String> {
        let address = self.address.as_ref().context("No address")?;
        substitute_env_vars(address).context("Invalid address")
    }

    fn uri(&self, address: &str) -> anyhow::Result<(Uri, bool)> {
        let uri = address.parse().context("Invalid address")?;
        let tls_override = if self.plaintext {
            Some(false)
        } else {
            self.settings.tls.tls
        };
        prepare_uri(uri, tls_override).context("Invalid URI")
    }

    /// The helper is asked for the address with the scheme the TLS setting implies,
    /// `https://host:port/`, which is what a helper written for Bazel keys its answers on.
    async fn credentials(&self) -> anyhow::Result<Option<Arc<Credentials>>> {
        let Some(helper) = &self.credential_helper else {
            return Ok(None);
        };
        let (uri, _tls) = self.uri(&self.address()?)?;
        Ok(Some(helper.get(&uri.to_string()).await?))
    }

    fn with_keepalive(&self, endpoint: tonic::transport::Endpoint) -> tonic::transport::Endpoint {
        endpoint
            .http2_keep_alive_interval(Duration::from_secs(self.settings.grpc_keepalive_time_secs))
            .keep_alive_timeout(Duration::from_secs(
                self.settings.grpc_keepalive_timeout_secs,
            ))
            .keep_alive_while_idle(self.settings.grpc_keepalive_while_idle)
    }

    async fn connect(&self) -> anyhow::Result<Channel> {
        let address = self.address()?;
        let connection_error = |error: tonic::transport::Error| {
            anyhow::Error::from(REClientError {
                code: TCode::UNAVAILABLE,
                message: format!("Error connecting to `{address}`: {error:#}"),
                group: TCodeReasonGroup::RE_CONNECTION,
            })
        };

        if let Some(path) = address.strip_prefix("unix://") {
            if !cfg!(unix) {
                return Err(anyhow::anyhow!(
                    "Unix socket addresses are not supported on this platform: `{address}`"
                ));
            }
            // The URI only fills the `:authority` header; the connector ignores it.
            let endpoint =
                self.with_keepalive(Channel::builder(Uri::from_static("http://unix.invalid/")));
            let connector = CountingConnector::new(CheckedConnector::new(
                UnixConnector::new(Arc::new(PathBuf::from(path))),
                self.upstream_check.clone(),
            ));
            return endpoint
                .connect_with_connector(connector)
                .await
                .map_err(connection_error);
        }

        let (uri, tls) = self.uri(&address)?;

        let mut endpoint = Channel::builder(uri);
        if tls {
            let tls_config = self
                .tls_config
                .get_or_try_init(|| async {
                    create_tls_config(&self.settings.tls)
                        .await
                        .context("Invalid TLS config")
                })
                .await?
                .clone();
            endpoint = endpoint.tls_config(tls_config)?;
        }

        let endpoint = self.with_keepalive(endpoint);

        // Since we are creating the HttpConnector ourselves, any TCP settings
        // need to be set here instead of on the endpoint.
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        if let Some(tcp_keepalive_secs) = self.settings.tcp_keepalive_secs {
            http.set_keepalive(Some(Duration::from_secs(tcp_keepalive_secs)));
        }
        let connector =
            CountingConnector::new(CheckedConnector::new(http, self.upstream_check.clone()));

        endpoint
            .connect_with_connector(connector)
            .await
            .map_err(connection_error)
    }
}

/// How to bring the machine-local CAS daemon back when it disappears while this client lives:
/// it crashed, `buck2 killall` stopped it, or its directory was removed.
struct CasDaemonLauncher {
    opts: Buck2OssReConfiguration,
    address: DaemonAddress,
    dir: PathBuf,
}

impl CasDaemonLauncher {
    async fn ensure_running(&self) -> anyhow::Result<()> {
        crate::casd_autostart::ensure_running(
            &self.opts,
            &self.address,
            &self.dir,
            substitute_env_vars,
        )
        .await
        .context("Error starting buck2-casd")
    }
}

/// Every request in flight on a connection that breaks fails at about the same moment, and each
/// asks for a reconnect. One reconnect serves all of them, so another within this interval is
/// skipped. Nothing is lost by that: tonic's Channel redials a connection that closes under it on
/// its own (tonic 0.14.6 src/transport/channel/service/reconnect.rs:111-130).
const RECONNECT_MIN_INTERVAL: Duration = Duration::from_secs(1);

struct ResettableGrpcClient<C> {
    channel: tokio::sync::RwLock<Channel>,
    /// When the channel was last replaced. Held through a reconnect, so the reconnects that
    /// arrive meanwhile wait for it and then find it fresh.
    reconnected_at: tokio::sync::Mutex<Option<Instant>>,
    connector: GrpcChannelConnector,
    interceptor: InjectHeadersInterceptor,
    max_decoding_message_size: usize,
    build_client: fn(Channel, InjectHeadersInterceptor, usize) -> C,
}

impl<C> ResettableGrpcClient<C> {
    fn new(
        channel: Channel,
        connector: GrpcChannelConnector,
        interceptor: InjectHeadersInterceptor,
        max_decoding_message_size: usize,
        build_client: fn(Channel, InjectHeadersInterceptor, usize) -> C,
    ) -> Self {
        Self {
            channel: tokio::sync::RwLock::new(channel),
            reconnected_at: tokio::sync::Mutex::new(None),
            connector,
            interceptor,
            max_decoding_message_size,
            build_client,
        }
    }

    /// A client for one request. It is built here rather than once, so the request carries the
    /// credentials current at this moment; the channel underneath is shared and outlives them.
    async fn client(&self) -> anyhow::Result<C> {
        let credentials = self.connector.credentials().await?;
        let channel = self.channel.read().await.clone();
        Ok((self.build_client)(
            channel,
            self.interceptor.with_credentials(credentials),
            self.max_decoding_message_size,
        ))
    }

    async fn reconnect(&self) -> anyhow::Result<()> {
        let mut reconnected_at = self.reconnected_at.lock().await;
        if reconnected_at.is_some_and(|at| at.elapsed() < RECONNECT_MIN_INTERVAL) {
            return Ok(());
        }
        let channel = self.connector.connect().await?;
        *self.channel.write().await = channel;
        *reconnected_at = Some(Instant::now());
        Ok(())
    }
}

fn configured_connection_count(
    count: Option<usize>,
    execution_concurrency_limit: Option<usize>,
) -> usize {
    if let Some(count) = count {
        return count.max(1);
    }

    let execution_concurrency_limit =
        execution_concurrency_limit.unwrap_or(DEFAULT_EXECUTION_CONCURRENCY_LIMIT);
    let connections = execution_concurrency_limit.div_ceil(DEFAULT_ENGINE_REQUESTS_PER_CONNECTION);
    connections.clamp(1, DEFAULT_MAX_ENGINE_CONNECTION_COUNT)
}

struct ResettableGrpcClientPool<C> {
    clients: Vec<ResettableGrpcClient<C>>,
    next_client: AtomicUsize,
}

impl<C> ResettableGrpcClientPool<C> {
    fn new(
        channels: Vec<Channel>,
        connector: GrpcChannelConnector,
        interceptor: InjectHeadersInterceptor,
        max_decoding_message_size: usize,
        build_client: fn(Channel, InjectHeadersInterceptor, usize) -> C,
    ) -> Self {
        let clients = channels
            .into_iter()
            .map(|channel| {
                ResettableGrpcClient::new(
                    channel,
                    connector.clone(),
                    interceptor.dupe(),
                    max_decoding_message_size,
                    build_client,
                )
            })
            .collect();
        Self {
            clients,
            next_client: AtomicUsize::new(0),
        }
    }

    async fn client(&self) -> anyhow::Result<C> {
        Ok(self.next_client().await?.1)
    }

    /// The next member in turn, and a client on it. The member is what `reconnect_member` takes.
    async fn next_client(&self) -> anyhow::Result<(usize, C)> {
        let member = self.next_client.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        Ok((member, self.clients[member].client().await?))
    }

    /// Redials one member and leaves the others' connections alone.
    async fn reconnect_member(&self, member: usize) -> anyhow::Result<()> {
        self.clients[member].reconnect().await
    }

    async fn reconnect(&self) -> anyhow::Result<()> {
        let mut last_error = None;
        for client in &self.clients {
            if let Err(error) = client.reconnect().await {
                last_error = Some(error);
            }
        }
        match last_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn build_cas_client(
    channel: Channel,
    interceptor: InjectHeadersInterceptor,
    max_decoding_message_size: usize,
) -> ContentAddressableStorageClient<GrpcService> {
    ContentAddressableStorageClient::with_interceptor(channel, interceptor)
        .max_decoding_message_size(max_decoding_message_size)
}

fn build_execution_client(
    channel: Channel,
    interceptor: InjectHeadersInterceptor,
    max_decoding_message_size: usize,
) -> ExecutionClient<GrpcService> {
    ExecutionClient::with_interceptor(channel, interceptor)
        .max_decoding_message_size(max_decoding_message_size)
}

fn build_action_cache_client(
    channel: Channel,
    interceptor: InjectHeadersInterceptor,
    max_decoding_message_size: usize,
) -> ActionCacheClient<GrpcService> {
    ActionCacheClient::with_interceptor(channel, interceptor)
        .max_decoding_message_size(max_decoding_message_size)
}

fn build_bytestream_client(
    channel: Channel,
    interceptor: InjectHeadersInterceptor,
    max_decoding_message_size: usize,
) -> ByteStreamClient<GrpcService> {
    ByteStreamClient::with_interceptor(channel, interceptor)
        .max_decoding_message_size(max_decoding_message_size)
}

fn build_capabilities_client(
    channel: Channel,
    interceptor: InjectHeadersInterceptor,
    max_decoding_message_size: usize,
) -> CapabilitiesClient<GrpcService> {
    CapabilitiesClient::with_interceptor(channel, interceptor)
        .max_decoding_message_size(max_decoding_message_size)
}

#[derive(Debug, Copy, Clone)]
enum GrpcClientKind {
    Cas,
    Execution,
    /// The member of the ActionCache pool the request went out on. A reconnect redials that
    /// member only: a lookup times out because its own connection stopped answering, and the
    /// other members' connections are still answering theirs.
    ActionCache {
        member: usize,
    },
    ByteStream,
}

pub struct GRPCClients {
    cas_client: ResettableGrpcClient<ContentAddressableStorageClient<GrpcService>>,
    execution_client: ResettableGrpcClientPool<ExecutionClient<GrpcService>>,
    action_cache_client: ResettableGrpcClientPool<ActionCacheClient<GrpcService>>,
    bytestream_client: ResettableGrpcClient<ByteStreamClient<GrpcService>>,
    /// The helper every connector above asks; held here to drop its cache when a remote
    /// rejects what it handed out.
    credential_helper: Option<Arc<CredentialHelper>>,
    /// Set when CAS and ByteStream go to an auto-started buck2-casd.
    cas_daemon: Option<Arc<CasDaemonLauncher>>,
}

impl GRPCClients {
    async fn cas_client(&self) -> anyhow::Result<ContentAddressableStorageClient<GrpcService>> {
        self.cas_client.client().await
    }

    async fn execution_client(&self) -> anyhow::Result<ExecutionClient<GrpcService>> {
        self.execution_client.client().await
    }

    /// A client on the next member of the ActionCache pool, and that member, for
    /// `GrpcClientKind::ActionCache`.
    async fn action_cache_client(&self) -> anyhow::Result<(usize, ActionCacheClient<GrpcService>)> {
        self.action_cache_client.next_client().await
    }

    async fn bytestream_client(&self) -> anyhow::Result<ByteStreamClient<GrpcService>> {
        self.bytestream_client.client().await
    }

    /// True when the next attempt will carry different credentials.
    async fn recover(&self, kind: GrpcClientKind, recovery: Recovery) -> bool {
        match recovery {
            Recovery::None => false,
            Recovery::Reconnect => {
                self.reconnect_after_broken_connection(kind).await;
                false
            }
            Recovery::RefreshCredentials => match &self.credential_helper {
                Some(helper) => {
                    helper.invalidate().await;
                    true
                }
                None => false,
            },
        }
    }

    async fn reconnect(&self, kind: GrpcClientKind) -> anyhow::Result<()> {
        // A broken connection to buck2-casd most likely means the daemon is gone, and redialing
        // a socket nobody listens on only fails again. Start a successor at the same address
        // first. Whoever answers there then, a successor or a daemon another buck2 daemon
        // started, is asked for its upstream by the connector before it gets any traffic.
        if let (GrpcClientKind::Cas | GrpcClientKind::ByteStream, Some(daemon)) =
            (kind, &self.cas_daemon)
        {
            daemon.ensure_running().await?;
        }
        match kind {
            GrpcClientKind::Cas => self.cas_client.reconnect().await,
            GrpcClientKind::Execution => self.execution_client.reconnect().await,
            GrpcClientKind::ActionCache { member } => {
                self.action_cache_client.reconnect_member(member).await
            }
            GrpcClientKind::ByteStream => self.bytestream_client.reconnect().await,
        }
    }

    async fn reconnect_after_broken_connection(&self, kind: GrpcClientKind) {
        match self.reconnect(kind).await {
            Ok(()) => {
                tracing::debug!(?kind, "Reconnected gRPC client after transport failure");
            }
            Err(error) => {
                tracing::debug!(
                    ?kind,
                    error = %error,
                    "Failed to reconnect gRPC client after transport failure"
                );
            }
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum DigestRemoteState {
    ExistsOnRemote,
    Missing,
}

struct FindMissingCache {
    cache: LruCache<TDigest, DigestRemoteState>,
    /// To avoid a situation where we cache that an artifact is available remotely, but the artifact then expires
    /// we clear our local cache once every `ttl`.
    ttl: Duration,
    last_check: Instant,
}

impl FindMissingCache {
    fn clear_if_ttl_expires(&mut self) {
        if self.last_check.elapsed() > self.ttl {
            self.cache.clear();
            self.last_check = Instant::now();
        }
    }

    pub fn get(&mut self, digest: &TDigest) -> Option<DigestRemoteState> {
        self.clear_if_ttl_expires();
        self.cache.get(digest).copied()
    }

    pub fn put(&mut self, digest: TDigest, state: DigestRemoteState) {
        self.clear_if_ttl_expires();
        self.cache.put(digest, state);
    }
}

struct ActiveTransferState<T> {
    sender: tokio::sync::watch::Sender<Option<Result<Arc<T>, String>>>,
    receiver: tokio::sync::watch::Receiver<Option<Result<Arc<T>, String>>>,
}

impl<T> ActiveTransferState<T> {
    fn new() -> Self {
        let (sender, receiver) = tokio::sync::watch::channel(None);
        Self { sender, receiver }
    }
}

struct ActiveTransferRegistry<T> {
    active: Mutex<HashMap<TDigest, Arc<ActiveTransferState<T>>>>,
}

impl<T> ActiveTransferRegistry<T> {
    fn new() -> Self {
        Self {
            active: Mutex::new(HashMap::new()),
        }
    }

    // Only coalesce work while a same-digest ByteStream transfer is active.
    // Completed transfers are removed immediately; this is not a cache.
    fn enter(&self, digest: TDigest) -> ActiveTransfer<'_, T> {
        let mut active = self.active.lock().unwrap();
        if let Some(state) = active.get(&digest) {
            return ActiveTransfer::Follower(state.dupe());
        }

        let state = Arc::new(ActiveTransferState::new());
        active.insert(digest.clone(), state.dupe());
        ActiveTransfer::Leader(ActiveTransferLeader {
            registry: self,
            digest,
            state,
            completed: false,
        })
    }

    async fn wait(state: Arc<ActiveTransferState<T>>) -> anyhow::Result<Arc<T>> {
        let mut receiver = state.receiver.clone();
        loop {
            if let Some(result) = receiver.borrow().clone() {
                return result.map_err(|err| anyhow::anyhow!(err));
            }
            receiver
                .changed()
                .await
                .context("active transfer completed without a result")?;
        }
    }

    fn complete(
        &self,
        digest: &TDigest,
        state: &Arc<ActiveTransferState<T>>,
        result: Result<Arc<T>, String>,
    ) {
        drop(state.sender.send(Some(result)));
        let mut active = self.active.lock().unwrap();
        if active
            .get(digest)
            .is_some_and(|active_state| Arc::ptr_eq(active_state, state))
        {
            active.remove(digest);
        }
    }
}

enum ActiveTransfer<'a, T> {
    Leader(ActiveTransferLeader<'a, T>),
    Follower(Arc<ActiveTransferState<T>>),
}

struct ActiveTransferLeader<'a, T> {
    registry: &'a ActiveTransferRegistry<T>,
    digest: TDigest,
    state: Arc<ActiveTransferState<T>>,
    completed: bool,
}

impl<T> ActiveTransferLeader<'_, T> {
    fn finish(mut self, result: anyhow::Result<T>) -> anyhow::Result<Arc<T>> {
        self.completed = true;
        match result {
            Ok(value) => {
                let value = Arc::new(value);
                self.registry
                    .complete(&self.digest, &self.state, Ok(value.dupe()));
                Ok(value)
            }
            Err(err) => {
                let err = format!("{err:#}");
                self.registry
                    .complete(&self.digest, &self.state, Err(err.clone()));
                Err(anyhow::anyhow!(err))
            }
        }
    }
}

impl<T> Drop for ActiveTransferLeader<'_, T> {
    fn drop(&mut self) {
        if !self.completed {
            self.registry.complete(
                &self.digest,
                &self.state,
                Err("active transfer was cancelled before completion".to_owned()),
            );
        }
    }
}

/// How a CAS call that several callers share ended, as each of them sees it.
#[derive(Clone, Debug)]
struct SharedCallFailure {
    message: String,
    /// The gRPC code, kept so the error each caller gets is classified as the original was.
    code: Option<TCode>,
    group: TCodeReasonGroup,
    /// The server answered about the digests themselves, so a caller that sent the call again
    /// would get the same answer. Any other failure, a timeout, a broken connection, an error
    /// reading the starter's own copy of a blob, says nothing about another caller's attempt,
    /// so a caller that only joined the call makes its own.
    final_for_every_caller: bool,
}

impl SharedCallFailure {
    fn from_error(err: &anyhow::Error) -> Self {
        Self {
            message: format!("{err:#}"),
            code: error_tcode(err),
            group: err
                .downcast_ref::<REClientError>()
                .map_or(TCodeReasonGroup::UNKNOWN, |err| err.group),
            final_for_every_caller: error_is_the_servers_answer(err),
        }
    }

    fn into_error(self) -> anyhow::Error {
        match self.code {
            Some(code) => anyhow::Error::from(REClientError {
                code,
                message: self.message,
                group: self.group,
            }),
            None => anyhow::anyhow!(self.message),
        }
    }
}

/// A gRPC status the retry policy would not repeat. CANCELLED is the client's own doing, a
/// request timing out or being dropped, so it is not the server's answer.
fn error_is_the_servers_answer(err: &anyhow::Error) -> bool {
    error_tcode(err).is_some_and(|code| code != TCode::CANCELLED && !tcode_is_retryable(code))
        && grpc_error_retry_delay(err).is_none()
        && !is_broken_connection_error(err)
}

type SharedCallFuture<T> = BoxFuture<'static, Result<Arc<T>, SharedCallFailure>>;
type SharedCall<T> = futures::future::Shared<SharedCallFuture<T>>;

struct SharedCallMap<T> {
    next_id: u64,
    calls: HashMap<TDigest, (u64, futures::future::WeakShared<SharedCallFuture<T>>)>,
}

/// The CAS calls in flight for each digest, shared by every caller in the client that needs the
/// same digest, as Bazel's `AsyncTaskCache` shares one execution among its subscribers
/// (src/main/java/com/google/devtools/build/lib/remote/util/AsyncTaskCache.java). Each caller
/// awaits its own handle and any of them drives the call; the map holds only weak handles, so the
/// call is dropped, and its request cancelled, when the last caller waiting on it is dropped.
/// A call leaves the map when it finishes or is dropped, so the map holds what is in flight and
/// nothing more: what is known to be on the remote is `FindMissingCache`'s to remember.
struct SharedCallRegistry<T> {
    map: Arc<Mutex<SharedCallMap<T>>>,
}

impl<T: Send + Sync + 'static> SharedCallRegistry<T> {
    fn new() -> Self {
        Self {
            map: Arc::new(Mutex::new(SharedCallMap {
                next_id: 0,
                calls: HashMap::new(),
            })),
        }
    }

    /// Callers decide what to join and what to start under one lock, so two of them cannot both
    /// start a call for a digest.
    ///
    /// No handle may be dropped while the guard is held: the last handle's drop runs the call's
    /// removal from the map, which takes the same lock. `get` and `start` hand every handle they
    /// make to the caller for that reason.
    fn lock(&self) -> SharedCallGuard<'_, T> {
        SharedCallGuard {
            map: &self.map,
            guard: self.map.lock().unwrap(),
        }
    }
}

struct SharedCallGuard<'a, T> {
    map: &'a Arc<Mutex<SharedCallMap<T>>>,
    guard: std::sync::MutexGuard<'a, SharedCallMap<T>>,
}

impl<T: Send + Sync + 'static> SharedCallGuard<'_, T> {
    /// The call in flight for `digest`, to join.
    fn get(&self, digest: &TDigest) -> Option<SharedCall<T>> {
        self.guard
            .calls
            .get(digest)
            .and_then(|(_, call)| call.upgrade())
    }

    /// Registers `call` as the one in flight for every digest of `digests`. It is not polled
    /// until a caller awaits the handle.
    fn start(
        &mut self,
        digests: Vec<TDigest>,
        call: impl Future<Output = anyhow::Result<T>> + Send + 'static,
    ) -> SharedCall<T> {
        let id = self.guard.next_id;
        self.guard.next_id += 1;
        let removal = SharedCallRemoval {
            map: Arc::downgrade(self.map),
            id,
            digests: digests.clone(),
        };
        let call: SharedCall<T> = async move {
            let _removal = removal;
            call.await
                .map(Arc::new)
                .map_err(|err| SharedCallFailure::from_error(&err))
        }
        .boxed()
        .shared();
        let weak = call
            .downgrade()
            .expect("a call that was never polled has no output");
        for digest in digests {
            self.guard.calls.insert(digest, (id, weak.clone()));
        }
        call
    }
}

/// Takes a call's digests out of the map when the call finishes or is dropped, unless a later
/// call has replaced them.
struct SharedCallRemoval<T> {
    map: std::sync::Weak<Mutex<SharedCallMap<T>>>,
    id: u64,
    digests: Vec<TDigest>,
}

impl<T> Drop for SharedCallRemoval<T> {
    fn drop(&mut self) {
        let Some(map) = self.map.upgrade() else {
            return;
        };
        let mut map = map.lock().unwrap();
        for digest in &self.digests {
            if map.calls.get(digest).is_some_and(|(id, _)| *id == self.id) {
                map.calls.remove(digest);
            }
        }
    }
}

/// What a CAS call needs from the client, owned, so a call several callers share outlives the
/// caller that started it.
#[derive(Clone)]
struct CasCallContext {
    grpc_clients: Arc<GRPCClients>,
    instance_name: Arc<str>,
    retries: usize,
    retry_max_delay: Duration,
    use_fbcode_metadata: bool,
    request_metadata_tool_name: Arc<str>,
    grpc_request_timeout: Duration,
    request_digest_function_config: DigestFunctionConfig,
}

impl CasCallContext {
    /// FindMissingBlobs for `digests`, answering with the ones the CAS lacks.
    async fn find_missing_blobs(
        self,
        metadata: RemoteExecutionMetadata,
        digests: Vec<TDigest>,
    ) -> anyhow::Result<HashSet<TDigest>> {
        tracing::debug!(num_digests = digests.len(), "FindMissingBlobs");
        let requested_digests = digests
            .iter()
            .map(|digest| tdigest_to(digest.clone()))
            .collect::<Vec<_>>();
        let request_digest_function = digest_function_to_grpc(
            self.request_digest_function_config
                .for_common_digest_function(&requested_digests),
        );
        let (digest_count, bytes) = request_stats_for_grpc_digests(requested_digests.iter());
        let mut start = remote_request_start("CAS", "FindMissingBlobs", &metadata, None);
        start.digest_count = Some(digest_count);
        start.bytes = Some(bytes);
        let missing_blobs = remote_request_span(
            start,
            retry_idempotent_with_client_reconnect(
                self.grpc_clients.clone(),
                GrpcClientKind::Cas,
                self.retries,
                self.retry_max_delay,
                || {
                    let metadata = metadata.clone();
                    let requested_digests = requested_digests.clone();
                    let context = self.clone();
                    async move {
                        let mut cas_client = context.grpc_clients.cas_client().await?;
                        cas_client
                            .find_missing_blobs(with_re_metadata_timeout(
                                FindMissingBlobsRequest {
                                    instance_name: context.instance_name.to_string(),
                                    blob_digests: requested_digests,
                                    digest_function: request_digest_function,
                                    ..Default::default()
                                },
                                metadata,
                                context.use_fbcode_metadata,
                                &context.request_metadata_tool_name,
                                context.grpc_request_timeout,
                            ))
                            .await
                            .map_err(anyhow::Error::from)
                    }
                },
            ),
        )
        .await
        .context("Failed to request what blobs are not present on remote")?;
        let resp: FindMissingBlobsResponse = missing_blobs.into_inner();
        validate_find_missing_blobs_response_digests(&requested_digests, &resp)?;
        Ok(resp
            .missing_blob_digests
            .into_iter()
            .map(tdigest_from)
            .collect())
    }

    /// BatchUpdateBlobs, retried on the codes Bazel's `RemoteRetrier` retries and on a request
    /// that timed out on the client's side: writing a blob under its content address again
    /// leaves the CAS as it was.
    async fn batch_update_blobs(
        self,
        metadata: RemoteExecutionMetadata,
        re_request: BatchUpdateBlobsRequest,
    ) -> anyhow::Result<BatchUpdateBlobsResponse> {
        let mut start = remote_request_start("CAS", "BatchUpdateBlobs", &metadata, None);
        start.digest_count = Some(re_request.requests.len() as u64);
        start.bytes = Some(
            re_request
                .requests
                .iter()
                .map(|request| request.data.len() as u64)
                .sum(),
        );
        remote_request_span(
            start,
            retry_idempotent_with_client_reconnect(
                self.grpc_clients.clone(),
                GrpcClientKind::Cas,
                self.retries,
                self.retry_max_delay,
                || {
                    let metadata = metadata.clone();
                    let re_request = re_request.clone();
                    let context = self.clone();
                    async move {
                        let mut cas_client = context.grpc_clients.cas_client().await?;
                        Ok(cas_client
                            .batch_update_blobs(with_re_metadata_timeout(
                                re_request,
                                metadata,
                                context.use_fbcode_metadata,
                                &context.request_metadata_tool_name,
                                context.grpc_request_timeout,
                            ))
                            .await?
                            .into_inner())
                    }
                },
            ),
        )
        .await
    }
}

enum ActiveDownloadResult {
    Bytes(Vec<u8>),
    File(PathBuf),
}

fn download_output_options(is_executable: bool) -> OpenOptions {
    let mut opts = OpenOptions::new();
    opts.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        if is_executable {
            opts.mode(0o755);
        } else {
            opts.mode(0o644);
        }
    }
    opts
}

async fn read_active_download_file(path: &PathBuf) -> anyhow::Result<Vec<u8>> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("Error opening active download source {}", path.display()))?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)
        .await
        .with_context(|| format!("Error reading active download source {}", path.display()))?;
    Ok(data)
}

async fn active_download_result_to_bytes(result: &ActiveDownloadResult) -> anyhow::Result<Vec<u8>> {
    match result {
        ActiveDownloadResult::Bytes(data) => Ok(data.clone()),
        ActiveDownloadResult::File(path) => read_active_download_file(path).await,
    }
}

async fn active_download_result_into_bytes(
    result: Arc<ActiveDownloadResult>,
) -> anyhow::Result<Vec<u8>> {
    match Arc::try_unwrap(result) {
        Ok(ActiveDownloadResult::Bytes(data)) => Ok(data),
        Ok(ActiveDownloadResult::File(path)) => read_active_download_file(&path).await,
        Err(result) => active_download_result_to_bytes(&result).await,
    }
}

async fn write_active_download_result_to_file(
    result: &ActiveDownloadResult,
    output_path: &str,
    is_executable: bool,
) -> anyhow::Result<()> {
    match result {
        ActiveDownloadResult::File(source_path) if source_path == &PathBuf::from(output_path) => {
            Ok(())
        }
        ActiveDownloadResult::Bytes(data) => {
            let mut file = download_output_options(is_executable)
                .open(output_path)
                .await
                .context("Error opening")?;
            file.write_all(data)
                .await
                .with_context(|| format!("Error writing active download to {output_path}"))?;
            file.flush().await.context("Error flushing")?;
            Ok(())
        }
        ActiveDownloadResult::File(source_path) => {
            let mut source = tokio::fs::File::open(source_path).await.with_context(|| {
                format!(
                    "Error opening active download source {}",
                    source_path.display()
                )
            })?;
            let mut file = download_output_options(is_executable)
                .open(output_path)
                .await
                .context("Error opening")?;
            tokio::io::copy(&mut source, &mut file)
                .await
                .with_context(|| format!("Error copying active download to {output_path}"))?;
            file.flush().await.context("Error flushing")?;
            Ok(())
        }
    }
}

pub struct REClient {
    runtime_opts: RERuntimeOpts,
    grpc_clients: Arc<GRPCClients>,
    capabilities: RECapabilities,
    instance_name: InstanceName,
    // buck2 calls find_missing for same blobs
    find_missing_cache: Mutex<FindMissingCache>,
    bystream_compressor: Option<Compressor>,
    batch_update_compressor: Option<Compressor>,
    local_chunk_cache: Option<LocalChunkCache>,
    /// The machine-local CAS daemon's directory, for cloning blobs out of it.
    shared_cache: Option<SharedCasCache>,
    query_write_status_supported: AtomicBool,
    active_uploads: ActiveTransferRegistry<()>,
    active_downloads: ActiveTransferRegistry<ActiveDownloadResult>,
    cas_context: CasCallContext,
    /// The BatchUpdateBlobs calls in flight, by the digests they carry.
    shared_batch_uploads: SharedCallRegistry<()>,
    /// The FindMissingBlobs calls in flight, by the digests they ask about, answering with the
    /// ones the CAS lacks.
    shared_find_missing: SharedCallRegistry<HashSet<TDigest>>,
}

impl Drop for REClient {
    fn drop(&mut self) {
        // Important we have a drop implementation since the real one does, and we
        // don't want errors coming from the stub not having one
    }
}

/// Information on components of a batch upload.
/// Used to defer reading of NamedDigest contents till
/// actual execution of upload and prevent opening too many
/// files at the same time.
enum BatchUploadRequest {
    Blob(InlinedBlobWithDigest),
    File(NamedDigest),
}

/// Builds up a vector of batch upload requests based upon the maximum allowed message size.
#[derive(Default)]
struct BatchUploadReqAggregator {
    max_msg_size: i64,
    curr_req: Vec<BatchUploadRequest>,
    requests: Vec<Vec<BatchUploadRequest>>,
    curr_request_size: i64,
}

impl BatchUploadReqAggregator {
    pub fn new(max_msg_size: usize) -> Self {
        BatchUploadReqAggregator {
            max_msg_size: max_msg_size as i64,
            ..Default::default()
        }
    }

    pub fn push(&mut self, req: BatchUploadRequest) {
        let size_in_bytes = match &req {
            BatchUploadRequest::Blob(blob) => blob.digest.size_in_bytes,
            BatchUploadRequest::File(file) => file.digest.size_in_bytes,
        };

        // As an optimization, we can silently skip uploading empty blobs
        if size_in_bytes == 0 {
            return;
        }

        self.curr_request_size += size_in_bytes;

        if self.curr_request_size > self.max_msg_size {
            self.requests.push(std::mem::take(&mut self.curr_req));
            self.curr_request_size = size_in_bytes;
        }
        self.curr_req.push(req);
    }

    pub fn done(mut self) -> Vec<Vec<BatchUploadRequest>> {
        if !self.curr_req.is_empty() {
            self.requests.push(std::mem::take(&mut self.curr_req));
        }
        self.requests
    }
}

impl REClient {
    fn new(
        runtime_opts: RERuntimeOpts,
        grpc_clients: GRPCClients,
        capabilities: RECapabilities,
        instance_name: InstanceName,
        bystream_compressor: Option<Compressor>,
        batch_update_compressor: Option<Compressor>,
        local_chunk_cache: Option<LocalChunkCache>,
        shared_cache: Option<SharedCasCache>,
    ) -> Self {
        let find_missing_cache_ttl = Duration::from_secs(runtime_opts.cas_ttl_secs.max(0) as u64);
        let grpc_clients = Arc::new(grpc_clients);
        let cas_context = CasCallContext {
            grpc_clients: grpc_clients.clone(),
            instance_name: Arc::from(instance_name.as_str()),
            retries: runtime_opts.retries,
            retry_max_delay: Duration::from_millis(runtime_opts.retry_max_delay_ms),
            use_fbcode_metadata: runtime_opts.use_fbcode_metadata,
            request_metadata_tool_name: Arc::from(runtime_opts.request_metadata_tool_name.as_str()),
            grpc_request_timeout: runtime_opts.grpc_request_timeout,
            request_digest_function_config: runtime_opts.request_digest_function_config,
        };
        REClient {
            runtime_opts,
            grpc_clients,
            capabilities,
            instance_name,
            find_missing_cache: Mutex::new(FindMissingCache {
                cache: LruCache::new(NonZeroUsize::new(500_000).unwrap()),
                ttl: find_missing_cache_ttl,
                last_check: Instant::now(),
            }),
            bystream_compressor,
            batch_update_compressor,
            local_chunk_cache,
            shared_cache,
            query_write_status_supported: AtomicBool::new(true),
            active_uploads: ActiveTransferRegistry::new(),
            active_downloads: ActiveTransferRegistry::new(),
            cas_context,
            shared_batch_uploads: SharedCallRegistry::new(),
            shared_find_missing: SharedCallRegistry::new(),
        }
    }

    pub fn action_cache_update_enabled(&self) -> Option<bool> {
        self.capabilities.action_cache_update_enabled
    }

    async fn bystream_write_plan(
        &self,
        bytestream_client: &mut ByteStreamClient<GrpcService>,
        metadata: RemoteExecutionMetadata,
        segments: Vec<WriteRequest>,
    ) -> anyhow::Result<BystreamWritePlan> {
        if segments.is_empty() || !self.query_write_status_supported.load(Ordering::Relaxed) {
            return Ok(BystreamWritePlan::Write(segments));
        }

        let resource_name = segments[0].resource_name.clone();
        let total_size = total_bystream_write_size(&segments);

        let mut start = remote_request_start("ByteStream", "QueryWriteStatus", &metadata, None);
        start.bytes = Some(u64::try_from(total_size).unwrap_or_default());
        start
            .details
            .insert("resource_name".to_owned(), resource_name.clone());
        let query_write_status = span_async(start, async {
            let result = bytestream_client
                .query_write_status(with_re_metadata_timeout(
                    QueryWriteStatusRequest {
                        resource_name: resource_name.clone(),
                    },
                    metadata,
                    self.runtime_opts.use_fbcode_metadata,
                    self.runtime_opts.request_metadata_tool_name.as_str(),
                    self.runtime_opts.grpc_request_timeout,
                ))
                .await;
            let end = match &result {
                Ok(_) => buck2_data::RemoteRequestEnd {
                    success: true,
                    ..Default::default()
                },
                Err(status) => buck2_data::RemoteRequestEnd {
                    success: false,
                    error: Some(status.message().to_owned()),
                    re_error_code: Some(format!("{:?}", status.code())),
                    ..Default::default()
                },
            };
            (result, end)
        })
        .await;
        match query_write_status {
            Ok(resp) => {
                let status = resp.into_inner();
                if status.complete || status.committed_size >= total_size {
                    return Ok(BystreamWritePlan::AlreadyCommitted(total_size));
                }

                Ok(BystreamWritePlan::Write(trim_bystream_write_segments(
                    segments,
                    status.committed_size,
                )))
            }
            Err(status) if status.code() == tonic::Code::Unimplemented => {
                self.query_write_status_supported
                    .store(false, Ordering::Relaxed);
                tracing::debug!(
                    resource_name = %resource_name,
                    "Bytestream QueryWriteStatus is not supported by server; disabling resume probes"
                );
                Ok(BystreamWritePlan::Write(segments))
            }
            Err(status) => {
                tracing::debug!(
                    resource_name = %resource_name,
                    code = ?status.code(),
                    "Bytestream QueryWriteStatus failed; retrying write from offset 0"
                );
                Ok(BystreamWritePlan::Write(segments))
            }
        }
    }

    pub async fn get_action_result(
        &self,
        metadata: &RemoteExecutionMetadata,
        request: ActionResultRequest,
    ) -> anyhow::Result<ActionResultResponse> {
        let action_digest = tdigest_to(request.digest);
        let digest_function = self
            .runtime_opts
            .request_digest_function_config
            .for_digest(&action_digest);
        let digest_function = digest_function_to_grpc(digest_function);
        let res = remote_request_span(
            remote_request_start(
                "ActionCache",
                "GetActionResult",
                metadata,
                Some(grpc_digest_string(&action_digest)),
            ),
            retry_action_cache_request(
                self.grpc_clients.clone(),
                self.runtime_opts.retries,
                Duration::from_millis(self.runtime_opts.retry_max_delay_ms),
                true,
                |mut client| {
                    let metadata = metadata.clone();
                    let action_digest = action_digest.clone();
                    async move {
                        client
                            .get_action_result(with_re_metadata_timeout(
                                GetActionResultRequest {
                                    instance_name: self.instance_name.as_str().to_owned(),
                                    action_digest: Some(action_digest),
                                    digest_function,
                                    ..Default::default()
                                },
                                metadata,
                                self.runtime_opts.use_fbcode_metadata,
                                self.runtime_opts.request_metadata_tool_name.as_str(),
                                self.runtime_opts.grpc_request_timeout,
                            ))
                            .await
                            .map_err(anyhow::Error::from)
                    }
                },
            ),
        )
        .await?;

        Ok(ActionResultResponse {
            action_result: convert_action_result(res.into_inner(), self.runtime_opts.cas_ttl_secs)?,
            ttl: self.runtime_opts.cas_ttl_secs,
        })
    }

    pub async fn write_action_result(
        &self,
        metadata: &RemoteExecutionMetadata,
        request: WriteActionResultRequest,
    ) -> anyhow::Result<WriteActionResultResponse> {
        let action_digest = tdigest_to(request.action_digest);
        let digest_function = self
            .runtime_opts
            .request_digest_function_config
            .for_digest(&action_digest);
        let digest_function = digest_function_to_grpc(digest_function);
        let action_result = convert_t_action_result2(request.action_result)?;
        let mut start = remote_request_start(
            "ActionCache",
            "UpdateActionResult",
            &metadata,
            Some(grpc_digest_string(&action_digest)),
        );
        start.details.insert(
            "output_file_count".to_owned(),
            action_result.output_files.len().to_string(),
        );
        start.details.insert(
            "output_directory_count".to_owned(),
            action_result.output_directories.len().to_string(),
        );
        let res = remote_request_span(
            start,
            retry_action_cache_request(
                self.grpc_clients.clone(),
                self.runtime_opts.retries,
                Duration::from_millis(self.runtime_opts.retry_max_delay_ms),
                false,
                |mut client| {
                    let metadata = metadata.clone();
                    let action_digest = action_digest.clone();
                    let action_result = action_result.clone();
                    async move {
                        client
                            .update_action_result(with_re_metadata_timeout(
                                UpdateActionResultRequest {
                                    instance_name: self.instance_name.as_str().to_owned(),
                                    action_digest: Some(action_digest),
                                    action_result: Some(action_result),
                                    results_cache_policy: None,
                                    digest_function,
                                    ..Default::default()
                                },
                                metadata,
                                self.runtime_opts.use_fbcode_metadata,
                                self.runtime_opts.request_metadata_tool_name.as_str(),
                                self.runtime_opts.grpc_request_timeout,
                            ))
                            .await
                            .map_err(anyhow::Error::from)
                    }
                },
            ),
        )
        .await?;

        Ok(WriteActionResultResponse {
            actual_action_result: convert_action_result(
                res.into_inner(),
                self.runtime_opts.cas_ttl_secs,
            )?,
            ttl_seconds: self.runtime_opts.cas_ttl_secs,
        })
    }

    pub async fn execute_with_progress(
        &self,
        metadata: &RemoteExecutionMetadata,
        mut execute_request: ExecuteRequest,
    ) -> anyhow::Result<BoxStream<'static, anyhow::Result<ExecuteWithProgressResponse>>> {
        validate_remote_execution_enabled(self.capabilities.execution_enabled)?;

        // TODO(aloiscochard): Map those properly in the request
        // use crate::proto::build::bazel::remote::execution::v2::ExecutionPolicy;

        let action_digest = tdigest_to(execute_request.action_digest.clone());
        let digest_function = self
            .runtime_opts
            .request_digest_function_config
            .for_digest(&action_digest);
        let digest_function = digest_function_to_grpc(digest_function);
        let execution_priority = execute_request
            .execution_policy
            .as_ref()
            .map(|ep| ep.priority)
            .unwrap_or_default();
        validate_priority_in_range(
            execution_priority,
            "remote_execution_priority",
            &self.capabilities.execution_priority_ranges,
        )?;

        let grpc_request = GExecuteRequest {
            instance_name: self.instance_name.as_str().to_owned(),
            skip_cache_lookup: execute_request.skip_cache_lookup,
            execution_policy: Some(ExecutionPolicy {
                priority: execution_priority,
            }),
            results_cache_policy: Some(ResultsCachePolicy { priority: 0 }),
            action_digest: Some(action_digest.clone()),
            digest_function,
            ..Default::default()
        };
        let request_metadata_tool_name = self.runtime_opts.request_metadata_tool_name.clone();

        let stream = execute_stream(
            self.grpc_clients.clone(),
            metadata.clone(),
            self.runtime_opts.use_fbcode_metadata,
            request_metadata_tool_name.as_str(),
            grpc_request.clone(),
            self.runtime_opts.retries,
            Duration::from_millis(self.runtime_opts.retry_max_delay_ms),
        )
        .await?;

        let grpc_clients = self.grpc_clients.clone();
        let metadata_for_wait_execution = metadata.clone();
        let use_fbcode_metadata = self.runtime_opts.use_fbcode_metadata;
        let retries = self.runtime_opts.retries;
        let retry_max_delay = Duration::from_millis(self.runtime_opts.retry_max_delay_ms);
        let cas_ttl_secs = self.runtime_opts.cas_ttl_secs;
        let grpc_request_timeout = self.runtime_opts.grpc_request_timeout;

        let stream = futures::stream::try_unfold(
            Some(OperationStream {
                stream,
                operation_name: None,
                execute_retry_attempts: 0,
                stalled_resumes: 0,
                resumed: false,
                queued: QueuedDeadline::new(self.runtime_opts.queued_operation_timeout),
                stalled: StallDeadline::new(self.runtime_opts.stalled_operation_timeout),
                superseded: Vec::new(),
                execute_request: grpc_request.clone(),
            }),
            move |state| {
                let grpc_clients = grpc_clients.clone();
                let metadata = metadata_for_wait_execution.clone();
                let request_metadata_tool_name = request_metadata_tool_name.clone();
                async move {
                    let Some(OperationStream {
                        mut stream,
                        mut operation_name,
                        mut execute_retry_attempts,
                        mut stalled_resumes,
                        mut resumed,
                        mut queued,
                        mut stalled,
                        mut superseded,
                        execute_request: mut grpc_request,
                    }) = state
                    else {
                        return Ok(None);
                    };
                    let stalled_reexecute = StalledReexecute {
                        grpc_clients: grpc_clients.clone(),
                        metadata: &metadata,
                        use_fbcode_metadata,
                        request_metadata_tool_name: request_metadata_tool_name.as_str(),
                        retries,
                        retry_max_delay,
                        grpc_request_timeout,
                    };
                    loop {
                        let msg = loop {
                            match next_operation_event(
                                &mut stream,
                                &mut superseded,
                                queued.at.or(stalled.at),
                            )
                            .await
                            {
                                OperationStreamEvent::Next(Ok(Some(msg))) => break msg,
                                OperationStreamEvent::Superseded(first, msg) => {
                                    tracing::debug!(
                                        operation_name =
                                            first.operation_name.as_deref().unwrap_or(""),
                                        "An operation that stayed QUEUED was claimed or finished first; dropping the later Execute"
                                    );
                                    stream = first.stream;
                                    operation_name = first.operation_name;
                                    queued.seen_queued = first.seen_queued;
                                    stalled.reset();
                                    resumed = false;
                                    break msg;
                                }
                                // A claimed operation has no QUEUED clock, so this is its stall
                                // deadline.
                                OperationStreamEvent::Deadline if queued.at.is_none() => {
                                    let (next_stream, next_request) = stalled_reexecute
                                        .execute(
                                            &grpc_request,
                                            operation_name.as_deref(),
                                            &mut stalled,
                                        )
                                        .await?;
                                    stream = next_stream;
                                    grpc_request = next_request;
                                    operation_name = None;
                                    resumed = false;
                                    superseded.clear();
                                    queued.start();
                                    stalled.reset();
                                }
                                OperationStreamEvent::Deadline => {
                                    let waited = queued.wait;
                                    if !can_retry_execute(execute_retry_attempts, retries) {
                                        warn_stayed_queued(
                                            &grpc_request,
                                            operation_name.as_deref(),
                                            waited,
                                            None,
                                            retries,
                                        );
                                        queued.at = None;
                                        continue;
                                    }
                                    execute_retry_attempts += 1;
                                    warn_stayed_queued(
                                        &grpc_request,
                                        operation_name.as_deref(),
                                        waited,
                                        Some(execute_retry_attempts),
                                        retries,
                                    );
                                    queued.reexecutes += 1;
                                    match execute_stream(
                                        grpc_clients.clone(),
                                        metadata.clone(),
                                        use_fbcode_metadata,
                                        request_metadata_tool_name.as_str(),
                                        grpc_request.clone(),
                                        retries,
                                        retry_max_delay,
                                    )
                                    .await
                                    {
                                        Ok(next_stream) => {
                                            superseded.push(SupersededOperation {
                                                stream: std::mem::replace(&mut stream, next_stream),
                                                operation_name: operation_name.take(),
                                                seen_queued: queued.seen_queued,
                                            });
                                            resumed = false;
                                            queued.start();
                                            stalled.reset();
                                        }
                                        // The operation may still be queued legitimately, so it is
                                        // waited for rather than failed; the attempt is spent.
                                        Err(err) => {
                                            warn_re_execution_retry(format!(
                                                "RE action {} could not be executed again, so its operation `{}` is still waited for: {err:#}",
                                                execute_request_action(&grpc_request),
                                                operation_name.as_deref().unwrap_or(""),
                                            ));
                                            queued.rearm();
                                        }
                                    }
                                }
                                OperationStreamEvent::Next(Ok(None)) => {
                                    let Some(name) = operation_name.clone() else {
                                        return Err(anyhow::anyhow!(
                                            "RE Execute stream ended before operation creation"
                                        ));
                                    };

                                    if !pause_before_resume(
                                        &mut stalled_resumes,
                                        retries,
                                        retry_max_delay,
                                    )
                                    .await
                                    {
                                        return Err(anyhow::anyhow!(
                                            "RE operation `{name}` ended its stream {stalled_resumes} times in a row without progress"
                                        ));
                                    }
                                    resumed = true;
                                    tracing::debug!(
                                        operation_name = %name,
                                        "Execute stream ended before completion; resuming with WaitExecution"
                                    );
                                    let resumption = resume_or_retry_execute(
                                        grpc_clients.clone(),
                                        metadata.clone(),
                                        use_fbcode_metadata,
                                        request_metadata_tool_name.as_str(),
                                        name,
                                        grpc_request.clone(),
                                        execute_retry_attempts,
                                        retries,
                                        retry_max_delay,
                                        "RE WaitExecution failed after Execute stream ended before completion".to_owned(),
                                        &mut queued,
                                        &mut stalled,
                                    )
                                    .await;
                                    let (
                                        next_stream,
                                        next_operation_name,
                                        next_execute_retry_attempts,
                                    ) = match resumption {
                                        Err(err) if err.is::<ResumptionStalled>() => {
                                            let (next_stream, next_request) = stalled_reexecute
                                                .execute(
                                                    &grpc_request,
                                                    operation_name.as_deref(),
                                                    &mut stalled,
                                                )
                                                .await?;
                                            stream = next_stream;
                                            grpc_request = next_request;
                                            operation_name = None;
                                            resumed = false;
                                            queued.start();
                                            stalled.reset();
                                            continue;
                                        }
                                        resumption => resumption?,
                                    };
                                    stream = next_stream;
                                    operation_name = next_operation_name;
                                    execute_retry_attempts = next_execute_retry_attempts;
                                }
                                OperationStreamEvent::Next(Err(err)) => {
                                    let err = anyhow::Error::from(err);
                                    if should_retry_execute_after_operation_stream_error(&err) {
                                        if !can_retry_execute(execute_retry_attempts, retries) {
                                            return Err(err.context(match &operation_name {
                                                Some(name) => format!(
                                                    "RE operation `{name}` was lost after retry limit"
                                                ),
                                                None => "RE Execute stream returned NOT_FOUND before operation creation after retry limit".to_owned(),
                                            }));
                                        }

                                        execute_retry_attempts += 1;
                                        warn_re_execution_retry(format!(
                                            "Executing RE action {} again (re-Execute {execute_retry_attempts}/{retries}): its operation stream failed: {err:#}",
                                            execute_request_action(&grpc_request),
                                        ));
                                        stream = execute_stream(
                                            grpc_clients.clone(),
                                            metadata.clone(),
                                            use_fbcode_metadata,
                                            request_metadata_tool_name.as_str(),
                                            grpc_request.clone(),
                                            retries,
                                            retry_max_delay,
                                        )
                                        .await
                                        .context("RE operation stream returned NOT_FOUND and Execute retry failed")?;
                                        queued.start();
                                        stalled.reset();
                                        operation_name = None;
                                        continue;
                                    }

                                    if !is_retryable_grpc_error(&err) {
                                        return Err(err.context("RE channel error"));
                                    }
                                    let reconnected = if is_broken_connection_error(&err) {
                                        grpc_clients
                                            .reconnect_after_broken_connection(
                                                GrpcClientKind::Execution,
                                            )
                                            .await;
                                        " on a new connection"
                                    } else {
                                        ""
                                    };

                                    let Some(name) = operation_name.clone() else {
                                        if !can_retry_execute(execute_retry_attempts, retries) {
                                            return Err(err.context(
                                                "RE Execute stream failed before operation creation after retry limit",
                                            ));
                                        }

                                        execute_retry_attempts += 1;
                                        warn_re_execution_retry(format!(
                                            "Executing RE action {} again (re-Execute {execute_retry_attempts}/{retries}){reconnected}: its Execute stream failed before the operation was created: {err:#}",
                                            execute_request_action(&grpc_request),
                                        ));
                                        stream = execute_stream(
                                            grpc_clients.clone(),
                                            metadata.clone(),
                                            use_fbcode_metadata,
                                            request_metadata_tool_name.as_str(),
                                            grpc_request.clone(),
                                            retries,
                                            retry_max_delay,
                                        )
                                        .await
                                        .context(
                                            "Execute stream failed before operation creation and Execute retry failed",
                                        )?;
                                        queued.start();
                                        stalled.reset();
                                        continue;
                                    };

                                    if !pause_before_resume(
                                        &mut stalled_resumes,
                                        retries,
                                        retry_max_delay,
                                    )
                                    .await
                                    {
                                        return Err(err.context(format!(
                                            "RE operation `{name}` lost its stream {stalled_resumes} times in a row without progress"
                                        )));
                                    }
                                    resumed = true;
                                    warn_re_execution_retry(format!(
                                        "Resuming RE operation `{name}` of action {} with WaitExecution (resume {stalled_resumes}/{}){reconnected}: its stream failed: {err:#}",
                                        execute_request_action(&grpc_request),
                                        retries + 1,
                                    ));
                                    let resumption = resume_or_retry_execute(
                                        grpc_clients.clone(),
                                        metadata.clone(),
                                        use_fbcode_metadata,
                                        request_metadata_tool_name.as_str(),
                                        name,
                                        grpc_request.clone(),
                                        execute_retry_attempts,
                                        retries,
                                        retry_max_delay,
                                        format!("RE WaitExecution failed after Execute stream interruption ({err:#})"),
                                        &mut queued,
                                        &mut stalled,
                                    )
                                    .await;
                                    let (
                                        next_stream,
                                        next_operation_name,
                                        next_execute_retry_attempts,
                                    ) = match resumption {
                                        Err(err) if err.is::<ResumptionStalled>() => {
                                            let (next_stream, next_request) = stalled_reexecute
                                                .execute(
                                                    &grpc_request,
                                                    operation_name.as_deref(),
                                                    &mut stalled,
                                                )
                                                .await?;
                                            stream = next_stream;
                                            grpc_request = next_request;
                                            operation_name = None;
                                            resumed = false;
                                            queued.start();
                                            stalled.reset();
                                            continue;
                                        }
                                        resumption => resumption?,
                                    };
                                    stream = next_stream;
                                    operation_name = next_operation_name;
                                    execute_retry_attempts = next_execute_retry_attempts;
                                }
                            }
                        };

                        if !resumed {
                            stalled_resumes = 0;
                        }
                        resumed = false;
                        if !msg.name.is_empty() {
                            operation_name = Some(msg.name.clone());
                        }

                        if msg.done {
                            drain_finished_operation_stream(stream);
                            match msg
                                .result
                                .context("Missing `result` when message was `done`")?
                            {
                                OpResult::Error(rpc_status) => {
                                    if should_retry_execute_after_operation_error(&rpc_status)
                                        && can_retry_execute(execute_retry_attempts, retries)
                                    {
                                        execute_retry_attempts += 1;
                                        warn_re_execution_retry(format!(
                                            "Executing RE action {} again (re-Execute {execute_retry_attempts}/{retries}): operation `{}` failed with code {}: {}",
                                            execute_request_action(&grpc_request),
                                            operation_name.as_deref().unwrap_or(""),
                                            rpc_status.code,
                                            format_rpc_status_message(&rpc_status),
                                        ));
                                        sleep_for_execute_retry_info(
                                            &rpc_status,
                                            retry_max_delay,
                                            operation_name.as_deref(),
                                        )
                                        .await;
                                        stream = execute_stream(
                                            grpc_clients.clone(),
                                            metadata.clone(),
                                            use_fbcode_metadata,
                                            request_metadata_tool_name.as_str(),
                                            grpc_request.clone(),
                                            retries,
                                            retry_max_delay,
                                        )
                                        .await
                                        .context(
                                            "Execute operation failed and Execute retry failed",
                                        )?;
                                        queued.start();
                                        stalled.reset();
                                        operation_name = None;
                                        continue;
                                    }

                                    return Err(re_client_error_from_rpc_status(&rpc_status).into());
                                }
                                OpResult::Response(any) => {
                                    let execute_response_grpc: GExecuteResponse =
                                        GExecuteResponse::decode(&any.value[..])?;

                                    let execute_response_status =
                                        execute_response_grpc.status.unwrap_or_default();
                                    if should_retry_execute_after_execute_response_status(
                                        &execute_response_status,
                                    ) && can_retry_execute(execute_retry_attempts, retries)
                                    {
                                        execute_retry_attempts += 1;
                                        warn_re_execution_retry(format!(
                                            "Executing RE action {} again (re-Execute {execute_retry_attempts}/{retries}): operation `{}` returned code {}: {}",
                                            execute_request_action(&grpc_request),
                                            operation_name.as_deref().unwrap_or(""),
                                            execute_response_status.code,
                                            format_rpc_status_message(&execute_response_status),
                                        ));
                                        sleep_for_execute_retry_info(
                                            &execute_response_status,
                                            retry_max_delay,
                                            operation_name.as_deref(),
                                        )
                                        .await;
                                        stream = execute_stream(
                                            grpc_clients.clone(),
                                            metadata.clone(),
                                            use_fbcode_metadata,
                                            request_metadata_tool_name.as_str(),
                                            grpc_request.clone(),
                                            retries,
                                            retry_max_delay,
                                        )
                                        .await
                                        .context(
                                            "Execute response failed and Execute retry failed",
                                        )?;
                                        queued.start();
                                        stalled.reset();
                                        operation_name = None;
                                        continue;
                                    }
                                    check_status(execute_response_status)?;

                                    let action_result = execute_response_grpc
                                        .result
                                        .with_context(|| "The action result is not defined.")?;

                                    let action_result =
                                        convert_action_result(action_result, cas_ttl_secs)?;

                                    let execute_response = ExecuteResponse {
                                        action_result,
                                        action_result_digest: TDigest::default(),
                                        action_result_ttl: cas_ttl_secs,
                                        status: TStatus {
                                            code: TCode::OK,
                                            message: execute_response_grpc.message,
                                            ..Default::default()
                                        },
                                        cached_result: execute_response_grpc.cached_result,
                                        action_digest: Default::default(), // Filled in below.
                                    };

                                    return anyhow::Ok(Some((
                                        ExecuteWithProgressResponse {
                                            stage: Stage::COMPLETED,
                                            execute_response: Some(execute_response),
                                            ..Default::default()
                                        },
                                        None,
                                    )));
                                }
                            }
                        }

                        let metadata = msg.metadata.unwrap_or_default().value;
                        let meta = ExecuteOperationMetadata::decode(&metadata[..])?;
                        let stage = match execution_stage::Value::try_from(meta.stage) {
                            Ok(execution_stage::Value::Unknown) => Stage::UNKNOWN,
                            Ok(execution_stage::Value::CacheCheck) => Stage::CACHE_CHECK,
                            Ok(execution_stage::Value::Queued) => Stage::QUEUED,
                            Ok(execution_stage::Value::Executing) => Stage::EXECUTING,
                            Ok(execution_stage::Value::Completed) => Stage::COMPLETED,
                            _ => Stage::UNKNOWN,
                        };
                        if operation_claimed(meta.stage, &mut queued.seen_queued) {
                            queued.at = None;
                            superseded.clear();
                            stalled.observe(metadata);
                        }
                        return anyhow::Ok(Some((
                            ExecuteWithProgressResponse {
                                stage,
                                execute_response: None,
                                ..Default::default()
                            },
                            Some(OperationStream {
                                stream,
                                operation_name,
                                execute_retry_attempts,
                                stalled_resumes,
                                resumed,
                                queued,
                                stalled,
                                superseded,
                                execute_request: grpc_request,
                            }),
                        )));
                    }
                }
            },
        );

        // We fill in the action digest a little later here. We do it this way so we don't have to
        // clone the execute_request into every future we create above.

        let stream = stream.map(move |mut r| {
            match &mut r {
                Ok(ExecuteWithProgressResponse {
                    execute_response: Some(response),
                    ..
                }) => {
                    response.action_digest = std::mem::take(&mut execute_request.action_digest);
                }
                _ => {}
            };

            r
        });

        Ok(stream.boxed())
    }

    pub async fn upload(
        &self,
        metadata: RemoteExecutionMetadata,
        mut request: UploadRequest,
    ) -> anyhow::Result<UploadResponse> {
        if request.upload_only_missing {
            request = self
                .filter_upload_request_to_missing(metadata.clone(), request)
                .await?;
        } else {
            // A caller that asked FindMissingBlobs itself may have heard "missing" before
            // another action's upload of the same digest finished. As Bazel's
            // RemoteExecutionCache replaces the cached "missing" with "present" once an upload
            // completes, an upload the client knows has happened is not sent again.
            request = self.without_digests_known_on_remote(request);
        }
        let (request, spliced_digests) = self
            .upload_chunked_inlined_blobs(metadata.clone(), request)
            .await?;
        let (request, file_spliced_digests) =
            self.upload_chunked_files(metadata.clone(), request).await?;
        let response = self.upload_direct(metadata, request).await?;
        self.mark_digests_exist_on_remote(spliced_digests);
        self.mark_digests_exist_on_remote(file_spliced_digests);

        Ok(response)
    }

    async fn upload_direct(
        &self,
        metadata: RemoteExecutionMetadata,
        request: UploadRequest,
    ) -> anyhow::Result<UploadResponse> {
        validate_upload_request_sizes(&request, self.capabilities.max_cas_blob_size_bytes)?;
        let uploaded_digests = upload_payload_digests(&request);
        let cas_context = self.cas_context.clone();
        let batch_metadata = metadata.clone();
        let response = upload_impl(
            &self.instance_name,
            request,
            self.bystream_compressor,
            self.batch_update_compressor,
            self.capabilities.max_total_batch_size,
            self.runtime_opts.remote_cache_compression_threshold,
            self.runtime_opts.max_concurrent_uploads_per_action,
            self.runtime_opts.request_digest_function_config,
            &self.active_uploads,
            &self.shared_batch_uploads,
            move |re_request| {
                cas_context
                    .clone()
                    .batch_update_blobs(batch_metadata.clone(), re_request)
            },
            |segments| {
                let metadata = metadata.clone();
                async move {
                    let mut start = remote_request_start("ByteStream", "Write", &metadata, None);
                    start.digest_count = Some(1);
                    start.bytes = Some(
                        u64::try_from(total_bystream_write_size(&segments)).unwrap_or_default(),
                    );
                    start
                        .details
                        .insert("segment_count".to_owned(), segments.len().to_string());
                    if let Some(segment) = segments.first() {
                        start
                            .details
                            .insert("resource_name".to_owned(), segment.resource_name.clone());
                    }
                    remote_request_span(
                        start,
                        retry_idempotent_with_client_reconnect(
                            self.grpc_clients.clone(),
                            GrpcClientKind::ByteStream,
                            self.runtime_opts.retries,
                            Duration::from_millis(self.runtime_opts.retry_max_delay_ms),
                            || {
                                let grpc_clients = self.grpc_clients.clone();
                                let metadata = metadata.clone();
                                let segments = segments.clone();
                                async move {
                                    let mut bytestream_client =
                                        grpc_clients.bytestream_client().await?;
                                    let segments = match self
                                        .bystream_write_plan(
                                            &mut bytestream_client,
                                            metadata.clone(),
                                            segments,
                                        )
                                        .await?
                                    {
                                        BystreamWritePlan::Write(segments) => segments,
                                        BystreamWritePlan::AlreadyCommitted(committed_size) => {
                                            return Ok(WriteResponse { committed_size });
                                        }
                                    };
                                    let requests = futures::stream::iter(segments);
                                    Ok(bytestream_client
                                        .write(with_re_metadata(
                                            requests,
                                            &metadata,
                                            self.runtime_opts.use_fbcode_metadata,
                                            self.runtime_opts.request_metadata_tool_name.as_str(),
                                        ))
                                        .await?
                                        .into_inner())
                                }
                            },
                        ),
                    )
                    .await
                }
            },
        )
        .await?;

        self.mark_digests_exist_on_remote(uploaded_digests);

        Ok(response)
    }

    async fn upload_chunked_inlined_blobs(
        &self,
        metadata: RemoteExecutionMetadata,
        mut request: UploadRequest,
    ) -> anyhow::Result<(UploadRequest, Vec<TDigest>)> {
        let Some(config) = self.fast_cdc_2020_upload_config() else {
            return Ok((request, Vec::new()));
        };

        let inlined_blobs = request.inlined_blobs_with_digest.take().unwrap_or_default();
        if inlined_blobs.is_empty() {
            return Ok((request, Vec::new()));
        }

        let mut remaining_blobs = Vec::new();
        let mut spliced_digests = Vec::new();

        for blob in inlined_blobs {
            if blob.digest.size_in_bytes <= 0
                || blob.digest.size_in_bytes as u64 <= config.chunking_threshold_bytes()
            {
                remaining_blobs.push(blob);
                continue;
            }

            let Some(digest_function) = self
                .runtime_opts
                .request_digest_function_config
                .for_digest(&tdigest_to(blob.digest.clone()))
            else {
                tracing::debug!(
                    digest = %blob.digest,
                    "Skipping FastCDC chunked upload because the digest function is ambiguous"
                );
                remaining_blobs.push(blob);
                continue;
            };
            if !supports_hash_validation(digest_function) {
                tracing::debug!(
                    digest = %blob.digest,
                    digest_function = %digest_function_name(digest_function),
                    "Skipping FastCDC chunked upload because the digest function is unsupported"
                );
                remaining_blobs.push(blob);
                continue;
            }

            let chunked_digest = blob.digest.clone();
            let chunks = chunk_inlined_blob_fast_cdc_2020(&blob, &config, digest_function)
                .with_context(|| format!("Failed to chunk `{chunked_digest}` for upload"))?;
            if chunks.is_empty() {
                remaining_blobs.push(blob);
                continue;
            }

            self.upload_fast_cdc_chunks(
                metadata.clone(),
                chunked_digest.clone(),
                chunks,
                &config,
                digest_function,
            )
            .await?;
            spliced_digests.push(chunked_digest);
        }

        request.inlined_blobs_with_digest = if remaining_blobs.is_empty() {
            None
        } else {
            Some(remaining_blobs)
        };
        Ok((request, spliced_digests))
    }

    async fn upload_chunked_files(
        &self,
        metadata: RemoteExecutionMetadata,
        mut request: UploadRequest,
    ) -> anyhow::Result<(UploadRequest, Vec<TDigest>)> {
        let Some(config) = self.fast_cdc_2020_upload_config() else {
            return Ok((request, Vec::new()));
        };

        let files = request.files_with_digest.take().unwrap_or_default();
        if files.is_empty() {
            return Ok((request, Vec::new()));
        }

        let mut remaining_files = Vec::new();
        let mut spliced_digests = Vec::new();

        for file in files {
            if file.digest.size_in_bytes <= 0
                || file.digest.size_in_bytes as u64 <= config.chunking_threshold_bytes()
            {
                remaining_files.push(file);
                continue;
            }

            let Some(digest_function) = self
                .runtime_opts
                .request_digest_function_config
                .for_digest(&tdigest_to(file.digest.clone()))
            else {
                tracing::debug!(
                    digest = %file.digest,
                    path = %file.name,
                    "Skipping FastCDC chunked upload because the digest function is ambiguous"
                );
                remaining_files.push(file);
                continue;
            };
            if !supports_hash_validation(digest_function) {
                tracing::debug!(
                    digest = %file.digest,
                    path = %file.name,
                    digest_function = %digest_function_name(digest_function),
                    "Skipping FastCDC chunked upload because the digest function is unsupported"
                );
                remaining_files.push(file);
                continue;
            }

            let chunked_digest = file.digest.clone();
            let path = file.name.clone();
            let chunk_config = config.clone();
            let chunks = tokio::task::spawn_blocking(move || {
                chunk_file_fast_cdc_2020(&path, &chunk_config, digest_function)
            })
            .await
            .context("FastCDC chunking task failed")?
            .with_context(|| format!("Failed to chunk `{chunked_digest}` for upload"))?;
            if chunks.is_empty() {
                remaining_files.push(file);
                continue;
            }

            self.upload_fast_cdc_chunks(
                metadata.clone(),
                chunked_digest.clone(),
                chunks,
                &config,
                digest_function,
            )
            .await?;
            spliced_digests.push(chunked_digest);
        }

        request.files_with_digest = if remaining_files.is_empty() {
            None
        } else {
            Some(remaining_files)
        };
        Ok((request, spliced_digests))
    }

    async fn upload_fast_cdc_chunks(
        &self,
        metadata: RemoteExecutionMetadata,
        blob_digest: TDigest,
        chunks: Vec<InlinedBlobWithDigest>,
        config: &FastCdc2020Config,
        digest_function: digest_function::Value,
    ) -> anyhow::Result<()> {
        let chunk_digests = chunks
            .iter()
            .map(|chunk| chunk.digest.clone())
            .collect::<Vec<_>>();
        self.write_fast_cdc_chunks_to_local_cache(&chunks, digest_function)
            .await;
        let missing = self
            .get_digests_ttl(
                &metadata,
                GetDigestsTtlRequest {
                    digests: chunk_digests.clone(),
                    is_for_upload: Some(true),
                    _dot_dot: (),
                },
            )
            .await?
            .digests_with_ttl
            .into_iter()
            .filter(|digest| digest.ttl == 0)
            .map(|digest| digest.digest)
            .collect::<HashSet<_>>();

        let mut uploaded = HashSet::new();
        let missing_chunks = chunks
            .into_iter()
            .filter(|chunk| missing.contains(&chunk.digest))
            .filter(|chunk| uploaded.insert(chunk.digest.clone()))
            .collect::<Vec<_>>();

        if !missing_chunks.is_empty() {
            self.upload_direct(
                metadata.clone(),
                UploadRequest {
                    inlined_blobs_with_digest: Some(missing_chunks),
                    upload_only_missing: false,
                    ..Default::default()
                },
            )
            .await?;
        }

        self.splice_blob(
            metadata,
            SpliceBlobRequest {
                blob_digest,
                chunk_digests,
                chunking_function: config.chunking_function(),
                ..Default::default()
            },
        )
        .await?;

        Ok(())
    }

    async fn write_fast_cdc_chunks_to_local_cache(
        &self,
        chunks: &[InlinedBlobWithDigest],
        digest_function: digest_function::Value,
    ) {
        let Some(cache) = &self.local_chunk_cache else {
            return;
        };

        for chunk in chunks {
            cache
                .write(&chunk.digest, &chunk.blob, Some(digest_function))
                .await;
        }
    }

    fn fast_cdc_2020_upload_config(&self) -> Option<FastCdc2020Config> {
        if !self.runtime_opts.remote_cache_chunking || !self.capabilities.blob_splice_supported {
            return None;
        }

        self.capabilities.fast_cdc_2020.clone()
    }

    fn fast_cdc_2020_download_config(&self) -> Option<FastCdc2020Config> {
        if !self.runtime_opts.remote_cache_chunking || !self.capabilities.blob_split_supported {
            return None;
        }

        self.capabilities.fast_cdc_2020.clone()
    }

    fn should_chunk_blob(&self, digest: &TDigest, config: &FastCdc2020Config) -> bool {
        digest.size_in_bytes > 0 && digest.size_in_bytes as u64 > config.chunking_threshold_bytes()
    }

    async fn filter_upload_request_to_missing(
        &self,
        metadata: RemoteExecutionMetadata,
        request: UploadRequest,
    ) -> anyhow::Result<UploadRequest> {
        let digests = upload_request_digests(&request);
        if digests.is_empty() {
            return Ok(request);
        }

        let missing = self
            .get_digests_ttl(
                &metadata,
                GetDigestsTtlRequest {
                    digests,
                    is_for_upload: Some(true),
                    _dot_dot: (),
                },
            )
            .await?
            .digests_with_ttl
            .into_iter()
            .filter(|digest| digest.ttl == 0)
            .map(|digest| digest.digest)
            .collect::<HashSet<_>>();

        Ok(filter_upload_request_by_missing_digests(request, &missing))
    }

    fn without_digests_known_on_remote(&self, mut request: UploadRequest) -> UploadRequest {
        let mut find_missing_cache = self.find_missing_cache.lock().unwrap();
        let mut known = |digest: &TDigest| {
            find_missing_cache.get(digest) == Some(DigestRemoteState::ExistsOnRemote)
        };
        if let Some(blobs) = &mut request.inlined_blobs_with_digest {
            blobs.retain(|blob| !known(&blob.digest));
        }
        if let Some(files) = &mut request.files_with_digest {
            files.retain(|file| !known(&file.digest));
        }
        request
    }

    fn mark_digests_exist_on_remote(&self, digests: impl IntoIterator<Item = TDigest>) {
        let mut find_missing_cache = self.find_missing_cache.lock().unwrap();
        for digest in digests {
            find_missing_cache.put(digest, DigestRemoteState::ExistsOnRemote);
        }
    }

    pub async fn upload_blob_with_digest(
        &self,
        blob: Vec<u8>,
        digest: TDigest,
        metadata: &RemoteExecutionMetadata,
    ) -> anyhow::Result<TDigest> {
        let blob = InlinedBlobWithDigest {
            digest: digest.clone(),
            blob,
            ..Default::default()
        };
        self.upload(
            metadata.clone(),
            UploadRequest {
                inlined_blobs_with_digest: Some(vec![blob]),
                files_with_digest: None,
                directories: None,
                upload_only_missing: false,
                ..Default::default()
            },
        )
        .await?;
        Ok(digest)
    }

    pub async fn split_blob(
        &self,
        metadata: RemoteExecutionMetadata,
        request: SplitBlobRequest,
    ) -> anyhow::Result<SplitBlobResponse> {
        validate_blob_split_supported(self.capabilities.blob_split_supported)?;

        let SplitBlobRequest {
            blob_digest,
            chunking_function,
            ..
        } = request;
        let blob_digest = tdigest_to(blob_digest);
        let request_chunking_function =
            preferred_split_blob_chunking_function(&self.capabilities, chunking_function);
        validate_chunking_function_supported(&self.capabilities, request_chunking_function)?;
        let request_chunking_function = chunking_function_to_grpc(request_chunking_function);
        let digest_function = digest_function_to_grpc(
            self.runtime_opts
                .request_digest_function_config
                .for_digest(&blob_digest),
        );
        let res: GSplitBlobResponse = retry_grpc_request_with_client_reconnect(
            self.grpc_clients.clone(),
            GrpcClientKind::Cas,
            self.runtime_opts.retries,
            Duration::from_millis(self.runtime_opts.retry_max_delay_ms),
            || {
                let grpc_clients = self.grpc_clients.clone();
                let metadata = metadata.clone();
                let blob_digest = blob_digest.clone();
                async move {
                    let mut client = grpc_clients.cas_client().await?;
                    client
                        .split_blob(with_re_metadata_timeout(
                            GSplitBlobRequest {
                                instance_name: self.instance_name.as_str().to_owned(),
                                blob_digest: Some(blob_digest),
                                digest_function,
                                chunking_function: request_chunking_function,
                            },
                            metadata,
                            self.runtime_opts.use_fbcode_metadata,
                            self.runtime_opts.request_metadata_tool_name.as_str(),
                            self.runtime_opts.grpc_request_timeout,
                        ))
                        .await
                        .map(|response| response.into_inner())
                        .map_err(anyhow::Error::from)
                }
            },
        )
        .await?;

        let chunking_function = chunking_function_from_grpc(res.chunking_function);
        Ok(SplitBlobResponse {
            chunk_digests: validate_split_blob_response(&blob_digest, res)?,
            chunking_function,
        })
    }

    pub async fn splice_blob(
        &self,
        metadata: RemoteExecutionMetadata,
        request: SpliceBlobRequest,
    ) -> anyhow::Result<SpliceBlobResponse> {
        validate_blob_splice_supported(self.capabilities.blob_splice_supported)?;

        let SpliceBlobRequest {
            blob_digest,
            chunk_digests,
            chunking_function,
            ..
        } = request;
        let blob_digest = tdigest_to(blob_digest);
        let chunk_digests = chunk_digests
            .into_iter()
            .map(tdigest_to)
            .collect::<Vec<_>>();
        validate_chunking_function_supported(&self.capabilities, chunking_function)?;
        let request_chunking_function = chunking_function_to_grpc(chunking_function);
        validate_chunk_digests_reconstruct_blob(
            "SpliceBlob request",
            &blob_digest,
            &chunk_digests,
        )?;
        let mut request_digests = vec![blob_digest.clone()];
        request_digests.extend(chunk_digests.iter().cloned());
        let digest_function = digest_function_to_grpc(
            self.runtime_opts
                .request_digest_function_config
                .for_common_digest_function(&request_digests),
        );

        let res: GSpliceBlobResponse = retry_grpc_request_with_client_reconnect(
            self.grpc_clients.clone(),
            GrpcClientKind::Cas,
            self.runtime_opts.retries,
            Duration::from_millis(self.runtime_opts.retry_max_delay_ms),
            || {
                let grpc_clients = self.grpc_clients.clone();
                let metadata = metadata.clone();
                let blob_digest = blob_digest.clone();
                let chunk_digests = chunk_digests.clone();
                async move {
                    let mut client = grpc_clients.cas_client().await?;
                    client
                        .splice_blob(with_re_metadata_timeout(
                            GSpliceBlobRequest {
                                instance_name: self.instance_name.as_str().to_owned(),
                                blob_digest: Some(blob_digest),
                                chunk_digests,
                                digest_function,
                                chunking_function: request_chunking_function,
                            },
                            metadata,
                            self.runtime_opts.use_fbcode_metadata,
                            self.runtime_opts.request_metadata_tool_name.as_str(),
                            self.runtime_opts.grpc_request_timeout,
                        ))
                        .await
                        .map(|response| response.into_inner())
                        .map_err(anyhow::Error::from)
                }
            },
        )
        .await?;

        let blob_digest = validate_splice_blob_response_digest(&blob_digest, res)?;

        Ok(SpliceBlobResponse { blob_digest })
    }

    pub async fn download(
        &self,
        metadata: &RemoteExecutionMetadata,
        request: DownloadRequest,
    ) -> anyhow::Result<DownloadResponse> {
        let (request, chunked_inlined_blobs) = self
            .download_chunked_blobs(metadata.clone(), request)
            .await?;
        let mut response = self.download_direct(metadata.clone(), request).await?;
        if !chunked_inlined_blobs.is_empty() {
            let mut chunked_inlined_blobs =
                chunked_inlined_blobs.into_iter().collect::<HashMap<_, _>>();
            let direct_inlined_blobs = response.inlined_blobs.take().unwrap_or_default();
            let mut direct_inlined_blobs = direct_inlined_blobs.into_iter();
            let mut inlined_blobs = Vec::new();
            for index in 0..chunked_inlined_blobs.len() + direct_inlined_blobs.len() {
                if let Some(chunked_blob) = chunked_inlined_blobs.remove(&index) {
                    inlined_blobs.push(chunked_blob);
                } else if let Some(direct_blob) = direct_inlined_blobs.next() {
                    inlined_blobs.push(direct_blob);
                }
            }
            response.inlined_blobs = Some(inlined_blobs);
        }

        Ok(response)
    }

    async fn download_direct(
        &self,
        metadata: RemoteExecutionMetadata,
        request: DownloadRequest,
    ) -> anyhow::Result<DownloadResponse> {
        download_impl(
            &self.instance_name,
            request,
            self.bystream_compressor,
            self.capabilities.max_total_batch_size,
            self.runtime_opts.remote_cache_compression_threshold,
            self.runtime_opts.download_hash_digest_function,
            self.runtime_opts.request_digest_function_config,
            self.runtime_opts.retries,
            Duration::from_millis(self.runtime_opts.retry_max_delay_ms),
            self.runtime_opts.bytestream_progress_timeout,
            &self.active_downloads,
            self.shared_cache.as_ref(),
            |re_request| {
                let metadata = metadata.clone();
                async move {
                    let (digest_count, bytes) =
                        request_stats_for_grpc_digests(re_request.digests.iter());
                    let mut start = remote_request_start("CAS", "BatchReadBlobs", &metadata, None);
                    start.digest_count = Some(digest_count);
                    start.bytes = Some(bytes);
                    remote_request_span(
                        start,
                        retry_grpc_request_with_client_reconnect(
                            self.grpc_clients.clone(),
                            GrpcClientKind::Cas,
                            self.runtime_opts.retries,
                            Duration::from_millis(self.runtime_opts.retry_max_delay_ms),
                            || {
                                let grpc_clients = self.grpc_clients.clone();
                                let metadata = metadata.clone();
                                let re_request = re_request.clone();
                                async move {
                                    let mut client = grpc_clients.cas_client().await?;
                                    Ok(client
                                        .batch_read_blobs(with_re_metadata_timeout(
                                            re_request,
                                            metadata,
                                            self.runtime_opts.use_fbcode_metadata,
                                            self.runtime_opts.request_metadata_tool_name.as_str(),
                                            self.runtime_opts.grpc_request_timeout,
                                        ))
                                        .await?
                                        .into_inner())
                                }
                            },
                        ),
                    )
                    .await
                }
            },
            |read_request| {
                let metadata = metadata.clone();
                async move {
                    let mut start = remote_request_start("ByteStream", "Read", &metadata, None);
                    start.details.insert(
                        "resource_name".to_owned(),
                        read_request.resource_name.clone(),
                    );
                    if read_request.read_limit > 0 {
                        start.bytes =
                            Some(u64::try_from(read_request.read_limit).unwrap_or_default());
                    }
                    let response = remote_request_span(
                        start,
                        retry_grpc_request_with_client_reconnect(
                            self.grpc_clients.clone(),
                            GrpcClientKind::ByteStream,
                            self.runtime_opts.retries,
                            Duration::from_millis(self.runtime_opts.retry_max_delay_ms),
                            || {
                                let grpc_clients = self.grpc_clients.clone();
                                let metadata = metadata.clone();
                                let read_request = read_request.clone();
                                async move {
                                    let mut client = grpc_clients.bytestream_client().await?;
                                    Ok(client
                                        .read(with_re_metadata(
                                            read_request,
                                            &metadata,
                                            self.runtime_opts.use_fbcode_metadata,
                                            self.runtime_opts.request_metadata_tool_name.as_str(),
                                        ))
                                        .await?
                                        .into_inner())
                                }
                            },
                        ),
                    )
                    .await?;
                    Ok(Box::pin(response.into_stream()))
                }
            },
            || {
                let grpc_clients = self.grpc_clients.clone();
                async move {
                    grpc_clients
                        .reconnect_after_broken_connection(GrpcClientKind::ByteStream)
                        .await;
                }
            },
        )
        .await
    }

    async fn download_chunked_blobs(
        &self,
        metadata: RemoteExecutionMetadata,
        mut request: DownloadRequest,
    ) -> anyhow::Result<(DownloadRequest, Vec<(usize, InlinedDigestWithStatus)>)> {
        let Some(config) = self.fast_cdc_2020_download_config() else {
            return Ok((request, Vec::new()));
        };

        let inlined_digests = request.inlined_digests.take().unwrap_or_default();
        let file_digests = request.file_digests.take().unwrap_or_default();
        if inlined_digests.is_empty() && file_digests.is_empty() {
            return Ok((request, Vec::new()));
        }

        let mut remaining_inlined_digests = Vec::new();
        let mut chunked_inlined_blobs = Vec::new();
        for (index, digest) in inlined_digests.into_iter().enumerate() {
            if !self.should_chunk_blob(&digest, &config) {
                remaining_inlined_digests.push(digest);
                continue;
            }

            let blob = self
                .download_fast_cdc_blob(metadata.clone(), digest.clone(), &config)
                .await?;
            chunked_inlined_blobs.push((
                index,
                InlinedDigestWithStatus {
                    digest,
                    status: tstatus_ok(),
                    blob,
                },
            ));
        }

        let mut remaining_file_digests = Vec::new();
        for file in file_digests {
            if !self.should_chunk_blob(&file.named_digest.digest, &config) {
                remaining_file_digests.push(file);
                continue;
            }

            self.download_fast_cdc_file(metadata.clone(), file, &config)
                .await?;
        }

        request.inlined_digests = if remaining_inlined_digests.is_empty() {
            None
        } else {
            Some(remaining_inlined_digests)
        };
        request.file_digests = if remaining_file_digests.is_empty() {
            None
        } else {
            Some(remaining_file_digests)
        };

        Ok((request, chunked_inlined_blobs))
    }

    async fn download_fast_cdc_blob(
        &self,
        metadata: RemoteExecutionMetadata,
        digest: TDigest,
        config: &FastCdc2020Config,
    ) -> anyhow::Result<Vec<u8>> {
        let split = self
            .split_blob(
                metadata.clone(),
                SplitBlobRequest {
                    blob_digest: digest.clone(),
                    chunking_function: config.chunking_function(),
                    ..Default::default()
                },
            )
            .await?;
        let chunk_digests = split.chunk_digests;
        let chunk_blobs = self
            .download_fast_cdc_chunks(metadata, &chunk_digests)
            .await?;

        let mut blob = Vec::new();
        for chunk_digest in chunk_digests {
            let chunk_blob = chunk_blobs
                .get(&chunk_digest)
                .with_context(|| format!("Chunked download missing chunk `{chunk_digest}`"))?;
            blob.extend_from_slice(chunk_blob);
        }
        validate_downloaded_blob(
            &digest,
            &blob,
            self.runtime_opts
                .download_hash_digest_function_for_hash(&digest.hash),
        )?;

        Ok(blob)
    }

    async fn download_fast_cdc_chunks(
        &self,
        metadata: RemoteExecutionMetadata,
        chunk_digests: &[TDigest],
    ) -> anyhow::Result<HashMap<TDigest, Vec<u8>>> {
        let mut chunk_blobs = HashMap::new();
        let mut missing_digests = Vec::new();
        let mut queued_missing = HashSet::new();

        for digest in chunk_digests {
            if chunk_blobs.contains_key(digest) {
                continue;
            }
            if let Some(cache) = &self.local_chunk_cache {
                if let Some(blob) = cache
                    .read(
                        digest,
                        self.runtime_opts
                            .download_hash_digest_function_for_hash(&digest.hash),
                    )
                    .await?
                {
                    chunk_blobs.insert(digest.clone(), blob);
                    continue;
                }
            }
            if queued_missing.insert(digest.clone()) {
                missing_digests.push(digest.clone());
            }
        }

        if missing_digests.is_empty() {
            return Ok(chunk_blobs);
        }

        let response = self
            .download_direct(
                metadata,
                DownloadRequest {
                    inlined_digests: Some(missing_digests.clone()),
                    ..Default::default()
                },
            )
            .await?;
        let remote_chunk_blobs = response.inlined_blobs.unwrap_or_default();
        anyhow::ensure!(
            remote_chunk_blobs.len() == missing_digests.len(),
            "Chunked download received {} chunks, expected {}",
            remote_chunk_blobs.len(),
            missing_digests.len()
        );

        for (expected_digest, chunk_blob) in missing_digests.into_iter().zip(remote_chunk_blobs) {
            anyhow::ensure!(
                chunk_blob.digest == expected_digest,
                "Chunked download received digest `{}`, expected `{expected_digest}`",
                chunk_blob.digest
            );
            if let Some(cache) = &self.local_chunk_cache {
                cache
                    .write(
                        &expected_digest,
                        &chunk_blob.blob,
                        self.runtime_opts
                            .download_hash_digest_function_for_hash(&expected_digest.hash),
                    )
                    .await;
            }
            chunk_blobs.insert(expected_digest, chunk_blob.blob);
        }

        Ok(chunk_blobs)
    }

    async fn download_fast_cdc_chunk(
        &self,
        metadata: RemoteExecutionMetadata,
        digest: &TDigest,
    ) -> anyhow::Result<Vec<u8>> {
        if let Some(cache) = &self.local_chunk_cache {
            if let Some(blob) = cache
                .read(
                    digest,
                    self.runtime_opts
                        .download_hash_digest_function_for_hash(&digest.hash),
                )
                .await?
            {
                return Ok(blob);
            }
        }

        let response = self
            .download_direct(
                metadata,
                DownloadRequest {
                    inlined_digests: Some(vec![digest.clone()]),
                    ..Default::default()
                },
            )
            .await?;
        let mut remote_chunk_blobs = response.inlined_blobs.unwrap_or_default();
        anyhow::ensure!(
            remote_chunk_blobs.len() == 1,
            "Chunked download received {} chunks, expected 1",
            remote_chunk_blobs.len()
        );

        let chunk_blob = remote_chunk_blobs
            .pop()
            .context("Missing downloaded chunk")?;
        anyhow::ensure!(
            chunk_blob.digest == *digest,
            "Chunked download received digest `{}`, expected `{digest}`",
            chunk_blob.digest
        );
        if let Some(cache) = &self.local_chunk_cache {
            cache
                .write(
                    digest,
                    &chunk_blob.blob,
                    self.runtime_opts
                        .download_hash_digest_function_for_hash(&digest.hash),
                )
                .await;
        }

        Ok(chunk_blob.blob)
    }

    async fn download_fast_cdc_file(
        &self,
        metadata: RemoteExecutionMetadata,
        file: NamedDigestWithPermissions,
        config: &FastCdc2020Config,
    ) -> anyhow::Result<()> {
        let split = self
            .split_blob(
                metadata.clone(),
                SplitBlobRequest {
                    blob_digest: file.named_digest.digest.clone(),
                    chunking_function: config.chunking_function(),
                    ..Default::default()
                },
            )
            .await?;
        let chunk_digests = split.chunk_digests;

        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            if file.is_executable {
                opts.mode(0o755);
            } else {
                opts.mode(0o644);
            }
        }

        let mut output = opts
            .open(&file.named_digest.name)
            .await
            .context("Error opening")?;
        let mut hash_validators = BlobHashValidators::new(
            &file.named_digest.digest.hash,
            self.runtime_opts
                .download_hash_digest_function_for_hash(&file.named_digest.digest.hash),
        )?;
        let mut copied_bytes = 0usize;

        for chunk_digest in chunk_digests {
            let chunk_blob = self
                .download_fast_cdc_chunk(metadata.clone(), &chunk_digest)
                .await?;
            copied_bytes = copied_bytes
                .checked_add(chunk_blob.len())
                .with_context(|| {
                    format!(
                        "Downloaded blob is too large to validate on this platform: {}",
                        file.named_digest.digest
                    )
                })?;
            hash_validators.update(&chunk_blob);
            output
                .write_all(chunk_blob.as_slice())
                .await
                .with_context(|| format!("Error writing chunk of: {}", file.named_digest.digest))?;
        }
        validate_downloaded_blob_size(&file.named_digest.digest, copied_bytes)?;
        hash_validators.finish(&file.named_digest.digest)?;
        output.flush().await.context("Error flushing")?;

        Ok(())
    }

    pub async fn get_digests_ttl(
        &self,
        metadata: &RemoteExecutionMetadata,
        request: GetDigestsTtlRequest,
    ) -> anyhow::Result<GetDigestsTtlResponse> {
        let mut remote_results: HashMap<TDigest, DigestRemoteState> = HashMap::new();
        let mut to_resolve = request.digests.clone();

        // Each round answers what it can from the cache, joins the FindMissingBlobs calls and
        // uploads already in flight for other digests, as Bazel's RemoteExecutionCache shares
        // one find-missing-then-upload task per digest through its `findMissingCache`
        // (src/main/java/com/google/devtools/build/lib/remote/RemoteExecutionCache.java), and
        // asks the CAS about the rest. A digest whose joined call failed, other than with the
        // server's answer, goes round again, and the next round asks about it itself.
        while !to_resolve.is_empty() {
            let mut to_check = Vec::new();
            let mut uploads = Vec::new();
            {
                let mut find_missing_cache = self.find_missing_cache.lock().unwrap();
                let shared_batch_uploads = self.shared_batch_uploads.lock();
                for digest in to_resolve.drain(..) {
                    if remote_results.contains_key(&digest) {
                        continue;
                    }
                    if let Some(state) = find_missing_cache.get(&digest) {
                        remote_results.insert(digest, state);
                    } else if let Some(upload) = shared_batch_uploads.get(&digest) {
                        uploads.push((digest, upload));
                    } else {
                        to_check.push(digest);
                    }
                }
            }

            let (started, joined) = {
                let mut calls = self.shared_find_missing.lock();
                let mut claimed = HashSet::new();
                let mut joined = Vec::new();
                let mut own = Vec::new();
                for digest in to_check {
                    if claimed.contains(&digest) {
                        continue;
                    }
                    match calls.get(&digest) {
                        Some(call) => joined.push((digest, call)),
                        None => {
                            claimed.insert(digest.clone());
                            own.push(digest);
                        }
                    }
                }
                let started = own
                    .chunks(self.runtime_opts.find_missing_blobs_batch_size.max(1))
                    .map(|chunk| {
                        let call = calls.start(
                            chunk.to_vec(),
                            self.cas_context
                                .clone()
                                .find_missing_blobs(metadata.clone(), chunk.to_vec()),
                        );
                        (chunk.to_vec(), call)
                    })
                    .collect::<Vec<_>>();
                (started, joined)
            };

            for (digests, call) in started {
                let missing = call.await.map_err(SharedCallFailure::into_error)?;
                let mut find_missing_cache = self.find_missing_cache.lock().unwrap();
                record_find_missing_results(
                    &digests,
                    &missing,
                    &mut remote_results,
                    &mut find_missing_cache,
                );
            }
            for (digest, call) in joined {
                match call.await {
                    Ok(missing) => {
                        let mut find_missing_cache = self.find_missing_cache.lock().unwrap();
                        record_find_missing_results(
                            std::slice::from_ref(&digest),
                            &missing,
                            &mut remote_results,
                            &mut find_missing_cache,
                        );
                    }
                    Err(failure) if failure.final_for_every_caller => {
                        return Err(failure
                            .into_error()
                            .context("Failed to request what blobs are not present on remote"));
                    }
                    Err(_) => to_resolve.push(digest),
                }
            }
            for (digest, upload) in uploads {
                match upload.await {
                    // `upload_direct` marks the digest present in the cache too, once the
                    // whole request it was part of is stored.
                    Ok(_) => {
                        remote_results.insert(digest, DigestRemoteState::ExistsOnRemote);
                    }
                    Err(_) => to_resolve.push(digest),
                }
            }
        }

        Ok(GetDigestsTtlResponse {
            digests_with_ttl: digests_with_ttl_for_requested_digests(
                &request.digests,
                &remote_results,
                self.runtime_opts.cas_ttl_secs,
            )?,
        })
    }

    pub async fn extend_digest_ttl(
        &self,
        metadata: RemoteExecutionMetadata,
        request: ExtendDigestsTtlRequest,
    ) -> anyhow::Result<TDigest> {
        let response = self
            .get_digests_ttl(
                &metadata,
                GetDigestsTtlRequest {
                    digests: request.digests.clone(),
                    ..Default::default()
                },
            )
            .await
            .context("Failed to refresh CAS TTLs with FindMissingBlobs")?;
        validate_extend_digests_ttl_response(&request.digests, response)?;
        Ok(TDigest::default())
    }

    pub fn get_execution_client(&self) -> &Self {
        self
    }

    pub fn get_cas_client(&self) -> &Self {
        self
    }

    pub fn get_action_cache_client(&self) -> &Self {
        self
    }

    pub fn get_metrics_client(&self) -> &Self {
        self
    }

    pub fn get_session_id(&self) -> &str {
        // TODO(aloiscochard): Return a unique ID, ideally from the GRPC client
        "GRPC-SESSION-ID"
    }

    pub fn get_experiment_name(&self) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
}

fn validate_upload_digest_size(
    digest: &TDigest,
    max_cas_blob_size_bytes: Option<i64>,
) -> anyhow::Result<()> {
    let Some(max_cas_blob_size_bytes) = max_cas_blob_size_bytes else {
        return Ok(());
    };

    if digest.size_in_bytes > max_cas_blob_size_bytes {
        return Err(anyhow::anyhow!(
            "CAS blob `{digest}` is {} bytes, exceeding server max_cas_blob_size_bytes {}",
            digest.size_in_bytes,
            max_cas_blob_size_bytes
        ));
    }

    Ok(())
}

fn validate_upload_request_sizes(
    request: &UploadRequest,
    max_cas_blob_size_bytes: Option<i64>,
) -> anyhow::Result<()> {
    for blob in request.inlined_blobs_with_digest.iter().flatten() {
        validate_upload_digest_size(&blob.digest, max_cas_blob_size_bytes)
            .context("Upload request contains an oversized inlined blob")?;
    }

    for file in request.files_with_digest.iter().flatten() {
        validate_upload_digest_size(&file.digest, max_cas_blob_size_bytes)
            .with_context(|| format!("Upload request contains oversized file `{}`", file.name))?;
    }

    for directory in request.directories.iter().flatten() {
        if let Some(digest) = &directory.digest {
            validate_upload_digest_size(digest, max_cas_blob_size_bytes).with_context(|| {
                format!(
                    "Upload request contains oversized directory `{}`",
                    directory.path
                )
            })?;
        }
    }

    Ok(())
}

fn upload_request_digests(request: &UploadRequest) -> Vec<TDigest> {
    let mut digests = Vec::new();

    digests.extend(upload_payload_digests(request));
    if let Some(directories) = &request.directories {
        digests.extend(
            directories
                .iter()
                .filter_map(|directory| directory.digest.clone()),
        );
    }

    digests
}

fn upload_payload_digests(request: &UploadRequest) -> Vec<TDigest> {
    let mut digests = Vec::new();

    if let Some(blobs) = &request.inlined_blobs_with_digest {
        digests.extend(blobs.iter().map(|blob| blob.digest.clone()));
    }
    if let Some(files) = &request.files_with_digest {
        digests.extend(files.iter().map(|file| file.digest.clone()));
    }

    digests
}

fn filter_upload_request_by_missing_digests(
    mut request: UploadRequest,
    missing_digests: &HashSet<TDigest>,
) -> UploadRequest {
    request.upload_only_missing = false;

    if let Some(blobs) = request.inlined_blobs_with_digest.take() {
        request.inlined_blobs_with_digest = Some(
            blobs
                .into_iter()
                .filter(|blob| missing_digests.contains(&blob.digest))
                .collect(),
        );
    }
    if let Some(files) = request.files_with_digest.take() {
        request.files_with_digest = Some(
            files
                .into_iter()
                .filter(|file| missing_digests.contains(&file.digest))
                .collect(),
        );
    }
    if let Some(directories) = request.directories.take() {
        request.directories = Some(
            directories
                .into_iter()
                .filter(|directory| {
                    directory
                        .digest
                        .as_ref()
                        .is_some_and(|digest| missing_digests.contains(digest))
                })
                .collect(),
        );
    }

    request
}

fn digests_with_ttl_for_requested_digests(
    requested_digests: &[TDigest],
    remote_results: &HashMap<TDigest, DigestRemoteState>,
    cas_ttl_secs: i64,
) -> anyhow::Result<Vec<DigestWithTtl>> {
    requested_digests
        .iter()
        .map(|digest| {
            let state = remote_results.get(digest).with_context(|| {
                format!("No FindMissingBlobs result recorded for requested digest `{digest}`")
            })?;
            let ttl = match state {
                DigestRemoteState::Missing => 0,
                DigestRemoteState::ExistsOnRemote => cas_ttl_secs,
            };
            Ok(DigestWithTtl {
                digest: digest.clone(),
                ttl,
            })
        })
        .collect()
}

fn record_find_missing_results(
    digests_to_check: &[TDigest],
    missing: &HashSet<TDigest>,
    remote_results: &mut HashMap<TDigest, DigestRemoteState>,
    find_missing_cache: &mut FindMissingCache,
) {
    for digest in digests_to_check {
        if missing.contains(digest) {
            remote_results.insert(digest.clone(), DigestRemoteState::Missing);
        } else {
            remote_results.insert(digest.clone(), DigestRemoteState::ExistsOnRemote);
            find_missing_cache.put(digest.clone(), DigestRemoteState::ExistsOnRemote);
        }
    }
}

fn validate_extend_digests_ttl_response(
    requested_digests: &[TDigest],
    response: GetDigestsTtlResponse,
) -> anyhow::Result<()> {
    let response_count = response.digests_with_ttl.len();
    anyhow::ensure!(
        requested_digests.len() == response_count,
        "Invalid CAS TTL refresh response: expected {} digests, got {}",
        requested_digests.len(),
        response_count,
    );

    let missing_digests = response
        .digests_with_ttl
        .into_iter()
        .filter(|digest_ttl| digest_ttl.ttl <= 0)
        .map(|digest_ttl| digest_ttl.digest.to_string())
        .collect::<Vec<_>>();

    anyhow::ensure!(
        missing_digests.is_empty(),
        "Cannot refresh CAS TTL for missing digests: {}",
        missing_digests.join(", ")
    );

    Ok(())
}

fn digest_name(digest: &Digest) -> String {
    format!("{}/{}", digest.hash, digest.size_bytes)
}

fn validate_find_missing_blobs_response_digests(
    requested_digests: &[Digest],
    response: &FindMissingBlobsResponse,
) -> anyhow::Result<()> {
    let mut unmatched_digests = requested_digests.to_vec();
    let mut failures = Vec::new();

    for digest in &response.missing_blob_digests {
        let Some(index) = unmatched_digests
            .iter()
            .position(|requested| requested == digest)
        else {
            failures.push(format!(
                "FindMissingBlobs response included unexpected digest `{}`",
                digest_name(digest)
            ));
            continue;
        };
        unmatched_digests.swap_remove(index);
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("FindMissingBlobs failed: {:?}", failures))
    }
}

fn validate_batch_read_blobs_response_digests(
    requested_digests: &[Digest],
    response: &BatchReadBlobsResponse,
) -> anyhow::Result<()> {
    let mut missing_digests = requested_digests.to_vec();
    let mut failures = Vec::new();

    for response in &response.responses {
        let Some(digest) = &response.digest else {
            failures.push("BatchReadBlobs response omitted a digest".to_owned());
            continue;
        };

        let Some(index) = missing_digests
            .iter()
            .position(|requested| requested == digest)
        else {
            failures.push(format!(
                "BatchReadBlobs response included unexpected digest `{}`",
                digest_name(digest)
            ));
            continue;
        };
        missing_digests.swap_remove(index);
    }

    for digest in &missing_digests {
        failures.push(format!(
            "BatchReadBlobs response missing digest `{}`",
            digest_name(digest)
        ));
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Batch download failed: {:?}", failures))
    }
}

fn validate_batch_update_blobs_response(
    requested_digests: &[Digest],
    response: &BatchUpdateBlobsResponse,
) -> anyhow::Result<()> {
    let mut missing_digests = requested_digests.to_vec();
    let mut failures = Vec::new();

    for response in &response.responses {
        let Some(digest) = &response.digest else {
            failures.push("BatchUpdateBlobs response omitted a digest".to_owned());
            continue;
        };

        let Some(index) = missing_digests
            .iter()
            .position(|requested| requested == digest)
        else {
            failures.push(format!(
                "BatchUpdateBlobs response included unexpected digest `{}`",
                digest_name(digest)
            ));
            continue;
        };
        missing_digests.swap_remove(index);

        let status = response.status.as_ref().cloned().unwrap_or_default();
        // ALREADY_EXISTS says the blob is stored, which is what the upload was for; Bazel's
        // RemoteRetrier counts it a success too.
        if status.code != Code::Ok as i32 && status.code != Code::AlreadyExists as i32 {
            failures.push(format!(
                "Unable to upload blob '{}', rpc status code: {}, message: \"{}\"",
                digest_name(digest),
                status.code,
                status.message
            ));
        }
    }

    for digest in &missing_digests {
        failures.push(format!(
            "BatchUpdateBlobs response missing digest `{}`",
            digest_name(digest)
        ));
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Batch upload failed: {:?}", failures))
    }
}

fn validate_splice_blob_response_digest(
    requested_digest: &Digest,
    response: GSpliceBlobResponse,
) -> anyhow::Result<TDigest> {
    let Some(response_digest) = response.blob_digest else {
        return Err(anyhow::anyhow!("SpliceBlob response omitted blob digest"));
    };

    if &response_digest != requested_digest {
        return Err(anyhow::anyhow!(
            "SpliceBlob response included unexpected digest `{}`; requested `{}`",
            digest_name(&response_digest),
            digest_name(requested_digest),
        ));
    }

    Ok(tdigest_from(response_digest))
}

fn validate_split_blob_response(
    requested_digest: &Digest,
    response: GSplitBlobResponse,
) -> anyhow::Result<Vec<TDigest>> {
    validate_chunk_digests_reconstruct_blob(
        "SplitBlob response",
        requested_digest,
        &response.chunk_digests,
    )?;

    Ok(response.chunk_digests.into_map(tdigest_from))
}

fn validate_chunk_digests_reconstruct_blob(
    context: &str,
    requested_digest: &Digest,
    chunk_digests: &[Digest],
) -> anyhow::Result<()> {
    if requested_digest.size_bytes > 0 && chunk_digests.is_empty() {
        return Err(anyhow::anyhow!(
            "{context} included no chunks for non-empty digest `{}`",
            digest_name(requested_digest),
        ));
    }

    let mut total_size = 0i64;
    for chunk_digest in chunk_digests {
        if chunk_digest.hash.len() != requested_digest.hash.len() {
            return Err(anyhow::anyhow!(
                "{context} included chunk digest `{}` with hash length {}, expected {}",
                digest_name(chunk_digest),
                chunk_digest.hash.len(),
                requested_digest.hash.len(),
            ));
        }
        if chunk_digest.size_bytes < 0 {
            return Err(anyhow::anyhow!(
                "{context} included negative-size chunk digest `{}`",
                digest_name(chunk_digest),
            ));
        }
        total_size = total_size
            .checked_add(chunk_digest.size_bytes)
            .with_context(|| {
                format!(
                    "{context} chunks are too large to sum for requested digest `{}`",
                    digest_name(requested_digest),
                )
            })?;
    }

    if total_size != requested_digest.size_bytes {
        return Err(anyhow::anyhow!(
            "{context} chunks sum to {total_size} bytes; requested digest `{}` has {} bytes",
            digest_name(requested_digest),
            requested_digest.size_bytes,
        ));
    }

    Ok(())
}

fn convert_action_result(
    action_result: ActionResult,
    cas_ttl_secs: i64,
) -> anyhow::Result<TActionResult2> {
    let execution_metadata = action_result
        .execution_metadata
        .with_context(|| "The execution metadata are not defined.")?;

    let output_files = action_result.output_files.into_try_map(|output_file| {
        let output_file_digest = output_file.digest.with_context(|| "Digest not found.")?;

        anyhow::Ok(TFile {
            digest: DigestWithStatus {
                status: tstatus_ok(),
                digest: tdigest_from(output_file_digest),
                _dot_dot_default: (),
            },
            name: output_file.path,
            existed: false,
            executable: output_file.is_executable,
            ttl: cas_ttl_secs,
            _dot_dot_default: (),
        })
    })?;

    let output_symlinks = action_result
        .output_symlinks
        .into_try_map(|output_symlink| {
            anyhow::Ok(TSymlink {
                name: output_symlink.path,
                target: output_symlink.target,
                _dot_dot_default: (),
            })
        })?;

    let output_directories = action_result
        .output_directories
        .into_try_map(|output_directory| {
            let digest = tdigest_from(
                output_directory
                    .tree_digest
                    .with_context(|| "Tree digest not defined.")?,
            );
            anyhow::Ok(TDirectory2 {
                path: output_directory.path,
                tree_digest: digest.clone(),
                root_directory_digest: digest,
                _dot_dot_default: (),
            })
        })?;

    let action_result = TActionResult2 {
        output_files,
        output_symlinks,
        output_directories,
        exit_code: action_result.exit_code,
        stdout_raw: Some(action_result.stdout_raw),
        stdout_digest: action_result.stdout_digest.map(tdigest_from),
        stderr_raw: Some(action_result.stderr_raw),
        stderr_digest: action_result.stderr_digest.map(tdigest_from),

        execution_metadata: TExecutedActionMetadata {
            worker: execution_metadata.worker,
            queued_timestamp: ttimestamp_from(execution_metadata.queued_timestamp),
            worker_start_timestamp: ttimestamp_from(execution_metadata.worker_start_timestamp),
            worker_completed_timestamp: ttimestamp_from(
                execution_metadata.worker_completed_timestamp,
            ),
            input_fetch_start_timestamp: ttimestamp_from(
                execution_metadata.input_fetch_start_timestamp,
            ),
            input_fetch_completed_timestamp: ttimestamp_from(
                execution_metadata.input_fetch_completed_timestamp,
            ),
            execution_start_timestamp: ttimestamp_from(
                execution_metadata.execution_start_timestamp,
            ),
            execution_completed_timestamp: ttimestamp_from(
                execution_metadata.execution_completed_timestamp,
            ),
            output_upload_start_timestamp: ttimestamp_from(
                execution_metadata.output_upload_start_timestamp,
            ),
            output_upload_completed_timestamp: ttimestamp_from(
                execution_metadata.output_upload_completed_timestamp,
            ),
            input_analyzing_start_timestamp: Default::default(),
            input_analyzing_completed_timestamp: Default::default(),
            execution_dir: "".to_owned(),
            execution_attempts: 0,
            last_queued_timestamp: Default::default(),
            auxiliary_metadata: execution_metadata
                .auxiliary_metadata
                .into_map(|metadata| TAny {
                    type_url: metadata.type_url,
                    value: metadata.value,
                    ..Default::default()
                }),
            ..Default::default()
        },
        ..Default::default()
    };

    Ok(action_result)
}

fn convert_t_action_result2(t_action_result: TActionResult2) -> anyhow::Result<ActionResult> {
    let t_execution_metadata = t_action_result.execution_metadata;
    let virtual_execution_duration = prost_types::Duration::try_from(
        t_execution_metadata
            .execution_completed_timestamp
            .saturating_duration_since(&t_execution_metadata.execution_start_timestamp),
    )?;
    let execution_metadata = Some(ExecutedActionMetadata {
        worker: t_execution_metadata.worker,
        queued_timestamp: Some(ttimestamp_to(t_execution_metadata.queued_timestamp)),
        worker_start_timestamp: Some(ttimestamp_to(t_execution_metadata.worker_start_timestamp)),
        worker_completed_timestamp: Some(ttimestamp_to(
            t_execution_metadata.worker_completed_timestamp,
        )),
        input_fetch_start_timestamp: Some(ttimestamp_to(
            t_execution_metadata.input_fetch_start_timestamp,
        )),
        input_fetch_completed_timestamp: Some(ttimestamp_to(
            t_execution_metadata.input_fetch_completed_timestamp,
        )),
        execution_start_timestamp: Some(ttimestamp_to(
            t_execution_metadata.execution_start_timestamp,
        )),
        execution_completed_timestamp: Some(ttimestamp_to(
            t_execution_metadata.execution_completed_timestamp,
        )),
        virtual_execution_duration: Some(virtual_execution_duration),
        output_upload_start_timestamp: Some(ttimestamp_to(
            t_execution_metadata.output_upload_start_timestamp,
        )),
        output_upload_completed_timestamp: Some(ttimestamp_to(
            t_execution_metadata.output_upload_completed_timestamp,
        )),
        auxiliary_metadata: t_execution_metadata
            .auxiliary_metadata
            .into_map(|metadata| prost_types::Any {
                type_url: metadata.type_url,
                value: metadata.value,
            }),
    });

    let output_files = t_action_result
        .output_files
        .into_map(|output_file| OutputFile {
            path: output_file.name,
            digest: Some(tdigest_to(output_file.digest.digest)),
            is_executable: output_file.executable,
            contents: Vec::new(),
            node_properties: None,
        });

    let output_symlinks =
        t_action_result
            .output_symlinks
            .into_map(|output_symlink| OutputSymlink {
                path: output_symlink.name,
                target: output_symlink.target,
                node_properties: None,
            });

    let output_directories = t_action_result
        .output_directories
        .into_map(|output_directory| {
            let digest = tdigest_to(output_directory.tree_digest);
            OutputDirectory {
                path: output_directory.path,
                tree_digest: Some(digest.clone()),
                is_topologically_sorted: false,
                root_directory_digest: None,
            }
        });

    let action_result = ActionResult {
        output_files,
        output_symlinks,
        output_directories,
        exit_code: t_action_result.exit_code,
        stdout_raw: t_action_result.stdout_raw.unwrap_or_default(),
        stdout_digest: t_action_result.stdout_digest.map(tdigest_to),
        stderr_raw: t_action_result.stderr_raw.unwrap_or_default(),
        stderr_digest: t_action_result.stderr_digest.map(tdigest_to),
        execution_metadata,
        ..Default::default()
    };

    Ok(action_result)
}

async fn read_with_progress_timeout(
    reader: &mut Pin<Box<dyn AsyncRead + Unpin + Send>>,
    buffer: &mut [u8],
    timeout: Duration,
) -> anyhow::Result<usize> {
    match tokio::time::timeout(timeout, reader.read(buffer)).await {
        Ok(result) => result.map_err(anyhow::Error::from),
        Err(_) => Err(REClientError {
            code: TCode::DEADLINE_EXCEEDED,
            message: format!(
                "ByteStream read made no progress for {}s",
                timeout.as_secs()
            ),
            group: TCodeReasonGroup::RE_CONNECTION,
        }
        .into()),
    }
}

async fn download_impl<Byt, BytRet, Cas, RetryFut>(
    instance_name: &InstanceName,
    request: DownloadRequest,
    bystream_compressor: Option<Compressor>,
    max_total_batch_size: usize,
    remote_cache_compression_threshold: usize,
    download_hash_digest_function: Option<digest_function::Value>,
    request_digest_function_config: DigestFunctionConfig,
    retries: usize,
    retry_max_delay: Duration,
    bystream_progress_timeout: Duration,
    active_downloads: &ActiveTransferRegistry<ActiveDownloadResult>,
    shared_cache: Option<&SharedCasCache>,
    cas_f: impl Fn(BatchReadBlobsRequest) -> Cas,
    bystream_fut: impl Fn(ReadRequest) -> Byt + Sync + Send + Copy,
    bystream_retry_hook: impl Fn() -> RetryFut + Sync + Send + Copy,
) -> anyhow::Result<DownloadResponse>
where
    Byt: Future<Output = anyhow::Result<Pin<Box<BytRet>>>>,
    BytRet: Stream<Item = Result<ReadResponse, tonic::Status>> + Send + 'static,
    Cas: Future<Output = anyhow::Result<BatchReadBlobsResponse>>,
    RetryFut: Future<Output = ()>,
{
    fn resource_name(
        instance_name: &InstanceName,
        compressor: Option<Compressor>,
        digest: &TDigest,
        request_digest_function_config: DigestFunctionConfig,
    ) -> String {
        let digest_function_segment =
            digest_function_resource_segment(request_digest_function_config.for_hash(&digest.hash));
        if let Some(compressor) = compressor {
            if let Some(digest_function_segment) = digest_function_segment {
                format!(
                    "{}compressed-blobs/{}/{}/{}/{}",
                    instance_name.as_resource_prefix(),
                    compressor.name(),
                    digest_function_segment,
                    digest.hash,
                    digest.size_in_bytes,
                )
            } else {
                format!(
                    "{}compressed-blobs/{}/{}/{}",
                    instance_name.as_resource_prefix(),
                    compressor.name(),
                    digest.hash,
                    digest.size_in_bytes,
                )
            }
        } else if let Some(digest_function_segment) = digest_function_segment {
            format!(
                "{}blobs/{}/{}/{}",
                instance_name.as_resource_prefix(),
                digest_function_segment,
                digest.hash,
                digest.size_in_bytes,
            )
        } else {
            format!(
                "{}blobs/{}/{}",
                instance_name.as_resource_prefix(),
                digest.hash,
                digest.size_in_bytes,
            )
        }
    }

    let bystream_reader = |digest: TDigest, read_offset: i64| async move {
        let compressor = compression_for_blob(
            bystream_compressor,
            digest.size_in_bytes,
            remote_cache_compression_threshold,
        );
        let resource_name = resource_name(
            instance_name,
            compressor,
            &digest,
            request_digest_function_config,
        );

        bystream_fut(ReadRequest {
            resource_name: resource_name.clone(),
            read_offset,
            read_limit: 0,
        })
        .await
        // adapt the tokio Stream of ReadResponse into a StreamReader
        .map(|p| {
            let blob_reader = StreamReader::new(
                p.map(|r| r.map(|rr| Cursor::new(rr.data)).map_err(io::Error::other)),
            );
            // Wrap the blob reader in a compression reader
            let reader: Pin<Box<dyn AsyncRead + Unpin + Send>> = match compressor {
                None => Pin::new(Box::new(blob_reader)),
                Some(Compressor::Zstd) => {
                    let mut decoder = ZstdDecoder::new(blob_reader);
                    decoder.multiple_members(true);
                    Pin::new(Box::new(decoder))
                }
                Some(Compressor::Deflate) => {
                    let mut decoder = DeflateDecoder::new(blob_reader);
                    decoder.multiple_members(true);
                    Pin::new(Box::new(decoder))
                }
                Some(Compressor::Brotli) => {
                    let mut decoder = BrotliDecoder::new(blob_reader);
                    decoder.multiple_members(true);
                    Pin::new(Box::new(decoder))
                }
            };

            reader
        })
        .with_context(|| format!("Failed to read {resource_name} from Bytestream service"))
    };

    let inlined_digests = request.inlined_digests.unwrap_or_default();
    let counters = SharedCacheCounters::default();
    // Clone whatever the daemon's directory already has, then have the daemon fetch the rest and
    // clone that too. Only what is still not there is received over gRPC below.
    let file_digests = match shared_cache {
        Some(cache) => {
            let (_, misses) = materialize_from_shared_cache(
                cache,
                request.file_digests.unwrap_or_default(),
                Some(&counters),
            )
            .await;
            warm_shared_cache(
                |digest| resource_name(instance_name, None, digest, request_digest_function_config),
                bystream_fut,
                &misses,
            )
            .await;
            materialize_from_shared_cache(cache, misses, None).await.1
        }
        None => request.file_digests.unwrap_or_default(),
    };

    let new_read_blob_req = |digests: &mut Vec<Digest>, batch_compressor: Option<Compressor>| {
        let digest_function = request_digest_function_config.for_common_digest_function(digests);
        let mut acceptable_compressors = vec![compressor::Value::Identity as i32];
        if let Some(compressor) = batch_compressor {
            acceptable_compressors.push(compressor.as_grpc());
        }
        BatchReadBlobsRequest {
            instance_name: instance_name.as_str().to_owned(),
            digests: std::mem::take(digests),
            acceptable_compressors,
            digest_function: digest_function_to_grpc(digest_function),
            ..Default::default()
        }
    };

    let mut curr_size = 0;
    let mut requests = vec![];
    let mut curr_digests = vec![];
    let mut curr_batch_compressor = None;
    for digest in file_digests
        .iter()
        .map(|req| &req.named_digest.digest)
        .chain(inlined_digests.iter())
        .map(|d| tdigest_to(d.clone()))
        .filter(|d| d.size_bytes > 0)
    {
        if digest.size_bytes as usize > max_total_batch_size {
            // digest is too big to download in a BatchReadBlobsRequest
            // need to use the bytstream api
            continue;
        }
        let digest_compressor = compression_for_blob(
            bystream_compressor,
            digest.size_bytes,
            remote_cache_compression_threshold,
        );
        let would_exceed = curr_size + digest.size_bytes > max_total_batch_size as i64;
        if !curr_digests.is_empty() && (would_exceed || digest_compressor != curr_batch_compressor)
        {
            requests.push(new_read_blob_req(&mut curr_digests, curr_batch_compressor));
            curr_size = digest.size_bytes;
        } else {
            curr_size += digest.size_bytes;
        }
        curr_digests.push(digest.clone());
        curr_batch_compressor = digest_compressor;
    }

    if !curr_digests.is_empty() {
        requests.push(new_read_blob_req(&mut curr_digests, curr_batch_compressor));
    }

    let mut batched_blobs_response = HashMap::new();
    for read_blob_req in requests {
        let requested_digests = read_blob_req.digests.clone();
        let resp = cas_f(read_blob_req)
            .await
            .context("Failed to make BatchReadBlobs request")?;
        validate_batch_read_blobs_response_digests(&requested_digests, &resp)?;
        for r in resp.responses.into_iter() {
            let digest = tdigest_from(r.digest.context("Response digest not found.")?);
            check_status(r.status.unwrap_or_default())?;
            let data = match Compressor::from_grpc(r.compressor) {
                Some(compressor) => decompress_data(r.data, compressor)
                    .await
                    .with_context(|| format!("Failed to decompress batch blob `{digest}`"))?,
                None if r.compressor == compressor::Value::Identity as i32 => r.data,
                None => {
                    return Err(anyhow::anyhow!(
                        "Unsupported BatchReadBlobs response compressor `{}` for `{}`",
                        r.compressor,
                        digest
                    ));
                }
            };
            batched_blobs_response.insert(digest, data);
        }
    }

    let download_hash_digest_function_for_hash = |hash: &str| {
        request_digest_function_config
            .for_hash(hash)
            .or(download_hash_digest_function)
    };

    let get = |digest: &TDigest| -> anyhow::Result<Vec<u8>> {
        if digest.size_in_bytes == 0 {
            validate_downloaded_blob(
                digest,
                &[],
                download_hash_digest_function_for_hash(&digest.hash),
            )?;
            return Ok(Vec::new());
        }

        let data = batched_blobs_response
            .get(digest)
            .with_context(|| format!("Did not receive digest data for `{digest}`"))?
            .clone();
        validate_downloaded_blob(
            digest,
            &data,
            download_hash_digest_function_for_hash(&digest.hash),
        )?;
        Ok(data)
    };

    let mut inlined_blobs = vec![];
    for digest in inlined_digests {
        let data = if digest.size_in_bytes as usize > max_total_batch_size {
            match active_downloads.enter(digest.clone()) {
                ActiveTransfer::Follower(state) => {
                    let result = ActiveTransferRegistry::wait(state).await?;
                    let data = active_download_result_to_bytes(&result).await?;
                    validate_downloaded_blob(
                        &digest,
                        &data,
                        download_hash_digest_function_for_hash(&digest.hash),
                    )?;
                    data
                }
                ActiveTransfer::Leader(leader) => {
                    let result = async {
                        let mut accum = vec![];
                        let restart_from_zero = compression_for_blob(
                            bystream_compressor,
                            digest.size_in_bytes,
                            remote_cache_compression_threshold,
                        )
                        .is_some();
                        let mut retry_attempt = 0usize;
                        let mut next_delay = Duration::from_millis(GRPC_RETRY_INITIAL_DELAY_MILLIS);
                        loop {
                            if restart_from_zero {
                                accum.clear();
                            }
                            let read_offset = if restart_from_zero {
                                0
                            } else {
                                i64::try_from(accum.len()).with_context(|| {
                                    format!("Downloaded blob is too large to resume on this platform: {digest}")
                                })?
                            };

                            let read_result: anyhow::Result<()> = async {
                                let mut reader = bystream_reader(digest.clone(), read_offset).await?;
                                let mut buffer = vec![0u8; 64 * 1024];
                                loop {
                                    let read_bytes = read_with_progress_timeout(
                                        &mut reader,
                                        &mut buffer,
                                        bystream_progress_timeout,
                                    )
                                    .await
                                    .with_context(|| format!("Error reading chunk of: {digest}"))?;
                                    if read_bytes == 0 {
                                        break;
                                    }
                                    accum.extend_from_slice(&buffer[..read_bytes]);
                                }
                                anyhow::Ok(())
                            }
                            .await;

                            match read_result {
                                Ok(()) => {
                                    validate_downloaded_blob(
                                        &digest,
                                        &accum,
                                        download_hash_digest_function_for_hash(&digest.hash),
                                    )?;
                                    break anyhow::Ok(accum);
                                }
                                Err(err)
                                    if retry_attempt < retries && is_retryable_grpc_error(&err) =>
                                {
                                    if is_broken_connection_error(&err) {
                                        bystream_retry_hook().await;
                                    }
                                    let delay = grpc_error_retry_delay(&err)
                                        .map(|delay| std::cmp::min(delay, retry_max_delay))
                                        .unwrap_or_else(|| jittered_retry_delay(next_delay));
                                    tracing::debug!(
                                        digest = %digest,
                                        retry_attempt = retry_attempt + 1,
                                        retries,
                                        read_offset,
                                        delay_ms = delay.as_millis(),
                                        "Retrying ByteStream read"
                                    );
                                    tokio::time::sleep(delay).await;
                                    retry_attempt += 1;
                                    next_delay =
                                        std::cmp::min(next_delay.saturating_mul(2), retry_max_delay);
                                }
                                Err(err) => {
                                    return Err(normalize_grpc_error(err)
                                        .context(format!("Error downloading digest `{digest}`")));
                                }
                            }
                        }
                    }
                    .await;
                    let result = leader.finish(result.map(ActiveDownloadResult::Bytes))?;
                    active_download_result_into_bytes(result).await?
                }
            }
        } else {
            get(&digest)?
        };
        inlined_blobs.push(InlinedDigestWithStatus {
            digest,
            status: tstatus_ok(),
            blob: data,
        })
    }

    let writes = file_digests.iter().map(|req| async {
        let fut = async {
            // If the data is small enough to be transferred in a batch
            // blob update, write it all at once to the file. Otherwise, it'll
            // be streamed in chunks as the remote responds.
            if req.named_digest.digest.size_in_bytes <= max_total_batch_size as i64 {
                let data = get(&req.named_digest.digest)?;
                write_active_download_result_to_file(
                    &ActiveDownloadResult::Bytes(data),
                    &req.named_digest.name,
                    req.is_executable,
                )
                .await?;
                return anyhow::Ok(());
            }
            match active_downloads.enter(req.named_digest.digest.clone()) {
                ActiveTransfer::Follower(state) => {
                    let result = ActiveTransferRegistry::wait(state).await?;
                    write_active_download_result_to_file(
                        &result,
                        &req.named_digest.name,
                        req.is_executable,
                    )
                    .await?;
                }
                ActiveTransfer::Leader(leader) => {
                    let result = async {
                        let mut file = download_output_options(req.is_executable)
                            .open(&req.named_digest.name)
                            .await
                            .context("Error opening")?;
                        let restart_from_zero = compression_for_blob(
                            bystream_compressor,
                            req.named_digest.digest.size_in_bytes,
                            remote_cache_compression_threshold,
                        )
                        .is_some();
                        let mut hash_validators = BlobHashValidators::new(
                            &req.named_digest.digest.hash,
                            download_hash_digest_function_for_hash(&req.named_digest.digest.hash),
                        )?;
                        let mut copied_bytes = 0usize;
                        let mut retry_attempt = 0usize;
                        let mut next_delay = Duration::from_millis(GRPC_RETRY_INITIAL_DELAY_MILLIS);
                        loop {
                            if restart_from_zero {
                                copied_bytes = 0;
                                hash_validators = BlobHashValidators::new(
                                    &req.named_digest.digest.hash,
                                    download_hash_digest_function_for_hash(
                                        &req.named_digest.digest.hash,
                                    ),
                                )?;
                                file.set_len(0).await.with_context(|| {
                                    format!("Error truncating: {}", req.named_digest.digest)
                                })?;
                                file.seek(SeekFrom::Start(0)).await.with_context(|| {
                                    format!("Error seeking: {}", req.named_digest.digest)
                                })?;
                            }

                            let read_offset = if restart_from_zero {
                                0
                            } else {
                                i64::try_from(copied_bytes).with_context(|| {
                                    format!(
                                        "Downloaded blob is too large to resume on this platform: {}",
                                        req.named_digest.digest
                                    )
                                })?
                            };

                            let read_result: anyhow::Result<()> = async {
                                let mut reader =
                                    bystream_reader(req.named_digest.digest.clone(), read_offset)
                                        .await?;
                                let mut buffer = vec![0u8; 64 * 1024];
                                loop {
                                    let read_bytes = read_with_progress_timeout(
                                        &mut reader,
                                        &mut buffer,
                                        bystream_progress_timeout,
                                    )
                                    .await
                                    .with_context(|| {
                                        format!("Error reading chunk of: {}", req.named_digest.digest)
                                    })?;
                                    if read_bytes == 0 {
                                        break;
                                    }
                                    copied_bytes = copied_bytes.checked_add(read_bytes).with_context(
                                        || {
                                            format!(
                                                "Downloaded blob is too large to validate on this platform: {}",
                                                req.named_digest.digest
                                            )
                                        },
                                    )?;
                                    hash_validators.update(&buffer[..read_bytes]);
                                    file.write_all(&buffer[..read_bytes]).await.with_context(
                                        || {
                                            format!(
                                                "Error writing chunk of: {}",
                                                req.named_digest.digest
                                            )
                                        },
                                    )?;
                                }
                                anyhow::Ok(())
                            }
                            .await;

                            match read_result {
                                Ok(()) => break,
                                Err(err)
                                    if retry_attempt < retries && is_retryable_grpc_error(&err) =>
                                {
                                    if is_broken_connection_error(&err) {
                                        bystream_retry_hook().await;
                                    }
                                    let delay = grpc_error_retry_delay(&err)
                                        .map(|delay| std::cmp::min(delay, retry_max_delay))
                                        .unwrap_or_else(|| jittered_retry_delay(next_delay));
                                    tracing::debug!(
                                        digest = %req.named_digest.digest,
                                        retry_attempt = retry_attempt + 1,
                                        retries,
                                        read_offset,
                                        delay_ms = delay.as_millis(),
                                        "Retrying ByteStream file download"
                                    );
                                    tokio::time::sleep(delay).await;
                                    retry_attempt += 1;
                                    next_delay =
                                        std::cmp::min(next_delay.saturating_mul(2), retry_max_delay);
                                }
                                Err(err) => {
                                    return Err(normalize_grpc_error(err));
                                }
                            }
                        }
                        validate_downloaded_blob_size(&req.named_digest.digest, copied_bytes)?;
                        hash_validators.finish(&req.named_digest.digest)?;
                        file.flush().await.context("Error flushing")?;
                        anyhow::Ok(ActiveDownloadResult::File(PathBuf::from(
                            req.named_digest.name.clone(),
                        )))
                    }
                    .await;
                    leader.finish(result)?;
                }
            }
            anyhow::Ok(())
        };
        fut.await.with_context(|| {
            format!(
                "Error downloading digest `{}` to `{}`",
                req.named_digest.digest, req.named_digest.name,
            )
        })
    });

    buck2_util::future::try_join_all(writes).await?;

    Ok(DownloadResponse {
        inlined_blobs: Some(inlined_blobs),
        directories: None,
        local_cache_stats: counters.take_stats(),
    })
}

/// Clones every file the daemon's directory can serve and returns `(hits, misses)`. With
/// `counters`, records each file as a hit or a miss.
///
/// A directory that fails to serve a blob is treated as a miss: the blob is then received over
/// gRPC, which surfaces any persistent problem with the directory without failing the build.
async fn materialize_from_shared_cache(
    cache: &SharedCasCache,
    files: Vec<NamedDigestWithPermissions>,
    counters: Option<&SharedCacheCounters>,
) -> (
    Vec<NamedDigestWithPermissions>,
    Vec<NamedDigestWithPermissions>,
) {
    let outcomes = futures::future::join_all(files.into_iter().map(|req| async move {
        let digest = &req.named_digest.digest;
        if digest.size_in_bytes == 0 {
            return (req, false);
        }
        let hit = match cache
            .materialize(
                digest,
                std::path::Path::new(&req.named_digest.name),
                req.is_executable,
            )
            .await
        {
            Ok(hit) => hit,
            Err(e) => {
                tracing::warn!(
                    "Shared CAS cache failed to clone `{}` to `{}`, downloading instead: {:#}",
                    digest,
                    req.named_digest.name,
                    e
                );
                false
            }
        };
        (req, hit)
    }))
    .await;

    let mut hits = Vec::new();
    let mut misses = Vec::new();
    for (req, hit) in outcomes {
        // Empty files are written directly and never involve the directory.
        if let Some(counters) = counters.filter(|_| req.named_digest.digest.size_in_bytes > 0) {
            if hit {
                counters.hit(req.named_digest.digest.size_in_bytes);
            } else {
                counters.miss(req.named_digest.digest.size_in_bytes);
            }
        }
        if hit {
            hits.push(req);
        } else {
            misses.push(req);
        }
    }
    (hits, misses)
}

/// Asks the daemon for the first byte of each distinct blob in `files`. Serving that read makes
/// the daemon fetch and store the whole blob, after which it can be cloned from its directory.
/// Failures are ignored here; the ordinary download that follows reports them properly.
async fn warm_shared_cache<Byt, BytRet>(
    resource_name: impl Fn(&TDigest) -> String,
    bystream_fut: impl Fn(ReadRequest) -> Byt + Copy,
    files: &[NamedDigestWithPermissions],
) where
    Byt: Future<Output = anyhow::Result<Pin<Box<BytRet>>>>,
    BytRet: Stream<Item = Result<ReadResponse, tonic::Status>> + Send,
{
    let mut seen = HashSet::new();
    let distinct: Vec<&TDigest> = files
        .iter()
        .map(|req| &req.named_digest.digest)
        .filter(|d| d.size_in_bytes > 0 && seen.insert((*d).clone()))
        .collect();
    futures::future::join_all(distinct.into_iter().map(|digest| {
        let resource_name = resource_name(digest);
        async move {
            let result = async {
                let mut stream = bystream_fut(ReadRequest {
                    resource_name,
                    read_offset: 0,
                    read_limit: 1,
                })
                .await?;
                while let Some(item) = stream.next().await {
                    item?;
                }
                anyhow::Ok(())
            }
            .await;
            if let Err(e) = result {
                tracing::debug!("Warming the shared CAS cache for `{digest}` failed: {e:#}");
            }
        }
    }))
    .await;
}

async fn send_batch_update_blobs<Cas>(
    mut request: BatchUpdateBlobsRequest,
    request_digest_function_config: DigestFunctionConfig,
    cas_f: &impl Fn(BatchUpdateBlobsRequest) -> Cas,
) -> anyhow::Result<Vec<String>>
where
    Cas: Future<Output = anyhow::Result<BatchUpdateBlobsResponse>>,
{
    if request.requests.is_empty() {
        return Ok(Vec::new());
    }

    let blob_hashes = request
        .requests
        .iter()
        .map(|x| x.digest.as_ref().unwrap().hash.clone())
        .collect::<Vec<String>>();
    let requested_digests = request
        .requests
        .iter()
        .map(|x| x.digest.as_ref().unwrap().clone())
        .collect::<Vec<_>>();
    let digest_function =
        request_digest_function_config.for_common_digest_function(&requested_digests);
    request.digest_function = digest_function_to_grpc(digest_function);

    let response = cas_f(request).await?;
    validate_batch_update_blobs_response(&requested_digests, &response)?;
    Ok(blob_hashes)
}

impl BatchUploadRequest {
    fn digest(&self) -> &TDigest {
        match self {
            BatchUploadRequest::Blob(blob) => &blob.digest,
            BatchUploadRequest::File(file) => &file.digest,
        }
    }

    fn duplicate(&self) -> Self {
        match self {
            BatchUploadRequest::Blob(blob) => BatchUploadRequest::Blob(InlinedBlobWithDigest {
                blob: blob.blob.clone(),
                digest: blob.digest.clone(),
                ..Default::default()
            }),
            BatchUploadRequest::File(file) => BatchUploadRequest::File(NamedDigest {
                name: file.name.clone(),
                digest: file.digest.clone(),
                ..Default::default()
            }),
        }
    }
}

/// Everything a BatchUpdateBlobs upload needs, owned, so that a call several actions share
/// outlives the action that started it.
#[derive(Clone)]
struct BatchUploader<F> {
    instance_name: Arc<str>,
    batch_update_compressor: Option<Compressor>,
    max_total_batch_size: usize,
    remote_cache_compression_threshold: usize,
    request_digest_function_config: DigestFunctionConfig,
    cas_f: F,
}

impl<F, Cas> BatchUploader<F>
where
    F: Fn(BatchUpdateBlobsRequest) -> Cas,
    Cas: Future<Output = anyhow::Result<BatchUpdateBlobsResponse>>,
{
    /// Sends `batch` in BatchUpdateBlobs requests of at most `max_total_batch_size` bytes each.
    async fn upload(self, batch: Vec<BatchUploadRequest>) -> anyhow::Result<()> {
        let request_digest_function_config = self.request_digest_function_config;
        let new_request = || BatchUpdateBlobsRequest {
            instance_name: self.instance_name.to_string(),
            requests: vec![],
            ..Default::default()
        };
        let mut re_request = new_request();
        let mut request_size = 0usize;
        for blob in batch {
            let (digest, data) = match blob {
                BatchUploadRequest::Blob(blob) => {
                    let digest = blob.digest;
                    let data = blob.blob;
                    validate_upload_blob(
                        &digest,
                        &data,
                        request_digest_function_config.for_hash(&digest.hash),
                    )?;
                    (digest, data)
                }
                BatchUploadRequest::File(file) => {
                    // These should be small files, so no need to use a buffered reader.
                    let mut fin = tokio::fs::File::open(&file.name)
                        .await
                        .with_context(|| format!("Opening {} for reading failed", file.name))?;
                    let mut data = vec![];
                    fin.read_to_end(&mut data).await?;
                    validate_upload_blob(
                        &file.digest,
                        &data,
                        request_digest_function_config.for_hash(&file.digest.hash),
                    )?;
                    (file.digest, data)
                }
            };
            let blob_compressor = compression_for_blob(
                self.batch_update_compressor,
                digest.size_in_bytes,
                self.remote_cache_compression_threshold,
            );
            let data = if let Some(compressor) = blob_compressor {
                compress_data(data, compressor).await.with_context(|| {
                    format!("Failed to compress BatchUpdateBlobs request for `{digest}`")
                })?
            } else {
                data
            };
            let additional_size = data.len();
            if !re_request.requests.is_empty()
                && request_size + additional_size > self.max_total_batch_size
            {
                send_batch_update_blobs(re_request, request_digest_function_config, &self.cas_f)
                    .await?;
                re_request = new_request();
                request_size = 0;
            }
            re_request.requests.push(Request {
                digest: Some(tdigest_to(digest)),
                data,
                compressor: blob_compressor
                    .map(|compressor| compressor.as_grpc())
                    .unwrap_or(compressor::Value::Identity as i32),
            });
            request_size += additional_size;
        }
        send_batch_update_blobs(re_request, request_digest_function_config, &self.cas_f).await?;
        Ok(())
    }
}

/// Waits for the upload another caller started of `upload`'s digest. A waiter never takes a
/// failed upload for a stored blob, as Bazel's RemoteExecutionCache invalidates a digest whose
/// upload failed (src/main/java/com/google/devtools/build/lib/remote/RemoteExecutionCache.java,
/// `maybeCreateUploadTask`). A failure that is the server's answer about the blob is this
/// caller's answer too. After any other, this caller joins the next upload of the digest, or
/// starts one itself, and its own upload's result is final, so the loop ends.
async fn follow_batch_upload<F, Cas>(
    shared_batch_uploads: &SharedCallRegistry<()>,
    batch_uploader: BatchUploader<F>,
    upload: BatchUploadRequest,
    mut call: SharedCall<()>,
) -> anyhow::Result<()>
where
    F: Fn(BatchUpdateBlobsRequest) -> Cas + Clone + Send + Sync + 'static,
    Cas: Future<Output = anyhow::Result<BatchUpdateBlobsResponse>> + Send + 'static,
{
    // A caller whose own copy is wrong or gone fails as it would have uploading it, as a
    // ByteStream follower does.
    match &upload {
        BatchUploadRequest::Blob(blob) => validate_upload_blob(
            &blob.digest,
            &blob.blob,
            batch_uploader
                .request_digest_function_config
                .for_hash(&blob.digest.hash),
        )?,
        BatchUploadRequest::File(file) => {
            tokio::fs::metadata(&file.name)
                .await
                .with_context(|| format!("Opening {} for reading failed", file.name))?;
        }
    }
    loop {
        match call.await {
            Ok(_) => return Ok(()),
            Err(failure) if failure.final_for_every_caller => return Err(failure.into_error()),
            Err(failure) => {
                tracing::debug!(
                    digest = %upload.digest(),
                    error = %failure.message,
                    "Uploading a blob again after the upload this caller waited on failed"
                );
            }
        }
        let (next, own) = {
            let mut calls = shared_batch_uploads.lock();
            match calls.get(upload.digest()) {
                Some(next) => (next, false),
                None => (
                    calls.start(
                        vec![upload.digest().clone()],
                        batch_uploader.clone().upload(vec![upload.duplicate()]),
                    ),
                    true,
                ),
            }
        };
        if own {
            return next
                .await
                .map(|_| ())
                .map_err(SharedCallFailure::into_error);
        }
        call = next;
    }
}

async fn upload_impl<Byt, Cas>(
    instance_name: &InstanceName,
    request: UploadRequest,
    bystream_compressor: Option<Compressor>,
    batch_update_compressor: Option<Compressor>,
    max_total_batch_size: usize,
    remote_cache_compression_threshold: usize,
    max_concurrent_uploads: Option<usize>,
    request_digest_function_config: DigestFunctionConfig,
    active_uploads: &ActiveTransferRegistry<()>,
    shared_batch_uploads: &SharedCallRegistry<()>,
    cas_f: impl Fn(BatchUpdateBlobsRequest) -> Cas + Clone + Sync + Send + 'static,
    bystream_fut: impl Fn(Vec<WriteRequest>) -> Byt + Sync + Send + Copy,
) -> anyhow::Result<UploadResponse>
where
    Cas: Future<Output = anyhow::Result<BatchUpdateBlobsResponse>> + Send + 'static,
    Byt: Future<Output = anyhow::Result<WriteResponse>> + Send,
{
    fn resource_name(
        instance_name: &InstanceName,
        client_uuid: &str,
        compressor: Option<Compressor>,
        digest: &TDigest,
        request_digest_function_config: DigestFunctionConfig,
    ) -> String {
        let digest_function_segment =
            digest_function_resource_segment(request_digest_function_config.for_hash(&digest.hash));
        if let Some(compressor) = compressor {
            if let Some(digest_function_segment) = digest_function_segment {
                format!(
                    "{}uploads/{}/compressed-blobs/{}/{}/{}/{}",
                    instance_name.as_resource_prefix(),
                    client_uuid,
                    compressor.name(),
                    digest_function_segment,
                    digest.hash,
                    digest.size_in_bytes,
                )
            } else {
                format!(
                    "{}uploads/{}/compressed-blobs/{}/{}/{}",
                    instance_name.as_resource_prefix(),
                    client_uuid,
                    compressor.name(),
                    digest.hash,
                    digest.size_in_bytes,
                )
            }
        } else if let Some(digest_function_segment) = digest_function_segment {
            format!(
                "{}uploads/{}/blobs/{}/{}/{}",
                instance_name.as_resource_prefix(),
                client_uuid,
                digest_function_segment,
                digest.hash,
                digest.size_in_bytes,
            )
        } else {
            format!(
                "{}uploads/{}/blobs/{}/{}",
                instance_name.as_resource_prefix(),
                client_uuid,
                digest.hash,
                digest.size_in_bytes,
            )
        }
    }

    // NOTE if we stop recording blob_hashes, we can drop out a lot of allocations.
    let mut upload_futures: Vec<BoxFuture<anyhow::Result<Vec<String>>>> = vec![];

    // For small file uploads the client should group them together and call `BatchUpdateBlobs`
    // https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto#L205
    let mut small_uploads = Vec::new();

    // Adapt the given bystream_fut to take in an AsyncBufRead
    let bystream_fut = |resource_name: String,
                        reader: Box<dyn AsyncBufRead + Unpin + Send>,
                        expected_digest: Option<TDigest>,
                        blob_compressor: Option<Compressor>| async move {
        let mut reader: Pin<Box<dyn AsyncRead + Unpin + Send>> = match blob_compressor {
            None => Pin::new(Box::new(reader)),
            Some(Compressor::Zstd) => Pin::new(Box::new(ZstdEncoder::new(reader))),
            Some(Compressor::Deflate) => Pin::new(Box::new(DeflateEncoder::new(reader))),
            Some(Compressor::Brotli) => Pin::new(Box::new(BrotliEncoder::new(reader))),
        };
        let mut hash_validators = expected_digest
            .as_ref()
            .filter(|digest| blob_compressor.is_none() && should_validate_upload_hash(digest))
            .map(|digest| {
                BlobHashValidators::new(
                    &digest.hash,
                    request_digest_function_config.for_hash(&digest.hash),
                )
            })
            .transpose()?;

        let mut current_offset = 0;
        let mut upload_segments = Vec::new();
        let mut buf = vec![0; max_total_batch_size];
        loop {
            let n_read = reader
                .read(&mut buf)
                .await
                .with_context(|| format!("Failed reading upload source for `{resource_name}`"))?;
            if n_read == 0 {
                break;
            }
            if let Some(hash_validators) = &mut hash_validators {
                hash_validators.update(&buf[0..n_read]);
            }
            upload_segments.push(WriteRequest {
                resource_name: resource_name.clone(),
                write_offset: current_offset,
                finish_write: false,
                data: buf[0..n_read].to_vec(),
            });
            current_offset += n_read as i64;
        }
        if blob_compressor.is_none() {
            if let Some(expected_digest) = &expected_digest {
                validate_downloaded_blob_size(expected_digest, current_offset as usize)?;
                if let Some(hash_validators) = hash_validators {
                    hash_validators.finish(expected_digest)?;
                }
            }
        }
        if let Some(last_segment) = upload_segments.last_mut() {
            last_segment.finish_write = true;
        }

        if upload_segments.is_empty() {
            // As an optimization, we can silently skip uploading empty blobs
            return Ok(());
        }

        let response = bystream_fut(upload_segments).await?;
        if response.committed_size != current_offset && response.committed_size != -1 {
            return Err(anyhow::anyhow!(
                "Failed to upload `{resource_name}`: invalid committed_size from WriteResponse"
            ));
        }

        Ok(())
    };

    // Create futures for any blobs that need uploading.
    for blob in request.inlined_blobs_with_digest.unwrap_or_default() {
        let hash = blob.digest.hash.clone();
        let size = blob.digest.size_in_bytes;

        if size <= max_total_batch_size as i64 {
            small_uploads.push(BatchUploadRequest::Blob(blob));
            continue;
        }

        let blob_compressor = compression_for_blob(
            bystream_compressor,
            size,
            remote_cache_compression_threshold,
        );
        let client_uuid = uuid::Uuid::new_v4().to_string();
        let resource_name = resource_name(
            instance_name,
            &client_uuid,
            blob_compressor,
            &blob.digest,
            request_digest_function_config,
        );
        let digest = blob.digest;
        let data = blob.blob;
        let active_upload = active_uploads.enter(digest.clone());
        let fut = async move {
            match active_upload {
                ActiveTransfer::Leader(leader) => {
                    let result = bystream_fut(
                        resource_name,
                        Box::new(Cursor::new(data)),
                        Some(digest),
                        blob_compressor,
                    )
                    .await;
                    leader.finish(result)?;
                }
                ActiveTransfer::Follower(state) => {
                    if blob_compressor.is_none() {
                        validate_upload_blob(
                            &digest,
                            &data,
                            request_digest_function_config.for_hash(&digest.hash),
                        )?;
                    }
                    ActiveTransferRegistry::wait(state).await?;
                }
            }

            Ok(vec![hash])
        };
        upload_futures.push(Box::pin(fut));
    }

    // Create futures for any files that needs uploading.
    for file in request.files_with_digest.unwrap_or_default() {
        let hash = file.digest.hash.clone();
        let size = file.digest.size_in_bytes;
        let name = file.name.clone();
        if size <= max_total_batch_size as i64 {
            small_uploads.push(BatchUploadRequest::File(file));
            continue;
        }
        let blob_compressor = compression_for_blob(
            bystream_compressor,
            size,
            remote_cache_compression_threshold,
        );
        let client_uuid = uuid::Uuid::new_v4().to_string();
        let resource_name = resource_name(
            instance_name,
            &client_uuid,
            blob_compressor,
            &file.digest,
            request_digest_function_config,
        );
        let active_upload = active_uploads.enter(file.digest.clone());

        let fut = async move {
            match active_upload {
                ActiveTransfer::Leader(leader) => {
                    let expected_digest = file.digest;
                    let result = async {
                        let file = tokio::fs::File::open(&name)
                            .await
                            .with_context(|| format!("Opening `{name}` for reading failed"))?;

                        bystream_fut(
                            resource_name,
                            Box::new(BufReader::new(file)),
                            Some(expected_digest),
                            blob_compressor,
                        )
                        .await
                    }
                    .await;
                    leader.finish(result)?;
                }
                ActiveTransfer::Follower(state) => {
                    tokio::fs::metadata(&name)
                        .await
                        .with_context(|| format!("Opening `{name}` for reading failed"))?;
                    ActiveTransferRegistry::wait(state).await?;
                }
            }
            Ok(vec![hash])
        };
        upload_futures.push(Box::pin(fut));
    }

    // Small blobs go out in BatchUpdateBlobs calls the whole client shares: a digest another
    // action is already uploading is waited for rather than sent again. Actions that start
    // together, such as the links of many tests of one library, need the same directory blobs
    // and each heard from FindMissingBlobs that they were missing.
    let batch_uploader = BatchUploader {
        instance_name: Arc::from(instance_name.as_str()),
        batch_update_compressor,
        max_total_batch_size,
        remote_cache_compression_threshold,
        request_digest_function_config,
        cas_f,
    };
    let (started, joined) = {
        let mut calls = shared_batch_uploads.lock();
        let mut batches = BatchUploadReqAggregator::new(max_total_batch_size);
        let mut claimed = HashSet::new();
        let mut joined = Vec::new();
        for upload in small_uploads {
            let digest = upload.digest();
            // An empty blob is never sent, and a digest twice in this request goes out once.
            if digest.size_in_bytes == 0 || claimed.contains(digest) {
                continue;
            }
            match calls.get(digest) {
                Some(call) => joined.push((upload, call)),
                None => {
                    claimed.insert(digest.clone());
                    batches.push(upload);
                }
            }
        }
        let started = batches
            .done()
            .into_iter()
            .map(|batch| {
                let digests = batch
                    .iter()
                    .map(|upload| upload.digest().clone())
                    .collect::<Vec<_>>();
                let hashes = digests.iter().map(|digest| digest.hash.clone()).collect();
                let call = calls.start(digests, batch_uploader.clone().upload(batch));
                (hashes, call)
            })
            .collect::<Vec<_>>();
        (started, joined)
    };
    for (hashes, call) in started {
        upload_futures.push(Box::pin(async move {
            call.await.map_err(SharedCallFailure::into_error)?;
            Ok(hashes)
        }));
    }
    for (upload, call) in joined {
        let batch_uploader = batch_uploader.clone();
        upload_futures.push(Box::pin(async move {
            let hash = upload.digest().hash.clone();
            follow_batch_upload(shared_batch_uploads, batch_uploader, upload, call).await?;
            Ok(vec![hash])
        }));
    }

    let blob_hashes = if let Some(concurrency_limit) = max_concurrent_uploads {
        futures::stream::iter(upload_futures)
            .buffer_unordered(concurrency_limit)
            .try_collect::<Vec<Vec<String>>>()
            .await?
    } else {
        futures::future::try_join_all(upload_futures).await?
    };

    tracing::debug!("uploaded: {:?}", blob_hashes);
    Ok(UploadResponse {})
}

fn with_re_metadata<T>(
    t: T,
    metadata: &RemoteExecutionMetadata,
    use_fbcode_metadata: bool,
    request_metadata_tool_name: &str,
) -> tonic::Request<T> {
    // This creates a new Tonic request with attached metadata for the RE
    // backend. There are two cases here we need to support:
    //
    //   - Servers that abide by the remote execution apis defined with Bazel,
    //     AKA the "OSS RE API", which this package implements
    //   - The internal RE solution used at Meta, which uses a different API,
    //     but is compatible with the OSS RE API to some extent.
    //
    // The second case is supported only through attaching some metadata to the
    // request, which the fbcode RE service understands; and the reason for all
    // of this is that it allows this OSS client package to be tested inside of
    // fbcode builds within Meta. So there doesn't need to be a separate CI
    // check.
    //
    // However, we don't need it for FOSS builds of Buck2. And in theory we
    // could test the OSS Bazel API in the upstream GitHub CI, but doing it this
    // way is only a little ugly, it's hidden, and it helps ensure the internal
    // Meta builds catch those issues earlier.

    let mut msg = tonic::Request::new(t);

    if use_fbcode_metadata {
        // This is pretty ugly, but the protobuf spec that defines this is
        // internal, so considering field numbers need to be stable anyway (=
        // low risk), and this is not used in prod (= low impact if this goes
        // wrong), we just inline it here. This is a small hack that lets us use
        // our internal RE using this GRPC client for testing.
        //
        // This is defined in `fbcode/remote_execution/re_cas_common/grpc/proto/metadata.proto`.
        #[derive(prost::Message)]
        struct Metadata {
            #[prost(message, optional, tag = "15")]
            platform: Option<crate::grpc::Platform>,
            #[prost(string, optional, tag = "18")]
            use_case_id: Option<String>,
        }

        let mut encoded = Vec::new();
        Metadata {
            platform: metadata.platform.clone(),
            use_case_id: Some(metadata.use_case_id.clone()),
        }
        .encode(&mut encoded)
        .expect("Encoding into a Vec cannot not fail");

        msg.metadata_mut()
            .insert_bin("re-metadata-bin", MetadataValue::from_bytes(&encoded));
    } else {
        let mut encoded = Vec::new();
        let RemoteExecutionMetadata {
            correlated_invocations_id,
            buck_info,
            action_id,
            action_mnemonic,
            target_id,
            configuration_id,
            ..
        } = metadata.clone();

        let correlated_invocations_id = correlated_invocations_id
            .filter(|id| !id.is_empty())
            .or_else(|| {
                buck_info
                    .as_ref()
                    .map(|b| b.build_id.clone())
                    .filter(|id| !id.is_empty())
            })
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let (tool_invocation_id, tool_version) = match buck_info {
            Some(buck_info) => {
                let tool_version = if buck_info.version.is_empty() {
                    "dev".to_owned()
                } else {
                    buck_info.version
                };
                (buck_info.build_id, tool_version)
            }
            None => (String::new(), "dev".to_owned()),
        };

        RequestMetadata {
            tool_details: Some(ToolDetails {
                tool_name: request_metadata_tool_name.to_owned(),
                tool_version,
            }),
            action_id: action_id.unwrap_or_default(),
            tool_invocation_id,
            correlated_invocations_id,
            action_mnemonic: action_mnemonic.unwrap_or_default(),
            target_id: target_id.unwrap_or_default(),
            configuration_id: configuration_id.unwrap_or_default(),
        }
        .encode(&mut encoded)
        .expect("Encoding into a Vec cannot not fail");

        msg.metadata_mut()
            .insert_bin(REQUEST_METADATA_HEADER, MetadataValue::from_bytes(&encoded));
    };
    msg
}

fn with_re_metadata_timeout<T>(
    t: T,
    metadata: RemoteExecutionMetadata,
    use_fbcode_metadata: bool,
    request_metadata_tool_name: &str,
    timeout: Duration,
) -> tonic::Request<T> {
    let mut request = with_re_metadata(
        t,
        &metadata,
        use_fbcode_metadata,
        request_metadata_tool_name,
    );
    request.set_timeout(timeout);
    request
}

/// Replace occurrences of $FOO in a string with the value of the env var $FOO.
pub(crate) fn substitute_env_vars(s: &str) -> anyhow::Result<String> {
    substitute_env_vars_impl(s, |v| std::env::var(v))
}

fn substitute_env_vars_impl(
    s: &str,
    getter: impl Fn(&str) -> Result<String, VarError>,
) -> anyhow::Result<String> {
    static ENV_REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new("\\$[a-zA-Z_][a-zA-Z_0-9]*").unwrap());

    let mut out = String::with_capacity(s.len());
    let mut last_idx = 0;

    for mat in ENV_REGEX.find_iter(s) {
        out.push_str(&s[last_idx..mat.start()]);
        let var = &mat.as_str()[1..];
        let val = getter(var).with_context(|| format!("Error substituting `{}`", mat.as_str()))?;
        out.push_str(&val);
        last_idx = mat.end();
    }

    if last_idx < s.len() {
        out.push_str(&s[last_idx..s.len()]);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use core::sync::atomic::Ordering;
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicU16;

    use re_grpc_proto::build::bazel::remote::execution::v2::ActionCacheUpdateCapabilities;
    use re_grpc_proto::build::bazel::remote::execution::v2::FastCdc2020Params;
    use re_grpc_proto::build::bazel::remote::execution::v2::action_cache_server::ActionCache;
    use re_grpc_proto::build::bazel::remote::execution::v2::action_cache_server::ActionCacheServer;
    use re_grpc_proto::build::bazel::remote::execution::v2::batch_read_blobs_response;
    use re_grpc_proto::build::bazel::remote::execution::v2::batch_update_blobs_response;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;

    async fn test_download_impl<Byt, BytRet, Cas>(
        instance_name: &InstanceName,
        request: DownloadRequest,
        bystream_compressor: Option<Compressor>,
        max_total_batch_size: usize,
        download_hash_digest_function: Option<digest_function::Value>,
        request_digest_function_config: DigestFunctionConfig,
        cas_f: impl Fn(BatchReadBlobsRequest) -> Cas,
        bystream_fut: impl Fn(ReadRequest) -> Byt + Sync + Send + Copy,
    ) -> anyhow::Result<DownloadResponse>
    where
        Byt: std::future::Future<Output = anyhow::Result<Pin<Box<BytRet>>>>,
        BytRet: futures::Stream<Item = Result<ReadResponse, tonic::Status>> + Send + 'static,
        Cas: std::future::Future<Output = anyhow::Result<BatchReadBlobsResponse>>,
    {
        test_download_impl_with_shared_cache(
            instance_name,
            request,
            bystream_compressor,
            max_total_batch_size,
            download_hash_digest_function,
            request_digest_function_config,
            None,
            cas_f,
            bystream_fut,
        )
        .await
    }

    async fn test_download_impl_with_shared_cache<Byt, BytRet, Cas>(
        instance_name: &InstanceName,
        request: DownloadRequest,
        bystream_compressor: Option<Compressor>,
        max_total_batch_size: usize,
        download_hash_digest_function: Option<digest_function::Value>,
        request_digest_function_config: DigestFunctionConfig,
        shared_cache: Option<&SharedCasCache>,
        cas_f: impl Fn(BatchReadBlobsRequest) -> Cas,
        bystream_fut: impl Fn(ReadRequest) -> Byt + Sync + Send + Copy,
    ) -> anyhow::Result<DownloadResponse>
    where
        Byt: std::future::Future<Output = anyhow::Result<Pin<Box<BytRet>>>>,
        BytRet: futures::Stream<Item = Result<ReadResponse, tonic::Status>> + Send + 'static,
        Cas: std::future::Future<Output = anyhow::Result<BatchReadBlobsResponse>>,
    {
        let active_downloads = ActiveTransferRegistry::new();
        download_impl(
            instance_name,
            request,
            bystream_compressor,
            max_total_batch_size,
            DEFAULT_REMOTE_CACHE_COMPRESSION_THRESHOLD,
            download_hash_digest_function,
            request_digest_function_config,
            0,
            Duration::from_millis(1),
            Duration::from_secs(DEFAULT_BYTESTREAM_PROGRESS_TIMEOUT_SECS),
            &active_downloads,
            shared_cache,
            cas_f,
            bystream_fut,
            || async {},
        )
        .await
    }

    async fn upload_impl<Byt, Cas>(
        instance_name: &InstanceName,
        request: UploadRequest,
        bystream_compressor: Option<Compressor>,
        max_total_batch_size: usize,
        max_concurrent_uploads: Option<usize>,
        request_digest_function_config: DigestFunctionConfig,
        cas_f: impl Fn(BatchUpdateBlobsRequest) -> Cas + Clone + Sync + Send + 'static,
        bystream_fut: impl Fn(Vec<WriteRequest>) -> Byt + Sync + Send + Copy,
    ) -> anyhow::Result<UploadResponse>
    where
        Cas: Future<Output = anyhow::Result<BatchUpdateBlobsResponse>> + Send + 'static,
        Byt: Future<Output = anyhow::Result<WriteResponse>> + Send,
    {
        let active_uploads = ActiveTransferRegistry::new();
        let shared_batch_uploads = SharedCallRegistry::new();
        super::upload_impl(
            instance_name,
            request,
            bystream_compressor,
            None,
            max_total_batch_size,
            DEFAULT_REMOTE_CACHE_COMPRESSION_THRESHOLD,
            max_concurrent_uploads,
            request_digest_function_config,
            &active_uploads,
            &shared_batch_uploads,
            cas_f,
            bystream_fut,
        )
        .await
    }

    #[test]
    fn wait_execution_not_found_retries_execute() {
        let err = anyhow::Error::from(tonic::Status::not_found("operation was lost"));

        assert!(should_retry_execute_after_wait_execution_error(&err));
    }

    #[test]
    fn operation_stream_not_found_retries_execute() {
        let err = anyhow::Error::from(tonic::Status::not_found("operation was lost"));

        assert!(should_retry_execute_after_operation_stream_error(&err));
    }

    #[test]
    fn operation_stream_transient_error_does_not_restart_execute_directly() {
        let err = anyhow::Error::from(tonic::Status::unavailable("try wait again"));

        assert!(!should_retry_execute_after_operation_stream_error(&err));
    }

    #[test]
    fn wait_execution_transient_error_does_not_restart_execute_directly() {
        let err = anyhow::Error::from(tonic::Status::unavailable("try wait again"));

        assert!(!should_retry_execute_after_wait_execution_error(&err));
    }

    /// What tonic makes of the error hyper gives a request whose connection closed before it was
    /// sent.
    async fn canceled_connection_status() -> tonic::Status {
        let (io, _server) = tokio::io::duplex(1 << 16);
        let (mut send_request, connection) = hyper::client::conn::http2::handshake(
            hyper_util::rt::TokioExecutor::new(),
            hyper_util::rt::TokioIo::new(io),
        )
        .await
        .unwrap();
        let response = send_request.send_request(http::Request::new(http_body_util::Empty::<
            bytes::Bytes,
        >::new()));
        drop(connection);
        let error = response.await.expect_err("the connection is gone");
        tonic::Status::from_error(Box::new(error))
    }

    #[tokio::test]
    async fn a_request_cancelled_by_its_connection_is_retried_after_a_reconnect() {
        let status = canceled_connection_status().await;
        assert_eq!(status.code(), tonic::Code::Cancelled, "{status:?}");

        let err = anyhow::Error::from(status.clone());
        assert!(is_retryable_grpc_error(&err), "{err:#}");
        assert_eq!(recovery_for_error(&err), Recovery::Reconnect);
        assert!(should_retry_execute_after_wait_execution_error(&err));
        let err = normalize_grpc_error(err);
        assert!(is_retryable_grpc_error(&err), "{err:#}");
        assert_eq!(recovery_for_error(&err), Recovery::Reconnect);
        assert!(should_retry_execute_after_wait_execution_error(&err));

        let attempts = AtomicU16::new(0);
        let recoveries = Mutex::new(Vec::new());
        retry_grpc_request_with_recovery(
            1,
            Duration::from_millis(1),
            || async {
                if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                    Err(anyhow::Error::from(status.clone()))
                } else {
                    Ok(())
                }
            },
            |recovery| {
                recoveries.lock().unwrap().push(recovery);
                async { false }
            },
        )
        .await
        .unwrap();
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
        assert_eq!(*recoveries.lock().unwrap(), [Recovery::Reconnect]);
    }

    /// Whatever its message says: Go servers that lose an executor write "connection closed".
    #[test]
    fn a_cancelled_from_the_server_is_final() {
        for message in [
            "operation was canceled",
            "connection closed by executor",
            "connection reset",
            "transport error",
        ] {
            for err in [
                anyhow::Error::from(tonic::Status::cancelled(message)),
                normalize_grpc_error(anyhow::Error::from(tonic::Status::cancelled(message))),
            ] {
                assert!(!is_retryable_grpc_error(&err), "{err:#}");
                assert!(!is_broken_connection_error(&err), "{err:#}");
                assert_eq!(recovery_for_error(&err), Recovery::None, "{err:#}");
                assert!(
                    !should_retry_execute_after_wait_execution_error(&err),
                    "{err:#}"
                );
            }
        }
    }

    #[test]
    fn a_wait_execution_answer_from_the_server_does_not_restart_execute() {
        for status in [
            tonic::Status::aborted("executor was lost"),
            tonic::Status::internal("server failed"),
            tonic::Status::invalid_argument("bad operation name"),
            tonic::Status::failed_precondition("missing input"),
            tonic::Status::permission_denied("not allowed"),
            tonic::Status::resource_exhausted("quota"),
            tonic::Status::deadline_exceeded("too slow"),
        ] {
            let err = anyhow::Error::from(status);
            assert!(
                !should_retry_execute_after_wait_execution_error(&err),
                "{err:#}"
            );
        }
        let err = anyhow::Error::from(tonic::Status::unknown("transport error"));
        assert!(should_retry_execute_after_wait_execution_error(&err));
    }

    #[test]
    fn configured_connection_count_uses_bazel_ratio_by_default() {
        assert_eq!(configured_connection_count(None, Some(2000)), 20);
        assert_eq!(configured_connection_count(None, Some(1)), 1);
        assert_eq!(configured_connection_count(None, Some(10_001)), 100);
        assert_eq!(configured_connection_count(None, None), 4);
    }

    #[test]
    fn configured_connection_count_honors_override() {
        assert_eq!(configured_connection_count(Some(0), Some(2000)), 1);
        assert_eq!(configured_connection_count(Some(16), Some(2000)), 16);
    }

    #[test]
    fn prepare_uri_lets_the_tls_key_override_the_scheme() -> anyhow::Result<()> {
        let (uri, tls) = prepare_uri("grpc://reapi.example:444".parse()?, Some(true))?;
        assert_eq!(uri.scheme_str(), Some("https"));
        assert!(tls);

        let (uri, tls) = prepare_uri("grpcs://reapi.example:444".parse()?, Some(false))?;
        assert_eq!(uri.scheme_str(), Some("http"));
        assert!(!tls);
        Ok(())
    }

    #[test]
    fn prepare_uri_adds_root_path_to_bare_authority() -> anyhow::Result<()> {
        let (uri, tls) = prepare_uri("remote.buildbuddy.io".parse()?, None)?;
        assert_eq!(uri.to_string(), "https://remote.buildbuddy.io/");
        assert!(tls);

        let (uri, tls) = prepare_uri("grpc://localhost:8980".parse()?, None)?;
        assert_eq!(uri.to_string(), "http://localhost:8980/");
        assert!(!tls);

        let (uri, tls) = prepare_uri("grpcs://remote.buildbuddy.io/cache".parse()?, None)?;
        assert_eq!(uri.to_string(), "https://remote.buildbuddy.io/cache");
        assert!(tls);

        Ok(())
    }

    fn decode_request_metadata<T>(request: &tonic::Request<T>) -> RequestMetadata {
        let metadata = request
            .metadata()
            .get_bin(REQUEST_METADATA_HEADER)
            .expect("request metadata header should be present");
        RequestMetadata::decode(metadata.to_bytes().unwrap()).unwrap()
    }

    #[test]
    fn request_metadata_tool_name_from_options_defaults_to_buck2() -> anyhow::Result<()> {
        let opts = Buck2OssReConfiguration::default();

        assert_eq!(
            request_metadata_tool_name_from_options(&opts)?,
            DEFAULT_REQUEST_METADATA_TOOL_NAME
        );
        Ok(())
    }

    #[test]
    fn request_metadata_tool_name_from_options_uses_configured_value() -> anyhow::Result<()> {
        let opts = Buck2OssReConfiguration {
            request_metadata_tool_name: Some("bazel".to_owned()),
            ..Default::default()
        };

        assert_eq!(request_metadata_tool_name_from_options(&opts)?, "bazel");
        Ok(())
    }

    #[test]
    fn request_metadata_tool_name_from_options_rejects_empty_value() {
        let opts = Buck2OssReConfiguration {
            request_metadata_tool_name: Some(String::new()),
            ..Default::default()
        };

        let err = request_metadata_tool_name_from_options(&opts)
            .unwrap_err()
            .to_string();
        assert!(err.contains("request_metadata_tool_name"));
    }

    #[test]
    fn with_re_metadata_sets_request_metadata_tool_name() {
        let request = with_re_metadata(
            (),
            &RemoteExecutionMetadata {
                buck_info: Some(BuckInfo {
                    build_id: "build-id".to_owned(),
                    version: "version".to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            false,
            "bazel",
        );

        let request_metadata = decode_request_metadata(&request);
        let tool_details = request_metadata
            .tool_details
            .expect("tool details should be set");
        assert_eq!(tool_details.tool_name, "bazel");
        assert_eq!(tool_details.tool_version, "version");
    }

    fn status_for_code(code: TCode) -> Status {
        Status {
            code: code.0,
            ..Default::default()
        }
    }

    fn retry_info_rpc_status(code: TCode, retry_delay: Duration) -> Status {
        let mut retry_info = Vec::new();
        RetryInfoDetail {
            retry_delay: Some(prost_types::Duration {
                seconds: retry_delay.as_secs() as i64,
                nanos: retry_delay.subsec_nanos() as i32,
            }),
        }
        .encode(&mut retry_info)
        .unwrap();

        Status {
            code: code.0,
            message: "retry later".to_owned(),
            details: vec![prost_types::Any {
                type_url: RETRY_INFO_TYPE_URL.to_owned(),
                value: retry_info,
            }],
        }
    }

    fn retry_info_tonic_status(code: tonic::Code, retry_delay: Duration) -> tonic::Status {
        let mut details = Vec::new();
        retry_info_rpc_status(tcode_from_grpc_code(code), retry_delay)
            .encode(&mut details)
            .unwrap();

        tonic::Status::with_details(code, "retry later", details.into())
    }

    #[test]
    fn operation_error_retry_policy_matches_grpc_retry_codes() {
        assert!(should_retry_execute_after_operation_error(
            &status_for_code(TCode::UNAVAILABLE)
        ));
        assert!(should_retry_execute_after_operation_error(
            &status_for_code(TCode::DEADLINE_EXCEEDED)
        ));
        assert!(!should_retry_execute_after_operation_error(
            &status_for_code(TCode::PERMISSION_DENIED)
        ));
        assert!(!should_retry_execute_after_operation_error(
            &status_for_code(TCode::CANCELLED)
        ));
    }

    #[test]
    fn execute_retry_delay_from_retry_info_is_capped() {
        let status = retry_info_rpc_status(TCode::RESOURCE_EXHAUSTED, Duration::from_secs(5));

        assert_eq!(
            capped_retry_delay_from_rpc_status(&status, Duration::from_millis(250)),
            Some(Duration::from_millis(250))
        );
    }

    #[test]
    fn execute_response_status_retry_policy_matches_bazel() {
        assert!(should_retry_execute_after_execute_response_status(
            &status_for_code(TCode::UNAVAILABLE)
        ));
        assert!(!should_retry_execute_after_execute_response_status(
            &status_for_code(TCode::DEADLINE_EXCEEDED)
        ));
        assert!(!should_retry_execute_after_execute_response_status(
            &status_for_code(TCode::PERMISSION_DENIED)
        ));
        assert!(!should_retry_execute_after_execute_response_status(
            &status_for_code(TCode::CANCELLED)
        ));
    }

    #[tokio::test]
    async fn retry_grpc_request_retries_retry_info_status() {
        let attempts = AtomicU16::new(0);
        retry_grpc_request(1, Duration::from_millis(1), || async {
            if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                Err(anyhow::Error::from(retry_info_tonic_status(
                    tonic::Code::FailedPrecondition,
                    Duration::from_millis(1),
                )))
            } else {
                Ok(())
            }
        })
        .await
        .unwrap();

        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn execute_retry_attempts_are_bounded() {
        assert!(!can_retry_execute(0, 0));
        assert!(can_retry_execute(0, 1));
        assert!(!can_retry_execute(1, 1));
    }

    #[test]
    fn test_select_download_hash_digest_function() -> anyhow::Result<()> {
        assert_eq!(
            select_download_hash_digest_function(
                &["BLAKE3".to_owned()],
                &[
                    digest_function::Value::Sha256,
                    digest_function::Value::Blake3
                ],
            )?,
            Some(digest_function::Value::Blake3)
        );
        assert_eq!(
            select_download_hash_digest_function(
                &[],
                &[
                    digest_function::Value::Sha256,
                    digest_function::Value::Blake3
                ],
            )?,
            None
        );
        assert_eq!(
            select_download_hash_digest_function(&[], &[digest_function::Value::Sha256],)?,
            Some(digest_function::Value::Sha256)
        );
        assert!(
            select_download_hash_digest_function(
                &["SHA256".to_owned()],
                &[digest_function::Value::Blake3],
            )
            .is_err()
        );
        assert_eq!(
            select_download_hash_digest_function(
                &["BLAKE3".to_owned(), "SHA256".to_owned()],
                &[
                    digest_function::Value::Sha256,
                    digest_function::Value::Blake3
                ],
            )?,
            None
        );
        assert_eq!(
            select_download_hash_digest_function(
                &["SHA256".to_owned(), "SHA1".to_owned()],
                &[digest_function::Value::Sha1, digest_function::Value::Sha256],
            )?,
            None
        );
        assert_eq!(
            select_download_hash_digest_function(
                &["SHA384".to_owned()],
                &[digest_function::Value::Sha384],
            )?,
            None
        );
        let err = select_download_hash_digest_function(
            &["SHA256".to_owned()],
            &[digest_function::Value::Blake3],
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("configured=SHA256"));
        assert!(err.contains("server=BLAKE3"));
        Ok(())
    }

    fn test_re_capabilities(
        capabilities_queried: bool,
        cache_digest_functions: Vec<digest_function::Value>,
        execution_digest_functions: Vec<digest_function::Value>,
    ) -> RECapabilities {
        RECapabilities {
            capabilities_queried,
            max_total_batch_size: DEFAULT_MAX_TOTAL_BATCH_SIZE,
            max_cas_blob_size_bytes: None,
            supported_compressors: Vec::new(),
            supported_batch_update_compressors: Vec::new(),
            supported_digest_functions: Vec::new(),
            cache_digest_functions,
            execution_digest_functions,
            execution_priority_ranges: Vec::new(),
            action_cache_update_enabled: None,
            execution_enabled: Some(true),
            blob_split_supported: false,
            blob_splice_supported: false,
            fast_cdc_2020: None,
        }
    }

    #[test]
    fn cache_digest_functions_default_to_sha256_when_missing() {
        let (digest_functions, assumed_sha256) =
            cache_digest_functions_from_capabilities(Some(&CacheCapabilities::default()));

        assert!(assumed_sha256);
        assert_eq!(digest_functions, vec![digest_function::Value::Sha256]);
    }

    #[test]
    fn cache_digest_functions_preserve_advertised_blake3() {
        let (digest_functions, assumed_sha256) =
            cache_digest_functions_from_capabilities(Some(&CacheCapabilities {
                digest_functions: vec![digest_function::Value::Blake3 as i32],
                ..Default::default()
            }));

        assert!(!assumed_sha256);
        assert_eq!(digest_functions, vec![digest_function::Value::Blake3]);
    }

    #[test]
    fn test_validate_digest_function_capabilities() -> anyhow::Result<()> {
        validate_digest_function_capabilities(
            &["SHA256".to_owned()],
            &test_re_capabilities(
                true,
                vec![digest_function::Value::Sha256],
                vec![digest_function::Value::Sha256],
            ),
        )?;

        validate_digest_function_capabilities(
            &["BLAKE3".to_owned()],
            &test_re_capabilities(
                true,
                vec![digest_function::Value::Blake3],
                vec![digest_function::Value::Blake3],
            ),
        )?;

        let cache_err = validate_digest_function_capabilities(
            &["SHA256".to_owned()],
            &test_re_capabilities(
                true,
                vec![digest_function::Value::Blake3],
                vec![digest_function::Value::Sha256],
            ),
        )
        .unwrap_err()
        .to_string();
        assert!(cache_err.contains("remote cache capabilities"));

        let multi_err = validate_digest_function_capabilities(
            &["SHA1".to_owned(), "SHA256".to_owned()],
            &test_re_capabilities(
                true,
                vec![digest_function::Value::Sha256],
                vec![digest_function::Value::Sha1, digest_function::Value::Sha256],
            ),
        )
        .unwrap_err()
        .to_string();
        assert!(multi_err.contains("SHA1"));

        let execution_err = validate_digest_function_capabilities(
            &["SHA256".to_owned()],
            &test_re_capabilities(
                true,
                vec![digest_function::Value::Sha256],
                vec![digest_function::Value::Blake3],
            ),
        )
        .unwrap_err()
        .to_string();
        assert!(execution_err.contains("remote execution capabilities"));

        validate_digest_function_capabilities(
            &["SHA256".to_owned()],
            &test_re_capabilities(false, Vec::new(), Vec::new()),
        )?;

        let mut execution_disabled =
            test_re_capabilities(true, vec![digest_function::Value::Sha256], Vec::new());
        execution_disabled.execution_enabled = Some(false);
        validate_digest_function_capabilities(&["SHA256".to_owned()], &execution_disabled)?;

        Ok(())
    }

    #[test]
    fn test_digest_function_config_selects_unambiguous_requests() {
        let sha256 = Digest {
            hash: "a".repeat(64),
            size_bytes: 1,
        };
        let sha1 = Digest {
            hash: "b".repeat(40),
            size_bytes: 1,
        };

        let sha256_config = DigestFunctionConfig::from_configured_algorithms(&["SHA256".into()]);
        assert_eq!(
            sha256_config.for_digest(&sha256),
            Some(digest_function::Value::Sha256)
        );
        assert_eq!(sha256_config.for_digest(&sha1), None);

        let multi_config =
            DigestFunctionConfig::from_configured_algorithms(&["SHA1".into(), "SHA256".into()]);
        assert_eq!(
            multi_config.for_digest(&sha1),
            Some(digest_function::Value::Sha1)
        );
        assert_eq!(
            multi_config.for_digest(&sha256),
            Some(digest_function::Value::Sha256)
        );
        assert_eq!(
            multi_config.for_common_digest_function(std::slice::from_ref(&sha1)),
            Some(digest_function::Value::Sha1)
        );
        assert_eq!(
            multi_config.for_common_digest_function(&[sha1.clone(), sha256.clone()]),
            None
        );

        let ambiguous_config =
            DigestFunctionConfig::from_configured_algorithms(&["SHA256".into(), "BLAKE3".into()]);
        assert_eq!(ambiguous_config.for_digest(&sha256), None);
    }

    #[test]
    fn test_download_validation_selects_digest_function_by_hash() -> anyhow::Result<()> {
        let data = b"mixed digest validation";
        let sha1_digest = TDigest {
            hash: format!("{:x}", Sha1::digest(data)),
            size_in_bytes: data.len() as i64,
            ..Default::default()
        };
        let sha256_digest = digest_for_test_data(data);
        let digest_function_config =
            DigestFunctionConfig::from_configured_algorithms(&["SHA256".into(), "SHA1".into()]);
        let runtime_opts = RERuntimeOpts {
            use_fbcode_metadata: false,
            request_metadata_tool_name: DEFAULT_REQUEST_METADATA_TOOL_NAME.to_owned(),
            max_concurrent_uploads_per_action: None,
            cas_ttl_secs: 0,
            find_missing_blobs_batch_size: 100,
            remote_cache_chunking: false,
            remote_cache_compression_threshold: DEFAULT_REMOTE_CACHE_COMPRESSION_THRESHOLD,
            retries: 0,
            retry_max_delay_ms: 0,
            grpc_request_timeout: Duration::from_secs(DEFAULT_GRPC_REQUEST_TIMEOUT_SECS),
            bytestream_progress_timeout: Duration::from_secs(
                DEFAULT_BYTESTREAM_PROGRESS_TIMEOUT_SECS,
            ),
            queued_operation_timeout: Duration::from_secs(DEFAULT_QUEUED_OPERATION_TIMEOUT_SECS),
            stalled_operation_timeout: Duration::from_secs(DEFAULT_STALLED_OPERATION_TIMEOUT_SECS),
            download_hash_digest_function: Some(digest_function::Value::Sha256),
            request_digest_function_config: digest_function_config,
        };

        assert_eq!(
            runtime_opts.download_hash_digest_function_for_hash(&sha1_digest.hash),
            Some(digest_function::Value::Sha1)
        );
        validate_downloaded_blob(
            &sha1_digest,
            data,
            runtime_opts.download_hash_digest_function_for_hash(&sha1_digest.hash),
        )?;
        validate_downloaded_blob(
            &sha256_digest,
            data,
            runtime_opts.download_hash_digest_function_for_hash(&sha256_digest.hash),
        )?;

        Ok(())
    }

    #[test]
    fn test_chunking_function_conversion() {
        assert_eq!(
            chunking_function_to_grpc(TChunkingFunction::Unknown),
            chunking_function::Value::Unknown as i32
        );
        assert_eq!(
            chunking_function_to_grpc(TChunkingFunction::FastCdc2020),
            chunking_function::Value::FastCdc2020 as i32
        );
        assert_eq!(
            chunking_function_to_grpc(TChunkingFunction::RepMaxCdc),
            chunking_function::Value::RepMaxCdc as i32
        );

        assert_eq!(
            chunking_function_from_grpc(chunking_function::Value::FastCdc2020 as i32),
            TChunkingFunction::FastCdc2020
        );
        assert_eq!(
            chunking_function_from_grpc(chunking_function::Value::RepMaxCdc as i32),
            TChunkingFunction::RepMaxCdc
        );
        assert_eq!(
            chunking_function_from_grpc(i32::MAX),
            TChunkingFunction::Unknown
        );
    }

    #[test]
    fn test_fast_cdc_2020_config_from_capabilities() {
        let none = fast_cdc_2020_config_from_capabilities(Some(&CacheCapabilities::default()));
        assert_eq!(none, None);

        let config = fast_cdc_2020_config_from_capabilities(Some(&CacheCapabilities {
            fast_cdc_2020_params: Some(FastCdc2020Params {
                avg_chunk_size_bytes: 256 * 1024,
                seed: 7,
            }),
            ..Default::default()
        }));
        assert_eq!(
            config,
            Some(FastCdc2020Config {
                avg_chunk_size_bytes: 256 * 1024,
                seed: 7,
            })
        );
        let config = config.unwrap();
        assert_eq!(config.min_chunk_size_bytes(), 64 * 1024);
        assert_eq!(config.max_chunk_size_bytes(), 1024 * 1024);
        assert_eq!(config.chunking_threshold_bytes(), 1024 * 1024);
        assert_eq!(config.chunking_function(), TChunkingFunction::FastCdc2020);
        assert_eq!(
            config.normalization_level(),
            fastcdc::v2020::Normalization::Level2
        );
        assert_eq!(config.seed, 7);

        let invalid_avg = fast_cdc_2020_config_from_capabilities(Some(&CacheCapabilities {
            fast_cdc_2020_params: Some(FastCdc2020Params {
                avg_chunk_size_bytes: 17,
                seed: 9,
            }),
            ..Default::default()
        }));
        assert_eq!(
            invalid_avg,
            Some(FastCdc2020Config {
                avg_chunk_size_bytes: DEFAULT_FAST_CDC_2020_AVG_CHUNK_SIZE,
                seed: 9,
            })
        );
    }

    #[test]
    fn test_preferred_split_blob_chunking_function() {
        let mut capabilities = test_re_capabilities(true, Vec::new(), Vec::new());
        assert_eq!(
            preferred_split_blob_chunking_function(&capabilities, TChunkingFunction::Unknown),
            TChunkingFunction::Unknown
        );

        capabilities.fast_cdc_2020 = Some(FastCdc2020Config {
            avg_chunk_size_bytes: DEFAULT_FAST_CDC_2020_AVG_CHUNK_SIZE,
            seed: 0,
        });
        assert_eq!(
            preferred_split_blob_chunking_function(&capabilities, TChunkingFunction::Unknown),
            TChunkingFunction::FastCdc2020
        );
        assert_eq!(
            preferred_split_blob_chunking_function(&capabilities, TChunkingFunction::RepMaxCdc),
            TChunkingFunction::RepMaxCdc
        );
    }

    #[test]
    fn test_validate_chunking_function_supported() -> anyhow::Result<()> {
        let mut capabilities = test_re_capabilities(true, Vec::new(), Vec::new());
        validate_chunking_function_supported(&capabilities, TChunkingFunction::Unknown)?;

        let fast_cdc_err =
            validate_chunking_function_supported(&capabilities, TChunkingFunction::FastCdc2020)
                .unwrap_err()
                .to_string();
        assert!(fast_cdc_err.contains("FastCDC 2020"));

        capabilities.fast_cdc_2020 = Some(FastCdc2020Config {
            avg_chunk_size_bytes: DEFAULT_FAST_CDC_2020_AVG_CHUNK_SIZE,
            seed: 0,
        });
        validate_chunking_function_supported(&capabilities, TChunkingFunction::FastCdc2020)?;

        let rep_max_err =
            validate_chunking_function_supported(&capabilities, TChunkingFunction::RepMaxCdc)
                .unwrap_err()
                .to_string();
        assert!(rep_max_err.contains("RepMaxCDC"));

        Ok(())
    }

    #[test]
    fn test_validate_remote_cache_chunking_enabled() -> anyhow::Result<()> {
        let mut capabilities = test_re_capabilities(false, Vec::new(), Vec::new());
        validate_remote_cache_chunking_enabled(false, &capabilities)?;

        let unqueried = validate_remote_cache_chunking_enabled(true, &capabilities)
            .unwrap_err()
            .to_string();
        assert!(unqueried.contains("capabilities"));

        capabilities.capabilities_queried = true;
        let missing_split = validate_remote_cache_chunking_enabled(true, &capabilities)
            .unwrap_err()
            .to_string();
        assert!(missing_split.contains("SplitBlob"));

        capabilities.blob_split_supported = true;
        let missing_splice = validate_remote_cache_chunking_enabled(true, &capabilities)
            .unwrap_err()
            .to_string();
        assert!(missing_splice.contains("SpliceBlob"));

        capabilities.blob_splice_supported = true;
        let missing_fast_cdc = validate_remote_cache_chunking_enabled(true, &capabilities)
            .unwrap_err()
            .to_string();
        assert!(missing_fast_cdc.contains("FastCDC 2020"));

        capabilities.fast_cdc_2020 = Some(FastCdc2020Config {
            avg_chunk_size_bytes: DEFAULT_FAST_CDC_2020_AVG_CHUNK_SIZE,
            seed: 0,
        });
        validate_remote_cache_chunking_enabled(true, &capabilities)?;

        Ok(())
    }

    #[test]
    fn test_chunk_inlined_blob_fast_cdc_2020() -> anyhow::Result<()> {
        let data = (0..10_000)
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        let blob = InlinedBlobWithDigest {
            digest: digest_for_test_data(&data),
            blob: data.clone(),
            ..Default::default()
        };
        let config = FastCdc2020Config {
            avg_chunk_size_bytes: 1024,
            seed: 0,
        };

        let chunks =
            chunk_inlined_blob_fast_cdc_2020(&blob, &config, digest_function::Value::Sha256)?;

        assert!(chunks.len() > 1);
        let mut reassembled = Vec::new();
        for chunk in &chunks {
            validate_downloaded_blob(
                &chunk.digest,
                &chunk.blob,
                Some(digest_function::Value::Sha256),
            )?;
            assert!(chunk.digest.size_in_bytes <= config.max_chunk_size_bytes() as i64);
            reassembled.extend_from_slice(&chunk.blob);
        }
        assert_eq!(reassembled, data);

        let chunk_digests = chunks
            .into_iter()
            .map(|chunk| tdigest_to(chunk.digest))
            .collect::<Vec<_>>();
        validate_chunk_digests_reconstruct_blob(
            "FastCDC test",
            &tdigest_to(blob.digest),
            &chunk_digests,
        )?;

        Ok(())
    }

    #[test]
    fn test_chunk_file_fast_cdc_2020() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;
        let path = work.path().join("blob");
        let data = (0..10_000)
            .map(|value| ((value * 7) % 251) as u8)
            .collect::<Vec<_>>();
        std::fs::write(&path, &data)?;

        let blob = InlinedBlobWithDigest {
            digest: digest_for_test_data(&data),
            blob: data,
            ..Default::default()
        };
        let config = FastCdc2020Config {
            avg_chunk_size_bytes: 1024,
            seed: 0,
        };
        let inlined_chunks =
            chunk_inlined_blob_fast_cdc_2020(&blob, &config, digest_function::Value::Sha256)?;
        let file_chunks = chunk_file_fast_cdc_2020(
            path.to_str().context("temp path is not utf8")?,
            &config,
            digest_function::Value::Sha256,
        )?;

        assert_eq!(
            inlined_chunks
                .iter()
                .map(|chunk| &chunk.digest)
                .collect::<Vec<_>>(),
            file_chunks
                .iter()
                .map(|chunk| &chunk.digest)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            inlined_chunks
                .iter()
                .map(|chunk| &chunk.blob)
                .collect::<Vec<_>>(),
            file_chunks
                .iter()
                .map(|chunk| &chunk.blob)
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_local_chunk_cache_reads_valid_chunks() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;
        let cache = LocalChunkCache::new(work.path().to_path_buf());
        let data = b"cached chunk".to_vec();
        let digest = digest_for_test_data(&data);

        assert!(
            cache
                .read(&digest, Some(digest_function::Value::Sha256))
                .await?
                .is_none()
        );
        cache
            .write(&digest, &data, Some(digest_function::Value::Sha256))
            .await;

        assert_eq!(
            cache
                .read(&digest, Some(digest_function::Value::Sha256))
                .await?,
            Some(data)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_local_chunk_cache_ignores_corrupt_chunks() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;
        let cache = LocalChunkCache::new(work.path().to_path_buf());
        let data = b"cached chunk".to_vec();
        let digest = digest_for_test_data(&data);

        cache
            .write(&digest, &data, Some(digest_function::Value::Sha256))
            .await;
        let path = cache.path_for(&digest).context("expected cache path")?;
        tokio::fs::write(&path, b"bad").await?;

        assert!(
            cache
                .read(&digest, Some(digest_function::Value::Sha256))
                .await?
                .is_none()
        );
        assert!(!path.exists());

        Ok(())
    }

    #[test]
    fn test_local_chunk_cache_rejects_non_hex_paths() {
        let cache = LocalChunkCache::new(PathBuf::from("cache"));
        let digest = TDigest {
            hash: "../not-a-digest".to_owned(),
            size_in_bytes: 1,
            ..Default::default()
        };

        assert!(cache.path_for(&digest).is_none());
    }

    #[test]
    fn test_capability_names_are_readable() {
        assert_eq!(
            digest_function_names(&[
                digest_function::Value::Sha256,
                digest_function::Value::Blake3
            ]),
            "SHA256,BLAKE3"
        );
        assert_eq!(digest_function_names(&[]), "<unknown>");
        assert_eq!(
            compressor_names(&[Compressor::Zstd, Compressor::Brotli]),
            "zstd,brotli"
        );
        assert_eq!(compressor_names(&[]), "<none>");
        assert_eq!(
            priority_range_names(&[
                PriorityRange {
                    min_priority: 1,
                    max_priority: 10,
                },
                PriorityRange {
                    min_priority: 20,
                    max_priority: 30,
                },
            ]),
            "1-10,20-30"
        );
        assert_eq!(priority_range_names(&[]), "<unknown>");
    }

    #[test]
    fn test_validate_remote_execution_enabled() -> anyhow::Result<()> {
        validate_remote_execution_enabled(None)?;
        validate_remote_execution_enabled(Some(true))?;

        let err = validate_remote_execution_enabled(Some(false))
            .unwrap_err()
            .to_string();
        assert!(err.contains("Remote execution is not supported"));

        Ok(())
    }

    #[test]
    fn test_validate_split_splice_supported() {
        assert!(validate_blob_split_supported(true).is_ok());
        assert!(
            validate_blob_split_supported(false)
                .unwrap_err()
                .to_string()
                .contains("SplitBlob")
        );

        assert!(validate_blob_splice_supported(true).is_ok());
        assert!(
            validate_blob_splice_supported(false)
                .unwrap_err()
                .to_string()
                .contains("SpliceBlob")
        );
    }

    #[test]
    fn test_validate_splice_blob_response_digest() -> anyhow::Result<()> {
        let requested = tdigest_to(test_digest("aa", 1));

        let digest = validate_splice_blob_response_digest(
            &requested,
            GSpliceBlobResponse {
                blob_digest: Some(requested.clone()),
            },
        )?;
        assert_eq!(digest, tdigest_from(requested.clone()));

        let missing = validate_splice_blob_response_digest(
            &requested,
            GSpliceBlobResponse { blob_digest: None },
        )
        .unwrap_err()
        .to_string();
        assert!(missing.contains("omitted blob digest"));

        let unexpected = validate_splice_blob_response_digest(
            &requested,
            GSpliceBlobResponse {
                blob_digest: Some(tdigest_to(test_digest("bb", 2))),
            },
        )
        .unwrap_err()
        .to_string();
        assert!(unexpected.contains("unexpected digest"));

        Ok(())
    }

    #[test]
    fn test_validate_split_blob_response() -> anyhow::Result<()> {
        let requested = tdigest_to(test_digest("aa", 3));
        let chunks = validate_split_blob_response(
            &requested,
            GSplitBlobResponse {
                chunk_digests: vec![
                    tdigest_to(test_digest("bb", 1)),
                    tdigest_to(test_digest("cc", 2)),
                ],
                ..Default::default()
            },
        )?;
        assert_eq!(chunks, vec![test_digest("bb", 1), test_digest("cc", 2)]);

        let empty = validate_split_blob_response(
            &requested,
            GSplitBlobResponse {
                chunk_digests: Vec::new(),
                ..Default::default()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(empty.contains("no chunks"));

        let wrong_size = validate_split_blob_response(
            &requested,
            GSplitBlobResponse {
                chunk_digests: vec![tdigest_to(test_digest("bb", 1))],
                ..Default::default()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(wrong_size.contains("sum to 1 bytes"));

        let wrong_hash_length = validate_split_blob_response(
            &requested,
            GSplitBlobResponse {
                chunk_digests: vec![tdigest_to(test_digest("bbbb", 3))],
                ..Default::default()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(wrong_hash_length.contains("hash length"));

        let negative_size = validate_split_blob_response(
            &requested,
            GSplitBlobResponse {
                chunk_digests: vec![Digest {
                    hash: "bb".to_owned(),
                    size_bytes: -1,
                }],
                ..Default::default()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(negative_size.contains("negative-size"));

        Ok(())
    }

    #[test]
    fn test_validate_splice_blob_request_chunks() -> anyhow::Result<()> {
        let requested = tdigest_to(test_digest("aa", 3));
        validate_chunk_digests_reconstruct_blob(
            "SpliceBlob request",
            &requested,
            &[
                tdigest_to(test_digest("bb", 1)),
                tdigest_to(test_digest("cc", 2)),
            ],
        )?;

        let wrong_size = validate_chunk_digests_reconstruct_blob(
            "SpliceBlob request",
            &requested,
            &[tdigest_to(test_digest("bb", 1))],
        )
        .unwrap_err()
        .to_string();
        assert!(wrong_size.contains("SpliceBlob request"));

        Ok(())
    }

    #[test]
    fn test_default_capabilities_are_disabled() {
        assert!(!action_cache_update_enabled_from_capabilities(None));
        assert!(!action_cache_update_enabled_from_capabilities(Some(
            &CacheCapabilities::default()
        )));
        assert!(action_cache_update_enabled_from_capabilities(Some(
            &CacheCapabilities {
                action_cache_update_capabilities: Some(ActionCacheUpdateCapabilities {
                    update_enabled: true,
                }),
                ..Default::default()
            }
        )));

        assert!(!execution_enabled_from_capabilities(None));
        assert!(!execution_enabled_from_capabilities(Some(
            &ExecutionCapabilities::default()
        )));
        assert!(execution_enabled_from_capabilities(Some(
            &ExecutionCapabilities {
                exec_enabled: true,
                ..Default::default()
            }
        )));
    }

    #[test]
    fn test_validate_priority_in_range() -> anyhow::Result<()> {
        let ranges = vec![
            PriorityRange {
                min_priority: 1,
                max_priority: 10,
            },
            PriorityRange {
                min_priority: 20,
                max_priority: 30,
            },
        ];

        validate_priority_in_range(0, "remote_execution_priority", &[])?;
        validate_priority_in_range(1, "remote_execution_priority", &ranges)?;
        validate_priority_in_range(30, "remote_execution_priority", &ranges)?;

        let err = validate_priority_in_range(11, "remote_execution_priority", &ranges)
            .unwrap_err()
            .to_string();
        assert!(err.contains("1-10,20-30"));

        let err = validate_priority_in_range(1, "remote_execution_priority", &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("<unknown>"));

        Ok(())
    }

    fn semver(major: i32, minor: i32, patch: i32) -> SemVer {
        SemVer {
            major,
            minor,
            patch,
            ..Default::default()
        }
    }

    #[test]
    fn test_validate_re_api_versions() -> anyhow::Result<()> {
        assert_eq!(
            validate_re_api_versions(Some(&semver(2, 0, 0)), Some(&semver(2, 11, 0)), None)?,
            None
        );
        assert_eq!(
            validate_re_api_versions(Some(&semver(2, 1, 0)), Some(&semver(2, 3, 0)), None)?,
            None
        );

        let warning = validate_re_api_versions(
            Some(&semver(3, 0, 0)),
            Some(&semver(3, 1, 0)),
            Some(&semver(2, 0, 0)),
        )?
        .expect("deprecated overlap should warn");
        assert!(warning.contains("deprecated"));

        let err = validate_re_api_versions(Some(&semver(3, 0, 0)), Some(&semver(3, 1, 0)), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("not supported by the server"));

        Ok(())
    }

    fn test_digest(hash: &str, size_in_bytes: i64) -> TDigest {
        TDigest {
            hash: hash.to_owned(),
            size_in_bytes,
            ..Default::default()
        }
    }

    #[test]
    fn test_digests_with_ttl_preserves_requested_duplicates() -> anyhow::Result<()> {
        let digest1 = test_digest("aa", 1);
        let digest2 = test_digest("bb", 2);

        let mut remote_results = HashMap::new();
        remote_results.insert(digest1.clone(), DigestRemoteState::ExistsOnRemote);
        remote_results.insert(digest2.clone(), DigestRemoteState::Missing);

        let digests_with_ttl = digests_with_ttl_for_requested_digests(
            &[digest1.clone(), digest2.clone(), digest1.clone()],
            &remote_results,
            17,
        )?;

        assert_eq!(digests_with_ttl.len(), 3);
        assert_eq!(digests_with_ttl[0].digest, digest1);
        assert_eq!(digests_with_ttl[0].ttl, 17);
        assert_eq!(digests_with_ttl[1].digest, digest2);
        assert_eq!(digests_with_ttl[1].ttl, 0);
        assert_eq!(digests_with_ttl[2].digest, digest1);
        assert_eq!(digests_with_ttl[2].ttl, 17);

        Ok(())
    }

    #[test]
    fn test_record_find_missing_results_does_not_cache_missing() {
        let present = test_digest("aa", 1);
        let missing = test_digest("bb", 2);
        let mut remote_results = HashMap::new();
        let mut cache = FindMissingCache {
            cache: LruCache::new(std::num::NonZeroUsize::new(10).unwrap()),
            ttl: std::time::Duration::from_secs(60),
            last_check: std::time::Instant::now(),
        };

        record_find_missing_results(
            &[present.clone(), missing.clone()],
            &HashSet::from([missing.clone()]),
            &mut remote_results,
            &mut cache,
        );

        assert_eq!(
            remote_results.get(&present),
            Some(&DigestRemoteState::ExistsOnRemote)
        );
        assert_eq!(
            remote_results.get(&missing),
            Some(&DigestRemoteState::Missing)
        );
        assert_eq!(cache.get(&present), Some(DigestRemoteState::ExistsOnRemote));
        assert_eq!(cache.get(&missing), None);
    }

    #[test]
    fn test_convert_action_result_sets_output_file_ttl() -> anyhow::Result<()> {
        let digest = test_digest("aa", 1);
        let action_result = ActionResult {
            execution_metadata: Some(ExecutedActionMetadata::default()),
            output_files: vec![OutputFile {
                path: "out".to_owned(),
                digest: Some(tdigest_to(digest.clone())),
                ..Default::default()
            }],
            ..Default::default()
        };

        let converted = convert_action_result(action_result, 42)?;

        assert_eq!(converted.output_files.len(), 1);
        assert_eq!(converted.output_files[0].digest.digest, digest);
        assert_eq!(converted.output_files[0].ttl, 42);
        Ok(())
    }

    #[test]
    fn test_convert_action_result_preserves_execution_auxiliary_metadata() -> anyhow::Result<()> {
        let action_result = ActionResult {
            execution_metadata: Some(ExecutedActionMetadata {
                auxiliary_metadata: vec![prost_types::Any {
                    type_url: "type.googleapis.com/buck2.RemoteDepFile".to_owned(),
                    value: b"dep-file-metadata".to_vec(),
                }],
                ..Default::default()
            }),
            ..Default::default()
        };

        let converted = convert_action_result(action_result, 42)?;

        assert_eq!(converted.execution_metadata.auxiliary_metadata.len(), 1);
        let metadata = &converted.execution_metadata.auxiliary_metadata[0];
        assert_eq!(metadata.type_url, "type.googleapis.com/buck2.RemoteDepFile");
        assert_eq!(metadata.value, b"dep-file-metadata".to_vec());
        Ok(())
    }

    #[test]
    fn test_convert_t_action_result2_preserves_execution_auxiliary_metadata_and_raw_stdio()
    -> anyhow::Result<()> {
        let t_action_result = TActionResult2 {
            stdout_raw: Some(b"inline stdout".to_vec()),
            stderr_raw: Some(b"inline stderr".to_vec()),
            execution_metadata: TExecutedActionMetadata {
                auxiliary_metadata: vec![TAny {
                    type_url: "type.googleapis.com/buck2.RemoteDepFile".to_owned(),
                    value: b"dep-file-metadata".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            ..Default::default()
        };

        let converted = convert_t_action_result2(t_action_result)?;

        assert_eq!(converted.stdout_raw, b"inline stdout".to_vec());
        assert_eq!(converted.stderr_raw, b"inline stderr".to_vec());
        let metadata = &converted
            .execution_metadata
            .as_ref()
            .expect("execution metadata should be set")
            .auxiliary_metadata;
        assert_eq!(metadata.len(), 1);
        assert_eq!(
            metadata[0].type_url,
            "type.googleapis.com/buck2.RemoteDepFile"
        );
        assert_eq!(metadata[0].value, b"dep-file-metadata".to_vec());
        Ok(())
    }

    #[test]
    fn test_validate_extend_digests_ttl_response_rejects_missing() {
        let digest = test_digest("aa", 1);

        let error = validate_extend_digests_ttl_response(
            std::slice::from_ref(&digest),
            GetDigestsTtlResponse {
                digests_with_ttl: vec![DigestWithTtl {
                    digest: digest.clone(),
                    ttl: 0,
                }],
            },
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("Cannot refresh CAS TTL for missing digests")
        );
    }

    #[test]
    fn test_validate_extend_digests_ttl_response_accepts_present() -> anyhow::Result<()> {
        let digest = test_digest("aa", 1);

        validate_extend_digests_ttl_response(
            std::slice::from_ref(&digest),
            GetDigestsTtlResponse {
                digests_with_ttl: vec![DigestWithTtl {
                    digest: digest.clone(),
                    ttl: 17,
                }],
            },
        )
    }

    #[test]
    fn test_validate_upload_request_sizes_allows_unknown_limit() -> anyhow::Result<()> {
        validate_upload_request_sizes(
            &UploadRequest {
                inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                    blob: vec![0; 4],
                    digest: test_digest("aa", 4),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            None,
        )?;

        Ok(())
    }

    #[test]
    fn test_validate_upload_request_sizes_rejects_oversized_blob() {
        let err = validate_upload_request_sizes(
            &UploadRequest {
                inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                    blob: vec![0; 11],
                    digest: test_digest("aa", 11),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            Some(10),
        )
        .unwrap_err();
        let err = format!("{err:#}");

        assert!(err.contains("oversized inlined blob"));
        assert!(err.contains("max_cas_blob_size_bytes 10"));
    }

    #[test]
    fn test_validate_upload_request_sizes_checks_files_and_directories() {
        let file_err = validate_upload_request_sizes(
            &UploadRequest {
                files_with_digest: Some(vec![NamedDigest {
                    name: "file.out".to_owned(),
                    digest: test_digest("bb", 12),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            Some(10),
        )
        .unwrap_err()
        .to_string();

        assert!(file_err.contains("oversized file `file.out`"));

        let directory_err = validate_upload_request_sizes(
            &UploadRequest {
                directories: Some(vec![Path {
                    path: "tree".to_owned(),
                    digest: Some(test_digest("cc", 13)),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            Some(10),
        )
        .unwrap_err()
        .to_string();

        assert!(directory_err.contains("oversized directory `tree`"));
    }

    #[test]
    fn test_filter_upload_request_by_missing_digests() {
        let present = test_digest("aa", 1);
        let missing_file = test_digest("bb", 2);
        let missing_blob = test_digest("cc", 3);
        let missing_directory = test_digest("dd", 4);

        let request = UploadRequest {
            files_with_digest: Some(vec![
                NamedDigest {
                    name: "present.out".to_owned(),
                    digest: present.clone(),
                    ..Default::default()
                },
                NamedDigest {
                    name: "missing.out".to_owned(),
                    digest: missing_file.clone(),
                    ..Default::default()
                },
            ]),
            inlined_blobs_with_digest: Some(vec![
                InlinedBlobWithDigest {
                    blob: b"present".to_vec(),
                    digest: present.clone(),
                    ..Default::default()
                },
                InlinedBlobWithDigest {
                    blob: b"missing".to_vec(),
                    digest: missing_blob.clone(),
                    ..Default::default()
                },
            ]),
            directories: Some(vec![
                Path {
                    path: "present-tree".to_owned(),
                    digest: Some(present.clone()),
                    ..Default::default()
                },
                Path {
                    path: "missing-tree".to_owned(),
                    digest: Some(missing_directory.clone()),
                    ..Default::default()
                },
                Path {
                    path: "unknown-tree".to_owned(),
                    digest: None,
                    ..Default::default()
                },
            ]),
            upload_only_missing: true,
            ..Default::default()
        };

        assert_eq!(upload_request_digests(&request).len(), 6);
        assert_eq!(upload_payload_digests(&request).len(), 4);

        let missing_digests = HashSet::from([
            missing_file.clone(),
            missing_blob.clone(),
            missing_directory.clone(),
        ]);
        let request = filter_upload_request_by_missing_digests(request, &missing_digests);

        assert!(!request.upload_only_missing);
        assert_eq!(
            request
                .files_with_digest
                .unwrap()
                .into_iter()
                .map(|file| file.digest)
                .collect::<Vec<_>>(),
            vec![missing_file]
        );
        assert_eq!(
            request
                .inlined_blobs_with_digest
                .unwrap()
                .into_iter()
                .map(|blob| blob.digest)
                .collect::<Vec<_>>(),
            vec![missing_blob]
        );
        assert_eq!(
            request
                .directories
                .unwrap()
                .into_iter()
                .map(|directory| directory.digest.unwrap())
                .collect::<Vec<_>>(),
            vec![missing_directory]
        );
    }

    #[test]
    fn test_validate_batch_update_blobs_response_checks_digests() -> anyhow::Result<()> {
        let digest1 = tdigest_to(test_digest("aa", 1));
        let digest2 = tdigest_to(test_digest("bb", 2));

        validate_batch_update_blobs_response(
            &[digest1.clone(), digest2.clone()],
            &BatchUpdateBlobsResponse {
                responses: vec![
                    batch_update_blobs_response::Response {
                        digest: Some(digest2.clone()),
                        status: Some(Status::default()),
                    },
                    batch_update_blobs_response::Response {
                        digest: Some(digest1.clone()),
                        status: Some(Status::default()),
                    },
                ],
            },
        )?;

        let missing = validate_batch_update_blobs_response(
            &[digest1.clone(), digest2.clone()],
            &BatchUpdateBlobsResponse {
                responses: vec![batch_update_blobs_response::Response {
                    digest: Some(digest1.clone()),
                    status: Some(Status::default()),
                }],
            },
        )
        .unwrap_err()
        .to_string();
        assert!(missing.contains("missing digest"));

        let unexpected = validate_batch_update_blobs_response(
            &[digest1],
            &BatchUpdateBlobsResponse {
                responses: vec![batch_update_blobs_response::Response {
                    digest: Some(digest2),
                    status: Some(Status::default()),
                }],
            },
        )
        .unwrap_err()
        .to_string();
        assert!(unexpected.contains("unexpected digest"));

        let failed = validate_batch_update_blobs_response(
            &[tdigest_to(test_digest("cc", 3))],
            &BatchUpdateBlobsResponse {
                responses: vec![batch_update_blobs_response::Response {
                    digest: Some(tdigest_to(test_digest("cc", 3))),
                    status: Some(Status {
                        code: Code::InvalidArgument as i32,
                        message: "bad digest".to_owned(),
                        ..Default::default()
                    }),
                }],
            },
        )
        .unwrap_err()
        .to_string();
        assert!(failed.contains("bad digest"));

        Ok(())
    }

    #[tokio::test]
    async fn retry_grpc_request_retries_once_after_credentials_are_refreshed() {
        let attempts = AtomicU16::new(0);
        let result: anyhow::Result<()> = retry_grpc_request_with_recovery(
            0,
            Duration::from_millis(1),
            || async {
                attempts.fetch_add(1, Ordering::Relaxed);
                Err(anyhow::Error::from(tonic::Status::unauthenticated(
                    "token expired",
                )))
            },
            |recovery| {
                assert_eq!(recovery, Recovery::RefreshCredentials);
                async { true }
            },
        )
        .await;

        let err = result.unwrap_err();
        let err = err.downcast_ref::<REClientError>().expect("REClientError");
        assert_eq!(err.code, TCode::UNAUTHENTICATED);
        assert_eq!(attempts.load(Ordering::Relaxed), 2);

        let attempts = AtomicU16::new(0);
        retry_grpc_request_with_recovery(
            0,
            Duration::from_millis(1),
            || async {
                if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                    Err(anyhow::Error::from(tonic::Status::unauthenticated(
                        "token expired",
                    )))
                } else {
                    Ok(())
                }
            },
            |_| async { true },
        )
        .await
        .unwrap();
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn retry_grpc_request_does_not_retry_unauthenticated_without_a_helper() {
        let attempts = AtomicU16::new(0);
        let result: anyhow::Result<()> =
            retry_grpc_request(3, Duration::from_millis(1), || async {
                attempts.fetch_add(1, Ordering::Relaxed);
                Err(anyhow::Error::from(tonic::Status::unauthenticated(
                    "token expired",
                )))
            })
            .await;

        let err = result.unwrap_err();
        let err = err.downcast_ref::<REClientError>().expect("REClientError");
        assert_eq!(err.code, TCode::UNAUTHENTICATED);
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    /// Execute takes this path, and an Execute that timed out on the client's side may still be
    /// running, so it is not sent again.
    #[tokio::test]
    async fn retry_grpc_request_does_not_retry_a_client_timeout_outside_idempotent_reads() {
        let attempts = AtomicU16::new(0);
        let result: anyhow::Result<()> = retry_grpc_request_with_recovery(
            3,
            Duration::from_millis(1),
            || async {
                attempts.fetch_add(1, Ordering::Relaxed);
                Err(anyhow::Error::from(tonic::Status::cancelled(
                    "Timeout expired",
                )))
            },
            |_| async { false },
        )
        .await;

        let err = result.unwrap_err();
        let err = err.downcast_ref::<REClientError>().expect("REClientError");
        assert_eq!(err.code, TCode::CANCELLED);
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    struct FakeActionCacheState {
        expected_token: Mutex<String>,
        seen_tokens: Mutex<Vec<String>>,
        hold_next: AtomicBool,
        arrived: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    struct FakeActionCache(Arc<FakeActionCacheState>);

    #[tonic::async_trait]
    impl ActionCache for FakeActionCache {
        async fn get_action_result(
            &self,
            request: tonic::Request<GetActionResultRequest>,
        ) -> Result<tonic::Response<ActionResult>, tonic::Status> {
            let state = &self.0;
            let token = request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            state.seen_tokens.lock().unwrap().push(token.clone());
            state.arrived.notify_one();
            if token != *state.expected_token.lock().unwrap() {
                return Err(tonic::Status::unauthenticated("bad token"));
            }
            if state.hold_next.swap(false, Ordering::SeqCst) {
                state.release.notified().await;
            }
            Ok(tonic::Response::new(ActionResult {
                execution_metadata: Some(ExecutedActionMetadata::default()),
                ..Default::default()
            }))
        }

        async fn update_action_result(
            &self,
            _request: tonic::Request<UpdateActionResultRequest>,
        ) -> Result<tonic::Response<ActionResult>, tonic::Status> {
            Err(tonic::Status::unimplemented("not used by this test"))
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn get_action_result_refreshes_credentials_from_the_helper() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let token_file = dir.path().join("token");
        std::fs::write(&token_file, "one")?;
        // Run through `sh`, as the helper crate's tests do, so a concurrent test writing its
        // own script never makes this exec fail with "text file busy".
        let script = dir.path().join("helper.sh");
        std::fs::write(
            &script,
            format!(
                "[ \"$1\" = get ] || exit 3\nprintf '{{\"headers\": {{\"authorization\": [\"Bearer %s\"]}}}}' \"$(cat {})\"\n",
                token_file.display()
            ),
        )?;

        let state = Arc::new(FakeActionCacheState {
            expected_token: Mutex::new("Bearer one".to_owned()),
            seen_tokens: Mutex::new(Vec::new()),
            hold_next: AtomicBool::new(false),
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = format!("grpc://{}", listener.local_addr()?);
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(ActionCacheServer::new(FakeActionCache(state.clone())))
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );

        let opts = Buck2OssReConfiguration {
            cas_address: Some(address.clone()),
            engine_address: Some(address.clone()),
            action_cache_address: Some(address),
            tls: Some(false),
            capabilities: Some(false),
            retries: Some(0),
            credential_helper: Some(format!("/bin/sh {}", script.display())),
            ..Default::default()
        };
        let client = REClientBuilder::build_and_connect(&opts).await?;
        let metadata = RemoteExecutionMetadata::default();
        let request = || ActionResultRequest {
            digest: TDigest {
                hash: "ab".repeat(32),
                size_in_bytes: 1,
                _dot_dot: (),
            },
            platform: None,
            _dot_dot: (),
        };

        state.hold_next.store(true, Ordering::SeqCst);
        let held = client.get_action_result(&metadata, request());
        let while_held = async {
            state.arrived.notified().await;
            std::fs::write(&token_file, "two")?;
            *state.expected_token.lock().unwrap() = "Bearer two".to_owned();

            client.get_action_result(&metadata, request()).await?;
            assert_eq!(
                *state.seen_tokens.lock().unwrap(),
                ["Bearer one", "Bearer one", "Bearer two"]
            );

            state.release.notify_one();
            anyhow::Ok(())
        };
        let (held, while_held) = futures::join!(held, while_held);
        while_held?;
        held?;

        assert_eq!(state.seen_tokens.lock().unwrap().len(), 3);
        server.abort();
        Ok(())
    }

    /// Stands in front of the real server the way Namespace's ingress does: a request whose
    /// bearer it does not accept gets HTTP 401 with a plain-text body and no gRPC status.
    #[derive(Clone)]
    struct RefuseLikeAProxy<S> {
        inner: S,
        expected_token: Arc<Mutex<String>>,
        seen_tokens: Arc<Mutex<Vec<String>>>,
    }

    impl<S> tower::Service<http::Request<tonic::body::Body>> for RefuseLikeAProxy<S>
    where
        S: tower::Service<
                http::Request<tonic::body::Body>,
                Response = http::Response<tonic::body::Body>,
            > + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
            let token = request
                .headers()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            self.seen_tokens.lock().unwrap().push(token.clone());
            if token != *self.expected_token.lock().unwrap() {
                let response = http::Response::builder()
                    .status(http::StatusCode::UNAUTHORIZED)
                    .header("content-type", "text/plain")
                    .body(tonic::body::Body::new(http_body_util::Full::new(
                        bytes::Bytes::from_static(b"invalid bearer token\n"),
                    )))
                    .unwrap();
                return Box::pin(async move { Ok(response) });
            }
            Box::pin(self.inner.call(request))
        }
    }

    async fn serve_behind_refusing_proxy(
        expected_token: &str,
    ) -> anyhow::Result<(String, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>)> {
        let expected_token = Arc::new(Mutex::new(expected_token.to_owned()));
        let seen_tokens = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = format!("grpc://{}", listener.local_addr()?);
        let layer = {
            let expected_token = expected_token.clone();
            let seen_tokens = seen_tokens.clone();
            tower::layer::layer_fn(move |inner| RefuseLikeAProxy {
                inner,
                expected_token: expected_token.clone(),
                seen_tokens: seen_tokens.clone(),
            })
        };
        let server = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .layer(layer)
                .add_service(ActionCacheServer::new(PassingActionCache))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });
        Ok((address, seen_tokens, server))
    }

    struct PassingActionCache;

    #[tonic::async_trait]
    impl ActionCache for PassingActionCache {
        async fn get_action_result(
            &self,
            _request: tonic::Request<GetActionResultRequest>,
        ) -> Result<tonic::Response<ActionResult>, tonic::Status> {
            Ok(tonic::Response::new(ActionResult {
                execution_metadata: Some(ExecutedActionMetadata::default()),
                ..Default::default()
            }))
        }

        async fn update_action_result(
            &self,
            _request: tonic::Request<UpdateActionResultRequest>,
        ) -> Result<tonic::Response<ActionResult>, tonic::Status> {
            Err(tonic::Status::unimplemented("not used by this test"))
        }
    }

    fn action_result_request() -> ActionResultRequest {
        ActionResultRequest {
            digest: TDigest {
                hash: "ab".repeat(32),
                size_in_bytes: 1,
                _dot_dot: (),
            },
            platform: None,
            _dot_dot: (),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_proxy_401_reruns_the_helper_and_retries_once() -> anyhow::Result<()> {
        let (address, seen_tokens, server) = serve_behind_refusing_proxy("Bearer two").await?;
        let dir = tempfile::tempdir()?;
        let calls = dir.path().join("calls");
        std::fs::write(&calls, "0")?;
        // The first call hands out the token the proxy refuses, later calls the one it takes.
        let script = dir.path().join("helper.sh");
        std::fs::write(
            &script,
            format!(
                "[ \"$1\" = get ] || exit 3\nn=$(cat {c}); echo $((n + 1)) > {c}\nif [ \"$n\" = 0 ]; then t=one; else t=two; fi\nprintf '{{\"headers\": {{\"authorization\": [\"Bearer %s\"]}}}}' \"$t\"\n",
                c = calls.display()
            ),
        )?;
        let opts = Buck2OssReConfiguration {
            cas_address: Some(address.clone()),
            engine_address: Some(address.clone()),
            action_cache_address: Some(address),
            tls: Some(false),
            capabilities: Some(false),
            retries: Some(0),
            credential_helper: Some(format!("/bin/sh {}", script.display())),
            ..Default::default()
        };
        let client = REClientBuilder::build_and_connect(&opts).await?;

        client
            .get_action_result(&RemoteExecutionMetadata::default(), action_result_request())
            .await?;

        assert_eq!(*seen_tokens.lock().unwrap(), ["Bearer one", "Bearer two"]);
        assert_eq!(std::fs::read_to_string(&calls)?.trim(), "2");
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn a_proxy_401_without_a_helper_is_not_retried() -> anyhow::Result<()> {
        let (address, seen_tokens, server) = serve_behind_refusing_proxy("Bearer two").await?;
        let opts = Buck2OssReConfiguration {
            cas_address: Some(address.clone()),
            engine_address: Some(address.clone()),
            action_cache_address: Some(address),
            tls: Some(false),
            capabilities: Some(false),
            retries: Some(3),
            ..Default::default()
        };
        let client = REClientBuilder::build_and_connect(&opts).await?;

        let Err(err) = client
            .get_action_result(&RemoteExecutionMetadata::default(), action_result_request())
            .await
        else {
            panic!("the proxy refused the only credentials there are");
        };

        assert!(format!("{err:#}").contains("401"), "{err:#}");
        assert_eq!(seen_tokens.lock().unwrap().len(), 1);
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn retry_grpc_request_preserves_exhausted_status_code() {
        let result: anyhow::Result<()> =
            retry_grpc_request(0, Duration::from_millis(1), || async {
                Err(anyhow::Error::from(tonic::Status::unavailable(
                    "cache down",
                )))
            })
            .await;

        let err = result.unwrap_err();
        let err = err.downcast_ref::<REClientError>().expect("REClientError");
        assert_eq!(err.code, TCode::UNAVAILABLE);
    }

    const H2_DATA: u8 = 0x0;
    const H2_HEADERS: u8 = 0x1;
    const H2_RST_STREAM: u8 = 0x3;
    const H2_SETTINGS: u8 = 0x4;
    const H2_PING: u8 = 0x6;
    const H2_GOAWAY: u8 = 0x7;
    const H2_WINDOW_UPDATE: u8 = 0x8;
    const H2_FLAG_END_STREAM: u8 = 0x1;
    const H2_FLAG_ACK: u8 = 0x1;
    const H2_FLAG_END_HEADERS: u8 = 0x4;
    const H2_CANCEL: u32 = 0x8;
    const H2_ENHANCE_YOUR_CALM: u32 = 0xb;
    // HPACK (RFC 7541): `:status: 200` is static entry 8; `content-type` (static name 31) and
    // `grpc-status` go as literals without indexing, so the client's decoder needs no state.
    const H2_RESPONSE_HEADERS: &[u8] = b"\x88\x0f\x10\x10application/grpc";
    const H2_OK_TRAILERS: &[u8] = b"\x00\x0bgrpc-status\x010";

    /// When the raw server sends a stream's trailers.
    #[derive(Clone, Copy)]
    enum RawTrailers {
        /// When the client resets the stream. These are the frames a server had already put on
        /// the wire when the client's RST_STREAM reached it, which over a real network arrive
        /// after the reset; on loopback they could only be produced this way.
        OnReset,
        /// After the delay, or at the client's reset if that comes first.
        After(Duration),
        Never,
    }

    /// What the raw server does with a request.
    enum RawReply {
        /// Answers with the Operation and sends the trailers as `RawTrailers` says.
        Operation(Operation, RawTrailers),
        /// Answers with the Operation, then closes the connection, as a server or a proxy that
        /// goes away does.
        OperationThenClose(Operation),
        /// Closes the connection without an answer.
        Close,
        /// Answers with each Operation once its delay after the previous one has passed. The
        /// trailers follow the last Operation after the delay of a `RawTrailers::After`; with any
        /// other `RawTrailers` the stream stays open.
        Operations(Vec<(Duration, Operation)>, RawTrailers),
        /// Holds the stream open without even its response headers.
        Nothing,
        /// Answers with one encoded message, as a unary call such as a CAS read does, and the
        /// trailers.
        Unary(Vec<u8>),
        /// Stops reading the connection, PINGs included, and holds it open: a peer whose
        /// process stopped while its kernel keeps the TCP session.
        Silence,
    }

    #[derive(Default)]
    struct RawH2Log {
        /// The client's RST_STREAM frames, by error code.
        resets: Mutex<HashMap<u32, usize>>,
        /// The client's GOAWAY with an error, as its code and debug data.
        go_away: Mutex<Option<(u32, Vec<u8>)>>,
        trailers_sent: AtomicUsize,
        connections: AtomicUsize,
        /// The requests each connection carried, by the order the server accepted it in.
        requests_by_connection: Mutex<HashMap<usize, usize>>,
    }

    fn h2_frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
        let mut frame = (payload.len() as u32).to_be_bytes()[1..].to_vec();
        frame.extend([kind, flags]);
        frame.extend(stream.to_be_bytes());
        frame.extend(payload);
        frame
    }

    fn send_raw_trailers(
        stream: u32,
        pending: &Mutex<HashSet<u32>>,
        frames: &tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
        log: &RawH2Log,
    ) {
        if pending.lock().unwrap().remove(&stream) {
            let _ = frames.send(h2_frame(
                H2_HEADERS,
                H2_FLAG_END_HEADERS | H2_FLAG_END_STREAM,
                stream,
                H2_OK_TRAILERS,
            ));
            log.trailers_sent.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// An HTTP/2 server at the frame level, which answers every request with one Operation and
    /// sends the trailers when `reply` says. tonic's server stops writing to a stream the client
    /// reset, as it should, so it cannot send the frames a real server had in flight then.
    async fn serve_raw_h2(
        reply: impl Fn(&[u8]) -> RawReply + Send + Sync + 'static,
    ) -> anyhow::Result<(String, Arc<RawH2Log>, tokio::task::JoinHandle<()>)> {
        serve_raw_h2_by_connection(move |_, request| reply(request)).await
    }

    /// `serve_raw_h2`, whose `reply` is also given the connection, numbered from 0 in the order
    /// the server accepted it.
    async fn serve_raw_h2_by_connection(
        reply: impl Fn(usize, &[u8]) -> RawReply + Send + Sync + 'static,
    ) -> anyhow::Result<(String, Arc<RawH2Log>, tokio::task::JoinHandle<()>)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = format!("grpc://{}", listener.local_addr()?);
        let log = Arc::new(RawH2Log::default());
        let reply: Arc<dyn Fn(usize, &[u8]) -> RawReply + Send + Sync> = Arc::new(reply);
        let server = tokio::spawn({
            let log = log.clone();
            async move {
                while let Ok((socket, _)) = listener.accept().await {
                    let connection = log.connections.fetch_add(1, Ordering::SeqCst);
                    let reply = reply.clone();
                    tokio::spawn(serve_raw_h2_connection(
                        socket,
                        Arc::new(move |request: &[u8]| reply(connection, request)),
                        connection,
                        log.clone(),
                    ));
                }
            }
        });
        Ok((address, log, server))
    }

    fn raw_grpc_message(operation: &Operation) -> Vec<u8> {
        raw_grpc_bytes(operation.encode_to_vec())
    }

    fn raw_grpc_bytes(message: Vec<u8>) -> Vec<u8> {
        let mut data = vec![0u8];
        data.extend((message.len() as u32).to_be_bytes());
        data.extend(message);
        data
    }

    async fn serve_raw_h2_connection(
        socket: tokio::net::TcpStream,
        reply: Arc<dyn Fn(&[u8]) -> RawReply + Send + Sync>,
        connection: usize,
        log: Arc<RawH2Log>,
    ) -> anyhow::Result<()> {
        let (mut reader, mut writer) = socket.into_split();
        let (frames, mut outgoing) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        tokio::spawn(async move {
            // An empty frame closes the connection, once everything before it is written.
            while let Some(frame) = outgoing.recv().await {
                if frame.is_empty() || writer.write_all(&frame).await.is_err() {
                    break;
                }
            }
        });
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let mut bodies = HashMap::<u32, Vec<u8>>::new();

        let mut preface = [0u8; 24];
        reader.read_exact(&mut preface).await?;
        // SETTINGS_MAX_CONCURRENT_STREAMS = 10000, so every stream of a test is open at once.
        frames.send(h2_frame(H2_SETTINGS, 0, 0, &[0, 3, 0, 0, 0x27, 0x10]))?;
        loop {
            let mut header = [0u8; 9];
            if reader.read_exact(&mut header).await.is_err() {
                return Ok(());
            }
            let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
            let (kind, flags) = (header[3], header[4]);
            let stream = u32::from_be_bytes(header[5..9].try_into()?) & 0x7fff_ffff;
            let mut payload = vec![0u8; len];
            reader.read_exact(&mut payload).await?;
            match kind {
                H2_SETTINGS if flags & H2_FLAG_ACK == 0 => {
                    frames.send(h2_frame(H2_SETTINGS, H2_FLAG_ACK, 0, &[]))?;
                }
                H2_PING if flags & H2_FLAG_ACK == 0 => {
                    frames.send(h2_frame(H2_PING, H2_FLAG_ACK, 0, &payload))?;
                }
                H2_HEADERS => {
                    bodies.insert(stream, Vec::new());
                }
                H2_DATA => {
                    if len > 0 {
                        // Give the connection window back, or requests stall after 64 KiB.
                        frames.send(h2_frame(
                            H2_WINDOW_UPDATE,
                            0,
                            0,
                            &(len as u32).to_be_bytes(),
                        ))?;
                    }
                    bodies.entry(stream).or_default().extend(&payload);
                    if flags & H2_FLAG_END_STREAM == 0 {
                        continue;
                    }
                    let body = bodies.remove(&stream).unwrap_or_default();
                    *log.requests_by_connection
                        .lock()
                        .unwrap()
                        .entry(connection)
                        .or_default() += 1;
                    let (operation, trailers) = match reply(body.get(5..).unwrap_or_default()) {
                        RawReply::Operation(operation, trailers) => (operation, Some(trailers)),
                        RawReply::OperationThenClose(operation) => (operation, None),
                        RawReply::Close => {
                            frames.send(Vec::new())?;
                            return Ok(());
                        }
                        RawReply::Nothing => continue,
                        RawReply::Unary(message) => {
                            frames.send(h2_frame(
                                H2_HEADERS,
                                H2_FLAG_END_HEADERS,
                                stream,
                                H2_RESPONSE_HEADERS,
                            ))?;
                            frames.send(h2_frame(H2_DATA, 0, stream, &raw_grpc_bytes(message)))?;
                            pending.lock().unwrap().insert(stream);
                            send_raw_trailers(stream, &pending, &frames, &log);
                            continue;
                        }
                        RawReply::Silence => return std::future::pending().await,
                        RawReply::Operations(operations, trailers) => {
                            frames.send(h2_frame(
                                H2_HEADERS,
                                H2_FLAG_END_HEADERS,
                                stream,
                                H2_RESPONSE_HEADERS,
                            ))?;
                            let (pending, frames, log) =
                                (pending.clone(), frames.clone(), log.clone());
                            tokio::spawn(async move {
                                for (delay, operation) in operations {
                                    tokio::time::sleep(delay).await;
                                    let _ = frames.send(h2_frame(
                                        H2_DATA,
                                        0,
                                        stream,
                                        &raw_grpc_message(&operation),
                                    ));
                                }
                                if let RawTrailers::After(delay) = trailers {
                                    pending.lock().unwrap().insert(stream);
                                    tokio::time::sleep(delay).await;
                                    send_raw_trailers(stream, &pending, &frames, &log);
                                }
                            });
                            continue;
                        }
                    };
                    let data = raw_grpc_message(&operation);
                    frames.send(h2_frame(
                        H2_HEADERS,
                        H2_FLAG_END_HEADERS,
                        stream,
                        H2_RESPONSE_HEADERS,
                    ))?;
                    frames.send(h2_frame(H2_DATA, 0, stream, &data))?;
                    let Some(trailers) = trailers else {
                        frames.send(Vec::new())?;
                        return Ok(());
                    };
                    match trailers {
                        RawTrailers::Never => {}
                        RawTrailers::OnReset => {
                            pending.lock().unwrap().insert(stream);
                        }
                        RawTrailers::After(delay) => {
                            pending.lock().unwrap().insert(stream);
                            let (pending, frames, log) =
                                (pending.clone(), frames.clone(), log.clone());
                            tokio::spawn(async move {
                                tokio::time::sleep(delay).await;
                                send_raw_trailers(stream, &pending, &frames, &log);
                            });
                        }
                    }
                }
                H2_RST_STREAM => {
                    let code = u32::from_be_bytes(payload[..4].try_into()?);
                    *log.resets.lock().unwrap().entry(code).or_default() += 1;
                    send_raw_trailers(stream, &pending, &frames, &log);
                }
                H2_GOAWAY => {
                    // NO_ERROR is a graceful close, such as of a channel the client dropped.
                    let code = u32::from_be_bytes(payload[4..8].try_into()?);
                    if code != 0 {
                        *log.go_away.lock().unwrap() = Some((code, payload[8..].to_vec()));
                    }
                }
                _ => {}
            }
        }
    }

    fn done_operation() -> Operation {
        let response = GExecuteResponse {
            result: Some(ActionResult {
                execution_metadata: Some(ExecutedActionMetadata::default()),
                ..Default::default()
            }),
            status: Some(Status::default()),
            ..Default::default()
        };
        Operation {
            name: "operations/done".to_owned(),
            done: true,
            result: Some(OpResult::Response(prost_types::Any {
                type_url: "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteResponse"
                    .to_owned(),
                value: response.encode_to_vec(),
            })),
            ..Default::default()
        }
    }

    /// A client whose engine, CAS and action cache are all `address`, over one connection.
    async fn raw_h2_client(address: String, retries: usize) -> anyhow::Result<REClient> {
        raw_h2_client_with(
            address,
            Buck2OssReConfiguration {
                retries: Some(retries),
                ..Default::default()
            },
        )
        .await
    }

    /// `raw_h2_client`, with the rest of its configuration from `opts`.
    async fn raw_h2_client_with(
        address: String,
        opts: Buck2OssReConfiguration,
    ) -> anyhow::Result<REClient> {
        REClientBuilder::build_and_connect(&Buck2OssReConfiguration {
            cas_address: Some(address.clone()),
            engine_address: Some(address.clone()),
            action_cache_address: Some(address),
            tls: Some(false),
            capabilities: Some(false),
            engine_connection_count: Some(1),
            retry_max_delay_ms: Some(10),
            ..opts
        })
        .await
    }

    /// Runs an action and stops reading once its ExecuteResponse is in, as the executor does
    /// (buck2_execute src/re/client.rs, `execute_impl`), which drops the stream.
    async fn execute_raw_h2_action(client: &REClient) -> anyhow::Result<ExecuteResponse> {
        let mut stream = client
            .execute_with_progress(
                &RemoteExecutionMetadata::default(),
                ExecuteRequest {
                    action_digest: TDigest {
                        hash: "ab".repeat(32),
                        size_in_bytes: 1,
                        _dot_dot: (),
                    },
                    ..Default::default()
                },
            )
            .await?;
        while let Some(response) = stream.try_next().await? {
            if let Some(response) = response.execute_response {
                return Ok(response);
            }
        }
        Err(anyhow::anyhow!(
            "the stream ended without an ExecuteResponse"
        ))
    }

    /// A cache that never answers: tonic ends each attempt at the request timeout with CANCELLED
    /// "Timeout expired", each retry goes out on a new connection, and the last one comes back as the DEADLINE_EXCEEDED an executor with
    /// `remote_cache_unavailable_fallback` skips.
    #[tokio::test]
    async fn a_cache_lookup_that_times_out_on_the_client_is_retried_then_deadline_exceeded()
    -> anyhow::Result<()> {
        let requests = Arc::new(AtomicUsize::new(0));
        let (address, log, server) = serve_raw_h2({
            let requests = requests.clone();
            move |_| {
                requests.fetch_add(1, Ordering::SeqCst);
                RawReply::Nothing
            }
        })
        .await?;
        let client = raw_h2_client_with(
            address,
            Buck2OssReConfiguration {
                retries: Some(2),
                grpc_request_timeout_secs: Some(1),
                ..Default::default()
            },
        )
        .await?;
        let connections_before = log.connections.load(Ordering::SeqCst);

        let err = client
            .get_action_result(
                &RemoteExecutionMetadata::default(),
                ActionResultRequest {
                    digest: TDigest {
                        hash: "ab".repeat(32),
                        size_in_bytes: 1,
                        _dot_dot: (),
                    },
                    platform: None,
                    _dot_dot: (),
                },
            )
            .await
            .err()
            .expect("a lookup the cache never answers fails");

        let err = err.downcast_ref::<REClientError>().expect("REClientError");
        assert_eq!(err.code, TCode::DEADLINE_EXCEEDED, "{}", err.message);
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        // Each retry redials rather than waiting on the connection that did not answer.
        assert!(
            log.connections.load(Ordering::SeqCst) - connections_before >= 2,
            "a retry after a timeout reconnects first"
        );
        server.abort();
        Ok(())
    }

    /// The keepalive set through `Buck2OssReConfiguration` reaches channels built with
    /// `connect_with_connector`: a lookup on a connection whose peer stopped answering fails as
    /// a broken connection after the PING goes unanswered, well before the request timeout.
    #[tokio::test]
    async fn keepalive_ends_a_lookup_on_a_connection_that_stopped_answering()
    -> anyhow::Result<()> {
        let requests = Arc::new(AtomicUsize::new(0));
        let (address, log, server) = serve_raw_h2({
            let requests = requests.clone();
            move |_| {
                if requests.fetch_add(1, Ordering::SeqCst) == 0 {
                    RawReply::Silence
                } else {
                    RawReply::Close
                }
            }
        })
        .await?;
        let client = raw_h2_client_with(
            address,
            Buck2OssReConfiguration {
                retries: Some(0),
                grpc_request_timeout_secs: Some(30),
                grpc_keepalive_time_secs: Some(1),
                grpc_keepalive_timeout_secs: Some(1),
                action_cache_connection_count: Some(1),
                ..Default::default()
            },
        )
        .await?;
        let metadata = RemoteExecutionMetadata::default();
        let lookup = || {
            client.get_action_result(
                &metadata,
                ActionResultRequest {
                    digest: TDigest {
                        hash: "ab".repeat(32),
                        size_in_bytes: 1,
                        _dot_dot: (),
                    },
                    platform: None,
                    _dot_dot: (),
                },
            )
        };

        let started = Instant::now();
        let err = tokio::time::timeout(Duration::from_secs(10), lookup())
            .await
            .expect("the lookup on the silent connection ended within 10 s")
            .err()
            .expect("a lookup the server never answers fails");
        let elapsed = started.elapsed();
        assert!(is_broken_connection_error(&err), "{err:#}");
        assert!(
            elapsed < Duration::from_secs(5),
            "ended after {elapsed:?}, not at keepalive interval + timeout (2 s)"
        );

        let connections_after_failure = log.connections.load(Ordering::SeqCst);
        let _ = tokio::time::timeout(Duration::from_secs(10), lookup())
            .await
            .expect("the next lookup ended within 10 s");
        // The silent connection reads nothing, so a second request seen by the server came
        // over another connection.
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        assert!(log.connections.load(Ordering::SeqCst) > connections_after_failure);
        server.abort();
        Ok(())
    }

    /// Without the drain, the 1500 dropped streams provoke h2's GOAWAY ENHANCE_YOUR_CALM
    /// "too_many_internal_resets", and the actions still in flight on the connection fail.
    #[tokio::test]
    async fn finished_operation_streams_end_without_a_reset() -> anyhow::Result<()> {
        const ACTIONS: usize = 1500;
        let (address, log, server) = serve_raw_h2(|_| {
            RawReply::Operation(
                done_operation(),
                RawTrailers::After(Duration::from_millis(200)),
            )
        })
        .await?;
        let client = raw_h2_client(address, 0).await?;

        let executed =
            futures::future::join_all((0..ACTIONS).map(|_| execute_raw_h2_action(&client))).await;
        let _ = tokio::time::timeout(Duration::from_secs(10), async {
            while log.trailers_sent.load(Ordering::SeqCst) < ACTIONS {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        execute_raw_h2_action(&client).await?;

        assert_eq!(*log.go_away.lock().unwrap(), None);
        assert_eq!(*log.resets.lock().unwrap(), HashMap::new());
        executed.into_iter().collect::<anyhow::Result<Vec<_>>>()?;
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn a_finished_stream_the_server_never_ends_is_reset_after_the_drain_timeout()
    -> anyhow::Result<()> {
        let (address, log, server) =
            serve_raw_h2(|_| RawReply::Operation(done_operation(), RawTrailers::Never)).await?;
        let client = raw_h2_client(address, 0).await?;

        let started = Instant::now();
        execute_raw_h2_action(&client).await?;
        // A second action, while the first one's stream is still being drained.
        execute_raw_h2_action(&client).await?;
        assert!(started.elapsed() < OPERATION_STREAM_DRAIN_TIMEOUT);
        assert_eq!(*log.resets.lock().unwrap(), HashMap::new());

        let _ = tokio::time::timeout(OPERATION_STREAM_DRAIN_TIMEOUT * 3, async {
            while log.resets.lock().unwrap().get(&H2_CANCEL) != Some(&2) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert_eq!(*log.resets.lock().unwrap(), HashMap::from([(H2_CANCEL, 2)]));
        assert_eq!(*log.go_away.lock().unwrap(), None);
        server.abort();
        Ok(())
    }

    static STALLED_OPERATION_WAITS: AtomicUsize = AtomicUsize::new(0);

    /// Every stream sends the operation's status, which never changes, and ends.
    fn raw_stalled_operation_reply(body: &[u8]) -> RawReply {
        if WaitExecutionRequest::decode(body).is_ok_and(|request| !request.name.is_empty()) {
            STALLED_OPERATION_WAITS.fetch_add(1, Ordering::SeqCst);
        }
        RawReply::Operation(
            Operation {
                name: "operations/stalled".to_owned(),
                ..Default::default()
            },
            RawTrailers::After(Duration::ZERO),
        )
    }

    #[tokio::test]
    async fn an_operation_whose_stream_keeps_ending_without_progress_fails() -> anyhow::Result<()> {
        let (address, _log, server) = serve_raw_h2(raw_stalled_operation_reply).await?;
        let client = raw_h2_client(address, 2).await?;

        let Err(err) =
            tokio::time::timeout(Duration::from_secs(30), execute_raw_h2_action(&client)).await?
        else {
            panic!("the operation never finishes");
        };

        assert!(
            format!("{err:#}").contains("3 times in a row without progress"),
            "{err:#}"
        );
        assert_eq!(STALLED_OPERATION_WAITS.load(Ordering::SeqCst), 3);
        server.abort();
        Ok(())
    }

    /// The action's Execute stream stays open; WaitExecution finishes it. Every other stream is
    /// dropped after its first message, and its trailers arrive after the client reset it.
    fn raw_go_away_reply(body: &[u8]) -> RawReply {
        let name = WaitExecutionRequest::decode(body)
            .map(|request| request.name)
            .unwrap_or_default();
        match name.as_str() {
            "" => RawReply::Operation(
                Operation {
                    name: "operations/in-flight".to_owned(),
                    ..Default::default()
                },
                RawTrailers::Never,
            ),
            "operations/in-flight" => {
                RawReply::Operation(done_operation(), RawTrailers::After(Duration::ZERO))
            }
            "bystander" => RawReply::Operation(
                Operation {
                    name,
                    ..Default::default()
                },
                RawTrailers::Never,
            ),
            _ => RawReply::Operation(
                Operation {
                    name,
                    ..Default::default()
                },
                RawTrailers::OnReset,
            ),
        }
    }

    /// h2 resets a stream with STREAM_CLOSED when a frame arrives for one it no longer remembers,
    /// and remembers a stream the client dropped for 1 s and only 50 at a time (h2 0.4.15
    /// src/proto/mod.rs:34-41, streams/recv.rs:988-1003). The 1025th such reset on a connection
    /// becomes GOAWAY ENHANCE_YOUR_CALM instead (streams/streams.rs:1641-1660), which fails every
    /// stream on the connection, the action's among them.
    #[tokio::test]
    async fn an_action_in_flight_when_h2_closes_its_connection_is_resumed() -> anyhow::Result<()> {
        let (address, log, server) = serve_raw_h2(raw_go_away_reply).await?;
        let client = raw_h2_client(address, 2).await?;
        let mut action = client
            .execute_with_progress(
                &RemoteExecutionMetadata::default(),
                ExecuteRequest {
                    action_digest: TDigest {
                        hash: "ab".repeat(32),
                        size_in_bytes: 1,
                        _dot_dot: (),
                    },
                    ..Default::default()
                },
            )
            .await?;
        action.try_next().await?;

        // The pool has one channel, so these share the action's connection.
        let execution = client.grpc_clients.execution_client().await?;
        let wait_execution = |name: String| {
            let mut execution = execution.clone();
            async move {
                execution
                    .wait_execution(WaitExecutionRequest { name })
                    .await
                    .map(tonic::Response::into_inner)
            }
        };
        let mut bystander = wait_execution("bystander".to_owned()).await?;
        bystander.message().await?;
        futures::future::join_all((0..1500).map(|i| {
            let stream = wait_execution(format!("operations/{i}"));
            async move { stream.await?.message().await }
        }))
        .await;
        let lost = bystander
            .message()
            .await
            .expect_err("the bystander's connection was closed under it");

        // What a stream in flight sees: RESOURCE_EXHAUSTED, from the GOAWAY its own client sent.
        assert_eq!(lost.code(), tonic::Code::ResourceExhausted, "{lost:?}");
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            while log.go_away.lock().unwrap().is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert_eq!(
            *log.go_away.lock().unwrap(),
            Some((H2_ENHANCE_YOUR_CALM, b"too_many_internal_resets".to_vec()))
        );
        let lost = anyhow::Error::from(lost);
        assert!(is_broken_connection_error(&lost), "{lost:#}");
        assert!(is_retryable_grpc_error(&lost), "{lost:#}");
        assert!(should_retry_execute_after_wait_execution_error(&lost));
        assert!(is_broken_connection_error(&normalize_grpc_error(lost)));

        let (mut events, sink) = buck2_events::create_source_sink_pair();
        let dispatcher = buck2_events::dispatch::EventDispatcher::new(
            buck2_wrapper_common::invocation_id::TraceId::null(),
            buck2_events::daemon_id::DaemonId::null(),
            sink,
        );
        let mut completed = None;
        buck2_events::dispatch::with_dispatcher_async(
            dispatcher,
            tokio::time::timeout(Duration::from_secs(30), async {
                while let Some(response) = action.try_next().await? {
                    if let Some(response) = response.execute_response {
                        completed = Some(response);
                    }
                }
                anyhow::Ok(())
            }),
        )
        .await??;
        assert_eq!(
            completed.map(|response| response.status.code),
            Some(TCode::OK)
        );

        let mut warnings = Vec::new();
        while let Some(event) = events.try_receive() {
            if let buck2_events::Event::Buck(event) = event
                && let buck2_data::buck_event::Data::Instant(buck2_data::InstantEvent {
                    data: Some(buck2_data::instant_event::Data::ConsoleWarning(warning)),
                }) = event.data()
            {
                warnings.push(warning.message.clone());
            }
        }
        assert_eq!(warnings.len(), 1, "{warnings:#?}");
        assert!(
            warnings[0].starts_with("Resuming RE operation `operations/in-flight`")
                && warnings[0].contains("(resume 1/3) on a new connection")
                && warnings[0].contains("too_many_internal_resets"),
            "{}",
            warnings[0]
        );
        server.abort();
        Ok(())
    }

    static LOST_OPERATION_EXECUTES: AtomicUsize = AtomicUsize::new(0);
    static LOST_OPERATION_WAITS: AtomicUsize = AtomicUsize::new(0);

    /// The first Execute loses its connection after the first Operation, and so does every
    /// WaitExecution; the second Execute finishes.
    fn raw_lost_operation_reply(body: &[u8]) -> RawReply {
        if WaitExecutionRequest::decode(body).is_ok_and(|request| !request.name.is_empty()) {
            LOST_OPERATION_WAITS.fetch_add(1, Ordering::SeqCst);
            return RawReply::Close;
        }
        if LOST_OPERATION_EXECUTES.fetch_add(1, Ordering::SeqCst) == 0 {
            RawReply::OperationThenClose(Operation {
                name: "operations/lost".to_owned(),
                ..Default::default()
            })
        } else {
            RawReply::Operation(done_operation(), RawTrailers::After(Duration::ZERO))
        }
    }

    #[tokio::test]
    async fn an_operation_whose_connection_keeps_closing_is_executed_again() -> anyhow::Result<()> {
        let (address, _log, server) = serve_raw_h2(raw_lost_operation_reply).await?;
        let client = raw_h2_client(address, 2).await?;

        let completed = execute_raw_h2_action(&client).await?;

        assert_eq!(completed.status.code, TCode::OK);
        assert_eq!(LOST_OPERATION_EXECUTES.load(Ordering::SeqCst), 2);
        assert_eq!(LOST_OPERATION_WAITS.load(Ordering::SeqCst), 3);
        server.abort();
        Ok(())
    }

    fn staged_operation(name: &str, stage: execution_stage::Value) -> Operation {
        Operation {
            name: name.to_owned(),
            metadata: Some(prost_types::Any {
                type_url:
                    "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteOperationMetadata"
                        .to_owned(),
                value: ExecuteOperationMetadata {
                    stage: stage as i32,
                    ..Default::default()
                }
                .encode_to_vec(),
            }),
            ..Default::default()
        }
    }

    fn queued_operation(name: &str) -> Operation {
        staged_operation(name, execution_stage::Value::Queued)
    }

    /// The Executes and WaitExecutions a raw server has answered.
    #[derive(Default)]
    struct ExecutionCalls {
        executes: AtomicUsize,
        waits: AtomicUsize,
    }

    impl ExecutionCalls {
        /// Counts the request, and returns the number of earlier requests of its kind and the
        /// operation name of a WaitExecution, or None for an Execute.
        fn count(&self, body: &[u8]) -> (usize, Option<String>) {
            match WaitExecutionRequest::decode(body) {
                Ok(request) if !request.name.is_empty() => (
                    self.waits.fetch_add(1, Ordering::SeqCst),
                    Some(request.name),
                ),
                _ => (self.executes.fetch_add(1, Ordering::SeqCst), None),
            }
        }

        fn executes(&self) -> usize {
            self.executes.load(Ordering::SeqCst)
        }

        fn waits(&self) -> usize {
            self.waits.load(Ordering::SeqCst)
        }
    }

    /// A raw server that answers through `reply`, and a client of it whose operations may stay
    /// QUEUED for 1 s.
    async fn serve_queued(
        retries: usize,
        reply: impl Fn(usize, Option<String>) -> RawReply + Send + Sync + 'static,
    ) -> anyhow::Result<(
        REClient,
        Arc<ExecutionCalls>,
        Arc<RawH2Log>,
        tokio::task::JoinHandle<()>,
    )> {
        let calls = Arc::new(ExecutionCalls::default());
        let (address, log, server) = serve_raw_h2({
            let calls = calls.clone();
            move |body| {
                let (earlier, wait) = calls.count(body);
                reply(earlier, wait)
            }
        })
        .await?;
        let client = raw_h2_client_with(
            address,
            Buck2OssReConfiguration {
                retries: Some(retries),
                queued_operation_timeout_secs: Some(1),
                ..Default::default()
            },
        )
        .await?;
        Ok((client, calls, log, server))
    }

    /// Runs an action as `execute_raw_h2_action` does, and returns the console warnings it put
    /// on the event stream.
    async fn execute_raw_h2_action_with_warnings(
        client: &REClient,
    ) -> anyhow::Result<(ExecuteResponse, Vec<String>)> {
        let (mut events, sink) = buck2_events::create_source_sink_pair();
        let dispatcher = buck2_events::dispatch::EventDispatcher::new(
            buck2_wrapper_common::invocation_id::TraceId::null(),
            buck2_events::daemon_id::DaemonId::null(),
            sink,
        );
        let response = buck2_events::dispatch::with_dispatcher_async(
            dispatcher,
            tokio::time::timeout(Duration::from_secs(30), execute_raw_h2_action(client)),
        )
        .await??;
        let mut warnings = Vec::new();
        while let Some(event) = events.try_receive() {
            if let buck2_events::Event::Buck(event) = event
                && let buck2_data::buck_event::Data::Instant(buck2_data::InstantEvent {
                    data: Some(buck2_data::instant_event::Data::ConsoleWarning(warning)),
                }) = event.data()
            {
                warnings.push(warning.message.clone());
            }
        }
        Ok((response, warnings))
    }

    /// The incident of 2026-10-02 on BuildBuddy v2.310.0: the scheduler lost a task it had
    /// accepted, so its Execute stream sent one QUEUED Operation and then nothing for 1h35m.
    #[tokio::test]
    async fn an_operation_that_stays_queued_is_executed_again() -> anyhow::Result<()> {
        let (client, calls, _log, server) = serve_queued(2, |earlier, _| match earlier {
            0 => RawReply::Operation(queued_operation("operations/stuck"), RawTrailers::Never),
            _ => RawReply::Operation(done_operation(), RawTrailers::After(Duration::ZERO)),
        })
        .await?;

        let started = Instant::now();
        let (completed, warnings) = execute_raw_h2_action_with_warnings(&client).await?;

        assert_eq!(completed.status.code, TCode::OK);
        assert!(started.elapsed() >= Duration::from_secs(1));
        assert_eq!((calls.executes(), calls.waits()), (2, 0));
        assert_eq!(
            warnings,
            vec![format!(
                "Executing RE action {}/1 again (re-Execute 1/2): operation `operations/stuck` stayed QUEUED for 1s",
                "ab".repeat(32)
            )]
        );
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn an_operation_that_starts_executing_before_its_deadline_is_not_executed_again()
    -> anyhow::Result<()> {
        let (client, calls, _log, server) = serve_queued(2, |earlier, _| match earlier {
            0 => RawReply::Operations(
                vec![
                    (Duration::ZERO, queued_operation("operations/slow")),
                    (
                        Duration::from_millis(200),
                        staged_operation("operations/slow", execution_stage::Value::Executing),
                    ),
                    (Duration::from_millis(2300), done_operation()),
                ],
                RawTrailers::After(Duration::ZERO),
            ),
            _ => RawReply::Operation(done_operation(), RawTrailers::After(Duration::ZERO)),
        })
        .await?;

        let started = Instant::now();
        let (completed, warnings) = execute_raw_h2_action_with_warnings(&client).await?;

        assert_eq!(completed.status.code, TCode::OK);
        assert!(started.elapsed() >= Duration::from_millis(2500));
        assert_eq!((calls.executes(), calls.waits()), (1, 0));
        assert_eq!(warnings, Vec::<String>::new());
        server.abort();
        Ok(())
    }

    /// The operation executed again because it stayed QUEUED is not dropped: when it is the one
    /// an executor picks up first, the action takes its result.
    #[tokio::test]
    async fn an_operation_that_stayed_queued_still_finishes_the_action_if_it_starts_first()
    -> anyhow::Result<()> {
        let (client, calls, log, server) = serve_queued(2, |earlier, _| match earlier {
            0 => RawReply::Operations(
                vec![
                    (Duration::ZERO, queued_operation("operations/first")),
                    (Duration::from_millis(1500), done_operation()),
                ],
                RawTrailers::After(Duration::ZERO),
            ),
            _ => RawReply::Operation(queued_operation("operations/second"), RawTrailers::Never),
        })
        .await?;

        let (completed, warnings) = execute_raw_h2_action_with_warnings(&client).await?;

        assert_eq!(completed.status.code, TCode::OK);
        assert_eq!((calls.executes(), calls.waits()), (2, 0));
        assert_eq!(warnings.len(), 1, "{warnings:#?}");
        // The second Execute's stream, which never finishes, is dropped with the action.
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            while log.resets.lock().unwrap().get(&H2_CANCEL) != Some(&1) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert_eq!(*log.resets.lock().unwrap(), HashMap::from([(H2_CANCEL, 1)]));
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn an_operation_that_stays_queued_past_the_retries_is_still_waited_for()
    -> anyhow::Result<()> {
        let (client, calls, _log, server) = serve_queued(0, |_, _| {
            RawReply::Operations(
                vec![
                    (Duration::ZERO, queued_operation("operations/backlog")),
                    (Duration::from_millis(1500), done_operation()),
                ],
                RawTrailers::After(Duration::ZERO),
            )
        })
        .await?;

        let (completed, warnings) = execute_raw_h2_action_with_warnings(&client).await?;

        assert_eq!(completed.status.code, TCode::OK);
        assert_eq!((calls.executes(), calls.waits()), (1, 0));
        assert_eq!(
            warnings,
            vec![format!(
                "RE operation `operations/backlog` of action {}/1 stayed QUEUED for 1s and its re-Executes are spent (0/0); still waiting for it",
                "ab".repeat(32)
            )]
        );
        server.abort();
        Ok(())
    }

    /// A resumption repeats the operation's status, which is not progress: the clock that started
    /// with the Execute keeps running through it.
    #[tokio::test]
    async fn resuming_an_operation_that_stays_queued_does_not_restart_its_clock()
    -> anyhow::Result<()> {
        let (client, calls, _log, server) =
            serve_queued(2, |earlier, wait| match (earlier, wait) {
                (0, None) => RawReply::Operations(
                    vec![(Duration::ZERO, queued_operation("operations/stuck"))],
                    RawTrailers::After(Duration::from_millis(800)),
                ),
                (_, Some(name)) => RawReply::Operation(queued_operation(&name), RawTrailers::Never),
                _ => RawReply::Operation(done_operation(), RawTrailers::After(Duration::ZERO)),
            })
            .await?;

        let started = Instant::now();
        let (completed, warnings) = execute_raw_h2_action_with_warnings(&client).await?;

        assert_eq!(completed.status.code, TCode::OK);
        // The Execute's clock ends by 1.25 s with its jitter, and one restarted by the resumption
        // at 0.8 s could not end before 1.8 s, so the bound sits between them with room for a
        // loaded runner on either side.
        assert!(
            started.elapsed() < Duration::from_millis(1650),
            "{:?}",
            started.elapsed()
        );
        assert_eq!((calls.executes(), calls.waits()), (2, 1));
        assert_eq!(warnings.len(), 1, "{warnings:#?}");
        assert!(
            warnings[0].contains("stayed QUEUED for 1s"),
            "{}",
            warnings[0]
        );
        server.abort();
        Ok(())
    }

    /// BuildBuddy answers a WaitExecution with the last status published for the execution, and
    /// nothing is published for one no executor has claimed, so not even the response headers
    /// come back.
    #[tokio::test]
    async fn a_resumption_of_an_operation_that_stays_queued_that_never_answers_is_abandoned()
    -> anyhow::Result<()> {
        let (client, calls, _log, server) =
            serve_queued(2, |earlier, wait| match (earlier, wait) {
                (0, None) => RawReply::Operations(
                    vec![(Duration::ZERO, queued_operation("operations/stuck"))],
                    RawTrailers::After(Duration::from_millis(400)),
                ),
                (_, Some(_)) => RawReply::Nothing,
                _ => RawReply::Operation(done_operation(), RawTrailers::After(Duration::ZERO)),
            })
            .await?;

        let (completed, warnings) = execute_raw_h2_action_with_warnings(&client).await?;

        assert_eq!(completed.status.code, TCode::OK);
        assert_eq!((calls.executes(), calls.waits()), (2, 1));
        assert_eq!(
            warnings,
            vec![format!(
                "Executing RE action {}/1 again (re-Execute 1/2): operation `operations/stuck` stayed QUEUED for 1s",
                "ab".repeat(32)
            )]
        );
        server.abort();
        Ok(())
    }

    /// An operation that stayed QUEUED and was executed again may still finish, with an error once
    /// the scheduler recovers enough to fail it. The later Execute's operation is still queued and
    /// may well succeed, so the failure is not the action's.
    #[tokio::test]
    async fn an_operation_that_stayed_queued_and_then_fails_does_not_replace_the_later_one()
    -> anyhow::Result<()> {
        let (client, calls, _log, server) = serve_queued(2, |earlier, _| match earlier {
            0 => RawReply::Operations(
                vec![
                    (Duration::ZERO, queued_operation("operations/stuck")),
                    (
                        Duration::from_millis(1500),
                        Operation {
                            name: "operations/stuck".to_owned(),
                            done: true,
                            result: Some(OpResult::Error(Status {
                                code: TCode::UNAVAILABLE.0,
                                message: "failed to dispatch".to_owned(),
                                ..Default::default()
                            })),
                            ..Default::default()
                        },
                    ),
                ],
                RawTrailers::After(Duration::ZERO),
            ),
            1 => RawReply::Operations(
                vec![
                    (Duration::ZERO, queued_operation("operations/fresh")),
                    (Duration::from_millis(1000), done_operation()),
                ],
                RawTrailers::After(Duration::ZERO),
            ),
            _ => RawReply::Operation(done_operation(), RawTrailers::After(Duration::ZERO)),
        })
        .await?;

        let (completed, warnings) = execute_raw_h2_action_with_warnings(&client).await?;

        assert_eq!(completed.status.code, TCode::OK);
        assert_eq!((calls.executes(), calls.waits()), (2, 0));
        assert_eq!(warnings.len(), 1, "{warnings:#?}");
        assert!(
            warnings[0].contains("stayed QUEUED for 1s"),
            "{}",
            warnings[0]
        );
        server.abort();
        Ok(())
    }

    /// BuildBuddy's executor sends CACHE_CHECK when it claims a task, after the app's QUEUED, and
    /// gets a runner before it sends EXECUTING.
    #[tokio::test]
    async fn an_operation_claimed_in_cache_check_before_its_deadline_is_not_executed_again()
    -> anyhow::Result<()> {
        let (client, calls, _log, server) = serve_queued(2, |_, _| {
            RawReply::Operations(
                vec![
                    (Duration::ZERO, queued_operation("operations/claimed")),
                    (
                        Duration::from_millis(200),
                        staged_operation("operations/claimed", execution_stage::Value::CacheCheck),
                    ),
                    (Duration::from_millis(1300), done_operation()),
                ],
                RawTrailers::After(Duration::ZERO),
            )
        })
        .await?;

        let (completed, warnings) = execute_raw_h2_action_with_warnings(&client).await?;

        assert_eq!(completed.status.code, TCode::OK);
        assert_eq!((calls.executes(), calls.waits()), (1, 0));
        assert_eq!(warnings, Vec::<String>::new());
        server.abort();
        Ok(())
    }

    /// A resumption of a claimed operation runs without a clock, even when it has not said yet
    /// that the operation is executing.
    #[tokio::test]
    async fn resuming_an_executing_operation_does_not_start_its_clock_again() -> anyhow::Result<()>
    {
        let (client, calls, _log, server) =
            serve_queued(2, |earlier, wait| match (earlier, wait) {
                (0, None) => RawReply::Operations(
                    vec![
                        (Duration::ZERO, queued_operation("operations/running")),
                        (
                            Duration::from_millis(100),
                            staged_operation("operations/running", execution_stage::Value::Executing),
                        ),
                    ],
                    RawTrailers::After(Duration::from_millis(200)),
                ),
                (_, Some(_)) => RawReply::Operations(
                    vec![(Duration::from_millis(1500), done_operation())],
                    RawTrailers::After(Duration::ZERO),
                ),
                _ => RawReply::Operation(done_operation(), RawTrailers::After(Duration::ZERO)),
            })
            .await?;

        let (completed, warnings) = execute_raw_h2_action_with_warnings(&client).await?;

        assert_eq!(completed.status.code, TCode::OK);
        assert_eq!((calls.executes(), calls.waits()), (1, 1));
        assert_eq!(warnings, Vec::<String>::new());
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn an_unset_queued_operation_timeout_is_fifteen_minutes() -> anyhow::Result<()> {
        let (address, _log, server) = serve_raw_h2(|_| RawReply::Close).await?;
        let client = raw_h2_client(address, 0).await?;

        assert_eq!(
            client.runtime_opts.queued_operation_timeout,
            Duration::from_secs(900)
        );
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn queued_deadline_doubles_with_each_reexecute_and_zero_turns_it_off() {
        let mut deadline = QueuedDeadline::new(Duration::from_secs(900));
        assert!(deadline.at.is_some());
        assert!(
            (Duration::from_secs(900)..=Duration::from_secs(1125)).contains(&deadline.wait),
            "{:?}",
            deadline.wait
        );
        deadline.reexecutes = 2;
        assert_eq!(deadline.period(), Duration::from_secs(3600));
        deadline.reexecutes = u32::MAX;
        assert_eq!(deadline.period(), Duration::from_secs(900 << 16));

        assert!(QueuedDeadline::new(Duration::ZERO).at.is_none());
        assert!(QueuedDeadline::new(Duration::MAX).at.is_none());
    }

    /// A request to the raw server of the stall tests, told apart by which message its body
    /// decodes to with the fields that message needs.
    enum StallRequest {
        Wait(String),
        Execute(Digest),
        Read(Vec<Digest>),
        Update(Vec<(Digest, Vec<u8>)>),
    }

    fn stall_request(body: &[u8]) -> StallRequest {
        if let Ok(request) = WaitExecutionRequest::decode(body)
            && !request.name.is_empty()
        {
            return StallRequest::Wait(request.name);
        }
        if let Ok(request) = BatchUpdateBlobsRequest::decode(body)
            && !request.requests.is_empty()
        {
            return StallRequest::Update(
                request
                    .requests
                    .into_iter()
                    .map(|blob| (blob.digest.unwrap_or_default(), blob.data))
                    .collect(),
            );
        }
        if let Ok(request) = BatchReadBlobsRequest::decode(body)
            && !request.digests.is_empty()
        {
            return StallRequest::Read(request.digests);
        }
        match GExecuteRequest::decode(body) {
            Ok(GExecuteRequest {
                action_digest: Some(digest),
                ..
            }) => StallRequest::Execute(digest),
            _ => panic!("the raw server got a request it does not know"),
        }
    }

    /// What the raw server of the stall tests has been asked.
    #[derive(Default)]
    struct StallCalls {
        /// The action digest of each Execute, in order.
        executes: Mutex<Vec<Digest>>,
        waits: AtomicUsize,
        /// The blobs uploaded with BatchUpdateBlobs.
        uploads: Mutex<Vec<(Digest, Vec<u8>)>>,
    }

    /// The Action the raw server keeps under every digest it is asked for.
    fn stalling_action() -> Action {
        Action {
            command_digest: Some(Digest {
                hash: "cd".repeat(32),
                size_bytes: 3,
            }),
            input_root_digest: Some(Digest {
                hash: "ef".repeat(32),
                size_bytes: 4,
            }),
            ..Default::default()
        }
    }

    /// An EXECUTING Operation whose metadata differs with `ping`, as BuildBuddy's periodic
    /// progress updates do by their timestamp.
    fn executing_ping(name: &str, ping: u32) -> Operation {
        let mut operation = staged_operation(name, execution_stage::Value::Executing);
        operation.metadata = Some(prost_types::Any {
            type_url:
                "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteOperationMetadata"
                    .to_owned(),
            value: ExecuteOperationMetadata {
                stage: execution_stage::Value::Executing as i32,
                partial_execution_metadata: Some(ExecutedActionMetadata {
                    worker: format!("ping {ping}"),
                    ..Default::default()
                }),
                ..Default::default()
            }
            .encode_to_vec(),
        });
        operation
    }

    /// A finished Operation whose command exited with `exit_code`, with `status` as its
    /// ExecuteResponse's status.
    fn finished_operation(exit_code: i32, status: Status) -> Operation {
        let response = GExecuteResponse {
            result: Some(ActionResult {
                exit_code,
                execution_metadata: Some(ExecutedActionMetadata::default()),
                ..Default::default()
            }),
            status: Some(status),
            ..Default::default()
        };
        Operation {
            name: "operations/finished".to_owned(),
            done: true,
            result: Some(OpResult::Response(prost_types::Any {
                type_url: "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteResponse"
                    .to_owned(),
                value: response.encode_to_vec(),
            })),
            ..Default::default()
        }
    }

    /// A raw server that keeps `stalling_action` in its CAS, stores what is uploaded, and answers
    /// the nth Execute or WaitExecution through `reply`; and a client of it whose claimed
    /// operations may make no progress for 1 s.
    async fn serve_stalling(
        reply: impl Fn(usize, Option<String>) -> RawReply + Send + Sync + 'static,
    ) -> anyhow::Result<(REClient, Arc<StallCalls>, tokio::task::JoinHandle<()>)> {
        let calls = Arc::new(StallCalls::default());
        let (address, _log, server) = serve_raw_h2({
            let calls = calls.clone();
            move |body| match stall_request(body) {
                StallRequest::Wait(name) => {
                    let earlier = calls.waits.fetch_add(1, Ordering::SeqCst);
                    reply(earlier, Some(name))
                }
                StallRequest::Execute(digest) => {
                    let mut executes = calls.executes.lock().unwrap();
                    executes.push(digest);
                    reply(executes.len() - 1, None)
                }
                StallRequest::Read(digests) => RawReply::Unary(
                    BatchReadBlobsResponse {
                        responses: digests
                            .into_iter()
                            .map(|digest| batch_read_blobs_response::Response {
                                digest: Some(digest),
                                data: stalling_action().encode_to_vec(),
                                status: Some(Status::default()),
                                ..Default::default()
                            })
                            .collect(),
                    }
                    .encode_to_vec(),
                ),
                StallRequest::Update(blobs) => {
                    let responses = blobs
                        .iter()
                        .map(|(digest, _)| batch_update_blobs_response::Response {
                            digest: Some(digest.clone()),
                            status: Some(Status::default()),
                        })
                        .collect();
                    calls.uploads.lock().unwrap().extend(blobs);
                    RawReply::Unary(BatchUpdateBlobsResponse { responses }.encode_to_vec())
                }
            }
        })
        .await?;
        let client = raw_h2_client_with(
            address,
            Buck2OssReConfiguration {
                retries: Some(2),
                stalled_operation_timeout_secs: Some(1),
                ..Default::default()
            },
        )
        .await?;
        Ok((client, calls, server))
    }

    /// Runs an action as `execute_raw_h2_action` does, and returns its outcome with the console
    /// warnings it put on the event stream.
    async fn execute_stalling_action(
        client: &REClient,
    ) -> (anyhow::Result<ExecuteResponse>, Vec<String>) {
        let (mut events, sink) = buck2_events::create_source_sink_pair();
        let dispatcher = buck2_events::dispatch::EventDispatcher::new(
            buck2_wrapper_common::invocation_id::TraceId::null(),
            buck2_events::daemon_id::DaemonId::null(),
            sink,
        );
        let outcome = buck2_events::dispatch::with_dispatcher_async(
            dispatcher,
            tokio::time::timeout(Duration::from_secs(20), execute_raw_h2_action(client)),
        )
        .await
        .map_err(anyhow::Error::from)
        .and_then(|outcome| outcome);
        let mut warnings = Vec::new();
        while let Some(event) = events.try_receive() {
            if let buck2_events::Event::Buck(event) = event
                && let buck2_data::buck_event::Data::Instant(buck2_data::InstantEvent {
                    data: Some(buck2_data::instant_event::Data::ConsoleWarning(warning)),
                }) = event.data()
            {
                warnings.push(warning.message.clone());
            }
        }
        (outcome, warnings)
    }

    /// The Action the client uploaded, and the digest the server stores it under, which must be
    /// the digest of the second Execute.
    fn uploaded_action(calls: &StallCalls) -> (Action, Digest) {
        let uploads = calls.uploads.lock().unwrap();
        assert_eq!(uploads.len(), 1, "one Action is uploaded");
        let (digest, data) = &uploads[0];
        let expected = tdigest_to(
            digest_blob(data, digest_function::Value::Sha256).expect("a SHA-256 digest"),
        );
        assert_eq!(
            digest, &expected,
            "the upload is stored under its own digest"
        );
        (
            Action::decode(&data[..]).expect("an Action"),
            digest.clone(),
        )
    }

    /// The incident of 2026-10-03 on BuildBuddy v2.310.0: an execution that had reached
    /// EXECUTING lost its task, and its failure was never published, so the stream that every
    /// merged request was reading said nothing more for over an hour.
    #[tokio::test]
    async fn an_executing_operation_that_stalls_is_executed_again_uncached() -> anyhow::Result<()> {
        let (client, calls, server) = serve_stalling(|earlier, _| match earlier {
            0 => RawReply::Operations(
                vec![
                    (Duration::ZERO, queued_operation("operations/stuck")),
                    (Duration::ZERO, executing_ping("operations/stuck", 0)),
                ],
                RawTrailers::Never,
            ),
            _ => RawReply::Operation(done_operation(), RawTrailers::After(Duration::ZERO)),
        })
        .await?;

        let started = Instant::now();
        let (completed, warnings) = execute_stalling_action(&client).await;

        assert_eq!(completed?.status.code, TCode::OK);
        assert!(started.elapsed() >= Duration::from_secs(1));
        let (action, uploaded) = uploaded_action(&calls);
        assert!(action.do_not_cache);
        assert_eq!(
            Action {
                do_not_cache: false,
                ..action
            },
            stalling_action()
        );
        let original = Digest {
            hash: "ab".repeat(32),
            size_bytes: 1,
        };
        assert_eq!(
            *calls.executes.lock().unwrap(),
            vec![original, uploaded.clone()]
        );
        assert_eq!(calls.waits.load(Ordering::SeqCst), 0);
        assert_eq!(
            warnings,
            vec![format!(
                "Executing RE action {}/1 again as action {}/{} with do_not_cache set: its operation `operations/stuck` made no progress for 1s. The server does not merge this Execute into the stalled operation, and does not cache its result",
                "ab".repeat(32),
                uploaded.hash,
                uploaded.size_bytes,
            )]
        );
        server.abort();
        Ok(())
    }

    /// A WaitExecution of a claimed operation replays its last status (BuildBuddy v2.310.0
    /// execution_server.go:1286-1293), which is not progress, so a stream that keeps being cut and
    /// resumed onto a stalled operation does not hold the action forever.
    #[tokio::test]
    async fn resuming_a_stalled_operation_does_not_restart_its_clock() -> anyhow::Result<()> {
        let (client, calls, server) = serve_stalling(|earlier, wait| match (earlier, wait) {
            (0, None) => RawReply::Operations(
                vec![(Duration::ZERO, executing_ping("operations/stuck", 0))],
                RawTrailers::After(Duration::from_millis(600)),
            ),
            (_, Some(name)) => RawReply::Operation(executing_ping(&name, 0), RawTrailers::Never),
            _ => RawReply::Operation(done_operation(), RawTrailers::After(Duration::ZERO)),
        })
        .await?;

        let started = Instant::now();
        let (completed, warnings) = execute_stalling_action(&client).await;

        assert_eq!(completed?.status.code, TCode::OK);
        // A clock restarted by the resumption at 0.6 s would not end before 1.6 s.
        assert!(started.elapsed() < Duration::from_millis(1500));
        assert_eq!(calls.waits.load(Ordering::SeqCst), 1);
        assert_eq!(calls.executes.lock().unwrap().len(), 2);
        assert!(uploaded_action(&calls).0.do_not_cache);
        assert_eq!(warnings.len(), 1, "{warnings:#?}");
        server.abort();
        Ok(())
    }

    /// An action that runs longer than the stall timeout is not cut while its executor keeps
    /// sending progress updates.
    #[tokio::test]
    async fn an_executing_operation_that_keeps_making_progress_is_not_executed_again()
    -> anyhow::Result<()> {
        let (client, calls, server) = serve_stalling(|earlier, _| match earlier {
            0 => RawReply::Operations(
                (0..6)
                    .map(|ping| {
                        (
                            Duration::from_millis(if ping == 0 { 0 } else { 500 }),
                            executing_ping("operations/long", ping),
                        )
                    })
                    .chain([(Duration::from_millis(500), done_operation())])
                    .collect(),
                RawTrailers::After(Duration::ZERO),
            ),
            _ => RawReply::Operation(done_operation(), RawTrailers::After(Duration::ZERO)),
        })
        .await?;

        let started = Instant::now();
        let (completed, warnings) = execute_stalling_action(&client).await;

        assert_eq!(completed?.status.code, TCode::OK);
        assert!(started.elapsed() >= Duration::from_secs(3));
        assert_eq!(calls.executes.lock().unwrap().len(), 1);
        assert!(calls.uploads.lock().unwrap().is_empty());
        assert_eq!(warnings, Vec::<String>::new());
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn an_action_that_stalls_twice_fails() -> anyhow::Result<()> {
        let (client, calls, server) = serve_stalling(|_, _| {
            RawReply::Operations(
                vec![(Duration::ZERO, executing_ping("operations/stuck", 0))],
                RawTrailers::Never,
            )
        })
        .await?;

        let (completed, warnings) = execute_stalling_action(&client).await;

        let Err(err) = completed else {
            panic!("the action finished");
        };
        let message = format!(
            "RE operation `operations/stuck` of action {}/{} made no progress for 1s, after the action had already been executed again once because an earlier operation made none; failing the action instead of waiting on it",
            uploaded_action(&calls).1.hash,
            uploaded_action(&calls).1.size_bytes,
        );
        assert_eq!(
            err.downcast_ref::<REClientError>()
                .map(|err| (err.code, err.message.clone())),
            Some((TCode::DEADLINE_EXCEEDED, message.clone())),
            "{err:#}"
        );
        assert_eq!(calls.executes.lock().unwrap().len(), 2);
        assert_eq!(warnings.len(), 2, "{warnings:#?}");
        assert_eq!(warnings[1], message);
        server.abort();
        Ok(())
    }

    /// The re-Execute's own answer is the action's: a command that exits non-zero, or an error
    /// status, is returned as it is rather than retried for having followed a stall.
    #[tokio::test]
    async fn the_answer_to_a_stalled_actions_reexecute_is_not_retried() -> anyhow::Result<()> {
        for (finished, exit_code, status_code) in [
            (finished_operation(1, Status::default()), Some(1), None),
            (
                finished_operation(
                    0,
                    Status {
                        code: TCode::INVALID_ARGUMENT.0,
                        message: "bad action".to_owned(),
                        ..Default::default()
                    },
                ),
                None,
                Some(TCode::INVALID_ARGUMENT),
            ),
        ] {
            let (client, calls, server) = serve_stalling(move |earlier, _| match earlier {
                0 => RawReply::Operations(
                    vec![(Duration::ZERO, executing_ping("operations/stuck", 0))],
                    RawTrailers::Never,
                ),
                _ => RawReply::Operation(finished.clone(), RawTrailers::After(Duration::ZERO)),
            })
            .await?;

            let (completed, warnings) = execute_stalling_action(&client).await;

            match completed {
                Ok(response) => {
                    assert_eq!(Some(response.action_result.exit_code), exit_code)
                }
                Err(err) => assert_eq!(
                    err.downcast_ref::<REClientError>().map(|err| err.code),
                    status_code,
                    "{err:#}"
                ),
            }
            assert_eq!(calls.executes.lock().unwrap().len(), 2);
            assert_eq!(warnings.len(), 1, "{warnings:#?}");
            server.abort();
        }
        Ok(())
    }

    #[tokio::test]
    async fn an_unset_stalled_operation_timeout_is_ten_minutes() -> anyhow::Result<()> {
        let (address, _log, server) = serve_raw_h2(|_| RawReply::Close).await?;
        let client = raw_h2_client(address, 0).await?;

        assert_eq!(
            client.runtime_opts.stalled_operation_timeout,
            Duration::from_secs(600)
        );
        let mut off = StallDeadline::new(Duration::ZERO);
        off.observe(vec![1]);
        assert!(off.at.is_none());
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_reconnects_of_the_execution_pool_dial_each_channel_once()
    -> anyhow::Result<()> {
        let (address, log, server) = serve_raw_h2(|_| {
            RawReply::Operation(done_operation(), RawTrailers::After(Duration::ZERO))
        })
        .await?;
        let client = REClientBuilder::build_and_connect(&Buck2OssReConfiguration {
            cas_address: Some(address.clone()),
            engine_address: Some(address.clone()),
            action_cache_address: Some(address),
            tls: Some(false),
            capabilities: Some(false),
            engine_connection_count: Some(4),
            ..Default::default()
        })
        .await?;
        let settled = || async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            log.connections.load(Ordering::SeqCst)
        };
        let before = settled().await;

        futures::future::join_all((0..50).map(|_| {
            client
                .grpc_clients
                .reconnect_after_broken_connection(GrpcClientKind::Execution)
        }))
        .await;

        assert_eq!(settled().await, before + 4);
        execute_raw_h2_action(&client).await?;
        server.abort();
        Ok(())
    }

    /// A client whose action cache is `action_cache` over `opts.action_cache_connection_count`
    /// connections, and whose other services are a server that answers nothing, so every
    /// connection `action_cache` accepts is one of the pool's.
    async fn action_cache_pool_client(
        action_cache: String,
        opts: Buck2OssReConfiguration,
    ) -> anyhow::Result<(REClient, tokio::task::JoinHandle<()>)> {
        let (others, _log, others_server) = serve_raw_h2(|_| RawReply::Close).await?;
        let client = REClientBuilder::build_and_connect(&Buck2OssReConfiguration {
            cas_address: Some(others.clone()),
            engine_address: Some(others),
            action_cache_address: Some(action_cache),
            tls: Some(false),
            capabilities: Some(false),
            engine_connection_count: Some(1),
            retry_max_delay_ms: Some(10),
            ..opts
        })
        .await?;
        Ok((client, others_server))
    }

    fn raw_action_result_reply() -> RawReply {
        RawReply::Unary(
            ActionResult {
                execution_metadata: Some(ExecutedActionMetadata::default()),
                ..Default::default()
            }
            .encode_to_vec(),
        )
    }

    async fn raw_h2_lookup(client: &REClient) -> anyhow::Result<ActionResultResponse> {
        client
            .get_action_result(
                &RemoteExecutionMetadata::default(),
                ActionResultRequest {
                    digest: TDigest {
                        hash: "ab".repeat(32),
                        size_in_bytes: 1,
                        _dot_dot: (),
                    },
                    platform: None,
                    _dot_dot: (),
                },
            )
            .await
    }

    #[tokio::test]
    async fn action_cache_lookups_take_the_pooled_connections_in_turn() -> anyhow::Result<()> {
        let (address, log, server) =
            serve_raw_h2_by_connection(|_, _| raw_action_result_reply()).await?;
        let (client, others_server) = action_cache_pool_client(
            address,
            Buck2OssReConfiguration {
                action_cache_connection_count: Some(4),
                ..Default::default()
            },
        )
        .await?;

        for _ in 0..8 {
            raw_h2_lookup(&client).await?;
        }

        assert_eq!(log.connections.load(Ordering::SeqCst), 4);
        assert_eq!(
            *log.requests_by_connection.lock().unwrap(),
            (0..4)
                .map(|connection| (connection, 2))
                .collect::<HashMap<_, _>>()
        );
        server.abort();
        others_server.abort();
        Ok(())
    }

    /// One pooled connection stops answering while it still acknowledges PINGs, so keepalive
    /// cannot tell, as the action cache connection did for 96 s on 2026-10-03. The lookups on
    /// the other connections are answered at once. Each lookup on the stalled one times out,
    /// is retried on another connection and succeeds, and only the stalled connection is
    /// redialed.
    #[tokio::test]
    async fn a_stalled_action_cache_connection_delays_only_its_own_lookups() -> anyhow::Result<()> {
        let (address, log, server) = serve_raw_h2_by_connection(|connection, _| {
            if connection == 0 {
                RawReply::Nothing
            } else {
                raw_action_result_reply()
            }
        })
        .await?;
        let (client, others_server) = action_cache_pool_client(
            address,
            Buck2OssReConfiguration {
                action_cache_connection_count: Some(4),
                grpc_request_timeout_secs: Some(2),
                retries: Some(1),
                ..Default::default()
            },
        )
        .await?;

        let started = Instant::now();
        let lookups = futures::future::join_all((0..8).map(|_| async {
            let result = raw_h2_lookup(&client).await;
            (result, started.elapsed())
        }))
        .await;

        for (result, _) in &lookups {
            if let Err(err) = result {
                panic!("every lookup succeeds, at the latest on its retry: {err:#}");
            }
        }
        let answered_before_the_timeout = lookups
            .iter()
            .filter(|(_, elapsed)| *elapsed < Duration::from_secs(1))
            .count();
        assert_eq!(
            answered_before_the_timeout,
            6,
            "{:?}",
            lookups
                .iter()
                .map(|(_, elapsed)| elapsed)
                .collect::<Vec<_>>()
        );
        let requests = log.requests_by_connection.lock().unwrap().clone();
        assert_eq!(requests.get(&0), Some(&2), "{requests:?}");
        assert_eq!(requests.values().sum::<usize>(), 10, "{requests:?}");
        assert_eq!(
            log.connections.load(Ordering::SeqCst),
            5,
            "the stalled connection is redialed once, and the others are kept"
        );
        server.abort();
        others_server.abort();
        Ok(())
    }

    #[test]
    fn test_validate_batch_read_blobs_response_checks_digests() -> anyhow::Result<()> {
        let digest1 = tdigest_to(test_digest("aa", 1));
        let digest2 = tdigest_to(test_digest("bb", 2));

        validate_batch_read_blobs_response_digests(
            &[digest1.clone(), digest2.clone()],
            &BatchReadBlobsResponse {
                responses: vec![
                    batch_read_blobs_response::Response {
                        digest: Some(digest2.clone()),
                        status: Some(Status::default()),
                        data: Vec::new(),
                        ..Default::default()
                    },
                    batch_read_blobs_response::Response {
                        digest: Some(digest1.clone()),
                        status: Some(Status::default()),
                        data: Vec::new(),
                        ..Default::default()
                    },
                ],
            },
        )?;

        let missing = validate_batch_read_blobs_response_digests(
            &[digest1.clone(), digest2.clone()],
            &BatchReadBlobsResponse {
                responses: vec![batch_read_blobs_response::Response {
                    digest: Some(digest1.clone()),
                    status: Some(Status::default()),
                    data: Vec::new(),
                    ..Default::default()
                }],
            },
        )
        .unwrap_err()
        .to_string();
        assert!(missing.contains("missing digest"));

        let unexpected = validate_batch_read_blobs_response_digests(
            &[digest1],
            &BatchReadBlobsResponse {
                responses: vec![batch_read_blobs_response::Response {
                    digest: Some(digest2),
                    status: Some(Status::default()),
                    data: Vec::new(),
                    ..Default::default()
                }],
            },
        )
        .unwrap_err()
        .to_string();
        assert!(unexpected.contains("unexpected digest"));

        Ok(())
    }

    #[test]
    fn test_validate_find_missing_blobs_response_checks_digests() -> anyhow::Result<()> {
        let digest1 = tdigest_to(test_digest("aa", 1));
        let digest2 = tdigest_to(test_digest("bb", 2));

        validate_find_missing_blobs_response_digests(
            &[digest1.clone(), digest2.clone()],
            &FindMissingBlobsResponse {
                missing_blob_digests: vec![digest2.clone()],
            },
        )?;

        validate_find_missing_blobs_response_digests(
            &[digest1.clone(), digest2.clone()],
            &FindMissingBlobsResponse {
                missing_blob_digests: Vec::new(),
            },
        )?;

        let unexpected = validate_find_missing_blobs_response_digests(
            &[digest1],
            &FindMissingBlobsResponse {
                missing_blob_digests: vec![digest2.clone()],
            },
        )
        .unwrap_err()
        .to_string();
        assert!(unexpected.contains("unexpected digest"));

        let duplicate = validate_find_missing_blobs_response_digests(
            std::slice::from_ref(&digest2),
            &FindMissingBlobsResponse {
                missing_blob_digests: vec![digest2.clone(), digest2.clone()],
            },
        )
        .unwrap_err()
        .to_string();
        assert!(duplicate.contains("unexpected digest"));

        Ok(())
    }

    /// What `FakeCas` does with the next BatchUpdateBlobs call.
    enum FakeBatchUpdate {
        Fail(tonic::Status),
        /// Never answers, so the client's request timeout ends the call.
        Silence,
        /// Answers every blob with this code.
        BlobStatus(Code),
    }

    #[derive(Default)]
    struct FakeCasState {
        stored: Mutex<HashSet<String>>,
        /// How many FindMissingBlobs calls asked about each hash.
        asked: Mutex<HashMap<String, usize>>,
        /// How many BatchUpdateBlobs calls carried each hash.
        uploaded: Mutex<HashMap<String, usize>>,
        batch_update_calls: AtomicUsize,
        /// How long each FindMissingBlobs and BatchUpdateBlobs call takes to answer.
        delay: Duration,
        script: Mutex<VecDeque<FakeBatchUpdate>>,
    }

    impl FakeCasState {
        fn asked(&self, digest: &TDigest) -> usize {
            self.asked
                .lock()
                .unwrap()
                .get(&digest.hash)
                .copied()
                .unwrap_or(0)
        }

        fn uploaded(&self, digest: &TDigest) -> usize {
            self.uploaded
                .lock()
                .unwrap()
                .get(&digest.hash)
                .copied()
                .unwrap_or(0)
        }
    }

    /// A CAS that stores what BatchUpdateBlobs sends it and counts, per digest, the calls that
    /// asked about it and the calls that uploaded it.
    struct FakeCas(Arc<FakeCasState>);

    #[tonic::async_trait]
    impl re_grpc_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorage
        for FakeCas
    {
        type GetTreeStream = futures::stream::BoxStream<
            'static,
            Result<re_grpc_proto::build::bazel::remote::execution::v2::GetTreeResponse, tonic::Status>,
        >;

        async fn find_missing_blobs(
            &self,
            request: tonic::Request<FindMissingBlobsRequest>,
        ) -> Result<tonic::Response<FindMissingBlobsResponse>, tonic::Status> {
            let state = &self.0;
            let digests = request.into_inner().blob_digests;
            for digest in &digests {
                *state.asked.lock().unwrap().entry(digest.hash.clone()).or_default() += 1;
            }
            tokio::time::sleep(state.delay).await;
            let stored = state.stored.lock().unwrap();
            Ok(tonic::Response::new(FindMissingBlobsResponse {
                missing_blob_digests: digests
                    .into_iter()
                    .filter(|digest| !stored.contains(&digest.hash))
                    .collect(),
            }))
        }

        async fn batch_update_blobs(
            &self,
            request: tonic::Request<BatchUpdateBlobsRequest>,
        ) -> Result<tonic::Response<BatchUpdateBlobsResponse>, tonic::Status> {
            let state = &self.0;
            state.batch_update_calls.fetch_add(1, Ordering::SeqCst);
            let requests = request.into_inner().requests;
            for request in &requests {
                let hash = request.digest.as_ref().unwrap().hash.clone();
                *state.uploaded.lock().unwrap().entry(hash).or_default() += 1;
            }
            let next = state.script.lock().unwrap().pop_front();
            tokio::time::sleep(state.delay).await;
            let code = match next {
                Some(FakeBatchUpdate::Fail(status)) => return Err(status),
                Some(FakeBatchUpdate::Silence) => {
                    tokio::time::sleep(Duration::from_secs(600)).await;
                    return Err(tonic::Status::internal("unreachable"));
                }
                Some(FakeBatchUpdate::BlobStatus(code)) => code,
                None => Code::Ok,
            };
            let mut stored = state.stored.lock().unwrap();
            Ok(tonic::Response::new(BatchUpdateBlobsResponse {
                responses: requests
                    .into_iter()
                    .map(|request| {
                        let digest = request.digest.unwrap();
                        stored.insert(digest.hash.clone());
                        batch_update_blobs_response::Response {
                            digest: Some(digest),
                            status: Some(Status {
                                code: code as i32,
                                ..Default::default()
                            }),
                        }
                    })
                    .collect(),
            }))
        }

        async fn batch_read_blobs(
            &self,
            _request: tonic::Request<BatchReadBlobsRequest>,
        ) -> Result<tonic::Response<BatchReadBlobsResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not used by these tests"))
        }

        async fn get_tree(
            &self,
            _request: tonic::Request<re_grpc_proto::build::bazel::remote::execution::v2::GetTreeRequest>,
        ) -> Result<tonic::Response<Self::GetTreeStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("not used by these tests"))
        }

        async fn split_blob(
            &self,
            _request: tonic::Request<re_grpc_proto::build::bazel::remote::execution::v2::SplitBlobRequest>,
        ) -> Result<
            tonic::Response<re_grpc_proto::build::bazel::remote::execution::v2::SplitBlobResponse>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("not used by these tests"))
        }

        async fn splice_blob(
            &self,
            _request: tonic::Request<re_grpc_proto::build::bazel::remote::execution::v2::SpliceBlobRequest>,
        ) -> Result<
            tonic::Response<re_grpc_proto::build::bazel::remote::execution::v2::SpliceBlobResponse>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("not used by these tests"))
        }
    }

    /// A client of a `FakeCas` with `state`, configured by `opts` otherwise.
    async fn fake_cas_client(
        state: Arc<FakeCasState>,
        opts: Buck2OssReConfiguration,
    ) -> anyhow::Result<(REClient, tokio::task::JoinHandle<()>)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = format!("grpc://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(
                    re_grpc_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorageServer::new(
                        FakeCas(state),
                    ),
                )
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });
        Ok((raw_h2_client_with(address, opts).await?, server))
    }

    fn small_blob_upload(data: &[u8]) -> UploadRequest {
        UploadRequest {
            inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                blob: data.to_vec(),
                digest: digest_for_test_data(data),
                ..Default::default()
            }]),
            ..Default::default()
        }
    }

    fn ttl_request(digest: &TDigest) -> GetDigestsTtlRequest {
        GetDigestsTtlRequest {
            digests: vec![digest.clone()],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn concurrent_uploads_of_one_missing_blob_send_it_once() -> anyhow::Result<()> {
        let state = Arc::new(FakeCasState {
            delay: Duration::from_millis(300),
            ..Default::default()
        });
        let (client, _server) = fake_cas_client(state.clone(), Default::default()).await?;
        let data = b"a directory blob that eight actions need";
        let metadata = RemoteExecutionMetadata::default();

        let results = futures::future::join_all(
            (0..8).map(|_| client.upload(metadata.clone(), small_blob_upload(data))),
        )
        .await;

        assert!(results.iter().all(|result| result.is_ok()), "{results:?}");
        assert_eq!(state.uploaded(&digest_for_test_data(data)), 1);
        Ok(())
    }

    #[tokio::test]
    async fn waiters_upload_again_after_the_upload_they_waited_on_fails() -> anyhow::Result<()> {
        let state = Arc::new(FakeCasState {
            delay: Duration::from_millis(300),
            script: Mutex::new(VecDeque::from([FakeBatchUpdate::Fail(
                tonic::Status::unavailable("the CAS is busy"),
            )])),
            ..Default::default()
        });
        // No retries, so the first upload fails and what follows is the waiters' doing.
        let (client, _server) = fake_cas_client(
            state.clone(),
            Buck2OssReConfiguration {
                retries: Some(0),
                ..Default::default()
            },
        )
        .await?;
        let data = b"a blob whose first upload fails";
        let metadata = RemoteExecutionMetadata::default();

        let results = futures::future::join_all(
            (0..4).map(|_| client.upload(metadata.clone(), small_blob_upload(data))),
        )
        .await;

        let failed = results.iter().filter(|result| result.is_err()).count();
        assert_eq!(
            failed, 1,
            "only the caller whose upload failed fails: {results:?}"
        );
        assert_eq!(state.uploaded(&digest_for_test_data(data)), 2);
        assert!(
            state
                .stored
                .lock()
                .unwrap()
                .contains(&digest_for_test_data(data).hash)
        );
        Ok(())
    }

    #[tokio::test]
    async fn find_missing_for_a_digest_already_asked_about_is_not_sent_again() -> anyhow::Result<()>
    {
        let state = Arc::new(FakeCasState {
            delay: Duration::from_millis(300),
            ..Default::default()
        });
        let (client, _server) = fake_cas_client(state.clone(), Default::default()).await?;
        let digest = digest_for_test_data(b"a digest six actions ask about");
        let metadata = RemoteExecutionMetadata::default();

        let results = futures::future::join_all(
            (0..6).map(|_| client.get_digests_ttl(&metadata, ttl_request(&digest))),
        )
        .await;

        for result in results {
            assert_eq!(result?.digests_with_ttl[0].ttl, 0);
        }
        assert_eq!(state.asked(&digest), 1);
        Ok(())
    }

    #[tokio::test]
    async fn find_missing_for_a_digest_being_uploaded_waits_for_the_upload() -> anyhow::Result<()> {
        let state = Arc::new(FakeCasState {
            delay: Duration::from_millis(300),
            ..Default::default()
        });
        let (client, _server) = fake_cas_client(state.clone(), Default::default()).await?;
        let data = b"a blob one action uploads while another asks about it";
        let digest = digest_for_test_data(data);
        let metadata = RemoteExecutionMetadata::default();

        let (uploaded, ttl) = futures::future::join(
            client.upload(metadata.clone(), small_blob_upload(data)),
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                client
                    .get_digests_ttl(&metadata, ttl_request(&digest))
                    .await
            },
        )
        .await;

        uploaded?;
        assert!(ttl?.digests_with_ttl[0].ttl > 0);
        assert_eq!(state.asked(&digest), 0);
        assert_eq!(state.uploaded(&digest), 1);
        Ok(())
    }

    #[tokio::test]
    async fn an_upload_that_times_out_on_the_client_is_retried() -> anyhow::Result<()> {
        let state = Arc::new(FakeCasState {
            script: Mutex::new(VecDeque::from([FakeBatchUpdate::Silence])),
            ..Default::default()
        });
        let (client, _server) = fake_cas_client(
            state.clone(),
            Buck2OssReConfiguration {
                retries: Some(2),
                grpc_request_timeout_secs: Some(1),
                ..Default::default()
            },
        )
        .await?;
        let data = b"a blob whose first upload is never answered";

        client
            .upload(RemoteExecutionMetadata::default(), small_blob_upload(data))
            .await?;

        assert_eq!(state.batch_update_calls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn an_upload_the_server_rejects_is_not_sent_again() -> anyhow::Result<()> {
        let state = Arc::new(FakeCasState {
            delay: Duration::from_millis(300),
            script: Mutex::new(VecDeque::from([FakeBatchUpdate::Fail(
                tonic::Status::invalid_argument("the blob does not match its digest"),
            )])),
            ..Default::default()
        });
        let (client, _server) = fake_cas_client(
            state.clone(),
            Buck2OssReConfiguration {
                retries: Some(3),
                ..Default::default()
            },
        )
        .await?;
        let data = b"a blob the CAS refuses";
        let metadata = RemoteExecutionMetadata::default();

        let results = futures::future::join_all(
            (0..4).map(|_| client.upload(metadata.clone(), small_blob_upload(data))),
        )
        .await;

        for result in results {
            let err = result.unwrap_err();
            assert_eq!(
                err.downcast_ref::<REClientError>().map(|err| err.code),
                Some(TCode::INVALID_ARGUMENT),
                "{err:#}"
            );
        }
        assert_eq!(state.batch_update_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn a_blob_the_cas_already_has_is_uploaded() -> anyhow::Result<()> {
        let state = Arc::new(FakeCasState {
            script: Mutex::new(VecDeque::from([FakeBatchUpdate::BlobStatus(
                Code::AlreadyExists,
            )])),
            ..Default::default()
        });
        let (client, _server) = fake_cas_client(state.clone(), Default::default()).await?;

        client
            .upload(
                RemoteExecutionMetadata::default(),
                small_blob_upload(b"a blob the CAS already has"),
            )
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_upload_outlives_its_starter_while_another_caller_waits() -> anyhow::Result<()> {
        let state = Arc::new(FakeCasState {
            delay: Duration::from_millis(500),
            ..Default::default()
        });
        let (client, _server) = fake_cas_client(state.clone(), Default::default()).await?;
        let data = b"a blob whose first uploader is cancelled";
        let metadata = RemoteExecutionMetadata::default();

        let (starter, waiter) = futures::future::join(
            tokio::time::timeout(
                Duration::from_millis(100),
                client.upload(metadata.clone(), small_blob_upload(data)),
            ),
            client.upload(metadata.clone(), small_blob_upload(data)),
        )
        .await;

        assert!(starter.is_err(), "the starter was cancelled");
        waiter?;
        assert_eq!(state.uploaded(&digest_for_test_data(data)), 1);
        Ok(())
    }

    #[tokio::test]
    async fn an_upload_every_caller_dropped_is_cancelled_and_forgotten() -> anyhow::Result<()> {
        let state = Arc::new(FakeCasState {
            delay: Duration::from_millis(500),
            ..Default::default()
        });
        let (client, _server) = fake_cas_client(state.clone(), Default::default()).await?;
        let data = b"a blob every uploader gives up on";
        let metadata = RemoteExecutionMetadata::default();

        let cancelled = tokio::time::timeout(
            Duration::from_millis(100),
            futures::future::join(
                client.upload(metadata.clone(), small_blob_upload(data)),
                client.upload(metadata.clone(), small_blob_upload(data)),
            ),
        )
        .await;
        assert!(cancelled.is_err());
        assert!(
            client
                .shared_batch_uploads
                .map
                .lock()
                .unwrap()
                .calls
                .is_empty()
        );

        client
            .upload(metadata.clone(), small_blob_upload(data))
            .await?;
        assert_eq!(state.uploaded(&digest_for_test_data(data)), 2);
        Ok(())
    }

    fn digest_for_test_data(data: &[u8]) -> TDigest {
        TDigest {
            hash: format!("{:x}", Sha256::digest(data)),
            size_in_bytes: data.len() as i64,
            ..Default::default()
        }
    }

    fn blake3_digest_for_test_data(data: &[u8]) -> TDigest {
        TDigest {
            hash: blake3::hash(data).to_hex().to_string(),
            size_in_bytes: data.len() as i64,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_download_named() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;

        let path2 = work.path().join("path2");
        let path2 = path2.to_str().context("tempdir is not utf8")?;

        let blob1 = vec![1, 2, 3];
        let blob2 = vec![4, 5, 6];
        let digest1 = digest_for_test_data(&blob1);
        let digest2 = digest_for_test_data(&blob2);

        let req = DownloadRequest {
            file_digests: Some(vec![
                NamedDigestWithPermissions {
                    named_digest: NamedDigest {
                        name: path1.to_owned(),
                        digest: digest1.clone(),
                        ..Default::default()
                    },
                    is_executable: true,
                    ..Default::default()
                },
                NamedDigestWithPermissions {
                    named_digest: NamedDigest {
                        name: path2.to_owned(),
                        digest: digest2.clone(),
                        ..Default::default()
                    },
                    is_executable: false,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let res = BatchReadBlobsResponse {
            responses: vec![
                // Reply out of order
                batch_read_blobs_response::Response {
                    digest: Some(tdigest_to(digest2.clone())),
                    data: blob2.clone(),
                    ..Default::default()
                },
                batch_read_blobs_response::Response {
                    digest: Some(tdigest_to(digest1.clone())),
                    data: blob1.clone(),
                    ..Default::default()
                },
            ],
        };

        test_download_impl(
            &InstanceName(None),
            req,
            None,
            10000,
            None,
            DigestFunctionConfig::default(),
            |req| {
                let res = res.clone();
                let digest1 = digest1.clone();
                let digest2 = digest2.clone();
                async move {
                    assert_eq!(req.digests.len(), 2);
                    assert_eq!(req.digests[0], tdigest_to(digest1));
                    assert_eq!(req.digests[1], tdigest_to(digest2));
                    Ok(res.clone())
                }
            },
            |_digest| async move { anyhow::Ok(Box::pin(futures::stream::iter(vec![]))) },
        )
        .await?;

        assert_eq!(tokio::fs::read(&path1).await?, vec![1, 2, 3]);
        assert_eq!(tokio::fs::read(&path2).await?, vec![4, 5, 6]);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                tokio::fs::metadata(&path1).await?.permissions().mode() & 0o111,
                0o111
            );
            assert_eq!(
                tokio::fs::metadata(&path2).await?.permissions().mode() & 0o111,
                0o000
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_download_large_named() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;

        let path2 = work.path().join("path2");
        let path2 = path2.to_str().context("tempdir is not utf8")?;

        let blob1 = vec![1, 2, 3];
        let digest1 = digest_for_test_data(&blob1);

        let blob_data = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];

        let digest2 = digest_for_test_data(&blob_data);

        let req = DownloadRequest {
            file_digests: Some(vec![
                NamedDigestWithPermissions {
                    named_digest: NamedDigest {
                        name: path1.to_owned(),
                        digest: digest1.clone(),
                        ..Default::default()
                    },
                    is_executable: true,
                    ..Default::default()
                },
                NamedDigestWithPermissions {
                    named_digest: NamedDigest {
                        name: path2.to_owned(),
                        digest: digest2.clone(),
                        ..Default::default()
                    },
                    is_executable: false,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let res = BatchReadBlobsResponse {
            responses: vec![
                // Reply out of order
                batch_read_blobs_response::Response {
                    digest: Some(tdigest_to(digest1.clone())),
                    data: blob1.clone(),
                    ..Default::default()
                },
            ],
        };

        let read_response1 = ReadResponse {
            data: blob_data[..10].to_vec(),
        };
        let read_response2 = ReadResponse {
            data: blob_data[10..].to_vec(),
        };

        test_download_impl(
            &InstanceName(None),
            req,
            None,
            10, // kept small to simulate a large file download
            None,
            DigestFunctionConfig::default(),
            |req| {
                let res = res.clone();
                let digest1 = digest1.clone();
                async move {
                    assert_eq!(req.digests.len(), 1);
                    assert_eq!(req.digests[0], tdigest_to(digest1));
                    Ok(res.clone())
                }
            },
            |req| {
                let read_response1 = read_response1.clone();
                let read_response2 = read_response2.clone();
                let digest2 = digest2.clone();
                async move {
                    assert_eq!(req.resource_name, format!("blobs/{}/18", digest2.hash));
                    anyhow::Ok(Box::pin(futures::stream::iter(vec![
                        Ok(read_response1),
                        Ok(read_response2),
                    ])))
                }
            },
        )
        .await?;

        assert_eq!(tokio::fs::read(&path1).await?, vec![1, 2, 3]);
        assert_eq!(tokio::fs::read(&path2).await?, blob_data);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                tokio::fs::metadata(&path1).await?.permissions().mode() & 0o111,
                0o111
            );
            assert_eq!(
                tokio::fs::metadata(&path2).await?.permissions().mode() & 0o111,
                0o000
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_download_large_named_dedupes_concurrent_bytestream() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;

        let path2 = work.path().join("path2");
        let path2 = path2.to_str().context("tempdir is not utf8")?;

        let blob_data = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];
        let blob_data_ref = blob_data.as_slice();
        let digest = digest_for_test_data(&blob_data);
        let digest_hash = digest.hash.as_str();
        let reads = AtomicU16::new(0);

        let req = DownloadRequest {
            file_digests: Some(vec![
                NamedDigestWithPermissions {
                    named_digest: NamedDigest {
                        name: path1.to_owned(),
                        digest: digest.clone(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                NamedDigestWithPermissions {
                    named_digest: NamedDigest {
                        name: path2.to_owned(),
                        digest: digest.clone(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        test_download_impl(
            &InstanceName(None),
            req,
            None,
            10,
            None,
            DigestFunctionConfig::default(),
            |_req| async { Ok(BatchReadBlobsResponse { responses: vec![] }) },
            |req| {
                reads.fetch_add(1, Ordering::Relaxed);
                async move {
                    tokio::task::yield_now().await;
                    assert_eq!(req.resource_name, format!("blobs/{digest_hash}/18"));
                    anyhow::Ok(Box::pin(futures::stream::iter(vec![Ok(ReadResponse {
                        data: blob_data_ref.to_vec(),
                    })])))
                }
            },
        )
        .await?;

        assert_eq!(tokio::fs::read(&path1).await?, blob_data);
        assert_eq!(tokio::fs::read(&path2).await?, blob_data);
        assert_eq!(reads.load(Ordering::Relaxed), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_download_inlined() -> anyhow::Result<()> {
        let blob1 = vec![1, 2, 3];
        let blob2 = vec![4, 5, 6];
        let digest1 = &digest_for_test_data(&blob1);
        let digest2 = &digest_for_test_data(&blob2);

        let req = DownloadRequest {
            inlined_digests: Some(vec![digest1.clone(), digest2.clone()]),
            ..Default::default()
        };

        let res = BatchReadBlobsResponse {
            responses: vec![
                // Reply out of order
                batch_read_blobs_response::Response {
                    digest: Some(tdigest_to(digest2.clone())),
                    data: blob2.clone(),
                    ..Default::default()
                },
                batch_read_blobs_response::Response {
                    digest: Some(tdigest_to(digest1.clone())),
                    data: blob1.clone(),
                    ..Default::default()
                },
            ],
        };

        let res = test_download_impl(
            &InstanceName(None),
            req,
            None,
            100000,
            None,
            DigestFunctionConfig::default(),
            |req| {
                let res = res.clone();
                let digest1 = digest1.clone();
                let digest2 = digest2.clone();
                async move {
                    assert_eq!(req.digests.len(), 2);
                    assert_eq!(req.digests[0], tdigest_to(digest1));
                    assert_eq!(req.digests[1], tdigest_to(digest2));
                    Ok(res)
                }
            },
            |_digest| async move { anyhow::Ok(Box::pin(futures::stream::iter(vec![]))) },
        )
        .await?;

        let inlined_blobs = res.inlined_blobs.unwrap();

        assert_eq!(inlined_blobs.len(), 2);

        assert_eq!(inlined_blobs[0].digest, *digest1);
        assert_eq!(inlined_blobs[0].blob, blob1);

        assert_eq!(inlined_blobs[1].digest, *digest2);
        assert_eq!(inlined_blobs[1].blob, blob2);

        Ok(())
    }

    #[tokio::test]
    async fn test_download_batch_compressed() -> anyhow::Result<()> {
        let blob = vec![1u8; DEFAULT_REMOTE_CACHE_COMPRESSION_THRESHOLD + 1];
        let digest = &digest_for_test_data(&blob);
        let compressed_blob = compress_data(blob.clone(), Compressor::Zstd).await?;

        let req = DownloadRequest {
            inlined_digests: Some(vec![digest.clone()]),
            ..Default::default()
        };

        let res = test_download_impl(
            &InstanceName(None),
            req,
            Some(Compressor::Zstd),
            100000,
            None,
            DigestFunctionConfig::default(),
            |req| {
                let digest = digest.clone();
                let compressed_blob = compressed_blob.clone();
                async move {
                    assert_eq!(
                        req.acceptable_compressors,
                        vec![
                            compressor::Value::Identity as i32,
                            compressor::Value::Zstd as i32,
                        ]
                    );
                    Ok(BatchReadBlobsResponse {
                        responses: vec![batch_read_blobs_response::Response {
                            digest: Some(tdigest_to(digest)),
                            data: compressed_blob,
                            compressor: compressor::Value::Zstd as i32,
                            ..Default::default()
                        }],
                    })
                }
            },
            |_digest| async move { anyhow::Ok(Box::pin(futures::stream::iter(vec![]))) },
        )
        .await?;

        assert_eq!(res.inlined_blobs.unwrap()[0].blob, blob);
        Ok(())
    }

    #[tokio::test]
    async fn test_download_batch_compression_respects_threshold() -> anyhow::Result<()> {
        let blob = vec![1u8; DEFAULT_REMOTE_CACHE_COMPRESSION_THRESHOLD - 1];
        let digest = &digest_for_test_data(&blob);

        let req = DownloadRequest {
            inlined_digests: Some(vec![digest.clone()]),
            ..Default::default()
        };

        test_download_impl(
            &InstanceName(None),
            req,
            Some(Compressor::Zstd),
            100000,
            None,
            DigestFunctionConfig::default(),
            |req| {
                let digest = digest.clone();
                let blob = blob.clone();
                async move {
                    assert_eq!(
                        req.acceptable_compressors,
                        vec![compressor::Value::Identity as i32]
                    );
                    Ok(BatchReadBlobsResponse {
                        responses: vec![batch_read_blobs_response::Response {
                            digest: Some(tdigest_to(digest)),
                            data: blob,
                            compressor: compressor::Value::Identity as i32,
                            ..Default::default()
                        }],
                    })
                }
            },
            |_digest| async move { anyhow::Ok(Box::pin(futures::stream::iter(vec![]))) },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_download_multiple_batches() -> anyhow::Result<()> {
        let blob_data = vec![0, 1, 2];
        let digest1 = &digest_for_test_data(&blob_data);
        let digest2 = &digest_for_test_data(&blob_data);
        let digest3 = &digest_for_test_data(&blob_data);
        let digest4 = &digest_for_test_data(&blob_data);
        let digest5 = &digest_for_test_data(&blob_data);
        let digest6 = &digest_for_test_data(&blob_data);

        let digests = vec![
            digest1.clone(),
            digest2.clone(),
            digest3.clone(),
            digest4.clone(),
            digest5.clone(),
            digest6.clone(),
        ];

        let req = DownloadRequest {
            inlined_digests: Some(digests.clone()),
            ..Default::default()
        };

        let counter = AtomicU16::new(0);

        let res = test_download_impl(
            &InstanceName(None),
            req,
            None,
            7,
            None,
            DigestFunctionConfig::default(),
            |req| {
                counter.fetch_add(1, Ordering::Relaxed);
                let res = BatchReadBlobsResponse {
                    responses: req.digests.map(|d| batch_read_blobs_response::Response {
                        digest: Some(d.clone()),
                        data: blob_data.clone(),
                        ..Default::default()
                    }),
                };
                async { Ok(res) }
            },
            |_digest| async move { anyhow::Ok(Box::pin(futures::stream::iter(vec![]))) },
        )
        .await?;

        let inlined_blobs = res.inlined_blobs.unwrap();

        assert_eq!(inlined_blobs.len(), digests.len());
        assert_eq!(counter.load(Ordering::Relaxed), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_download_large_inlined() -> anyhow::Result<()> {
        let blob1 = vec![1, 2, 3];
        let digest1 = &digest_for_test_data(&blob1);
        let blob_data = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];
        let digest2 = &digest_for_test_data(&blob_data);

        let req = DownloadRequest {
            inlined_digests: Some(vec![digest1.clone(), digest2.clone()]),
            ..Default::default()
        };

        let res = BatchReadBlobsResponse {
            responses: vec![
                // Reply out of order
                batch_read_blobs_response::Response {
                    digest: Some(tdigest_to(digest1.clone())),
                    data: blob1.clone(),
                    ..Default::default()
                },
            ],
        };

        let read_response1 = ReadResponse {
            data: blob_data[..10].to_vec(),
        };
        let read_response2 = ReadResponse {
            data: blob_data[10..].to_vec(),
        };

        let res = test_download_impl(
            &InstanceName(None),
            req,
            None,
            10, // intentionally small value to keep data in the test blobs small
            None,
            DigestFunctionConfig::default(),
            |req| {
                let res = res.clone();
                let digest1 = digest1.clone();
                async move {
                    assert_eq!(req.digests.len(), 1);
                    assert_eq!(req.digests[0], tdigest_to(digest1));
                    Ok(res)
                }
            },
            |req| {
                let read_response1 = read_response1.clone();
                let read_response2 = read_response2.clone();
                let digest2 = digest2.clone();
                async move {
                    assert_eq!(req.resource_name, format!("blobs/{}/18", digest2.hash));
                    anyhow::Ok(Box::pin(futures::stream::iter(vec![
                        Ok(read_response1),
                        Ok(read_response2),
                    ])))
                }
            },
        )
        .await?;

        let inlined_blobs = res.inlined_blobs.unwrap();

        assert_eq!(inlined_blobs.len(), 2);

        assert_eq!(inlined_blobs[0].digest, *digest1);
        assert_eq!(inlined_blobs[0].blob, blob1);

        assert_eq!(inlined_blobs[1].digest, *digest2);
        assert_eq!(inlined_blobs[1].blob, blob_data);

        Ok(())
    }

    #[tokio::test]
    async fn test_download_large_inlined_dedupes_concurrent_bytestream() -> anyhow::Result<()> {
        let blob_data = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];
        let blob_data_ref = blob_data.as_slice();
        let digest = digest_for_test_data(&blob_data);
        let digest_hash = digest.hash.as_str();
        let reads = AtomicU16::new(0);
        let active_downloads = ActiveTransferRegistry::new();

        let download = || {
            download_impl(
                &InstanceName(None),
                DownloadRequest {
                    inlined_digests: Some(vec![digest.clone()]),
                    ..Default::default()
                },
                None,
                10,
                DEFAULT_REMOTE_CACHE_COMPRESSION_THRESHOLD,
                None,
                DigestFunctionConfig::default(),
                0,
                Duration::from_millis(1),
                Duration::from_secs(DEFAULT_BYTESTREAM_PROGRESS_TIMEOUT_SECS),
                &active_downloads,
                None,
                |_req| async { Ok(BatchReadBlobsResponse { responses: vec![] }) },
                |req| {
                    reads.fetch_add(1, Ordering::Relaxed);
                    async move {
                        tokio::task::yield_now().await;
                        assert_eq!(req.resource_name, format!("blobs/{digest_hash}/18"));
                        anyhow::Ok(Box::pin(futures::stream::iter(vec![Ok(ReadResponse {
                            data: blob_data_ref.to_vec(),
                        })])))
                    }
                },
                || async {},
            )
        };

        let (res1, res2) = tokio::try_join!(download(), download())?;

        assert_eq!(res1.inlined_blobs.unwrap()[0].blob, blob_data);
        assert_eq!(res2.inlined_blobs.unwrap()[0].blob, blob_data);
        assert_eq!(reads.load(Ordering::Relaxed), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_download_large_inlined_resumes_bystream_after_error() -> anyhow::Result<()> {
        let blob_data = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];
        let digest = &digest_for_test_data(&blob_data);

        let req = DownloadRequest {
            inlined_digests: Some(vec![digest.clone()]),
            ..Default::default()
        };

        let attempts = AtomicU16::new(0);
        let reconnects = AtomicU16::new(0);
        let active_downloads = ActiveTransferRegistry::new();
        let res = download_impl(
            &InstanceName(None),
            req,
            None,
            10,
            DEFAULT_REMOTE_CACHE_COMPRESSION_THRESHOLD,
            None,
            DigestFunctionConfig::default(),
            1,
            Duration::from_millis(1),
            Duration::from_secs(DEFAULT_BYTESTREAM_PROGRESS_TIMEOUT_SECS),
            &active_downloads,
            None,
            |_req| async { Ok(BatchReadBlobsResponse { responses: vec![] }) },
            |req| {
                let attempt = attempts.fetch_add(1, Ordering::Relaxed);
                let digest = digest.clone();
                let blob_data = blob_data.clone();
                async move {
                    assert_eq!(req.resource_name, format!("blobs/{}/18", digest.hash));
                    let responses = if attempt == 0 {
                        assert_eq!(req.read_offset, 0);
                        vec![
                            Ok(ReadResponse {
                                data: blob_data[..10].to_vec(),
                            }),
                            Err(tonic::Status::unavailable(
                                "transport error: connection reset",
                            )),
                        ]
                    } else {
                        assert_eq!(req.read_offset, 10);
                        vec![Ok(ReadResponse {
                            data: blob_data[10..].to_vec(),
                        })]
                    };
                    anyhow::Ok(Box::pin(futures::stream::iter(responses)))
                }
            },
            || {
                reconnects.fetch_add(1, Ordering::Relaxed);
                async {}
            },
        )
        .await?;

        let inlined_blobs = res.inlined_blobs.unwrap();
        assert_eq!(inlined_blobs[0].blob, blob_data);
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
        assert_eq!(reconnects.load(Ordering::Relaxed), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_download_empty() -> anyhow::Result<()> {
        let digest1 = &digest_for_test_data(&[]);

        let req = DownloadRequest {
            inlined_digests: Some(vec![digest1.clone()]),
            ..Default::default()
        };

        let res = BatchReadBlobsResponse { responses: vec![] };

        let res = test_download_impl(
            &InstanceName(None),
            req,
            None,
            100000,
            None,
            DigestFunctionConfig::default(),
            |req| {
                let res = res.clone();
                async move {
                    assert_eq!(req.digests.len(), 0);
                    Ok(res)
                }
            },
            |_digest| async move { anyhow::Ok(Box::pin(futures::stream::iter(vec![]))) },
        )
        .await?;

        let inlined_blobs = res.inlined_blobs.unwrap();

        assert_eq!(inlined_blobs.len(), 1);

        assert_eq!(inlined_blobs[0].digest, *digest1);
        assert!(inlined_blobs[0].blob.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_download_inlined_size_mismatch_fails() -> anyhow::Result<()> {
        let digest1 = digest_for_test_data(&[1, 2, 3]);

        let req = DownloadRequest {
            inlined_digests: Some(vec![digest1.clone()]),
            ..Default::default()
        };

        let res = BatchReadBlobsResponse {
            responses: vec![batch_read_blobs_response::Response {
                digest: Some(tdigest_to(digest1.clone())),
                data: vec![1, 2],
                ..Default::default()
            }],
        };

        let err = match test_download_impl(
            &InstanceName(None),
            req,
            None,
            100000,
            None,
            DigestFunctionConfig::default(),
            |_req| {
                let res = res.clone();
                async move { Ok(res) }
            },
            |_digest| async move { anyhow::Ok(Box::pin(futures::stream::iter(vec![]))) },
        )
        .await
        {
            Ok(_) => anyhow::bail!("expected size mismatch error"),
            Err(err) => err,
        };

        assert!(
            err.chain()
                .any(|e| e.to_string().contains("Downloaded blob size mismatch"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_download_named_stream_size_mismatch_fails() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;

        let path = work.path().join("path");
        let path = path.to_str().context("tempdir is not utf8")?;

        let digest = digest_for_test_data(&[1; 18]);
        let req = DownloadRequest {
            file_digests: Some(vec![NamedDigestWithPermissions {
                named_digest: NamedDigest {
                    name: path.to_owned(),
                    digest: digest.clone(),
                    ..Default::default()
                },
                is_executable: false,
                ..Default::default()
            }]),
            ..Default::default()
        };

        let read_response = ReadResponse { data: vec![1; 17] };
        let err = match test_download_impl(
            &InstanceName(None),
            req,
            None,
            10,
            None,
            DigestFunctionConfig::default(),
            |_req| async { Ok(BatchReadBlobsResponse { responses: vec![] }) },
            |req| {
                let read_response = read_response.clone();
                let digest = digest.clone();
                async move {
                    assert_eq!(req.resource_name, format!("blobs/{}/18", digest.hash));
                    anyhow::Ok(Box::pin(futures::stream::iter(vec![Ok(read_response)])))
                }
            },
        )
        .await
        {
            Ok(_) => anyhow::bail!("expected size mismatch error"),
            Err(err) => err,
        };

        assert!(
            err.chain()
                .any(|e| e.to_string().contains("Downloaded blob size mismatch"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_download_inlined_hash_mismatch_fails() -> anyhow::Result<()> {
        let digest = digest_for_test_data(&[1, 2, 3]);
        let req = DownloadRequest {
            inlined_digests: Some(vec![digest.clone()]),
            ..Default::default()
        };

        let res = BatchReadBlobsResponse {
            responses: vec![batch_read_blobs_response::Response {
                digest: Some(tdigest_to(digest.clone())),
                data: vec![4, 5, 6],
                ..Default::default()
            }],
        };

        let err = match test_download_impl(
            &InstanceName(None),
            req,
            None,
            100000,
            None,
            DigestFunctionConfig::default(),
            |_req| {
                let res = res.clone();
                async move { Ok(res) }
            },
            |_digest| async move { anyhow::Ok(Box::pin(futures::stream::iter(vec![]))) },
        )
        .await
        {
            Ok(_) => anyhow::bail!("expected hash mismatch error"),
            Err(err) => err,
        };

        assert!(
            err.chain()
                .any(|e| e.to_string().contains("Downloaded blob hash mismatch"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_download_named_stream_hash_mismatch_fails() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;

        let path = work.path().join("path");
        let path = path.to_str().context("tempdir is not utf8")?;

        let expected_data = vec![1u8; 18];
        let actual_data = vec![2u8; 18];
        let digest = digest_for_test_data(&expected_data);
        let req = DownloadRequest {
            file_digests: Some(vec![NamedDigestWithPermissions {
                named_digest: NamedDigest {
                    name: path.to_owned(),
                    digest: digest.clone(),
                    ..Default::default()
                },
                is_executable: false,
                ..Default::default()
            }]),
            ..Default::default()
        };

        let read_response = ReadResponse {
            data: actual_data.clone(),
        };
        let err = match test_download_impl(
            &InstanceName(None),
            req,
            None,
            10,
            None,
            DigestFunctionConfig::default(),
            |_req| async { Ok(BatchReadBlobsResponse { responses: vec![] }) },
            |req| {
                let read_response = read_response.clone();
                let digest = digest.clone();
                async move {
                    assert_eq!(req.resource_name, format!("blobs/{}/18", digest.hash));
                    anyhow::Ok(Box::pin(futures::stream::iter(vec![Ok(read_response)])))
                }
            },
        )
        .await
        {
            Ok(_) => anyhow::bail!("expected hash mismatch error"),
            Err(err) => err,
        };

        assert!(
            err.chain()
                .any(|e| e.to_string().contains("Downloaded blob hash mismatch"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_download_resource_name() -> anyhow::Result<()> {
        let digest1 = &digest_for_test_data(&[]);

        let req = DownloadRequest {
            inlined_digests: Some(vec![digest1.clone()]),
            ..Default::default()
        };

        test_download_impl(
            &InstanceName(Some("instance".to_owned())),
            req,
            None,
            0,
            None,
            DigestFunctionConfig::default(),
            |_req| async { panic!("not called") },
            |req| async move {
                assert_eq!(
                    req.resource_name,
                    format!("instance/blobs/{}/0", digest1.hash)
                );
                anyhow::Ok(Box::pin(futures::stream::iter(vec![])))
            },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_download_sets_blake3_digest_function() -> anyhow::Result<()> {
        let blob = b"aaa".to_vec();
        let digest = blake3_digest_for_test_data(&blob);
        let config = DigestFunctionConfig::from_configured_algorithms(&["BLAKE3".into()]);

        let batch_req = DownloadRequest {
            inlined_digests: Some(vec![digest.clone()]),
            ..Default::default()
        };

        test_download_impl(
            &InstanceName(None),
            batch_req,
            None,
            10000,
            Some(digest_function::Value::Blake3),
            config,
            |req| {
                let digest = digest.clone();
                let blob = blob.clone();
                async move {
                    assert_eq!(req.digest_function, digest_function::Value::Blake3 as i32);
                    Ok(BatchReadBlobsResponse {
                        responses: vec![batch_read_blobs_response::Response {
                            digest: Some(tdigest_to(digest)),
                            data: blob,
                            ..Default::default()
                        }],
                    })
                }
            },
            |_req| async { anyhow::Ok(Box::pin(futures::stream::iter(vec![]))) },
        )
        .await?;

        let stream_req = DownloadRequest {
            inlined_digests: Some(vec![digest.clone()]),
            ..Default::default()
        };

        test_download_impl(
            &InstanceName(None),
            stream_req,
            None,
            1,
            Some(digest_function::Value::Blake3),
            config,
            |_req| async { panic!("not called") },
            |req| {
                let digest = digest.clone();
                let blob = blob.clone();
                async move {
                    assert_eq!(req.resource_name, format!("blobs/blake3/{}/3", digest.hash));
                    anyhow::Ok(Box::pin(futures::stream::iter(vec![Ok(ReadResponse {
                        data: blob,
                    })])))
                }
            },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_named() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path1, "aaa").await?;

        let path2 = work.path().join("path2");
        let path2 = path2.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path2, "bbb").await?;

        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let digest2 = TDigest {
            hash: "bb".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let req = UploadRequest {
            files_with_digest: Some(vec![
                NamedDigest {
                    name: path1.to_owned(),
                    digest: digest1.clone(),
                    ..Default::default()
                },
                NamedDigest {
                    name: path2.to_owned(),
                    digest: digest2.clone(),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let res = BatchUpdateBlobsResponse {
            responses: vec![
                // Reply out of order
                batch_update_blobs_response::Response {
                    digest: Some(tdigest_to(digest2.clone())),
                    status: Some(Status::default()),
                },
                batch_update_blobs_response::Response {
                    digest: Some(tdigest_to(digest1.clone())),
                    status: Some(Status::default()),
                },
            ],
        };

        upload_impl(
            &InstanceName(None),
            req,
            None,
            10000,
            None,
            DigestFunctionConfig::default(),
            move |req: BatchUpdateBlobsRequest| {
                let res = res.clone();
                let digest1 = digest1.clone();
                let digest2 = digest2.clone();
                async move {
                    assert_eq!(req.requests.len(), 2);
                    assert_eq!(req.requests[0].digest, Some(tdigest_to(digest1)));
                    assert_eq!(req.requests[0].data, b"aaa");
                    assert_eq!(req.requests[1].digest, Some(tdigest_to(digest2)));
                    assert_eq!(req.requests[1].data, b"bbb");
                    Ok(res)
                }
            },
            |_req| async { panic!("A Bytestream upload should not be triggered") },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_batch_compressed() -> anyhow::Result<()> {
        let blob = vec![1u8; DEFAULT_REMOTE_CACHE_COMPRESSION_THRESHOLD + 1];
        let digest = digest_for_test_data(&blob);

        let active_uploads = ActiveTransferRegistry::new();
        super::upload_impl(
            &InstanceName(None),
            UploadRequest {
                inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                    blob: blob.clone(),
                    digest: digest.clone(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            None,
            Some(Compressor::Zstd),
            100000,
            DEFAULT_REMOTE_CACHE_COMPRESSION_THRESHOLD,
            None,
            DigestFunctionConfig::default(),
            &active_uploads,
            &SharedCallRegistry::new(),
            move |req: BatchUpdateBlobsRequest| {
                let digest = digest.clone();
                let blob = blob.clone();
                async move {
                    assert_eq!(req.requests.len(), 1);
                    assert_eq!(req.requests[0].compressor, compressor::Value::Zstd as i32);
                    let decompressed =
                        decompress_data(req.requests[0].data.clone(), Compressor::Zstd).await?;
                    assert_eq!(decompressed, blob);
                    Ok(BatchUpdateBlobsResponse {
                        responses: vec![batch_update_blobs_response::Response {
                            digest: Some(tdigest_to(digest)),
                            status: Some(Status::default()),
                        }],
                    })
                }
            },
            |_req| async { panic!("A Bytestream upload should not be triggered") },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_batch_compression_respects_threshold() -> anyhow::Result<()> {
        let blob = vec![1u8; DEFAULT_REMOTE_CACHE_COMPRESSION_THRESHOLD - 1];
        let digest = digest_for_test_data(&blob);

        let active_uploads = ActiveTransferRegistry::new();
        super::upload_impl(
            &InstanceName(None),
            UploadRequest {
                inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                    blob: blob.clone(),
                    digest: digest.clone(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            None,
            Some(Compressor::Zstd),
            100000,
            DEFAULT_REMOTE_CACHE_COMPRESSION_THRESHOLD,
            None,
            DigestFunctionConfig::default(),
            &active_uploads,
            &SharedCallRegistry::new(),
            move |req: BatchUpdateBlobsRequest| {
                let digest = digest.clone();
                let blob = blob.clone();
                async move {
                    assert_eq!(req.requests.len(), 1);
                    assert_eq!(
                        req.requests[0].compressor,
                        compressor::Value::Identity as i32
                    );
                    assert_eq!(req.requests[0].data, blob);
                    Ok(BatchUpdateBlobsResponse {
                        responses: vec![batch_update_blobs_response::Response {
                            digest: Some(tdigest_to(digest)),
                            status: Some(Status::default()),
                        }],
                    })
                }
            },
            |_req| async { panic!("A Bytestream upload should not be triggered") },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_large_named() -> anyhow::Result<()> {
        let blob_data = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];

        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path1, "aaa").await?;

        let path2 = work.path().join("path2");
        let path2 = path2.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path2, &blob_data).await?;

        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };

        let digest2 = TDigest {
            hash: "xl".to_owned(),
            size_in_bytes: 18,
            ..Default::default()
        };

        let req = UploadRequest {
            files_with_digest: Some(vec![
                NamedDigest {
                    name: path1.to_owned(),
                    digest: digest1.clone(),
                    ..Default::default()
                },
                NamedDigest {
                    name: path2.to_owned(),
                    digest: digest2.clone(),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let res = BatchUpdateBlobsResponse {
            responses: vec![batch_update_blobs_response::Response {
                digest: Some(tdigest_to(digest1.clone())),
                status: Some(Status::default()),
            }],
        };

        upload_impl(
            &InstanceName(None),
            req,
            None,
            10, // kept small to simulate a large file upload
            None,
            DigestFunctionConfig::default(),
            move |req: BatchUpdateBlobsRequest| {
                let res = res.clone();
                let digest1 = digest1.clone();
                async move {
                    assert_eq!(req.requests.len(), 1);
                    assert_eq!(req.requests[0].digest, Some(tdigest_to(digest1)));
                    assert_eq!(req.requests[0].data, b"aaa");
                    Ok(res)
                }
            },
            |write_reqs| {
                let blob_data = blob_data.clone();
                async move {
                    assert_eq!(write_reqs.len(), 2);
                    assert_eq!(write_reqs[0].write_offset, 0);
                    assert!(!write_reqs[0].finish_write);
                    assert_eq!(write_reqs[0].data, blob_data[..10]);
                    assert_eq!(write_reqs[1].write_offset, 10);
                    assert!(write_reqs[1].finish_write);
                    assert_eq!(write_reqs[1].data, blob_data[10..]);
                    anyhow::Ok(WriteResponse { committed_size: 18 })
                }
            },
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_upload_large_named_dedupes_concurrent_bytestream() -> anyhow::Result<()> {
        let blob_data = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];
        let blob_data_ref = blob_data.as_slice();
        let digest = digest_for_test_data(&blob_data);
        let writes = AtomicU16::new(0);

        let work = tempfile::tempdir()?;
        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path1, &blob_data).await?;

        let path2 = work.path().join("path2");
        let path2 = path2.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path2, &blob_data).await?;

        let req = UploadRequest {
            files_with_digest: Some(vec![
                NamedDigest {
                    name: path1.to_owned(),
                    digest: digest.clone(),
                    ..Default::default()
                },
                NamedDigest {
                    name: path2.to_owned(),
                    digest: digest.clone(),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        upload_impl(
            &InstanceName(None),
            req,
            None,
            10,
            None,
            DigestFunctionConfig::default(),
            |_req| async { panic!("A BatchUpdateBlobs upload should not be triggered") },
            |write_reqs| {
                writes.fetch_add(1, Ordering::Relaxed);
                async move {
                    tokio::task::yield_now().await;
                    let uploaded = write_reqs
                        .iter()
                        .flat_map(|req| req.data.iter().copied())
                        .collect::<Vec<_>>();
                    assert_eq!(uploaded.as_slice(), blob_data_ref);
                    anyhow::Ok(WriteResponse {
                        committed_size: blob_data_ref.len() as i64,
                    })
                }
            },
        )
        .await?;

        assert_eq!(writes.load(Ordering::Relaxed), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_large_inlined() -> anyhow::Result<()> {
        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };
        let blob_data1 = b"aaa".to_vec();

        let digest2 = TDigest {
            hash: "xl".to_owned(),
            size_in_bytes: 18,
            ..Default::default()
        };
        let blob_data2 = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];

        let req = UploadRequest {
            inlined_blobs_with_digest: Some(vec![
                InlinedBlobWithDigest {
                    blob: blob_data2.clone(),
                    digest: digest2.clone(),
                    ..Default::default()
                },
                InlinedBlobWithDigest {
                    blob: blob_data1.clone(),
                    digest: digest1.clone(),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        let res = BatchUpdateBlobsResponse {
            responses: vec![batch_update_blobs_response::Response {
                digest: Some(tdigest_to(digest1.clone())),
                status: Some(Status::default()),
            }],
        };

        upload_impl(
            &InstanceName(None),
            req,
            None,
            10, // kept small to simulate a large inlined upload
            None,
            DigestFunctionConfig::default(),
            move |req: BatchUpdateBlobsRequest| {
                let res = res.clone();
                let digest1 = digest1.clone();
                let blob_data1 = blob_data1.clone();
                async move {
                    assert_eq!(req.requests.len(), 1);
                    assert_eq!(req.requests[0].digest, Some(tdigest_to(digest1)));
                    assert_eq!(req.requests[0].data, blob_data1);
                    Ok(res)
                }
            },
            |write_reqs| {
                let blob_data2 = blob_data2.clone();
                async move {
                    assert_eq!(write_reqs.len(), 2);
                    assert_eq!(write_reqs[0].write_offset, 0);
                    assert!(!write_reqs[0].finish_write);
                    assert_eq!(write_reqs[0].data, blob_data2[..10]);
                    assert_eq!(write_reqs[1].write_offset, 10);
                    assert!(write_reqs[1].finish_write);
                    assert_eq!(write_reqs[1].data, blob_data2[10..]);
                    anyhow::Ok(WriteResponse { committed_size: 18 })
                }
            },
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_upload_large_inlined_dedupes_concurrent_bytestream() -> anyhow::Result<()> {
        let blob_data = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];
        let blob_data_ref = blob_data.as_slice();
        let digest = digest_for_test_data(&blob_data);
        let writes = AtomicU16::new(0);

        let req = UploadRequest {
            inlined_blobs_with_digest: Some(vec![
                InlinedBlobWithDigest {
                    blob: blob_data.clone(),
                    digest: digest.clone(),
                    ..Default::default()
                },
                InlinedBlobWithDigest {
                    blob: blob_data.clone(),
                    digest: digest.clone(),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        upload_impl(
            &InstanceName(None),
            req,
            None,
            10,
            None,
            DigestFunctionConfig::default(),
            |_req| async { panic!("A BatchUpdateBlobs upload should not be triggered") },
            |write_reqs| {
                writes.fetch_add(1, Ordering::Relaxed);
                async move {
                    tokio::task::yield_now().await;
                    let uploaded = write_reqs
                        .iter()
                        .flat_map(|req| req.data.iter().copied())
                        .collect::<Vec<_>>();
                    assert_eq!(uploaded.as_slice(), blob_data_ref);
                    anyhow::Ok(WriteResponse {
                        committed_size: blob_data_ref.len() as i64,
                    })
                }
            },
        )
        .await?;

        assert_eq!(writes.load(Ordering::Relaxed), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_invalid_committed_size() -> anyhow::Result<()> {
        let blob_data = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        ];

        let work = tempfile::tempdir()?;

        let path2 = work.path().join("path2");
        let path2 = path2.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path2, &blob_data).await?;

        let digest2 = TDigest {
            hash: "xl".to_owned(),
            size_in_bytes: 18,
            ..Default::default()
        };

        let req = UploadRequest {
            files_with_digest: Some(vec![NamedDigest {
                name: path2.to_owned(),
                digest: digest2.clone(),
                ..Default::default()
            }]),
            ..Default::default()
        };

        let resp: Result<UploadResponse, anyhow::Error> = upload_impl(
            &InstanceName(None), // TODO
            req,
            None,
            10,
            None,
            DigestFunctionConfig::default(),
            |_req| async move {
                panic!("This should not be called as there are no blobs to upload in batch");
            },
            |_write_reqs| async move {
                // Not the right size
                anyhow::Ok(WriteResponse { committed_size: 10 })
            },
        )
        .await;

        let err: anyhow::Error = resp.unwrap_err();
        // can't compare the full message because tempfile is used
        assert!(
            err.root_cause()
                .to_string()
                .contains("invalid committed_size")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_exact() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path1, "aaabbb").await?;

        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 6,
            ..Default::default()
        };

        let digest2 = TDigest {
            hash: "bb".to_owned(),
            size_in_bytes: 6,
            ..Default::default()
        };
        let blob_data2 = vec![1, 2, 3, 4, 5, 6];

        let req = UploadRequest {
            files_with_digest: Some(vec![NamedDigest {
                name: path1.to_owned(),
                digest: digest1.clone(),
                ..Default::default()
            }]),
            inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                blob: blob_data2.clone(),
                digest: digest2.clone(),
                ..Default::default()
            }]),
            ..Default::default()
        };

        upload_impl(
            &InstanceName(None),
            req,
            None,
            3,
            None,
            DigestFunctionConfig::default(),
            |_req| async move {
                panic!("Not called");
            },
            |write_reqs| async move {
                assert_eq!(write_reqs.len(), 2);
                assert!(write_reqs[1].finish_write);
                anyhow::Ok(WriteResponse { committed_size: 6 })
            },
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_upload_empty() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path1, "").await?;

        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 0,
            ..Default::default()
        };

        for compressor in [
            None,
            Some(Compressor::Deflate),
            Some(Compressor::Brotli),
            Some(Compressor::Zstd),
        ] {
            assert!(
                upload_impl(
                    &InstanceName(None),
                    UploadRequest {
                        files_with_digest: Some(vec![NamedDigest {
                            name: path1.to_owned(),
                            digest: digest1.clone(),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    },
                    compressor,
                    0, // max_total_batch_size=0 forces bytestream API
                    None,
                    DigestFunctionConfig::default(),
                    |_req| async move {
                        panic!("Not called");
                    },
                    |_write_reqs| async move {
                        panic!("Not called");
                    },
                )
                .await
                .is_ok()
            );

            assert!(
                upload_impl(
                    &InstanceName(None),
                    UploadRequest {
                        files_with_digest: Some(vec![NamedDigest {
                            name: path1.to_owned(),
                            digest: digest1.clone(),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    },
                    compressor,
                    1024, // forces the batch API
                    None,
                    DigestFunctionConfig::default(),
                    |_req| async move {
                        panic!("Not called");
                    },
                    |_write_reqs| async move {
                        panic!("Not called");
                    },
                )
                .await
                .is_ok()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_upload_resource_name() -> anyhow::Result<()> {
        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };
        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path1, "aaa").await?;

        let req = UploadRequest {
            inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                digest: digest1.clone(),
                blob: b"aaa".to_vec(),
                ..Default::default()
            }]),
            files_with_digest: Some(vec![NamedDigest {
                name: path1.to_owned(),
                digest: digest1.clone(),
                ..Default::default()
            }]),
            ..Default::default()
        };

        upload_impl(
            &InstanceName(Some("instance".to_owned())),
            req,
            None,
            1,
            None,
            DigestFunctionConfig::default(),
            |_req| async move {
                panic!("Not called");
            },
            |write_reqs| async move {
                assert!(write_reqs[0].resource_name.starts_with("instance/uploads/"));
                assert!(write_reqs[0].resource_name.ends_with("/blobs/aa/3"));
                anyhow::Ok(WriteResponse { committed_size: 3 })
            },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_sets_blake3_digest_function() -> anyhow::Result<()> {
        let blob = b"aaa".to_vec();
        let digest = blake3_digest_for_test_data(&blob);
        let config = DigestFunctionConfig::from_configured_algorithms(&["BLAKE3".into()]);

        upload_impl(
            &InstanceName(None),
            UploadRequest {
                inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                    digest: digest.clone(),
                    blob: blob.clone(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
            None,
            10000,
            None,
            config,
            {
                let digest = digest.clone();
                move |req: BatchUpdateBlobsRequest| {
                    let digest = digest.clone();
                    async move {
                        assert_eq!(req.digest_function, digest_function::Value::Blake3 as i32);
                        assert_eq!(req.requests.len(), 1);
                        Ok(BatchUpdateBlobsResponse {
                            responses: vec![batch_update_blobs_response::Response {
                                digest: Some(tdigest_to(digest)),
                                status: Some(Status::default()),
                            }],
                        })
                    }
                }
            },
            |_req| async { panic!("not called") },
        )
        .await?;

        upload_impl(
            &InstanceName(None),
            UploadRequest {
                inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                    digest: digest.clone(),
                    blob,
                    ..Default::default()
                }]),
                ..Default::default()
            },
            None,
            1,
            None,
            config,
            |_req| async { panic!("not called") },
            |write_reqs| {
                let digest = digest.clone();
                async move {
                    assert_eq!(write_reqs.len(), 3);
                    assert!(write_reqs[0].resource_name.starts_with("uploads/"));
                    assert!(
                        write_reqs
                            .iter()
                            .all(|req| req.resource_name == write_reqs[0].resource_name)
                    );
                    assert!(
                        write_reqs[0]
                            .resource_name
                            .ends_with(&format!("/blobs/blake3/{}/3", digest.hash))
                    );
                    anyhow::Ok(WriteResponse { committed_size: 3 })
                }
            },
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_resource_name_compressed() -> anyhow::Result<()> {
        let digest1 = TDigest {
            hash: "aa".to_owned(),
            size_in_bytes: 3,
            ..Default::default()
        };
        let work = tempfile::tempdir()?;

        let path1 = work.path().join("path1");
        let path1 = path1.to_str().context("tempdir is not utf8")?;
        tokio::fs::write(path1, "aaa").await?;

        let req = UploadRequest {
            inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                digest: digest1.clone(),
                blob: b"aaa".to_vec(),
                ..Default::default()
            }]),
            files_with_digest: Some(vec![NamedDigest {
                name: path1.to_owned(),
                digest: digest1.clone(),
                ..Default::default()
            }]),
            ..Default::default()
        };

        let active_uploads = ActiveTransferRegistry::new();
        super::upload_impl(
            &InstanceName(Some("instance".to_owned())),
            req,
            Some(Compressor::Zstd),
            None,
            1,
            0,
            None,
            DigestFunctionConfig::default(),
            &active_uploads,
            &SharedCallRegistry::new(),
            |_req| async move {
                panic!("Not called");
            },
            |write_reqs| async move {
                assert!(write_reqs[0].resource_name.starts_with("instance/uploads/"));
                assert!(
                    write_reqs[0]
                        .resource_name
                        .ends_with("/compressed-blobs/zstd/aa/3")
                );
                anyhow::Ok(WriteResponse { committed_size: -1 })
            },
        )
        .await?;

        Ok(())
    }

    fn named(path: &str, digest: &TDigest, is_executable: bool) -> NamedDigestWithPermissions {
        NamedDigestWithPermissions {
            named_digest: NamedDigest {
                name: path.to_owned(),
                digest: digest.clone(),
                ..Default::default()
            },
            is_executable,
            ..Default::default()
        }
    }

    fn tdigest(hash: &str, size: i64) -> TDigest {
        TDigest {
            hash: hash.to_owned(),
            size_in_bytes: size,
            ..Default::default()
        }
    }

    /// The part of buck2-casd this code relies on: serving any read of a blob, even a one-byte
    /// one, publishes the whole blob into the directory first.
    struct FakeDaemon {
        root: PathBuf,
        blobs: HashMap<String, Vec<u8>>,
        reads: std::sync::atomic::AtomicUsize,
    }

    impl FakeDaemon {
        fn new(work: &tempfile::TempDir, blobs: &[(&TDigest, &[u8])]) -> Self {
            Self {
                root: crate::shared_cache::tests::fake_daemon_dir(work.path()),
                blobs: blobs
                    .iter()
                    .map(|(d, data)| (d.hash.clone(), data.to_vec()))
                    .collect(),
                reads: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn cache(&self) -> SharedCasCache {
            SharedCasCache::new(self.root.clone(), CopyPolicy::Hybrid).unwrap()
        }

        fn serve(
            &self,
            req: ReadRequest,
        ) -> anyhow::Result<
            Pin<
                Box<futures::stream::Iter<std::vec::IntoIter<Result<ReadResponse, tonic::Status>>>>,
            >,
        > {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let parts: Vec<&str> = req.resource_name.split('/').collect();
            assert_eq!(parts[parts.len() - 3], "blobs", "{}", req.resource_name);
            let hash = parts[parts.len() - 2];
            let size: i64 = parts[parts.len() - 1].parse()?;
            let data = self
                .blobs
                .get(hash)
                .with_context(|| format!("fake daemon has no blob {hash}"))?;
            crate::shared_cache::tests::publish(&self.root, &tdigest(hash, size), data);
            let limit = if req.read_limit > 0 {
                req.read_limit as usize
            } else {
                data.len()
            };
            Ok(Box::pin(futures::stream::iter(vec![Ok(ReadResponse {
                data: data[..limit.min(data.len())].to_vec(),
            })])))
        }
    }

    #[tokio::test]
    async fn test_download_shared_cache_hit() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;
        let path = |name: &str| work.path().join(name).to_str().unwrap().to_owned();
        let digest1 = tdigest("aa", 3);
        let digest2 = tdigest("bb", 3);
        let daemon = FakeDaemon::new(&work, &[]);
        crate::shared_cache::tests::publish(&daemon.root, &digest1, &[1, 2, 3]);
        crate::shared_cache::tests::publish(&daemon.root, &digest2, &[4, 5, 6]);
        let cache = daemon.cache();
        let daemon = &daemon;
        let upstream_calls = std::sync::atomic::AtomicUsize::new(0);
        let upstream_calls = &upstream_calls;

        let response = test_download_impl_with_shared_cache(
            &InstanceName(None),
            DownloadRequest {
                file_digests: Some(vec![
                    named(&path("one"), &digest1, true),
                    named(&path("two"), &digest2, false),
                    named(&path("three"), &digest2, false),
                ]),
                ..Default::default()
            },
            None,
            10000,
            None,
            DigestFunctionConfig::default(),
            Some(&cache),
            |_req| async move {
                upstream_calls.fetch_add(1, Ordering::Relaxed);
                anyhow::Ok(BatchReadBlobsResponse::default())
            },
            |req| async move { daemon.serve(req) },
        )
        .await?;

        assert_eq!(
            upstream_calls.load(Ordering::Relaxed),
            0,
            "nothing to fetch"
        );
        assert_eq!(daemon.reads.load(Ordering::Relaxed), 0, "nothing to warm");
        assert_eq!(response.local_cache_stats.hits_files, 3);
        assert_eq!(response.local_cache_stats.hits_bytes, 9);
        assert_eq!(response.local_cache_stats.misses_files, 0);
        assert_eq!(tokio::fs::read(path("one")).await?, vec![1, 2, 3]);
        assert_eq!(tokio::fs::read(path("two")).await?, vec![4, 5, 6]);
        assert_eq!(tokio::fs::read(path("three")).await?, vec![4, 5, 6]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode =
                |name: &str| std::fs::metadata(path(name)).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode("one"), 0o755);
            assert_eq!(mode("two"), 0o644);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_download_shared_cache_miss_warms_daemon_then_clones() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;
        let path = |name: &str| work.path().join(name).to_str().unwrap().to_owned();
        let digest1 = tdigest("aa", 3);
        let digest2 = tdigest("bb", 3);
        let daemon = FakeDaemon::new(&work, &[(&digest1, &[1, 2, 3]), (&digest2, &[4, 5, 6])]);
        let cache = daemon.cache();
        let daemon = &daemon;
        let upstream_calls = std::sync::atomic::AtomicUsize::new(0);
        let upstream_calls = &upstream_calls;

        let response = test_download_impl_with_shared_cache(
            &InstanceName(None),
            DownloadRequest {
                file_digests: Some(vec![
                    named(&path("one"), &digest1, false),
                    named(&path("two"), &digest1, true),
                    named(&path("three"), &digest2, false),
                ]),
                ..Default::default()
            },
            None,
            10000,
            None,
            DigestFunctionConfig::default(),
            Some(&cache),
            |_req| async move {
                upstream_calls.fetch_add(1, Ordering::Relaxed);
                anyhow::Ok(BatchReadBlobsResponse::default())
            },
            |req| async move { daemon.serve(req) },
        )
        .await?;

        assert_eq!(
            daemon.reads.load(Ordering::Relaxed),
            2,
            "one warm-up per distinct digest"
        );
        assert_eq!(
            upstream_calls.load(Ordering::Relaxed),
            0,
            "everything came from the directory"
        );
        assert_eq!(response.local_cache_stats.hits_files, 0);
        assert_eq!(response.local_cache_stats.misses_files, 3);
        assert_eq!(response.local_cache_stats.misses_bytes, 9);
        assert_eq!(tokio::fs::read(path("one")).await?, vec![1, 2, 3]);
        assert_eq!(tokio::fs::read(path("two")).await?, vec![1, 2, 3]);
        assert_eq!(tokio::fs::read(path("three")).await?, vec![4, 5, 6]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode =
                |name: &str| std::fs::metadata(path(name)).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode("one"), 0o644);
            assert_eq!(mode("two"), 0o755);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_download_shared_cache_falls_back_to_grpc() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;
        let path = work.path().join("out").to_str().unwrap().to_owned();
        // Received over gRPC, so validated against its hash.
        let digest = digest_for_test_data(&[7, 8]);
        // The daemon's directory never gets this blob (say, it was evicted at once).
        let daemon = FakeDaemon::new(&work, &[]);
        let cache = daemon.cache();
        let daemon = &daemon;
        let res = BatchReadBlobsResponse {
            responses: vec![batch_read_blobs_response::Response {
                digest: Some(tdigest_to(digest.clone())),
                data: vec![7, 8],
                ..Default::default()
            }],
        };

        let response = test_download_impl_with_shared_cache(
            &InstanceName(None),
            DownloadRequest {
                file_digests: Some(vec![named(&path, &digest, false)]),
                ..Default::default()
            },
            None,
            10000,
            None,
            DigestFunctionConfig::default(),
            Some(&cache),
            |req| {
                let res = res.clone();
                async move {
                    assert_eq!(req.digests.len(), 1);
                    Ok(res)
                }
            },
            |req| async move { daemon.serve(req) },
        )
        .await?;

        assert_eq!(
            daemon.reads.load(Ordering::Relaxed),
            1,
            "warm-up was attempted"
        );
        assert_eq!(tokio::fs::read(&path).await?, vec![7, 8]);
        assert_eq!(response.local_cache_stats.misses_files, 1);
        assert_eq!(response.local_cache_stats.hits_files, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_download_empty_with_shared_cache() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;
        let path = work.path().join("empty").to_str().unwrap().to_owned();
        let digest = digest_for_test_data(&[]);
        let daemon = FakeDaemon::new(&work, &[]);
        let cache = daemon.cache();
        let daemon = &daemon;
        let response = test_download_impl_with_shared_cache(
            &InstanceName(None),
            DownloadRequest {
                file_digests: Some(vec![named(&path, &digest, true)]),
                ..Default::default()
            },
            None,
            10000,
            None,
            DigestFunctionConfig::default(),
            Some(&cache),
            |_req| async move { anyhow::Ok(BatchReadBlobsResponse::default()) },
            |req| async move { daemon.serve(req) },
        )
        .await?;
        assert_eq!(tokio::fs::read(&path).await?, Vec::<u8>::new());
        assert_eq!(daemon.reads.load(Ordering::Relaxed), 0);
        assert_eq!(response.local_cache_stats.total_cache_lookup_attempts, 0);
        Ok(())
    }

    #[test]
    fn test_substitute_env_vars() {
        let getter = |s: &str| match s {
            "FOO" => Ok("foo_value".to_owned()),
            "BAR" => Ok("bar_value".to_owned()),
            "BAZ" => Err(VarError::NotPresent),
            _ => panic!("Unexpected"),
        };

        assert_eq!(
            substitute_env_vars_impl("$FOO", getter).unwrap(),
            "foo_value"
        );
        assert_eq!(
            substitute_env_vars_impl("$FOO$BAR", getter).unwrap(),
            "foo_valuebar_value"
        );
        assert_eq!(
            substitute_env_vars_impl("some$FOO.bar", getter).unwrap(),
            "somefoo_value.bar"
        );
        assert_eq!(substitute_env_vars_impl("foo", getter).unwrap(), "foo");
        assert_eq!(substitute_env_vars_impl("FOO", getter).unwrap(), "FOO");
        assert!(substitute_env_vars_impl("$FOO$BAZ", getter).is_err());
    }

    #[test]
    fn test_trim_bystream_write_segments_partial() {
        let resource_name = "uploads/uuid/blobs/hash/18".to_owned();
        let segments = vec![
            WriteRequest {
                resource_name: resource_name.clone(),
                write_offset: 0,
                finish_write: false,
                data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            },
            WriteRequest {
                resource_name,
                write_offset: 10,
                finish_write: true,
                data: vec![11, 12, 13, 14, 15, 16, 17, 18],
            },
        ];

        assert_eq!(18, total_bystream_write_size(&segments));

        let resumed = trim_bystream_write_segments(segments, 12);
        assert_eq!(1, resumed.len());
        assert_eq!(12, resumed[0].write_offset);
        assert_eq!(vec![13, 14, 15, 16, 17, 18], resumed[0].data);
        assert!(resumed[0].finish_write);
    }

    #[test]
    fn test_trim_bystream_write_segments_no_trim() {
        let segments = vec![WriteRequest {
            resource_name: "uploads/uuid/blobs/hash/3".to_owned(),
            write_offset: 0,
            finish_write: true,
            data: vec![1, 2, 3],
        }];

        let resumed = trim_bystream_write_segments(segments.clone(), 0);
        assert_eq!(segments, resumed);
    }
}

#[tokio::test]
async fn test_upload_compressed() -> anyhow::Result<()> {
    let blob_data = vec![1; 10 * 1024 * 1024];
    let digest1 = TDigest {
        hash: "aa".to_owned(),
        size_in_bytes: blob_data.len() as i64,
        ..Default::default()
    };

    let req = UploadRequest {
        inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
            digest: digest1.clone(),
            blob: blob_data.clone(),
            ..Default::default()
        }]),
        ..Default::default()
    };

    let blob_data_ref = &blob_data;
    let active_uploads = ActiveTransferRegistry::new();
    upload_impl(
        &InstanceName(Some("instance".to_owned())),
        req,
        Some(Compressor::Zstd),
        None,
        1,
        DEFAULT_REMOTE_CACHE_COMPRESSION_THRESHOLD,
        None,
        DigestFunctionConfig::default(),
        &active_uploads,
        &SharedCallRegistry::new(),
        |_req| async move {
            panic!("Not called");
        },
        {
            |write_reqs| async move {
                let compressed_data: Vec<u8> =
                    write_reqs.iter().flat_map(|wr| wr.data.clone()).collect();
                let mut data = vec![];
                ZstdDecoder::new(Cursor::new(compressed_data))
                    .read_to_end(&mut data)
                    .await
                    .unwrap();
                assert_eq!(&data, blob_data_ref);
                anyhow::Ok(WriteResponse { committed_size: -1 })
            }
        },
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_download_compressed() -> anyhow::Result<()> {
    let blob_data = vec![1; 1024];

    let mut compressed_data = vec![];
    ZstdEncoder::new(Cursor::new(blob_data.clone()))
        .read_to_end(&mut compressed_data)
        .await
        .unwrap();
    let active_downloads = ActiveTransferRegistry::new();
    let d_resp = download_impl(
        &InstanceName(None),
        DownloadRequest {
            inlined_digests: Some(vec![TDigest {
                hash: format!("{:x}", Sha256::digest(&blob_data)),
                size_in_bytes: blob_data.len() as i64,
                ..Default::default()
            }]),
            file_digests: None,
            ..Default::default()
        },
        Some(Compressor::Zstd),
        10,
        DEFAULT_REMOTE_CACHE_COMPRESSION_THRESHOLD,
        None,
        DigestFunctionConfig::default(),
        0,
        Duration::from_millis(1),
        Duration::from_secs(DEFAULT_BYTESTREAM_PROGRESS_TIMEOUT_SECS),
        &active_downloads,
        None,
        |_req| async { panic!("not called") },
        |_req| {
            let compressed_data = compressed_data.clone();
            async move {
                let responses = compressed_data
                    .chunks(10)
                    .map(|d| Result::Ok(ReadResponse { data: d.to_vec() }))
                    .collect::<Vec<_>>();
                Ok(Box::pin(futures::stream::iter(responses)))
            }
        },
        || async {},
    )
    .await?;

    assert_eq!(
        d_resp.inlined_blobs.as_ref().unwrap()[0].blob.len(),
        blob_data.len()
    );
    assert_eq!(d_resp.inlined_blobs.unwrap()[0].blob, blob_data);
    Ok(())
}
