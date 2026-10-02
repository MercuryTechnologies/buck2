/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! buck2's own event log as a Build Event Protocol build tool log.
//!
//! Writing (daemon): at `CommandEnd` the Build Event Service sink holds back the stream's
//! `BuildToolLogs` event, waits for the client to finish the log it writes under
//! `buck-out/<isolation dir>/log`, writes the file to the CAS and names it in `BuildToolLogs` by
//! digest. The file is the client's own, so whatever reads it back reads what `buck2 log` reads
//! on the machine that ran the command.
//!
//! The client's log writer (`buck2 debug persist-event-logs`) holds an exclusive advisory lock
//! on the file from before its first byte until it has written its last, and the kernel drops
//! the lock when that process exits however it exits. A file that is non-empty and unlocked is
//! therefore finished.
//!
//! Reading (client): Build Event Protocol has no way to read an invocation back, so finding the
//! digest from a trace ID is backend-specific. [`EventLogLookup::BuildBuddyApi`] asks BuildBuddy's
//! `api.v1.ApiService/GetInvocation`. The bytes then come from the CAS with a plain ByteStream
//! `Read`, which any REAPI server answers.

use std::fs::File;
use std::fs::TryLockError;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use std::time::Instant;

use buck2_credential_helper::CredentialHelperSettings;
use buck2_error::ErrorTag;
use google_grpc_proto::google::bytestream::ReadRequest;
use google_grpc_proto::google::bytestream::byte_stream_client::ByteStreamClient;
use sha2::Digest as _;
use sha2::Sha256;
use tonic::Status;
use tonic::codegen::http::uri::PathAndQuery;

use crate::sink::bes_client::BesTls;
use crate::sink::bes_client::attach_headers;
use crate::sink::bes_client::bes_backend;
use crate::sink::bes_client::endpoint_for;

/// The `BuildToolLogs` entry of a log the client finished writing.
pub const EVENT_LOG_FILE_NAME: &str = "buck2-events.pb.zst";
/// The entry of a log still being written when the sink stopped waiting: a prefix of the stream,
/// which `buck2 log` reads up to its last complete event.
pub const INCOMPLETE_EVENT_LOG_FILE_NAME: &str = "buck2-events.incomplete.pb.zst";

/// The size of each ByteStream `WriteRequest`: under any server's 4 MiB default receive limit.
pub(crate) const EVENT_LOG_UPLOAD_CHUNK_BYTES: usize = 1024 * 1024;
const EVENT_LOG_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The event log the client writes for `trace_id`: `<time>_<command>_<trace id>_events.pb.zst`
/// (`buck2_event_log::file_names::get_logfile_name`). A `dl-` copy downloaded by `buck2 log` has
/// another name and is never taken for it.
pub(crate) fn find_event_log(log_dir: &Path, trace_id: &str) -> Option<PathBuf> {
    let suffix = format!("_{trace_id}_events.pb.zst");
    std::fs::read_dir(log_dir)
        .ok()?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|t| t.is_file()))
        .find(|entry| entry.file_name().to_string_lossy().ends_with(&suffix))
        .map(|entry| entry.path())
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum EventLogState {
    /// No file yet, or one the writer has opened and not yet written to.
    NotStarted,
    /// The writer still holds the lock.
    Writing { size: u64 },
    /// Non-empty and unlocked: the writer has exited.
    Finished { size: u64 },
}

pub(crate) fn event_log_state(path: &Path) -> EventLogState {
    let Ok(file) = File::open(path) else {
        return EventLogState::NotStarted;
    };
    let size = file.metadata().map_or(0, |m| m.len());
    if size == 0 {
        return EventLogState::NotStarted;
    }
    match file.try_lock_shared() {
        Ok(()) => EventLogState::Finished { size },
        Err(TryLockError::WouldBlock) => EventLogState::Writing { size },
        // A filesystem without advisory locks: nothing says the writer is done, so the caller
        // waits out its timeout and attaches what is there.
        Err(TryLockError::Error(_)) => EventLogState::Writing { size },
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FoundEventLog {
    pub(crate) path: PathBuf,
    /// The bytes to upload. For a log still being written, its size when the wait ended.
    pub(crate) size: u64,
    pub(crate) complete: bool,
}

/// Waits up to `timeout` for the client to finish the log for `trace_id`.
pub(crate) async fn wait_for_event_log(
    log_dir: &Path,
    trace_id: &str,
    timeout: Duration,
) -> Option<FoundEventLog> {
    let deadline = Instant::now() + timeout;
    loop {
        let path = find_event_log(log_dir, trace_id);
        let state = path
            .as_deref()
            .map_or(EventLogState::NotStarted, event_log_state);
        match (path, state) {
            (Some(path), EventLogState::Finished { size }) => {
                return Some(FoundEventLog {
                    path,
                    size,
                    complete: true,
                });
            }
            (path, state) if Instant::now() >= deadline => {
                return match (path, state) {
                    (Some(path), EventLogState::Writing { size }) => Some(FoundEventLog {
                        path,
                        size,
                        complete: false,
                    }),
                    _ => None,
                };
            }
            _ => tokio::time::sleep(EVENT_LOG_POLL_INTERVAL).await,
        }
    }
}

/// How `buck2 log --trace-id` finds an invocation's build tool logs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventLogLookup {
    /// BuildBuddy's `api.v1.ApiService/GetInvocation` with `include_build_tool_logs`.
    BuildBuddyApi,
}

impl FromStr for EventLogLookup {
    type Err = buck2_error::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "buildbuddy_api" => Ok(Self::BuildBuddyApi),
            value => Err(buck2_error::buck2_error!(
                ErrorTag::Input,
                "Invalid `bes.event_log_lookup` value `{}` (expected `buildbuddy_api`)",
                value
            )),
        }
    }
}

/// Everything `buck2 log` needs to fetch a log by trace ID from a Build Event Service backend.
#[derive(Clone, Debug)]
pub struct EventLogDownloadConfig {
    pub lookup: EventLogLookup,
    /// `grpc[s]://host:port` serving the lookup: the BES backend unless configured otherwise.
    pub lookup_backend: String,
    /// `grpc[s]://host:port` of the CAS the log was written to.
    pub cas_backend: String,
    /// The instance name the URI does not carry. Unused when it does.
    pub instance_name: String,
    /// Already expanded: `[bes] header`, else `[buck2_re_client] http_headers`.
    pub headers: Vec<(String, String)>,
    pub tls: BesTls,
    pub credential_helper: Option<CredentialHelperSettings>,
    pub timeout: Duration,
    /// `[bes] results_url`, to name the invocation in errors.
    pub results_url: Option<String>,
}

#[derive(Debug)]
pub struct DownloadedEventLog {
    pub uri: String,
    pub size: u64,
    /// False for a log the sink attached before its writer finished.
    pub complete: bool,
}

#[derive(Debug, buck2_error::Error)]
#[buck2(tag = LogCmd)]
pub enum EventLogDownloadError {
    #[error("The Build Event Service backend has no invocation `{0}`, or this key may not read it")]
    NoInvocation(String),
    #[error(
        "Invocation `{0}` has no event log attached{1}. It is attached at the end of a command whose daemon has `[bes] upload_event_log = true`"
    )]
    NotAttached(String, String),
    #[error("The event log of invocation `{0}` is no longer in the CAS (`{1}`){2}")]
    Evicted(String, String, String),
    #[error(
        "Unrecognised event log URI `{0}` (expected `bytestream://HOST/[INSTANCE/]blobs/HASH/SIZE`)"
    )]
    BadUri(String),
    #[error("The event log read from `{uri}` is {actual}, expected {expected}")]
    DigestMismatch {
        uri: String,
        expected: String,
        actual: String,
    },
    #[error("{0} failed: {1}")]
    Rpc(&'static str, String),
}

pub(crate) mod buildbuddy_api {
    //! The fields of BuildBuddy's `proto/api/v1` the lookup reads (v2.310.0 field numbers).

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct GetInvocationRequest {
        #[prost(message, optional, tag = "1")]
        pub selector: Option<InvocationSelector>,
        #[prost(bool, tag = "6")]
        pub include_build_tool_logs: bool,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct InvocationSelector {
        #[prost(string, tag = "1")]
        pub invocation_id: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct GetInvocationResponse {
        #[prost(message, repeated, tag = "1")]
        pub invocation: Vec<Invocation>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Invocation {
        #[prost(message, optional, tag = "1")]
        pub id: Option<InvocationId>,
        #[prost(bool, tag = "3")]
        pub success: bool,
        #[prost(int32, tag = "25")]
        pub invocation_status: i32,
        #[prost(message, repeated, tag = "27")]
        pub build_tool_logs: Vec<File>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct InvocationId {
        #[prost(string, tag = "1")]
        pub invocation_id: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct File {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub uri: String,
        #[prost(string, tag = "3")]
        pub hash: String,
        #[prost(int64, tag = "4")]
        pub size_bytes: i64,
    }

    pub const GET_INVOCATION: &str = "/api.v1.ApiService/GetInvocation";
    pub const PARTIAL_INVOCATION_STATUS: i32 = 2;
    pub const DISCONNECTED_INVOCATION_STATUS: i32 = 3;
}

/// A `bytestream://` URI split into the resource name to read and the digest it names.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BytestreamBlob {
    pub(crate) resource_name: String,
    pub(crate) hash: String,
    pub(crate) size: u64,
}

pub(crate) fn parse_bytestream_uri(uri: &str) -> Option<BytestreamBlob> {
    let rest = uri.strip_prefix("bytestream://")?;
    let (_authority, path) = rest.split_once('/')?;
    let parts: Vec<&str> = path.split('/').collect();
    // [instance...]/blobs/HASH/SIZE, or [instance...]/blobs/FUNCTION/HASH/SIZE (REAPI 2.1+).
    let blobs = parts.iter().rposition(|p| *p == "blobs")?;
    let tail = &parts[blobs + 1..];
    let (hash, size) = match tail {
        [hash, size] | [_, hash, size] => (*hash, size.parse::<u64>().ok()?),
        _ => return None,
    };
    if hash.is_empty() {
        return None;
    }
    Some(BytestreamBlob {
        resource_name: path.to_owned(),
        hash: hash.to_owned(),
        size,
    })
}

async fn channel_for(
    backend: &str,
    config: &EventLogDownloadConfig,
) -> buck2_error::Result<tonic::transport::Channel> {
    let endpoint = bes_backend(Some(backend))?;
    endpoint_for(&endpoint, config.timeout, &config.tls)
        .map_err(|s| EventLogDownloadError::Rpc("Connecting", s.message().to_owned()))?
        .connect()
        .await
        .map_err(|e| EventLogDownloadError::Rpc("Connecting", format!("{backend}: {e}")).into())
}

async fn request_with_headers<T>(
    message: T,
    backend: &str,
    config: &EventLogDownloadConfig,
    helper: Option<&buck2_credential_helper::CredentialHelper>,
) -> Result<tonic::Request<T>, Status> {
    let mut request = tonic::Request::new(message);
    attach_headers(&mut request, &config.headers, helper, backend).await?;
    Ok(request)
}

/// Finds the event log of invocation `trace_id` through the configured lookup.
async fn lookup_event_log(
    config: &EventLogDownloadConfig,
    helper: Option<&buck2_credential_helper::CredentialHelper>,
    trace_id: &str,
) -> buck2_error::Result<buildbuddy_api::File> {
    match config.lookup {
        EventLogLookup::BuildBuddyApi => {}
    }
    let channel = channel_for(&config.lookup_backend, config).await?;
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready()
        .await
        .map_err(|e| EventLogDownloadError::Rpc("GetInvocation", e.to_string()))?;
    let request = request_with_headers(
        buildbuddy_api::GetInvocationRequest {
            selector: Some(buildbuddy_api::InvocationSelector {
                invocation_id: trace_id.to_owned(),
            }),
            include_build_tool_logs: true,
        },
        &config.lookup_backend,
        config,
        helper,
    )
    .await
    .map_err(|s| EventLogDownloadError::Rpc("GetInvocation", status_text(&s)))?;
    let response: buildbuddy_api::GetInvocationResponse = grpc
        .unary(
            request,
            PathAndQuery::from_static(buildbuddy_api::GET_INVOCATION),
            tonic_prost::ProstCodec::default(),
        )
        .await
        .map_err(|s| EventLogDownloadError::Rpc("GetInvocation", status_text(&s)))?
        .into_inner();
    let invocation = response
        .invocation
        .into_iter()
        .next()
        .ok_or_else(|| EventLogDownloadError::NoInvocation(trace_id.to_owned()))?;
    let pick = |name: &str| {
        invocation
            .build_tool_logs
            .iter()
            .find(|f| f.name == name && !f.uri.is_empty())
            .cloned()
    };
    if let Some(file) = pick(EVENT_LOG_FILE_NAME).or_else(|| pick(INCOMPLETE_EVENT_LOG_FILE_NAME)) {
        return Ok(file);
    }
    let why = match invocation.invocation_status {
        buildbuddy_api::PARTIAL_INVOCATION_STATUS => {
            " yet: the command is still running, or its stream has not ended".to_owned()
        }
        buildbuddy_api::DISCONNECTED_INVOCATION_STATUS => {
            ": its stream broke before the command ended (was the daemon killed?)".to_owned()
        }
        _ => String::new(),
    };
    Err(EventLogDownloadError::NotAttached(trace_id.to_owned(), why).into())
}

fn status_text(status: &Status) -> String {
    format!("{:?}: {}", status.code(), status.message())
}

/// Fetches the event log of invocation `trace_id` into `dest`, checking it against its digest.
pub async fn download_event_log(
    config: &EventLogDownloadConfig,
    trace_id: &str,
    dest: &Path,
) -> buck2_error::Result<DownloadedEventLog> {
    let helper = config
        .credential_helper
        .as_ref()
        .map(|settings| settings.build())
        .transpose()
        .map_err(|e| buck2_error::buck2_error!(ErrorTag::Input, "{e:#}"))?;
    let file = lookup_event_log(config, helper.as_ref(), trace_id).await?;
    let blob = parse_bytestream_uri(&file.uri)
        .ok_or_else(|| EventLogDownloadError::BadUri(file.uri.clone()))?;
    let resource_name =
        if blob.resource_name.starts_with("blobs/") && !config.instance_name.is_empty() {
            format!(
                "{}/{}",
                config.instance_name.trim_matches('/'),
                blob.resource_name
            )
        } else {
            blob.resource_name.clone()
        };

    let channel = channel_for(&config.cas_backend, config).await?;
    let mut client = ByteStreamClient::new(channel).max_decoding_message_size(64 * 1024 * 1024);
    let request = request_with_headers(
        ReadRequest {
            resource_name,
            read_offset: 0,
            read_limit: 0,
        },
        &config.cas_backend,
        config,
        helper.as_ref(),
    )
    .await
    .map_err(|s| EventLogDownloadError::Rpc("ByteStream.Read", status_text(&s)))?;
    let evicted = |status: &Status| -> buck2_error::Error {
        if status.code() == tonic::Code::NotFound {
            let hint = config
                .results_url
                .as_deref()
                .and_then(|url| file_download_url(url, trace_id, &file.uri))
                .map(|url| {
                    format!(
                        ". BuildBuddy keeps a copy of each build tool log, served at {url} on its web port"
                    )
                })
                .unwrap_or_default();
            EventLogDownloadError::Evicted(trace_id.to_owned(), file.uri.clone(), hint).into()
        } else {
            EventLogDownloadError::Rpc("ByteStream.Read", status_text(status)).into()
        }
    };
    let mut stream = client
        .read(request)
        .await
        .map_err(|s| evicted(&s))?
        .into_inner();

    let mut out = File::create(dest).map_err(|e| {
        buck2_error::buck2_error!(ErrorTag::Tier0, "creating `{}`: {e}", dest.display())
    })?;
    let mut hasher = Sha256::new();
    let mut size = 0u64;
    loop {
        match stream.message().await {
            Ok(Some(chunk)) => {
                hasher.update(&chunk.data);
                size += chunk.data.len() as u64;
                out.write_all(&chunk.data).map_err(|e| {
                    buck2_error::buck2_error!(ErrorTag::Tier0, "writing `{}`: {e}", dest.display())
                })?;
            }
            Ok(None) => break,
            Err(status) => return Err(evicted(&status)),
        }
    }
    out.flush().map_err(|e| {
        buck2_error::buck2_error!(ErrorTag::Tier0, "writing `{}`: {e}", dest.display())
    })?;
    let actual = format!("{:x}/{}", hasher.finalize(), size);
    let expected = format!("{}/{}", blob.hash, blob.size);
    if actual != expected {
        return Err(EventLogDownloadError::DigestMismatch {
            uri: file.uri,
            expected,
            actual,
        }
        .into());
    }
    Ok(DownloadedEventLog {
        complete: file.name == EVENT_LOG_FILE_NAME,
        uri: file.uri,
        size,
    })
}

/// BuildBuddy's `/file/download`, which falls back to the copy it persisted of each build tool
/// log once the CAS has evicted the blob. Served on the web port, not the gRPC one.
fn file_download_url(results_url: &str, trace_id: &str, uri: &str) -> Option<String> {
    let (scheme, rest) = results_url.split_once("://")?;
    let origin = rest.split('/').next()?;
    Some(format!(
        "{scheme}://{origin}/file/download?invocation_id={trace_id}&bytestream_url={}",
        percent_encode(uri)
    ))
}

fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_event_log_matches_the_client_log_only() {
        let dir = tempfile::tempdir().unwrap();
        let id = "0a1b2c3d-0000-4000-8000-000000000001";
        std::fs::write(dir.path().join(format!("dl-{id}.pb.zst")), b"x").unwrap();
        std::fs::write(
            dir.path().join("20261002-000000_build_other_events.pb.zst"),
            b"x",
        )
        .unwrap();
        assert_eq!(find_event_log(dir.path(), id), None);
        let log = dir
            .path()
            .join(format!("20261002-000000_build_{id}_events.pb.zst"));
        std::fs::write(&log, b"x").unwrap();
        assert_eq!(find_event_log(dir.path(), id), Some(log));
    }

    #[test]
    fn a_locked_log_is_still_being_written() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log");
        assert_eq!(event_log_state(&path), EventLogState::NotStarted);
        let writer = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        writer.lock().unwrap();
        assert_eq!(event_log_state(&path), EventLogState::NotStarted);
        (&writer).write_all(b"abc").unwrap();
        assert_eq!(event_log_state(&path), EventLogState::Writing { size: 3 });
        drop(writer);
        assert_eq!(event_log_state(&path), EventLogState::Finished { size: 3 });
    }

    #[tokio::test]
    async fn wait_for_event_log_returns_the_prefix_written_by_the_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let id = "0a1b2c3d-0000-4000-8000-000000000002";
        let path = dir.path().join(format!("t_build_{id}_events.pb.zst"));
        let writer = File::create(&path).unwrap();
        writer.lock().unwrap();
        (&writer).write_all(b"abcd").unwrap();
        let found = wait_for_event_log(dir.path(), id, Duration::from_millis(250)).await;
        assert_eq!(
            found,
            Some(FoundEventLog {
                path: path.clone(),
                size: 4,
                complete: false
            })
        );
        drop(writer);
        let found = wait_for_event_log(dir.path(), id, Duration::from_millis(250)).await;
        assert_eq!(
            found,
            Some(FoundEventLog {
                path,
                size: 4,
                complete: true
            })
        );
        assert_eq!(
            wait_for_event_log(dir.path(), "missing", Duration::from_millis(150)).await,
            None
        );
    }

    #[test]
    fn parses_bytestream_uris() {
        assert_eq!(
            parse_bytestream_uri("bytestream://cas:443/blobs/abc/12"),
            Some(BytestreamBlob {
                resource_name: "blobs/abc/12".to_owned(),
                hash: "abc".to_owned(),
                size: 12
            })
        );
        assert_eq!(
            parse_bytestream_uri("bytestream://cas/a/b/blobs/blake3/abc/12"),
            Some(BytestreamBlob {
                resource_name: "a/b/blobs/blake3/abc/12".to_owned(),
                hash: "abc".to_owned(),
                size: 12
            })
        );
        assert_eq!(parse_bytestream_uri("https://cas/blobs/abc/12"), None);
        assert_eq!(parse_bytestream_uri("bytestream://cas/blobs/abc"), None);
    }

    #[test]
    fn file_download_url_is_on_the_results_origin() {
        assert_eq!(
            file_download_url(
                "https://bb.example/invocation/",
                "id",
                "bytestream://cas:443/blobs/abc/1"
            )
            .as_deref(),
            Some(
                "https://bb.example/file/download?invocation_id=id&bytestream_url=bytestream%3A%2F%2Fcas%3A443%2Fblobs%2Fabc%2F1"
            )
        );
    }
}
