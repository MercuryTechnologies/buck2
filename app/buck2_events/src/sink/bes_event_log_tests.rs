/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! `[bes] upload_event_log` and `buck2 log --trace-id` against a fake backend: one gRPC server
//! that is a Build Event Service, a ByteStream CAS and BuildBuddy's invocation API.

use std::convert::Infallible;
use std::task::Context;
use std::task::Poll;

use bazel_bep_proto::build_event_stream as bep;
use bes_grpc_proto::google::devtools::build::v1::PublishLifecycleEventRequest;
use bes_grpc_proto::google::devtools::build::v1::publish_build_event_server::PublishBuildEvent;
use bes_grpc_proto::google::devtools::build::v1::publish_build_event_server::PublishBuildEventServer;
use buck2_wrapper_common::invocation_id::TraceId;
use google_grpc_proto::google::bytestream::QueryWriteStatusRequest;
use google_grpc_proto::google::bytestream::QueryWriteStatusResponse;
use google_grpc_proto::google::bytestream::ReadRequest;
use google_grpc_proto::google::bytestream::ReadResponse;
use google_grpc_proto::google::bytestream::WriteResponse;
use google_grpc_proto::google::bytestream::byte_stream_server::ByteStream;
use google_grpc_proto::google::bytestream::byte_stream_server::ByteStreamServer;
use prost::Message as _;
use sha2::Digest as _;
use tonic::codegen::http;

use super::*;
use crate::sink::bes_event_log::EVENT_LOG_FILE_NAME;
use crate::sink::bes_event_log::EventLogDownloadConfig;
use crate::sink::bes_event_log::EventLogLookup;
use crate::sink::bes_event_log::INCOMPLETE_EVENT_LOG_FILE_NAME;
use crate::sink::bes_event_log::buildbuddy_api as api;
use crate::sink::bes_event_log::download_event_log;

const API_KEY_HEADER: &str = "x-buildbuddy-api-key";
const API_KEY: &str = "test-key";

#[derive(Default)]
struct Backend {
    /// CAS blobs by SHA-256.
    blobs: std::sync::Mutex<HashMap<String, Vec<u8>>>,
    /// The Bazel events of each invocation, in order, and whether its stream finished.
    invocations: std::sync::Mutex<HashMap<String, (Vec<bep::BuildEvent>, bool)>>,
    /// `(rpc, value of the API key header)` for every call.
    calls: std::sync::Mutex<Vec<(&'static str, Option<String>)>>,
    /// The data size of every ByteStream `WriteRequest`.
    write_chunks: std::sync::Mutex<Vec<usize>>,
}

impl Backend {
    fn record_call<T>(&self, rpc: &'static str, request: &tonic::Request<T>) {
        let key = request
            .metadata()
            .get(API_KEY_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        self.calls.lock().unwrap().push((rpc, key));
    }

    fn build_tool_logs(&self, invocation_id: &str) -> Vec<bep::File> {
        let invocations = self.invocations.lock().unwrap();
        let Some((events, _)) = invocations.get(invocation_id) else {
            return Vec::new();
        };
        events
            .iter()
            .filter_map(|event| match &event.payload {
                Some(bep::build_event::Payload::BuildToolLogs(logs)) => Some(logs.log.clone()),
                _ => None,
            })
            .flatten()
            .collect()
    }

    fn last_event_is_build_tool_logs(&self, invocation_id: &str) -> bool {
        let invocations = self.invocations.lock().unwrap();
        invocations[invocation_id]
            .0
            .last()
            .is_some_and(|event| event.last_message && is_build_tool_logs(event))
    }
}

#[derive(Clone)]
struct FakeBes(Arc<Backend>);

#[tonic::async_trait]
impl PublishBuildEvent for FakeBes {
    async fn publish_lifecycle_event(
        &self,
        _request: tonic::Request<PublishLifecycleEventRequest>,
    ) -> Result<tonic::Response<()>, Status> {
        Ok(tonic::Response::new(()))
    }

    type PublishBuildToolEventStreamStream =
        ReceiverStream<Result<PublishBuildToolEventStreamResponse, Status>>;

    async fn publish_build_tool_event_stream(
        &self,
        request: tonic::Request<tonic::Streaming<PublishBuildToolEventStreamRequest>>,
    ) -> Result<tonic::Response<Self::PublishBuildToolEventStreamStream>, Status> {
        self.0.record_call("PublishBuildToolEventStream", &request);
        let backend = self.0.clone();
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            while let Ok(Some(request)) = inbound.message().await {
                let Some(ordered) = request.ordered_build_event else {
                    continue;
                };
                let invocation_id = ordered
                    .stream_id
                    .as_ref()
                    .map(|s| s.invocation_id.clone())
                    .unwrap_or_default();
                {
                    let mut invocations = backend.invocations.lock().unwrap();
                    let entry = invocations.entry(invocation_id).or_default();
                    match ordered.event.as_ref().and_then(|e| e.event.as_ref()) {
                        Some(build_event::Event::BazelEvent(any)) => {
                            entry
                                .0
                                .push(bep::BuildEvent::decode(any.value.as_slice()).unwrap());
                        }
                        Some(build_event::Event::ComponentStreamFinished(_)) => entry.1 = true,
                        _ => {}
                    }
                }
                let response = PublishBuildToolEventStreamResponse {
                    stream_id: ordered.stream_id,
                    sequence_number: ordered.sequence_number,
                };
                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

#[derive(Clone)]
struct FakeCas(Arc<Backend>);

fn hash_in_resource_name(name: &str) -> Option<String> {
    let parts: Vec<&str> = name.split('/').collect();
    let blobs = parts.iter().rposition(|p| *p == "blobs")?;
    parts.get(blobs + 1).map(|h| (*h).to_owned())
}

#[tonic::async_trait]
impl ByteStream for FakeCas {
    type ReadStream = ReceiverStream<Result<ReadResponse, Status>>;

    async fn read(
        &self,
        request: tonic::Request<ReadRequest>,
    ) -> Result<tonic::Response<Self::ReadStream>, Status> {
        self.0.record_call("ByteStream.Read", &request);
        let hash = hash_in_resource_name(&request.get_ref().resource_name)
            .ok_or_else(|| Status::invalid_argument("bad resource name"))?;
        let data = self
            .0
            .blobs
            .lock()
            .unwrap()
            .get(&hash)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("blob {hash} not found")))?;
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            for chunk in data.chunks(64 * 1024) {
                if tx
                    .send(Ok(ReadResponse {
                        data: chunk.to_vec(),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn write(
        &self,
        request: tonic::Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<tonic::Response<WriteResponse>, Status> {
        self.0.record_call("ByteStream.Write", &request);
        let mut inbound = request.into_inner();
        let mut resource_name = String::new();
        let mut data = Vec::new();
        while let Some(request) = inbound.message().await? {
            if resource_name.is_empty() {
                resource_name = request.resource_name.clone();
            }
            if request.write_offset != data.len() as i64 {
                return Err(Status::invalid_argument("write_offset out of order"));
            }
            self.0.write_chunks.lock().unwrap().push(request.data.len());
            data.extend_from_slice(&request.data);
            if request.finish_write {
                break;
            }
        }
        let hash = hash_in_resource_name(&resource_name)
            .ok_or_else(|| Status::invalid_argument("bad resource name"))?;
        let actual = format!("{:x}", Sha256::digest(&data));
        if actual != hash {
            return Err(Status::invalid_argument("digest mismatch"));
        }
        let committed_size = data.len() as i64;
        self.0.blobs.lock().unwrap().insert(hash, data);
        Ok(tonic::Response::new(WriteResponse { committed_size }))
    }

    async fn query_write_status(
        &self,
        _request: tonic::Request<QueryWriteStatusRequest>,
    ) -> Result<tonic::Response<QueryWriteStatusResponse>, Status> {
        Err(Status::unimplemented("query_write_status"))
    }
}

/// BuildBuddy's `api.v1.ApiService`, `GetInvocation` only, answered from what the fake BES saw.
#[derive(Clone)]
struct FakeApi(Arc<Backend>);

impl tonic::server::NamedService for FakeApi {
    const NAME: &'static str = "api.v1.ApiService";
}

struct GetInvocation(Arc<Backend>);

impl tonic::server::UnaryService<api::GetInvocationRequest> for GetInvocation {
    type Response = api::GetInvocationResponse;
    type Future = tonic::codegen::BoxFuture<tonic::Response<Self::Response>, Status>;

    fn call(&mut self, request: tonic::Request<api::GetInvocationRequest>) -> Self::Future {
        let backend = self.0.clone();
        Box::pin(async move {
            backend.record_call("GetInvocation", &request);
            if request.metadata().get(API_KEY_HEADER).is_none() {
                return Err(Status::unauthenticated("no API key"));
            }
            let id = request
                .into_inner()
                .selector
                .map(|s| s.invocation_id)
                .unwrap_or_default();
            let finished = match backend.invocations.lock().unwrap().get(&id) {
                None => {
                    return Ok(tonic::Response::new(api::GetInvocationResponse {
                        invocation: Vec::new(),
                    }));
                }
                Some((_, finished)) => *finished,
            };
            let build_tool_logs = backend
                .build_tool_logs(&id)
                .into_iter()
                .map(|file| api::File {
                    name: file.name,
                    uri: match file.file {
                        Some(bep::file::File::Uri(uri)) => uri,
                        _ => String::new(),
                    },
                    hash: file.digest,
                    size_bytes: file.length,
                })
                .collect();
            Ok(tonic::Response::new(api::GetInvocationResponse {
                invocation: vec![api::Invocation {
                    id: Some(api::InvocationId { invocation_id: id }),
                    success: true,
                    invocation_status: if finished { 1 } else { 2 },
                    build_tool_logs,
                }],
            }))
        })
    }
}

impl<B> tonic::codegen::Service<http::Request<B>> for FakeApi
where
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let backend = self.0.clone();
        Box::pin(async move {
            let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
            Ok(grpc.unary(GetInvocation(backend), request).await)
        })
    }
}

async fn serve_backend() -> (String, Arc<Backend>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("local addr");
    let backend = Arc::new(Backend::default());
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(PublishBuildEventServer::new(FakeBes(backend.clone())))
            .add_service(
                ByteStreamServer::new(FakeCas(backend.clone()))
                    .max_decoding_message_size(64 * 1024 * 1024),
            )
            .add_service(FakeApi(backend.clone()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    (format!("127.0.0.1:{}", address.port()), backend)
}

fn worker(host: &str, log_dir: &Path, timeout: Duration) -> WorkerState {
    let config = BesConfig {
        event_format: BesEventFormat::Bazel,
        grpc_timeout: Duration::from_secs(5),
        bazel_artifact_upload_backend: Some(format!("grpc://{host}")),
        upload_event_log: true,
        event_log_dir: Some(log_dir.to_path_buf()),
        event_log_upload_timeout: timeout,
        ..BesConfig::default()
    };
    let connection = ConnectionConfig {
        endpoint: format!("http://{host}"),
        headers: vec![(API_KEY_HEADER.to_owned(), API_KEY.to_owned())],
        tls: BesTls::default(),
        credential_helper: None,
    };
    WorkerState::new(config, connection, Arc::new(CounterState::default()))
}

fn download_config(host: &str, key: Option<&str>) -> EventLogDownloadConfig {
    EventLogDownloadConfig {
        lookup: EventLogLookup::BuildBuddyApi,
        lookup_backend: format!("grpc://{host}"),
        cas_backend: format!("grpc://{host}"),
        instance_name: String::new(),
        headers: key
            .map(|key| vec![(API_KEY_HEADER.to_owned(), key.to_owned())])
            .unwrap_or_default(),
        tls: BesTls::default(),
        credential_helper: None,
        timeout: Duration::from_secs(5),
        results_url: Some(format!("https://{host}/invocation/")),
    }
}

fn message(trace_id: &str, data: buck2_data::buck_event::Data) -> Message {
    let event = buck2_data::BuckEvent {
        timestamp: Some(SystemTime::now().into()),
        trace_id: trace_id.to_owned(),
        span_id: 1,
        parent_id: 0,
        data: Some(data),
    };
    Message {
        category: "test".to_owned(),
        message: event.encode_to_vec(),
        message_key: Some(1),
    }
}

fn command_start() -> buck2_data::buck_event::Data {
    buck2_data::buck_event::Data::SpanStart(buck2_data::SpanStartEvent {
        data: Some(buck2_data::CommandStart::default().into()),
    })
}

fn command_end(is_success: bool) -> buck2_data::buck_event::Data {
    buck2_data::buck_event::Data::SpanEnd(buck2_data::SpanEndEvent {
        data: Some(
            buck2_data::CommandEnd {
                is_success,
                ..Default::default()
            }
            .into(),
        ),
        ..Default::default()
    })
}

/// A stand-in for the client's log writer: holds the lock `persist-event-logs` holds.
struct ClientLog {
    path: PathBuf,
    file: Option<std::fs::File>,
}

impl ClientLog {
    fn start(dir: &Path, trace_id: &str, contents: &[u8]) -> Self {
        let path = dir.join(format!("20261002-120000_build_{trace_id}_events.pb.zst"));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        file.lock().unwrap();
        (&file).write_all(contents).unwrap();
        Self {
            path,
            file: Some(file),
        }
    }

    fn append(&self, contents: &[u8]) {
        (self.file.as_ref().unwrap()).write_all(contents).unwrap();
    }

    fn finish(&mut self) {
        self.file = None;
    }
}

use std::io::Write as _;

/// Drives the worker's poll loop until the invocation's stream has closed.
async fn run_until_closed(worker: &mut WorkerState, trace_id: &str, limit: Duration) {
    let deadline = Instant::now() + limit;
    while worker.streams.contains_key(trace_id) {
        assert!(
            Instant::now() < deadline,
            "stream for {trace_id} never closed"
        );
        worker.close_due_streams().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Pseudo-random bytes, so a test log neither compresses nor repeats.
fn bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

async fn run_command(
    worker: &mut WorkerState,
    trace_id: &str,
    is_success: bool,
    log: &mut ClientLog,
    tail: &[u8],
) {
    worker
        .send_message_with_retry(&message(trace_id, command_start()), false)
        .await
        .unwrap();
    worker
        .send_message_with_retry(&message(trace_id, command_end(is_success)), false)
        .await
        .unwrap();
    // The client writes the result after `CommandEnd`, and only then lets go of the file.
    for _ in 0..5 {
        worker.close_due_streams().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        worker.streams.contains_key(trace_id),
        "the stream waits for the client to finish its log"
    );
    log.append(tail);
    log.finish();
    run_until_closed(worker, trace_id, Duration::from_secs(20)).await;
}

#[tokio::test]
async fn a_successful_command_attaches_its_whole_event_log() {
    let (host, backend) = serve_backend().await;
    let dir = tempfile::tempdir().unwrap();
    let mut worker = worker(&host, dir.path(), Duration::from_secs(20));
    let trace_id = TraceId::new().to_string();
    let mut log = ClientLog::start(dir.path(), &trace_id, &bytes(1000, 1));

    run_command(&mut worker, &trace_id, true, &mut log, b"command result").await;

    let local = std::fs::read(&log.path).unwrap();
    let logs = backend.build_tool_logs(&trace_id);
    let event_log = logs
        .iter()
        .find(|f| f.name == EVENT_LOG_FILE_NAME)
        .expect("BuildToolLogs names the event log");
    assert_eq!(event_log.length, local.len() as i64);
    assert_eq!(event_log.digest, format!("{:x}", Sha256::digest(&local)));
    assert!(matches!(
        &event_log.file,
        Some(bep::file::File::Uri(uri)) if uri.starts_with(&format!("bytestream://{host}/blobs/"))
    ));
    assert_eq!(
        backend.blobs.lock().unwrap().get(&event_log.digest),
        Some(&local),
        "the CAS holds the client's file, tail included"
    );
    assert!(
        logs.iter().any(|f| f.name == "buck2-invocation.json"),
        "the log joins the files BuildToolLogs already carried"
    );
    assert!(
        backend.last_event_is_build_tool_logs(&trace_id),
        "BuildToolLogs is still the last message"
    );
}

#[tokio::test]
async fn a_failed_command_attaches_its_event_log() {
    let (host, backend) = serve_backend().await;
    let dir = tempfile::tempdir().unwrap();
    let mut worker = worker(&host, dir.path(), Duration::from_secs(20));
    let trace_id = TraceId::new().to_string();
    let mut log = ClientLog::start(dir.path(), &trace_id, &bytes(500, 2));

    run_command(&mut worker, &trace_id, false, &mut log, b"failed result").await;

    let local = std::fs::read(&log.path).unwrap();
    let digest = format!("{:x}", Sha256::digest(&local));
    assert!(
        backend
            .build_tool_logs(&trace_id)
            .iter()
            .any(|f| f.name == EVENT_LOG_FILE_NAME && f.digest == digest)
    );
}

#[tokio::test]
async fn a_log_past_the_inline_cap_is_streamed_in_chunks() {
    let (host, backend) = serve_backend().await;
    let dir = tempfile::tempdir().unwrap();
    let mut worker = worker(&host, dir.path(), Duration::from_secs(20));
    let trace_id = TraceId::new().to_string();
    let size = DEFAULT_BAZEL_ARTIFACT_UPLOAD_MAX_BYTES + 3 * 1024 * 1024 + 17;
    let mut log = ClientLog::start(dir.path(), &trace_id, &bytes(size, 3));

    run_command(&mut worker, &trace_id, true, &mut log, b"").await;

    let local = std::fs::read(&log.path).unwrap();
    assert_eq!(local.len(), size);
    let digest = format!("{:x}", Sha256::digest(&local));
    assert_eq!(backend.blobs.lock().unwrap().get(&digest), Some(&local));
    let chunks = backend.write_chunks.lock().unwrap().clone();
    assert!(
        chunks
            .iter()
            .all(|n| *n <= crate::sink::bes_event_log::EVENT_LOG_UPLOAD_CHUNK_BYTES),
        "no WriteRequest exceeds a server's default 4 MiB receive limit: {chunks:?}"
    );
    assert!(chunks.iter().filter(|n| **n > 0).count() >= 14);
}

#[tokio::test]
async fn a_log_still_written_at_the_timeout_is_attached_as_incomplete() {
    let (host, backend) = serve_backend().await;
    let dir = tempfile::tempdir().unwrap();
    let mut worker = worker(&host, dir.path(), Duration::from_millis(300));
    let trace_id = TraceId::new().to_string();
    let written = bytes(2048, 4);
    let log = ClientLog::start(dir.path(), &trace_id, &written);

    worker
        .send_message_with_retry(&message(&trace_id, command_start()), false)
        .await
        .unwrap();
    worker
        .send_message_with_retry(&message(&trace_id, command_end(false)), false)
        .await
        .unwrap();
    run_until_closed(&mut worker, &trace_id, Duration::from_secs(10)).await;
    // A hung client keeps writing after the sink gave up on it.
    log.append(b"later");

    let file = backend
        .build_tool_logs(&trace_id)
        .into_iter()
        .find(|f| f.name == INCOMPLETE_EVENT_LOG_FILE_NAME)
        .expect("the prefix written so far is attached");
    assert_eq!(file.length, written.len() as i64);
    assert_eq!(
        backend.blobs.lock().unwrap().get(&file.digest),
        Some(&written)
    );
}

#[tokio::test]
async fn no_log_means_build_tool_logs_goes_without_one() {
    let (host, backend) = serve_backend().await;
    let dir = tempfile::tempdir().unwrap();
    let mut worker = worker(&host, dir.path(), Duration::from_millis(300));
    let trace_id = TraceId::new().to_string();

    worker
        .send_message_with_retry(&message(&trace_id, command_start()), false)
        .await
        .unwrap();
    worker
        .send_message_with_retry(&message(&trace_id, command_end(true)), false)
        .await
        .unwrap();
    run_until_closed(&mut worker, &trace_id, Duration::from_secs(10)).await;

    let logs = backend.build_tool_logs(&trace_id);
    assert!(logs.iter().any(|f| f.name == "buck2-invocation.json"));
    assert!(!logs.iter().any(|f| f.name.starts_with("buck2-events")));
    assert!(backend.last_event_is_build_tool_logs(&trace_id));
}

#[tokio::test]
async fn upload_event_log_off_sends_build_tool_logs_at_command_end() {
    let (host, backend) = serve_backend().await;
    let dir = tempfile::tempdir().unwrap();
    let mut worker = worker(&host, dir.path(), Duration::from_secs(20));
    worker.config.upload_event_log = false;
    let trace_id = TraceId::new().to_string();
    let _log = ClientLog::start(dir.path(), &trace_id, b"still being written");

    worker
        .send_message_with_retry(&message(&trace_id, command_start()), false)
        .await
        .unwrap();
    worker
        .send_message_with_retry(&message(&trace_id, command_end(true)), false)
        .await
        .unwrap();
    // Closes after the usual grace, without waiting on the client's lock.
    run_until_closed(&mut worker, &trace_id, Duration::from_secs(5)).await;
    assert!(
        !backend
            .build_tool_logs(&trace_id)
            .iter()
            .any(|f| f.name.starts_with("buck2-events"))
    );
}

#[tokio::test]
async fn shutdown_attaches_a_log_the_poll_had_not_reached() {
    let (host, backend) = serve_backend().await;
    let dir = tempfile::tempdir().unwrap();
    let mut worker = worker(&host, dir.path(), Duration::from_secs(20));
    let trace_id = TraceId::new().to_string();
    let mut log = ClientLog::start(dir.path(), &trace_id, &bytes(100, 5));
    worker
        .send_message_with_retry(&message(&trace_id, command_start()), false)
        .await
        .unwrap();
    worker
        .send_message_with_retry(&message(&trace_id, command_end(true)), false)
        .await
        .unwrap();
    log.finish();

    worker.close_all_streams_for_shutdown().await;

    let local = std::fs::read(&log.path).unwrap();
    let digest = format!("{:x}", Sha256::digest(&local));
    assert!(
        backend
            .build_tool_logs(&trace_id)
            .iter()
            .any(|f| f.name == EVENT_LOG_FILE_NAME && f.digest == digest)
    );
}

#[tokio::test]
async fn every_call_carries_the_configured_key() {
    let (host, backend) = serve_backend().await;
    let dir = tempfile::tempdir().unwrap();
    let mut worker = worker(&host, dir.path(), Duration::from_secs(20));
    let trace_id = TraceId::new().to_string();
    let mut log = ClientLog::start(dir.path(), &trace_id, &bytes(100, 6));
    run_command(&mut worker, &trace_id, true, &mut log, b"").await;

    let out = dir.path().join("dl");
    download_event_log(&download_config(&host, Some(API_KEY)), &trace_id, &out)
        .await
        .unwrap();

    let calls = backend.calls.lock().unwrap().clone();
    for rpc in [
        "PublishBuildToolEventStream",
        "ByteStream.Write",
        "GetInvocation",
        "ByteStream.Read",
    ] {
        assert!(
            calls.iter().any(|(name, _)| *name == rpc),
            "{rpc} was called: {calls:?}"
        );
    }
    assert!(
        calls.iter().all(|(_, key)| key.as_deref() == Some(API_KEY)),
        "{calls:?}"
    );

    let err = download_event_log(&download_config(&host, None), &trace_id, &out)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("Unauthenticated"), "{err:#}");
}

#[tokio::test]
async fn a_log_fetched_by_trace_id_is_the_local_log() {
    let (host, backend) = serve_backend().await;
    let build_machine = tempfile::tempdir().unwrap();
    let mut worker = worker(&host, build_machine.path(), Duration::from_secs(20));
    let trace_id = TraceId::new().to_string();
    let mut log = ClientLog::start(
        build_machine.path(),
        &trace_id,
        &bytes(DEFAULT_BAZEL_ARTIFACT_UPLOAD_MAX_BYTES + 1, 7),
    );
    run_command(&mut worker, &trace_id, true, &mut log, b"result").await;
    drop(backend);

    let other_machine = tempfile::tempdir().unwrap();
    let out = other_machine.path().join(format!("dl-{trace_id}.pb.zst"));
    let downloaded = download_event_log(&download_config(&host, Some(API_KEY)), &trace_id, &out)
        .await
        .unwrap();
    assert!(downloaded.complete);
    assert_eq!(
        std::fs::read(&out).unwrap(),
        std::fs::read(&log.path).unwrap(),
        "byte for byte the file `buck2 log` reads on the machine that ran the build"
    );
}

#[tokio::test]
async fn an_evicted_log_names_the_persisted_copy() {
    let (host, backend) = serve_backend().await;
    let dir = tempfile::tempdir().unwrap();
    let mut worker = worker(&host, dir.path(), Duration::from_secs(20));
    let trace_id = TraceId::new().to_string();
    let mut log = ClientLog::start(dir.path(), &trace_id, &bytes(100, 8));
    run_command(&mut worker, &trace_id, true, &mut log, b"").await;
    backend.blobs.lock().unwrap().clear();

    let out = dir.path().join("dl");
    let err = download_event_log(&download_config(&host, Some(API_KEY)), &trace_id, &out)
        .await
        .unwrap_err();
    let text = format!("{err:#}");
    assert!(text.contains("no longer in the CAS"), "{text}");
    assert!(
        text.contains(&format!(
            "https://{host}/file/download?invocation_id={trace_id}&bytestream_url=bytestream%3A%2F%2F"
        )),
        "{text}"
    );
}

#[tokio::test]
async fn a_missing_invocation_or_log_says_which() {
    let (host, _backend) = serve_backend().await;
    let dir = tempfile::tempdir().unwrap();
    let mut worker = worker(&host, dir.path(), Duration::from_millis(200));
    worker.config.upload_event_log = false;
    let out = dir.path().join("dl");

    let unknown = TraceId::new().to_string();
    let err = download_event_log(&download_config(&host, Some(API_KEY)), &unknown, &out)
        .await
        .unwrap_err();
    assert!(
        format!("{err:#}").contains(&format!("has no invocation `{unknown}`")),
        "{err:#}"
    );

    let trace_id = TraceId::new().to_string();
    worker
        .send_message_with_retry(&message(&trace_id, command_start()), false)
        .await
        .unwrap();
    worker
        .send_message_with_retry(&message(&trace_id, command_end(true)), false)
        .await
        .unwrap();
    run_until_closed(&mut worker, &trace_id, Duration::from_secs(5)).await;
    let err = download_event_log(&download_config(&host, Some(API_KEY)), &trace_id, &out)
        .await
        .unwrap_err();
    assert!(
        format!("{err:#}").contains("has no event log attached"),
        "{err:#}"
    );
}

fn finished_exit_name(backend: &Backend, invocation_id: &str) -> Option<String> {
    let invocations = backend.invocations.lock().unwrap();
    invocations[invocation_id].0.iter().find_map(|event| match &event.payload {
        Some(bep::build_event::Payload::Finished(finished)) => {
            finished.exit_code.as_ref().map(|code| code.name.clone())
        }
        _ => None,
    })
}

/// CTRL-C or a killed client: the daemon's command never sends `CommandEnd`. Once the client's
/// log is finished and the daemon has gone quiet, the stream ends as interrupted, with the log.
#[tokio::test]
async fn an_interrupted_command_attaches_its_log_once_the_client_is_gone() {
    let (host, backend) = serve_backend().await;
    let dir = tempfile::tempdir().unwrap();
    let mut worker = worker(&host, dir.path(), Duration::from_secs(20));
    worker.client_gone_quiet = Duration::from_millis(300);
    let trace_id = TraceId::new().to_string();
    let mut log = ClientLog::start(dir.path(), &trace_id, &bytes(3000, 9));

    worker
        .send_message_with_retry(&message(&trace_id, command_start()), false)
        .await
        .unwrap();
    // The client is still writing: however quiet the daemon, the command is running.
    for _ in 0..30 {
        worker.close_due_streams().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(worker.streams.contains_key(&trace_id));

    log.finish();
    tokio::time::sleep(CLIENT_GONE_CHECK_INTERVAL).await;
    run_until_closed(&mut worker, &trace_id, Duration::from_secs(20)).await;

    let local = std::fs::read(&log.path).unwrap();
    let digest = format!("{:x}", Sha256::digest(&local));
    assert!(
        backend
            .build_tool_logs(&trace_id)
            .iter()
            .any(|f| f.name == EVENT_LOG_FILE_NAME && f.digest == digest)
    );
    assert_eq!(
        finished_exit_name(&backend, &trace_id).as_deref(),
        Some("INTERRUPTED")
    );
    assert!(
        backend.last_event_is_build_tool_logs(&trace_id),
        "BuildFinished gives up last_message to BuildToolLogs"
    );
    assert!(backend.invocations.lock().unwrap()[&trace_id].1, "stream finished");

    // The cancelled command may still send events; they must not open a second stream, which
    // the server would take for a new attempt of the invocation.
    let streams_before = backend
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(rpc, _)| *rpc == "PublishBuildToolEventStream")
        .count();
    worker
        .send_message_with_retry(&message(&trace_id, command_end(false)), false)
        .await
        .unwrap();
    for _ in 0..5 {
        worker.close_due_streams().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!worker.streams.contains_key(&trace_id));
    let streams_after = backend
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(rpc, _)| *rpc == "PublishBuildToolEventStream")
        .count();
    assert_eq!(streams_before, streams_after);
}

#[tokio::test]
async fn shutdown_before_command_end_attaches_the_log_after_build_finished() {
    let (host, backend) = serve_backend().await;
    let dir = tempfile::tempdir().unwrap();
    let mut worker = worker(&host, dir.path(), Duration::from_secs(20));
    let trace_id = TraceId::new().to_string();
    let mut log = ClientLog::start(dir.path(), &trace_id, &bytes(700, 10));
    worker
        .send_message_with_retry(&message(&trace_id, command_start()), false)
        .await
        .unwrap();
    log.finish();

    worker.close_all_streams_for_shutdown().await;

    assert_eq!(
        finished_exit_name(&backend, &trace_id).as_deref(),
        Some("INTERRUPTED")
    );
    assert!(backend.last_event_is_build_tool_logs(&trace_id));
    assert!(
        backend
            .build_tool_logs(&trace_id)
            .iter()
            .any(|f| f.name == EVENT_LOG_FILE_NAME)
    );
}
