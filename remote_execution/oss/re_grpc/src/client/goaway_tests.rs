/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Uploads rejected by a graceful GOAWAY, as sent by load balancers that
//! recycle connections. A rejected request was not processed and can be sent
//! again. tonic fails it instead, with `INTERNAL` or `UNAVAILABLE`, so these
//! uploads rely on the `retry_grpc_request` around each upload RPC, as in
//! `REClient::upload`.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use futures::stream;
use prost::bytes::Bytes;
use re_grpc_proto::build::bazel::remote::execution::v2::batch_read_blobs_response;
use re_grpc_proto::build::bazel::remote::execution::v2::batch_update_blobs_response;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tonic::transport::Endpoint;

use super::*;

/// How much request body the transport buffers to send a request again.
const REPLAY_BUFFER_SIZE: usize = 256 * 1024;

const BATCH_UPDATE_BLOBS: &str =
    "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs";
const BATCH_READ_BLOBS: &str =
    "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs";
const READ: &str = "/google.bytestream.ByteStream/Read";
const WRITE: &str = "/google.bytestream.ByteStream/Write";
const EXECUTE: &str = "/build.bazel.remote.execution.v2.Execution/Execute";
const WAIT_EXECUTION: &str = "/build.bazel.remote.execution.v2.Execution/WaitExecution";
const OPERATION: &str = "operations/action-1";

/// A server whose first connection takes more than `REPLAY_BUFFER_SIZE` of
/// the first request's body and then sends GOAWAY with last stream ID 0 and
/// NO_ERROR, so the request was not processed. Later connections serve
/// `BatchUpdateBlobs`, `BatchReadBlobs`, `ByteStream.Read`,
/// `ByteStream.Write`, `Execute` and `WaitExecution`.
struct GoawayServer {
    channel: Channel,
    goaway_sent: Arc<AtomicBool>,
    /// Method and blob bytes of each request served after the GOAWAY.
    served: Arc<Mutex<Vec<(String, usize)>>>,
}

impl GoawayServer {
    async fn start() -> Self {
        Self::start_with(Some(REPLAY_BUFFER_SIZE), false).await
    }

    /// Refuses the first request once any of its body arrives, so even a
    /// small request is sent in full before the GOAWAY.
    async fn refusing_first_request() -> Self {
        Self::start_with(Some(0), false).await
    }

    /// Refuses nothing. `Execute` reports a named operation that is still
    /// running and ends the stream, as a server may for a long action, so the
    /// client resumes it with `WaitExecution`.
    async fn leaving_executions_running() -> Self {
        Self::start_with(None, true).await
    }

    async fn start_with(refuse_after: Option<usize>, leave_running: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let goaway_sent = Arc::new(AtomicBool::new(false));
        let served = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn({
            let goaway_sent = goaway_sent.clone();
            let served = served.clone();
            async move {
                if let Some(refuse_after) = refuse_after {
                    let (tcp, _) = listener.accept().await.unwrap();
                    tokio::spawn(reject_first_request(tcp, refuse_after, goaway_sent));
                }
                loop {
                    let (tcp, _) = listener.accept().await.unwrap();
                    tokio::spawn(serve(tcp, served.clone(), leave_running));
                }
            }
        });
        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_lazy();
        Self {
            channel,
            goaway_sent,
            served,
        }
    }

    fn served(&self) -> Vec<(String, usize)> {
        self.served.lock().unwrap().clone()
    }
}

/// Speaks just enough HTTP/2 to receive the first request and send a GOAWAY
/// that rejects it once more than `refuse_after` bytes of its body arrived,
/// which `h2`'s server can't do once it has seen the request.
async fn reject_first_request(
    mut tcp: TcpStream,
    refuse_after: usize,
    goaway_sent: Arc<AtomicBool>,
) {
    const DATA: u8 = 0x0;
    const SETTINGS: u8 = 0x4;
    const PING: u8 = 0x6;
    const GOAWAY: u8 = 0x7;
    const WINDOW_UPDATE: u8 = 0x8;
    const ACK: u8 = 0x1;
    const MAX_WINDOW: u32 = (1 << 31) - 1;

    let mut preface = [0; 24];
    tcp.read_exact(&mut preface).await.unwrap();
    assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");

    // Let the client send the whole body without waiting for window updates.
    let mut initial_window_size = vec![0x00, 0x04];
    initial_window_size.extend_from_slice(&MAX_WINDOW.to_be_bytes());
    write_frame(&mut tcp, SETTINGS, 0, &initial_window_size)
        .await
        .unwrap();
    write_frame(
        &mut tcp,
        WINDOW_UPDATE,
        0,
        &(MAX_WINDOW - 65_535).to_be_bytes(),
    )
    .await
    .unwrap();

    let mut body_received = 0;
    let mut header = [0; 9];
    // Read until the client closes the connection, closing it here first with
    // unread data would reset it instead.
    while tcp.read_exact(&mut header).await.is_ok() {
        let len = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        let mut payload = vec![0; len];
        if tcp.read_exact(&mut payload).await.is_err() {
            break;
        }
        let written = match (header[3], header[4] & ACK) {
            (SETTINGS, 0) => write_frame(&mut tcp, SETTINGS, ACK, &[]).await,
            (PING, 0) => write_frame(&mut tcp, PING, ACK, &payload).await,
            (DATA, _) if body_received <= refuse_after => {
                body_received += len;
                if body_received > refuse_after {
                    // Last stream ID 0 and NO_ERROR.
                    goaway_sent.store(true, Ordering::SeqCst);
                    write_frame(&mut tcp, GOAWAY, 0, &[0; 8]).await
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        };
        if written.is_err() {
            break;
        }
    }
}

/// Writes a frame for stream 0, the connection.
async fn write_frame(tcp: &mut TcpStream, kind: u8, flags: u8, payload: &[u8]) -> io::Result<()> {
    let mut frame = (payload.len() as u32).to_be_bytes()[1..].to_vec();
    frame.extend_from_slice(&[kind, flags, 0, 0, 0, 0]);
    frame.extend_from_slice(payload);
    tcp.write_all(&frame).await
}

async fn serve(tcp: TcpStream, served: Arc<Mutex<Vec<(String, usize)>>>, leave_running: bool) {
    let mut connection = h2::server::handshake(tcp).await.unwrap();
    while let Some(request) = connection.accept().await {
        let (request, mut respond) = request.unwrap();
        let served = served.clone();
        tokio::spawn(async move {
            let method = request.uri().path().to_owned();
            let mut body = request.into_body();
            let mut data = Vec::new();
            while let Some(chunk) = body.data().await {
                let chunk = chunk.unwrap();
                body.flow_control().release_capacity(chunk.len()).unwrap();
                data.extend_from_slice(&chunk);
            }
            let messages = grpc_messages(&data);
            let (blob_bytes, responses) = match method.as_str() {
                BATCH_UPDATE_BLOBS => {
                    let request = BatchUpdateBlobsRequest::decode(messages[0]).unwrap();
                    let response = BatchUpdateBlobsResponse {
                        responses: request
                            .requests
                            .iter()
                            .map(|r| batch_update_blobs_response::Response {
                                digest: r.digest.clone(),
                                status: Some(Status::default()),
                            })
                            .collect(),
                    };
                    let blob_bytes = request.requests.iter().map(|r| r.data.len()).sum();
                    (blob_bytes, vec![response.encode_to_vec()])
                }
                BATCH_READ_BLOBS => {
                    let request = BatchReadBlobsRequest::decode(messages[0]).unwrap();
                    let response = BatchReadBlobsResponse {
                        responses: request
                            .digests
                            .iter()
                            .map(|digest| batch_read_blobs_response::Response {
                                digest: Some(digest.clone()),
                                data: vec![1; digest.size_bytes as usize],
                                status: Some(Status::default()),
                                ..Default::default()
                            })
                            .collect(),
                    };
                    (0, vec![response.encode_to_vec()])
                }
                READ => {
                    let request = ReadRequest::decode(messages[0]).unwrap();
                    let blob = read_blob(&request);
                    let responses = blob
                        .chunks(READ_CHUNK_SIZE)
                        .map(|chunk| ReadResponse {
                            data: chunk.to_vec(),
                        })
                        .map(|response| response.encode_to_vec())
                        .collect();
                    (blob.len(), responses)
                }
                EXECUTE if leave_running => (0, vec![running_operation().encode_to_vec()]),
                EXECUTE | WAIT_EXECUTION => (0, vec![finished_operation().encode_to_vec()]),
                WRITE => {
                    let blob_bytes = messages
                        .iter()
                        .map(|m| WriteRequest::decode(*m).unwrap().data.len())
                        .sum::<usize>();
                    let response = WriteResponse {
                        committed_size: blob_bytes as i64,
                    };
                    (blob_bytes, vec![response.encode_to_vec()])
                }
                _ => panic!("Unexpected call to {method}"),
            };
            served.lock().unwrap().push((method, blob_bytes));

            let headers = http::Response::builder()
                .header("content-type", "application/grpc")
                .body(())
                .unwrap();
            let mut stream = respond.send_response(headers, false).unwrap();
            for response in responses {
                stream.send_data(grpc_frame(&response), false).unwrap();
            }
            let mut trailers = http::HeaderMap::new();
            trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
            stream.send_trailers(trailers).unwrap();
        });
    }
}

/// Splits a gRPC body into its length-prefixed messages.
fn grpc_messages(mut body: &[u8]) -> Vec<&[u8]> {
    let mut messages = Vec::new();
    while !body.is_empty() {
        let len = u32::from_be_bytes(body[1..5].try_into().unwrap()) as usize;
        messages.push(&body[5..5 + len]);
        body = &body[5 + len..];
    }
    messages
}

fn grpc_frame(message: &[u8]) -> Bytes {
    let mut frame = vec![0];
    frame.extend_from_slice(&(message.len() as u32).to_be_bytes());
    frame.extend_from_slice(message);
    frame.into()
}

const READ_CHUNK_SIZE: usize = 1024 * 1024;

/// The part of a blob of ones, sized as its resource name says, that a
/// `ByteStream.Read` asks for.
fn read_blob(request: &ReadRequest) -> Vec<u8> {
    let size: usize = request
        .resource_name
        .rsplit('/')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    vec![1; size - request.read_offset as usize]
}

/// A `ByteStream.Read` the server refused is sent again by the
/// `retry_grpc_request` around opening it, as in `REClient::download`.
#[tokio::test]
async fn test_bytestream_read_after_goaway() -> anyhow::Result<()> {
    let server = GoawayServer::refusing_first_request().await;
    let bytestream = ByteStreamClient::new(server.channel.clone());
    let blob_size = 3 * READ_CHUNK_SIZE;
    let request = ReadRequest {
        resource_name: format!("blobs/aa/{blob_size}"),
        ..Default::default()
    };

    let read = async {
        let mut stream = retry_grpc_request(
            DEFAULT_RETRIES,
            Duration::from_millis(DEFAULT_RETRY_MAX_DELAY_MILLIS),
            || {
                let mut bytestream = bytestream.clone();
                let request = request.clone();
                async move { Ok(bytestream.read(request).await?.into_inner()) }
            },
        )
        .await?;
        let mut data = Vec::new();
        while let Some(response) = stream.message().await? {
            data.extend(response.data);
        }
        anyhow::Ok(data)
    };
    let data = tokio::time::timeout(Duration::from_secs(60), read).await??;

    assert_eq!(data, vec![1; blob_size]);
    assert!(server.goaway_sent.load(Ordering::SeqCst));
    assert_eq!(server.served(), [(READ.to_owned(), blob_size)]);
    Ok(())
}

fn running_operation() -> Operation {
    Operation {
        name: OPERATION.to_owned(),
        metadata: Some(prost_types::Any {
            type_url: String::new(),
            value: ExecuteOperationMetadata {
                stage: execution_stage::Value::Executing as i32,
                ..Default::default()
            }
            .encode_to_vec(),
        }),
        ..Default::default()
    }
}

fn finished_operation() -> Operation {
    Operation {
        name: OPERATION.to_owned(),
        done: true,
        result: Some(OpResult::Response(prost_types::Any {
            type_url: String::new(),
            value: GExecuteResponse {
                result: Some(ActionResult {
                    execution_metadata: Some(ExecutedActionMetadata::default()),
                    ..Default::default()
                }),
                ..Default::default()
            }
            .encode_to_vec(),
        })),
        ..Default::default()
    }
}

/// Runs an action like `REClient::execute_with_progress`, sending `Execute`
/// to `execute` and `WaitExecution` to `resume`, and returns its last event.
async fn execute(
    execute: &Channel,
    resume: &Channel,
) -> anyhow::Result<ExecuteWithProgressResponse> {
    let open = |operation: Option<String>| {
        let mut execute = ExecutionClient::new(execute.clone());
        let mut resume = ExecutionClient::new(resume.clone());
        async move {
            anyhow::Ok(match operation {
                None => execute
                    .execute(GExecuteRequest::default())
                    .await?
                    .into_inner(),
                Some(name) => resume
                    .wait_execution(WaitExecutionRequest { name })
                    .await?
                    .into_inner(),
            })
        }
    };
    let events: Vec<_> = execution_progress_stream(open, 0).collect().await;
    events.into_iter().last().context("No execution events")?
}

/// An `Execute` the server refused was not processed, so it is sent again.
#[tokio::test]
async fn test_execute_after_goaway() -> anyhow::Result<()> {
    let server = GoawayServer::refusing_first_request().await;

    let response = tokio::time::timeout(
        Duration::from_secs(60),
        execute(&server.channel, &server.channel),
    )
    .await??;

    assert!(response.execute_response.is_some());
    assert!(server.goaway_sent.load(Ordering::SeqCst));
    assert_eq!(server.served(), [(EXECUTE.to_owned(), 0)]);
    Ok(())
}

/// A `WaitExecution` the server refused while resuming an execution is sent
/// again.
#[tokio::test]
async fn test_wait_execution_after_goaway() -> anyhow::Result<()> {
    let executor = GoawayServer::leaving_executions_running().await;
    let resumer = GoawayServer::refusing_first_request().await;

    let response = tokio::time::timeout(
        Duration::from_secs(60),
        execute(&executor.channel, &resumer.channel),
    )
    .await??;

    assert!(response.execute_response.is_some());
    assert!(resumer.goaway_sent.load(Ordering::SeqCst));
    assert_eq!(executor.served(), [(EXECUTE.to_owned(), 0)]);
    assert_eq!(resumer.served(), [(WAIT_EXECUTION.to_owned(), 0)]);
    Ok(())
}

/// A `BatchReadBlobs` the server refused, as in facebook/buck2#1294, is sent
/// again by the `retry_grpc_request` around it, as in `REClient::download`.
#[tokio::test]
async fn test_batch_read_after_goaway() -> anyhow::Result<()> {
    let server = GoawayServer::refusing_first_request().await;
    let cas = ContentAddressableStorageClient::new(server.channel.clone());
    let request = BatchReadBlobsRequest {
        digests: vec![Digest {
            hash: "aa".to_owned(),
            size_bytes: 3,
        }],
        ..Default::default()
    };

    let read = retry_grpc_request(
        DEFAULT_RETRIES,
        Duration::from_millis(DEFAULT_RETRY_MAX_DELAY_MILLIS),
        || {
            let mut cas = cas.clone();
            let request = request.clone();
            async move { Ok(cas.batch_read_blobs(request).await?.into_inner()) }
        },
    );
    let response = tokio::time::timeout(Duration::from_secs(60), read).await??;

    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].data, [1, 1, 1]);
    assert!(server.goaway_sent.load(Ordering::SeqCst));
    assert_eq!(server.served(), [(BATCH_READ_BLOBS.to_owned(), 0)]);
    Ok(())
}

fn upload_request(blob: Vec<u8>) -> UploadRequest {
    UploadRequest {
        inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
            digest: TDigest {
                hash: "aa".to_owned(),
                size_in_bytes: blob.len() as i64,
                ..Default::default()
            },
            blob,
            ..Default::default()
        }]),
        ..Default::default()
    }
}

/// Uploads like `REClient::upload`, retrying each BatchUpdateBlobs and
/// ByteStream.Write call.
async fn upload(
    request: UploadRequest,
    max_total_batch_size: usize,
    channel: &Channel,
) -> anyhow::Result<UploadResponse> {
    let cas = &ContentAddressableStorageClient::new(channel.clone());
    let bytestream = &ByteStreamClient::new(channel.clone());
    let retry_max_delay = Duration::from_millis(DEFAULT_RETRY_MAX_DELAY_MILLIS);
    upload_impl(
        &InstanceName(None),
        request,
        None,
        max_total_batch_size,
        None,
        |re_request| async move {
            retry_grpc_request(DEFAULT_RETRIES, retry_max_delay, || {
                let re_request = re_request.clone();
                async move {
                    Ok(cas
                        .clone()
                        .batch_update_blobs(re_request)
                        .await?
                        .into_inner())
                }
            })
            .await
        },
        |segments| async move {
            retry_grpc_request(DEFAULT_RETRIES, retry_max_delay, || {
                let segments = segments.clone();
                async move {
                    Ok(bytestream
                        .clone()
                        .write(stream::iter(segments))
                        .await?
                        .into_inner())
                }
            })
            .await
        },
    )
    .await
}

#[tokio::test]
async fn test_upload_batch_after_goaway() -> anyhow::Result<()> {
    let server = GoawayServer::start().await;
    let blob_size = 2 * REPLAY_BUFFER_SIZE;

    // A batch limit above the blob size puts it in a BatchUpdateBlobs request.
    let upload = upload(
        upload_request(vec![1; blob_size]),
        2 * blob_size,
        &server.channel,
    );
    tokio::time::timeout(Duration::from_secs(60), upload).await??;

    assert!(server.goaway_sent.load(Ordering::SeqCst));
    assert_eq!(
        server.served(),
        [(BATCH_UPDATE_BLOBS.to_owned(), blob_size)]
    );
    Ok(())
}

#[tokio::test]
async fn test_upload_bytestream_after_goaway() -> anyhow::Result<()> {
    let server = GoawayServer::start().await;
    let blob_size = 2 * REPLAY_BUFFER_SIZE;

    // A batch limit below the blob size sends it with ByteStream.Write.
    let upload = upload(
        upload_request(vec![1; blob_size]),
        REPLAY_BUFFER_SIZE,
        &server.channel,
    );
    tokio::time::timeout(Duration::from_secs(60), upload).await??;

    assert!(server.goaway_sent.load(Ordering::SeqCst));
    assert_eq!(server.served(), [(WRITE.to_owned(), blob_size)]);
    Ok(())
}
