/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! The gRPC surface: the remote execution API's ContentAddressableStorage and ByteStream
//! services plus Capabilities, all answered from the local store and, on a miss, the upstream.

use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Context;
use async_compression::tokio::bufread::BrotliDecoder;
use async_compression::tokio::bufread::BrotliEncoder;
use async_compression::tokio::bufread::DeflateDecoder;
use async_compression::tokio::bufread::DeflateEncoder;
use async_compression::tokio::bufread::ZstdDecoder;
use async_compression::tokio::bufread::ZstdEncoder;
use futures::Stream;
use futures::StreamExt;
use re_grpc_proto::build::bazel::remote::execution::v2 as re;
use re_grpc_proto::build::bazel::remote::execution::v2::capabilities_server::Capabilities;
use re_grpc_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorage;
use re_grpc_proto::build::bazel::semver::SemVer;
use re_grpc_proto::google::bytestream as bs;
use re_grpc_proto::google::bytestream::byte_stream_server::ByteStream;
use re_grpc_proto::google::rpc;
use remote_execution::PID_HEADER;
use remote_execution::UPSTREAM_CREDENTIALS_HEADER;
use remote_execution::UPSTREAM_HEADER;
use remote_execution::UPSTREAM_INSTANCE_NAME_HEADER;
use remote_execution::UPSTREAM_TLS_HEADER;
use tokio::io::AsyncBufRead;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio_util::io::ReaderStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use tonic::metadata::Ascii;
use tonic::metadata::MetadataMap;
use tonic::metadata::MetadataValue;

use crate::digest::Digest;
use crate::store::CommitError;
use crate::store::Store;
use crate::upstream::Upstream;
use crate::upstream::UpstreamConfig;

/// Largest `BatchReadBlobs`/`BatchUpdateBlobs` payload we advertise.
pub const MAX_BATCH_TOTAL_SIZE_BYTES: usize = 4 * 1024 * 1024;
/// Size of each `ReadResponse` message.
const READ_CHUNK_SIZE: usize = 1 << 20;

#[derive(Clone)]
pub struct Cas {
    inner: Arc<Inner>,
}

struct Inner {
    store: Arc<Store>,
    upstream: Option<Arc<Upstream>>,
    /// Sent with every `GetCapabilities` answer, so that a buck2 client that finds this daemon
    /// already running can tell whether its upstream is the client's own CAS.
    upstream_metadata: MetadataMap,
}

impl Cas {
    pub fn new(
        store: Arc<Store>,
        upstream: Option<Arc<Upstream>>,
        upstream_config: Option<&UpstreamConfig>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                store,
                upstream,
                upstream_metadata: upstream_metadata(upstream_config),
            }),
        }
    }

    fn store(&self) -> &Arc<Store> {
        &self.inner.store
    }

    fn digest(&self, d: &re::Digest) -> Result<Digest, Status> {
        Digest::from_proto(d, self.store().digest_function()).map_err(invalid)
    }

    /// The blob's local path, fetching from upstream if needed. `None` means nobody has it.
    async fn ensure_local(&self, digest: &Digest) -> Result<Option<PathBuf>, Status> {
        let result = match &self.inner.upstream {
            Some(upstream) => {
                self.store()
                    .ensure_local(digest, |tmp| async move {
                        upstream.fetch_to(digest, &tmp).await
                    })
                    .await
            }
            None => self.store().lookup(digest).await,
        };
        let path = result.map_err(internal)?;
        self.evict_in_background();
        Ok(path)
    }

    fn evict_in_background(&self) {
        if self.store().over_cap() {
            let store = Arc::clone(self.store());
            tokio::spawn(async move {
                if let Err(e) = store.evict().await {
                    tracing::warn!("Eviction failed: {e:#}");
                }
            });
        }
    }
}

#[tonic::async_trait]
impl ContentAddressableStorage for Cas {
    async fn find_missing_blobs(
        &self,
        request: Request<re::FindMissingBlobsRequest>,
    ) -> Result<Response<re::FindMissingBlobsResponse>, Status> {
        let digests = request
            .into_inner()
            .blob_digests
            .iter()
            .map(|d| self.digest(d))
            .collect::<Result<Vec<_>, _>>()?;
        let missing = match &self.inner.upstream {
            Some(upstream) => upstream.find_missing(&digests).await.map_err(internal)?,
            None => {
                let mut missing = Vec::new();
                for d in &digests {
                    if d.size != 0 && self.store().lookup(d).await.map_err(internal)?.is_none() {
                        missing.push(d.clone());
                    }
                }
                missing
            }
        };
        Ok(Response::new(re::FindMissingBlobsResponse {
            missing_blob_digests: missing.iter().map(Digest::to_proto).collect(),
        }))
    }

    async fn batch_update_blobs(
        &self,
        request: Request<re::BatchUpdateBlobsRequest>,
    ) -> Result<Response<re::BatchUpdateBlobsResponse>, Status> {
        let request = request.into_inner();
        let mut responses = Vec::with_capacity(request.requests.len());
        let mut stored: Vec<(usize, Digest, Vec<u8>)> = Vec::new();
        for (index, blob) in request.requests.into_iter().enumerate() {
            let Some(proto_digest) = blob.digest else {
                responses.push(update_response(
                    None,
                    status(rpc_code::INVALID_ARGUMENT, "missing digest"),
                ));
                continue;
            };
            let digest = match self.digest(&proto_digest) {
                Ok(d) => d,
                Err(e) => {
                    responses.push(update_response(Some(proto_digest), status_of(&e)));
                    continue;
                }
            };
            if blob.compressor != re::compressor::Value::Identity as i32 {
                responses.push(update_response(
                    Some(proto_digest),
                    status(
                        rpc_code::UNIMPLEMENTED,
                        "compressed batch updates are not supported",
                    ),
                ));
                continue;
            }
            let outcome = if digest.size == 0 {
                if blob.data.is_empty() {
                    Ok(())
                } else {
                    Err(CommitError::Mismatch(format!(
                        "Blob offered as `{digest}` has {} bytes",
                        blob.data.len()
                    )))
                }
            } else {
                self.store().insert_bytes(&digest, blob.data.clone()).await
            };
            match outcome {
                Ok(()) => {
                    responses.push(update_response(Some(proto_digest), ok()));
                    stored.push((index, digest, blob.data));
                }
                Err(CommitError::Mismatch(m)) => {
                    responses.push(update_response(
                        Some(proto_digest),
                        status(rpc_code::INVALID_ARGUMENT, &m),
                    ));
                }
                Err(CommitError::Other(e)) => {
                    responses.push(update_response(
                        Some(proto_digest),
                        status(rpc_code::INTERNAL, &format!("{e:#}")),
                    ));
                }
            }
        }

        if let Some(upstream) = &self.inner.upstream {
            if !stored.is_empty() {
                let indices: Vec<usize> = stored.iter().map(|(i, _, _)| *i).collect();
                let blobs = stored.into_iter().map(|(_, d, data)| (d, data)).collect();
                if let Err(e) = upstream.upload_blobs(blobs).await {
                    tracing::warn!("Forwarding batch upload to upstream failed: {e:#}");
                    for i in indices {
                        responses[i].status = Some(status(rpc_code::INTERNAL, &format!("{e:#}")));
                    }
                }
            }
        }
        self.evict_in_background();
        Ok(Response::new(re::BatchUpdateBlobsResponse { responses }))
    }

    async fn batch_read_blobs(
        &self,
        request: Request<re::BatchReadBlobsRequest>,
    ) -> Result<Response<re::BatchReadBlobsResponse>, Status> {
        let request = request.into_inner();
        let reads = request.digests.iter().map(|proto_digest| async move {
            let digest = match self.digest(proto_digest) {
                Ok(d) => d,
                Err(e) => return read_response(proto_digest.clone(), Err(status_of(&e))),
            };
            if digest.size == 0 {
                return read_response(proto_digest.clone(), Ok(Vec::new()));
            }
            let result = match self.ensure_local(&digest).await {
                Ok(Some(path)) => match tokio::fs::read(&path).await {
                    Ok(data) => Ok(data),
                    Err(e) => Err(status(
                        rpc_code::INTERNAL,
                        &format!("Error reading `{}`: {e}", path.display()),
                    )),
                },
                Ok(None) => Err(status(
                    rpc_code::NOT_FOUND,
                    &format!("Blob `{digest}` not found"),
                )),
                Err(e) => Err(status_of(&e)),
            };
            read_response(proto_digest.clone(), result)
        });
        let responses = futures::future::join_all(reads).await;
        Ok(Response::new(re::BatchReadBlobsResponse { responses }))
    }

    type GetTreeStream =
        Pin<Box<dyn Stream<Item = Result<re::GetTreeResponse, Status>> + Send + 'static>>;

    async fn get_tree(
        &self,
        _request: Request<re::GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        Err(Status::unimplemented(
            "GetTree is not supported by buck2-casd",
        ))
    }

    async fn split_blob(
        &self,
        _request: Request<re::SplitBlobRequest>,
    ) -> Result<Response<re::SplitBlobResponse>, Status> {
        Err(Status::unimplemented(
            "SplitBlob is not supported by buck2-casd",
        ))
    }

    async fn splice_blob(
        &self,
        _request: Request<re::SpliceBlobRequest>,
    ) -> Result<Response<re::SpliceBlobResponse>, Status> {
        Err(Status::unimplemented(
            "SpliceBlob is not supported by buck2-casd",
        ))
    }
}

#[tonic::async_trait]
impl ByteStream for Cas {
    type ReadStream =
        Pin<Box<dyn Stream<Item = Result<bs::ReadResponse, Status>> + Send + 'static>>;

    async fn read(
        &self,
        request: Request<bs::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let request = request.into_inner();
        let resource = ResourceName::parse(&request.resource_name).map_err(invalid)?;
        let digest = Digest::new(
            &resource.hash,
            resource.size,
            self.store().digest_function(),
        )
        .map_err(invalid)?;
        if request.read_offset < 0 || request.read_limit < 0 {
            return Err(Status::out_of_range("negative read_offset or read_limit"));
        }
        if request.read_offset > digest.size {
            return Err(Status::out_of_range(
                "read_offset is past the end of the blob",
            ));
        }

        if digest.size == 0 {
            let stream = futures::stream::once(async { Ok(bs::ReadResponse { data: vec![] }) });
            return Ok(Response::new(Box::pin(stream)));
        }

        let path = self
            .ensure_local(&digest)
            .await?
            .ok_or_else(|| Status::not_found(format!("Blob `{digest}` not found")))?;
        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| Status::internal(format!("Error opening `{}`: {e}", path.display())))?;
        if request.read_offset > 0 {
            file.seek(SeekFrom::Start(request.read_offset as u64))
                .await
                .map_err(|e| {
                    Status::internal(format!("Error seeking `{}`: {e}", path.display()))
                })?;
        }
        let reader = BufReader::with_capacity(READ_CHUNK_SIZE, file);
        let reader: Pin<Box<dyn AsyncRead + Send>> = match resource.compressor {
            None => Box::pin(reader),
            Some(compressor) => compressor.encoder(reader),
        };
        let reader: Pin<Box<dyn AsyncRead + Send>> = if request.read_limit > 0 {
            Box::pin(reader.take(request.read_limit as u64))
        } else {
            reader
        };
        let stream = ReaderStream::with_capacity(reader, READ_CHUNK_SIZE).map(|chunk| {
            chunk
                .map(|data| bs::ReadResponse {
                    data: data.to_vec(),
                })
                .map_err(|e| Status::internal(format!("Error streaming blob: {e}")))
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn write(
        &self,
        request: Request<Streaming<bs::WriteRequest>>,
    ) -> Result<Response<bs::WriteResponse>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty write stream"))?;
        let resource = ResourceName::parse(&first.resource_name).map_err(invalid)?;
        let digest = Digest::new(
            &resource.hash,
            resource.size,
            self.store().digest_function(),
        )
        .map_err(invalid)?;

        // Receive the whole stream into a temporary file.
        let wire_tmp = self.store().new_tmp_path();
        let received = match receive_write(&mut stream, first, &wire_tmp).await {
            Ok(received) => received,
            Err(e) => {
                let _ignored = tokio::fs::remove_file(&wire_tmp).await;
                return Err(e);
            }
        };

        // Undo any wire compression so the store gets raw bytes.
        let raw_tmp = match resource.compressor {
            None => wire_tmp,
            Some(compressor) => {
                let raw_tmp = self.store().new_tmp_path();
                let decoded = compressor.decompress_file(&wire_tmp, &raw_tmp).await;
                let _ignored = tokio::fs::remove_file(&wire_tmp).await;
                if let Err(e) = decoded {
                    let _ignored = tokio::fs::remove_file(&raw_tmp).await;
                    return Err(Status::invalid_argument(format!(
                        "Error decompressing upload of `{digest}`: {e:#}"
                    )));
                }
                raw_tmp
            }
        };

        if digest.size == 0 {
            let _ignored = tokio::fs::remove_file(&raw_tmp).await;
        } else {
            match self.store().commit(&digest, raw_tmp).await {
                Ok(()) => {}
                Err(CommitError::Mismatch(m)) => return Err(Status::invalid_argument(m)),
                Err(CommitError::Other(e)) => return Err(internal(e)),
            }
            if let Some(upstream) = &self.inner.upstream {
                upstream
                    .upload_file(&digest, &self.store().blob_path(&digest))
                    .await
                    .map_err(internal)?;
            }
        }
        self.evict_in_background();
        Ok(Response::new(bs::WriteResponse {
            committed_size: received,
        }))
    }

    async fn query_write_status(
        &self,
        _request: Request<bs::QueryWriteStatusRequest>,
    ) -> Result<Response<bs::QueryWriteStatusResponse>, Status> {
        Err(Status::unimplemented(
            "QueryWriteStatus is not supported by buck2-casd; writes are not resumable",
        ))
    }
}

#[tonic::async_trait]
impl Capabilities for Cas {
    async fn get_capabilities(
        &self,
        _request: Request<re::GetCapabilitiesRequest>,
    ) -> Result<Response<re::ServerCapabilities>, Status> {
        let mut response = Response::new(re::ServerCapabilities {
            cache_capabilities: Some(re::CacheCapabilities {
                digest_functions: vec![self.store().digest_function().proto_value() as i32],
                action_cache_update_capabilities: Some(re::ActionCacheUpdateCapabilities {
                    update_enabled: false,
                }),
                max_batch_total_size_bytes: MAX_BATCH_TOTAL_SIZE_BYTES as i64,
                symlink_absolute_path_strategy: re::symlink_absolute_path_strategy::Value::Allowed
                    as i32,
                supported_compressors: vec![
                    re::compressor::Value::Zstd as i32,
                    re::compressor::Value::Deflate as i32,
                    re::compressor::Value::Brotli as i32,
                ],
                ..Default::default()
            }),
            low_api_version: Some(SemVer {
                major: 2,
                minor: 0,
                ..Default::default()
            }),
            high_api_version: Some(SemVer {
                major: 2,
                minor: 3,
                ..Default::default()
            }),
            ..Default::default()
        });
        *response.metadata_mut() = self.inner.upstream_metadata.clone();
        Ok(response)
    }
}

/// The upstream as buck2's `casd_autostart` reads it back: the address as given, empty for a
/// standalone CAS, and the TLS setting, instance name and credentials that change which server
/// and whose CAS in it the blobs go to; and this process, for the warning a refused client
/// prints.
///
/// An upstream that cannot be written as headers (a value outside visible ASCII) is left out
/// whole and logged, rather than stopping the daemon: clients then see a daemon that does not
/// say, and go to their CAS directly.
fn upstream_metadata(upstream: Option<&UpstreamConfig>) -> MetadataMap {
    let mut metadata = MetadataMap::new();
    metadata.insert(PID_HEADER, MetadataValue::from(std::process::id()));
    match upstream_headers(upstream) {
        Ok(headers) => {
            for (name, value) in headers {
                metadata.insert(name, value);
            }
        }
        Err(e) => tracing::warn!(
            "Not reporting the upstream to clients, which will not use this daemon: {e:#}"
        ),
    }
    metadata
}

fn upstream_headers(
    upstream: Option<&UpstreamConfig>,
) -> anyhow::Result<Vec<(&'static str, MetadataValue<Ascii>)>> {
    let value = |v: &str| {
        MetadataValue::try_from(v).with_context(|| format!("`{v}` cannot be sent as a header"))
    };
    let Some(upstream) = upstream else {
        return Ok(vec![(UPSTREAM_HEADER, value("")?)]);
    };
    let mut headers = vec![(UPSTREAM_HEADER, value(&upstream.address)?)];
    if let Some(tls) = upstream.tls {
        headers.push((UPSTREAM_TLS_HEADER, value(&tls.to_string())?));
    }
    if let Some(instance) = &upstream.instance_name {
        headers.push((UPSTREAM_INSTANCE_NAME_HEADER, value(instance)?));
    }
    let credentials = remote_execution::upstream_credentials_fingerprint(
        &upstream.parsed_http_headers()?,
        upstream.tls_client_cert.as_deref(),
    )?;
    if let Some(credentials) = credentials {
        headers.push((UPSTREAM_CREDENTIALS_HEADER, value(&credentials)?));
    }
    Ok(headers)
}

/// Drains a ByteStream write into `tmp`, returning the number of bytes received on the wire.
async fn receive_write(
    stream: &mut Streaming<bs::WriteRequest>,
    first: bs::WriteRequest,
    tmp: &Path,
) -> Result<i64, Status> {
    let mut file = tokio::fs::File::create(tmp)
        .await
        .map_err(|e| Status::internal(format!("Error creating `{}`: {e}", tmp.display())))?;
    let mut received: i64 = 0;
    let mut message = first;
    let mut finished = false;
    loop {
        if message.write_offset != received {
            return Err(Status::invalid_argument(format!(
                "write_offset {} does not match bytes received so far ({received})",
                message.write_offset
            )));
        }
        file.write_all(&message.data)
            .await
            .map_err(|e| Status::internal(format!("Error writing `{}`: {e}", tmp.display())))?;
        received += message.data.len() as i64;
        if message.finish_write {
            finished = true;
            break;
        }
        match stream.message().await? {
            Some(next) => message = next,
            None => break,
        }
    }
    file.flush()
        .await
        .map_err(|e| Status::internal(format!("Error flushing `{}`: {e}", tmp.display())))?;
    if !finished {
        return Err(Status::invalid_argument(
            "write stream ended without finish_write",
        ));
    }
    Ok(received)
}

/// A parsed ByteStream resource name: `[instance/]blobs/[<digest function>/]<hash>/<size>[/...]`,
/// `[instance/]compressed-blobs/<compressor>/[<digest function>/]<hash>/<size>[/...]`, and the
/// `uploads/<uuid>/` forms of both. The digest function segment is the REAPI 2.3 form buck2 sends
/// for BLAKE3; the store has one digest function, so the segment is skipped.
#[derive(Debug, PartialEq, Eq)]
pub struct ResourceName {
    pub compressor: Option<Compressor>,
    pub hash: String,
    pub size: i64,
}

impl ResourceName {
    pub fn parse(name: &str) -> anyhow::Result<Self> {
        let all: Vec<&str> = name.split('/').collect();
        let parts: Vec<&str> = all
            .iter()
            .enumerate()
            .filter(|(i, part)| {
                let after_marker = i.checked_sub(1).is_some_and(|j| all[j] == "blobs")
                    || i.checked_sub(2)
                        .is_some_and(|j| all[j] == "compressed-blobs");
                !(after_marker && is_digest_function_segment(part))
            })
            .map(|(_, part)| *part)
            .collect();
        for i in (0..parts.len()).rev() {
            if parts[i] == "blobs" && i + 2 < parts.len() {
                return Ok(Self {
                    compressor: None,
                    hash: parts[i + 1].to_owned(),
                    size: parts[i + 2]
                        .parse()
                        .with_context(|| format!("Invalid size in resource name `{name}`"))?,
                });
            }
            if parts[i] == "compressed-blobs" && i + 3 < parts.len() {
                return Ok(Self {
                    compressor: Some(Compressor::from_name(parts[i + 1])?),
                    hash: parts[i + 2].to_owned(),
                    size: parts[i + 3]
                        .parse()
                        .with_context(|| format!("Invalid size in resource name `{name}`"))?,
                });
            }
        }
        Err(anyhow::anyhow!("Unrecognized resource name `{name}`"))
    }
}

/// The lower-case digest function names REAPI 2.3 allows in a resource name.
fn is_digest_function_segment(part: &str) -> bool {
    matches!(
        part,
        "sha256"
            | "sha1"
            | "md5"
            | "vso"
            | "sha384"
            | "sha512"
            | "murmur3"
            | "sha256tree"
            | "blake3"
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compressor {
    Zstd,
    Deflate,
    Brotli,
}

impl Compressor {
    fn from_name(name: &str) -> anyhow::Result<Self> {
        match name {
            "zstd" => Ok(Self::Zstd),
            "deflate" => Ok(Self::Deflate),
            "brotli" => Ok(Self::Brotli),
            other => Err(anyhow::anyhow!("Unsupported compressor `{other}`")),
        }
    }

    fn encoder(
        self,
        reader: impl AsyncBufRead + Send + Unpin + 'static,
    ) -> Pin<Box<dyn AsyncRead + Send>> {
        match self {
            Self::Zstd => Box::pin(ZstdEncoder::new(reader)),
            Self::Deflate => Box::pin(DeflateEncoder::new(reader)),
            Self::Brotli => Box::pin(BrotliEncoder::new(reader)),
        }
    }

    fn decoder(
        self,
        reader: impl AsyncBufRead + Send + Unpin + 'static,
    ) -> Pin<Box<dyn AsyncRead + Send>> {
        match self {
            Self::Zstd => {
                let mut d = ZstdDecoder::new(reader);
                d.multiple_members(true);
                Box::pin(d)
            }
            Self::Deflate => {
                let mut d = DeflateDecoder::new(reader);
                d.multiple_members(true);
                Box::pin(d)
            }
            Self::Brotli => {
                let mut d = BrotliDecoder::new(reader);
                d.multiple_members(true);
                Box::pin(d)
            }
        }
    }

    async fn decompress_file(self, src: &Path, dst: &Path) -> anyhow::Result<()> {
        let input = tokio::fs::File::open(src)
            .await
            .with_context(|| format!("Error opening `{}`", src.display()))?;
        let mut decoder = self.decoder(BufReader::new(input));
        let mut output = tokio::fs::File::create(dst)
            .await
            .with_context(|| format!("Error creating `{}`", dst.display()))?;
        tokio::io::copy(&mut decoder, &mut output)
            .await
            .context("Error decompressing")?;
        output.flush().await.context("Error flushing")?;
        Ok(())
    }
}

mod rpc_code {
    pub const INVALID_ARGUMENT: i32 = 3;
    pub const NOT_FOUND: i32 = 5;
    pub const UNIMPLEMENTED: i32 = 12;
    pub const INTERNAL: i32 = 13;
}

fn ok() -> rpc::Status {
    rpc::Status::default()
}

fn status(code: i32, message: &str) -> rpc::Status {
    rpc::Status {
        code,
        message: message.to_owned(),
        ..Default::default()
    }
}

fn status_of(s: &Status) -> rpc::Status {
    status(s.code() as i32, s.message())
}

fn update_response(
    digest: Option<re::Digest>,
    status: rpc::Status,
) -> re::batch_update_blobs_response::Response {
    re::batch_update_blobs_response::Response {
        digest,
        status: Some(status),
    }
}

fn read_response(
    digest: re::Digest,
    result: Result<Vec<u8>, rpc::Status>,
) -> re::batch_read_blobs_response::Response {
    let (data, status) = match result {
        Ok(data) => (data, ok()),
        Err(status) => (Vec::new(), status),
    };
    re::batch_read_blobs_response::Response {
        digest: Some(digest),
        data,
        status: Some(status),
        compressor: re::compressor::Value::Identity as i32,
    }
}

fn invalid(e: anyhow::Error) -> Status {
    Status::invalid_argument(format!("{e:#}"))
}

fn internal(e: anyhow::Error) -> Status {
    Status::internal(format!("{e:#}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_name_parsing() {
        let plain = ResourceName::parse("blobs/abc/12").unwrap();
        assert_eq!(
            plain,
            ResourceName {
                compressor: None,
                hash: "abc".into(),
                size: 12
            }
        );
        let instance = ResourceName::parse("my/instance/blobs/abc/12/meta.txt").unwrap();
        assert_eq!(instance.hash, "abc");
        assert_eq!(instance.size, 12);
        let upload = ResourceName::parse("inst/uploads/1234-uuid/blobs/abc/7").unwrap();
        assert_eq!(upload.hash, "abc");
        assert_eq!(upload.size, 7);
        let compressed = ResourceName::parse("uploads/u/compressed-blobs/zstd/abc/9").unwrap();
        assert_eq!(compressed.compressor, Some(Compressor::Zstd));
        assert_eq!(compressed.hash, "abc");
        assert_eq!(compressed.size, 9);
        let blake3 = ResourceName::parse("inst/blobs/blake3/abc/12").unwrap();
        assert_eq!((blake3.hash.as_str(), blake3.size), ("abc", 12));
        let blake3_upload =
            ResourceName::parse("uploads/u/compressed-blobs/zstd/blake3/abc/9").unwrap();
        assert_eq!(blake3_upload.compressor, Some(Compressor::Zstd));
        assert_eq!(
            (blake3_upload.hash.as_str(), blake3_upload.size),
            ("abc", 9)
        );
        assert!(ResourceName::parse("blobs/abc").is_err());
        assert!(ResourceName::parse("blobs/abc/notanumber").is_err());
        assert!(ResourceName::parse("compressed-blobs/lz4/abc/9").is_err());
    }
}
