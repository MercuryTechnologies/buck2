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
use std::collections::VecDeque;
use std::io::Read;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use bes_grpc_proto::google::devtools::build::v1::BuildEvent;
use bes_grpc_proto::google::devtools::build::v1::OrderedBuildEvent;
use bes_grpc_proto::google::devtools::build::v1::PublishBuildToolEventStreamRequest;
use bes_grpc_proto::google::devtools::build::v1::PublishBuildToolEventStreamResponse;
use bes_grpc_proto::google::devtools::build::v1::StreamId;
use bes_grpc_proto::google::devtools::build::v1::build_event;
use bes_grpc_proto::google::devtools::build::v1::publish_build_event_client::PublishBuildEventClient;
use bes_grpc_proto::google::devtools::build::v1::stream_id;
use buck2_credential_helper::CredentialHelper;
use buck2_credential_helper::CredentialHelperSettings;
use buck2_data::buck_event;
use buck2_data::record_event;
use buck2_data::span_end_event;
use buck2_error::ErrorTag;
use buck2_error::conversion::from_any_with_tag;
use fbinit::FacebookInit;
use google_grpc_proto::google::bytestream::WriteRequest;
use google_grpc_proto::google::bytestream::byte_stream_client::ByteStreamClient;
use prost::Message as _;
use prost_types::Any;
use prost_types::Timestamp;
use sha2::Digest as _;
use sha2::Sha256;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tonic::metadata::MetadataKey;
use tonic::metadata::MetadataValue;
use tonic::transport::Certificate;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;
use tonic::transport::Identity;

use crate::sink::bazel_converter::BazelEventConverter;
use crate::sink::bazel_converter::encode_bep_event;
use crate::sink::bazel_converter::interrupted_finish_event;

const BUCK2_EVENT_TYPE_URL: &str = "type.googleapis.com/buck.data.BuckEvent";
const DEFAULT_BATCH_SIZE: usize = 1;
const UNACKED_EVENTS_PER_QUEUED_EVENT: usize = 10;
const CLOSE_ACK_TIMEOUT_MULTIPLIER: u32 = 30;
const MIN_CLOSE_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const COMMAND_END_CLOSE_GRACE: Duration = Duration::from_millis(500);
const COMMAND_END_CLOSE_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_BAZEL_ARTIFACT_UPLOAD_MAX_BYTES: usize = 10 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct BesConfig {
    pub buffer_size: usize,
    pub retry_backoff: Duration,
    pub retry_attempts: usize,
    /// How long one invocation's stream may keep failing before the sink gives up on it and
    /// drops its events. Past this the stream's memory is worth more than its events.
    pub retry_window: Duration,
    pub message_batch_size: Option<usize>,
    pub grpc_timeout: Duration,
    pub bes_backend: Option<String>,
    pub bes_headers: Vec<(String, String)>,
    pub bes_tls: BesTls,
    /// Set with `bes_headers` by `[bes] connection = re_client`: the remote execution client's
    /// helper, asked again here so the sink's headers follow the same refresh.
    pub bes_credential_helper: Option<CredentialHelperSettings>,
    pub build_metadata: Vec<(String, String)>,
    pub event_format: BesEventFormat,
    pub bazel_artifact_upload: bool,
    pub upload_successful_action_events: bool,
    pub bazel_artifact_upload_backend: Option<String>,
    pub re_client_cas_address: Option<String>,
    pub bazel_artifact_upload_instance_name: Option<String>,
    pub re_client_instance_name: Option<String>,
    pub bazel_artifact_uri_authority: Option<String>,
    pub bazel_artifact_upload_max_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BesEventFormat {
    #[default]
    Buck,
    Bazel,
}

impl Default for BesConfig {
    fn default() -> Self {
        Self {
            buffer_size: 10_000,
            retry_backoff: Duration::from_millis(500),
            retry_attempts: 5,
            retry_window: Duration::from_secs(60),
            message_batch_size: None,
            grpc_timeout: Duration::from_secs(10),
            bes_backend: None,
            bes_headers: Vec::new(),
            bes_tls: BesTls::default(),
            bes_credential_helper: None,
            build_metadata: Vec::new(),
            event_format: BesEventFormat::Buck,
            bazel_artifact_upload: true,
            upload_successful_action_events: true,
            bazel_artifact_upload_backend: None,
            re_client_cas_address: None,
            bazel_artifact_upload_instance_name: None,
            re_client_instance_name: None,
            bazel_artifact_uri_authority: None,
            bazel_artifact_upload_max_bytes: DEFAULT_BAZEL_ARTIFACT_UPLOAD_MAX_BYTES,
        }
    }
}

impl BesConfig {
    pub(crate) fn bes_enabled(&self) -> bool {
        self.bes_backend
            .as_deref()
            .is_some_and(|backend| !backend.trim().is_empty())
    }
}

impl FromStr for BesEventFormat {
    type Err = buck2_error::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "buck" => Ok(Self::Buck),
            "bazel" => Ok(Self::Bazel),
            value => Err(buck2_error::buck2_error!(
                ErrorTag::Input,
                "Invalid `bes.event_format` value `{}` (expected `buck` or `bazel`)",
                value
            )),
        }
    }
}

pub struct Message {
    pub category: String,
    pub message: Vec<u8>,
    pub message_key: Option<i64>,
}

struct SendNowRequest {
    messages: Vec<Message>,
    wait_for_acks: bool,
    /// Close every stream after sending: the daemon is shutting down.
    close_all: bool,
    done: oneshot::Sender<buck2_error::Result<()>>,
}

#[derive(Clone, Debug, Default)]
pub struct Counters {
    pub successes: u64,
    pub failures_invalid_request: u64,
    pub failures_unauthorized: u64,
    pub failures_rate_limited: u64,
    pub failures_pushed_back: u64,
    pub failures_enqueue_failed: u64,
    pub failures_internal_error: u64,
    pub failures_timed_out: u64,
    pub failures_unknown: u64,
    pub queue_depth: u64,
    pub dropped: u64,
    pub bytes_written: u64,
}

#[derive(Default)]
struct CounterState {
    successes: AtomicU64,
    failures_invalid_request: AtomicU64,
    failures_unauthorized: AtomicU64,
    failures_rate_limited: AtomicU64,
    failures_pushed_back: AtomicU64,
    failures_enqueue_failed: AtomicU64,
    failures_internal_error: AtomicU64,
    failures_timed_out: AtomicU64,
    failures_unknown: AtomicU64,
    queue_depth: AtomicU64,
    dropped: AtomicU64,
    bytes_written: AtomicU64,
}

impl CounterState {
    fn inc_success(&self, bytes: u64) {
        self.successes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    fn inc_failures_invalid_request(&self) {
        self.failures_invalid_request
            .fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failures_unauthorized(&self) {
        self.failures_unauthorized.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failures_rate_limited(&self) {
        self.failures_rate_limited.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failures_pushed_back(&self) {
        self.failures_pushed_back.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failures_enqueue_failed(&self) {
        self.failures_enqueue_failed.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failures_internal_error(&self) {
        self.failures_internal_error.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failures_timed_out(&self) {
        self.failures_timed_out.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_failures_unknown(&self) {
        self.failures_unknown.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_queue_depth(&self) {
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
    }

    fn dec_queue_depth(&self) {
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    fn inc_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }

    fn add_dropped(&self, count: u64) {
        self.dropped.fetch_add(count, Ordering::Relaxed);
    }

    fn snapshot(&self) -> Counters {
        Counters {
            successes: self.successes.load(Ordering::Relaxed),
            failures_invalid_request: self.failures_invalid_request.load(Ordering::Relaxed),
            failures_unauthorized: self.failures_unauthorized.load(Ordering::Relaxed),
            failures_rate_limited: self.failures_rate_limited.load(Ordering::Relaxed),
            failures_pushed_back: self.failures_pushed_back.load(Ordering::Relaxed),
            failures_enqueue_failed: self.failures_enqueue_failed.load(Ordering::Relaxed),
            failures_internal_error: self.failures_internal_error.load(Ordering::Relaxed),
            failures_timed_out: self.failures_timed_out.load(Ordering::Relaxed),
            failures_unknown: self.failures_unknown.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
        }
    }
}

/// PEM files for the sink's TLS connections. Set by `[bes] connection = re_client`, which hands
/// the remote execution client's identity to the sink. Paths may reference environment
/// variables.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BesTls {
    pub client_cert: Option<String>,
    pub ca_certs: Option<String>,
}

#[derive(Clone, Debug)]
struct ConnectionConfig {
    endpoint: String,
    headers: Vec<(String, String)>,
    tls: BesTls,
    credential_helper: Option<Arc<CredentialHelper>>,
}

#[derive(Clone, Debug)]
struct BazelArtifactUploadConfig {
    endpoint: String,
    headers: Vec<(String, String)>,
    tls: BesTls,
    credential_helper: Option<Arc<CredentialHelper>>,
    instance_name: String,
    uri_authority: String,
    max_bytes: usize,
    grpc_timeout: Duration,
}

impl BazelArtifactUploadConfig {
    fn from_bes(
        config: &BesConfig,
        connection: &ConnectionConfig,
    ) -> buck2_error::Result<Option<Self>> {
        if config.event_format != BesEventFormat::Bazel || !config.bazel_artifact_upload {
            return Ok(None);
        }
        let endpoint = config
            .bazel_artifact_upload_backend
            .as_deref()
            .map(|backend| bes_backend(Some(backend)))
            .transpose()?
            .or_else(|| {
                config
                    .re_client_cas_address
                    .as_deref()
                    .and_then(re_client_cas_endpoint)
            })
            .unwrap_or_else(|| connection.endpoint.clone());
        let uri_authority = config
            .bazel_artifact_uri_authority
            .as_deref()
            .map(str::trim)
            .filter(|authority| !authority.is_empty())
            .map(str::to_owned)
            .or_else(|| endpoint_authority(&endpoint))
            .unwrap_or_default();
        if uri_authority.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            endpoint,
            headers: connection.headers.clone(),
            tls: connection.tls.clone(),
            credential_helper: connection.credential_helper.clone(),
            instance_name: config
                .bazel_artifact_upload_instance_name
                .clone()
                .or_else(|| config.re_client_instance_name.clone())
                .unwrap_or_default(),
            uri_authority,
            max_bytes: config.bazel_artifact_upload_max_bytes,
            grpc_timeout: config.grpc_timeout,
        }))
    }
}

struct BazelArtifactUploader {
    config: BazelArtifactUploadConfig,
    client: Option<ByteStreamClient<Channel>>,
    repo_path: Option<PathBuf>,
    directory_outputs: HashSet<BepFileIdentity>,
    #[cfg(test)]
    test_writes: Vec<WriteRequest>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BepFileIdentity {
    path_prefix: Vec<String>,
    name: String,
    digest: String,
    length: i64,
}

impl BepFileIdentity {
    fn new(file: &bazel_bep_proto::build_event_stream::File) -> Self {
        Self {
            path_prefix: file.path_prefix.clone(),
            name: file.name.clone(),
            digest: file.digest.clone(),
            length: file.length,
        }
    }
}

impl BazelArtifactUploader {
    fn new(config: BazelArtifactUploadConfig) -> Self {
        Self {
            config,
            client: None,
            repo_path: None,
            directory_outputs: HashSet::new(),
            #[cfg(test)]
            test_writes: Vec::new(),
        }
    }

    fn observe_buck_event(&mut self, event: &buck2_data::BuckEvent) {
        match event.data.as_ref() {
            Some(buck_event::Data::SpanStart(span_start)) => {
                if let Some(buck2_data::span_start_event::Data::Command(command)) =
                    span_start.data.as_ref()
                {
                    self.observe_workspace_directory(
                        command
                            .metadata
                            .get("REPO_ROOT")
                            .or_else(|| command.metadata.get("WORKSPACE_DIRECTORY")),
                    );
                }
            }
            Some(buck_event::Data::Record(record)) => {
                if let Some(record_event::Data::InvocationRecord(record)) = record.data.as_ref() {
                    self.observe_workspace_directory(record.repo_path.as_ref());
                }
            }
            _ => {}
        }
    }

    fn observe_workspace_directory(&mut self, path: Option<&String>) {
        let Some(path) = path else {
            return;
        };
        if path.is_empty() {
            return;
        }
        let path = PathBuf::from(path);
        if path.is_absolute() {
            self.repo_path = Some(path);
        }
    }

    fn observe_bazel_events(&mut self, events: &[bazel_bep_proto::build_event_stream::BuildEvent]) {
        use bazel_bep_proto::build_event_stream::build_event::Payload;

        for event in events {
            let Some(Payload::Completed(completed)) = event.payload.as_ref() else {
                continue;
            };
            for file in &completed.directory_output {
                self.directory_outputs.insert(BepFileIdentity::new(file));
            }
        }
    }

    async fn upload_event_files(
        &mut self,
        event: &mut bazel_bep_proto::build_event_stream::BuildEvent,
    ) {
        use bazel_bep_proto::build_event_stream::build_event::Payload;

        match event.payload.as_mut() {
            Some(Payload::Action(action)) => {
                upload_named_file(self, action.stdout.as_mut()).await;
                upload_named_file(self, action.stderr.as_mut()).await;
                #[allow(deprecated)]
                for file in &mut action.action_metadata_logs {
                    self.upload_file_if_inline(file).await;
                }
            }
            Some(Payload::TestResult(result)) => {
                for file in &mut result.test_action_output {
                    self.upload_file_if_inline(file).await;
                }
            }
            Some(Payload::TestSummary(summary)) => {
                for file in &mut summary.passed {
                    self.upload_file_if_inline(file).await;
                }
                for file in &mut summary.failed {
                    self.upload_file_if_inline(file).await;
                }
            }
            Some(Payload::BuildToolLogs(logs)) => {
                for file in &mut logs.log {
                    self.upload_file_if_inline(file).await;
                }
            }
            Some(Payload::NamedSetOfFiles(files)) => {
                for file in &mut files.files {
                    if self.is_directory_output(file) {
                        continue;
                    }
                    if !self.upload_file_if_local(file).await {
                        self.add_uri_for_digest_file(file);
                    }
                }
            }
            _ => {}
        }
    }

    fn is_directory_output(&self, file: &bazel_bep_proto::build_event_stream::File) -> bool {
        self.directory_outputs.contains(&BepFileIdentity::new(file))
    }

    async fn upload_file_if_inline(
        &mut self,
        file: &mut bazel_bep_proto::build_event_stream::File,
    ) {
        let Some(bazel_bep_proto::build_event_stream::file::File::Contents(contents)) =
            file.file.as_ref()
        else {
            return;
        };
        if file.name == "primary_output"
            || contents.is_empty()
            || contents.len() > self.config.max_bytes
        {
            return;
        }
        let contents = contents.clone();
        let Some((uri, digest, len)) = self.upload_bytes(&contents).await else {
            return;
        };
        file.file = Some(bazel_bep_proto::build_event_stream::file::File::Uri(uri));
        file.digest = digest;
        file.length = len;
    }

    async fn upload_file_if_local(
        &mut self,
        file: &mut bazel_bep_proto::build_event_stream::File,
    ) -> bool {
        if file.file.is_some() {
            return false;
        }
        let Some(path) = self.local_file_path(file) else {
            return false;
        };
        let Ok(metadata) = std::fs::metadata(&path) else {
            return false;
        };
        if !metadata.is_file() {
            return false;
        }
        let Ok(size) = i64::try_from(metadata.len()) else {
            return false;
        };
        if let Some((_hash, expected_size)) = file_digest_and_size(file)
            && expected_size != size
        {
            return false;
        }
        let Some((uri, digest, len)) = self.upload_local_file(&path, size).await else {
            return false;
        };
        file.file = Some(bazel_bep_proto::build_event_stream::file::File::Uri(uri));
        file.digest = digest;
        file.length = len;
        true
    }

    fn local_file_path(&self, file: &bazel_bep_proto::build_event_stream::File) -> Option<PathBuf> {
        let repo_path = self.repo_path.as_ref()?;
        let mut relative = PathBuf::new();
        for path in file.path_prefix.iter().chain(std::iter::once(&file.name)) {
            let path = Path::new(path);
            if path.is_absolute() {
                return None;
            }
            for component in path.components() {
                match component {
                    Component::Normal(component) => relative.push(component),
                    Component::CurDir => {}
                    Component::Prefix(_) | Component::RootDir | Component::ParentDir => {
                        return None;
                    }
                }
            }
        }
        if relative.as_os_str().is_empty() {
            return None;
        }
        Some(repo_path.join(relative))
    }

    fn add_uri_for_digest_file(&self, file: &mut bazel_bep_proto::build_event_stream::File) {
        if file.file.is_some() {
            return;
        }
        let Some((hash, size)) = file_digest_and_size(file) else {
            return;
        };
        file.file = Some(bazel_bep_proto::build_event_stream::file::File::Uri(
            bytestream_uri(
                &self.config.uri_authority,
                &self.config.instance_name,
                hash,
                size,
            ),
        ));
        file.length = size;
    }

    async fn upload_bytes(&mut self, contents: &[u8]) -> Option<(String, String, i64)> {
        let mut hasher = Sha256::new();
        hasher.update(contents);
        let hash = format!("{:x}", hasher.finalize());
        let size = i64::try_from(contents.len()).ok()?;
        let resource_name = upload_resource_name(&self.config.instance_name, &hash, size);
        let request = WriteRequest {
            resource_name,
            write_offset: 0,
            finish_write: true,
            data: contents.to_vec(),
        };
        let response = self.write_request(request).await.ok()?;
        if response.committed_size != size && response.committed_size != -1 {
            return None;
        }
        let uri = bytestream_uri(
            &self.config.uri_authority,
            &self.config.instance_name,
            &hash,
            size,
        );
        Some((uri, format!("{hash}:{size}"), size))
    }

    async fn upload_local_file(&mut self, path: &Path, size: i64) -> Option<(String, String, i64)> {
        let hash = sha256_file(path)?;
        let resource_name = upload_resource_name(&self.config.instance_name, &hash, size);
        let response = self
            .write_file_requests(resource_name, path, size)
            .await
            .ok()?;
        if response.committed_size != size && response.committed_size != -1 {
            return None;
        }
        let uri = bytestream_uri(
            &self.config.uri_authority,
            &self.config.instance_name,
            &hash,
            size,
        );
        Some((uri, format!("{hash}:{size}"), size))
    }

    async fn write_request(
        &mut self,
        request: WriteRequest,
    ) -> Result<google_grpc_proto::google::bytestream::WriteResponse, Status> {
        self.write_requests(vec![request]).await
    }

    async fn write_requests(
        &mut self,
        requests: Vec<WriteRequest>,
    ) -> Result<google_grpc_proto::google::bytestream::WriteResponse, Status> {
        #[cfg(test)]
        if self.config.endpoint == "test://bytestream" {
            let committed_size = requests
                .iter()
                .map(|request| i64::try_from(request.data.len()).unwrap_or(i64::MAX))
                .sum();
            self.test_writes.extend(requests);
            return Ok(google_grpc_proto::google::bytestream::WriteResponse { committed_size });
        }

        if self.client.is_none() {
            let endpoint = endpoint_for(&self.config.endpoint, self.config.grpc_timeout, &self.config.tls)?;
            let channel = endpoint.connect().await.map_err(map_transport_error)?;
            self.client = Some(ByteStreamClient::new(channel));
        }
        let client = self.client.as_mut().expect("client was initialized");
        let outbound = tokio_stream::iter(requests);
        let mut request = tonic::Request::new(outbound);
        attach_headers(
            &mut request,
            &self.config.headers,
            self.config.credential_helper.as_deref(),
            &self.config.endpoint,
        )
        .await?;
        Ok(client.write(request).await?.into_inner())
    }

    async fn write_file_requests(
        &mut self,
        resource_name: String,
        path: &Path,
        size: i64,
    ) -> Result<google_grpc_proto::google::bytestream::WriteResponse, Status> {
        let chunk_size = self.config.max_bytes.max(1);

        #[cfg(test)]
        if self.config.endpoint == "test://bytestream" {
            let requests = write_requests_for_file(resource_name, path, size, chunk_size)?;
            return self.write_requests(requests).await;
        }

        if self.client.is_none() {
            let endpoint = endpoint_for(&self.config.endpoint, self.config.grpc_timeout, &self.config.tls)?;
            let channel = endpoint.connect().await.map_err(map_transport_error)?;
            self.client = Some(ByteStreamClient::new(channel));
        }
        let client = self.client.as_mut().expect("client was initialized");
        let outbound = file_write_stream(resource_name, path, size, chunk_size)?;
        let mut request = tonic::Request::new(outbound);
        attach_headers(
            &mut request,
            &self.config.headers,
            self.config.credential_helper.as_deref(),
            &self.config.endpoint,
        )
        .await?;
        Ok(client.write(request).await?.into_inner())
    }
}

struct FileWriteState {
    file: std::fs::File,
    resource_name: Option<String>,
    offset: i64,
    size: i64,
    chunk_size: usize,
    finished: bool,
}

fn sha256_file(path: &Path) -> Option<String> {
    let mut file = std::fs::File::open(path).ok()?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let bytes_read = file.read(&mut buffer).ok()?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    Some(format!("{:x}", hasher.finalize()))
}

fn file_write_stream(
    resource_name: String,
    path: &Path,
    size: i64,
    chunk_size: usize,
) -> Result<impl futures::Stream<Item = WriteRequest> + Send + 'static, Status> {
    let file = std::fs::File::open(path).map_err(map_io_status)?;
    Ok(futures::stream::unfold(
        FileWriteState {
            file,
            resource_name: Some(resource_name),
            offset: 0,
            size,
            chunk_size,
            finished: false,
        },
        |mut state| async move {
            match next_file_write_request(&mut state) {
                Ok(Some(request)) => Some((request, state)),
                Ok(None) | Err(_) => None,
            }
        },
    ))
}

#[cfg(test)]
fn write_requests_for_file(
    resource_name: String,
    path: &Path,
    size: i64,
    chunk_size: usize,
) -> Result<Vec<WriteRequest>, Status> {
    let mut state = FileWriteState {
        file: std::fs::File::open(path).map_err(map_io_status)?,
        resource_name: Some(resource_name),
        offset: 0,
        size,
        chunk_size,
        finished: false,
    };
    let mut requests = Vec::new();
    while let Some(request) = next_file_write_request(&mut state)? {
        requests.push(request);
    }
    Ok(requests)
}

fn next_file_write_request(state: &mut FileWriteState) -> Result<Option<WriteRequest>, Status> {
    if state.finished {
        return Ok(None);
    }
    if state.offset >= state.size {
        state.finished = true;
        return Ok(Some(WriteRequest {
            resource_name: state.resource_name.take().unwrap_or_default(),
            write_offset: state.offset,
            finish_write: true,
            data: Vec::new(),
        }));
    }

    let remaining = usize::try_from(state.size - state.offset).unwrap_or(usize::MAX);
    let mut data = vec![0; state.chunk_size.min(remaining).max(1)];
    let bytes_read = state.file.read(&mut data).map_err(map_io_status)?;
    if bytes_read == 0 {
        state.finished = true;
        return Ok(Some(WriteRequest {
            resource_name: state.resource_name.take().unwrap_or_default(),
            write_offset: state.offset,
            finish_write: true,
            data: Vec::new(),
        }));
    }

    data.truncate(bytes_read);
    let write_offset = state.offset;
    state.offset += i64::try_from(bytes_read).unwrap_or(i64::MAX);
    let finish_write = state.offset >= state.size;
    state.finished = finish_write;
    Ok(Some(WriteRequest {
        resource_name: state.resource_name.take().unwrap_or_default(),
        write_offset,
        finish_write,
        data,
    }))
}

fn map_io_status(error: std::io::Error) -> Status {
    Status::internal(error.to_string())
}

async fn upload_named_file(
    uploader: &mut BazelArtifactUploader,
    file: Option<&mut bazel_bep_proto::build_event_stream::File>,
) {
    if let Some(file) = file {
        uploader.upload_file_if_inline(file).await;
    }
}

fn endpoint_authority(endpoint: &str) -> Option<String> {
    let without_scheme = endpoint
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(endpoint);
    without_scheme
        .split('/')
        .next()
        .map(str::trim)
        .filter(|authority| !authority.is_empty())
        .map(str::to_owned)
}

fn re_client_cas_endpoint(address: &str) -> Option<String> {
    let address = address.trim();
    if address.is_empty() {
        return None;
    }

    if let Some((scheme, target)) = address.split_once("://") {
        let target = target.trim();
        if target.is_empty() {
            return None;
        }
        match scheme.to_ascii_lowercase().as_str() {
            "grpc" | "http" => Some(format!("http://{target}")),
            "grpcs" | "https" => Some(format!("https://{target}")),
            _ => None,
        }
    } else {
        Some(format!("https://{address}"))
    }
}

fn upload_resource_name(instance_name: &str, hash: &str, size: i64) -> String {
    let prefix = if instance_name.is_empty() {
        String::new()
    } else {
        format!("{}/", instance_name.trim_matches('/'))
    };
    format!(
        "{prefix}uploads/{}/blobs/{hash}/{size}",
        uuid::Uuid::new_v4()
    )
}

fn bytestream_uri(authority: &str, instance_name: &str, hash: &str, size: i64) -> String {
    let prefix = if instance_name.is_empty() {
        String::new()
    } else {
        format!("{}/", instance_name.trim_matches('/'))
    };
    format!("bytestream://{authority}/{prefix}blobs/{hash}/{size}")
}

fn file_digest_and_size(file: &bazel_bep_proto::build_event_stream::File) -> Option<(&str, i64)> {
    if file.digest.is_empty() {
        return None;
    }
    if let Some((hash, size)) = file.digest.split_once(':') {
        let size = size.parse::<i64>().ok()?;
        if hash.is_empty() || size < 0 {
            return None;
        }
        Some((hash, size))
    } else if file.length >= 0 {
        Some((file.digest.as_str(), file.length))
    } else {
        None
    }
}

pub struct BesClient {
    tx: crossbeam_channel::Sender<Message>,
    send_now_tx: crossbeam_channel::Sender<SendNowRequest>,
    counters: Arc<CounterState>,
}

impl BesClient {
    pub fn new(_fb: FacebookInit, config: BesConfig) -> buck2_error::Result<Self> {
        let connection = ConnectionConfig {
            endpoint: bes_backend(config.bes_backend.as_deref())?,
            headers: config.bes_headers.clone(),
            tls: config.bes_tls.clone(),
            credential_helper: config
                .bes_credential_helper
                .as_ref()
                .map(|settings| settings.build())
                .transpose()
                .map_err(|e| from_any_with_tag(e, ErrorTag::Input))?
                .map(Arc::new),
        };
        let queue_capacity = config.buffer_size.max(1);
        let (tx, rx) = crossbeam_channel::bounded(queue_capacity);
        let (send_now_tx, send_now_rx) = crossbeam_channel::unbounded();
        let counters = Arc::new(CounterState::default());

        let thread_counters = counters.clone();
        let thread_config = config.clone();
        let thread_connection = connection.clone();

        thread::Builder::new()
            .name("buck2-bes-sink".to_owned())
            .spawn(move || {
                let runtime = match bes_worker_runtime() {
                    Ok(runtime) => runtime,
                    Err(_) => {
                        thread_counters.inc_failures_internal_error();
                        return;
                    }
                };

                let mut worker =
                    WorkerState::new(thread_config, thread_connection, thread_counters);
                let batch_size = worker.batch_size();
                let mut send_now_open = true;
                loop {
                    if send_now_open {
                        loop {
                            match send_now_rx.try_recv() {
                                Ok(request) => {
                                    process_send_now_request(&runtime, &mut worker, request);
                                }
                                Err(crossbeam_channel::TryRecvError::Empty) => break,
                                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                                    send_now_open = false;
                                    break;
                                }
                            }
                        }
                    }

                    if send_now_open {
                        crossbeam_channel::select! {
                            recv(send_now_rx) -> request => match request {
                                Ok(request) => {
                                    process_send_now_request(&runtime, &mut worker, request);
                                }
                                Err(_) => {
                                    send_now_open = false;
                                }
                            },
                            recv(rx) -> message => match message {
                                Ok(message) => {
                                    process_queued_message(&runtime, &mut worker, message);
                                    for _ in 1..batch_size {
                                        let mut handled_send_now = false;
                                        loop {
                                            match send_now_rx.try_recv() {
                                                Ok(request) => {
                                                    handled_send_now = true;
                                                    process_send_now_request(
                                                        &runtime,
                                                        &mut worker,
                                                        request,
                                                    );
                                                }
                                                Err(crossbeam_channel::TryRecvError::Empty) => break,
                                                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                                                    send_now_open = false;
                                                    break;
                                                }
                                            }
                                        }
                                        if handled_send_now {
                                            break;
                                        }

                                        let Ok(message) = rx.try_recv() else {
                                            break;
                                        };
                                        process_queued_message(&runtime, &mut worker, message);
                                    }
                                    runtime.block_on(worker.close_due_streams());
                                }
                                Err(_) => break,
                            },
                            default(COMMAND_END_CLOSE_POLL_INTERVAL) => {
                                runtime.block_on(worker.close_due_streams());
                            }
                        }
                    } else {
                        match rx.recv_timeout(COMMAND_END_CLOSE_POLL_INTERVAL) {
                            Ok(message) => {
                                process_queued_message(&runtime, &mut worker, message);
                                for _ in 1..batch_size {
                                    let Ok(message) = rx.try_recv() else {
                                        break;
                                    };
                                    process_queued_message(&runtime, &mut worker, message);
                                }
                                runtime.block_on(worker.close_due_streams());
                            }
                            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                                runtime.block_on(worker.close_due_streams());
                            }
                            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                        }
                    }
                }
                runtime.block_on(worker.close_all_streams());
            })
            .map_err(|e| {
                buck2_error::buck2_error!(ErrorTag::Tier0, "Failed to start BES worker thread: {e}")
            })?;

        Ok(Self {
            tx,
            send_now_tx,
            counters,
        })
    }

    pub fn offer(&self, message: Message) {
        match self.tx.try_send(message) {
            Ok(()) => {
                self.counters.inc_queue_depth();
            }
            Err(_) => {
                self.counters.inc_failures_enqueue_failed();
                self.counters.inc_dropped();
            }
        }
    }

    async fn send_messages_with_priority(
        &self,
        messages: Vec<Message>,
        wait_for_acks: bool,
    ) -> buck2_error::Result<()> {
        if messages.is_empty() {
            return Ok(());
        }

        let (done_tx, done_rx) = oneshot::channel();
        self.send_now_tx
            .send(SendNowRequest {
                messages,
                wait_for_acks,
                close_all: false,
                done: done_tx,
            })
            .map_err(|_| {
                buck2_error::buck2_error!(
                    ErrorTag::Tier0,
                    "Failed to enqueue BES priority send request"
                )
            })?;

        done_rx.await.map_err(|_| {
            buck2_error::buck2_error!(
                ErrorTag::Tier0,
                "BES worker dropped priority send response channel"
            )
        })?
    }

    // Send through a dedicated priority lane on the background worker and wait
    // for completion. This keeps emergency delivery semantics while reusing the
    // worker's existing per-invocation stream state.
    pub async fn send_messages_now(&self, messages: Vec<Message>) -> buck2_error::Result<()> {
        self.send_messages_with_priority(messages, true).await
    }

    pub async fn send_messages_without_waiting_for_acks(
        &self,
        messages: Vec<Message>,
    ) -> buck2_error::Result<()> {
        self.send_messages_with_priority(messages, false).await
    }

    pub fn export_counters(&self) -> Counters {
        self.counters.snapshot()
    }

    /// Finish every open stream and wait for the acknowledgements. For the daemon's shutdown:
    /// the worker would close streams when this client is dropped, but the process exits
    /// first, and a stream left open shows as a build still running on the server.
    pub async fn close_all_streams(&self) -> buck2_error::Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.send_now_tx
            .send(SendNowRequest {
                messages: Vec::new(),
                wait_for_acks: true,
                close_all: true,
                done: done_tx,
            })
            .map_err(|_| {
                buck2_error::buck2_error!(ErrorTag::Tier0, "Failed to enqueue BES close request")
            })?;
        done_rx.await.map_err(|_| {
            buck2_error::buck2_error!(
                ErrorTag::Tier0,
                "BES worker dropped close response channel"
            )
        })?
    }
}

fn bes_worker_runtime() -> std::io::Result<Runtime> {
    // The worker loop blocks on crossbeam while idle. Keep spawned tonic
    // transport tasks running so BES events can reach the server before close.
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("buck2-bes-runtime")
        .enable_all()
        .build()
}

fn process_queued_message(
    runtime: &tokio::runtime::Runtime,
    worker: &mut WorkerState,
    message: Message,
) {
    worker.counters.dec_queue_depth();
    drop(runtime.block_on(worker.send_message_with_retry(&message, false)));
}

fn process_send_now_request(
    runtime: &tokio::runtime::Runtime,
    worker: &mut WorkerState,
    request: SendNowRequest,
) {
    if request.close_all {
        runtime.block_on(worker.close_all_streams_for_shutdown());
        drop(request.done.send(Ok(())));
        return;
    }
    // Desired behavior for the ACK-waiting path (mirroring Bazel's BES
    // uploader semantics):
    //
    // 1) `send_messages_now()` is an emergency path. Returning `Ok(())` means every event of this
    //    request was queued on an open stream for its invocation and, where that stream has
    //    been acknowledged before, that BES acknowledged it. A stream with no acknowledgement
    //    yet is not waited on: BuildBuddy acknowledges nothing until the client half-closes the
    //    stream, so the wait could only time out, holding up every queued event behind it. Such
    //    a stream fails the call only if its transport had already ended when the call checked;
    //    a rejection that arrives later is counted when the stream closes. Its events stay held
    //    for replay like any other, and closing the stream waits for the server to acknowledge
    //    them, so they are confirmed only if the worker closes the stream before the process
    //    exits. A sink opened to send one event, as `buck2 rage` does, is always in this case.
    // 2) Stream retries must preserve delivery intent for already-enqueued events.
    //
    // Bazel keeps an "unacked queue" and, after reconnect, replays unacked events with their
    // original sequence numbers. This implementation mirrors that strategy per invocation stream:
    // - each stream keeps pending unacked requests in order;
    // - reconnect replays those requests before accepting new progress;
    // - sequence numbers remain stable across reconnects.
    //
    // Because sequence numbers are stable, `send_now` can safely wait for the max sequence per
    // invocation from this request.
    let mut ack_targets: HashMap<String, i64> = HashMap::new();
    let mut result = Ok(());
    for message in &request.messages {
        match runtime.block_on(worker.send_message_with_retry(message, true)) {
            Ok(Some((invocation_id, sequence_number))) => {
                ack_targets
                    .entry(invocation_id)
                    .and_modify(|seq| *seq = (*seq).max(sequence_number))
                    .or_insert(sequence_number);
            }
            Ok(None) => {}
            Err(e) => {
                result = Err(e);
                break;
            }
        }
    }
    if request.wait_for_acks && result.is_ok() && !ack_targets.is_empty() {
        if let Err(status) = runtime.block_on(worker.wait_for_acks(&ack_targets)) {
            worker.record_status_failure(&status);
            result = Err(buck2_error::buck2_error!(
                ErrorTag::Tier0,
                "Failed waiting for BES acknowledgements: {} ({})",
                status.message(),
                status.code()
            ));
        }
    }
    drop(request.done.send(result));
}

struct WorkerState {
    config: BesConfig,
    connection: ConnectionConfig,
    counters: Arc<CounterState>,
    streams: HashMap<String, StreamState>,
    /// The remote answered UNAUTHENTICATED since the last stream was opened, so the next one
    /// asks the credential helper afresh instead of reusing what it cached.
    credentials_rejected: bool,
    /// Set once the streams were finished for shutdown. Events that arrive afterwards, from
    /// commands being cancelled, are dropped rather than opening a second stream for an
    /// invocation the server already saw end.
    closed: bool,
    /// The server refused this daemon's credentials and nothing can change them: the configured
    /// headers are fixed for the daemon's life, and either no credential helper can refresh them
    /// or its refreshed ones were refused too. No stream opens again; events are dropped.
    credentials_refused: bool,
    /// The credential helper's credentials were refreshed after a refusal, so the next refusal
    /// is final.
    refreshed_after_refusal: bool,
}

impl WorkerState {
    fn new(config: BesConfig, connection: ConnectionConfig, counters: Arc<CounterState>) -> Self {
        Self {
            config,
            connection,
            counters,
            streams: HashMap::new(),
            credentials_rejected: false,
            closed: false,
            credentials_refused: false,
            refreshed_after_refusal: false,
        }
    }

    /// How many events a stream keeps for replay. A server may hold every acknowledgement until
    /// the client half-closes the stream, so a healthy stream can pass this; it then keeps
    /// sending and lets the oldest copies go, which costs it only the ability to replay.
    fn max_unacked(&self) -> usize {
        self.config.buffer_size.saturating_mul(UNACKED_EVENTS_PER_QUEUED_EVENT)
    }

    fn batch_size(&self) -> usize {
        self.config
            .message_batch_size
            .unwrap_or(DEFAULT_BATCH_SIZE)
            .max(1)
    }

    async fn send_message_with_retry(
        &mut self,
        message: &Message,
        fail_fast: bool,
    ) -> buck2_error::Result<Option<(String, i64)>> {
        if self.closed || self.credentials_refused {
            self.counters.inc_dropped();
            return Ok(None);
        }
        // Route by each event's own invocation ID. Buck2 can process commands
        // concurrently, so a shared "active invocation" can misroute events
        // across streams and cause one command's end event to close another.
        //
        // We intentionally keep standalone non-record events in this path.
        // `send_messages_now()` callers expect to bypass queueing and still
        // deliver emergency events even without an active command context.
        let parsed = match ParsedMessage::from_message(message) {
            Ok(parsed) => parsed,
            Err(_) => {
                self.counters.inc_failures_invalid_request();
                return if fail_fast {
                    Err(buck2_error::buck2_error!(
                        ErrorTag::Tier0,
                        "Invalid Buck event payload"
                    ))
                } else {
                    Ok(None)
                };
            }
        };
        // The daemon's dispatcher in buck2_server's daemon/state.rs sends events outside any
        // command under the nil trace ID; a stream for them would stay open for the daemon's life.
        // They are not counted as dropped: that count is daemon-wide, and each command's record
        // reports its growth as BES events lost during the command.
        if parsed.is_daemon_scoped {
            return Ok(None);
        }

        if let Err(e) = self.ensure_stream_exists(&parsed) {
            self.counters.inc_failures_invalid_request();
            return if fail_fast { Err(e) } else { Ok(None) };
        }

        let close_after = Instant::now() + COMMAND_END_CLOSE_GRACE;
        let close_immediately = parsed.is_invocation_record;
        let sequence_number;
        let mut abandoned;
        {
            let stream = self
                .streams
                .get_mut(&parsed.invocation_id)
                .expect("stream was inserted");
            abandoned = stream.abandoned;
            sequence_number = if abandoned {
                self.counters.inc_dropped();
                None
            } else {
                stream
                    .enqueue_event(&parsed, self.config.event_format)
                    .await
            };

            if parsed.is_command_end || close_immediately {
                stream.saw_command_end = true;
                // The record closes the stream below; the deadline is for when that fails, so
                // the poll loop keeps trying and the stream is eventually abandoned and freed
                // instead of holding its events until the daemon exits.
                stream.pending_close = Some(PendingClose {
                    close_after: if close_immediately {
                        Instant::now()
                    } else {
                        close_after
                    },
                    event_time: parsed.event_time,
                });
            } else if stream.saw_command_end {
                // Keep extending the quiet-period deadline while tail events arrive.
                stream.pending_close = Some(PendingClose {
                    close_after,
                    event_time: parsed.event_time,
                });
            }
        }

        if !abandoned
            && self.streams[&parsed.invocation_id].pending_unacked.len() > self.max_unacked()
        {
            abandoned = !self.bound_unacked(&parsed.invocation_id);
        }
        if abandoned {
            if close_immediately {
                drop(
                    self.close_stream(&parsed.invocation_id, parsed.event_time)
                        .await,
                );
            }
            return if fail_fast {
                Err(buck2_error::buck2_error!(
                    ErrorTag::Tier0,
                    "BES stream for invocation {} was abandoned",
                    parsed.invocation_id
                ))
            } else {
                Ok(None)
            };
        }

        // A queued message never waits on the stream: while it is down, events collect in
        // `pending_unacked` and the first message after `next_attempt_at` tries again. A
        // priority message has a caller waiting on it, so it waits out the backoff and retries.
        let retries = if fail_fast {
            self.config.retry_attempts
        } else {
            0
        };
        let mut last_error: Option<Status> = None;

        for _ in 0..=retries {
            let Some(stream) = self
                .streams
                .get(&parsed.invocation_id)
                .filter(|stream| !stream.abandoned)
            else {
                break;
            };
            if let Some(wait) = stream.backoff_remaining(Instant::now()) {
                if !fail_fast {
                    return Ok(None);
                }
                tokio::time::sleep(wait).await;
            }
            match self.flush_stream(&parsed.invocation_id).await {
                Ok(()) => {
                    if close_immediately {
                        match self
                            .close_stream(&parsed.invocation_id, parsed.event_time)
                            .await
                        {
                            Ok(()) => {
                                self.counters.inc_success(parsed.payload_size as u64);
                                return Ok(None);
                            }
                            Err(status) => {
                                self.stream_failed(&parsed.invocation_id, &status);
                                last_error = Some(status);
                            }
                        }
                    } else {
                        self.counters.inc_success(parsed.payload_size as u64);
                        return Ok(sequence_number.map(|sequence_number| {
                            (parsed.invocation_id.clone(), sequence_number)
                        }));
                    }
                }
                Err(status) => {
                    self.stream_failed(&parsed.invocation_id, &status);
                    last_error = Some(status);
                }
            }
        }

        if fail_fast {
            let reason = match last_error {
                Some(status) => {
                    format!(
                        "Failed to send BES event after retries: {} ({})",
                        status.message(),
                        status.code()
                    )
                }
                None => "Failed to send BES event after retries".to_owned(),
            };
            return Err(buck2_error::buck2_error!(ErrorTag::Tier0, "{reason}"));
        }

        Ok(None)
    }

    fn ensure_stream_exists(&mut self, parsed: &ParsedMessage) -> buck2_error::Result<()> {
        if self.streams.contains_key(&parsed.invocation_id) {
            return Ok(());
        }
        let upload_config = BazelArtifactUploadConfig::from_bes(&self.config, &self.connection)?;
        let stream = StreamState::new(
            parsed,
            &self.config.build_metadata,
            upload_config,
            self.config.upload_successful_action_events,
        );
        self.streams.insert(parsed.invocation_id.clone(), stream);
        Ok(())
    }

    async fn flush_stream(&mut self, invocation_id: &str) -> Result<(), Status> {
        self.ensure_stream_transport(invocation_id).await?;
        let Some(stream) = self.streams.get_mut(invocation_id) else {
            return Err(Status::unavailable(format!(
                "BES stream state missing for invocation {}",
                invocation_id
            )));
        };
        stream.flush_pending(self.config.grpc_timeout).await?;
        stream.failing = None;
        Ok(())
    }

    /// Brings a stream's `pending_unacked` back to `max_unacked` by dropping the oldest copies
    /// already sent, and returns whether the stream lives on. A stream that is down needs every
    /// copy to replay, and events never sent are the only copy, so either way it is abandoned.
    fn bound_unacked(&mut self, invocation_id: &str) -> bool {
        let max_unacked = self.max_unacked();
        let Some(stream) = self.streams.get_mut(invocation_id) else {
            return false;
        };
        stream.prune_acked_requests();
        if stream.pending_unacked.len() <= max_unacked {
            return true;
        }
        if stream.failing.is_some() {
            self.abandon_stream(
                invocation_id,
                "more events await acknowledgement than the stream can keep while it is down",
            );
            return false;
        }
        if stream.transport_needs_reopen() {
            // The server ended the stream since the last flush, and nothing has noticed yet.
            // These copies are what the reopen replays, so the flush that follows keeps them:
            // it either replays them or fails the stream, and the next event bounds it.
            return true;
        }
        let first_drop = stream.replay_copies_dropped == 0;
        if !stream.drop_oldest_replay_copies(max_unacked) {
            // Only one message that enqueues more than `max_unacked` events gets here, because
            // a live stream sends everything it holds on each flush.
            self.abandon_stream(
                invocation_id,
                "one message enqueued more events than the stream can keep unacknowledged",
            );
            return false;
        }
        if first_drop {
            tracing::info!(
                "More than {} events of invocation {} await acknowledgement on the BES stream; it keeps sending, and lets go of the oldest copies, so it cannot be replayed until the server acknowledges past them",
                max_unacked,
                invocation_id,
            );
        }
        true
    }

    fn stream_failed(&mut self, invocation_id: &str, status: &Status) {
        self.record_status_failure(status);
        if buck2_credential_helper::status_rejects_credentials(status) {
            if self.connection.credential_helper.is_none() || self.refreshed_after_refusal {
                self.refuse_credentials(status);
                return;
            }
            // The next open asks the helper afresh (`credentials_rejected`); a refusal of those
            // credentials is final.
            self.refreshed_after_refusal = true;
        }
        let max_unacked = self.max_unacked();
        let Some(stream) = self.streams.get_mut(invocation_id) else {
            return;
        };
        stream.discard_transport();
        let replay_copies_dropped = stream.replay_copies_dropped;
        let replay_has_gap = stream.replay_has_gap();
        let now = Instant::now();
        let failing = stream.failing.get_or_insert_with(|| StreamFailure {
            since: now,
            next_attempt_at: now,
            attempts: 0,
            last_status: status.clone(),
        });
        failing.last_status = status.clone();
        // The last attempt falls on the window's edge rather than a whole backoff past it.
        failing.next_attempt_at = (now + backoff_for(&self.config.retry_backoff, failing.attempts))
            .min(failing.since + self.config.retry_window);
        failing.attempts += 1;
        if replay_has_gap {
            // A replay starts at the first unacknowledged event, whose copy is gone.
            self.abandon_stream(
                invocation_id,
                &format!(
                    "the stream failed after letting go of {replay_copies_dropped} sent but unacknowledged events to keep within {max_unacked}, so it cannot be replayed"
                ),
            );
        } else if now.duration_since(failing.since) >= self.config.retry_window {
            self.abandon_stream(
                invocation_id,
                "the stream failed for the whole retry window",
            );
        }
    }

    /// Turns this daemon's BES off for good after a refusal no retry can cure, dropping what the
    /// streams hold. A retry would redial with the same headers and be refused again; against one
    /// BuildBuddy deployment a daemon redialled 30 to 117 times in 10 to 15 s. The warning names
    /// the status code, never a header.
    fn refuse_credentials(&mut self, status: &Status) {
        if !self.credentials_refused {
            tracing::warn!(
                "The BES server refused this daemon's credentials ({:?}); it sends no build events until it restarts, because its `bes.header` values are fixed at startup and no credential helper can replace them",
                status.code(),
            );
        }
        self.credentials_refused = true;
        for stream in self.streams.values_mut() {
            self.counters
                .add_dropped(stream.pending_unacked.len() as u64);
            stream.pending_unacked = VecDeque::new();
            stream.discard_transport();
            stream.failing = None;
            stream.abandoned = true;
        }
    }

    /// Events that arrive for an abandoned invocation are dropped without reconnecting; the
    /// next invocation starts afresh. A reporting sink must not cost the build its memory.
    ///
    /// The `dropped` counter takes the events still held here. Replay copies let go of earlier
    /// were counted as successes when they were sent, and nothing says which of them reached the
    /// server, so they stay out of it; the warning gives their number instead.
    fn abandon_stream(&mut self, invocation_id: &str, reason: &str) {
        let Some(stream) = self.streams.get_mut(invocation_id) else {
            return;
        };
        let dropped = stream.pending_unacked.len();
        tracing::warn!(
            "Giving up on the BES stream for invocation {}: {}; dropping {} unacknowledged events and every later one; last failure: {}",
            invocation_id,
            reason,
            dropped,
            stream.failing.as_ref().map_or_else(
                || "none".to_owned(),
                |failing| failing.last_status.to_string()
            ),
        );
        self.counters.add_dropped(dropped as u64);
        stream.pending_unacked = VecDeque::new();
        stream.discard_transport();
        stream.failing = None;
        stream.abandoned = true;
    }

    async fn ensure_stream_transport(&mut self, invocation_id: &str) -> Result<(), Status> {
        let needs_reopen = match self.streams.get(invocation_id) {
            Some(stream) => stream.transport_needs_reopen(),
            None => {
                return Err(Status::unavailable(format!(
                    "BES stream state missing for invocation {}",
                    invocation_id
                )));
            }
        };

        if !needs_reopen {
            return Ok(());
        }

        let stream = self
            .streams
            .get_mut(invocation_id)
            .expect("stream exists before reopen");
        if stream.replay_has_gap() {
            // The server ended the stream between flushes. Reopening would replay from the
            // first unacknowledged event, whose copy is gone, and leave a gap the server
            // rejects; failing here lets `stream_failed` abandon it.
            return Err(stream
                .finished_ack_task_status()
                .await
                .unwrap_or_else(|| Status::unavailable("BES stream closed")));
        }
        // A refused stream ends its ack task with UNAUTHENTICATED or PERMISSION_DENIED. Reopening
        // at once, as for a stream the server merely closed, would redial on every event; the
        // caller's `stream_failed` decides instead.
        if let Some(status) = stream.finished_ack_task_status().await
            && buck2_credential_helper::status_rejects_credentials(&status)
        {
            return Err(status);
        }

        self.discard_stream_transport(invocation_id);
        if std::mem::take(&mut self.credentials_rejected) {
            if let Some(helper) = &self.connection.credential_helper {
                helper.invalidate().await;
            }
        }
        let last_acked_sequence_number = {
            let stream = self
                .streams
                .get(invocation_id)
                .expect("stream still exists before reopen");
            stream.last_acked_sequence_number.clone()
        };
        let (sender, ack_task) = self
            .open_stream_transport(last_acked_sequence_number)
            .await?;
        let stream = self
            .streams
            .get_mut(invocation_id)
            .expect("stream still exists after reopen");
        stream.attach_transport(sender, ack_task);
        Ok(())
    }

    async fn open_stream_transport(
        &self,
        last_acked_sequence_number: Arc<AtomicI64>,
    ) -> Result<
        (
            mpsc::Sender<PublishBuildToolEventStreamRequest>,
            tokio::task::JoinHandle<Result<(), Status>>,
        ),
        Status,
    > {
        // Keep stream RPCs open for the duration of the build; use this value
        // only to bound connection establishment.
        let endpoint = endpoint_for(
            &self.connection.endpoint,
            self.config.grpc_timeout,
            &self.connection.tls,
        )?;
        let channel = endpoint.connect().await.map_err(map_transport_error)?;
        let mut client = PublishBuildEventClient::new(channel);
        let (tx, rx) = mpsc::channel(self.config.buffer_size.max(1));
        let outbound = ReceiverStream::new(rx);
        let mut request = tonic::Request::new(outbound);
        attach_headers(
            &mut request,
            &self.connection.headers,
            self.connection.credential_helper.as_deref(),
            &self.connection.endpoint,
        )
        .await?;

        let ack_sequence_for_task = last_acked_sequence_number;
        // Don't block stream creation on response headers; start sending events
        // immediately and handle response/ACK processing in the background task.
        let ack_task = tokio::spawn(async move {
            let response: tonic::Response<
                tonic::codec::Streaming<PublishBuildToolEventStreamResponse>,
            > = client.publish_build_tool_event_stream(request).await?;
            let mut inbound = response.into_inner();
            loop {
                match inbound.message().await {
                    Ok(Some(response)) => {
                        update_max_sequence_number(
                            &ack_sequence_for_task,
                            response.sequence_number,
                        );
                    }
                    Ok(None) => return Ok(()),
                    Err(e) => return Err(e),
                }
            }
        });

        Ok((tx, ack_task))
    }

    async fn close_due_streams(&mut self) {
        let now = Instant::now();
        let due = self
            .streams
            .iter()
            .filter(|(_, stream)| stream.backoff_remaining(now).is_none())
            .filter_map(|(invocation_id, stream)| {
                stream.pending_close.as_ref().and_then(|pending_close| {
                    (pending_close.close_after <= now)
                        .then(|| (invocation_id.clone(), pending_close.event_time))
                })
            })
            .collect::<Vec<_>>();

        for (invocation_id, event_time) in due {
            if let Err(status) = self.close_stream(&invocation_id, event_time).await {
                self.stream_failed(&invocation_id, &status);
            }
        }
    }

    async fn close_stream(
        &mut self,
        invocation_id: &str,
        event_time: Option<Timestamp>,
    ) -> Result<(), Status> {
        match self.streams.get(invocation_id) {
            None => return Ok(()),
            Some(stream) if stream.abandoned => {
                self.streams.remove(invocation_id);
                return Ok(());
            }
            Some(_) => {}
        }

        {
            let stream = self
                .streams
                .get_mut(invocation_id)
                .expect("stream exists before close");
            if !stream.stream_finished_enqueued {
                let finish_event = BuildEvent {
                    event_time: event_time.or_else(|| Some(SystemTime::now().into())),
                    event: Some(build_event::Event::ComponentStreamFinished(
                        build_event::BuildComponentStreamFinished {
                            r#type:
                                build_event::build_component_stream_finished::FinishType::Finished
                                    as i32,
                        },
                    )),
                };
                stream.enqueue_raw_event(finish_event);
                stream.stream_finished_enqueued = true;
            }
        }

        self.flush_stream(invocation_id).await?;

        {
            let stream = self
                .streams
                .get_mut(invocation_id)
                .expect("stream exists after successful close flush");
            stream.pending_close = None;
        }

        let Some(mut stream) = self.streams.remove(invocation_id) else {
            return Ok(());
        };
        drop(stream.sender.take());

        let close_timeout = close_ack_timeout(self.config.grpc_timeout);
        let Some(ack_task) = stream.ack_task.take() else {
            return Err(Status::unavailable(
                "BES stream was closed before finish acknowledgement",
            ));
        };
        match tokio::time::timeout(close_timeout, ack_task).await {
            Ok(joined) => match joined {
                Ok(Ok(())) => Ok(()),
                Ok(Err(status)) => Err(status),
                Err(e) => Err(Status::internal(e.to_string())),
            },
            Err(_) => Err(Status::deadline_exceeded(format!(
                "Timed out waiting for BES stream acknowledgements after {:?}",
                close_timeout
            ))),
        }
    }

    fn discard_stream_transport(&mut self, invocation_id: &str) {
        let Some(stream) = self.streams.get_mut(invocation_id) else {
            return;
        };
        stream.discard_transport();
    }

    /// Shutdown: a stream whose command never ended gets the final event the server keys on,
    /// marked interrupted, before the stream is finished. Without it the server has no
    /// `BuildFinished` and shows the build running forever.
    async fn close_all_streams_for_shutdown(&mut self) {
        if self.config.event_format == BesEventFormat::Bazel {
            let now: Option<Timestamp> = Some(SystemTime::now().into());
            for stream in self.streams.values_mut() {
                if !stream.saw_command_end && !stream.stream_finished_enqueued {
                    stream.enqueue_raw_event(BuildEvent {
                        event_time: now.clone(),
                        event: Some(build_event::Event::BazelEvent(encode_bep_event(
                            &interrupted_finish_event(now.clone()),
                        ))),
                    });
                    stream.saw_command_end = true;
                }
            }
        }
        self.close_all_streams().await;
        self.closed = true;
    }

    async fn close_all_streams(&mut self) {
        let invocation_ids = self.streams.keys().cloned().collect::<Vec<_>>();
        for invocation_id in invocation_ids {
            if let Err(status) = self.close_stream(&invocation_id, None).await {
                self.record_status_failure(&status);
            }
        }
    }

    async fn wait_for_acks(&self, ack_targets: &HashMap<String, i64>) -> Result<(), Status> {
        let deadline = Instant::now() + close_ack_timeout(self.config.grpc_timeout);
        loop {
            let mut all_acked = true;
            for (invocation_id, target_sequence_number) in ack_targets {
                let Some(stream) = self.streams.get(invocation_id) else {
                    continue;
                };
                let acked = stream.last_acked_sequence_number();
                if acked >= *target_sequence_number {
                    continue;
                }
                if !stream.can_receive_more_acks() {
                    return Err(Status::unavailable(format!(
                        "BES stream closed before sequence {} was acknowledged for invocation {}",
                        target_sequence_number, invocation_id
                    )));
                }
                // Sequence numbers start at 1, so this stream was never acknowledged, and its
                // server may be one that acknowledges only at EOF.
                if acked == 0 {
                    continue;
                }
                all_acked = false;
            }

            if all_acked {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Status::deadline_exceeded(format!(
                    "Timed out waiting for BES acknowledgements after {:?}",
                    close_ack_timeout(self.config.grpc_timeout)
                )));
            }

            tokio::time::sleep(COMMAND_END_CLOSE_POLL_INTERVAL).await;
        }
    }

    fn record_status_failure(&mut self, status: &Status) {
        // Also true of a proxy's HTTP 401, which tonic reports as INTERNAL.
        if buck2_credential_helper::status_rejects_credentials(status) {
            self.credentials_rejected = true;
        }
        match status.code() {
            tonic::Code::InvalidArgument | tonic::Code::FailedPrecondition => {
                self.counters.inc_failures_invalid_request();
            }
            tonic::Code::Unauthenticated => {
                self.counters.inc_failures_unauthorized();
            }
            tonic::Code::PermissionDenied => {
                self.counters.inc_failures_unauthorized();
            }
            tonic::Code::ResourceExhausted => {
                self.counters.inc_failures_rate_limited();
            }
            tonic::Code::Unavailable => {
                self.counters.inc_failures_pushed_back();
            }
            tonic::Code::DeadlineExceeded => {
                self.counters.inc_failures_timed_out();
            }
            tonic::Code::Unknown
            | tonic::Code::Internal
            | tonic::Code::DataLoss
            | tonic::Code::Aborted
            | tonic::Code::OutOfRange
            | tonic::Code::Unimplemented => {
                self.counters.inc_failures_internal_error();
            }
            _ => {
                self.counters.inc_failures_unknown();
            }
        }
    }
}

struct StreamState {
    stream_id: StreamId,
    next_sequence_number: i64,
    last_acked_sequence_number: Arc<AtomicI64>,
    sender: Option<mpsc::Sender<PublishBuildToolEventStreamRequest>>,
    ack_task: Option<tokio::task::JoinHandle<Result<(), Status>>>,
    project_id: String,
    pending_unacked: VecDeque<PublishBuildToolEventStreamRequest>,
    bazel_converter: BazelEventConverter,
    bazel_artifact_uploader: Option<BazelArtifactUploader>,
    last_sent_sequence_number: i64,
    saw_command_end: bool,
    pending_close: Option<PendingClose>,
    stream_finished_enqueued: bool,
    /// Set while the transport is down. A successful flush clears it.
    failing: Option<StreamFailure>,
    /// The sink gave up on this invocation: events are dropped and nothing reconnects.
    abandoned: bool,
    /// Sent events whose copies were dropped from `pending_unacked` before they were
    /// acknowledged, over the stream's life.
    replay_copies_dropped: u64,
    /// The sequence number of the newest dropped copy. Until the server acknowledges it, a
    /// replay would leave a gap, so a failure abandons the stream instead of retrying.
    newest_dropped_replay_copy: i64,
}

struct PendingClose {
    close_after: Instant,
    event_time: Option<Timestamp>,
}

struct StreamFailure {
    since: Instant,
    next_attempt_at: Instant,
    attempts: usize,
    last_status: Status,
}

impl StreamState {
    fn new(
        parsed: &ParsedMessage,
        build_metadata: &[(String, String)],
        bazel_artifact_upload_config: Option<BazelArtifactUploadConfig>,
        upload_successful_action_events: bool,
    ) -> Self {
        Self {
            stream_id: StreamId {
                build_id: parsed.build_id.clone(),
                invocation_id: parsed.invocation_id.clone(),
                component: stream_id::BuildComponent::Tool as i32,
            },
            next_sequence_number: 1,
            last_acked_sequence_number: Arc::new(AtomicI64::new(0)),
            sender: None,
            ack_task: None,
            project_id: parsed.project_id.clone(),
            pending_unacked: VecDeque::new(),
            bazel_converter: BazelEventConverter::new_with_options(
                build_metadata.iter().cloned(),
                upload_successful_action_events,
            ),
            bazel_artifact_uploader: bazel_artifact_upload_config.map(BazelArtifactUploader::new),
            last_sent_sequence_number: 0,
            saw_command_end: false,
            pending_close: None,
            stream_finished_enqueued: false,
            failing: None,
            abandoned: false,
            replay_copies_dropped: 0,
            newest_dropped_replay_copy: 0,
        }
    }

    /// How long until the stream may be retried, or `None` when it may be flushed now.
    fn backoff_remaining(&self, now: Instant) -> Option<Duration> {
        self.failing
            .as_ref()
            .and_then(|failing| failing.next_attempt_at.checked_duration_since(now))
            .filter(|remaining| !remaining.is_zero())
    }

    async fn enqueue_event(
        &mut self,
        parsed: &ParsedMessage,
        event_format: BesEventFormat,
    ) -> Option<i64> {
        match event_format {
            BesEventFormat::Buck => Some(self.enqueue_raw_event(BuildEvent {
                event_time: parsed.event_time,
                event: Some(build_event::Event::ExperimentalBuildToolEvent(Any {
                    type_url: BUCK2_EVENT_TYPE_URL.to_owned(),
                    value: parsed.payload.clone(),
                })),
            })),
            BesEventFormat::Bazel => {
                if let Some(uploader) = self.bazel_artifact_uploader.as_mut() {
                    uploader.observe_buck_event(&parsed.buck_event);
                }
                let events = self
                    .bazel_converter
                    .convert(self.next_sequence_number, &parsed.buck_event);
                if let Some(uploader) = self.bazel_artifact_uploader.as_mut() {
                    uploader.observe_bazel_events(&events);
                }
                let mut last_sequence_number = None;
                for mut event in events {
                    if let Some(uploader) = self.bazel_artifact_uploader.as_mut() {
                        uploader.upload_event_files(&mut event).await;
                    }
                    last_sequence_number = Some(self.enqueue_raw_event(BuildEvent {
                        event_time: parsed.event_time,
                        event: Some(build_event::Event::BazelEvent(encode_bep_event(&event))),
                    }));
                }
                last_sequence_number
            }
        }
    }

    fn enqueue_raw_event(&mut self, event: BuildEvent) -> i64 {
        let seq = self.next_sequence_number;
        self.next_sequence_number += 1;

        let mut request = PublishBuildToolEventStreamRequest {
            ordered_build_event: Some(OrderedBuildEvent {
                stream_id: Some(self.stream_id.clone()),
                sequence_number: seq,
                event: Some(event),
            }),
            notification_keywords: Vec::new(),
            project_id: self.project_id.clone(),
            check_preceding_lifecycle_events_present: false,
        };
        if seq == 1 {
            request
                .notification_keywords
                .push("source=buck2".to_owned());
        }
        self.pending_unacked.push_back(request);
        seq
    }

    fn transport_needs_reopen(&self) -> bool {
        match (&self.sender, &self.ack_task) {
            (Some(_), Some(task)) => task.is_finished(),
            _ => true,
        }
    }

    fn attach_transport(
        &mut self,
        sender: mpsc::Sender<PublishBuildToolEventStreamRequest>,
        ack_task: tokio::task::JoinHandle<Result<(), Status>>,
    ) {
        self.sender = Some(sender);
        self.ack_task = Some(ack_task);
        self.prune_acked_requests();
        self.last_sent_sequence_number = self.last_acked_sequence_number();
    }

    fn discard_transport(&mut self) {
        drop(self.sender.take());
        if let Some(ack_task) = self.ack_task.take() {
            if !ack_task.is_finished() {
                ack_task.abort();
            }
        }
    }

    async fn flush_pending(&mut self, send_timeout: Duration) -> Result<(), Status> {
        self.prune_acked_requests();
        let sender = match self.sender.clone() {
            Some(sender) => sender,
            None => {
                if let Some(status) = self.finished_ack_task_status().await {
                    return Err(status);
                }
                return Err(Status::unavailable("BES stream was not open"));
            }
        };

        let pending = self
            .pending_unacked
            .iter()
            .filter(|request| request_sequence_number(request) > self.last_sent_sequence_number)
            .cloned()
            .collect::<Vec<_>>();
        for request in pending {
            let sequence_number = request_sequence_number(&request);
            // A server that stays connected but stops reading fills the channel; without a
            // bound the worker thread would wait here for the rest of the daemon's life.
            match tokio::time::timeout(send_timeout, sender.send(request)).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    if let Some(status) = self.finished_ack_task_status().await {
                        return Err(status);
                    }
                    return Err(Status::unavailable("BES stream was closed"));
                }
                Err(_) => {
                    return Err(Status::deadline_exceeded(format!(
                        "BES stream accepted no event for {:?}",
                        send_timeout
                    )));
                }
            }
            self.last_sent_sequence_number = sequence_number;
        }
        Ok(())
    }

    /// Drops the oldest entries of `pending_unacked` down to `max`, provided they were all sent:
    /// a server that acknowledges at the end still acknowledges their sequence numbers. Returns
    /// false, dropping nothing, when the excess includes events never sent.
    fn drop_oldest_replay_copies(&mut self, max: usize) -> bool {
        let excess = self.pending_unacked.len().saturating_sub(max);
        if excess == 0 {
            return true;
        }
        // `pending_unacked` is in sequence order, so the newest entry to drop decides.
        let newest_dropped = request_sequence_number(&self.pending_unacked[excess - 1]);
        if newest_dropped > self.last_sent_sequence_number {
            return false;
        }
        self.pending_unacked.drain(..excess);
        self.replay_copies_dropped += excess as u64;
        self.newest_dropped_replay_copy = newest_dropped;
        true
    }

    /// Whether a replay from the first unacknowledged event would need a copy that is gone.
    fn replay_has_gap(&self) -> bool {
        self.last_acked_sequence_number() < self.newest_dropped_replay_copy
    }

    fn prune_acked_requests(&mut self) {
        let acked = self.last_acked_sequence_number();
        while self
            .pending_unacked
            .front()
            .is_some_and(|request| request_sequence_number(request) <= acked)
        {
            self.pending_unacked.pop_front();
        }
        if self.last_sent_sequence_number < acked {
            self.last_sent_sequence_number = acked;
        }
    }

    fn can_receive_more_acks(&self) -> bool {
        self.ack_task
            .as_ref()
            .is_some_and(|task| !task.is_finished())
    }

    fn last_acked_sequence_number(&self) -> i64 {
        self.last_acked_sequence_number.load(Ordering::Relaxed)
    }

    async fn finished_ack_task_status(&mut self) -> Option<Status> {
        let task = self.ack_task.as_ref()?;
        if !task.is_finished() {
            return None;
        }
        drop(self.sender.take());
        let task = self.ack_task.take()?;
        match task.await {
            Ok(Ok(())) => Some(Status::unavailable("BES stream closed")),
            Ok(Err(status)) => Some(status),
            Err(e) => Some(Status::internal(e.to_string())),
        }
    }
}

fn request_sequence_number(request: &PublishBuildToolEventStreamRequest) -> i64 {
    request
        .ordered_build_event
        .as_ref()
        .map_or(0, |ordered| ordered.sequence_number)
}

struct ParsedMessage {
    build_id: String,
    invocation_id: String,
    project_id: String,
    event_time: Option<Timestamp>,
    buck_event: buck2_data::BuckEvent,
    payload: Vec<u8>,
    payload_size: usize,
    is_command_end: bool,
    is_invocation_record: bool,
    /// Sent by the daemon's own dispatcher under the nil trace ID, outside any command.
    is_daemon_scoped: bool,
}

impl ParsedMessage {
    // Normalize invocation IDs at parse time so every downstream path uses the
    // same stable key. This avoids random remapping and keeps stream routing
    // deterministic when trace IDs are malformed.
    //
    // For missing IDs, we intentionally generate a random UUID so emergency
    // standalone events don't collapse into a shared synthetic stream key.
    fn from_message(message: &Message) -> Result<Self, ()> {
        let event = buck2_data::BuckEvent::decode(message.message.as_slice()).map_err(|_| ())?;
        let event_id = if !event.trace_id.is_empty() {
            normalize_invocation_id(&event.trace_id)
        } else if let Some(message_key) = message.message_key {
            normalize_invocation_id(&message_key.to_string())
        } else {
            uuid::Uuid::new_v4().to_string()
        };

        let event_time = event.timestamp;
        let is_command_end = is_command_end(&event);
        let is_invocation_record = is_invocation_record(&event);
        let is_daemon_scoped =
            uuid::Uuid::parse_str(&event.trace_id).is_ok_and(|trace_id| trace_id.is_nil());

        Ok(Self {
            build_id: event_id.clone(),
            invocation_id: event_id,
            project_id: message.category.clone(),
            event_time,
            buck_event: event,
            payload_size: message.message.len(),
            payload: message.message.clone(),
            is_command_end,
            is_invocation_record,
            is_daemon_scoped,
        })
    }
}

fn update_max_sequence_number(slot: &AtomicI64, value: i64) {
    let mut current = slot.load(Ordering::Relaxed);
    while value > current {
        match slot.compare_exchange(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

fn backoff_for(initial_backoff: &Duration, attempt: usize) -> Duration {
    let capped = attempt.min(8);
    let multiplier = 1u32 << capped;
    initial_backoff
        .checked_mul(multiplier)
        .unwrap_or(*initial_backoff)
}

fn close_ack_timeout(grpc_timeout: Duration) -> Duration {
    let multiplied = grpc_timeout
        .checked_mul(CLOSE_ACK_TIMEOUT_MULTIPLIER)
        .unwrap_or(grpc_timeout);
    if multiplied < MIN_CLOSE_ACK_TIMEOUT {
        MIN_CLOSE_ACK_TIMEOUT
    } else {
        multiplied
    }
}

// The BES stream ID expects UUID-shaped IDs. We preserve valid UUIDs
// and map malformed IDs deterministically so the same malformed input always
// routes to the same stream.
fn normalize_invocation_id(invocation_id: &str) -> String {
    if let Ok(invocation_id) = uuid::Uuid::parse_str(invocation_id) {
        invocation_id.to_string()
    } else {
        deterministic_uuid_from(invocation_id).to_string()
    }
}

fn deterministic_uuid_from(input: &str) -> uuid::Uuid {
    // FNV-1a 128-bit. Good enough for stable buck2-internal ID normalization
    // without pulling in an additional hash dependency.
    const FNV_OFFSET_BASIS: u128 = 0x6c62272e07bb014262b821756295c58d;
    const FNV_PRIME: u128 = 0x0000000001000000000000000000013B;

    let mut hash = FNV_OFFSET_BASIS;
    for b in input.bytes() {
        hash ^= u128::from(b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    let mut bytes = hash.to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

fn is_command_end(event: &buck2_data::BuckEvent) -> bool {
    match &event.data {
        Some(buck_event::Data::SpanEnd(span_end)) => {
            matches!(span_end.data, Some(span_end_event::Data::Command(_)))
        }
        _ => false,
    }
}

fn is_invocation_record(event: &buck2_data::BuckEvent) -> bool {
    match &event.data {
        Some(buck_event::Data::Record(record)) => {
            matches!(record.data, Some(record_event::Data::InvocationRecord(_)))
        }
        _ => false,
    }
}

fn map_transport_error(err: tonic::transport::Error) -> Status {
    Status::unavailable(err.to_string())
}

/// The configured headers, then the credential helper's for `endpoint`. A helper header
/// replaces a configured header of the same name, and a helper header with several values
/// keeps all of them, as in the remote execution client.
async fn attach_headers<T>(
    request: &mut tonic::Request<T>,
    headers: &[(String, String)],
    credential_helper: Option<&CredentialHelper>,
    endpoint: &str,
) -> Result<(), Status> {
    let metadata = request.metadata_mut();
    for (header_key, header_value) in headers {
        let metadata_key = MetadataKey::from_bytes(header_key.as_bytes())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let metadata_value = MetadataValue::try_from(header_value.as_str())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        metadata.insert(metadata_key, metadata_value);
    }
    let Some(helper) = credential_helper else {
        return Ok(());
    };
    // The same URI the remote execution client hands the helper for this endpoint, so a helper
    // that keys its answers on it sees one endpoint, not two.
    let uri = format!("{}/", endpoint.trim_end_matches('/'));
    let credentials = helper
        .get(&uri)
        .await
        .map_err(|e| Status::unauthenticated(format!("{e:#}")))?;
    for (key, _) in credentials.headers() {
        metadata.remove(key.clone());
    }
    for (key, value) in credentials.headers() {
        metadata.append(key.clone(), value.clone());
    }
    Ok(())
}

fn endpoint_for(uri: &str, connect_timeout: Duration, tls: &BesTls) -> Result<Endpoint, Status> {
    let mut endpoint = Endpoint::from_shared(uri.to_owned())
        .map_err(|e| Status::internal(e.to_string()))?
        .connect_timeout(connect_timeout);
    if uri
        .split_once("://")
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("https"))
    {
        let mut tls_config = match &tls.ca_certs {
            Some(path) => ClientTlsConfig::new().ca_certificate(Certificate::from_pem(read_pem(path)?)),
            None => ClientTlsConfig::new().with_enabled_roots(),
        };
        if let Some(path) = &tls.client_cert {
            // One file holds the certificate chain and the key, as for the remote execution
            // client's `tls_client_cert`.
            let pem = read_pem(path)?;
            tls_config = tls_config.identity(Identity::from_pem(&pem, &pem));
        }
        endpoint = endpoint
            .tls_config(tls_config)
            .map_err(|e| Status::internal(e.to_string()))?;
    }
    Ok(endpoint)
}

fn read_pem(path: &str) -> Result<Vec<u8>, Status> {
    let mut env = |name: &str| std::env::var(name).ok();
    let path = crate::sink::remote::expand_bes_config_env_vars_with(path, &mut env);
    std::fs::read(&path).map_err(|e| Status::internal(format!("reading `{path}`: {e}")))
}

fn bes_backend(configured_endpoint: Option<&str>) -> buck2_error::Result<String> {
    let endpoint = configured_endpoint
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            buck2_error::buck2_error!(
                ErrorTag::Input,
                "BES backend is not configured (set `[bes] backend`)"
            )
        })?;

    let (scheme, target) = endpoint.split_once("://").ok_or_else(|| {
        buck2_error::buck2_error!(
            ErrorTag::Input,
            "Invalid BES backend `{}` (expected `grpc://HOST[:PORT]` or `grpcs://HOST[:PORT]`)",
            endpoint
        )
    })?;

    let target = target.trim();
    if target.is_empty() {
        return Err(buck2_error::buck2_error!(
            ErrorTag::Input,
            "Invalid BES backend `{}` (missing target host)",
            endpoint
        ));
    }

    if scheme.eq_ignore_ascii_case("grpc") {
        Ok(format!("http://{target}"))
    } else if scheme.eq_ignore_ascii_case("grpcs") {
        Ok(format!("https://{target}"))
    } else {
        Err(buck2_error::buck2_error!(
            ErrorTag::Input,
            "Invalid BES backend `{}` (expected scheme `grpc://` or `grpcs://`)",
            endpoint
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use bes_grpc_proto::google::devtools::build::v1::PublishLifecycleEventRequest;
    use bes_grpc_proto::google::devtools::build::v1::publish_build_event_server::PublishBuildEvent;
    use bes_grpc_proto::google::devtools::build::v1::publish_build_event_server::PublishBuildEventServer;
    use buck2_wrapper_common::invocation_id::TraceId;

    use super::*;

    fn command_start_data() -> buck2_data::buck_event::Data {
        buck2_data::buck_event::Data::SpanStart(buck2_data::SpanStartEvent {
            data: Some(buck2_data::CommandStart::default().into()),
        })
    }

    fn invocation_record_data() -> buck2_data::buck_event::Data {
        buck2_data::buck_event::Data::Record(buck2_data::RecordEvent {
            data: Some(record_event::Data::InvocationRecord(Box::default())),
        })
    }

    fn make_message(
        trace_id: Option<&str>,
        message_key: Option<i64>,
        data: buck2_data::buck_event::Data,
    ) -> Message {
        let event = buck2_data::BuckEvent {
            timestamp: Some(SystemTime::now().into()),
            trace_id: trace_id.unwrap_or_default().to_owned(),
            span_id: 0,
            parent_id: 0,
            data: Some(data),
        };
        Message {
            category: "test".to_owned(),
            message: event.encode_to_vec(),
            message_key,
        }
    }

    #[test]
    fn normalize_invocation_id_is_deterministic_for_invalid_input() {
        let normalized_1 = normalize_invocation_id("not-a-uuid");
        let normalized_2 = normalize_invocation_id("not-a-uuid");
        assert_eq!(normalized_1, normalized_2);
        assert!(uuid::Uuid::parse_str(&normalized_1).is_ok());
    }

    #[test]
    fn parsed_message_preserves_per_event_trace_id() {
        let trace_a = TraceId::new().to_string();
        let trace_b = TraceId::new().to_string();

        let message_a = make_message(Some(&trace_a), Some(1), command_start_data());
        let message_b = make_message(Some(&trace_b), Some(2), command_start_data());

        let parsed_a = ParsedMessage::from_message(&message_a).expect("valid message");
        let parsed_b = ParsedMessage::from_message(&message_b).expect("valid message");

        assert_eq!(parsed_a.build_id, trace_a);
        assert_eq!(parsed_a.invocation_id, trace_a);
        assert_eq!(parsed_b.build_id, trace_b);
        assert_eq!(parsed_b.invocation_id, trace_b);
        assert_ne!(parsed_a.invocation_id, parsed_b.invocation_id);
    }

    #[test]
    fn parsed_message_uses_message_key_when_trace_id_is_missing() {
        let message = make_message(None, Some(42), command_start_data());
        let parsed = ParsedMessage::from_message(&message).expect("valid message");
        assert_eq!(parsed.build_id, normalize_invocation_id("42"));
        assert_eq!(parsed.invocation_id, normalize_invocation_id("42"));
    }

    #[test]
    fn parsed_message_marks_nil_trace_as_daemon_scoped() {
        let daemon = make_message(
            Some(&TraceId::null().to_string()),
            Some(1),
            command_start_data(),
        );
        let command = make_message(
            Some(&TraceId::new().to_string()),
            Some(1),
            command_start_data(),
        );
        assert!(
            ParsedMessage::from_message(&daemon)
                .expect("valid message")
                .is_daemon_scoped
        );
        assert!(
            !ParsedMessage::from_message(&command)
                .expect("valid message")
                .is_daemon_scoped
        );
    }

    #[test]
    fn parsed_message_uses_random_uuid_only_when_ids_are_missing() {
        let message_a = make_message(None, None, command_start_data());
        let message_b = make_message(None, None, command_start_data());
        let parsed_a = ParsedMessage::from_message(&message_a).expect("valid message");
        let parsed_b = ParsedMessage::from_message(&message_b).expect("valid message");

        assert!(uuid::Uuid::parse_str(&parsed_a.build_id).is_ok());
        assert!(uuid::Uuid::parse_str(&parsed_a.invocation_id).is_ok());
        assert_eq!(parsed_a.build_id, parsed_a.invocation_id);

        assert!(uuid::Uuid::parse_str(&parsed_b.build_id).is_ok());
        assert!(uuid::Uuid::parse_str(&parsed_b.invocation_id).is_ok());
        assert_eq!(parsed_b.build_id, parsed_b.invocation_id);

        // Random fallback should avoid forcing all such events into one stream.
        assert_ne!(parsed_a.build_id, parsed_b.build_id);
    }

    #[test]
    fn bes_backend_accepts_grpc_and_grpcs_endpoints() {
        assert_eq!(
            bes_backend(Some("grpc://localhost:8980")).unwrap(),
            "http://localhost:8980"
        );
        assert_eq!(
            bes_backend(Some("grpcs://example.com:443")).unwrap(),
            "https://example.com:443"
        );
    }

    #[test]
    fn bes_backend_rejects_non_grpc_endpoints() {
        for endpoint in [
            "http://localhost:8980",
            "https://localhost:8980",
            "localhost:8980",
        ] {
            assert!(
                bes_backend(Some(endpoint)).is_err(),
                "expected `{}` to be rejected",
                endpoint
            );
        }
    }

    #[test]
    fn endpoint_authority_extracts_host_port() {
        assert_eq!(
            endpoint_authority("http://localhost:1985/foo").as_deref(),
            Some("localhost:1985")
        );
        assert_eq!(
            endpoint_authority("https://bes.example.com").as_deref(),
            Some("bes.example.com")
        );
    }

    #[test]
    fn re_client_cas_endpoint_defaults_to_tls() {
        assert_eq!(
            re_client_cas_endpoint("remote.buildbuddy.io").as_deref(),
            Some("https://remote.buildbuddy.io")
        );
        assert_eq!(
            re_client_cas_endpoint("grpc://localhost:1985").as_deref(),
            Some("http://localhost:1985")
        );
        assert_eq!(
            re_client_cas_endpoint("grpcs://remote.buildbuddy.io").as_deref(),
            Some("https://remote.buildbuddy.io")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn helper_headers_replace_configured_headers_of_the_same_name() {
        let dir = tempfile::tempdir().unwrap();
        let request_file = dir.path().join("request.json");
        let script = dir.path().join("helper.sh");
        std::fs::write(
            &script,
            format!(
                "cat > {}\necho '{{\"headers\": {{\"authorization\": [\"Bearer helper\"], \"x-multi\": [\"a\", \"b\"]}}}}'",
                request_file.display()
            ),
        )
        .unwrap();
        let helper = CredentialHelper::new(
            vec!["/bin/sh".to_owned(), script.to_str().unwrap().to_owned()],
            None,
            None,
        )
        .unwrap();
        let headers = vec![
            ("authorization".to_owned(), "Bearer configured".to_owned()),
            ("x-configured".to_owned(), "kept".to_owned()),
        ];

        let mut request = tonic::Request::new(());
        attach_headers(
            &mut request,
            &headers,
            Some(&helper),
            "https://bes.example.com:443",
        )
        .await
        .unwrap();

        let metadata = request.metadata();
        assert_eq!(
            metadata
                .get_all("authorization")
                .iter()
                .map(|v| v.to_str().unwrap())
                .collect::<Vec<_>>(),
            ["Bearer helper"]
        );
        assert_eq!(metadata.get("x-configured").unwrap(), "kept");
        assert_eq!(
            metadata
                .get_all("x-multi")
                .iter()
                .map(|v| v.to_str().unwrap())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(
            std::fs::read_to_string(&request_file).unwrap(),
            r#"{"uri":"https://bes.example.com:443/"}"#
        );

        let mut request = tonic::Request::new(());
        attach_headers(&mut request, &headers, None, "https://bes.example.com:443")
            .await
            .unwrap();
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer configured"
        );
    }

    #[test]
    fn bazel_artifact_upload_defaults_to_re_client_cas() {
        let config = BesConfig {
            event_format: BesEventFormat::Bazel,
            re_client_cas_address: Some("remote.buildbuddy.io".to_owned()),
            re_client_instance_name: Some("instance".to_owned()),
            ..Default::default()
        };
        let connection = ConnectionConfig {
            endpoint: "https://bes.example.com".to_owned(),
            headers: Vec::new(),
            tls: BesTls::default(),
            credential_helper: None,
        };
        let upload = BazelArtifactUploadConfig::from_bes(&config, &connection)
            .unwrap()
            .unwrap();
        assert_eq!(upload.endpoint, "https://remote.buildbuddy.io");
        assert_eq!(upload.uri_authority, "remote.buildbuddy.io");
        assert_eq!(upload.instance_name, "instance");
        assert_eq!(upload.max_bytes, 10 * 1024 * 1024);
    }

    #[test]
    fn bytestream_uri_includes_instance_prefix() {
        assert_eq!(
            bytestream_uri("localhost:1985", "", "abc", 3),
            "bytestream://localhost:1985/blobs/abc/3"
        );
        assert_eq!(
            bytestream_uri("localhost:1985", "remote/instance", "abc", 3),
            "bytestream://localhost:1985/remote/instance/blobs/abc/3"
        );
    }

    fn test_artifact_upload_config() -> BazelArtifactUploadConfig {
        BazelArtifactUploadConfig {
            endpoint: "test://bytestream".to_owned(),
            headers: Vec::new(),
            tls: BesTls::default(),
            credential_helper: None,
            instance_name: "remote/instance".to_owned(),
            uri_authority: "localhost:1985".to_owned(),
            max_bytes: 1024,
            grpc_timeout: Duration::from_secs(1),
        }
    }

    #[tokio::test]
    async fn upload_event_files_adds_named_set_uris_from_digest() {
        let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let mut uploader = BazelArtifactUploader::new(test_artifact_upload_config());
        let mut event = bazel_bep_proto::build_event_stream::BuildEvent {
            id: None,
            children: Vec::new(),
            payload: Some(
                bazel_bep_proto::build_event_stream::build_event::Payload::NamedSetOfFiles(
                    bazel_bep_proto::build_event_stream::NamedSetOfFiles {
                        files: vec![bazel_bep_proto::build_event_stream::File {
                            name: "buck-out/gen/root/main".to_owned(),
                            path_prefix: Vec::new(),
                            file: None,
                            digest: format!("{hash}:3"),
                            length: 3,
                        }],
                        file_sets: Vec::new(),
                    },
                ),
            ),
            last_message: false,
        };

        uploader.upload_event_files(&mut event).await;

        let Some(bazel_bep_proto::build_event_stream::build_event::Payload::NamedSetOfFiles(files)) =
            event.payload
        else {
            panic!("expected named set");
        };
        let uri = match files.files[0].file.as_ref() {
            Some(bazel_bep_proto::build_event_stream::file::File::Uri(uri)) => uri,
            other => panic!("expected URI, got {other:?}"),
        };
        assert_eq!(
            uri,
            &format!("bytestream://localhost:1985/remote/instance/blobs/{hash}/3")
        );
    }

    #[tokio::test]
    async fn upload_event_files_leaves_directory_named_set_files_without_uri() {
        let directory = bazel_bep_proto::build_event_stream::File {
            name: "buck-out/gen/root/tree".to_owned(),
            path_prefix: Vec::new(),
            file: None,
            digest: "tree-digest:42".to_owned(),
            length: 42,
        };
        let mut uploader = BazelArtifactUploader::new(test_artifact_upload_config());
        uploader.observe_bazel_events(&[bazel_bep_proto::build_event_stream::BuildEvent {
            id: None,
            children: Vec::new(),
            payload: Some(
                bazel_bep_proto::build_event_stream::build_event::Payload::Completed(
                    bazel_bep_proto::build_event_stream::TargetComplete {
                        directory_output: vec![directory.clone()],
                        ..Default::default()
                    },
                ),
            ),
            last_message: false,
        }]);
        let mut event = bazel_bep_proto::build_event_stream::BuildEvent {
            id: None,
            children: Vec::new(),
            payload: Some(
                bazel_bep_proto::build_event_stream::build_event::Payload::NamedSetOfFiles(
                    bazel_bep_proto::build_event_stream::NamedSetOfFiles {
                        files: vec![directory],
                        file_sets: Vec::new(),
                    },
                ),
            ),
            last_message: false,
        };

        uploader.upload_event_files(&mut event).await;

        assert!(uploader.test_writes.is_empty());
        let Some(bazel_bep_proto::build_event_stream::build_event::Payload::NamedSetOfFiles(files)) =
            event.payload
        else {
            panic!("expected named set");
        };
        assert!(files.files[0].file.is_none());
        assert_eq!(files.files[0].digest, "tree-digest:42");
        assert_eq!(files.files[0].length, 42);
    }

    #[tokio::test]
    async fn upload_event_files_uploads_named_set_local_files() {
        let contents = b"abc";
        let mut hasher = Sha256::new();
        hasher.update(contents);
        let hash = format!("{:x}", hasher.finalize());
        let repo_path =
            std::env::temp_dir().join(format!("buck2-bes-client-test-{}", uuid::Uuid::new_v4()));
        let output_path = repo_path.join("buck-out/gen/root/main");
        std::fs::create_dir_all(output_path.parent().unwrap()).unwrap();
        std::fs::write(&output_path, contents).unwrap();
        let mut uploader = BazelArtifactUploader::new(test_artifact_upload_config());
        let mut metadata = HashMap::new();
        metadata.insert(
            "REPO_ROOT".to_owned(),
            repo_path.to_string_lossy().into_owned(),
        );
        uploader.observe_buck_event(&buck2_data::BuckEvent {
            timestamp: None,
            trace_id: String::new(),
            span_id: 0,
            parent_id: 0,
            data: Some(buck2_data::buck_event::Data::SpanStart(
                buck2_data::SpanStartEvent {
                    data: Some(
                        buck2_data::CommandStart {
                            metadata,
                            ..Default::default()
                        }
                        .into(),
                    ),
                },
            )),
        });
        let mut event = bazel_bep_proto::build_event_stream::BuildEvent {
            id: None,
            children: Vec::new(),
            payload: Some(
                bazel_bep_proto::build_event_stream::build_event::Payload::NamedSetOfFiles(
                    bazel_bep_proto::build_event_stream::NamedSetOfFiles {
                        files: vec![bazel_bep_proto::build_event_stream::File {
                            name: "buck-out/gen/root/main".to_owned(),
                            path_prefix: Vec::new(),
                            file: None,
                            digest: "buck-digest:3".to_owned(),
                            length: 3,
                        }],
                        file_sets: Vec::new(),
                    },
                ),
            ),
            last_message: false,
        };

        uploader.upload_event_files(&mut event).await;

        assert_eq!(uploader.test_writes.len(), 1);
        assert_eq!(uploader.test_writes[0].data, contents);
        assert!(
            uploader.test_writes[0]
                .resource_name
                .contains(&format!("blobs/{hash}/3"))
        );
        let Some(bazel_bep_proto::build_event_stream::build_event::Payload::NamedSetOfFiles(files)) =
            event.payload
        else {
            panic!("expected named set");
        };
        assert_eq!(files.files[0].digest, format!("{hash}:3"));
        assert_eq!(files.files[0].length, 3);
        let uri = match files.files[0].file.as_ref() {
            Some(bazel_bep_proto::build_event_stream::file::File::Uri(uri)) => uri,
            other => panic!("expected URI, got {other:?}"),
        };
        assert_eq!(
            uri,
            &format!("bytestream://localhost:1985/remote/instance/blobs/{hash}/3")
        );
        std::fs::remove_dir_all(repo_path).ok();
    }

    #[tokio::test]
    async fn upload_event_files_streams_oversized_named_set_local_files() {
        let contents = b"abcdef";
        let mut hasher = Sha256::new();
        hasher.update(contents);
        let hash = format!("{:x}", hasher.finalize());
        let repo_path =
            std::env::temp_dir().join(format!("buck2-bes-client-test-{}", uuid::Uuid::new_v4()));
        let output_path = repo_path.join("buck-out/gen/root/main");
        std::fs::create_dir_all(output_path.parent().unwrap()).unwrap();
        std::fs::write(&output_path, contents).unwrap();
        let mut config = test_artifact_upload_config();
        config.max_bytes = 4;
        let mut uploader = BazelArtifactUploader::new(config);
        uploader.repo_path = Some(repo_path.clone());
        let mut event = bazel_bep_proto::build_event_stream::BuildEvent {
            id: None,
            children: Vec::new(),
            payload: Some(
                bazel_bep_proto::build_event_stream::build_event::Payload::NamedSetOfFiles(
                    bazel_bep_proto::build_event_stream::NamedSetOfFiles {
                        files: vec![bazel_bep_proto::build_event_stream::File {
                            name: "buck-out/gen/root/main".to_owned(),
                            path_prefix: Vec::new(),
                            file: None,
                            digest: "buck-digest:6".to_owned(),
                            length: 6,
                        }],
                        file_sets: Vec::new(),
                    },
                ),
            ),
            last_message: false,
        };

        uploader.upload_event_files(&mut event).await;

        assert_eq!(uploader.test_writes.len(), 2);
        assert_eq!(uploader.test_writes[0].data, b"abcd");
        assert_eq!(uploader.test_writes[0].write_offset, 0);
        assert!(!uploader.test_writes[0].finish_write);
        assert!(
            uploader.test_writes[0]
                .resource_name
                .contains(&format!("blobs/{hash}/6"))
        );
        assert_eq!(uploader.test_writes[1].data, b"ef");
        assert_eq!(uploader.test_writes[1].write_offset, 4);
        assert!(uploader.test_writes[1].finish_write);
        assert!(uploader.test_writes[1].resource_name.is_empty());
        let Some(bazel_bep_proto::build_event_stream::build_event::Payload::NamedSetOfFiles(files)) =
            event.payload
        else {
            panic!("expected named set");
        };
        assert_eq!(files.files[0].digest, format!("{hash}:6"));
        assert_eq!(files.files[0].length, 6);
        let uri = match files.files[0].file.as_ref() {
            Some(bazel_bep_proto::build_event_stream::file::File::Uri(uri)) => uri,
            other => panic!("expected URI, got {other:?}"),
        };
        assert_eq!(
            uri,
            &format!("bytestream://localhost:1985/remote/instance/blobs/{hash}/6")
        );
        std::fs::remove_dir_all(repo_path).ok();
    }

    #[tokio::test]
    async fn upload_event_files_rejects_named_set_path_traversal() {
        let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let repo_path =
            std::env::temp_dir().join(format!("buck2-bes-client-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&repo_path).unwrap();
        let mut uploader = BazelArtifactUploader::new(test_artifact_upload_config());
        uploader.repo_path = Some(repo_path.clone());
        let mut event = bazel_bep_proto::build_event_stream::BuildEvent {
            id: None,
            children: Vec::new(),
            payload: Some(
                bazel_bep_proto::build_event_stream::build_event::Payload::NamedSetOfFiles(
                    bazel_bep_proto::build_event_stream::NamedSetOfFiles {
                        files: vec![bazel_bep_proto::build_event_stream::File {
                            name: "../secret".to_owned(),
                            path_prefix: Vec::new(),
                            file: None,
                            digest: format!("{hash}:3"),
                            length: 3,
                        }],
                        file_sets: Vec::new(),
                    },
                ),
            ),
            last_message: false,
        };

        uploader.upload_event_files(&mut event).await;

        assert!(uploader.test_writes.is_empty());
        let Some(bazel_bep_proto::build_event_stream::build_event::Payload::NamedSetOfFiles(files)) =
            event.payload
        else {
            panic!("expected named set");
        };
        let uri = match files.files[0].file.as_ref() {
            Some(bazel_bep_proto::build_event_stream::file::File::Uri(uri)) => uri,
            other => panic!("expected URI, got {other:?}"),
        };
        assert_eq!(
            uri,
            &format!("bytestream://localhost:1985/remote/instance/blobs/{hash}/3")
        );
        std::fs::remove_dir_all(repo_path).ok();
    }

    #[test]
    fn event_format_defaults_to_buck() {
        assert_eq!(BesConfig::default().event_format, BesEventFormat::Buck);
    }

    #[test]
    fn event_format_parses_supported_values() {
        assert_eq!(
            "buck".parse::<BesEventFormat>().unwrap(),
            BesEventFormat::Buck
        );
        assert_eq!(
            "bazel".parse::<BesEventFormat>().unwrap(),
            BesEventFormat::Bazel
        );
        assert_eq!(
            " bazel ".parse::<BesEventFormat>().unwrap(),
            BesEventFormat::Bazel
        );
    }

    #[test]
    fn event_format_rejects_unknown_values() {
        let err = "Bazel".parse::<BesEventFormat>().unwrap_err();
        assert!(err.to_string().contains("expected `buck` or `bazel`"));
    }

    #[test]
    fn bes_worker_runtime_drives_spawned_transport_tasks() {
        let runtime = bes_worker_runtime().expect("runtime should build");

        assert_eq!(
            runtime.handle().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        );
    }

    #[tokio::test]
    async fn bazel_enqueue_returns_highest_emitted_sequence_number() {
        let message = make_message(
            Some(&TraceId::new().to_string()),
            Some(1),
            command_start_data(),
        );
        let parsed = ParsedMessage::from_message(&message).expect("valid message");
        let mut stream = StreamState::new(&parsed, &[], None, true);

        let last_sequence = stream.enqueue_event(&parsed, BesEventFormat::Bazel).await;

        assert_eq!(last_sequence, Some(stream.pending_unacked.len() as i64));
        assert!(stream.pending_unacked.len() > 1);
        assert_eq!(request_sequence_number(&stream.pending_unacked[0]), 1);
        assert_eq!(request_sequence_number(&stream.pending_unacked[1]), 2);
        let event = stream.pending_unacked[0]
            .ordered_build_event
            .as_ref()
            .and_then(|ordered| ordered.event.as_ref())
            .and_then(|event| event.event.as_ref());
        assert!(matches!(event, Some(build_event::Event::BazelEvent(_))));
    }

    #[tokio::test]
    async fn standalone_non_record_events_are_not_silently_dropped() {
        let message = make_message(
            Some(&TraceId::new().to_string()),
            Some(1),
            command_start_data(),
        );
        let config = BesConfig {
            retry_attempts: 0,
            grpc_timeout: Duration::from_millis(20),
            ..BesConfig::default()
        };
        let connection = ConnectionConfig {
            endpoint: "http://127.0.0.1:1".to_owned(),
            headers: Vec::new(),
            tls: BesTls::default(),
            credential_helper: None,
        };
        let counters = Arc::new(CounterState::default());
        let mut worker = WorkerState::new(config, connection, counters);

        // Before this change, this returned Ok(()) because standalone non-record
        // events were dropped when there was no active command context.
        assert!(
            worker
                .send_message_with_retry(&message, true)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn immediate_close_failures_still_retry() {
        let trace_id = TraceId::new().to_string();
        let message = make_message(Some(&trace_id), Some(1), invocation_record_data());
        let config = BesConfig {
            retry_attempts: 1,
            retry_backoff: Duration::ZERO,
            grpc_timeout: Duration::from_millis(20),
            ..BesConfig::default()
        };
        let connection = ConnectionConfig {
            endpoint: "http://127.0.0.1:1".to_owned(),
            headers: Vec::new(),
            tls: BesTls::default(),
            credential_helper: None,
        };
        let counters = Arc::new(CounterState::default());
        let mut worker = WorkerState::new(config, connection, counters.clone());

        let parsed = ParsedMessage::from_message(&message).expect("valid message");
        worker.ensure_stream_exists(&parsed).unwrap();
        {
            let (sender, receiver) = mpsc::channel(1);
            drop(receiver);
            let ack_task =
                tokio::spawn(async { std::future::pending::<Result<(), Status>>().await });
            let stream = worker
                .streams
                .get_mut(&parsed.invocation_id)
                .expect("stream inserted");
            stream.attach_transport(sender, ack_task);
            // Make the outer flush a no-op so failure originates from the immediate-close path.
            stream.last_sent_sequence_number = 1;
        }

        assert!(
            worker
                .send_message_with_retry(&message, true)
                .await
                .is_err()
        );
        assert_eq!(counters.snapshot().failures_pushed_back, 2);
    }

    #[tokio::test]
    async fn close_stream_keeps_pending_close_when_close_flush_fails() {
        let trace_id = TraceId::new().to_string();
        let message = make_message(Some(&trace_id), Some(1), command_start_data());
        let config = BesConfig {
            grpc_timeout: Duration::from_millis(20),
            ..BesConfig::default()
        };
        let connection = ConnectionConfig {
            endpoint: "http://127.0.0.1:1".to_owned(),
            headers: Vec::new(),
            tls: BesTls::default(),
            credential_helper: None,
        };
        let counters = Arc::new(CounterState::default());
        let mut worker = WorkerState::new(config, connection, counters);

        let parsed = ParsedMessage::from_message(&message).expect("valid message");
        worker.ensure_stream_exists(&parsed).unwrap();
        let stream = worker
            .streams
            .get_mut(&parsed.invocation_id)
            .expect("stream inserted");
        stream.pending_close = Some(PendingClose {
            close_after: Instant::now(),
            event_time: None,
        });

        assert!(
            worker
                .close_stream(&parsed.invocation_id, None)
                .await
                .is_err()
        );
        assert!(
            worker
                .streams
                .get(&parsed.invocation_id)
                .and_then(|stream| stream.pending_close.as_ref())
                .is_some()
        );
    }

    struct BesThatStopsAcking {
        acks: i64,
        failed: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    }

    #[tonic::async_trait]
    impl PublishBuildEvent for BesThatStopsAcking {
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
            let mut inbound = request.into_inner();
            let (tx, rx) = mpsc::channel(16);
            let acks = self.acks;
            let failed = self.failed.lock().unwrap().take();
            tokio::spawn(async move {
                while let Ok(Some(request)) = inbound.message().await {
                    let sequence_number = request_sequence_number(&request);
                    if sequence_number > acks {
                        break;
                    }
                    let response = PublishBuildToolEventStreamResponse {
                        stream_id: request
                            .ordered_build_event
                            .and_then(|ordered| ordered.stream_id),
                        sequence_number,
                    };
                    if tx.send(Ok(response)).await.is_err() {
                        break;
                    }
                }
                if let Some(failed) = failed {
                    failed.send(()).ok();
                }
            });
            Ok(tonic::Response::new(ReceiverStream::new(rx)))
        }
    }

    async fn serve_bes_that_stops_acking(acks: i64) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let (failed_tx, failed_rx) = oneshot::channel();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(PublishBuildEventServer::new(BesThatStopsAcking {
                    acks,
                    failed: std::sync::Mutex::new(Some(failed_tx)),
                }))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        // Aborting drops the listener at once, so reconnects are refused rather than left in
        // the accept backlog; a graceful shutdown keeps listening while connections drain.
        tokio::spawn(async move {
            drop(failed_rx.await);
            server.abort();
        });
        endpoint
    }

    /// Acknowledges nothing until the client half-closes the stream, then every sequence number
    /// it carried, provided they run from 1 without a gap; BuildBuddy's `postProcessStream` does
    /// the same. Records each stream's sequence numbers. The first stream fails once it carries
    /// `fail_first_stream_at`.
    struct BesThatAcksAtEof {
        fail_first_stream_at: Option<i64>,
        streams: Arc<std::sync::Mutex<Vec<Vec<i64>>>>,
        /// The Bazel build events the streams carried, decoded, in arrival order.
        bazel_events: Arc<std::sync::Mutex<Vec<bazel_bep_proto::build_event_stream::BuildEvent>>>,
    }

    #[tonic::async_trait]
    impl PublishBuildEvent for BesThatAcksAtEof {
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
            let mut inbound = request.into_inner();
            let (tx, rx) = mpsc::channel(16);
            let index = {
                let mut streams = self.streams.lock().unwrap();
                streams.push(Vec::new());
                streams.len() - 1
            };
            let fail_at = self.fail_first_stream_at.filter(|_| index == 0);
            let streams = self.streams.clone();
            let bazel_events = self.bazel_events.clone();
            tokio::spawn(async move {
                let mut stream_id = None;
                loop {
                    match inbound.message().await {
                        Ok(Some(request)) => {
                            let sequence_number = request_sequence_number(&request);
                            if let Some(build_event::Event::BazelEvent(any)) = request
                                .ordered_build_event
                                .as_ref()
                                .and_then(|ordered| ordered.event.as_ref())
                                .and_then(|event| event.event.as_ref())
                                && let Ok(event) =
                                    bazel_bep_proto::build_event_stream::BuildEvent::decode(
                                        any.value.as_slice(),
                                    )
                            {
                                bazel_events.lock().unwrap().push(event);
                            }
                            stream_id = request
                                .ordered_build_event
                                .and_then(|ordered| ordered.stream_id);
                            streams.lock().unwrap()[index].push(sequence_number);
                            if fail_at == Some(sequence_number) {
                                drop(tx.send(Err(Status::unavailable("injected failure"))).await);
                                return;
                            }
                        }
                        Ok(None) => break,
                        Err(_) => return,
                    }
                }
                let received = streams.lock().unwrap()[index].clone();
                if received.iter().copied().ne(1..=received.len() as i64) {
                    drop(
                        tx.send(Err(Status::unknown("event sequence number mismatch")))
                            .await,
                    );
                    return;
                }
                for sequence_number in received {
                    let response = PublishBuildToolEventStreamResponse {
                        stream_id: stream_id.clone(),
                        sequence_number,
                    };
                    if tx.send(Ok(response)).await.is_err() {
                        return;
                    }
                }
            });
            Ok(tonic::Response::new(ReceiverStream::new(rx)))
        }
    }

    async fn serve_bes_that_acks_at_eof(
        fail_first_stream_at: Option<i64>,
    ) -> (String, Arc<std::sync::Mutex<Vec<Vec<i64>>>>) {
        let (endpoint, streams, _bazel_events) =
            serve_bes_that_acks_at_eof_keeping_bazel_events(fail_first_stream_at).await;
        (endpoint, streams)
    }

    async fn serve_bes_that_acks_at_eof_keeping_bazel_events(
        fail_first_stream_at: Option<i64>,
    ) -> (
        String,
        Arc<std::sync::Mutex<Vec<Vec<i64>>>>,
        Arc<std::sync::Mutex<Vec<bazel_bep_proto::build_event_stream::BuildEvent>>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let streams = Arc::new(std::sync::Mutex::new(Vec::new()));
        let bazel_events = Arc::new(std::sync::Mutex::new(Vec::new()));
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(PublishBuildEventServer::new(BesThatAcksAtEof {
                    fail_first_stream_at,
                    streams: streams.clone(),
                    bazel_events: bazel_events.clone(),
                }))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        (endpoint, streams, bazel_events)
    }

    /// Acknowledges each event up to sequence number `acks` as it arrives, then keeps the stream
    /// open and acknowledges nothing more.
    struct BesThatWithholdsAcks {
        acks: i64,
    }

    #[tonic::async_trait]
    impl PublishBuildEvent for BesThatWithholdsAcks {
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
            let mut inbound = request.into_inner();
            let (tx, rx) = mpsc::channel(16);
            let acks = self.acks;
            tokio::spawn(async move {
                while let Ok(Some(request)) = inbound.message().await {
                    let sequence_number = request_sequence_number(&request);
                    if sequence_number > acks {
                        continue;
                    }
                    let response = PublishBuildToolEventStreamResponse {
                        stream_id: request
                            .ordered_build_event
                            .and_then(|ordered| ordered.stream_id),
                        sequence_number,
                    };
                    if tx.send(Ok(response)).await.is_err() {
                        break;
                    }
                }
            });
            Ok(tonic::Response::new(ReceiverStream::new(rx)))
        }
    }

    async fn serve_bes_that_withholds_acks(acks: i64) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(PublishBuildEventServer::new(BesThatWithholdsAcks { acks }))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        endpoint
    }

    fn action_start_data() -> buck2_data::buck_event::Data {
        buck2_data::buck_event::Data::SpanStart(buck2_data::SpanStartEvent {
            data: Some(buck2_data::span_start_event::Data::ActionExecution(
                buck2_data::ActionExecutionStart::default(),
            )),
        })
    }

    fn command_end_data() -> buck2_data::buck_event::Data {
        buck2_data::buck_event::Data::SpanEnd(buck2_data::SpanEndEvent {
            data: Some(buck2_data::CommandEnd::default().into()),
            ..Default::default()
        })
    }

    /// A worker that sends Bazel build events, as lab's `[bes] event_format = bazel` does,
    /// without the artifact upload, which needs a CAS.
    fn bazel_worker_for(endpoint: String) -> WorkerState {
        let config = BesConfig {
            buffer_size: 3,
            retry_backoff: Duration::from_millis(20),
            retry_window: Duration::from_secs(5),
            grpc_timeout: Duration::from_secs(2),
            event_format: BesEventFormat::Bazel,
            bazel_artifact_upload: false,
            ..BesConfig::default()
        };
        let connection = ConnectionConfig {
            endpoint,
            headers: Vec::new(),
            tls: BesTls::default(),
            credential_helper: None,
        };
        WorkerState::new(config, connection, Arc::new(CounterState::default()))
    }

    fn worker_for(endpoint: String) -> (WorkerState, Arc<CounterState>) {
        let config = BesConfig {
            buffer_size: 3,
            retry_backoff: Duration::from_millis(20),
            retry_window: Duration::from_secs(5),
            grpc_timeout: Duration::from_secs(2),
            ..BesConfig::default()
        };
        let connection = ConnectionConfig {
            endpoint,
            headers: Vec::new(),
            tls: BesTls::default(),
            credential_helper: None,
        };
        let counters = Arc::new(CounterState::default());
        (
            WorkerState::new(config, connection, counters.clone()),
            counters,
        )
    }

    async fn send_queued_ok(
        worker: &mut WorkerState,
        trace_id: &str,
        data: buck2_data::buck_event::Data,
    ) {
        let message = make_message(Some(trace_id), Some(1), data);
        assert!(
            worker
                .send_message_with_retry(&message, false)
                .await
                .is_ok(),
            "queued sends never fail the command"
        );
        // The server and the ack task share this test's thread.
        tokio::task::yield_now().await;
    }

    fn failures(counters: &CounterState) -> u64 {
        let c = counters.snapshot();
        c.failures_invalid_request
            + c.failures_unauthorized
            + c.failures_rate_limited
            + c.failures_pushed_back
            + c.failures_enqueue_failed
            + c.failures_internal_error
            + c.failures_timed_out
            + c.failures_unknown
    }

    #[tokio::test]
    async fn stream_acked_only_at_eof_delivers_every_event_past_the_bound() {
        let (endpoint, streams) = serve_bes_that_acks_at_eof(None).await;
        let (mut worker, counters) = worker_for(endpoint);
        let bound = worker.max_unacked();
        let trace_id = TraceId::new().to_string();

        send_queued_ok(&mut worker, &trace_id, command_start_data()).await;
        let invocation_id = worker.streams.keys().next().expect("stream opened").clone();
        let actions = 3 * bound;
        for _ in 0..actions {
            send_queued_ok(&mut worker, &trace_id, action_start_data()).await;
            let stream = &worker.streams[&invocation_id];
            assert!(
                !stream.abandoned,
                "a stream that is not acked yet was abandoned"
            );
            assert_eq!(stream.last_acked_sequence_number(), 0);
            let pending = stream.pending_unacked.len();
            assert!(pending <= bound, "{pending} events held, bound is {bound}");
        }
        assert_eq!(
            worker.streams[&invocation_id].replay_copies_dropped,
            (1 + actions - bound) as u64
        );
        send_queued_ok(&mut worker, &trace_id, command_end_data()).await;
        send_queued_ok(&mut worker, &trace_id, invocation_record_data()).await;

        assert!(
            !worker.streams.contains_key(&invocation_id),
            "the record closes the stream once every event is acknowledged"
        );
        // Every message, the stream's finish event, and nothing twice.
        let events = (1 + actions + 2 + 1) as i64;
        assert_eq!(
            *streams.lock().unwrap(),
            vec![(1..=events).collect::<Vec<_>>()]
        );
        assert_eq!(counters.snapshot().successes, (events - 1) as u64);
        assert_eq!(counters.snapshot().dropped, 0);
        assert_eq!(failures(&counters), 0);
    }

    /// Refuses every stream as a server refuses a client without a valid API key, and counts
    /// the streams opened.
    struct BesThatRefusesCredentials {
        opens: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[tonic::async_trait]
    impl PublishBuildEvent for BesThatRefusesCredentials {
        async fn publish_lifecycle_event(
            &self,
            _request: tonic::Request<PublishLifecycleEventRequest>,
        ) -> Result<tonic::Response<()>, Status> {
            Err(Status::unauthenticated("anonymous access disabled"))
        }

        type PublishBuildToolEventStreamStream =
            ReceiverStream<Result<PublishBuildToolEventStreamResponse, Status>>;

        async fn publish_build_tool_event_stream(
            &self,
            _request: tonic::Request<tonic::Streaming<PublishBuildToolEventStreamRequest>>,
        ) -> Result<tonic::Response<Self::PublishBuildToolEventStreamStream>, Status> {
            self.opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(Status::unauthenticated("anonymous access disabled"))
        }
    }

    #[tokio::test]
    async fn a_refused_stream_is_not_redialled_and_no_later_stream_opens() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(PublishBuildEventServer::new(BesThatRefusesCredentials {
                    opens: opens.clone(),
                }))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let (mut worker, counters) = worker_for(endpoint);

        let first = TraceId::new().to_string();
        send_queued_ok(&mut worker, &first, command_start_data()).await;
        for _ in 0..10 {
            // Let the server's refusal reach the stream's acknowledgement task.
            tokio::time::sleep(Duration::from_millis(20)).await;
            send_queued_ok(&mut worker, &first, action_start_data()).await;
        }
        let second = TraceId::new().to_string();
        send_queued_ok(&mut worker, &second, command_start_data()).await;
        send_queued_ok(&mut worker, &second, action_start_data()).await;

        assert_eq!(
            opens.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a refusal must not be answered by redialling, nor a later invocation open a stream"
        );
        let snapshot = counters.snapshot();
        assert!(snapshot.failures_unauthorized >= 1, "{snapshot:?}");
        assert!(
            snapshot.dropped >= 2,
            "later events are dropped: {snapshot:?}"
        );
        // A priority send returns at once rather than waiting on a refused stream.
        let message = make_message(Some(&second), Some(1), action_start_data());
        let sent = tokio::time::timeout(
            Duration::from_secs(1),
            worker.send_message_with_retry(&message, true),
        )
        .await
        .expect("a priority send must not wait on a refused stream");
        assert_eq!(sent.expect("not an error"), None);
    }

    #[tokio::test]
    async fn daemon_scoped_events_open_no_stream() {
        let nil_trace_id = TraceId::null().to_string();
        for fail_fast in [false, true] {
            let (endpoint, streams) = serve_bes_that_acks_at_eof(None).await;
            let (mut worker, counters) = worker_for(endpoint);
            let trace_id = TraceId::new().to_string();

            let daemon_event = make_message(Some(&nil_trace_id), Some(1), action_start_data());
            let sent = worker
                .send_message_with_retry(&daemon_event, fail_fast)
                .await
                .expect("a daemon-scoped event is not an error");
            assert_eq!(sent, None, "fail_fast={fail_fast}");
            send_queued_ok(&mut worker, &trace_id, command_start_data()).await;

            assert_eq!(
                worker.streams.keys().collect::<Vec<_>>(),
                vec![&trace_id],
                "fail_fast={fail_fast}"
            );
            send_queued_ok(&mut worker, &trace_id, invocation_record_data()).await;

            // The command's start, its record and the stream's finish event, on one stream.
            assert_eq!(
                *streams.lock().unwrap(),
                vec![vec![1, 2, 3]],
                "fail_fast={fail_fast}"
            );
            assert_eq!(counters.snapshot().dropped, 0, "fail_fast={fail_fast}");
            assert_eq!(failures(&counters), 0, "fail_fast={fail_fast}");
        }
    }

    /// A `buck2 test //pkg/...` in Bazel format: the second test target is discovered after the
    /// first target's results set off the PatternExpanded, and its per-test result follows its
    /// run's span end, as buck2's test runner sends them. BuildBuddy's target tracker reads only
    /// announced targets (target_tracker.go at v2.310.0), so the server must see the late target
    /// announced before its TargetConfigured, and its TestSummary.
    #[tokio::test]
    async fn a_late_test_target_reaches_the_server_announced_with_its_summary() {
        let (endpoint, _streams, bazel_events) =
            serve_bes_that_acks_at_eof_keeping_bazel_events(None).await;
        let mut worker = bazel_worker_for(endpoint);
        let trace_id = TraceId::new().to_string();
        let target = |name: &str| buck2_data::ConfiguredTargetLabel {
            label: Some(buck2_data::TargetLabel {
                package: "root//pkg".to_owned(),
                name: name.to_owned(),
            }),
            configuration: Some(buck2_data::Configuration {
                full_name: "cfg".to_owned(),
            }),
            execution_configuration: None,
        };
        let discovery = |name: &str| {
            buck2_data::buck_event::Data::SpanStart(buck2_data::SpanStartEvent {
                data: Some(buck2_data::span_start_event::Data::TestDiscovery(
                    buck2_data::TestDiscoveryStart {
                        suite_name: name.to_owned(),
                        target_label: Some(target(name)),
                        labels: Vec::new(),
                    },
                )),
            })
        };
        let run_end = |name: &str| {
            buck2_data::buck_event::Data::SpanEnd(buck2_data::SpanEndEvent {
                data: Some(buck2_data::span_end_event::Data::TestRun(
                    buck2_data::TestRunEnd {
                        suite: Some(buck2_data::TestSuite {
                            suite_name: name.to_owned(),
                            test_names: Vec::new(),
                            target_label: Some(target(name)),
                            labels: Vec::new(),
                        }),
                        command_report: Some(buck2_data::CommandExecution {
                            details: Some(buck2_data::CommandExecutionDetails {
                                signed_exit_code: Some(0),
                                ..Default::default()
                            }),
                            status: Some(buck2_data::command_execution::Status::Success(
                                buck2_data::command_execution::Success {},
                            )),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            })
        };
        let case = |name: &str| {
            buck2_data::buck_event::Data::Instant(buck2_data::InstantEvent {
                data: Some(buck2_data::instant_event::Data::TestResult(
                    buck2_data::TestResult {
                        name: format!("root//pkg:{name}"),
                        status: buck2_data::TestStatus::Pass as i32,
                        target_label: Some(target(name)),
                        ..Default::default()
                    },
                )),
            })
        };

        send_queued_ok(
            &mut worker,
            &trace_id,
            buck2_data::buck_event::Data::SpanStart(buck2_data::SpanStartEvent {
                data: Some(buck2_data::span_start_event::Data::Command(
                    buck2_data::CommandStart {
                        cli_args: vec![
                            "buck2".to_owned(),
                            "test".to_owned(),
                            "//pkg/...".to_owned(),
                        ],
                        data: Some(buck2_data::command_start::Data::Test(
                            buck2_data::TestCommandStart {},
                        )),
                        ..Default::default()
                    },
                )),
            }),
        )
        .await;
        for data in [
            buck2_data::buck_event::Data::Instant(buck2_data::InstantEvent {
                data: Some(buck2_data::instant_event::Data::TargetPatterns(
                    buck2_data::ParsedTargetPatterns {
                        target_patterns: vec![buck2_data::TargetPattern {
                            value: "//pkg/...".to_owned(),
                        }],
                    },
                )),
            }),
            discovery("first"),
            run_end("first"),
            case("first"),
            discovery("second"),
            run_end("second"),
            case("second"),
            buck2_data::buck_event::Data::Instant(buck2_data::InstantEvent {
                data: Some(buck2_data::instant_event::Data::EndOfTestResults(
                    buck2_data::EndOfTestResults::default(),
                )),
            }),
            command_end_data(),
            invocation_record_data(),
        ] {
            send_queued_ok(&mut worker, &trace_id, data).await;
        }
        let waited = tokio::time::timeout(Duration::from_secs(5), async {
            while worker.streams.contains_key(&trace_id) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(waited.is_ok(), "the stream did not close");

        use bazel_bep_proto::build_event_stream::build_event::Payload;
        use bazel_bep_proto::build_event_stream::build_event_id::Id;
        let events = bazel_events.lock().unwrap().clone();
        let configured_second = |id: &bazel_bep_proto::build_event_stream::BuildEventId| matches!(id.id.as_ref(), Some(Id::TargetConfigured(c)) if c.label.ends_with(":second"));
        let announced = events
            .iter()
            .position(|e| {
                matches!(e.payload, Some(Payload::Expanded(_)))
                    && e.children.iter().any(configured_second)
            })
            .expect("a PatternExpanded announcing the second test target");
        let configured = events
            .iter()
            .position(|e| e.id.as_ref().is_some_and(configured_second))
            .expect("the second target's TargetConfigured");
        assert!(announced < configured);
        assert!(
            events.iter().any(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(Id::TestSummary(summary)) if summary.label.ends_with(":second")
            )),
            "no TestSummary for the second test target reached the server"
        );
    }

    #[tokio::test]
    async fn send_messages_now_does_not_wait_for_a_server_that_acks_only_at_eof() {
        let (endpoint, streams) = serve_bes_that_acks_at_eof(None).await;
        let config = BesConfig {
            bes_backend: Some(endpoint.replacen("http://", "grpc://", 1)),
            grpc_timeout: Duration::from_secs(1),
            ..BesConfig::default()
        };
        let ack_timeout = close_ack_timeout(config.grpc_timeout);
        // SAFETY: `BesClient::new` ignores the token, which outside fbcode stands for nothing.
        let client = BesClient::new(unsafe { fbinit::assume_init() }, config).expect("client");
        let trace_id = TraceId::new().to_string();
        let messages = vec![
            make_message(Some(&trace_id), Some(1), command_start_data()),
            make_message(Some(&trace_id), Some(1), action_start_data()),
        ];

        let sent =
            tokio::time::timeout(Duration::from_secs(5), client.send_messages_now(messages)).await;
        let Ok(sent) = sent else {
            panic!(
                "send_messages_now waited for acknowledgements the server sends only at EOF \
                 (close_ack_timeout {ack_timeout:?})"
            );
        };
        sent.expect("events sent");
        client.close_all_streams().await.expect("streams closed");

        // Both events and the stream's finish event, each acknowledged at EOF: the server
        // acknowledges only a stream without a gap, and closing fails on a missing one.
        assert_eq!(*streams.lock().unwrap(), vec![vec![1, 2, 3]]);
        assert_eq!(failures(&client.counters), 0);
        assert_eq!(client.counters.snapshot().dropped, 0);
    }

    #[tokio::test]
    async fn wait_for_acks_waits_on_a_stream_that_was_acknowledged_before() {
        let endpoint = serve_bes_that_withholds_acks(1).await;
        let (mut worker, _counters) = worker_for(endpoint);
        let trace_id = TraceId::new().to_string();

        send_queued_ok(&mut worker, &trace_id, command_start_data()).await;
        let invocation_id = worker.streams.keys().next().expect("stream opened").clone();
        let acked = tokio::time::timeout(Duration::from_secs(5), async {
            while worker.streams[&invocation_id].last_acked_sequence_number() < 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(acked.is_ok(), "the server did not ack the command start");

        let message = make_message(Some(&trace_id), Some(1), action_start_data());
        let target = worker
            .send_message_with_retry(&message, true)
            .await
            .expect("sent")
            .expect("the event has a sequence number");
        assert_eq!(target, (invocation_id, 2));
        let waited = tokio::time::timeout(
            Duration::from_millis(500),
            worker.wait_for_acks(&HashMap::from([target])),
        )
        .await;
        assert!(
            waited.is_err(),
            "returned {waited:?} for an event an acknowledging server has not acknowledged"
        );
    }

    #[tokio::test]
    async fn stream_that_fails_after_dropping_replay_copies_is_abandoned() {
        let (endpoint, streams) = serve_bes_that_acks_at_eof(Some(60)).await;
        let (mut worker, counters) = worker_for(endpoint);
        let bound = worker.max_unacked();
        assert!(bound < 60, "the stream must overflow before it fails");
        let trace_id = TraceId::new().to_string();

        send_queued_ok(&mut worker, &trace_id, command_start_data()).await;
        let invocation_id = worker.streams.keys().next().expect("stream opened").clone();
        let abandoned = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                send_queued_ok(&mut worker, &trace_id, action_start_data()).await;
                let stream = &worker.streams[&invocation_id];
                let pending = stream.pending_unacked.len();
                assert!(pending <= bound, "{pending} events held, bound is {bound}");
                if stream.abandoned {
                    return;
                }
                if stream.next_sequence_number > 60 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        })
        .await;
        assert!(abandoned.is_ok(), "stream not abandoned after it failed");

        let stream = &worker.streams[&invocation_id];
        assert!(stream.replay_copies_dropped > 0);
        assert!(stream.pending_unacked.is_empty());
        assert!(stream.sender.is_none() && stream.ack_task.is_none());
        assert_eq!(
            streams.lock().unwrap().len(),
            1,
            "a stream that dropped replay copies must not be replayed"
        );
        let dropped_before = counters.snapshot().dropped;
        send_queued_ok(&mut worker, &trace_id, action_start_data()).await;
        assert_eq!(counters.snapshot().dropped - dropped_before, 1);
    }

    #[tokio::test]
    async fn stream_that_fails_within_the_bound_replays_from_the_first_unacknowledged_event() {
        let (endpoint, streams) = serve_bes_that_acks_at_eof(Some(5)).await;
        let (mut worker, counters) = worker_for(endpoint);
        let trace_id = TraceId::new().to_string();

        send_queued_ok(&mut worker, &trace_id, command_start_data()).await;
        let invocation_id = worker.streams.keys().next().expect("stream opened").clone();
        let actions = 10;
        for _ in 0..actions {
            send_queued_ok(&mut worker, &trace_id, action_start_data()).await;
        }
        let failed = tokio::time::timeout(Duration::from_secs(5), async {
            while streams.lock().unwrap()[0].last() != Some(&5) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(failed.is_ok(), "the server did not fail the first stream");
        // Past the backoff of the stream's first failure, if the sink saw it yet.
        tokio::time::sleep(Duration::from_millis(50)).await;
        send_queued_ok(&mut worker, &trace_id, command_end_data()).await;
        send_queued_ok(&mut worker, &trace_id, invocation_record_data()).await;
        // A send that meets the failure backs off without closing; the worker's poll loop
        // closes the stream after it.
        let closed = tokio::time::timeout(Duration::from_secs(5), async {
            while worker.streams.contains_key(&invocation_id) {
                worker.close_due_streams().await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            closed.is_ok(),
            "the replayed stream closes once every event is acknowledged"
        );
        let events = (1 + actions + 2 + 1) as i64;
        assert_eq!(
            *streams.lock().unwrap(),
            vec![
                (1..=5).collect::<Vec<_>>(),
                (1..=events).collect::<Vec<_>>()
            ]
        );
        assert_eq!(counters.snapshot().dropped, 0);
    }

    #[tokio::test]
    async fn stream_whose_server_ends_it_at_the_bound_replays_before_dropping_copies() {
        let (endpoint, streams) = serve_bes_that_acks_at_eof(Some(30)).await;
        let (mut worker, counters) = worker_for(endpoint);
        let bound = worker.max_unacked();
        assert_eq!(bound, 30, "the server ends the first stream at the bound");
        let trace_id = TraceId::new().to_string();

        send_queued_ok(&mut worker, &trace_id, command_start_data()).await;
        let invocation_id = worker.streams.keys().next().expect("stream opened").clone();
        let actions = 2 * bound;
        for _ in 1..bound {
            send_queued_ok(&mut worker, &trace_id, action_start_data()).await;
        }
        let ended = tokio::time::timeout(Duration::from_secs(5), async {
            while !worker.streams[&invocation_id].transport_needs_reopen() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(ended.is_ok(), "the server did not end the first stream");
        assert!(worker.streams[&invocation_id].failing.is_none());
        for _ in bound..=actions {
            send_queued_ok(&mut worker, &trace_id, action_start_data()).await;
            assert!(!worker.streams[&invocation_id].abandoned);
        }
        send_queued_ok(&mut worker, &trace_id, command_end_data()).await;
        send_queued_ok(&mut worker, &trace_id, invocation_record_data()).await;

        assert!(
            !worker.streams.contains_key(&invocation_id),
            "the replayed stream closes once every event is acknowledged"
        );
        let events = (1 + actions + 2 + 1) as i64;
        assert_eq!(
            *streams.lock().unwrap(),
            vec![
                (1..=bound as i64).collect::<Vec<_>>(),
                (1..=events).collect::<Vec<_>>()
            ]
        );
        assert_eq!(counters.snapshot().dropped, 0);
    }

    #[tokio::test]
    async fn stream_is_replayable_again_once_acks_pass_its_dropped_copies() {
        let message = make_message(
            Some(&TraceId::new().to_string()),
            Some(1),
            command_start_data(),
        );
        let parsed = ParsedMessage::from_message(&message).expect("valid message");
        let mut stream = StreamState::new(&parsed, &[], None, true);
        for _ in 0..10 {
            stream.enqueue_event(&parsed, BesEventFormat::Buck).await;
        }
        let held = stream.pending_unacked.len() as i64;
        stream.last_sent_sequence_number = held;

        assert!(stream.drop_oldest_replay_copies(6));
        let newest_dropped = held - 6;
        assert_eq!(
            request_sequence_number(&stream.pending_unacked[0]),
            newest_dropped + 1
        );
        assert!(stream.replay_has_gap());
        stream
            .last_acked_sequence_number
            .store(newest_dropped - 1, Ordering::Relaxed);
        assert!(stream.replay_has_gap());
        stream
            .last_acked_sequence_number
            .store(newest_dropped, Ordering::Relaxed);
        assert!(!stream.replay_has_gap());
    }

    #[tokio::test]
    async fn stream_whose_server_goes_away_is_abandoned_within_bounds() {
        let endpoint = serve_bes_that_stops_acking(3).await;
        let config = BesConfig {
            buffer_size: 3,
            retry_backoff: Duration::from_millis(20),
            retry_attempts: 5,
            retry_window: Duration::from_millis(400),
            grpc_timeout: Duration::from_millis(500),
            ..BesConfig::default()
        };
        let connection = ConnectionConfig {
            endpoint,
            headers: Vec::new(),
            tls: BesTls::default(),
            credential_helper: None,
        };
        let counters = Arc::new(CounterState::default());
        let mut worker = WorkerState::new(config.clone(), connection, counters.clone());
        let trace_id = TraceId::new().to_string();
        async fn send_queued(
            worker: &mut WorkerState,
            trace_id: &str,
            data: buck2_data::buck_event::Data,
        ) -> Duration {
            let message = make_message(Some(trace_id), Some(1), data);
            let started = Instant::now();
            let result = worker.send_message_with_retry(&message, false).await;
            assert!(result.is_ok(), "queued sends never fail the command");
            started.elapsed()
        }
        let per_message_retry_cost = (0..=config.retry_attempts)
            .map(|attempt| backoff_for(&config.retry_backoff, attempt))
            .sum::<Duration>();
        let bound = per_message_retry_cost / 4;
        let mut slowest = Duration::ZERO;

        slowest = slowest.max(send_queued(&mut worker, &trace_id, command_start_data()).await);
        let invocation_id = worker.streams.keys().next().expect("stream opened").clone();
        let acked = tokio::time::timeout(Duration::from_secs(5), async {
            while worker.streams[&invocation_id].last_acked_sequence_number() < 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            acked.is_ok(),
            "the server acked the command start before dying"
        );
        let cap = config.buffer_size * UNACKED_EVENTS_PER_QUEUED_EVENT;
        for _ in 0..(2 * cap) {
            slowest = slowest.max(send_queued(&mut worker, &trace_id, action_start_data()).await);
            // The server and the ack task share this test's thread; without a yield the burst
            // would starve them and measure that starvation instead of the dead server.
            tokio::task::yield_now().await;
            let pending = worker.streams[&invocation_id].pending_unacked.len();
            assert!(
                pending <= cap,
                "{pending} unacknowledged events held, cap is {cap}"
            );
        }

        let stream = &worker.streams[&invocation_id];
        assert!(
            stream.abandoned,
            "stream not abandoned after the server went away"
        );
        assert!(stream.pending_unacked.is_empty());
        assert!(stream.sender.is_none() && stream.ack_task.is_none());
        let dropped_before = counters.snapshot().dropped;
        slowest = slowest.max(send_queued(&mut worker, &trace_id, action_start_data()).await);
        slowest = slowest.max(send_queued(&mut worker, &trace_id, command_end_data()).await);
        slowest = slowest.max(send_queued(&mut worker, &trace_id, invocation_record_data()).await);
        assert_eq!(counters.snapshot().dropped - dropped_before, 3);
        assert!(
            !worker.streams.contains_key(&invocation_id),
            "the record closes an abandoned stream without waiting for acks"
        );
        assert!(
            slowest < bound,
            "slowest send took {slowest:?}, bound {bound:?} (per-message retries cost {per_message_retry_cost:?})"
        );
    }

    #[tokio::test]
    async fn stream_failing_for_the_retry_window_is_abandoned() {
        let config = BesConfig {
            retry_backoff: Duration::from_millis(30),
            retry_window: Duration::from_millis(100),
            grpc_timeout: Duration::from_millis(500),
            ..BesConfig::default()
        };
        let connection = ConnectionConfig {
            endpoint: "http://127.0.0.1:1".to_owned(),
            headers: Vec::new(),
            tls: BesTls::default(),
            credential_helper: None,
        };
        let counters = Arc::new(CounterState::default());
        let mut worker = WorkerState::new(config, connection, counters.clone());
        let trace_id = TraceId::new().to_string();
        let message = make_message(Some(&trace_id), Some(1), command_start_data());
        let parsed = ParsedMessage::from_message(&message).expect("valid message");

        assert!(
            worker
                .send_message_with_retry(&message, false)
                .await
                .is_ok()
        );
        let failures_after_first = counters.snapshot().failures_pushed_back;
        assert_eq!(failures_after_first, 1);
        let started = Instant::now();
        let message = make_message(Some(&trace_id), Some(1), action_start_data());
        assert!(
            worker
                .send_message_with_retry(&message, false)
                .await
                .is_ok()
        );
        assert!(
            started.elapsed() < Duration::from_millis(30),
            "a send in backoff must not connect or sleep"
        );
        assert_eq!(
            counters.snapshot().failures_pushed_back,
            failures_after_first
        );
        assert_eq!(
            worker.streams[&parsed.invocation_id].pending_unacked.len(),
            2
        );

        tokio::time::sleep(Duration::from_millis(120)).await;
        let message = make_message(Some(&trace_id), Some(1), action_start_data());
        assert!(
            worker
                .send_message_with_retry(&message, false)
                .await
                .is_ok()
        );

        let stream = &worker.streams[&parsed.invocation_id];
        assert!(
            stream.abandoned,
            "stream not abandoned after failing for the whole window"
        );
        assert!(stream.pending_unacked.is_empty());
        assert_eq!(counters.snapshot().dropped, 3);
        let message = make_message(Some(&trace_id), Some(1), action_start_data());
        assert!(
            worker
                .send_message_with_retry(&message, false)
                .await
                .is_ok()
        );
        assert_eq!(counters.snapshot().dropped, 4);
        assert_eq!(
            counters.snapshot().failures_pushed_back,
            failures_after_first + 1
        );
    }
}
