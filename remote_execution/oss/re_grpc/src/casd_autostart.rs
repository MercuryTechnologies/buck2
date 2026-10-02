/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Reaching, and starting on demand, the machine-local CAS daemon (`buck2-casd`).
//!
//! The daemon only ever listens on this machine: on a Unix socket, by default one inside its own
//! cache directory, or on a loopback TCP port. It is shared by every buck2 daemon on the host, so
//! it is started at most once and never stopped from here. Several buck2 daemons may notice it
//! missing at the same time; a lock file in the cache directory makes one of them start it while
//! the others wait.
//!
//! Sharing one daemon means sharing its upstream. A daemon some other buck2 daemon started may pass
//! CAS traffic to another CAS than this one's (a remote that has since been replaced, or another
//! project's), and every blob this daemon then asks for or uploads goes to the wrong place. So the
//! daemon is asked which CAS it passes through to, when a client is built and again before every
//! connection the client opens to it, and is used only if that is this client's own CAS.

use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use buck2_re_configuration::Buck2OssReConfiguration;
use buck2_re_configuration::CASdAddress;
use buck2_re_configuration::HttpHeader;
use re_grpc_proto::build::bazel::remote::execution::v2::GetCapabilitiesRequest;
use re_grpc_proto::build::bazel::remote::execution::v2::capabilities_client::CapabilitiesClient;
use sha2::Digest;
use sha2::Sha256;
use tonic::metadata::MetadataMap;
use tonic::transport::Endpoint;
use tonic::transport::Uri;
use tower::Service;

use crate::client::substitute_env_vars;
use crate::unix_socket::UnixConnector;

/// How long to wait for a freshly started daemon, or one another process is starting.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const PROBE_INTERVAL: Duration = Duration::from_millis(100);
const LOCK_FILE_NAME: &str = "autostart.lock";
const LOG_FILE_NAME: &str = "buck2-casd.log";
/// The socket the daemon listens on when no address is configured, inside its directory.
pub const DEFAULT_SOCKET_NAME: &str = "buck2-casd.sock";
/// How long a daemon found running has to say which CAS it passes through to.
const UPSTREAM_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Response metadata on the daemon's `GetCapabilities`: the `--upstream` address it passes
/// through to, empty when it is a standalone CAS. A daemon that sends none predates the header.
pub const UPSTREAM_HEADER: &str = "x-buck2-casd-upstream";
/// `true` or `false` as the daemon was given `--upstream-tls`; absent when the scheme decides.
pub const UPSTREAM_TLS_HEADER: &str = "x-buck2-casd-upstream-tls";
/// The daemon's `--upstream-instance-name`; absent when it has none.
pub const UPSTREAM_INSTANCE_NAME_HEADER: &str = "x-buck2-casd-upstream-instance-name";
/// [`upstream_credentials_fingerprint`] of the daemon's upstream headers and client
/// certificate; absent when it has neither.
pub const UPSTREAM_CREDENTIALS_HEADER: &str = "x-buck2-casd-upstream-credentials";
/// The daemon's process id, so that a warning can say which process to stop.
pub const PID_HEADER: &str = "x-buck2-casd-pid";

/// Where the daemon listens.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DaemonAddress {
    Unix(PathBuf),
    Loopback(u16),
}

impl DaemonAddress {
    /// The address for the configured setting, or the default socket in `cache_dir`.
    pub fn resolve(
        configured: Option<&CASdAddress>,
        cache_dir: Option<&Path>,
        substitute_env_vars: impl Fn(&str) -> anyhow::Result<String>,
    ) -> anyhow::Result<Self> {
        match configured {
            Some(CASdAddress::Tcp(port)) => Ok(Self::Loopback(*port)),
            Some(CASdAddress::Uds(path)) => Ok(Self::Unix(PathBuf::from(
                substitute_env_vars(path).context("Invalid `cas_shared_cache_address`")?,
            ))),
            None => {
                let dir = cache_dir.context(
                    "`cas_shared_cache_address` is needed when `cas_shared_cache` is not set",
                )?;
                if !cfg!(unix) {
                    return Err(anyhow::anyhow!(
                        "Set `cas_shared_cache_address` to a port; this platform has no Unix \
                         sockets"
                    ));
                }
                Ok(Self::Unix(dir.join(DEFAULT_SOCKET_NAME)))
            }
        }
    }

    /// The address as the channel pool understands it.
    pub fn pool_address(&self) -> String {
        match self {
            Self::Unix(path) => format!("unix://{}", path.display()),
            Self::Loopback(port) => format!("grpc://127.0.0.1:{port}"),
        }
    }

    /// The daemon's `--listen` argument.
    fn listen_arg(&self) -> String {
        match self {
            Self::Unix(path) => format!("unix://{}", path.display()),
            Self::Loopback(port) => SocketAddr::new(Ipv4Addr::LOCALHOST.into(), *port).to_string(),
        }
    }

    async fn is_listening(&self) -> bool {
        let attempt = async {
            match self {
                #[cfg(unix)]
                Self::Unix(path) => tokio::net::UnixStream::connect(path).await.map(|_| ()),
                #[cfg(not(unix))]
                Self::Unix(_) => Err(std::io::Error::other("no unix sockets")),
                Self::Loopback(port) => tokio::net::TcpStream::connect(SocketAddr::new(
                    Ipv4Addr::LOCALHOST.into(),
                    *port,
                ))
                .await
                .map(|_| ()),
            }
        };
        matches!(
            tokio::time::timeout(Duration::from_secs(1), attempt).await,
            Ok(Ok(()))
        )
    }
}

impl fmt::Display for DaemonAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.pool_address())
    }
}

/// What an auto-started daemon is launched with.
#[derive(Debug)]
pub struct Launch {
    pub binary: PathBuf,
    pub dir: PathBuf,
    pub args: Vec<String>,
    /// Comma-separated `Header: value` pairs, passed through the environment rather than the
    /// command line so that literal secrets do not show up in `ps`.
    pub http_headers_env: Option<String>,
}

/// Makes sure a daemon answers at `address`, starting one if needed.
pub async fn ensure_running(
    opts: &Buck2OssReConfiguration,
    address: &DaemonAddress,
    cache_dir: &Path,
    substitute_env_vars: impl Fn(&str) -> anyhow::Result<String>,
) -> anyhow::Result<()> {
    if address.is_listening().await {
        return Ok(());
    }

    let launch = plan_launch(opts, address, cache_dir, &substitute_env_vars)?;
    std::fs::create_dir_all(&launch.dir)
        .with_context(|| format!("Error creating `{}`", launch.dir.display()))?;

    // Whoever holds the lock starts the daemon; everyone else waits for it to answer.
    let lock_path = launch.dir.join(LOCK_FILE_NAME);
    let lock_file = File::create(&lock_path)
        .with_context(|| format!("Error creating `{}`", lock_path.display()))?;
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    let held = loop {
        match lock_file.try_lock() {
            Ok(()) => break true,
            Err(std::fs::TryLockError::WouldBlock) => {
                if address.is_listening().await {
                    break false;
                }
                if Instant::now() > deadline {
                    return Err(anyhow::anyhow!(
                        "Another process holds `{}` but no buck2-casd came up at {address} \
                         within {STARTUP_TIMEOUT:?}",
                        lock_path.display()
                    ));
                }
                tokio::time::sleep(PROBE_INTERVAL).await;
            }
            Err(std::fs::TryLockError::Error(e)) => {
                return Err(e).with_context(|| format!("Error locking `{}`", lock_path.display()));
            }
        }
    };
    if !held {
        return Ok(());
    }

    // Re-check under the lock: the previous holder may have just started it.
    if address.is_listening().await {
        return Ok(());
    }

    tracing::info!(
        "Starting buck2-casd from `{}` at {address} for `{}`",
        launch.binary.display(),
        launch.dir.display()
    );
    spawn_detached(&launch)?;

    while !address.is_listening().await {
        if Instant::now() > deadline {
            return Err(anyhow::anyhow!(
                "buck2-casd did not start answering at {address} within {STARTUP_TIMEOUT:?}; \
                 see `{}`",
                launch.dir.join(LOG_FILE_NAME).display()
            ));
        }
        tokio::time::sleep(PROBE_INTERVAL).await;
    }
    // `lock_file` is released when dropped here, after the daemon is reachable.
    Ok(())
}

/// Which CAS a configuration reaches, reduced to what decides where its blobs go: the host and
/// port the channel dials, whether it uses TLS, the instance name requests carry, and the
/// credentials they carry.
///
/// The address is read the way the channel reads it (`prepare_uri` in client.rs): the scheme
/// only says whether to use TLS, `tls` overrides it, and a missing port is the default of the
/// TLS that results, 443 with it and 80 without, as tonic infers it. So
/// `grpc://cas.example.com:443` with `tls = true`, `grpcs://cas.example.com` and
/// `cas.example.com:443` are the same CAS. The host is compared without case, as DNS does (RFC
/// 4343). An address that does not parse that way, such as a `unix://` socket, is compared as
/// written. The credentials are compared by fingerprint ([`upstream_credentials_fingerprint`]),
/// because on a multi-tenant remote the API key in a header picks the organisation, and the
/// organisation partitions the CAS.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CasIdentity {
    endpoint: String,
    tls: Option<bool>,
    instance_name: Option<String>,
    credentials: Option<String>,
}

impl CasIdentity {
    /// `address` with any `$VAR` already substituted.
    pub(crate) fn new(
        address: &str,
        tls: Option<bool>,
        instance_name: Option<&str>,
        credentials: Option<String>,
    ) -> Self {
        let parsed = address
            .parse::<Uri>()
            .ok()
            .and_then(|uri| crate::client::prepare_uri(uri, tls).ok())
            .and_then(|(uri, tls)| {
                let host = uri.host().filter(|h| !h.is_empty())?.to_ascii_lowercase();
                let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });
                Some((format!("{host}:{port}"), Some(tls)))
            });
        // A `unix://` remote never uses TLS, whatever `tls` says.
        let (endpoint, tls) = parsed.unwrap_or_else(|| (address.to_owned(), None));
        Self {
            endpoint,
            tls,
            instance_name: instance_name.map(str::to_owned),
            credentials,
        }
    }

    /// The CAS `opts` sends its blobs to, or `None` when it has no remote CAS and the daemon is
    /// the CAS, whatever it passes on to.
    pub(crate) fn of(opts: &Buck2OssReConfiguration) -> anyhow::Result<Option<Self>> {
        let Some(cas_address) = &opts.cas_address else {
            return Ok(None);
        };
        Ok(Some(Self::new(
            &substitute_env_vars(cas_address).context("Invalid `cas_address`")?,
            opts.tls,
            opts.instance_name.as_deref(),
            upstream_credentials_fingerprint(&opts.http_headers, opts.tls_client_cert.as_deref())?,
        )))
    }
}

impl fmt::Display for CasIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`{}`", self.endpoint)?;
        match self.tls {
            Some(true) => f.write_str(" over TLS")?,
            Some(false) => f.write_str(" without TLS")?,
            None => {}
        }
        if let Some(instance) = &self.instance_name {
            write!(f, ", instance `{instance}`")?;
        }
        match &self.credentials {
            Some(credentials) => write!(
                f,
                ", credentials fingerprint `{}`",
                &credentials[..credentials.len().min(12)]
            ),
            None => f.write_str(", no credentials"),
        }
    }
}

/// A fingerprint of the credentials a configuration sends its CAS, or `None` when it sends
/// none: the SHA-256 of its `http_headers`, with `$VAR` substituted from this process's
/// environment, and of the path of its TLS client certificate, made absolute against this
/// process's working directory. Only the digest leaves the process, never a header value.
///
/// buck2-casd computes it from its own configuration in its own environment, which it inherited
/// from the buck2 daemon that started it, so the two agree only when the daemon would send what
/// the client would. The certificate goes in by path, not content, so that renewing it in place
/// does not turn every client away. A CA bundle is left out: it decides which servers to trust,
/// not whose CAS the requests reach.
pub fn upstream_credentials_fingerprint(
    http_headers: &[HttpHeader],
    tls_client_cert: Option<&str>,
) -> anyhow::Result<Option<String>> {
    let mut lines = http_headers
        .iter()
        .map(|h| {
            // Header names are case-insensitive (RFC 9110, section 5.1).
            let key = substitute_env_vars(&h.key)?.to_ascii_lowercase();
            let value = substitute_env_vars(&h.value)?;
            anyhow::Ok(format!("header {key}: {value}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .context("Invalid `http_headers`")?;
    if let Some(cert) = tls_client_cert {
        let cert = substitute_env_vars(cert).context("Invalid `tls_client_cert`")?;
        let cert =
            std::path::absolute(&cert).with_context(|| format!("Error resolving `{cert}`"))?;
        lines.push(format!("tls_client_cert {}", cert.display()));
    }
    if lines.is_empty() {
        return Ok(None);
    }
    lines.sort();
    let mut hasher = Sha256::new();
    for line in &lines {
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
    }
    Ok(Some(format!("{:x}", hasher.finalize())))
}

/// What a running daemon says about its upstream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReportedUpstream {
    Cas(CasIdentity),
    /// It is a standalone CAS and passes nothing on.
    Standalone,
    /// It sends no upstream header: a daemon from before buck2 asked, the kind found serving a
    /// replaced remote, or one whose upstream cannot be written as a header.
    Unreported,
}

impl fmt::Display for ReportedUpstream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cas(cas) => write!(f, "passes CAS traffic to {cas}"),
            Self::Standalone => f.write_str("is a standalone CAS with no upstream"),
            Self::Unreported => write!(
                f,
                "does not say which CAS it passes traffic to (it sends no `{UPSTREAM_HEADER}`)"
            ),
        }
    }
}

/// A daemon's answer to the question of which CAS it passes traffic to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Answer {
    upstream: ReportedUpstream,
    pid: Option<String>,
}

impl Answer {
    pub(crate) fn from_metadata(metadata: &MetadataMap) -> Self {
        let header = |name| metadata.get(name).and_then(|v| v.to_str().ok());
        let upstream = match header(UPSTREAM_HEADER) {
            None => ReportedUpstream::Unreported,
            Some("") => ReportedUpstream::Standalone,
            Some(address) => ReportedUpstream::Cas(CasIdentity::new(
                address,
                header(UPSTREAM_TLS_HEADER).and_then(|v| v.parse().ok()),
                header(UPSTREAM_INSTANCE_NAME_HEADER),
                header(UPSTREAM_CREDENTIALS_HEADER).map(str::to_owned),
            )),
        };
        Self {
            upstream,
            pid: header(PID_HEADER).map(str::to_owned),
        }
    }

    fn serves(&self, ours: &CasIdentity) -> bool {
        matches!(&self.upstream, ReportedUpstream::Cas(theirs) if theirs == ours)
    }

    /// The daemon as a message names it.
    fn daemon(&self, address: &DaemonAddress) -> String {
        match &self.pid {
            Some(pid) => format!("the buck2-casd at {address} (pid {pid})"),
            None => format!("the buck2-casd at {address}"),
        }
    }
}

/// A daemon this buck2 daemon has refused, and whether the warning has reached a console yet.
struct Refusal {
    warning: String,
    shown: bool,
}

/// The daemons this process has refused, by address and the CAS it refused each for. Only a
/// definite answer is kept: another upstream, none, or no header. A refusal holds for the life
/// of the buck2 daemon, because buck2 builds a new client for a command when no other command
/// holds one (buck2_execute's re/manager.rs), and those clients must not switch back to the
/// shared daemon halfway through a session.
///
/// Process-wide, so tests that share a binary stay apart only by using distinct addresses.
static REFUSED: LazyLock<Mutex<HashMap<(String, CasIdentity), Refusal>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Whether this client should send its CAS traffic through the daemon at `address`: whether the
/// daemon says it passes that traffic to this client's own CAS. It is asked whoever started it,
/// since a daemon this client just spawned may have lost the address to another one.
///
/// A daemon that says otherwise, or says nothing (one from before buck2 asked), is refused for
/// the life of this buck2 daemon and left running, because other buck2 daemons may be using it
/// correctly. A daemon that cannot be asked is not used by this client, and the next client
/// asks again: one that is busy or restarting is not one that serves another CAS.
pub(crate) async fn serves_this_cas(
    opts: &Buck2OssReConfiguration,
    address: &DaemonAddress,
) -> anyhow::Result<bool> {
    let Some(ours) = CasIdentity::of(opts)? else {
        return Ok(true);
    };
    let key = (address.pool_address(), ours.clone());
    if REFUSED.lock().unwrap().contains_key(&key) {
        tracing::debug!("Not using buck2-casd at {address}, refused earlier");
        show_refusal(&key);
        return Ok(false);
    }

    match query_upstream(address).await {
        Ok(answer) if answer.serves(&ours) => Ok(true),
        Ok(answer) => {
            let warning = refusal_warning(address, &ours, &answer);
            REFUSED
                .lock()
                .unwrap()
                .entry(key.clone())
                .or_insert_with(|| {
                    tracing::warn!("{warning}");
                    Refusal {
                        warning,
                        shown: false,
                    }
                });
            show_refusal(&key);
            Ok(false)
        }
        Err(e) => {
            let warning = format!(
                "Not using the shared CAS cache for now: asking the buck2-casd at {address} \
                 which CAS it passes traffic to failed ({e:#}). buck2 talks to {ours} directly, \
                 and asks the buck2-casd again for a later command."
            );
            tracing::warn!("{warning}");
            if let Some(dispatcher) = buck2_events::dispatch::get_dispatcher_opt() {
                dispatcher.console_warning(warning);
            }
            Ok(false)
        }
    }
}

/// Puts a refusal on the console the first time a client is built inside a command. A client
/// built outside one (the materializer can be first) has no console to show it on.
fn show_refusal(key: &(String, CasIdentity)) {
    let mut refused = REFUSED.lock().unwrap();
    let Some(refusal) = refused.get_mut(key) else {
        return;
    };
    if refusal.shown {
        return;
    }
    if let Some(dispatcher) = buck2_events::dispatch::get_dispatcher_opt() {
        dispatcher.console_warning(refusal.warning.clone());
        refusal.shown = true;
    }
}

const REMEDY: &str = "To share the cache again, stop that buck2-casd and run `buck2 kill`, or set \
                      `cas_shared_cache_address` to another socket.";

fn refusal_warning(address: &DaemonAddress, ours: &CasIdentity, theirs: &Answer) -> String {
    format!(
        "Not using the shared CAS cache: {} {}, but this daemon's CAS is {ours}. This buck2 \
         daemon talks to its CAS directly until it restarts. {REMEDY}",
        theirs.daemon(address),
        theirs.upstream,
    )
}

/// Asks the daemon, before every connection a client opens to it, whether it still passes
/// traffic to the client's CAS. The question at build time does not cover the life of the
/// client: a tonic channel redials its connector by itself when a connection breaks (tonic's
/// transport/channel/service/reconnect.rs), as `GRPCClients::reconnect` does after starting a
/// successor, and the daemon that answers then may be one another buck2 daemon started for
/// another CAS.
#[derive(Clone, Debug)]
pub(crate) struct UpstreamCheck {
    address: DaemonAddress,
    ours: CasIdentity,
}

impl UpstreamCheck {
    pub(crate) fn new(
        opts: &Buck2OssReConfiguration,
        address: &DaemonAddress,
    ) -> anyhow::Result<Option<Self>> {
        Ok(CasIdentity::of(opts)?.map(|ours| Self {
            address: address.clone(),
            ours,
        }))
    }

    /// A `FAILED_PRECONDITION` status when the daemon serves another CAS, which tonic hands to
    /// the request that needed the connection (it looks for a `Status` among an error's
    /// sources), so that the build fails naming both CASes and is not retried.
    async fn verify(&self) -> Result<(), tonic::Status> {
        let answer = query_upstream(&self.address)
            .await
            .map_err(|e| tonic::Status::unavailable(format!("{e:#}")))?;
        if answer.serves(&self.ours) {
            return Ok(());
        }
        let message = format!(
            "Not sending CAS traffic through {}: it {}, but this daemon's CAS is {}. It is not \
             the buck2-casd this client started with. {REMEDY}",
            answer.daemon(&self.address),
            answer.upstream,
            self.ours,
        );
        tracing::warn!("{message}");
        Err(tonic::Status::failed_precondition(message))
    }
}

/// A connector that runs an [`UpstreamCheck`] before each connection it makes, if it has one.
#[derive(Clone)]
pub(crate) struct CheckedConnector<C> {
    inner: C,
    check: Option<Arc<UpstreamCheck>>,
}

impl<C> CheckedConnector<C> {
    pub(crate) fn new(inner: C, check: Option<Arc<UpstreamCheck>>) -> Self {
        Self { inner, check }
    }
}

impl<C> Service<Uri> for CheckedConnector<C>
where
    C: Service<Uri> + Clone + Send + 'static,
    C::Future: Send + 'static,
    C::Error: Into<tonic::codegen::StdError>,
{
    type Response = C::Response;
    type Error = tonic::codegen::StdError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        // The instance polled ready makes the call; the clone waits for the next one.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let check = self.check.clone();
        Box::pin(async move {
            if let Some(check) = check {
                check.verify().await?;
            }
            inner.call(uri).await.map_err(Into::into)
        })
    }
}

async fn query_upstream(address: &DaemonAddress) -> anyhow::Result<Answer> {
    tokio::time::timeout(UPSTREAM_QUERY_TIMEOUT, query_capabilities_metadata(address))
        .await
        .with_context(|| {
            format!("buck2-casd at {address} did not answer within {UPSTREAM_QUERY_TIMEOUT:?}")
        })?
        .with_context(|| format!("Error asking buck2-casd at {address} for its upstream"))
        .map(|metadata| Answer::from_metadata(&metadata))
}

async fn query_capabilities_metadata(address: &DaemonAddress) -> anyhow::Result<MetadataMap> {
    let channel = match address {
        DaemonAddress::Unix(path) => {
            // The URI only fills the `:authority` header; the connector ignores it.
            Endpoint::from_static("http://unix.invalid/")
                .connect_with_connector(UnixConnector::new(Arc::new(path.clone())))
                .await?
        }
        DaemonAddress::Loopback(port) => {
            Endpoint::from_shared(format!("http://127.0.0.1:{port}"))?
                .connect()
                .await?
        }
    };
    let response = CapabilitiesClient::new(channel)
        .get_capabilities(GetCapabilitiesRequest::default())
        .await?;
    Ok(response.metadata().clone())
}

/// Works out the binary and arguments for the daemon from the same configuration this client
/// uses, so the daemon talks to the same upstream the same way.
pub fn plan_launch(
    opts: &Buck2OssReConfiguration,
    address: &DaemonAddress,
    cache_dir: &Path,
    substitute_env_vars: &impl Fn(&str) -> anyhow::Result<String>,
) -> anyhow::Result<Launch> {
    let binary = match &opts.cas_shared_cache_binary {
        Some(configured) => PathBuf::from(
            substitute_env_vars(configured).context("Invalid `cas_shared_cache_binary`")?,
        ),
        None => default_binary(),
    };

    let mut args = vec![
        "--dir".to_owned(),
        cache_dir.to_string_lossy().into_owned(),
        "--listen".to_owned(),
        address.listen_arg(),
    ];
    // The daemon addresses blobs by buck2's preferred algorithm, the first effective one.
    if let Some(algorithm) = opts.digest_algorithms.first() {
        args.push("--digest-function".to_owned());
        args.push(algorithm.to_ascii_lowercase());
    }
    if let Some(cap) = opts.cas_shared_cache_max_size_bytes {
        args.push("--max-size-bytes".to_owned());
        args.push(cap.to_string());
    }
    if let Some(upstream) = &opts.cas_address {
        // Substituted here, unlike the headers, so that the daemon reports the address it
        // dials and a client with the same configuration recognises it as its own.
        args.push("--upstream".to_owned());
        args.push(substitute_env_vars(upstream).context("Invalid `cas_address`")?);
        // Without the flag the daemon lets the address scheme decide, as buck2 does when
        // `tls` is unset. `false` is spelled out, or a `grpcs://` address would have the daemon
        // dial TLS on port 443 where buck2 dials plaintext on port 80. `true` stays the bare
        // flag, which every buck2-casd accepts.
        match opts.tls {
            Some(true) => args.push("--upstream-tls".to_owned()),
            Some(false) => args.push("--upstream-tls=false".to_owned()),
            None => {}
        }
        if let Some(ca) = &opts.tls_ca_certs {
            args.push("--upstream-tls-ca-certs".to_owned());
            args.push(ca.clone());
        }
        if let Some(cert) = &opts.tls_client_cert {
            args.push("--upstream-tls-client-cert".to_owned());
            args.push(cert.clone());
        }
        if let Some(instance) = &opts.instance_name {
            args.push("--upstream-instance-name".to_owned());
            args.push(instance.clone());
        }
    }
    let http_headers_env = if opts.http_headers.is_empty() {
        None
    } else {
        Some(
            opts.http_headers
                .iter()
                .map(|h| format!("{}: {}", h.key, h.value))
                .collect::<Vec<_>>()
                .join(","),
        )
    };

    Ok(Launch {
        binary,
        dir: cache_dir.to_owned(),
        args,
        http_headers_env,
    })
}

/// A `buck2-casd` next to the running executable if there is one, else whatever `PATH` finds.
fn default_binary() -> PathBuf {
    let name = if cfg!(windows) {
        "buck2-casd.exe"
    } else {
        "buck2-casd"
    };
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let sibling = dir.join(name);
            if sibling.is_file() {
                return sibling;
            }
        }
    }
    PathBuf::from(name)
}

/// Starts the daemon so that it outlives this process: its own session, no inherited stdio, and
/// its output in a log file in the cache directory.
fn spawn_detached(launch: &Launch) -> anyhow::Result<()> {
    let log_path = launch.dir.join(LOG_FILE_NAME);
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("Error opening `{}`", log_path.display()))?;
    let log_err = log
        .try_clone()
        .with_context(|| format!("Error opening `{}`", log_path.display()))?;

    let mut command = Command::new(&launch.binary);
    command
        .args(&launch.args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    if let Some(headers) = &launch.http_headers_env {
        command.env("BUCK2_CASD_UPSTREAM_HTTP_HEADERS", headers);
    }
    if std::env::var_os("RUST_LOG").is_none() {
        command.env("RUST_LOG", "info");
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }

    let mut child = command.spawn().with_context(|| {
        format!(
            "Error starting buck2-casd from `{}` (set `cas_shared_cache_binary` or disable \
             `cas_shared_cache_autostart`)",
            launch.binary.display()
        )
    })?;
    // The daemon is meant to outlive us, so nothing waits for it, but a child that dies while we
    // are still around must still be reaped or it lingers as a zombie.
    std::thread::Builder::new()
        .name("buck2-casd-reaper".to_owned())
        .spawn(move || {
            let _ignored = child.wait();
        })
        .context("Error spawning the buck2-casd reaper thread")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use buck2_re_configuration::HttpHeader;

    use super::*;

    fn no_subst(s: &str) -> anyhow::Result<String> {
        Ok(s.to_owned())
    }

    #[test]
    fn test_resolve_address() -> anyhow::Result<()> {
        let dir = Path::new("/var/cache/casd");
        assert_eq!(
            DaemonAddress::resolve(None, Some(dir), no_subst)?,
            DaemonAddress::Unix(PathBuf::from("/var/cache/casd/buck2-casd.sock"))
        );
        assert_eq!(
            DaemonAddress::resolve(Some(&CASdAddress::Tcp(9092)), Some(dir), no_subst)?,
            DaemonAddress::Loopback(9092)
        );
        assert_eq!(
            DaemonAddress::resolve(
                Some(&CASdAddress::Uds("/run/casd.sock".to_owned())),
                None,
                no_subst
            )?,
            DaemonAddress::Unix(PathBuf::from("/run/casd.sock"))
        );
        assert!(DaemonAddress::resolve(None, None, no_subst).is_err());
        assert_eq!(
            DaemonAddress::Unix(PathBuf::from("/x.sock")).pool_address(),
            "unix:///x.sock"
        );
        assert_eq!(
            DaemonAddress::Loopback(9092).pool_address(),
            "grpc://127.0.0.1:9092"
        );
        Ok(())
    }

    #[test]
    fn test_plan_launch_passes_upstream_settings() -> anyhow::Result<()> {
        let opts = Buck2OssReConfiguration {
            cas_address: Some("grpc://cas.example.com:443".to_owned()),
            tls: Some(true),
            tls_ca_certs: Some("/etc/ca.pem".to_owned()),
            instance_name: Some("main".to_owned()),
            http_headers: vec![HttpHeader {
                key: "Authorization".to_owned(),
                value: "Bearer $TOKEN".to_owned(),
            }],
            cas_shared_cache_binary: Some("/opt/buck2/buck2-casd".to_owned()),
            cas_shared_cache_max_size_bytes: Some(1234),
            digest_algorithms: vec!["SHA256".to_owned()],
            ..Default::default()
        };
        let launch = plan_launch(
            &opts,
            &DaemonAddress::Unix(PathBuf::from("/var/cache/casd/buck2-casd.sock")),
            Path::new("/var/cache/casd"),
            &no_subst,
        )?;
        assert_eq!(launch.binary, PathBuf::from("/opt/buck2/buck2-casd"));
        assert_eq!(
            launch.args,
            vec![
                "--dir",
                "/var/cache/casd",
                "--listen",
                "unix:///var/cache/casd/buck2-casd.sock",
                "--digest-function",
                "sha256",
                "--max-size-bytes",
                "1234",
                "--upstream",
                "grpc://cas.example.com:443",
                "--upstream-tls",
                "--upstream-tls-ca-certs",
                "/etc/ca.pem",
                "--upstream-instance-name",
                "main",
            ]
        );
        // Headers never go on the command line.
        assert!(!launch.args.iter().any(|a| a.contains("Bearer")));
        assert_eq!(
            launch.http_headers_env.as_deref(),
            Some("Authorization: Bearer $TOKEN")
        );
        Ok(())
    }

    #[test]
    fn test_plan_launch_substitutes_the_upstream_but_not_the_headers() -> anyhow::Result<()> {
        let opts = Buck2OssReConfiguration {
            cas_address: Some("grpc://$CAS_HOST:443".to_owned()),
            http_headers: vec![HttpHeader {
                key: "x-api-key".to_owned(),
                value: "$API_KEY".to_owned(),
            }],
            cas_shared_cache_binary: Some("buck2-casd".to_owned()),
            ..Default::default()
        };
        let subst = |s: &str| Ok(s.replace("$CAS_HOST", "cas.example.com"));
        let launch = plan_launch(&opts, &DaemonAddress::Loopback(1), Path::new("/c"), &subst)?;
        assert_eq!(
            launch.args[launch.args.len() - 2..],
            ["--upstream", "grpc://cas.example.com:443"]
        );
        // The daemon substitutes the headers itself, from the environment it inherits.
        assert_eq!(
            launch.http_headers_env.as_deref(),
            Some("x-api-key: $API_KEY")
        );
        Ok(())
    }

    #[test]
    fn test_plan_launch_spells_out_tls_false() -> anyhow::Result<()> {
        let opts = Buck2OssReConfiguration {
            cas_address: Some("grpcs://cas.example.com".to_owned()),
            tls: Some(false),
            cas_shared_cache_binary: Some("buck2-casd".to_owned()),
            ..Default::default()
        };
        let launch = plan_launch(
            &opts,
            &DaemonAddress::Loopback(1),
            Path::new("/c"),
            &no_subst,
        )?;
        assert_eq!(
            launch.args[launch.args.len() - 3..],
            [
                "--upstream",
                "grpcs://cas.example.com",
                "--upstream-tls=false"
            ]
        );
        Ok(())
    }

    #[test]
    fn test_cas_identity_compares_what_the_channel_dials() {
        let id = |address, tls, instance| CasIdentity::new(address, tls, instance, None);
        let cas = id("grpc://cas.example.com:443", Some(true), None);
        // The scheme only selects TLS, and the default port follows the TLS that results.
        assert_eq!(id("grpcs://cas.example.com", None, None), cas);
        assert_eq!(id("https://cas.example.com:443/", None, None), cas);
        assert_eq!(id("grpc://cas.example.com", Some(true), None), cas);
        assert_eq!(id("cas.example.com:443", None, None), cas);
        assert_eq!(id("grpc://CAS.Example.com:443", Some(true), None), cas);
        assert_eq!(
            id("grpcs://cas.example.com", Some(false), None),
            id("grpc://cas.example.com:80", None, None),
            "`tls = false` wins over the scheme, port and all"
        );

        assert_ne!(
            id("grpc://cas.example.com:443", None, None),
            cas,
            "plaintext to the port"
        );
        assert_ne!(id("grpc://other.example.com:443", Some(true), None), cas);
        assert_ne!(id("grpc://cas.example.com:8443", Some(true), None), cas);
        assert_ne!(id("grpc://cas.example.com", None, None), cas, "port 80");
        assert_ne!(
            id("grpc://cas.example.com:443", Some(true), Some("main")),
            cas
        );
        assert_ne!(
            id("grpc://cas.example.com:443", None, Some("main")),
            id("grpc://cas.example.com:443", None, Some("other"))
        );
        assert_ne!(
            CasIdentity::new("grpcs://cas.example.com", None, None, Some("k1".to_owned())),
            CasIdentity::new("grpcs://cas.example.com", None, None, Some("k2".to_owned())),
            "another organisation's key"
        );
        assert_ne!(
            CasIdentity::new("grpcs://cas.example.com", None, None, Some("k1".to_owned())),
            cas
        );

        // What does not parse as a gRPC address is compared as written.
        assert_eq!(
            id("unix:///run/cas.sock", Some(false), None),
            id("unix:///run/cas.sock", None, None)
        );
        assert_ne!(
            id("unix:///run/cas.sock", None, None),
            id("unix:///run/other.sock", None, None)
        );
        assert_eq!(
            cas.to_string(),
            "`cas.example.com:443` over TLS, no credentials"
        );
    }

    #[test]
    fn test_credentials_fingerprint() -> anyhow::Result<()> {
        let header = |key: &str, value: &str| HttpHeader {
            key: key.to_owned(),
            value: value.to_owned(),
        };
        let fingerprint =
            |headers: &[HttpHeader], cert| upstream_credentials_fingerprint(headers, cert).unwrap();
        assert_eq!(fingerprint(&[], None), None);

        let key = fingerprint(&[header("x-api-key", "k1"), header("x-other", "v")], None);
        let digest = key.as_deref().expect("headers are credentials");
        assert_eq!(digest.len(), 64);
        assert!(!digest.contains("k1"));
        // Neither the order of the headers nor the case of their names matters.
        assert_eq!(
            fingerprint(&[header("x-other", "v"), header("X-Api-Key", "k1")], None),
            key
        );
        assert_ne!(fingerprint(&[header("x-api-key", "k2")], None), key);
        assert_ne!(
            fingerprint(&[header("x-api-key", "k1")], None),
            key,
            "a header fewer"
        );

        // A relative certificate path is the file it names from this working directory.
        let cwd = std::env::current_dir()?;
        let cert = fingerprint(&[], Some("client.pem"));
        assert!(cert.is_some());
        let absolute = cwd.join("client.pem");
        assert_eq!(fingerprint(&[], Some(absolute.to_str().unwrap())), cert);
        assert_ne!(fingerprint(&[], Some("/etc/other.pem")), cert);
        Ok(())
    }

    #[test]
    fn test_answer_from_metadata() -> anyhow::Result<()> {
        let mut metadata = MetadataMap::new();
        let ours = CasIdentity::new("grpcs://cas.example.com", None, Some("main"), None);
        let answer = Answer::from_metadata(&metadata);
        assert_eq!(answer.upstream, ReportedUpstream::Unreported);
        assert!(
            !answer.serves(&ours),
            "a daemon that does not say is refused"
        );

        metadata.insert(UPSTREAM_HEADER, "".parse()?);
        assert_eq!(
            Answer::from_metadata(&metadata).upstream,
            ReportedUpstream::Standalone
        );

        metadata.insert(UPSTREAM_HEADER, "grpc://cas.example.com".parse()?);
        metadata.insert(UPSTREAM_TLS_HEADER, "true".parse()?);
        metadata.insert(UPSTREAM_INSTANCE_NAME_HEADER, "main".parse()?);
        metadata.insert(PID_HEADER, "4242".parse()?);
        let answer = Answer::from_metadata(&metadata);
        assert!(answer.serves(&ours));
        assert_eq!(answer.pid.as_deref(), Some("4242"));

        metadata.insert(UPSTREAM_CREDENTIALS_HEADER, "abc".parse()?);
        assert!(!Answer::from_metadata(&metadata).serves(&ours));
        metadata.insert(UPSTREAM_TLS_HEADER, "false".parse()?);
        metadata.remove(UPSTREAM_CREDENTIALS_HEADER);
        assert!(!Answer::from_metadata(&metadata).serves(&ours));
        Ok(())
    }

    #[test]
    fn test_refusal_warning_names_both_upstreams_and_the_daemon() -> anyhow::Result<()> {
        let mut metadata = MetadataMap::new();
        metadata.insert(UPSTREAM_HEADER, "grpcs://ns.example.com".parse()?);
        metadata.insert(PID_HEADER, "4242".parse()?);
        let warning = refusal_warning(
            &DaemonAddress::Unix(PathBuf::from("/c/buck2-casd.sock")),
            &CasIdentity::new("grpc://bb.example.com:443", Some(true), None, None),
            &Answer::from_metadata(&metadata),
        );
        assert!(warning.contains("unix:///c/buck2-casd.sock"), "{warning}");
        assert!(warning.contains("pid 4242"), "{warning}");
        assert!(warning.contains("`bb.example.com:443`"), "{warning}");
        assert!(warning.contains("`ns.example.com:443`"), "{warning}");
        assert!(warning.contains("cas_shared_cache_address"), "{warning}");
        Ok(())
    }

    #[test]
    fn test_plan_launch_standalone_on_a_port() -> anyhow::Result<()> {
        let opts = Buck2OssReConfiguration {
            cas_shared_cache_binary: Some("buck2-casd".to_owned()),
            ..Default::default()
        };
        let launch = plan_launch(
            &opts,
            &DaemonAddress::Loopback(1),
            Path::new("/c"),
            &no_subst,
        )?;
        assert_eq!(launch.args, vec!["--dir", "/c", "--listen", "127.0.0.1:1"]);
        assert!(launch.http_headers_env.is_none());
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_ensure_running_is_a_no_op_when_the_socket_answers() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;
        let socket = work.path().join(DEFAULT_SOCKET_NAME);
        let _listener = tokio::net::UnixListener::bind(&socket)?;
        let opts = Buck2OssReConfiguration {
            // Would fail loudly if a spawn were attempted.
            cas_shared_cache_binary: Some("/definitely/not/a/binary".to_owned()),
            ..Default::default()
        };
        ensure_running(&opts, &DaemonAddress::Unix(socket), work.path(), no_subst).await?;
        assert!(!work.path().join(LOCK_FILE_NAME).exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_ensure_running_is_a_no_op_when_the_port_answers() -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let work = tempfile::tempdir()?;
        let opts = Buck2OssReConfiguration {
            cas_shared_cache_binary: Some("/definitely/not/a/binary".to_owned()),
            ..Default::default()
        };
        ensure_running(&opts, &DaemonAddress::Loopback(port), work.path(), no_subst).await?;
        assert!(!work.path().join(LOCK_FILE_NAME).exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_ensure_running_reports_missing_binary() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;
        let opts = Buck2OssReConfiguration {
            cas_shared_cache_binary: Some("/definitely/not/a/binary".to_owned()),
            ..Default::default()
        };
        let address = DaemonAddress::Unix(work.path().join(DEFAULT_SOCKET_NAME));
        let err = ensure_running(&opts, &address, work.path(), no_subst)
            .await
            .expect_err("cannot start a missing binary");
        assert!(
            format!("{err:#}").contains("cas_shared_cache_binary"),
            "{err:#}"
        );
        Ok(())
    }
}
