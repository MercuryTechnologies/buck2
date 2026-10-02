/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! `buck2-casd`: a machine-local content-addressed storage daemon for open-source buck2.
//!
//! It speaks the remote execution API's CAS and ByteStream services, passes misses and uploads
//! through to a real remote CAS, and keeps every blob it has seen as a raw, read-only file in a
//! directory it alone owns. Buck2 daemons on the same machine point their CAS traffic at it and,
//! given the directory, materialize outputs by reflinking those files instead of receiving bytes
//! over gRPC. One store, one downloader, one eviction policy, any number of isolation dirs.

pub mod digest;
pub mod server;
pub mod store;
pub mod upstream;

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use re_grpc_proto::build::bazel::remote::execution::v2::capabilities_server::CapabilitiesServer;
use re_grpc_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorageServer;
use re_grpc_proto::google::bytestream::byte_stream_server::ByteStreamServer;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::digest::DigestFunction;
use crate::server::Cas;
use crate::store::Store;
use crate::upstream::Upstream;
use crate::upstream::UpstreamConfig;

/// gRPC message size limit. Batch payloads are capped well below this.
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Written into the store directory: the daemon's pid on the first line, its address on the
/// second.
pub const PID_FILE_NAME: &str = "buck2-casd.pid";
/// The socket inside the store directory that the daemon listens on by default.
pub const DEFAULT_SOCKET_NAME: &str = "buck2-casd.sock";

/// Where the daemon listens. It never listens anywhere other than this machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Listen {
    /// A Unix socket; the default is `buck2-casd.sock` inside the store directory.
    Unix(PathBuf),
    /// A loopback TCP port. Port 0 picks a free one; see [`Running::address`].
    Loopback(u16),
}

impl Listen {
    pub fn default_for(dir: &std::path::Path) -> Self {
        Self::Unix(dir.join(DEFAULT_SOCKET_NAME))
    }
}

impl FromStr for Listen {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(path) = s.strip_prefix("unix://") {
            return Ok(Self::Unix(PathBuf::from(path)));
        }
        if let Ok(port) = s.parse::<u16>() {
            return Ok(Self::Loopback(port));
        }
        let addr: SocketAddr = s.parse().with_context(|| {
            format!("`{s}` is not `unix://<path>`, a port, or `127.0.0.1:<port>`")
        })?;
        if !addr.ip().is_loopback() {
            return Err(anyhow::anyhow!(
                "`{s}` is not a loopback address; buck2-casd only serves this machine"
            ));
        }
        Ok(Self::Loopback(addr.port()))
    }
}

/// The address a daemon is reachable at, in the form the buck2 client accepts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Address {
    Unix(PathBuf),
    Loopback(SocketAddr),
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unix(path) => write!(f, "unix://{}", path.display()),
            Self::Loopback(addr) => write!(f, "grpc://{addr}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    /// Root of the store. Buck2 daemons are given this same path as `cas_shared_cache`.
    pub dir: PathBuf,
    pub listen: Listen,
    pub digest_function: DigestFunction,
    /// Size cap for the store; `None` never evicts.
    pub max_size_bytes: Option<u64>,
    /// The remote CAS to pass through to. `None` makes this a standalone CAS.
    pub upstream: Option<UpstreamConfig>,
    /// How often to check the cap when nothing is being written.
    pub eviction_interval: Duration,
}

/// A running daemon.
pub struct Running {
    pub address: Address,
    pub store: Arc<Store>,
    dir: PathBuf,
    shutdown: Option<oneshot::Sender<()>>,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
    evictor: JoinHandle<()>,
}

/// How long a stopping daemon lets in-flight requests finish before it exits anyway.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

impl Running {
    /// Stops serving: removes the socket and pid files at once, so that nothing points at a
    /// daemon that is going away (buck2 clients take a missing socket file as the cue to start
    /// a new one), then gives in-flight requests [`DRAIN_TIMEOUT`] to finish. Idle client
    /// connections do not hold it up.
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        self.remove_files();
        if let Some(tx) = self.shutdown.take() {
            let _ignored = tx.send(());
        }
        self.evictor.abort();
        match tokio::time::timeout(DRAIN_TIMEOUT, &mut self.server).await {
            Ok(joined) => joined
                .context("Server task panicked")?
                .context("Server failed"),
            Err(_) => {
                tracing::warn!("Requests still in flight after {DRAIN_TIMEOUT:?}; exiting anyway");
                self.server.abort();
                Ok(())
            }
        }
    }

    /// Waits until the server exits on its own, then removes the socket and pid files.
    pub async fn wait(mut self) -> anyhow::Result<()> {
        self.evictor.abort();
        let result = (&mut self.server)
            .await
            .context("Server task panicked")
            .and_then(|r| r.context("Server failed"));
        self.remove_files();
        result
    }

    fn remove_files(&self) {
        if let Address::Unix(path) = &self.address {
            let _ignored = std::fs::remove_file(path);
        }
        let _ignored = std::fs::remove_file(self.dir.join(PID_FILE_NAME));
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ignored = tx.send(());
        }
        self.evictor.abort();
    }
}

pub async fn start(config: Config) -> anyhow::Result<Running> {
    let store = Arc::new(Store::open(
        &config.dir,
        config.digest_function,
        config.max_size_bytes,
    )?);
    let upstream = match &config.upstream {
        Some(upstream) => Some(Arc::new(
            Upstream::connect(upstream, config.digest_function).await?,
        )),
        None => None,
    };
    let cas = Cas::new(Arc::clone(&store), upstream, config.upstream.as_ref());

    let (address, incoming) = bind(&config.listen).await?;

    // For operators and for whoever auto-started us: which process serves this directory.
    let pid_file = config.dir.join(PID_FILE_NAME);
    std::fs::write(&pid_file, format!("{}\n{}\n", std::process::id(), address))
        .with_context(|| format!("Error writing `{}`", pid_file.display()))?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let router = tonic::transport::Server::builder()
        .add_service(
            ContentAddressableStorageServer::new(cas.clone())
                .max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE),
        )
        .add_service(
            ByteStreamServer::new(cas.clone())
                .max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE),
        )
        .add_service(CapabilitiesServer::new(cas));
    let server = tokio::spawn(router.serve_with_incoming_shutdown(incoming, async {
        let _ignored = shutdown_rx.await;
    }));

    let evictor = tokio::spawn({
        let store = Arc::clone(&store);
        let interval = config.eviction_interval;
        async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = store.evict().await {
                    tracing::warn!("Eviction failed: {e:#}");
                }
            }
        }
    });

    tracing::info!(
        "buck2-casd serving at {address}, store `{}`, upstream {}",
        config.dir.display(),
        config
            .upstream
            .as_ref()
            .map_or("none (standalone)".to_owned(), |u| u.address.clone())
    );
    Ok(Running {
        address,
        store,
        dir: config.dir.clone(),
        shutdown: Some(shutdown_tx),
        server,
        evictor,
    })
}

/// One accepted connection, of either kind.
type Incoming =
    std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<Connection>> + Send + 'static>>;

/// A connection from either listener; tonic needs one concrete type per server.
#[derive(Debug)]
pub enum Connection {
    Tcp(tokio::net::TcpStream),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
}

impl tonic::transport::server::Connected for Connection {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

impl tokio::io::AsyncRead for Connection {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            #[cfg(unix)]
            Self::Unix(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for Connection {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            #[cfg(unix)]
            Self::Unix(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => std::pin::Pin::new(s).poll_flush(cx),
            #[cfg(unix)]
            Self::Unix(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            #[cfg(unix)]
            Self::Unix(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

async fn bind(listen: &Listen) -> anyhow::Result<(Address, Incoming)> {
    use futures::StreamExt;
    match listen {
        Listen::Loopback(port) => {
            let addr = SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), *port);
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("Error binding `{addr}`"))?;
            let address = Address::Loopback(listener.local_addr()?);
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener)
                .map(|c| c.map(Connection::Tcp));
            Ok((address, Box::pin(incoming)))
        }
        #[cfg(unix)]
        Listen::Unix(path) => {
            // A socket file left by a dead daemon must be cleared; a live one must be respected.
            if path.exists() {
                if tokio::net::UnixStream::connect(path).await.is_ok() {
                    return Err(anyhow::anyhow!(
                        "Another buck2-casd is already listening at `{}`",
                        path.display()
                    ));
                }
                std::fs::remove_file(path)
                    .with_context(|| format!("Error removing stale `{}`", path.display()))?;
            }
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("Error creating `{}`", parent.display()))?;
            }
            let listener = tokio::net::UnixListener::bind(path)
                .with_context(|| format!("Error binding `{}`", path.display()))?;
            let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener)
                .map(|c| c.map(Connection::Unix));
            Ok((Address::Unix(path.clone()), Box::pin(incoming)))
        }
        #[cfg(not(unix))]
        Listen::Unix(path) => Err(anyhow::anyhow!(
            "Unix sockets are not supported on this platform (`{}`); use --listen <port>",
            path.display()
        )),
    }
}
