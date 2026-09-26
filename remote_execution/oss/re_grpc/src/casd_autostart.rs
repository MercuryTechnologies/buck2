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

use std::fmt;
use std::fs::File;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use buck2_re_configuration::Buck2OssReConfiguration;
use buck2_re_configuration::CASdAddress;

/// How long to wait for a freshly started daemon, or one another process is starting.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const PROBE_INTERVAL: Duration = Duration::from_millis(100);
const LOCK_FILE_NAME: &str = "autostart.lock";
const LOG_FILE_NAME: &str = "buck2-casd.log";
/// The socket the daemon listens on when no address is configured, inside its directory.
pub const DEFAULT_SOCKET_NAME: &str = "buck2-casd.sock";

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
        args.push("--upstream".to_owned());
        args.push(upstream.clone());
        // Without the flag the daemon lets the address scheme decide, as buck2 does when
        // `tls` is unset.
        if opts.tls == Some(true) {
            args.push("--upstream-tls".to_owned());
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
