/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::path::PathBuf;
use std::time::Duration;

use buck2_casd::Config;
use buck2_casd::Listen;
use buck2_casd::digest::DigestFunction;
use buck2_casd::upstream::UpstreamConfig;
use clap::Parser;

// The static musl release carries jemalloc (see the tikv-jemalloc-sys fixups);
// musl's own malloc is much slower for a daemon that streams blobs.
#[global_allocator]
#[cfg(all(target_os = "linux", target_env = "musl"))]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// A machine-local CAS daemon that fronts a remote CAS and shares its blobs, as raw files that
/// buck2 can reflink, with every buck2 daemon on this host.
#[derive(Parser, Debug)]
#[command(name = "buck2-casd", version)]
struct Args {
    /// Directory to keep blobs in. Give the same path to buck2 as
    /// `[buck2_re_client] cas_shared_cache`. It must be on the same filesystem as the
    /// repositories' `buck-out` for reflinks to work.
    #[arg(long)]
    dir: PathBuf,

    /// Where to listen: `unix://<path>` or a loopback port. Defaults to `buck2-casd.sock` inside
    /// `--dir`, which is where buck2 looks unless `cas_shared_cache_address` says otherwise.
    #[arg(long)]
    listen: Option<Listen>,

    /// Hash function blobs are addressed by; must match `[buck2] digest_algorithms`.
    #[arg(long, default_value = "sha256")]
    digest_function: DigestFunction,

    /// Evict least recently used blobs once the store exceeds this many bytes.
    #[arg(long)]
    max_size_bytes: Option<u64>,

    /// Seconds between periodic checks of the size cap.
    #[arg(long, default_value_t = 60)]
    eviction_interval_secs: u64,

    /// gRPC address of the remote CAS to pass through to, e.g. `grpc://cas.example.com:443`.
    /// Without it the daemon is a standalone CAS.
    #[arg(long)]
    upstream: Option<String>,

    /// Use TLS for the upstream connection, or not with `--upstream-tls=false`, whatever the
    /// scheme of `--upstream` says. Without it the scheme decides, as it does for buck2 when
    /// `[buck2_re_client] tls` is unset.
    #[arg(
        long,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    upstream_tls: Option<bool>,

    /// PEM bundle of CA certificates for the upstream connection.
    #[arg(long)]
    upstream_tls_ca_certs: Option<String>,

    /// PEM client certificate and key for the upstream connection.
    #[arg(long)]
    upstream_tls_client_cert: Option<String>,

    /// `Header: value` to add to every upstream request. Repeatable. Can also be given as a
    /// comma-separated list in `BUCK2_CASD_UPSTREAM_HTTP_HEADERS`, which keeps secrets out of
    /// the process's command line.
    #[arg(
        long = "upstream-http-header",
        env = "BUCK2_CASD_UPSTREAM_HTTP_HEADERS",
        value_delimiter = ','
    )]
    upstream_http_headers: Vec<String>,

    /// Instance name for upstream requests.
    #[arg(long)]
    upstream_instance_name: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let upstream = args.upstream.map(|address| UpstreamConfig {
        address,
        tls: args.upstream_tls,
        tls_ca_certs: args.upstream_tls_ca_certs,
        tls_client_cert: args.upstream_tls_client_cert,
        http_headers: args.upstream_http_headers,
        instance_name: args.upstream_instance_name,
    });
    let listen = args
        .listen
        .unwrap_or_else(|| Listen::default_for(&args.dir));
    let running = buck2_casd::start(Config {
        dir: args.dir,
        listen,
        digest_function: args.digest_function,
        max_size_bytes: args.max_size_bytes,
        upstream,
        eviction_interval: Duration::from_secs(args.eviction_interval_secs),
    })
    .await?;

    wait_for_termination_signal().await?;
    tracing::info!("Shutting down");
    running.shutdown().await
}

#[cfg(unix)]
async fn wait_for_termination_signal() -> anyhow::Result<()> {
    use tokio::signal::unix::SignalKind;
    use tokio::signal::unix::signal;
    let mut term = signal(SignalKind::terminate())?;
    tokio::select! {
        r = tokio::signal::ctrl_c() => r?,
        _ = term.recv() => {}
    }
    Ok(())
}

#[cfg(not(unix))]
async fn wait_for_termination_signal() -> anyhow::Result<()> {
    Ok(tokio::signal::ctrl_c().await?)
}
