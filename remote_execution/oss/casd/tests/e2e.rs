/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! End-to-end tests: real daemons on loopback, driven by the same gRPC client buck2 uses.

use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use buck2_casd::Config;
use buck2_casd::Listen;
use buck2_casd::Running;
use buck2_casd::digest::DigestFunction;
use buck2_casd::server::MAX_BATCH_TOTAL_SIZE_BYTES;
use buck2_casd::upstream::UpstreamConfig;
use buck2_data::InstantEvent;
use buck2_data::buck_event;
use buck2_data::instant_event;
use buck2_events::Event;
use buck2_events::daemon_id::DaemonId;
use buck2_events::dispatch::EventDispatcher;
use buck2_events::dispatch::with_dispatcher_async;
use buck2_re_configuration::Buck2OssReConfiguration;
use buck2_re_configuration::CASdAddress;
use buck2_wrapper_common::invocation_id::TraceId;
use remote_execution::DownloadRequest;
use remote_execution::GetDigestsTtlRequest;
use remote_execution::InlinedBlobWithDigest;
use remote_execution::NamedDigest;
use remote_execution::NamedDigestWithPermissions;
use remote_execution::REClient;
use remote_execution::REClientBuilder;
use remote_execution::RemoteExecutionMetadata;
use remote_execution::TDigest;
use remote_execution::UploadRequest;

/// Origins listen on their default Unix socket (where there are Unix sockets); proxies on a
/// loopback port, so both kinds of listener and both kinds of client connection are exercised.
async fn daemon(dir: &Path, upstream: Option<&Running>, max_size_bytes: Option<u64>) -> Running {
    buck2_casd::start(Config {
        dir: dir.to_owned(),
        listen: if upstream.is_some() || !cfg!(unix) {
            Listen::Loopback(0)
        } else {
            Listen::default_for(dir)
        },
        digest_function: DigestFunction::Sha256,
        max_size_bytes,
        upstream: upstream.map(|origin| UpstreamConfig {
            address: origin.address.to_string(),
            ..Default::default()
        }),
        eviction_interval: Duration::from_secs(3600),
    })
    .await
    .expect("daemon starts")
}

async fn client(daemon: &Running) -> REClient {
    let address = daemon.address.to_string();
    REClientBuilder::build_and_connect(&Buck2OssReConfiguration {
        cas_address: Some(address.clone()),
        engine_address: Some(address.clone()),
        action_cache_address: Some(address),
        tls: Some(false),
        capabilities: Some(true),
        ..Default::default()
    })
    .await
    .expect("client connects")
}

fn digest_of(data: &[u8]) -> TDigest {
    TDigest {
        hash: DigestFunction::Sha256.hash_bytes(data),
        size_in_bytes: data.len() as i64,
        ..Default::default()
    }
}

fn stored_path(dir: &Path, d: &TDigest) -> PathBuf {
    dir.join("blobs")
        .join(&d.hash[..2])
        .join(format!("{}-{}", d.hash, d.size_in_bytes))
}

/// Deterministic, incompressible-ish bytes bigger than one batch, so it goes over ByteStream.
fn large_blob() -> Vec<u8> {
    let mut state: u64 = 0x9E3779B97F4A7C15;
    (0..MAX_BATCH_TOTAL_SIZE_BYTES + (1 << 20))
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn named(path: &Path, d: &TDigest) -> NamedDigest {
    NamedDigest {
        name: path.to_str().unwrap().to_owned(),
        digest: d.clone(),
        ..Default::default()
    }
}

async fn upload_inlined(client: &REClient, data: &[u8]) -> TDigest {
    let d = digest_of(data);
    client
        .upload(
            RemoteExecutionMetadata::default(),
            UploadRequest {
                inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                    digest: d.clone(),
                    blob: data.to_vec(),
                    ..Default::default()
                }]),
                upload_only_missing: false,
                ..Default::default()
            },
        )
        .await
        .expect("upload succeeds");
    d
}

async fn upload_file(client: &REClient, path: &Path) -> TDigest {
    let d = digest_of(&std::fs::read(path).unwrap());
    client
        .upload(
            RemoteExecutionMetadata::default(),
            UploadRequest {
                files_with_digest: Some(vec![named(path, &d)]),
                upload_only_missing: false,
                ..Default::default()
            },
        )
        .await
        .expect("upload succeeds");
    d
}

async fn download_to(client: &REClient, d: &TDigest, path: &Path) -> anyhow::Result<()> {
    client
        .download(
            &RemoteExecutionMetadata::default(),
            DownloadRequest {
                file_digests: Some(vec![NamedDigestWithPermissions {
                    named_digest: named(path, d),
                    is_executable: false,
                    ..Default::default()
                }]),
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}

async fn is_missing(client: &REClient, d: &TDigest) -> bool {
    let response = client
        .get_digests_ttl(
            &RemoteExecutionMetadata::default(),
            GetDigestsTtlRequest {
                digests: vec![d.clone()],
                ..Default::default()
            },
        )
        .await
        .expect("find missing succeeds");
    response.digests_with_ttl[0].ttl == 0
}

#[tokio::test(flavor = "multi_thread")]
async fn standalone_roundtrip() -> anyhow::Result<()> {
    let work = tempfile::tempdir()?;
    let store_dir = work.path().join("store");
    let origin = daemon(&store_dir, None, None).await;
    let client = client(&origin).await;

    let small = upload_inlined(&client, b"hello").await;
    let large_data = large_blob();
    let large_src = work.path().join("large.bin");
    std::fs::write(&large_src, &large_data)?;
    let large = upload_file(&client, &large_src).await;

    // Stored as raw, read-only files at the documented layout.
    assert_eq!(std::fs::read(stored_path(&store_dir, &small))?, b"hello");
    assert_eq!(std::fs::read(stored_path(&store_dir, &large))?, large_data);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(stored_path(&store_dir, &large))?
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o444);
    }

    assert!(!is_missing(&client, &small).await);
    assert!(!is_missing(&client, &large).await);
    assert!(is_missing(&client, &digest_of(b"never uploaded")).await);

    let out_small = work.path().join("out_small");
    let out_large = work.path().join("out_large");
    download_to(&client, &small, &out_small).await?;
    download_to(&client, &large, &out_large).await?;
    assert_eq!(std::fs::read(&out_small)?, b"hello");
    assert_eq!(std::fs::read(&out_large)?, large_data);

    let err = download_to(&client, &digest_of(b"absent"), &work.path().join("nope"))
        .await
        .expect_err("absent blob must fail");
    assert!(format!("{err:#}").contains("not found"), "{err:#}");

    // Uploads that lie about their content are rejected and never stored.
    let bogus = TDigest {
        hash: "0".repeat(64),
        size_in_bytes: 5,
        ..Default::default()
    };
    let rejected = client
        .upload(
            RemoteExecutionMetadata::default(),
            UploadRequest {
                inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                    digest: bogus.clone(),
                    blob: b"hello".to_vec(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
        )
        .await;
    assert!(rejected.is_err());
    assert!(!stored_path(&store_dir, &bogus).exists());

    origin.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn proxy_passes_through() -> anyhow::Result<()> {
    let work = tempfile::tempdir()?;
    let origin_dir = work.path().join("origin");
    let proxy_dir = work.path().join("proxy");
    let origin = daemon(&origin_dir, None, None).await;
    let proxy = daemon(&proxy_dir, Some(&origin), None).await;
    let origin_client = client(&origin).await;
    let proxy_client = client(&proxy).await;

    // Blobs uploaded at the origin are fetched through the proxy and land in its store.
    let small = upload_inlined(&origin_client, b"from origin").await;
    let large_data = large_blob();
    let large_src = work.path().join("large.bin");
    std::fs::write(&large_src, &large_data)?;
    let large = upload_file(&origin_client, &large_src).await;
    assert!(!stored_path(&proxy_dir, &small).exists());

    let out_small = work.path().join("out_small");
    let out_large = work.path().join("out_large");
    download_to(&proxy_client, &small, &out_small).await?;
    download_to(&proxy_client, &large, &out_large).await?;
    assert_eq!(std::fs::read(&out_small)?, b"from origin");
    assert_eq!(std::fs::read(&out_large)?, large_data);
    assert_eq!(
        std::fs::read(stored_path(&proxy_dir, &small))?,
        b"from origin"
    );
    assert_eq!(std::fs::read(stored_path(&proxy_dir, &large))?, large_data);

    // Uploads through the proxy reach both stores.
    let via_proxy_small = upload_inlined(&proxy_client, b"via proxy").await;
    let via_proxy_src = work.path().join("via_proxy.bin");
    let mut via_proxy_data = large_data.clone();
    via_proxy_data.reverse();
    std::fs::write(&via_proxy_src, &via_proxy_data)?;
    let via_proxy_large = upload_file(&proxy_client, &via_proxy_src).await;
    for d in [&via_proxy_small, &via_proxy_large] {
        assert!(stored_path(&proxy_dir, d).exists(), "{d} in proxy");
        assert!(stored_path(&origin_dir, d).exists(), "{d} in origin");
    }
    assert_eq!(
        std::fs::read(stored_path(&origin_dir, &via_proxy_large))?,
        via_proxy_data
    );

    // FindMissingBlobs answers for the origin, not the proxy's own store.
    assert!(!is_missing(&proxy_client, &small).await);
    assert!(is_missing(&proxy_client, &digest_of(b"unknown")).await);

    // Concurrent misses for one blob are served correctly.
    let fresh = upload_inlined(&origin_client, b"concurrent").await;
    let mut tasks = Vec::new();
    for i in 0..6 {
        let proxy_client = client(&proxy).await;
        let fresh = fresh.clone();
        let out = work.path().join(format!("concurrent_{i}"));
        tasks.push(tokio::spawn(async move {
            download_to(&proxy_client, &fresh, &out).await?;
            anyhow::Ok(std::fs::read(&out)?)
        }));
    }
    for t in tasks {
        assert_eq!(t.await??, b"concurrent");
    }

    // A blob nobody has is an error, not a hang or an empty file.
    let absent = digest_of(b"absent everywhere");
    assert!(
        download_to(&proxy_client, &absent, &work.path().join("absent"))
            .await
            .is_err()
    );
    assert!(!stored_path(&proxy_dir, &absent).exists());
    assert_eq!(std::fs::read_dir(proxy_dir.join("tmp"))?.count(), 0);

    proxy.shutdown().await?;
    origin.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn evicts_when_over_cap() -> anyhow::Result<()> {
    let work = tempfile::tempdir()?;
    let store_dir = work.path().join("store");
    let origin = daemon(&store_dir, None, Some(100)).await;
    let client = client(&origin).await;
    for i in 0..10u8 {
        upload_inlined(&client, &[i; 20]).await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while origin.store.stats().total_bytes > 100 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "eviction did not run"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    origin.shutdown().await?;
    Ok(())
}

/// A client whose CAS traffic goes to `cas_address` through the daemon at `casd`, which it finds
/// running and did not start.
///
/// buck2 remembers a refused daemon process-wide, by the daemon's address and the client's CAS,
/// and the tests in this binary run at once. They stay apart because every daemon they start
/// has an address of its own.
async fn client_through(casd: &Running, cas_address: &str) -> REClient {
    let casd_address = match &casd.address {
        buck2_casd::Address::Loopback(address) => CASdAddress::Tcp(address.port()),
        buck2_casd::Address::Unix(path) => CASdAddress::Uds(path.to_str().unwrap().to_owned()),
    };
    REClientBuilder::build_and_connect(&Buck2OssReConfiguration {
        cas_address: Some(cas_address.to_owned()),
        engine_address: Some(cas_address.to_owned()),
        action_cache_address: Some(cas_address.to_owned()),
        tls: Some(false),
        capabilities: Some(true),
        cas_shared_cache_address: Some(casd_address),
        ..Default::default()
    })
    .await
    .expect("client connects")
}

/// Runs `f` with an event dispatcher and returns its result and the console warnings it emitted.
async fn with_console_warnings<T>(f: impl Future<Output = T>) -> (T, Vec<String>) {
    let (mut source, sink) = buck2_events::create_source_sink_pair();
    let dispatcher = EventDispatcher::new(TraceId::new(), DaemonId::new(), sink);
    let result = with_dispatcher_async(dispatcher, f).await;
    let mut warnings = Vec::new();
    while let Some(event) = source.try_receive() {
        if let Event::Buck(event) = event
            && let buck_event::Data::Instant(InstantEvent {
                data: Some(instant_event::Data::ConsoleWarning(warning)),
                ..
            }) = event.data()
        {
            warnings.push(warning.message.clone());
        }
    }
    (result, warnings)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_daemon_with_another_upstream_is_not_used() -> anyhow::Result<()> {
    let work = tempfile::tempdir()?;
    let theirs_dir = work.path().join("theirs");
    let ours_dir = work.path().join("ours");
    let proxy_dir = work.path().join("proxy");
    let theirs = daemon(&theirs_dir, None, None).await;
    let ours = daemon(&ours_dir, None, None).await;
    // Left running by a buck2 daemon configured for another remote.
    let proxy = daemon(&proxy_dir, Some(&theirs), None).await;
    let ours_address = ours.address.to_string();

    let (uploaded, warnings) = with_console_warnings(async {
        // buck2 builds a new client for a later command; it must not warn again.
        let first = client_through(&proxy, &ours_address).await;
        let second = client_through(&proxy, &ours_address).await;
        (
            upload_inlined(&first, b"first command").await,
            upload_inlined(&second, b"second command").await,
        )
    })
    .await;

    for d in [&uploaded.0, &uploaded.1] {
        assert!(stored_path(&ours_dir, d).exists(), "{d} went to our CAS");
        assert!(
            !stored_path(&proxy_dir, d).exists(),
            "{d} not through the proxy"
        );
        assert!(
            !stored_path(&theirs_dir, d).exists(),
            "{d} not to their CAS"
        );
    }
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    let warning = &warnings[0];
    assert!(warning.contains(&proxy.address.to_string()), "{warning}");
    assert!(warning.contains(&theirs.address.to_string()), "{warning}");
    assert!(warning.contains(&ours_address), "{warning}");
    assert!(
        warning.contains(&format!("pid {}", std::process::id())),
        "{warning}"
    );
    assert!(warning.contains("cas_shared_cache_address"), "{warning}");

    // Reads and existence checks go to our CAS as well: a blob only their CAS holds is
    // neither found nor fetched, and one only ours holds is both.
    let refused = client_through(&proxy, &ours_address).await;
    let only_theirs = upload_inlined(&client(&theirs).await, b"only theirs").await;
    let only_ours = upload_inlined(&client(&ours).await, b"only ours").await;
    assert!(is_missing(&refused, &only_theirs).await);
    assert!(!is_missing(&refused, &only_ours).await);
    let out = work.path().join("out");
    assert!(
        download_to(&refused, &only_theirs, &out).await.is_err(),
        "their blob is not read through the proxy"
    );
    download_to(&refused, &only_ours, &out).await?;
    assert_eq!(std::fs::read(&out)?, b"only ours");
    assert!(!stored_path(&proxy_dir, &only_ours).exists());
    // The other daemon is left to the clients it serves.
    let theirs_client = client_through(&proxy, &theirs.address.to_string()).await;
    let theirs_blob = upload_inlined(&theirs_client, b"still serving").await;
    assert!(stored_path(&proxy_dir, &theirs_blob).exists());

    proxy.shutdown().await?;
    ours.shutdown().await?;
    theirs.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_standalone_daemon_is_not_used_as_a_proxy() -> anyhow::Result<()> {
    let work = tempfile::tempdir()?;
    let ours_dir = work.path().join("ours");
    let standalone_dir = work.path().join("standalone");
    let ours = daemon(&ours_dir, None, None).await;
    // On its default socket where there are Unix sockets, so that both kinds of address are
    // asked for their upstream.
    let standalone = buck2_casd::start(Config {
        dir: standalone_dir.clone(),
        listen: if cfg!(unix) {
            Listen::default_for(&standalone_dir)
        } else {
            Listen::Loopback(0)
        },
        digest_function: DigestFunction::Sha256,
        max_size_bytes: None,
        upstream: None,
        eviction_interval: Duration::from_secs(3600),
    })
    .await?;
    let ours_address = ours.address.to_string();

    let (uploaded, warnings) = with_console_warnings(async {
        let client = client_through(&standalone, &ours_address).await;
        upload_inlined(&client, b"direct").await
    })
    .await;

    assert!(stored_path(&ours_dir, &uploaded).exists());
    assert!(!stored_path(&standalone_dir, &uploaded).exists());
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("standalone"), "{}", warnings[0]);

    standalone.shutdown().await?;
    ours.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_daemon_with_our_upstream_is_used() -> anyhow::Result<()> {
    let work = tempfile::tempdir()?;
    let origin_dir = work.path().join("origin");
    let proxy_dir = work.path().join("proxy");
    let origin = daemon(&origin_dir, None, None).await;
    let proxy = daemon(&proxy_dir, Some(&origin), None).await;

    let (uploaded, warnings) = with_console_warnings(async {
        let client = client_through(&proxy, &origin.address.to_string()).await;
        upload_inlined(&client, b"through the proxy").await
    })
    .await;

    assert!(
        stored_path(&proxy_dir, &uploaded).exists(),
        "through the proxy"
    );
    assert!(
        stored_path(&origin_dir, &uploaded).exists(),
        "and on to the origin"
    );
    assert_eq!(warnings, Vec::<String>::new());

    proxy.shutdown().await?;
    origin.shutdown().await?;
    Ok(())
}

/// The daemon a client was built against exits, and another buck2 daemon starts one for another
/// CAS at the same socket before this client reconnects. The client must not send it anything.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_daemon_that_replaces_ours_is_not_used() -> anyhow::Result<()> {
    let work = tempfile::tempdir()?;
    let ours_dir = work.path().join("ours");
    let theirs_dir = work.path().join("theirs");
    let ours = daemon(&ours_dir, None, None).await;
    let theirs = daemon(&theirs_dir, None, None).await;
    let socket = work.path().join("shared.sock");
    let proxy_at = |dir: PathBuf, upstream: &Running| {
        buck2_casd::start(Config {
            dir,
            listen: Listen::Unix(socket.clone()),
            digest_function: DigestFunction::Sha256,
            max_size_bytes: None,
            upstream: Some(UpstreamConfig {
                address: upstream.address.to_string(),
                ..Default::default()
            }),
            eviction_interval: Duration::from_secs(3600),
        })
    };
    let ours_address = ours.address.to_string();

    let first_dir = work.path().join("first");
    let first = proxy_at(first_dir.clone(), &ours).await?;
    let client = client_through(&first, &ours_address).await;
    let before = upload_inlined(&client, b"before the swap").await;
    assert!(stored_path(&first_dir, &before).exists());
    first.shutdown().await?;

    let wrong_dir = work.path().join("wrong");
    let wrong = proxy_at(wrong_dir.clone(), &theirs).await?;
    let data = b"after the swap";
    let d = digest_of(data);
    let err = client
        .upload(
            RemoteExecutionMetadata::default(),
            UploadRequest {
                inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                    digest: d.clone(),
                    blob: data.to_vec(),
                    ..Default::default()
                }]),
                upload_only_missing: false,
                ..Default::default()
            },
        )
        .await
        .expect_err("the replacement serves another CAS");
    let message = format!("{err:#}");
    assert!(message.contains("Not sending CAS traffic"), "{message}");
    assert!(message.contains(&theirs.address.to_string()), "{message}");
    assert!(!stored_path(&wrong_dir, &d).exists(), "not through it");
    assert!(!stored_path(&theirs_dir, &d).exists(), "not to their CAS");
    wrong.shutdown().await?;

    // A successor with our upstream at the same socket is used again.
    let successor_dir = work.path().join("successor");
    let successor = proxy_at(successor_dir.clone(), &ours).await?;
    let after = upload_inlined(&client, data).await;
    assert!(stored_path(&successor_dir, &after).exists());
    assert!(stored_path(&ours_dir, &after).exists());

    successor.shutdown().await?;
    theirs.shutdown().await?;
    ours.shutdown().await?;
    Ok(())
}
