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
use buck2_re_configuration::Buck2OssReConfiguration;
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
