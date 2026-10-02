/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! The buck2 gRPC client starting the real `buck2-casd` binary on demand.

// Unix sockets and signals throughout.
#![cfg(unix)]

use std::time::Duration;

use buck2_casd::Config;
use buck2_casd::DEFAULT_SOCKET_NAME;
use buck2_casd::Listen;
use buck2_casd::PID_FILE_NAME;
use buck2_casd::digest::DigestFunction;
use buck2_re_configuration::Buck2OssReConfiguration;
use remote_execution::DownloadRequest;
use remote_execution::InlinedBlobWithDigest;
use remote_execution::NamedDigest;
use remote_execution::NamedDigestWithPermissions;
use remote_execution::REClientBuilder;
use remote_execution::RemoteExecutionMetadata;
use remote_execution::TDigest;
use remote_execution::UploadRequest;

#[tokio::test(flavor = "multi_thread")]
async fn client_autostarts_the_daemon() -> anyhow::Result<()> {
    // The client probes a clone from the store into its working directory and leaves the
    // daemon off when it fails, so the work directory is the working directory too. This is the
    // only test in the binary, so changing the process's directory races with nothing.
    // `BUCK2_CASD_REFLINK_TEST_DIR` names a reflink filesystem (the XFS /workspaces of a
    // Namespace devbox); elsewhere this test checks the fallback instead.
    let work = match std::env::var_os("BUCK2_CASD_REFLINK_TEST_DIR") {
        Some(dir) => tempfile::tempdir_in(dir)?,
        None => tempfile::tempdir()?,
    };
    std::env::set_current_dir(work.path())?;

    // The remote: a standalone daemon in-process.
    let origin = buck2_casd::start(Config {
        dir: work.path().join("origin"),
        listen: Listen::Loopback(0),
        digest_function: DigestFunction::Sha256,
        max_size_bytes: None,
        upstream: None,
        eviction_interval: Duration::from_secs(3600),
    })
    .await?;
    let remote = origin.address.to_string();

    // No address configured: the daemon is expected at its default socket in the directory.
    let casd_dir = work.path().join("casd");

    let opts = Buck2OssReConfiguration {
        cas_address: Some(remote.clone()),
        engine_address: Some(remote.clone()),
        action_cache_address: Some(remote),
        tls: Some(false),
        capabilities: Some(true),
        cas_shared_cache: Some(casd_dir.to_str().unwrap().to_owned()),
        cas_shared_cache_binary: Some(env!("CARGO_BIN_EXE_buck2-casd").to_owned()),
        cas_shared_cache_max_size_bytes: Some(1 << 30),
        digest_algorithms: vec!["SHA256".to_owned()],
        ..Default::default()
    };

    let reflink = remote_execution::probe_reflink(&casd_dir, work.path()).is_ok();

    // Building the client is what starts the daemon.
    let client = REClientBuilder::build_and_connect(&opts).await?;
    let pid_file = casd_dir.join(PID_FILE_NAME);
    if !reflink {
        assert!(!pid_file.exists(), "buck2-casd started without reflink");
        assert!(!casd_dir.join(DEFAULT_SOCKET_NAME).exists());
        return Ok(());
    }
    let pid_contents = std::fs::read_to_string(&pid_file)?;
    let pid: u32 = pid_contents.lines().next().unwrap().parse()?;
    assert!(pid != std::process::id());
    assert!(casd_dir.join("buck2-casd.log").exists());
    assert!(casd_dir.join(DEFAULT_SOCKET_NAME).exists());
    assert!(
        pid_contents.contains(&format!(
            "unix://{}",
            casd_dir.join(DEFAULT_SOCKET_NAME).display()
        )),
        "{pid_contents}"
    );

    let cleanup = || {
        #[cfg(unix)]
        {
            // SAFETY: plain libc call with a pid we just read from the file the daemon wrote.
            unsafe {
                libc_kill(pid as i32);
            }
        }
    };

    let outcome = async {
        // Upload through the auto-started daemon: it passes the blob to the origin.
        let data = b"started on demand".to_vec();
        let digest = TDigest {
            hash: DigestFunction::Sha256.hash_bytes(&data),
            size_in_bytes: data.len() as i64,
            ..Default::default()
        };
        client
            .upload(
                RemoteExecutionMetadata::default(),
                UploadRequest {
                    inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                        digest: digest.clone(),
                        blob: data.clone(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                },
            )
            .await?;
        assert!(
            origin
                .store
                .lookup_for_test(&digest.hash, digest.size_in_bytes)
                .await?
        );
        // Through the daemon, which the client asked for its upstream although it had just
        // started it, and was not turned away by its own configuration.
        let stored = |d: &TDigest| {
            casd_dir
                .join("blobs")
                .join(&d.hash[..2])
                .join(format!("{}-{}", d.hash, d.size_in_bytes))
        };
        assert!(
            stored(&digest).exists(),
            "the upload went through buck2-casd"
        );

        // A second client sees the daemon already running and reuses it.
        let again = REClientBuilder::build_and_connect(&opts).await?;
        let out = work.path().join("out");
        again
            .download(
                &RemoteExecutionMetadata::default(),
                DownloadRequest {
                    file_digests: Some(vec![NamedDigestWithPermissions {
                        named_digest: NamedDigest {
                            name: out.to_str().unwrap().to_owned(),
                            digest: digest.clone(),
                            ..Default::default()
                        },
                        is_executable: false,
                        ..Default::default()
                    }]),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(std::fs::read(&out)?, data);
        assert_eq!(
            std::fs::read_to_string(&pid_file)?,
            pid_contents,
            "not restarted"
        );

        // Stop the daemon under the live clients, as `buck2 killall` or a crash would. It takes
        // its socket file with it. The next call that needs the daemon (an upload; a download
        // of a blob the directory already holds never touches it) fails on the broken
        // connection, and the client's retry starts a new daemon before redialing.
        cleanup();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while casd_dir.join(DEFAULT_SOCKET_NAME).exists() {
            assert!(std::time::Instant::now() < deadline, "daemon did not exit");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(!pid_file.exists(), "pid file removed on exit");
        let more = b"after a restart".to_vec();
        let more_digest = TDigest {
            hash: DigestFunction::Sha256.hash_bytes(&more),
            size_in_bytes: more.len() as i64,
            ..Default::default()
        };
        again
            .upload(
                RemoteExecutionMetadata::default(),
                UploadRequest {
                    inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                        digest: more_digest.clone(),
                        blob: more.clone(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                },
            )
            .await?;
        assert!(
            origin
                .store
                .lookup_for_test(&more_digest.hash, more_digest.size_in_bytes)
                .await?,
            "the upload went through a new daemon to the origin"
        );
        assert!(stored(&more_digest).exists(), "and through the new daemon");
        let new_pid: u32 = std::fs::read_to_string(&pid_file)?
            .lines()
            .next()
            .unwrap()
            .parse()?;
        assert_ne!(new_pid, pid, "a new daemon was started");

        // Now kill it outright, as `buck2 killall` does. The stale socket file stays, and nothing
        // answers on it; the upload after that must still go through a third daemon.
        // SAFETY: a pid the new daemon just wrote.
        unsafe { libc_kill_signal(new_pid as i32, 9) };
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while unsafe { libc_kill_signal(new_pid as i32, 0) } == 0 {
            assert!(std::time::Instant::now() < deadline, "daemon did not die");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            casd_dir.join(DEFAULT_SOCKET_NAME).exists(),
            "SIGKILL leaves the socket"
        );
        let third = b"after a hard kill".to_vec();
        let third_digest = TDigest {
            hash: DigestFunction::Sha256.hash_bytes(&third),
            size_in_bytes: third.len() as i64,
            ..Default::default()
        };
        again
            .upload(
                RemoteExecutionMetadata::default(),
                UploadRequest {
                    inlined_blobs_with_digest: Some(vec![InlinedBlobWithDigest {
                        digest: third_digest.clone(),
                        blob: third.clone(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                },
            )
            .await?;
        assert!(
            origin
                .store
                .lookup_for_test(&third_digest.hash, third_digest.size_in_bytes)
                .await?
        );
        let new_pid: u32 = std::fs::read_to_string(&pid_file)?
            .lines()
            .next()
            .unwrap()
            .parse()?;
        anyhow::Ok(new_pid)
    }
    .await;

    // Whatever happened, leave no daemon behind.
    cleanup();
    if let Ok(new_pid) = &outcome {
        #[cfg(unix)]
        // SAFETY: as above, a pid the new daemon just wrote.
        unsafe {
            libc_kill(*new_pid as i32);
        }
    }
    origin.shutdown().await?;
    outcome.map(|_| ())
}

#[cfg(unix)]
unsafe fn libc_kill(pid: i32) {
    unsafe { libc_kill_signal(pid, 15) };
}

#[cfg(unix)]
unsafe fn libc_kill_signal(pid: i32, sig: i32) -> i32 {
    unsafe extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    unsafe { kill(pid, sig) }
}
