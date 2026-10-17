/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! `[buck2] expected_binary_digest`: refuse to run a buck2 binary the project did not pin.
//!
//! A client whose version differs from the running daemon's kills that daemon and starts its
//! own ("buck2 daemon constraint mismatch: Version mismatch; killing daemon..."). When a second
//! buck2 reaches a project, for example a tool that brings its own buck2 onto `PATH`, that kill
//! takes the first daemon's build and its persistent workers' parent with it, and the two
//! binaries go on replacing each other's daemon. With this key set, a binary whose digest is not
//! listed exits before it connects to, kills or starts a daemon, and says which binary the
//! project expects.
//!
//! The key holds one or more digests of the buck2 executable, separated by commas or whitespace,
//! each `sha256:<hex>`, `blake3:<hex>` or bare `<hex>` (read as sha256, which is what `sha256sum`
//! prints). Any match accepts the binary, so a project can list the binaries of several
//! platforms, or the old and new binary during a rollout. The digest is computed over the
//! executable `get_daemon_exe` names, which is the one whose build id decides the daemon's
//! version constraint, and is cached under `~/.buck/binary-digest` keyed by the file's path,
//! size, modification and change times, device and inode, so only the first run of a new binary
//! reads it.
//!
//! `BUCK2_IGNORE_EXPECTED_BINARY_DIGEST=1` skips the check for one deliberate override.

use std::fmt;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use buck2_common::cas_digest::CasDigestData;
use buck2_common::cas_digest::DigestAlgorithm;
use buck2_core::buck2_env;
use buck2_error::BuckErrorContext;

use crate::immediate_config::ImmediateConfigContext;

pub const EXPECTED_BINARY_DIGEST_KEY: &str = "expected_binary_digest";
const IGNORE_ENV: &str = "BUCK2_IGNORE_EXPECTED_BINARY_DIGEST";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BinaryDigestAlgorithm {
    Sha256,
    Blake3,
}

impl BinaryDigestAlgorithm {
    fn name(self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
            Self::Blake3 => "blake3",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BinaryDigest {
    algorithm: BinaryDigestAlgorithm,
    /// Lowercase hex.
    hex: String,
}

impl fmt::Display for BinaryDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.algorithm.name(), self.hex)
    }
}

#[derive(Debug, buck2_error::Error)]
#[buck2(tag = Input)]
enum ExpectedBinaryDigestError {
    #[error(
        "Invalid `[buck2] {EXPECTED_BINARY_DIGEST_KEY}` entry `{0}`: expected `sha256:<64 hex>`, `blake3:<64 hex>` or `<64 hex>`"
    )]
    InvalidEntry(String),
    #[error(
        "This buck2 binary is not the one this project expects, so it will not connect to, \
        restart or start a buck2 daemon here.\n  \
        binary:   {exe}\n  \
        digest:   {actual}\n  \
        expected: {expected} (`[buck2] {EXPECTED_BINARY_DIGEST_KEY}`, set in {project_root})\n\
        Run the project's own buck2 instead, usually through the launcher the project checks in \
        (for example `bin/buck2` on PATH ahead of any other buck2). To run this binary anyway, \
        which kills a daemon started by another buck2 binary, set {IGNORE_ENV}=1."
    )]
    Mismatch {
        exe: String,
        actual: String,
        expected: String,
        project_root: String,
    },
}

/// Parses the value of `[buck2] expected_binary_digest`.
pub(crate) fn parse_expected(value: &str) -> buck2_error::Result<Vec<BinaryDigest>> {
    value
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|s| !s.is_empty())
        .map(|entry| {
            let (algorithm, hex) = match entry.split_once(':') {
                Some(("sha256", hex)) => (BinaryDigestAlgorithm::Sha256, hex),
                Some(("blake3", hex)) => (BinaryDigestAlgorithm::Blake3, hex),
                Some(_) => {
                    return Err(ExpectedBinaryDigestError::InvalidEntry(entry.to_owned()).into());
                }
                None => (BinaryDigestAlgorithm::Sha256, entry),
            };
            if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(ExpectedBinaryDigestError::InvalidEntry(entry.to_owned()).into());
            }
            Ok(BinaryDigest {
                algorithm,
                hex: hex.to_ascii_lowercase(),
            })
        })
        .collect()
}

/// Called before a command connects to, restarts or starts a daemon. Returns an error when the
/// project sets `[buck2] expected_binary_digest` and this binary matches none of its entries.
pub fn check_expected_binary_digest(
    immediate_config: &ImmediateConfigContext<'_>,
) -> buck2_error::Result<()> {
    let Some(value) = immediate_config.expected_binary_digest()? else {
        return Ok(());
    };
    let expected = parse_expected(&value)?;
    if expected.is_empty() {
        return Ok(());
    }
    if buck2_env!("BUCK2_IGNORE_EXPECTED_BINARY_DIGEST", bool)? {
        tracing::warn!("{IGNORE_ENV} is set: not checking `[buck2] {EXPECTED_BINARY_DIGEST_KEY}`");
        return Ok(());
    }
    let exe = crate::daemon::client::connect::get_daemon_exe()?;
    let exe = std::fs::canonicalize(&exe).unwrap_or(exe);
    let cache_dir =
        buck2_wrapper_common::buck2_home_dir().map(|home| home.join(".buck").join("binary-digest"));
    match check_binary(&exe, &expected, cache_dir.as_deref())? {
        Ok(()) => Ok(()),
        Err(actual) => Err(ExpectedBinaryDigestError::Mismatch {
            exe: exe.display().to_string(),
            actual: actual
                .iter()
                .map(|d| d.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            expected: expected
                .iter()
                .map(|d| d.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            project_root: immediate_config
                .project_root_for_display()
                .unwrap_or_else(|_| "the project's buckconfig".to_owned()),
        }
        .into()),
    }
}

/// `Ok(Ok(()))` when `exe` matches an entry, `Ok(Err(actual))` with the digests computed when it
/// matches none.
pub(crate) fn check_binary(
    exe: &Path,
    expected: &[BinaryDigest],
    cache_dir: Option<&Path>,
) -> buck2_error::Result<Result<(), Vec<BinaryDigest>>> {
    let mut actual = Vec::new();
    for algorithm in [BinaryDigestAlgorithm::Sha256, BinaryDigestAlgorithm::Blake3] {
        if !expected.iter().any(|e| e.algorithm == algorithm) {
            continue;
        }
        let digest = cached_digest(exe, algorithm, cache_dir)?;
        if expected.contains(&digest) {
            return Ok(Ok(()));
        }
        actual.push(digest);
    }
    Ok(Err(actual))
}

/// The cache entry's name: a hash of everything that identifies this file's current contents
/// without reading them. A replaced binary changes its inode. A binary rewritten in place keeps
/// its inode and can keep its size and, through `touch -d` or `cp --preserve=timestamps`, its
/// mtime, but any write sets its ctime to the current time and no unprivileged call can set it
/// back (stat(2), inode(7)), so ctime is what makes a stale digest unreachable.
fn cache_key(exe: &Path, algorithm: BinaryDigestAlgorithm) -> buck2_error::Result<String> {
    let meta = std::fs::metadata(exe)
        .with_buck_error_context(|| format!("Error reading metadata of {}", exe.display()))?;
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_nanos());
    #[cfg(unix)]
    let (dev, ino, ctime) = {
        use std::os::unix::fs::MetadataExt;
        (
            meta.dev(),
            meta.ino(),
            (meta.ctime() as i128) * 1_000_000_000 + meta.ctime_nsec() as i128,
        )
    };
    #[cfg(not(unix))]
    let (dev, ino, ctime) = (0u64, 0u64, 0i128);
    let mut hasher = blake3::Hasher::new();
    hasher.update(
        format!(
            "v2\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
            exe.display(),
            meta.len(),
            mtime,
            ctime,
            dev,
            ino,
            algorithm.name()
        )
        .as_bytes(),
    );
    Ok(hasher.finalize().to_hex()[..32].to_owned())
}

fn cached_digest(
    exe: &Path,
    algorithm: BinaryDigestAlgorithm,
    cache_dir: Option<&Path>,
) -> buck2_error::Result<BinaryDigest> {
    let cache_file: Option<PathBuf> = match cache_dir {
        Some(dir) => Some(dir.join(cache_key(exe, algorithm)?)),
        None => None,
    };
    if let Some(file) = &cache_file {
        if let Ok(hex) = std::fs::read_to_string(file) {
            let hex = hex.trim();
            if hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Ok(BinaryDigest {
                    algorithm,
                    hex: hex.to_owned(),
                });
            }
        }
    }
    let digest = compute_digest(exe, algorithm)?;
    if let Some(file) = &cache_file {
        // Best effort: a cache that cannot be written only costs the next run a read of the file.
        if let Err(e) = write_atomically(file, digest.hex.as_bytes()) {
            tracing::debug!(
                "Not caching the binary digest in {}: {:#}",
                file.display(),
                e
            );
        }
    }
    Ok(digest)
}

fn write_atomically(file: &Path, contents: &[u8]) -> std::io::Result<()> {
    let dir = file
        .parent()
        .ok_or_else(|| std::io::Error::other("cache file has no parent"))?;
    std::fs::create_dir_all(dir)?;
    // Named after the entry as well as the pid: two processes with one pid (in different pid
    // namespaces sharing a home directory) writing different entries through one temporary file
    // would otherwise rename one binary's digest into the other's entry.
    let name = file
        .file_name()
        .ok_or_else(|| std::io::Error::other("cache file has no name"))?;
    let tmp = dir.join(format!(
        ".tmp-{}-{}",
        name.to_string_lossy(),
        std::process::id()
    ));
    std::fs::write(&tmp, contents)?;
    std::fs::rename(&tmp, file)
}

fn compute_digest(
    exe: &Path,
    algorithm: BinaryDigestAlgorithm,
) -> buck2_error::Result<BinaryDigest> {
    let mut file = std::fs::File::open(exe)
        .with_buck_error_context(|| format!("Error opening {} to digest it", exe.display()))?;
    let mut digester = CasDigestData::digester_for_algorithm(match algorithm {
        BinaryDigestAlgorithm::Sha256 => DigestAlgorithm::Sha256,
        BinaryDigestAlgorithm::Blake3 => DigestAlgorithm::Blake3,
    });
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = file
            .read(&mut buf)
            .with_buck_error_context(|| format!("Error reading {} to digest it", exe.display()))?;
        if n == 0 {
            break;
        }
        digester.update(&buf[..n]);
    }
    Ok(BinaryDigest {
        algorithm,
        hex: digester.finalize().raw_digest().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ABC_SHA256: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    const ABC_BLAKE3: &str = "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85";

    fn exe_with(contents: &[u8]) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("buck2");
        std::fs::write(&exe, contents).unwrap();
        (dir, exe)
    }

    #[test]
    fn test_parse_list() {
        let parsed = parse_expected(&format!(
            " sha256:{ABC_SHA256},\n blake3:{} {}",
            ABC_BLAKE3.to_ascii_uppercase(),
            ABC_SHA256
        ))
        .unwrap();
        assert_eq!(
            parsed.iter().map(|d| d.to_string()).collect::<Vec<_>>(),
            vec![
                format!("sha256:{ABC_SHA256}"),
                format!("blake3:{ABC_BLAKE3}"),
                format!("sha256:{ABC_SHA256}"),
            ]
        );
        assert!(parse_expected("  ").unwrap().is_empty());
    }

    #[test]
    fn test_parse_rejects() {
        assert!(parse_expected("md5:abc").is_err());
        assert!(parse_expected(&format!("sha1:{ABC_SHA256}")).is_err());
        assert!(parse_expected(&ABC_SHA256[1..]).is_err());
        assert!(parse_expected(&format!("{}g", &ABC_SHA256[1..])).is_err());
    }

    #[test]
    fn test_match_and_mismatch() {
        let (_dir, exe) = exe_with(b"abc");
        let sha = parse_expected(ABC_SHA256).unwrap();
        assert_eq!(check_binary(&exe, &sha, None).unwrap(), Ok(()));
        let blake = parse_expected(&format!("blake3:{ABC_BLAKE3}")).unwrap();
        assert_eq!(check_binary(&exe, &blake, None).unwrap(), Ok(()));

        let other = parse_expected(&format!("sha256:{}", "0".repeat(64))).unwrap();
        let actual = check_binary(&exe, &other, None).unwrap().unwrap_err();
        assert_eq!(
            actual.iter().map(|d| d.to_string()).collect::<Vec<_>>(),
            vec![format!("sha256:{ABC_SHA256}")]
        );

        // Any entry matching accepts the binary.
        let mut list = other.clone();
        list.extend(blake);
        assert_eq!(check_binary(&exe, &list, None).unwrap(), Ok(()));
    }

    #[test]
    fn test_cache_is_used_and_invalidated() {
        let (dir, exe) = exe_with(b"abc");
        let cache = dir.path().join("cache");
        let expected = parse_expected(ABC_SHA256).unwrap();
        assert_eq!(check_binary(&exe, &expected, Some(&cache)).unwrap(), Ok(()));
        let entries: Vec<_> = std::fs::read_dir(&cache).unwrap().collect();
        assert_eq!(entries.len(), 1);

        // A poisoned entry for the same path, size, mtime and inode is believed: that is the
        // point of the cache, and proves the second run did not read the binary.
        let entry = entries[0].as_ref().unwrap().path();
        std::fs::write(&entry, "0".repeat(64)).unwrap();
        assert!(
            check_binary(&exe, &expected, Some(&cache))
                .unwrap()
                .is_err()
        );

        // Replacing the binary changes its size (and inode), so the entry no longer applies.
        let replacement = dir.path().join("buck2.new");
        std::fs::write(&replacement, b"abcd").unwrap();
        std::fs::rename(&replacement, &exe).unwrap();
        let abcd =
            parse_expected("88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589")
                .unwrap();
        assert_eq!(check_binary(&exe, &abcd, Some(&cache)).unwrap(), Ok(()));
    }

    /// A binary rewritten in place with the same size, inode and restored mtime must not reuse
    /// the old digest.
    #[cfg(unix)]
    #[test]
    fn test_cache_misses_in_place_rewrite_with_restored_mtime() {
        let (dir, exe) = exe_with(b"abc");
        let cache = dir.path().join("cache");
        let abc = parse_expected(ABC_SHA256).unwrap();
        assert_eq!(check_binary(&exe, &abc, Some(&cache)).unwrap(), Ok(()));

        let before = std::fs::metadata(&exe).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::OpenOptions::new()
            .write(true)
            .open(&exe)
            .and_then(|mut f| std::io::Write::write_all(&mut f, b"abd"))
            .unwrap();
        std::fs::File::options()
            .write(true)
            .open(&exe)
            .unwrap()
            .set_modified(before.modified().unwrap())
            .unwrap();
        let after = std::fs::metadata(&exe).unwrap();
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(
                (before.len(), before.ino(), before.modified().unwrap()),
                (after.len(), after.ino(), after.modified().unwrap())
            );
        }

        // The cache holds abc's digest for the old ctime; the rewritten file must be read again.
        assert!(check_binary(&exe, &abc, Some(&cache)).unwrap().is_err());
    }
}
