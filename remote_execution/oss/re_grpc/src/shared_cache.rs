/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Read-only access to the blob directory of the machine-local CAS daemon (`buck2-casd`).
//!
//! The daemon is the single owner of that directory: it downloads, verifies, writes and evicts.
//! Buck2 only opens blobs for reading, to clone them into `buck-out`. On a filesystem with
//! reflink support (btrfs, XFS, APFS) the clone is copy-on-write, so the bytes exist once on disk
//! however many isolation dirs or checkouts materialize them. Elsewhere it degrades to a plain
//! copy, which still avoids receiving the bytes over gRPC.
//!
//! Layout, shared with the daemon:
//!
//! ```text
//! <root>/blobs/<first two hex chars>/<hash>-<size>   raw blob bytes, read-only
//! ```

use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

use anyhow::Context;
use buck2_re_configuration::CopyPolicy;

use crate::digest::TDigest;
use crate::response::TLocalCacheStats;

pub struct SharedCasCache {
    blobs_dir: PathBuf,
    copy_policy: CopyPolicy,
    /// Under the hybrid policy, set once a reflink failed because the filesystem cannot do it,
    /// so later materializations go straight to copying.
    reflink_unsupported: Arc<AtomicBool>,
}

impl std::fmt::Debug for SharedCasCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedCasCache")
            .field("blobs_dir", &self.blobs_dir)
            .field("copy_policy", &self.copy_policy)
            .finish()
    }
}

/// Hit and miss counters for one download request. The lookups that update them run
/// concurrently, hence the atomics.
#[derive(Default)]
pub struct SharedCacheCounters {
    hits_files: AtomicI64,
    hits_bytes: AtomicI64,
    misses_files: AtomicI64,
    misses_bytes: AtomicI64,
}

impl SharedCacheCounters {
    pub fn hit(&self, bytes: i64) {
        self.hits_files.fetch_add(1, Ordering::Relaxed);
        self.hits_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn miss(&self, bytes: i64) {
        self.misses_files.fetch_add(1, Ordering::Relaxed);
        self.misses_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn take_stats(&self) -> TLocalCacheStats {
        let hits_files = self.hits_files.load(Ordering::Relaxed);
        let misses_files = self.misses_files.load(Ordering::Relaxed);
        TLocalCacheStats {
            total_cache_lookup_attempts: hits_files + misses_files,
            hits_files,
            hits_bytes: self.hits_bytes.load(Ordering::Relaxed),
            misses_files,
            misses_bytes: self.misses_bytes.load(Ordering::Relaxed),
            ..Default::default()
        }
    }
}

impl SharedCasCache {
    /// Opens the daemon's directory at `root`. It must already exist: the daemon creates it.
    pub fn new(root: PathBuf, copy_policy: CopyPolicy) -> anyhow::Result<Self> {
        if !root.is_absolute() {
            return Err(anyhow::anyhow!(
                "`cas_shared_cache` must be an absolute path, got `{}`",
                root.display()
            ));
        }
        let blobs_dir = root.join("blobs");
        if !blobs_dir.is_dir() {
            return Err(anyhow::anyhow!(
                "`{}` does not exist; is buck2-casd running with `--dir {}`?",
                blobs_dir.display(),
                root.display()
            ));
        }
        Ok(Self {
            blobs_dir,
            copy_policy,
            reflink_unsupported: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn copy_policy(&self) -> CopyPolicy {
        self.copy_policy
    }

    /// Where the daemon keeps a blob with this digest, whether or not it is present.
    fn blob_path(&self, digest: &TDigest) -> anyhow::Result<PathBuf> {
        let hash = digest.hash.as_str();
        // The hash becomes a path component, so refuse anything that is not plain hex.
        if hash.len() < 2 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(anyhow::anyhow!(
                "Refusing to use non-hex digest hash `{hash}` as a shared cache key"
            ));
        }
        if digest.size_in_bytes < 0 {
            return Err(anyhow::anyhow!("Digest `{digest}` has a negative size"));
        }
        Ok(self
            .blobs_dir
            .join(&hash[..2])
            .join(format!("{}-{}", hash, digest.size_in_bytes)))
    }

    /// Clones `digest` to `dst` from the daemon's directory if the daemon has it.
    ///
    /// Returns `Ok(true)` on a hit, `Ok(false)` if the blob is not there. Errors are reserved
    /// for a blob that is present but could not be cloned under the configured copy policy.
    pub async fn materialize(
        &self,
        digest: &TDigest,
        dst: &Path,
        executable: bool,
    ) -> anyhow::Result<bool> {
        let blob_path = self.blob_path(digest)?;
        let expected_size = digest.size_in_bytes as u64;
        let dst = dst.to_owned();
        let copy_policy = self.copy_policy;
        let reflink_unsupported = Arc::clone(&self.reflink_unsupported);

        // Plain syscalls; keep them off the async executor.
        tokio::task::spawn_blocking(move || {
            let src = match File::open(&blob_path) {
                Ok(f) => f,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("Error opening `{}`", blob_path.display()));
                }
            };
            let meta = src
                .metadata()
                .with_context(|| format!("Error stat-ing `{}`", blob_path.display()))?;
            if meta.len() != expected_size {
                // The daemon never publishes a partial file, so treat this as absent and let
                // the gRPC path fetch it.
                tracing::warn!(
                    "Shared cache blob `{}` has size {} but the digest says {}; ignoring it",
                    blob_path.display(),
                    meta.len(),
                    expected_size
                );
                return Ok(false);
            }
            link_into(
                &src,
                &blob_path,
                &dst,
                executable,
                copy_policy,
                &reflink_unsupported,
            )?;
            Ok(true)
        })
        .await
        .context("Shared cache task panicked")?
    }
}

fn set_output_permissions(path: &Path, executable: bool) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = if executable { 0o755 } else { 0o644 };
        fs::set_permissions(path, fs::Permissions::from_mode(mode))
            .with_context(|| format!("Error setting permissions on `{}`", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let _ = executable;
        let mut perms = fs::metadata(path)
            .with_context(|| format!("Error stat-ing `{}`", path.display()))?
            .permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        fs::set_permissions(path, perms)
            .with_context(|| format!("Error setting permissions on `{}`", path.display()))?;
    }
    Ok(())
}

/// Puts the contents of the stored blob `src` at `dst` according to the copy policy.
fn link_into(
    src: &File,
    src_path: &Path,
    dst: &Path,
    executable: bool,
    copy_policy: CopyPolicy,
    reflink_unsupported: &AtomicBool,
) -> anyhow::Result<()> {
    let try_reflink = match copy_policy {
        CopyPolicy::Copy => false,
        CopyPolicy::Reflink => true,
        CopyPolicy::Hybrid => !reflink_unsupported.load(Ordering::Relaxed),
    };

    if try_reflink {
        match reflink(src, src_path, dst) {
            Ok(()) => {
                set_output_permissions(dst, executable)?;
                return Ok(());
            }
            Err(e) if matches!(copy_policy, CopyPolicy::Hybrid) && is_reflink_unsupported(&e) => {
                if !reflink_unsupported.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        "Reflinking from the shared CAS cache into `{}` is not supported ({}); \
                         falling back to copying. Blobs will not share disk space. Put the \
                         daemon's directory on the same reflink-capable filesystem as buck-out \
                         to fix this.",
                        dst.display(),
                        e
                    );
                }
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!(
                        "Error reflinking `{}` to `{}` (copy policy is `reflink`; use `hybrid` \
                         to fall back to copying)",
                        src_path.display(),
                        dst.display()
                    )
                });
            }
        }
    }

    fs::copy(src_path, dst).with_context(|| {
        format!(
            "Error copying `{}` to `{}`",
            src_path.display(),
            dst.display()
        )
    })?;
    set_output_permissions(dst, executable)
}

/// Whether a failed reflink means "this filesystem (or this pair of filesystems) cannot do
/// that", as opposed to a genuine I/O error.
fn is_reflink_unsupported(e: &io::Error) -> bool {
    if e.kind() == io::ErrorKind::Unsupported {
        return true;
    }
    let Some(code) = e.raw_os_error() else {
        return false;
    };
    #[cfg(unix)]
    {
        #[cfg(target_os = "macos")]
        if code == libc::ENOTSUP {
            return true;
        }
        matches!(
            code,
            libc::EOPNOTSUPP | libc::EXDEV | libc::EINVAL | libc::ENOSYS | libc::ENOTTY
        )
    }
    #[cfg(not(unix))]
    {
        let _ = code;
        true
    }
}

/// Creates `dst` as a copy-on-write clone of `src`.
#[cfg(target_os = "linux")]
fn reflink(src: &File, _src_path: &Path, dst: &Path) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let dst_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dst)?;
    // SAFETY: FICLONE takes the source file descriptor as its only argument. Both descriptors
    // are open for the duration of the call and the kernel does not retain them.
    let rc = unsafe { libc::ioctl(dst_file.as_raw_fd(), libc::FICLONE as _, src.as_raw_fd()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "macos")]
fn reflink(_src: &File, src_path: &Path, dst: &Path) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    // clonefile refuses to overwrite, so clear the destination first.
    match fs::remove_file(dst) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let src_c = CString::new(src_path.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let dst_c = CString::new(dst.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    // SAFETY: both arguments are valid NUL-terminated strings that outlive the call.
    let rc = unsafe { libc::clonefile(src_c.as_ptr(), dst_c.as_ptr(), 0) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn reflink(_src: &File, _src_path: &Path, _dst: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "reflinks are not supported on this platform",
    ))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn digest(hash: &str, size: i64) -> TDigest {
        TDigest {
            hash: hash.to_owned(),
            size_in_bytes: size,
            ..Default::default()
        }
    }

    /// Stands in for the daemon: creates the directory and publishes a blob into it.
    pub(crate) fn fake_daemon_dir(root: &Path) -> PathBuf {
        let root = root.join("casd");
        fs::create_dir_all(root.join("blobs")).unwrap();
        root
    }

    pub(crate) fn publish(root: &Path, d: &TDigest, data: &[u8]) {
        let dir = root.join("blobs").join(&d.hash[..2]);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(format!("{}-{}", d.hash, d.size_in_bytes)), data).unwrap();
    }

    #[tokio::test]
    async fn test_miss_then_hit() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;
        let root = fake_daemon_dir(work.path());
        let cache = SharedCasCache::new(root.clone(), CopyPolicy::Hybrid)?;
        let d = digest("abcdef", 3);
        let dst = work.path().join("out").join("file");
        fs::create_dir_all(dst.parent().unwrap())?;

        assert!(!cache.materialize(&d, &dst, false).await?);
        assert!(!dst.exists());

        publish(&root, &d, b"xyz");
        assert!(cache.materialize(&d, &dst, true).await?);
        assert_eq!(fs::read(&dst)?, b"xyz");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(fs::metadata(&dst)?.permissions().mode() & 0o777, 0o755);
        }

        // Materializing again over an existing file works and fixes the mode.
        assert!(cache.materialize(&d, &dst, false).await?);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(fs::metadata(&dst)?.permissions().mode() & 0o777, 0o644);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_copy_policy() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;
        let root = fake_daemon_dir(work.path());
        let cache = SharedCasCache::new(root.clone(), CopyPolicy::Copy)?;
        let d = digest("0011", 4);
        publish(&root, &d, b"data");
        let dst = work.path().join("dst");
        assert!(cache.materialize(&d, &dst, false).await?);
        assert_eq!(fs::read(&dst)?, b"data");
        Ok(())
    }

    #[tokio::test]
    async fn test_strict_reflink_policy() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;
        let root = fake_daemon_dir(work.path());
        let cache = SharedCasCache::new(root.clone(), CopyPolicy::Reflink)?;
        let d = digest("1234", 4);
        publish(&root, &d, b"data");
        let dst = work.path().join("dst");
        // On a filesystem with reflink support this simply works; anywhere else the strict
        // policy must refuse to silently degrade to a copy.
        match cache.materialize(&d, &dst, false).await {
            Ok(true) => assert_eq!(fs::read(&dst)?, b"data"),
            Ok(false) => panic!("blob was just published"),
            Err(e) => {
                let msg = format!("{e:#}");
                assert!(msg.contains("copy policy is `reflink`"), "{msg}");
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_wrong_size_is_treated_as_absent() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;
        let root = fake_daemon_dir(work.path());
        let cache = SharedCasCache::new(root.clone(), CopyPolicy::Hybrid)?;
        let d = digest("abcd", 10);
        publish(&root, &d, b"short");
        assert!(!cache.materialize(&d, &work.path().join("a"), false).await?);
        // Buck2 never deletes from the daemon's directory.
        assert!(root.join("blobs/ab/abcd-10").exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_rejects_bad_hash() -> anyhow::Result<()> {
        let work = tempfile::tempdir()?;
        let root = fake_daemon_dir(work.path());
        let cache = SharedCasCache::new(root, CopyPolicy::Hybrid)?;
        for bad in ["", "a", "../../etc/passwd", "zz", "AB/CD"] {
            let d = digest(bad, 1);
            assert!(
                cache
                    .materialize(&d, &work.path().join("x"), false)
                    .await
                    .is_err(),
                "{bad:?} should be rejected"
            );
        }
        Ok(())
    }

    #[test]
    fn test_requires_existing_absolute_dir() {
        assert!(SharedCasCache::new(PathBuf::from("relative/casd"), CopyPolicy::Hybrid).is_err());
        let work = tempfile::tempdir().unwrap();
        assert!(SharedCasCache::new(work.path().join("missing"), CopyPolicy::Hybrid).is_err());
    }

    #[test]
    fn test_counters() {
        let c = SharedCacheCounters::default();
        c.hit(10);
        c.hit(5);
        c.miss(7);
        let s = c.take_stats();
        assert_eq!(s.hits_files, 2);
        assert_eq!(s.hits_bytes, 15);
        assert_eq!(s.misses_files, 1);
        assert_eq!(s.misses_bytes, 7);
        assert_eq!(s.total_cache_lookup_attempts, 3);
    }
}
