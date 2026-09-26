/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! The on-disk blob store. This daemon is its single owner: it is the only process that writes
//! to it or removes from it. Buck2 daemons only ever open blobs for reading, to clone them into
//! their `buck-out`.
//!
//! ```text
//! <root>/blobs/<first two hex chars>/<hash>-<size>   raw blob bytes, mode 0444
//! <root>/tmp/<random>                                 in-flight writes
//! ```
//!
//! Blobs are raw: the file is byte-for-byte the blob, so a reader can reflink the whole file.
//! A blob is hashed before it is renamed into place, so a file that exists under a digest has
//! that digest's content.

use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use anyhow::Context;

use crate::digest::Digest;
use crate::digest::DigestFunction;

#[cfg(unix)]
const BLOB_MODE: u32 = 0o444;

pub struct Store {
    blobs_dir: PathBuf,
    tmp_dir: PathBuf,
    digest_function: DigestFunction,
    max_size_bytes: Option<u64>,
    /// Nominal bytes in `blobs_dir`. Kept exact by inserts and eviction passes.
    total_bytes: AtomicU64,
    /// One lock per digest currently being fetched, so concurrent misses download once.
    in_flight: Mutex<HashMap<Digest, Arc<tokio::sync::Mutex<()>>>>,
    /// Held by the running eviction pass.
    eviction: tokio::sync::Mutex<()>,
}

/// Why a blob could not be committed.
#[derive(Debug)]
pub enum CommitError {
    /// The content does not match the digest it was offered under.
    Mismatch(String),
    Other(anyhow::Error),
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mismatch(m) => write!(f, "{m}"),
            Self::Other(e) => write!(f, "{e:#}"),
        }
    }
}

impl std::error::Error for CommitError {}

impl From<anyhow::Error> for CommitError {
    fn from(e: anyhow::Error) -> Self {
        Self::Other(e)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StoreStats {
    pub total_bytes: u64,
}

impl Store {
    /// Opens the store at `root`, creating it if needed. Leftover temporary files belong to a
    /// previous instance and are removed; the blob directory is scanned once to learn its size.
    pub fn open(
        root: &Path,
        digest_function: DigestFunction,
        max_size_bytes: Option<u64>,
    ) -> anyhow::Result<Self> {
        let blobs_dir = root.join("blobs");
        let tmp_dir = root.join("tmp");
        fs::create_dir_all(&blobs_dir)
            .with_context(|| format!("Error creating `{}`", blobs_dir.display()))?;
        fs::create_dir_all(&tmp_dir)
            .with_context(|| format!("Error creating `{}`", tmp_dir.display()))?;

        for entry in fs::read_dir(&tmp_dir)
            .with_context(|| format!("Error listing `{}`", tmp_dir.display()))?
            .flatten()
        {
            let _ignored = fs::remove_file(entry.path());
        }

        let mut total = 0u64;
        for (_, len, _) in walk_blobs(&blobs_dir) {
            total += len;
        }
        tracing::info!(
            "Opened store at `{}`: {} bytes in blobs, cap {}",
            root.display(),
            total,
            max_size_bytes.map_or("none".to_owned(), |c| c.to_string())
        );

        Ok(Self {
            blobs_dir,
            tmp_dir,
            digest_function,
            max_size_bytes,
            total_bytes: AtomicU64::new(total),
            in_flight: Mutex::new(HashMap::new()),
            eviction: tokio::sync::Mutex::new(()),
        })
    }

    pub fn digest_function(&self) -> DigestFunction {
        self.digest_function
    }

    pub fn stats(&self) -> StoreStats {
        StoreStats {
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
        }
    }

    /// Where a blob with this digest lives, whether or not it is present.
    pub fn blob_path(&self, digest: &Digest) -> PathBuf {
        // `Digest::new` guarantees the hash is plain lowercase hex, so it is safe as a path
        // component.
        self.blobs_dir
            .join(&digest.hash[..2])
            .join(format!("{}-{}", digest.hash, digest.size))
    }

    pub fn new_tmp_path(&self) -> PathBuf {
        self.tmp_dir.join(uuid::Uuid::new_v4().to_string())
    }

    /// Returns the blob's path if it is present, touching it for LRU purposes.
    pub async fn lookup(&self, digest: &Digest) -> anyhow::Result<Option<PathBuf>> {
        let path = self.blob_path(digest);
        let expected = digest.size as u64;
        blocking(move || match fs::File::open(&path) {
            Ok(file) => {
                let meta = file.metadata()?;
                if meta.len() != expected {
                    // Cannot happen through this daemon; something else damaged the store.
                    tracing::warn!(
                        "Blob `{}` has {} bytes on disk, expected {}; dropping it",
                        path.display(),
                        meta.len(),
                        expected
                    );
                    let _ignored = fs::remove_file(&path);
                    return Ok(None);
                }
                let _ignored = file.set_modified(SystemTime::now());
                Ok(Some(path))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("Error opening `{}`", path.display())),
        })
        .await
    }

    /// Moves a fully written temporary file into the store, after checking that its size and
    /// hash are those of `digest`. On any failure the temporary file is removed.
    pub async fn commit(&self, digest: &Digest, tmp: PathBuf) -> Result<(), CommitError> {
        let final_path = self.blob_path(digest);
        let size = digest.size as u64;
        let digest = digest.clone();
        let function = self.digest_function;
        let result = blocking(move || {
            let outcome = (|| -> Result<(), CommitError> {
                let meta = fs::metadata(&tmp)
                    .with_context(|| format!("Error stat-ing `{}`", tmp.display()))?;
                if meta.len() != digest.size as u64 {
                    return Err(CommitError::Mismatch(format!(
                        "Blob offered as `{digest}` has {} bytes",
                        meta.len()
                    )));
                }
                let actual = function
                    .hash_file(&tmp)
                    .with_context(|| format!("Error hashing `{}`", tmp.display()))?;
                if actual != digest.hash {
                    return Err(CommitError::Mismatch(format!(
                        "Blob offered as `{digest}` hashes to `{actual}`"
                    )));
                }
                set_blob_mode(&tmp)?;
                if let Some(parent) = final_path.parent() {
                    fs::create_dir_all(parent)
                        .with_context(|| format!("Error creating `{}`", parent.display()))?;
                }
                // A concurrent commit of the same digest renames identical content over it.
                fs::rename(&tmp, &final_path).with_context(|| {
                    format!(
                        "Error moving `{}` to `{}`",
                        tmp.display(),
                        final_path.display()
                    )
                })?;
                Ok(())
            })();
            if outcome.is_err() {
                let _ignored = fs::remove_file(&tmp);
            }
            Ok(outcome)
        })
        .await;
        match result {
            Ok(Ok(())) => {
                self.total_bytes.fetch_add(size, Ordering::Relaxed);
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(e) => Err(CommitError::Other(e)),
        }
    }

    /// Stores an in-memory blob.
    pub async fn insert_bytes(&self, digest: &Digest, data: Vec<u8>) -> Result<(), CommitError> {
        let tmp = self.new_tmp_path();
        let write_to = tmp.clone();
        blocking(move || {
            fs::write(&write_to, data)
                .with_context(|| format!("Error writing `{}`", write_to.display()))
        })
        .await?;
        self.commit(digest, tmp).await
    }

    /// Returns the blob's path, fetching it with `fetch` if it is not present. Concurrent calls
    /// for the same digest fetch once. `fetch` receives the temporary path to write the blob to
    /// and returns whether the blob exists at all.
    pub async fn ensure_local<F, Fut>(
        &self,
        digest: &Digest,
        fetch: F,
    ) -> anyhow::Result<Option<PathBuf>>
    where
        F: FnOnce(PathBuf) -> Fut,
        Fut: Future<Output = anyhow::Result<bool>>,
    {
        if let Some(path) = self.lookup(digest).await? {
            return Ok(Some(path));
        }

        let lock = {
            let mut in_flight = self.in_flight.lock().unwrap();
            Arc::clone(
                in_flight
                    .entry(digest.clone())
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
            )
        };
        let result = {
            let _guard = lock.lock().await;
            match self.lookup(digest).await? {
                // Someone else fetched it while we waited for the lock.
                Some(path) => Ok(Some(path)),
                None => {
                    let tmp = self.new_tmp_path();
                    match fetch(tmp.clone()).await {
                        Ok(true) => match self.commit(digest, tmp).await {
                            Ok(()) => Ok(Some(self.blob_path(digest))),
                            Err(CommitError::Mismatch(m)) => Err(anyhow::anyhow!(
                                "Upstream returned wrong content for `{digest}`: {m}"
                            )),
                            Err(CommitError::Other(e)) => Err(e),
                        },
                        Ok(false) => {
                            let _ignored = fs::remove_file(&tmp);
                            Ok(None)
                        }
                        Err(e) => {
                            let _ignored = fs::remove_file(&tmp);
                            Err(e)
                        }
                    }
                }
            }
        };
        {
            let mut in_flight = self.in_flight.lock().unwrap();
            // Drop the entry once nobody else is queued on it.
            if Arc::strong_count(&lock) == 2 {
                in_flight.remove(digest);
            }
        }
        result
    }

    /// Whether a blob is present, for tests that only have the raw digest parts.
    pub async fn lookup_for_test(&self, hash: &str, size: i64) -> anyhow::Result<bool> {
        let digest = Digest::new(hash, size, self.digest_function)?;
        Ok(self.lookup(&digest).await?.is_some())
    }

    /// Whether the store is over its cap.
    pub fn over_cap(&self) -> bool {
        match self.max_size_bytes {
            Some(cap) => self.total_bytes.load(Ordering::Relaxed) > cap,
            None => false,
        }
    }

    /// Removes least recently used blobs until the store is a tenth below its cap. Returns the
    /// number of blobs removed. Only one pass runs at a time; a second caller returns at once.
    ///
    /// Sizes are nominal: a blob that a buck2 daemon has reflinked shares its extents with that
    /// clone, so removing it here frees the space only once the clone is gone too.
    pub async fn evict(&self) -> anyhow::Result<u64> {
        let Some(cap) = self.max_size_bytes else {
            return Ok(0);
        };
        let Ok(_guard) = self.eviction.try_lock() else {
            return Ok(0);
        };
        if self.total_bytes.load(Ordering::Relaxed) <= cap {
            return Ok(0);
        }
        let blobs_dir = self.blobs_dir.clone();
        let (removed, total) = blocking(move || {
            let mut entries = walk_blobs(&blobs_dir);
            let mut total: u64 = entries.iter().map(|e| e.1).sum();
            let target = cap - cap / 10;
            entries.sort_by_key(|e| e.0);
            let mut removed = 0u64;
            for (_, len, path) in entries {
                if total <= target {
                    break;
                }
                match fs::remove_file(&path) {
                    Ok(()) => {
                        total -= len;
                        removed += 1;
                    }
                    Err(e) => tracing::warn!("Error removing `{}`: {}", path.display(), e),
                }
            }
            Ok((removed, total))
        })
        .await?;
        self.total_bytes.store(total, Ordering::Relaxed);
        if removed > 0 {
            tracing::info!("Evicted {removed} blobs; store is now {total} bytes (cap {cap})");
        }
        Ok(removed)
    }
}

/// Every blob in the store as (mtime, size, path).
fn walk_blobs(blobs_dir: &Path) -> Vec<(SystemTime, u64, PathBuf)> {
    let mut out = Vec::new();
    let Ok(shards) = fs::read_dir(blobs_dir) else {
        return out;
    };
    for shard in shards.flatten() {
        let Ok(blobs) = fs::read_dir(shard.path()) else {
            continue;
        };
        for blob in blobs.flatten() {
            let Ok(meta) = blob.metadata() else {
                continue;
            };
            if meta.is_file() {
                let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                out.push((mtime, meta.len(), blob.path()));
            }
        }
    }
    out
}

fn set_blob_mode(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(BLOB_MODE))
            .with_context(|| format!("Error setting permissions on `{}`", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let mut perms = fs::metadata(path)?.permissions();
        perms.set_readonly(true);
        fs::set_permissions(path, perms)?;
    }
    Ok(())
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .context("Store task panicked")?
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use super::*;

    fn digest_of(data: &[u8]) -> Digest {
        Digest::new(
            &DigestFunction::Sha256.hash_bytes(data),
            data.len() as i64,
            DigestFunction::Sha256,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_insert_lookup_and_layout() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(dir.path(), DigestFunction::Sha256, None)?;
        let d = digest_of(b"hello");
        assert!(store.lookup(&d).await?.is_none());
        store.insert_bytes(&d, b"hello".to_vec()).await?;
        let path = store.lookup(&d).await?.expect("present");
        assert_eq!(
            path,
            dir.path()
                .join("blobs")
                .join(&d.hash[..2])
                .join(format!("{}-5", d.hash))
        );
        assert_eq!(fs::read(&path)?, b"hello");
        assert_eq!(store.stats().total_bytes, 5);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(fs::metadata(&path)?.permissions().mode() & 0o777, 0o444);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_rejects_wrong_content() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(dir.path(), DigestFunction::Sha256, None)?;
        let d = digest_of(b"hello");
        match store.insert_bytes(&d, b"jello".to_vec()).await {
            Err(CommitError::Mismatch(_)) => {}
            other => panic!("expected mismatch, got {other:?}"),
        }
        let wrong_size = Digest::new(&d.hash, 4, DigestFunction::Sha256)?;
        match store.insert_bytes(&wrong_size, b"hello".to_vec()).await {
            Err(CommitError::Mismatch(_)) => {}
            other => panic!("expected mismatch, got {other:?}"),
        }
        assert!(store.lookup(&d).await?.is_none());
        assert_eq!(fs::read_dir(dir.path().join("tmp"))?.count(), 0);
        assert_eq!(store.stats().total_bytes, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_ensure_local_coalesces_fetches() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Arc::new(Store::open(dir.path(), DigestFunction::Sha256, None)?);
        let d = digest_of(b"shared");
        let fetches = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let store = Arc::clone(&store);
            let d = d.clone();
            let fetches = Arc::clone(&fetches);
            tasks.push(tokio::spawn(async move {
                store
                    .ensure_local(&d, |tmp| async move {
                        fetches.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        tokio::fs::write(&tmp, b"shared").await?;
                        Ok(true)
                    })
                    .await
            }));
        }
        for t in tasks {
            assert!(t.await??.is_some());
        }
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        assert!(store.in_flight.lock().unwrap().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_ensure_local_missing_upstream() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(dir.path(), DigestFunction::Sha256, None)?;
        let d = digest_of(b"nope");
        let got = store.ensure_local(&d, |_tmp| async { Ok(false) }).await?;
        assert!(got.is_none());
        assert_eq!(fs::read_dir(dir.path().join("tmp"))?.count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_eviction_is_lru() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(dir.path(), DigestFunction::Sha256, Some(100))?;
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut digests = Vec::new();
        for i in 0..10u8 {
            let data = vec![i; 20];
            let d = digest_of(&data);
            store.insert_bytes(&d, data).await?;
            fs::File::open(store.blob_path(&d))?
                .set_modified(base + Duration::from_secs(i as u64))?;
            digests.push(d);
        }
        assert!(store.over_cap());
        let removed = store.evict().await?;
        assert_eq!(removed, 6);
        assert!(!store.over_cap());
        assert_eq!(store.stats().total_bytes, 80);
        for (i, d) in digests.iter().enumerate() {
            assert_eq!(store.blob_path(d).exists(), i >= 6, "blob {i}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_open_sweeps_tmp_and_counts_blobs() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        {
            let store = Store::open(dir.path(), DigestFunction::Sha256, None)?;
            store
                .insert_bytes(&digest_of(b"abc"), b"abc".to_vec())
                .await?;
            fs::write(store.new_tmp_path(), b"partial")?;
        }
        let store = Store::open(dir.path(), DigestFunction::Sha256, None)?;
        assert_eq!(store.stats().total_bytes, 3);
        assert_eq!(fs::read_dir(dir.path().join("tmp"))?.count(), 0);
        Ok(())
    }
}
