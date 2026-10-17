/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! `buck2 suspend` and resuming from what it wrote (`[hydration] suspend = true`).
//!
//! A suspend writes, under `buck-out/<isolation>/cache/suspend/`, an image of the daemon's graph
//! at its current version, the artifacts its materializer tracks, and the file watcher's last
//! snapshot of the files, beside the paging store (`cache/dice_state`) that holds the values. The
//! `MANIFEST`, written last, records everything a later daemon must match for the image to mean
//! what it meant: the binary, the startup config, the environment, the root buckconfig, the
//! project, the host and the user. A daemon that starts with the setting on and finds a matching
//! image resumes from it; every other start deletes it, so an image is used at most once and never
//! over state another daemon changed.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use allocative::Allocative;
use buck2_common::init::SuspendConfig;
use buck2_common::legacy_configs::configs::LegacyBuckConfig;
use buck2_common::legacy_configs::key::BuckconfigKeyRef;
use buck2_core::fs::project::ProjectRoot;
use buck2_error::BuckErrorContext;
use buck2_error::ErrorTag;
use buck2_error::buck2_error;
use buck2_execute::materialize::materializer::Materializer;
use buck2_execute::materialize::materializer::ResumedArtifacts;
use buck2_execute::materialize::materializer::SuspendedArtifact;
use buck2_file_watcher::file_watcher::FileWatcher;
use buck2_file_watcher::file_watcher::FileWatcherImage;
use dice::Dice;
use dice::DiceTransaction;
use dice::ResumeStats;
use dice::SuspendRow;
use serde::Deserialize;
use serde::Serialize;

/// The directory under the tenant's cache dir that holds an image.
pub(crate) const IMAGE_DIR: &str = "suspend";
/// The paging store's directory under the cache dir (`TenantPaths::dice_state_path`).
pub(crate) const DICE_STATE_DIR: &str = "dice_state";
const MANIFEST: &str = "MANIFEST";
const MANIFEST_CONSUMED: &str = "MANIFEST.consumed";
const IMAGE: &str = "image.bin";
const MAGIC: &str = "BUCK2SUS";
/// Bumped on any change to [`Manifest`] or [`ImageFile`].
const FORMAT: u32 = 1;

/// Environment variables that differ between two runs of the same job and that nothing a daemon
/// computes depends on, left out of the environment digest; a name ending in `*` is a prefix.
const PER_RUN_ENV: &[&str] = &[
    "_",
    "BUCK2_DAEMON_UUID",
    "BUCK_WRAPPER_START_TIME_MS",
    "BUCK_WRAPPER_UUID",
    "BUILDBUDDY_INVOCATION_ID",
    "COLUMNS",
    "GITHUB_*",
    "INVOCATION_ID",
    "LINES",
    "OLDPWD",
    "PWD",
    "RUNNER_*",
    "SHLVL",
    "SSH_*",
    "TERM",
    "TMUX*",
    "WINDOWID",
];

fn blake3_hex(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

fn ignored(name: &str, extra: &[String]) -> bool {
    PER_RUN_ENV.iter().any(|p| match p.strip_suffix('*') {
        Some(prefix) => name.starts_with(prefix),
        None => name == *p,
    }) || extra.iter().any(|e| e == name)
}

/// The digest of the environment, and a short digest of each variable's value by name, so that
/// a refusal can name what changed without recording any value.
fn env_digest(extra_ignored: &[String]) -> (String, BTreeMap<String, String>) {
    let mut vars: Vec<(String, String)> = std::env::vars_os()
        .map(|(k, v)| {
            (
                k.to_string_lossy().into_owned(),
                v.to_string_lossy().into_owned(),
            )
        })
        .filter(|(k, _)| !ignored(k, extra_ignored))
        .collect();
    vars.sort();
    let mut hasher = blake3::Hasher::new();
    let mut by_name = BTreeMap::new();
    for (k, v) in &vars {
        hasher.update(k.as_bytes());
        hasher.update(&[0]);
        hasher.update(v.as_bytes());
        hasher.update(&[0]);
        by_name.insert(k.clone(), blake3_hex(v.as_bytes())[..16].to_owned());
    }
    (hasher.finalize().to_hex().to_string(), by_name)
}

/// The digest of a resolved buckconfig, which the daemon read once at startup and DICE does not
/// track (ignore specs, digest algorithms, materializer settings and the like).
fn config_digest(config: &LegacyBuckConfig) -> String {
    let mut hasher = blake3::Hasher::new();
    for (section, values) in config.iter() {
        hasher.update(section.as_bytes());
        hasher.update(&[0]);
        for (key, value) in values {
            hasher.update(key.as_bytes());
            hasher.update(&[0]);
            hasher.update(value.as_bytes());
            hasher.update(&[0]);
        }
        hasher.update(&[1]);
    }
    hasher.finalize().to_hex().to_string()
}

fn exe_digest() -> buck2_error::Result<String> {
    let exe = std::env::current_exe().buck_error_context("Locating the daemon executable")?;
    let mut hasher = blake3::Hasher::new();
    let mut file = std::fs::File::open(&exe)
        .with_buck_error_context(|| format!("Opening `{}`", exe.display()))?;
    std::io::copy(&mut file, &mut hasher)
        .with_buck_error_context(|| format!("Reading `{}`", exe.display()))?;
    Ok(hasher.finalize().to_hex().to_string())
}

fn hostname() -> String {
    let mut buf = [0u8; 256];
    // SAFETY: the buffer is valid for its length.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return String::new();
    }
    let len = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..len]).into_owned()
}

fn uid() -> u32 {
    // SAFETY: getuid cannot fail.
    unsafe { libc::getuid() }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn dir_size(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|e| match e.metadata() {
            Ok(m) if m.is_dir() => dir_size(&e.path()),
            Ok(m) => m.len(),
            Err(_) => 0,
        })
        .sum()
}

/// Where an image's MANIFEST lives under a tenant's cache dir.
pub(crate) fn manifest_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join(IMAGE_DIR).join(MANIFEST)
}

/// What a daemon must match for an image to apply to it. Everything but the binary is known
/// at startup; the binary is hashed when needed.
#[derive(Allocative, Clone, Debug)]
pub(crate) struct SuspendIdentity {
    #[allocative(skip)]
    pub(crate) config: SuspendConfig,
    startup_config: String,
    root_config: String,
    project_root: String,
    isolation: String,
    /// `buck2.file_watcher` as this daemon resolved it.
    pub(crate) watcher: String,
}

impl SuspendIdentity {
    pub(crate) fn new(
        config: SuspendConfig,
        startup_config: &buck2_common::init::DaemonStartupConfig,
        root_config: &LegacyBuckConfig,
        project_root: &ProjectRoot,
        isolation: &str,
    ) -> buck2_error::Result<Self> {
        if std::env::var_os("BUCK2_DICE_DB_PATH").is_some() {
            return Err(buck2_error!(
                ErrorTag::Input,
                "`hydration.suspend` keeps the paging store under the isolation dir, so it \
                 cannot be used with `BUCK2_DICE_DB_PATH`"
            ));
        }
        Ok(Self {
            config,
            startup_config: blake3_hex(startup_config.serialize()?.as_bytes()),
            root_config: config_digest(root_config),
            project_root: project_root.root().to_string(),
            isolation: isolation.to_owned(),
            watcher: root_config
                .get(BuckconfigKeyRef {
                    section: "buck2",
                    property: "file_watcher",
                })
                .unwrap_or("notify")
                .to_owned(),
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Manifest {
    magic: String,
    format: u32,
    exe: String,
    startup_config: String,
    env: String,
    /// Each variable's short value digest, by name.
    env_vars: BTreeMap<String, String>,
    root_config: String,
    project_root: String,
    isolation: String,
    hostname: String,
    uid: u32,
    watcher: String,
    created_at_ms: u64,
    image_len: u64,
    image: String,
    dice_state_bytes: u64,
}

#[derive(Serialize, Deserialize)]
struct ImageFile {
    dice: Vec<u8>,
    artifacts: SuspendRow,
    artifact_count: usize,
    watcher: FileWatcherImage,
}

/// What a start found and did about an image, for `buck2 resume` and the logs.
#[derive(Clone, Debug, Allocative)]
pub(crate) enum ResumeOutcome {
    /// No image was found.
    NoImage,
    Resumed {
        #[allocative(skip)]
        stats: ResumeStats,
        #[allocative(skip)]
        artifacts: ResumedArtifacts,
        #[allocative(skip)]
        timings: Vec<(&'static str, Duration)>,
    },
    /// An image was found and not used: the start is cold.
    Cold { reason: String },
}

impl ResumeOutcome {
    pub(crate) fn describe(&self) -> String {
        match self {
            ResumeOutcome::NoImage => "cold: no suspended state".to_owned(),
            ResumeOutcome::Cold { reason } => format!("cold: {reason}"),
            ResumeOutcome::Resumed {
                stats,
                artifacts,
                timings,
            } => {
                let mut out = format!(
                    "resumed: {} keys, {} asserted, {} valid, {} candidates, {} not installed; \
                     artifacts: {} reactivated, {} declared again",
                    stats.keys,
                    stats.asserted,
                    stats.certified,
                    stats.candidates,
                    stats.dropped,
                    artifacts.reactivated,
                    artifacts.redeclared
                );
                for (step, d) in timings {
                    out.push_str(&format!("; {step} {:.3}s", d.as_secs_f64()));
                }
                out
            }
        }
    }

    pub(crate) fn is_resumed(&self) -> bool {
        matches!(self, ResumeOutcome::Resumed { .. })
    }
}

/// An image that passed every check, ready to replay.
pub(crate) struct LoadedImage {
    image: ImageFile,
    image_dir: PathBuf,
    loaded_in: Duration,
}

/// What a starting daemon does about `cache/suspend/`.
pub(crate) enum ResumePrep {
    /// Nothing to resume; the cache wipe deletes whatever is there.
    None,
    /// An image that must not be used, and why; the cache wipe deletes it.
    Refused(String),
    Ready(LoadedImage),
}

/// Reads and checks the image a suspended daemon left, before anything else touches the cache.
/// The MANIFEST is renamed first, so a crash from here on leaves no image to resume twice.
pub(crate) fn prepare_resume(cache_dir: &Path, identity: &SuspendIdentity) -> ResumePrep {
    let start = Instant::now();
    let image_dir = cache_dir.join(IMAGE_DIR);
    let manifest_path = image_dir.join(MANIFEST);
    let manifest_bytes = match std::fs::read(&manifest_path) {
        Ok(bytes) => bytes,
        Err(_) => return ResumePrep::None,
    };
    if std::fs::rename(&manifest_path, image_dir.join(MANIFEST_CONSUMED)).is_err() {
        return ResumePrep::Refused("the MANIFEST could not be claimed".to_owned());
    }
    match check(&manifest_bytes, &image_dir, cache_dir, identity) {
        Ok(image) => ResumePrep::Ready(LoadedImage {
            image,
            image_dir,
            loaded_in: start.elapsed(),
        }),
        Err(reason) => ResumePrep::Refused(reason),
    }
}

fn check(
    manifest_bytes: &[u8],
    image_dir: &Path,
    cache_dir: &Path,
    identity: &SuspendIdentity,
) -> Result<ImageFile, String> {
    let m: Manifest =
        serde_json::from_slice(manifest_bytes).map_err(|e| format!("unreadable MANIFEST: {e}"))?;
    if m.magic != MAGIC || m.format != FORMAT {
        return Err(format!("image format {} {}", m.magic, m.format));
    }
    let refuse = |what: &str| Err(format!("{what} differs from the suspended daemon's"));
    if m.exe != exe_digest().map_err(|e| format!("{e:#}"))? {
        return refuse("the buck2 binary");
    }
    if m.startup_config != identity.startup_config {
        return refuse("the daemon startup config");
    }
    let (env, vars) = env_digest(&identity.config.env_ignore);
    if m.env != env {
        let mut changed: Vec<&str> = vars
            .iter()
            .filter(|(k, v)| m.env_vars.get(*k) != Some(*v))
            .map(|(k, _)| k.as_str())
            .chain(
                m.env_vars
                    .keys()
                    .filter(|k| !vars.contains_key(*k))
                    .map(|k| k.as_str()),
            )
            .collect();
        changed.sort();
        changed.dedup();
        return Err(format!(
            "the environment differs from the suspended daemon's: {}",
            changed.join(", ")
        ));
    }
    if m.root_config != identity.root_config {
        return refuse("the root buckconfig");
    }
    if m.project_root != identity.project_root || m.isolation != identity.isolation {
        return refuse("the project root or isolation dir");
    }
    if m.hostname != hostname() {
        return refuse("the hostname");
    }
    if m.uid != uid() {
        return refuse("the user");
    }
    if m.watcher != identity.watcher {
        return refuse("the file watcher");
    }
    if now_ms() < m.created_at_ms {
        return Err("the clock is earlier than the suspend".to_owned());
    }
    let max = identity.config.max_db_gb.saturating_mul(1 << 30);
    if m.dice_state_bytes > max {
        return Err(format!(
            "the paging store is {} bytes, over `buck2_hydration.suspend_max_db_gb`",
            m.dice_state_bytes
        ));
    }
    if !cache_dir.join(DICE_STATE_DIR).is_dir() {
        return Err("the paging store is missing".to_owned());
    }
    let bytes =
        std::fs::read(image_dir.join(IMAGE)).map_err(|e| format!("unreadable image: {e}"))?;
    if bytes.len() as u64 != m.image_len || blake3_hex(&bytes) != m.image {
        return Err("the image is truncated or corrupt".to_owned());
    }
    let (image, _): (ImageFile, usize) =
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
            .map_err(|e| format!("undecodable image: {e}"))?;
    Ok(image)
}

impl LoadedImage {
    pub(crate) fn watcher(&self) -> &FileWatcherImage {
        &self.image.watcher
    }

    /// Replays the image: the materializer's artifacts first, so that every output the graph
    /// names is known before anything asks for it, then the graph onto `root`. A failure leaves
    /// the graph untouched, for the caller to start cold.
    pub(crate) async fn restore(
        &self,
        dice: &Arc<Dice>,
        root: dice::BranchId,
        materializer: &dyn Materializer,
    ) -> Result<(ResumeStats, ResumedArtifacts, Vec<(&'static str, Duration)>), String> {
        let mut timings = vec![("load", self.loaded_in)];
        let t = Instant::now();
        let artifacts: Vec<SuspendedArtifact> =
            dice.read_suspend_row(self.image.artifacts)
                .await
                .map_err(|e| format!("unreadable materializer state: {e:#}"))?;
        let resumed = materializer
            .resume_artifacts(artifacts)
            .await
            .map_err(|e| format!("materializer: {e:#}"))?;
        timings.push(("materializer", t.elapsed()));
        if resumed.lost > 0 {
            return Err(format!(
                "{} materialized artifacts are no longer in the materializer's state",
                resumed.lost
            ));
        }
        let t = Instant::now();
        let stats = dice
            .restore_image(root, &self.image.dice)
            .await
            .map_err(|e| format!("graph: {e:#}"))?;
        timings.push(("graph", t.elapsed()));
        Ok((stats, resumed, timings))
    }

    /// The image is single-use: once replayed, or refused, it goes.
    pub(crate) fn discard(&self) {
        drop(std::fs::remove_dir_all(&self.image_dir));
    }
}

fn write_durably(path: &Path, bytes: &[u8]) -> buck2_error::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut file = std::fs::File::create(&tmp)
        .with_buck_error_context(|| format!("Creating `{}`", tmp.display()))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

/// Drops `path`'s pages from the page cache, so a VM snapshot taken after the suspend need not
/// carry them; the data is on disk.
fn drop_page_cache(path: &Path) {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        if path.is_dir() {
            if let Ok(entries) = std::fs::read_dir(path) {
                for e in entries.filter_map(Result::ok) {
                    drop_page_cache(&e.path());
                }
            }
        } else if let Ok(file) = std::fs::File::open(path) {
            // SAFETY: the fd is open for the call.
            unsafe {
                libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = path;
}

/// Flushes every dirty page of the filesystem holding `path` (`syncfs`): the outputs the
/// materializer records as on disk must be there after a crash, or a resumed daemon would trust
/// files that are not.
fn sync_filesystem(path: &Path) -> buck2_error::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let dir = std::fs::File::open(path)?;
        // SAFETY: the fd is open for the call.
        if unsafe { libc::syncfs(dir.as_raw_fd()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = path;
    Ok(())
}

/// Everything `buck2 suspend` needs from the daemon.
pub(crate) struct SuspendInputs<'a> {
    pub(crate) identity: &'a SuspendIdentity,
    pub(crate) dice: &'a Arc<Dice>,
    pub(crate) transaction: &'a DiceTransaction,
    pub(crate) file_watcher: &'a dyn FileWatcher,
    pub(crate) materializer: &'a dyn Materializer,
    pub(crate) project_root: &'a ProjectRoot,
    pub(crate) cache_dir: PathBuf,
    pub(crate) buck_out: PathBuf,
}

/// Writes the image. Before the MANIFEST is written nothing is lost by failing: the partial image
/// is deleted and the daemon keeps running. Returns the report `buck2 suspend` prints.
pub(crate) async fn suspend(inputs: SuspendInputs<'_>) -> buck2_error::Result<String> {
    let image_dir = inputs.cache_dir.join(IMAGE_DIR);
    let result = suspend_inner(&inputs, &image_dir).await;
    if result.is_err() {
        drop(std::fs::remove_dir_all(&image_dir));
    }
    result
}

async fn suspend_inner(
    inputs: &SuspendInputs<'_>,
    image_dir: &Path,
) -> buck2_error::Result<String> {
    let mut timings: Vec<(&'static str, Duration)> = Vec::new();
    let mut step = Instant::now();
    let mut lap = |name: &'static str, timings: &mut Vec<(&'static str, Duration)>| {
        timings.push((name, step.elapsed()));
        step = Instant::now();
    };

    drop(std::fs::remove_dir_all(image_dir));
    std::fs::create_dir_all(image_dir)?;

    // The files first: an edit made after this snapshot statted a file differs from it at
    // resume, and one made before is in the graph or fails the suspend.
    let watcher = inputs
        .file_watcher
        .suspend_image(inputs.project_root)
        .await?;
    lap("files", &mut timings);

    let artifacts = inputs.materializer.suspend_artifacts().await?;
    let artifact_count = artifacts.len();
    let artifacts_row = inputs
        .dice
        .write_suspend_row(&artifacts)
        .map_err(|e| buck2_error!(ErrorTag::Tier0, "writing the materializer state: {e:#}"))?;
    drop(artifacts);
    lap("materializer", &mut timings);

    let dice_state = inputs.cache_dir.join(DICE_STATE_DIR);
    let free = free_bytes(&inputs.cache_dir);
    let used = dir_size(&dice_state);
    if let Some(free) = free
        && free < used / 2 + (1 << 30)
    {
        return Err(buck2_error!(
            ErrorTag::Environment,
            "{free} bytes free beside a {used}-byte paging store is too little to suspend into"
        ));
    }

    let image = inputs
        .dice
        .suspend_image(inputs.transaction.version())
        .await
        .map_err(|e| buck2_error!(ErrorTag::Tier0, "writing the graph image: {e:#}"))?;
    lap("graph", &mut timings);

    let file = ImageFile {
        dice: image.bytes,
        artifacts: artifacts_row,
        artifact_count,
        watcher,
    };
    let bytes = bincode::serde::encode_to_vec(&file, bincode::config::standard())
        .map_err(|e| buck2_error!(ErrorTag::Tier0, "encoding the image: {e}"))?;
    write_durably(&image_dir.join(IMAGE), &bytes)?;
    sync_filesystem(&inputs.buck_out)?;
    lap("sync", &mut timings);

    let (env, env_vars) = env_digest(&inputs.identity.config.env_ignore);
    let dice_state_bytes = dir_size(&dice_state);
    let manifest = Manifest {
        magic: MAGIC.to_owned(),
        format: FORMAT,
        exe: exe_digest()?,
        startup_config: inputs.identity.startup_config.clone(),
        env,
        env_vars,
        root_config: inputs.identity.root_config.clone(),
        project_root: inputs.identity.project_root.clone(),
        isolation: inputs.identity.isolation.clone(),
        hostname: hostname(),
        uid: uid(),
        watcher: inputs.identity.watcher.clone(),
        created_at_ms: now_ms(),
        image_len: bytes.len() as u64,
        image: blake3_hex(&bytes),
        dice_state_bytes,
    };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|e| buck2_error!(ErrorTag::Tier0, "encoding the MANIFEST: {e}"))?;
    write_durably(&image_dir.join(MANIFEST), &manifest_bytes)?;
    drop_page_cache(image_dir);
    drop_page_cache(&dice_state);
    lap("manifest", &mut timings);

    let stats = &image.stats;
    let mut out = String::new();
    out.push_str(&format!(
        "suspended: image {} bytes, paging store {} bytes, {} artifacts\n",
        bytes.len(),
        dice_state_bytes,
        artifact_count
    ));
    out.push_str(&format!(
        "graph: {} keys, {} asserted, {} certificates ({} candidates), {} values written now, \
         {} keys left out\n",
        stats.keys,
        stats.asserted,
        stats.certified,
        stats.candidates,
        stats.rows_written,
        stats.dropped_total()
    ));
    for ((cause, key_type), n) in &stats.dropped {
        out.push_str(&format!(
            "  left out: {n} {key_type} ({})\n",
            cause.as_str()
        ));
    }
    out.push_str("times:");
    for (name, d) in &timings {
        out.push_str(&format!(" {name} {:.3}s", d.as_secs_f64()));
    }
    out.push('\n');
    Ok(out)
}

fn free_bytes(path: &Path) -> Option<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        // SAFETY: valid path and out pointer.
        if unsafe { libc::statvfs(c.as_ptr(), &mut stat) } != 0 {
            return None;
        }
        Some(stat.f_bavail as u64 * stat.f_frsize as u64)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_run_variables_are_left_out() {
        assert!(ignored("GITHUB_RUN_ID", &[]));
        assert!(ignored("TERM", &[]));
        assert!(!ignored("PATH", &[]));
        assert!(ignored("CI_JOB_ID", &["CI_JOB_ID".to_owned()]));
    }

    #[test]
    fn config_digest_follows_values() -> buck2_error::Result<()> {
        use buck2_common::legacy_configs::configs::testing::parse;
        let a = parse(&[("c", "[s]\nk = 1\n")], "c")?;
        let b = parse(&[("c", "[s]\nk = 1\n")], "c")?;
        let c = parse(&[("c", "[s]\nk = 2\n")], "c")?;
        assert_eq!(config_digest(&a), config_digest(&b));
        assert_ne!(config_digest(&a), config_digest(&c));
        Ok(())
    }

    #[test]
    fn manifest_round_trips() {
        let m = Manifest {
            magic: MAGIC.to_owned(),
            format: FORMAT,
            exe: "e".to_owned(),
            startup_config: "s".to_owned(),
            env: "v".to_owned(),
            env_vars: BTreeMap::from([("PATH".to_owned(), "x".to_owned())]),
            root_config: "r".to_owned(),
            project_root: "/p".to_owned(),
            isolation: "v2".to_owned(),
            hostname: "h".to_owned(),
            uid: 1,
            watcher: "notify".to_owned(),
            created_at_ms: 2,
            image_len: 3,
            image: "i".to_owned(),
            dice_state_bytes: 4,
        };
        let back: Manifest = serde_json::from_slice(&serde_json::to_vec(&m).unwrap()).unwrap();
        assert_eq!(back.env_vars, m.env_vars);
        assert_eq!(back.created_at_ms, 2);
    }
}
