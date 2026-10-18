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
//! project, the host identity (`hydration.host_identity`, by default the hostname) and the user. A daemon that starts with the setting on and finds a matching
//! image resumes from it; every other start deletes it, so an image is used at most once and never
//! over state another daemon changed.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use allocative::Allocative;
use buck2_common::init::DaemonStartupConfig;
use buck2_common::init::HOST_IDENTITY_PROPERTY;
use buck2_build_api::actions::local_actions::LocalActionList;
use buck2_build_api::actions::local_actions::LocalActions;
use buck2_build_api::actions::local_actions::build_keys;
use buck2_build_api::actions::local_actions::partition_by_store_validity;
use buck2_common::init::SuspendConfig;
use buck2_common::legacy_configs::configs::LegacyBuckConfig;
use buck2_common::legacy_configs::key::BuckconfigKeyRef;
pub(crate) use buck2_common::suspend_image::DICE_STATE_DIR;
use buck2_common::suspend_image::FORMAT;
use buck2_common::suspend_image::IMAGE;
pub(crate) use buck2_common::suspend_image::IMAGE_DIR;
use buck2_common::suspend_image::MAGIC;
use buck2_common::suspend_image::MANIFEST;
use buck2_common::suspend_image::MANIFEST_CONSUMED;
use buck2_core::buck2_env;
use buck2_core::fs::project::ProjectRoot;
use buck2_error::BuckErrorContext;
use buck2_error::ErrorTag;
use buck2_error::buck2_error;
use buck2_execute::materialize::materializer::Materializer;
use buck2_execute::materialize::materializer::ResumedArtifacts;
use buck2_execute::materialize::materializer::SuspendedArtifact;
use buck2_execute::materialize::materializer::SuspendedArtifactCensus;
use buck2_execute::materialize::materializer::suspend_rows;
use buck2_file_watcher::file_watcher::FileWatcher;
use buck2_file_watcher::file_watcher::FileWatcherImage;
use buck2_file_watcher::file_watcher::ResumeInput;
use dice::Dice;
use dice::DiceTransaction;
use dice::ResumeStats;
use dice::SuspendRow;
use dupe::Dupe;
use serde::Deserialize;
use serde::Serialize;

/// How far the clock may be behind the suspend's and still resume: two runners' clocks are never
/// exactly in step. Nothing a resume trusts compares a time across the step except ctime on the
/// same identity, which a clock stepped back cannot make equal to the suspended value except by
/// coincidence.
const CLOCK_SKEW_MS: u64 = 5 * 60 * 1000;

mod vcs;

/// Environment variables that differ between two runs of the same job and that nothing a daemon
/// computes depends on, left out of the environment digest. A name ending in `*` is a prefix and a
/// name starting with `*` a suffix.
const PER_RUN_ENV: &[&str] = &[
    "_",
    "BUCK2_DAEMON_UUID",
    "BUCK_WRAPPER_START_TIME_MS",
    "BUCK_WRAPPER_UUID",
    "BUILDBUDDY_INVOCATION_ID",
    "COLUMNS",
    "GITHUB_*",
    "HOST",
    "HOSTNAME",
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

/// The per-run variables of a shell or an invocation rather than of a CI job: they change on every
/// command, and no action is expected to read them. Every other variable the environment check
/// leaves out (`GITHUB_*`, `HOSTNAME`, `suspend_env_ignore`'s list) is digested on its own, and a
/// resume whose digest differs runs again every action that ran locally, since those inherit the
/// daemon's environment. Credentials are in neither digest.
const SESSION_ENV: &[&str] = &[
    "_",
    "BUCK2_DAEMON_UUID",
    "BUCK_WRAPPER_START_TIME_MS",
    "BUCK_WRAPPER_UUID",
    "BUILDBUDDY_INVOCATION_ID",
    "COLUMNS",
    "INVOCATION_ID",
    "LINES",
    "OLDPWD",
    "PWD",
    "SHLVL",
    "SSH_*",
    "TERM",
    "TMUX*",
    "WINDOWID",
];

/// Credentials, left out of the environment digest and of the per-variable digests, because an
/// image may be uploaded and shared and a digest of a secret should not travel with it, and
/// because a PR runner holds its own credential rather than main's. Nothing a daemon computes may
/// depend on a credential's value; an action that reads one is not reproducible in any case.
const CREDENTIAL_ENV: &[&str] = &[
    "BUILDBUDDY_API_KEY",
    "*_API_KEY",
    "*_TOKEN",
    "*_SECRET",
    "*_PASSWORD",
];

fn env_pattern_matches(pattern: &str, name: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix('*') {
        name.starts_with(prefix)
    } else if let Some(suffix) = pattern.strip_prefix('*') {
        name.ends_with(suffix)
    } else {
        name == pattern
    }
}

fn blake3_hex(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

/// Whether a variable is left out of the environment check: a per-run variable, a credential, or
/// one `suspend_env_ignore` names (with the same `*` patterns).
fn ignored(name: &str, extra: &[String]) -> bool {
    PER_RUN_ENV
        .iter()
        .chain(CREDENTIAL_ENV)
        .any(|p| env_pattern_matches(p, name))
        || extra.iter().any(|e| env_pattern_matches(e, name))
}

/// The digest of the environment, and a short digest of each variable's value by name, so that
/// a refusal can name what changed without recording any value.
#[derive(Debug, Clone, PartialEq, Eq)]
struct EnvDigest {
    digest: String,
    vars: BTreeMap<String, String>,
    /// The digest of the variables left out of `digest` that a local action might read: neither
    /// a credential nor in [`SESSION_ENV`].
    run: String,
}

fn env_digest(extra_ignored: &[String]) -> EnvDigest {
    env_digest_of(
        std::env::vars_os().map(|(k, v)| {
            (
                k.to_string_lossy().into_owned(),
                v.to_string_lossy().into_owned(),
            )
        }),
        extra_ignored,
    )
}

fn env_digest_of(
    vars: impl IntoIterator<Item = (String, String)>,
    extra_ignored: &[String],
) -> EnvDigest {
    let mut vars: Vec<(String, String)> = vars.into_iter().collect();
    vars.sort();
    let mut hasher = blake3::Hasher::new();
    let mut run = blake3::Hasher::new();
    let mut by_name = BTreeMap::new();
    for (k, v) in &vars {
        let target = if !ignored(k, extra_ignored) {
            by_name.insert(k.clone(), blake3_hex(v.as_bytes())[..16].to_owned());
            &mut hasher
        } else if CREDENTIAL_ENV
            .iter()
            .chain(SESSION_ENV)
            .any(|p| env_pattern_matches(p, k))
        {
            continue;
        } else {
            &mut run
        };
        target.update(k.as_bytes());
        target.update(&[0]);
        target.update(v.as_bytes());
        target.update(&[0]);
    }
    EnvDigest {
        digest: hasher.finalize().to_hex().to_string(),
        vars: by_name,
        run: run.finalize().to_hex().to_string(),
    }
}

/// The digest of a resolved buckconfig, which the daemon read once at startup and DICE does not
/// track (ignore specs, digest algorithms, materializer settings and the like). The host identity
/// is left out: it is checked on its own, so that a refusal names it.
fn config_digest(config: &LegacyBuckConfig) -> String {
    // Each entry carries its section, so that a section left empty by the skip hashes as if it
    // were absent.
    let mut hasher = blake3::Hasher::new();
    for (section, values) in config.iter() {
        for (key, value) in values {
            if section == "buck2_hydration" && key == HOST_IDENTITY_PROPERTY {
                continue;
            }
            for part in [section, key, value] {
                hasher.update(part.as_bytes());
                hasher.update(&[0]);
            }
        }
    }
    hasher.finalize().to_hex().to_string()
}

/// The digest of the running binary. On Linux this reads `/proc/self/exe`, which is the image
/// this process runs even after the file at its path was replaced: hashing the path instead
/// would describe whatever binary is there now, or fail once the old one is unlinked.
fn exe_digest() -> buck2_error::Result<String> {
    #[cfg(target_os = "linux")]
    let exe = PathBuf::from("/proc/self/exe");
    #[cfg(not(target_os = "linux"))]
    let exe = std::env::current_exe().buck_error_context("Locating the daemon executable")?;
    buck2_common::suspend_image::exe_digest(&exe)
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
    /// `hydration.host_identity`, or the hostname.
    host_identity: String,
}

impl SuspendIdentity {
    pub(crate) fn new(
        config: SuspendConfig,
        startup_config: &DaemonStartupConfig,
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
            startup_config: blake3_hex(
                startup_config
                    .without_host_identity()
                    .serialize()?
                    .as_bytes(),
            ),
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
            host_identity: crate::daemon::disk_state::host_identity(startup_config),
        })
    }
}

/// Why an image was not resumed. Each check has its own reason, so that a test can tell that a
/// change refused for the reason it should and not for an earlier one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The MANIFEST is unreadable, or names another magic or format.
    Format(String),
    Binary,
    StartupConfig,
    /// The variables whose values differ, or that one side has and the other lacks.
    Env(Vec<String>),
    RootConfig,
    Project,
    HostIdentity,
    User,
    Watcher,
    Clock,
    TooLarge {
        bytes: u64,
    },
    NoPagingStore,
    ImageCorrupt(String),
    /// Materialized artifacts that are neither on this disk with the suspended entry nor
    /// available from the CAS.
    NotPortable {
        lost: usize,
    },
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let differs = |f: &mut fmt::Formatter<'_>, what: &str| {
            write!(f, "{what} differs from the suspended daemon's")
        };
        match self {
            Refusal::Format(detail) => write!(f, "image format: {detail}"),
            Refusal::Binary => differs(f, "the buck2 binary"),
            Refusal::StartupConfig => differs(f, "the daemon startup config"),
            Refusal::Env(names) => write!(
                f,
                "the environment differs from the suspended daemon's: {}",
                names.join(", ")
            ),
            Refusal::RootConfig => differs(f, "the root buckconfig"),
            Refusal::Project => differs(f, "the project root or isolation dir"),
            Refusal::HostIdentity => differs(f, "the host identity"),
            Refusal::User => differs(f, "the user"),
            Refusal::Watcher => differs(f, "the file watcher"),
            Refusal::Clock => write!(
                f,
                "the clock is more than {} minutes earlier than the suspend",
                CLOCK_SKEW_MS / 60_000
            ),
            Refusal::TooLarge { bytes } => write!(
                f,
                "the paging store is {bytes} bytes, over `buck2_hydration.suspend_max_db_gb`"
            ),
            Refusal::NoPagingStore => write!(f, "the paging store is missing"),
            Refusal::ImageCorrupt(detail) => write!(f, "the image is corrupt: {detail}"),
            Refusal::NotPortable { lost } => write!(
                f,
                "the image is not portable: {lost} materialized artifacts are not on this disk \
                 and have no CAS recipe (suspend with `--portable` to upload them)"
            ),
        }
    }
}

/// What this daemon observes, for [`compare`] against a MANIFEST.
struct Observed<'a> {
    identity: &'a SuspendIdentity,
    exe: String,
    env: EnvDigest,
    uid: u32,
    now_ms: u64,
}

/// The part of a MANIFEST that every format has, read first so that an image of another format
/// refuses as such rather than as an unreadable MANIFEST.
#[derive(Deserialize)]
struct ManifestHeader {
    magic: String,
    format: u32,
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
    /// `hydration.host_identity`, or the hostname: checked.
    host_identity: String,
    /// The hostname, whatever the identity: not checked. It tells a resume on the same machine
    /// from one on another machine of the same identity.
    hostname: String,
    uid: u32,
    watcher: String,
    created_at_ms: u64,
    image_len: u64,
    image: String,
    dice_state_bytes: u64,
    /// Written by `buck2 suspend --portable`: every materialized artifact has a CAS recipe.
    portable: bool,
    /// [`EnvDigest::run`]: the per-run variables the environment check leaves out.
    run_env: String,
}

impl Manifest {
    /// Whether a daemon resuming this image runs again every action the suspended daemon ran
    /// on its own machine: those read the daemon's environment and machine, which a portable
    /// image, another host, or other values of the per-run variables the environment check
    /// leaves out do not promise to match. Leaving those variables out of the check is sound
    /// only because of this.
    fn dirties_local_actions(&self, hostname_now: &str, run_env_now: &str) -> bool {
        self.portable || self.hostname != hostname_now || self.run_env != run_env_now
    }

    /// Whether the resumed watcher must treat the files as another machine's: no stat the image
    /// holds is compared, and a link that leaves the project is a change. A portable image is
    /// made to move, and a hostname says nothing about the machine when runners share one (VMs
    /// restored from one snapshot, containers given a fixed name), so only a non-portable image
    /// resumed under the hostname that wrote it is taken to be on the same machine.
    fn cross_host(&self, hostname_now: &str) -> bool {
        self.portable || self.hostname != hostname_now
    }
}

#[derive(Serialize, Deserialize)]
struct ImageFile {
    dice: Vec<u8>,
    /// The materializer's artifacts, in rows of `suspend_rows`'s bounds.
    artifact_rows: Vec<SuspendRow>,
    artifact_count: usize,
    /// The actions the suspended daemon ran on its own machine (`LocalActionList`).
    local_actions: SuspendRow,
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
        /// Locally executed actions dirtied so that they run again here.
        dirtied_local_actions: usize,
        /// Locally executed actions not dirtied, though the image asked for it, because every
        /// store path they declare is valid here.
        kept_store_actions: usize,
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
                dirtied_local_actions,
                kept_store_actions,
            } => {
                let mut out = format!(
                    "resumed: {} keys, {} asserted, {} valid, {} candidates, {} not installed; \
                     artifacts: {} reactivated, {} declared again, {} to fetch from the CAS",
                    stats.keys,
                    stats.asserted,
                    stats.certified,
                    stats.candidates,
                    stats.dropped,
                    artifacts.reactivated,
                    artifacts.redeclared,
                    artifacts.refetch
                );
                out.push_str(&format!(
                    "; {dirtied_local_actions} locally executed actions to run again; \
                     {kept_store_actions} kept, their store paths valid here"
                ));
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
    /// The image, its graph moved to `dice_bytes`.
    image: ImageFile,
    /// [`ImageFile::dice`], shared with the task that reads the graph's rows.
    dice_bytes: Arc<Vec<u8>>,
    /// The image was written on another machine, so no file stat it holds is comparable.
    cross_host: bool,
    /// See [`Manifest::dirties_local_actions`].
    dirty_local_actions: bool,
    /// `buck2_hydration.suspend_store_check`.
    store_check: Vec<String>,
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
        Ok((mut image, cross_host, dirty_local_actions)) => ResumePrep::Ready(LoadedImage {
            dice_bytes: Arc::new(std::mem::take(&mut image.dice)),
            image,
            cross_host,
            dirty_local_actions,
            store_check: identity.config.store_check.clone(),
            image_dir,
            loaded_in: start.elapsed(),
        }),
        Err(refusal) => ResumePrep::Refused(refusal.to_string()),
    }
}

fn parse_manifest(manifest_bytes: &[u8]) -> Result<Manifest, Refusal> {
    let header: ManifestHeader = serde_json::from_slice(manifest_bytes)
        .map_err(|e| Refusal::Format(format!("unreadable MANIFEST: {e}")))?;
    if header.magic != MAGIC || header.format != FORMAT {
        return Err(Refusal::Format(format!(
            "{} {}, this binary reads {MAGIC} {FORMAT}",
            header.magic, header.format
        )));
    }
    serde_json::from_slice(manifest_bytes)
        .map_err(|e| Refusal::ImageCorrupt(format!("unreadable MANIFEST: {e}")))
}

/// The checks that need no I/O, in the order a refusal reports them: the first that fails names
/// the reason.
fn compare(m: &Manifest, o: &Observed<'_>) -> Result<(), Refusal> {
    if m.magic != MAGIC || m.format != FORMAT {
        return Err(Refusal::Format(format!("{} {}", m.magic, m.format)));
    }
    if m.exe != o.exe {
        return Err(Refusal::Binary);
    }
    if m.startup_config != o.identity.startup_config {
        return Err(Refusal::StartupConfig);
    }
    if m.env != o.env.digest {
        let vars = &o.env.vars;
        let mut changed: Vec<String> = vars
            .iter()
            .filter(|(k, v)| m.env_vars.get(*k) != Some(*v))
            .map(|(k, _)| k.clone())
            .chain(
                m.env_vars
                    .keys()
                    .filter(|k| !vars.contains_key(*k))
                    .cloned(),
            )
            .collect();
        changed.sort();
        changed.dedup();
        return Err(Refusal::Env(changed));
    }
    if m.root_config != o.identity.root_config {
        return Err(Refusal::RootConfig);
    }
    if m.project_root != o.identity.project_root || m.isolation != o.identity.isolation {
        return Err(Refusal::Project);
    }
    if m.host_identity != o.identity.host_identity {
        return Err(Refusal::HostIdentity);
    }
    if m.uid != o.uid {
        return Err(Refusal::User);
    }
    if m.watcher != o.identity.watcher {
        return Err(Refusal::Watcher);
    }
    if o.now_ms.saturating_add(CLOCK_SKEW_MS) < m.created_at_ms {
        return Err(Refusal::Clock);
    }
    let max = o.identity.config.max_db_gb.saturating_mul(1 << 30);
    if m.dice_state_bytes > max {
        return Err(Refusal::TooLarge {
            bytes: m.dice_state_bytes,
        });
    }
    Ok(())
}

fn check(
    manifest_bytes: &[u8],
    image_dir: &Path,
    cache_dir: &Path,
    identity: &SuspendIdentity,
) -> Result<(ImageFile, bool, bool), Refusal> {
    let m = parse_manifest(manifest_bytes)?;
    let observed = Observed {
        identity,
        // A binary that cannot hash itself cannot show it is the suspended one.
        exe: exe_digest().map_err(|_| Refusal::Binary)?,
        env: env_digest(&identity.config.env_ignore),
        uid: uid(),
        now_ms: now_ms(),
    };
    compare(&m, &observed)?;
    let image = check_files(&m, image_dir, cache_dir)?;
    // The hostname is not checked, since the identity stands for it, but a stat taken on another
    // machine is never comparable with this one's.
    let hostname = buck2_events::metadata::hostname().unwrap_or_default();
    Ok((
        image,
        m.cross_host(&hostname),
        m.dirties_local_actions(&hostname, &observed.env.run),
    ))
}

/// The checks that read the image's files.
fn check_files(m: &Manifest, image_dir: &Path, cache_dir: &Path) -> Result<ImageFile, Refusal> {
    if !cache_dir.join(DICE_STATE_DIR).is_dir() {
        return Err(Refusal::NoPagingStore);
    }
    let bytes = std::fs::read(image_dir.join(IMAGE))
        .map_err(|e| Refusal::ImageCorrupt(format!("unreadable image: {e}")))?;
    if bytes.len() as u64 != m.image_len {
        return Err(Refusal::ImageCorrupt("truncated".to_owned()));
    }
    if blake3_hex(&bytes) != m.image {
        return Err(Refusal::ImageCorrupt(
            "the blake3 digest differs".to_owned(),
        ));
    }
    let (image, _): (ImageFile, usize) =
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
            .map_err(|e| Refusal::ImageCorrupt(format!("undecodable image: {e}")))?;
    Ok(image)
}

impl LoadedImage {
    /// What the resumed file watcher compares the files with. It asks git, if the suspend
    /// recorded a commit, once it has statted the files.
    pub(crate) fn resume_input<'a>(&'a self, project_root: &'a ProjectRoot) -> ResumeInput<'a> {
        let watcher = &self.image.watcher;
        ResumeInput {
            image: watcher,
            cross_host: self.cross_host,
            vcs: Box::new(move || {
                let head = watcher.vcs.as_ref()?;
                Some(vcs::resume_delta(project_root.root(), head))
            }),
        }
    }

    /// Replays the image. The materializer's artifacts and the graph's rows are read at the same
    /// time, and the graph is installed onto `root` only once both are done, the materializer
    /// lost nothing and the graph's rows all read, so that every output the graph names is known
    /// before anything asks for it and a [`RestoreError::Cold`] leaves the graph untouched, for
    /// the caller to start cold. Then the list of local actions. Once the graph holds the image,
    /// a failure is [`RestoreError::Fatal`].
    pub(crate) async fn restore(
        &self,
        dice: &Arc<Dice>,
        root: dice::BranchId,
        materializer: &dyn Materializer,
    ) -> Result<RestoredImage, RestoreError> {
        let mut timings = vec![("load", self.loaded_in)];
        // Read first: it is also the first read of the paging store, which turns on checking
        // every row read against its key, so that a cold start over this store after a refusal
        // below is checked as well.
        let actions: LocalActionList = dice
            .read_suspend_row(self.image.local_actions)
            .await
            .map_err(|e| RestoreError::Cold(format!("unreadable list of local actions: {e:#}")))?;
        let t = Instant::now();
        // The graph's rows are read on a task of their own: both reads block their thread in
        // sqlite, so on one task they would take turns.
        // `BUCK2_TEST_RESUME_GRAPH_ERROR` fails the read once it has read everything, as a
        // damaged row the last read finds would, for a test to check that the refusal comes
        // before anything is installed.
        let inject_graph_error = buck2_env!(
            "BUCK2_TEST_RESUME_GRAPH_ERROR",
            bool,
            applicability = testing
        )
        .unwrap_or(false);
        let graph_read = {
            let dice = dice.dupe();
            let bytes = self.dice_bytes.dupe();
            tokio::spawn(async move {
                let t = Instant::now();
                let prepared = match dice.prepare_restore(root, &bytes).await {
                    Ok(_) if inject_graph_error => {
                        Err("graph: injected by BUCK2_TEST_RESUME_GRAPH_ERROR".to_owned())
                    }
                    Ok(prepared) => Ok(prepared),
                    Err(e) => Err(format!("graph: {e:#}")),
                };
                (prepared, t.elapsed())
            })
        };
        let mut replay_took = Duration::ZERO;
        let mut rows_took = Duration::ZERO;
        let mut read_took = Duration::ZERO;
        let replay = async {
            let t = Instant::now();
            let (resumed, reading) =
                replay_artifacts(dice, &self.image.artifact_rows, materializer).await;
            replay_took = t.elapsed();
            rows_took = reading;
            resumed
        };
        let graph_read = async {
            match graph_read.await {
                Ok((prepared, took)) => {
                    read_took = took;
                    prepared
                }
                Err(e) => Err(format!("graph: {e}")),
            }
        };
        let overlapped = overlap_then_install(replay, graph_read, |prepared| {
            dice.install_restore(prepared)
        })
        .await;
        timings.push(("materializer", replay_took));
        timings.push(("materializer rows read", rows_took));
        timings.push(("graph read", read_took));
        timings.push(("materializer and graph", t.elapsed()));
        let (resumed, stats) = overlapped.map_err(RestoreError::Cold)?;
        timings.extend(stats.timings.iter().map(|(step, d)| (*step, *d)));
        // The suspended daemon's local actions join this daemon's, whether or not they are
        // dirtied: a value this daemon keeps from one of them is still that machine's, and a
        // later suspend of this daemon must list it for the resume after that to run it again.
        let t = Instant::now();
        let existing = dice.updater(root).existing_state().await;
        if let Some(local) = LocalActions::of(existing.global_data()) {
            local.extend(&actions);
        }
        drop(existing);
        let (dirtied_local_actions, kept_store_actions) = if self.dirty_local_actions {
            let check = self.store_check.clone();
            let (dirty, kept) = tokio::task::spawn_blocking(move || {
                partition_by_store_validity(actions, |path| store_path_valid(path, &check))
            })
            .await
            .map_err(|e| RestoreError::Fatal(format!("checking store paths: {e}")))?;
            let dirtied = dirty_keys(dice, root, build_keys(dirty))
                .await
                .map_err(RestoreError::Fatal)?;
            (dirtied, kept)
        } else {
            (0, 0)
        };
        timings.push(("local actions", t.elapsed()));
        Ok(RestoredImage {
            stats,
            artifacts: resumed,
            timings,
            dirtied_local_actions,
            kept_store_actions,
        })
    }

    /// The image is single-use: once replayed, or refused, it goes.
    pub(crate) fn discard(&self) {
        drop(std::fs::remove_dir_all(&self.image_dir));
    }
}

/// How many of the materializer's rows a resume reads at once, ahead of the replay.
const ARTIFACT_ROW_READERS: usize = 8;

/// Replays the materializer's artifacts, one row after another in the order the suspend wrote
/// them. The rows are read ahead of the replay on tasks of their own, several at once, since
/// reading a row, which reads every directory it names from the paging store, takes longer
/// than declaring its artifacts, and a row read blocks its thread in sqlite.
///
/// Returns too how long the reads took to deliver the last row.
async fn replay_artifacts(
    dice: &Arc<Dice>,
    rows: &[SuspendRow],
    materializer: &dyn Materializer,
) -> (Result<ResumedArtifacts, String>, Duration) {
    let (tx, mut rx) = tokio::sync::mpsc::channel(ARTIFACT_ROW_READERS);
    let reader = {
        let dice = dice.dupe();
        let rows = rows.to_vec();
        tokio::spawn(async move {
            let t = Instant::now();
            let mut rows = rows.into_iter();
            let mut in_flight = std::collections::VecDeque::with_capacity(ARTIFACT_ROW_READERS);
            loop {
                while in_flight.len() < ARTIFACT_ROW_READERS
                    && let Some(row) = rows.next()
                {
                    let dice = dice.dupe();
                    in_flight.push_back(tokio::spawn(async move {
                        dice.read_suspend_row::<Vec<SuspendedArtifact>>(row).await
                    }));
                }
                // Taken in the order of the rows, whichever read finishes first.
                let Some(next) = in_flight.pop_front() else {
                    break;
                };
                let read = match next.await {
                    Ok(read) => read.map_err(|e| format!("unreadable materializer state: {e:#}")),
                    Err(e) => Err(format!("unreadable materializer state: {e}")),
                };
                let failed = read.is_err();
                if tx.send(read).await.is_err() || failed {
                    // No read outlives the replay.
                    for read in in_flight {
                        drop(read.await);
                    }
                    break;
                }
            }
            t.elapsed()
        })
    };
    let mut resumed = ResumedArtifacts::default();
    let mut replayed = Ok(());
    while let Some(read) = rx.recv().await {
        let declared = match read {
            Ok(artifacts) => materializer
                .resume_artifacts(artifacts)
                .await
                .map_err(|e| format!("materializer: {e:#}")),
            Err(e) => Err(e),
        };
        match declared {
            Ok(r) => resumed += r,
            Err(e) => {
                replayed = Err(e);
                break;
            }
        }
    }
    // The reader stops at its next send once nothing receives, after the reads it started; it
    // is awaited so that no read outlives the replay.
    drop(rx);
    match reader.await {
        Ok(reading) => (replayed.map(|()| resumed), reading),
        Err(e) => (
            replayed.and_then(|()| Err(format!("unreadable materializer state: {e}"))),
            Duration::ZERO,
        ),
    }
}

/// Runs the materializer's replay and the graph's read together, and installs what the read
/// returned only if the replay lost nothing and both succeeded. Both are awaited to the end
/// whatever either returns: no replay command is then still on its way to the materializer when
/// the daemon starts cold, nor a read still running. The refusals come in the order the replay
/// and the read used to run one after the other: the materializer's, an artifact lost, the
/// graph's.
async fn overlap_then_install<P, S, I: Future<Output = S>>(
    replay: impl Future<Output = Result<ResumedArtifacts, String>>,
    read: impl Future<Output = Result<P, String>>,
    install: impl FnOnce(P) -> I,
) -> Result<(ResumedArtifacts, S), String> {
    let (replayed, prepared) = futures::future::join(replay, read).await;
    let resumed = replayed?;
    if resumed.lost > 0 {
        return Err(Refusal::NotPortable { lost: resumed.lost }.to_string());
    }
    let prepared = prepared?;
    Ok((resumed, install(prepared).await))
}

/// What [`LoadedImage::restore`] did.
pub(crate) struct RestoredImage {
    pub(crate) stats: ResumeStats,
    pub(crate) artifacts: ResumedArtifacts,
    pub(crate) timings: Vec<(&'static str, Duration)>,
    pub(crate) dirtied_local_actions: usize,
    pub(crate) kept_store_actions: usize,
}

/// Why [`LoadedImage::restore`] did not resume.
pub(crate) enum RestoreError {
    /// The graph is untouched: the daemon starts cold.
    Cold(String),
    /// The graph holds the image but not everything the image needs on top of it, such as its
    /// local actions dirtied: this daemon must not run anything.
    Fatal(String),
}

/// Whether a store path a local action's output links to is valid on this machine: it exists,
/// and `check` (`buck2_hydration.suspend_store_check`), when set, exits 0 with the path appended.
/// A check that cannot run counts as invalid, which costs only a rerun.
fn store_path_valid(path: &str, check: &[String]) -> bool {
    if std::fs::symlink_metadata(path).is_err() {
        return false;
    }
    let Some((program, args)) = check.split_first() else {
        return true;
    };
    std::process::Command::new(program)
        .args(args)
        .arg(path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Dirties `keys` on `root`, as a rewind does, so that the next transaction computes each again.
/// Returns how many distinct keys were dirtied. Duplicates are dropped first, since DICE refuses
/// a key changed twice in one transaction.
async fn dirty_keys<K: dice::Key>(
    dice: &Arc<Dice>,
    root: dice::BranchId,
    keys: Vec<K>,
) -> Result<usize, String> {
    let mut seen = std::collections::HashSet::with_capacity(keys.len());
    let keys: Vec<K> = keys
        .into_iter()
        .filter(|k| seen.insert(k.clone()))
        .collect();
    if keys.is_empty() {
        return Ok(0);
    }
    let n = keys.len();
    let mut updater = dice.updater(root);
    updater
        .changed(keys)
        .map_err(|e| format!("dirtying local actions: {e:#}"))?;
    updater.commit().await;
    Ok(n)
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
    /// `buck2 suspend --portable`.
    pub(crate) portable: bool,
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
    let project_root = inputs.project_root;
    let watcher = inputs
        .file_watcher
        .suspend_image(
            project_root,
            Box::new(move || vcs::suspend_tree(project_root.root())),
        )
        .await?;
    lap("files", &mut timings);

    let artifacts = inputs
        .materializer
        .suspend_artifacts(inputs.portable)
        .await?;
    let artifact_count = artifacts.len();
    let census = SuspendedArtifactCensus::of(&artifacts);
    let mut artifact_rows = Vec::new();
    for row in suspend_rows(artifacts) {
        artifact_rows.push(
            inputs.dice.write_suspend_row(&row).map_err(|e| {
                buck2_error!(ErrorTag::Tier0, "writing the materializer state: {e:#}")
            })?,
        );
    }
    let local_actions: LocalActionList = LocalActions::of(inputs.transaction.global_data())
        .map(LocalActions::snapshot)
        .unwrap_or_default();
    let local_actions_row = inputs
        .dice
        .write_suspend_row(&local_actions)
        .map_err(|e| buck2_error!(ErrorTag::Tier0, "writing the local actions: {e:#}"))?;
    let local_action_count = local_actions.len();
    drop(local_actions);
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
        artifact_rows,
        artifact_count,
        local_actions: local_actions_row,
        watcher,
    };
    let bytes = bincode::serde::encode_to_vec(&file, bincode::config::standard())
        .map_err(|e| buck2_error!(ErrorTag::Tier0, "encoding the image: {e}"))?;
    write_durably(&image_dir.join(IMAGE), &bytes)?;
    sync_filesystem(&inputs.buck_out)?;
    lap("sync", &mut timings);

    let EnvDigest {
        digest: env,
        vars: env_vars,
        run: run_env,
    } = env_digest(&inputs.identity.config.env_ignore);
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
        host_identity: inputs.identity.host_identity.clone(),
        hostname: buck2_events::metadata::hostname().unwrap_or_default(),
        uid: uid(),
        watcher: inputs.identity.watcher.clone(),
        created_at_ms: now_ms(),
        image_len: bytes.len() as u64,
        image: blake3_hex(&bytes),
        dice_state_bytes,
        portable: inputs.portable,
        run_env,
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
        "suspended{}: image {} bytes, paging store {} bytes, {} artifacts in {} rows, {} \
         locally executed actions\n",
        if inputs.portable { " (portable)" } else { "" },
        bytes.len(),
        dice_state_bytes,
        artifact_count,
        file.artifact_rows.len(),
        local_action_count
    ));
    out.push_str(&format!("artifacts: {census}\n"));
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
    out.push_str("graph times:");
    for (name, d) in &stats.timings {
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
    use buck2_common::init::DaemonStartupConfig;
    use buck2_common::legacy_configs::configs::testing::parse;
    use buck2_common::settings::BuckSettings;
    use buck2_core::fs::project::ProjectRootTemp;

    use super::*;

    const ROOT: &str =
        "[buck2_hydration]\nenable_paging = true\nsuspend = true\nhost_identity = A\n[s]\nk = 1\n";

    /// One side of a suspend and resume: what a daemon resolved at startup and what it observes.
    struct Side {
        identity: SuspendIdentity,
        exe: String,
        env: Vec<(String, String)>,
        uid: u32,
        now_ms: u64,
    }

    impl Side {
        fn observed(&self) -> Observed<'_> {
            Observed {
                identity: &self.identity,
                exe: self.exe.clone(),
                env: env_digest_of(self.env.clone(), &self.identity.config.env_ignore),
                uid: self.uid,
                now_ms: self.now_ms,
            }
        }

        /// The MANIFEST a suspend on this side writes.
        fn manifest(&self) -> Manifest {
            let o = self.observed();
            Manifest {
                magic: MAGIC.to_owned(),
                format: FORMAT,
                exe: o.exe,
                startup_config: self.identity.startup_config.clone(),
                env: o.env.digest,
                env_vars: o.env.vars,
                run_env: o.env.run,
                root_config: self.identity.root_config.clone(),
                project_root: self.identity.project_root.clone(),
                isolation: self.identity.isolation.clone(),
                host_identity: self.identity.host_identity.clone(),
                hostname: "suspend-host".to_owned(),
                uid: self.uid,
                watcher: self.identity.watcher.clone(),
                created_at_ms: self.now_ms,
                image_len: 0,
                image: String::new(),
                dice_state_bytes: 1 << 20,
                portable: false,
            }
        }
    }

    fn identity_of(
        startup: &DaemonStartupConfig,
        root: &LegacyBuckConfig,
        project: &ProjectRoot,
        isolation: &str,
    ) -> SuspendIdentity {
        let config = startup
            .hydration
            .as_ref()
            .and_then(|h| h.suspend.clone())
            .expect("suspend is on");
        SuspendIdentity::new(config, startup, root, project, isolation).unwrap()
    }

    fn startup_of(root: &LegacyBuckConfig) -> DaemonStartupConfig {
        DaemonStartupConfig::new(root, &BuckSettings::empty(), false).unwrap()
    }

    fn root_of(text: &str) -> LegacyBuckConfig {
        parse(&[("c", text)], "c").unwrap()
    }

    fn env() -> Vec<(String, String)> {
        [
            ("PATH", "/bin"),
            ("HOME", "/home/runner"),
            ("GITHUB_RUN_ID", "1"),
            ("GITHUB_SHA", "aaa"),
            ("RUNNER_NAME", "runner-1"),
            ("HOSTNAME", "runner-1"),
            ("ACTIONS_ID_TOKEN_REQUEST_URL", "https://a/1"),
            ("GIT_COMMIT", "aaa"),
            ("GIT_PR_NUMBER", "1"),
            ("ImageVersion", "20261001.1"),
            ("BUILDBUDDY_API_KEY", "main-key"),
            ("FOO_TOKEN", "t1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
    }

    fn set(env: &mut Vec<(String, String)>, name: &str, value: &str) {
        env.retain(|(k, _)| k != name);
        env.push((name.to_owned(), value.to_owned()));
    }

    fn side(project: &ProjectRoot) -> Side {
        side_with(project, &root_of(ROOT))
    }

    fn side_with(project: &ProjectRoot, root: &LegacyBuckConfig) -> Side {
        Side {
            identity: identity_of(&startup_of(root), root, project, "v2"),
            exe: "exe-1".to_owned(),
            env: env(),
            uid: 1000,
            now_ms: 1_800_000_000_000,
        }
    }

    /// Suspends on `side(..)`, lets `change` alter the resuming side, and compares.
    fn resume_after(change: impl FnOnce(&mut Side, &ProjectRoot)) -> Result<(), Refusal> {
        let project = ProjectRootTemp::new().unwrap();
        let suspended = side(project.path());
        let manifest = suspended.manifest();
        let mut resuming = side(project.path());
        change(&mut resuming, project.path());
        compare(&manifest, &resuming.observed())
    }

    #[test]
    fn an_unchanged_daemon_resumes() {
        assert_eq!(resume_after(|_, _| {}), Ok(()));
    }

    #[test]
    fn another_binary_refuses() {
        assert_eq!(
            resume_after(|s, _| s.exe = "exe-2".to_owned()),
            Err(Refusal::Binary)
        );
    }

    #[test]
    fn another_startup_config_refuses() {
        assert_eq!(
            resume_after(|s, project| {
                let root = root_of(ROOT);
                let mut startup = startup_of(&root);
                startup.num_tokio_workers = Some(7);
                s.identity = identity_of(&startup, &root, project, "v2");
            }),
            Err(Refusal::StartupConfig)
        );
    }

    #[test]
    fn another_identity_in_the_startup_config_refuses_as_the_identity() {
        assert_eq!(
            resume_after(|s, project| {
                let root = root_of(ROOT);
                let mut startup = startup_of(&root);
                startup.host_identity = Some("B".to_owned());
                s.identity = identity_of(&startup, &root, project, "v2");
            }),
            Err(Refusal::HostIdentity)
        );
    }

    #[test]
    fn another_identity_in_the_root_config_refuses_as_the_identity() {
        assert_eq!(
            resume_after(|s, project| {
                let root = root_of(&ROOT.replace("host_identity = A", "host_identity = B"));
                s.identity = identity_of(&startup_of(&root), &root, project, "v2");
            }),
            Err(Refusal::HostIdentity)
        );
    }

    #[test]
    fn the_identity_defaults_to_the_hostname() {
        let root = root_of(&ROOT.replace("host_identity = A\n", ""));
        let project = ProjectRootTemp::new().unwrap();
        let identity = identity_of(&startup_of(&root), &root, project.path(), "v2");
        assert_eq!(
            identity.host_identity,
            buck2_events::metadata::hostname().unwrap_or_default()
        );
    }

    #[test]
    fn another_path_refuses_and_names_it() {
        assert_eq!(
            resume_after(|s, _| set(&mut s.env, "PATH", "/usr/bin")),
            Err(Refusal::Env(vec!["PATH".to_owned()]))
        );
        assert_eq!(
            resume_after(|s, _| s.env.retain(|(k, _)| k != "HOME")),
            Err(Refusal::Env(vec!["HOME".to_owned()]))
        );
    }

    #[test]
    fn per_run_variables_do_not_refuse() {
        for (name, value) in [
            ("GITHUB_RUN_ID", "2"),
            ("GITHUB_SHA", "bbb"),
            ("RUNNER_NAME", "runner-2"),
            ("HOSTNAME", "runner-2"),
            ("HOST", "runner-2"),
            ("ACTIONS_ID_TOKEN_REQUEST_URL", "https://a/2"),
            ("ACTIONS_CACHE_URL", "https://cache/2"),
            ("GIT_COMMIT", "bbb"),
            ("GIT_PR_NUMBER", "2"),
            ("ImageVersion", "20261002.1"),
        ] {
            assert_eq!(
                resume_after(|s, _| set(&mut s.env, name, value)),
                Ok(()),
                "{name}"
            );
        }
    }

    #[test]
    fn credentials_do_not_refuse_and_leave_no_digest() {
        assert_eq!(
            resume_after(|s, _| {
                set(&mut s.env, "BUILDBUDDY_API_KEY", "pr-key");
                set(&mut s.env, "FOO_TOKEN", "t2");
                set(&mut s.env, "BAR_SECRET", "s");
                set(&mut s.env, "BAZ_PASSWORD", "p");
                set(&mut s.env, "OTHER_API_KEY", "k");
            }),
            Ok(())
        );
        let project = ProjectRootTemp::new().unwrap();
        let manifest = side(project.path()).manifest();
        assert!(manifest.env_vars.contains_key("PATH"));
        for name in ["BUILDBUDDY_API_KEY", "FOO_TOKEN"] {
            assert!(!manifest.env_vars.contains_key(name), "{name}");
        }
        let text = serde_json::to_string(&manifest).unwrap();
        let key_digest = &blake3_hex(b"main-key")[..16];
        assert!(!text.contains(key_digest));
    }

    #[test]
    fn suspend_env_ignore_replaces_the_default_list() {
        let project = ProjectRootTemp::new().unwrap();
        let root = root_of(&ROOT.replace(
            "suspend = true\n",
            "suspend = true\nsuspend_env_ignore = CI_*\n",
        ));
        let manifest = side_with(project.path(), &root).manifest();
        let resume = |change: &dyn Fn(&mut Side)| {
            let mut s = side_with(project.path(), &root);
            change(&mut s);
            compare(&manifest, &s.observed())
        };
        assert_eq!(resume(&|s| set(&mut s.env, "CI_JOB_ID", "9")), Ok(()));
        assert_eq!(
            resume(&|s| set(&mut s.env, "GIT_COMMIT", "bbb")),
            Err(Refusal::Env(vec!["GIT_COMMIT".to_owned()]))
        );
        // A list that differs is a startup config that differs.
        assert_eq!(
            compare(
                &side(project.path()).manifest(),
                &side_with(project.path(), &root).observed()
            ),
            Err(Refusal::StartupConfig)
        );
    }

    #[test]
    fn another_root_config_value_refuses() {
        assert_eq!(
            resume_after(|s, project| {
                let root = root_of(&ROOT.replace("k = 1", "k = 2"));
                s.identity = identity_of(&startup_of(&root), &root, project, "v2");
            }),
            Err(Refusal::RootConfig)
        );
    }

    #[test]
    fn another_project_or_isolation_refuses() {
        let other = ProjectRootTemp::new().unwrap();
        assert_eq!(
            resume_after(|s, _| {
                let root = root_of(ROOT);
                s.identity = identity_of(&startup_of(&root), &root, other.path(), "v2");
            }),
            Err(Refusal::Project)
        );
        assert_eq!(
            resume_after(|s, project| {
                let root = root_of(ROOT);
                s.identity = identity_of(&startup_of(&root), &root, project, "v3");
            }),
            Err(Refusal::Project)
        );
    }

    #[test]
    fn another_user_refuses() {
        assert_eq!(resume_after(|s, _| s.uid = 1001), Err(Refusal::User));
    }

    #[test]
    fn another_watcher_refuses() {
        // The watcher is a root config value, so a real change refuses as the root config first;
        // this checks the watcher's own comparison.
        assert_eq!(
            resume_after(|s, _| s.identity.watcher = "fs_hash_crawler".to_owned()),
            Err(Refusal::Watcher)
        );
    }

    #[test]
    fn a_clock_a_little_behind_resumes_and_one_far_behind_refuses() {
        assert_eq!(resume_after(|s, _| s.now_ms -= 60_000), Ok(()));
        assert_eq!(
            resume_after(|s, _| s.now_ms -= 10 * 60_000),
            Err(Refusal::Clock)
        );
        assert_eq!(resume_after(|s, _| s.now_ms += 86_400_000), Ok(()));
    }

    #[test]
    fn another_format_or_magic_refuses() {
        let project = ProjectRootTemp::new().unwrap();
        let s = side(project.path());
        let mut m = s.manifest();
        m.format = 1;
        assert!(matches!(
            compare(&m, &s.observed()),
            Err(Refusal::Format(_))
        ));
        let mut m = s.manifest();
        m.magic = "OTHER".to_owned();
        assert!(matches!(
            compare(&m, &s.observed()),
            Err(Refusal::Format(_))
        ));
    }

    #[test]
    fn a_format_1_manifest_refuses_as_the_format() {
        // FORMAT 1 had `hostname` and no `host_identity`: its MANIFEST must refuse as another
        // format, not as an unreadable one.
        let text = br#"{"magic":"BUCK2SUS","format":1,"exe":"e","hostname":"h"}"#;
        assert!(matches!(parse_manifest(text), Err(Refusal::Format(_))));
        assert!(matches!(
            parse_manifest(b"{not json"),
            Err(Refusal::Format(_))
        ));
        let project = ProjectRootTemp::new().unwrap();
        let m = side(project.path()).manifest();
        let mut text = serde_json::to_vec(&m).unwrap();
        assert!(parse_manifest(&text).is_ok());
        // A MANIFEST of this format with a field gone is corrupt.
        let cut = String::from_utf8(text.clone())
            .unwrap()
            .replace(r#""uid":1000,"#, "");
        text = cut.into_bytes();
        assert!(matches!(
            parse_manifest(&text),
            Err(Refusal::ImageCorrupt(_))
        ));
    }

    #[test]
    fn a_paging_store_over_the_cap_refuses() {
        let project = ProjectRootTemp::new().unwrap();
        let s = side(project.path());
        let mut m = s.manifest();
        m.dice_state_bytes = (s.identity.config.max_db_gb << 30) + 1;
        assert!(matches!(
            compare(&m, &s.observed()),
            Err(Refusal::TooLarge { .. })
        ));
    }

    #[test]
    fn the_refusal_names_the_identity() {
        assert_eq!(
            Refusal::HostIdentity.to_string(),
            "the host identity differs from the suspended daemon's"
        );
    }

    #[test]
    fn a_missing_paging_store_or_a_damaged_image_refuses() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path();
        let image_dir = cache.join(IMAGE_DIR);
        std::fs::create_dir_all(&image_dir).unwrap();
        let bytes = b"not an image".to_vec();
        std::fs::write(image_dir.join(IMAGE), &bytes).unwrap();
        let project = ProjectRootTemp::new().unwrap();
        let mut m = side(project.path()).manifest();
        m.image_len = bytes.len() as u64;
        m.image = blake3_hex(&bytes);

        assert!(matches!(
            check_files(&m, &image_dir, cache),
            Err(Refusal::NoPagingStore)
        ));
        std::fs::create_dir_all(cache.join(DICE_STATE_DIR)).unwrap();
        // The digest matches, but the bytes are no image.
        assert!(matches!(
            check_files(&m, &image_dir, cache),
            Err(Refusal::ImageCorrupt(_))
        ));
        let mut flipped = bytes.clone();
        flipped[3] ^= 1;
        std::fs::write(image_dir.join(IMAGE), &flipped).unwrap();
        assert_eq!(
            check_files(&m, &image_dir, cache).err(),
            Some(Refusal::ImageCorrupt(
                "the blake3 digest differs".to_owned()
            ))
        );
        std::fs::write(image_dir.join(IMAGE), &bytes[..4]).unwrap();
        assert_eq!(
            check_files(&m, &image_dir, cache).err(),
            Some(Refusal::ImageCorrupt("truncated".to_owned()))
        );
        std::fs::remove_file(image_dir.join(IMAGE)).unwrap();
        assert!(matches!(
            check_files(&m, &image_dir, cache),
            Err(Refusal::ImageCorrupt(_))
        ));
    }

    #[test]
    fn per_run_variables_are_left_out() {
        assert!(ignored("GITHUB_RUN_ID", &[]));
        assert!(ignored("TERM", &[]));
        assert!(ignored("HOSTNAME", &[]));
        assert!(ignored("BUILDBUDDY_API_KEY", &[]));
        assert!(ignored("ANY_TOKEN", &[]));
        assert!(!ignored("PATH", &[]));
        assert!(!ignored("TOKEN_FILE", &[]));
        assert!(ignored("CI_JOB_ID", &["CI_JOB_ID".to_owned()]));
        assert!(ignored("ACTIONS_X", &["ACTIONS_*".to_owned()]));
    }

    #[test]
    fn config_digest_follows_values_but_not_the_identity() -> buck2_error::Result<()> {
        let a = parse(&[("c", "[s]\nk = 1\n")], "c")?;
        let b = parse(&[("c", "[s]\nk = 1\n")], "c")?;
        let c = parse(&[("c", "[s]\nk = 2\n")], "c")?;
        let d = parse(
            &[("c", "[s]\nk = 1\n[buck2_hydration]\nhost_identity = x\n")],
            "c",
        )?;
        assert_eq!(config_digest(&a), config_digest(&b));
        assert_ne!(config_digest(&a), config_digest(&c));
        assert_eq!(config_digest(&a), config_digest(&d));
        let e = parse(&[("c", "[t]\nk = 1\n")], "c")?;
        assert_ne!(config_digest(&a), config_digest(&e));
        Ok(())
    }

    #[test]
    fn manifest_round_trips() {
        let project = ProjectRootTemp::new().unwrap();
        let m = side(project.path()).manifest();
        let back: Manifest = serde_json::from_slice(&serde_json::to_vec(&m).unwrap()).unwrap();
        assert_eq!(back.env_vars, m.env_vars);
        assert_eq!(back.host_identity, "A");
        assert_eq!(back.created_at_ms, m.created_at_ms);
        let run = m.run_env.clone();
        assert!(!back.dirties_local_actions("suspend-host", &run));
        assert!(back.dirties_local_actions("other", &run));
        let portable = Manifest {
            portable: true,
            ..back
        };
        assert!(portable.dirties_local_actions("suspend-host", &run));
    }

    /// Only a non-portable image under the hostname that wrote it compares stats; a portable
    /// one is another machine's files even when the hostname matches.
    #[test]
    fn a_portable_image_is_always_another_machine() {
        let project = ProjectRootTemp::new().unwrap();
        let m = side(project.path()).manifest();
        let host = m.hostname.clone();
        assert!(!m.cross_host(&host));
        assert!(m.cross_host("other"));
        let portable = Manifest {
            portable: true,
            ..m
        };
        assert!(portable.cross_host(&host));
    }

    /// The environment check leaves the per-run variables out, which is sound only because a
    /// resume that sees other values runs every local action again, on the same host too. A
    /// change of a shell's variables or of a credential does neither.
    #[test]
    fn per_run_variables_that_change_dirty_local_actions() {
        let project = ProjectRootTemp::new().unwrap();
        let suspended = side(project.path());
        let manifest = suspended.manifest();
        let dirties = |name: &str, value: &str| {
            let mut resuming = side(project.path());
            set(&mut resuming.env, name, value);
            let o = resuming.observed();
            assert_eq!(compare(&manifest, &o), Ok(()), "{name}");
            manifest.dirties_local_actions("suspend-host", &o.env.run)
        };
        assert!(!dirties("PATH", "/bin"));
        for (name, value) in [
            ("GITHUB_SHA", "bbb"),
            ("RUNNER_NAME", "runner-2"),
            ("HOSTNAME", "runner-2"),
            ("GIT_COMMIT", "bbb"),
            ("ACTIONS_ID_TOKEN_REQUEST_URL", "https://a/2"),
            ("ImageVersion", "20261002.1"),
        ] {
            assert!(dirties(name, value), "{name}");
        }
        for (name, value) in [
            ("TERM", "dumb"),
            ("PWD", "/elsewhere"),
            ("BUILDBUDDY_API_KEY", "pr-key"),
            ("FOO_TOKEN", "t2"),
        ] {
            assert!(!dirties(name, value), "{name}");
        }
    }

    /// [`check`] as a resume runs it, against this process: an image it accepts, and whether
    /// that image counts as another machine's files and dirties the local actions. The hostname
    /// is not checked, but it and `portable` decide `cross_host`.
    #[test]
    fn check_tells_another_host_and_dirties_local_actions() {
        use buck2_common::file_ops::baseline::FileSystemBaseline;
        use buck2_file_watcher::file_watcher::FileWatcherImageKind;

        let project = ProjectRootTemp::new().unwrap();
        let identity = side(project.path()).identity;
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path();
        let image_dir = cache.join(IMAGE_DIR);
        std::fs::create_dir_all(&image_dir).unwrap();
        std::fs::create_dir_all(cache.join(DICE_STATE_DIR)).unwrap();
        let file = ImageFile {
            dice: Vec::new(),
            artifact_rows: Vec::new(),
            artifact_count: 0,
            local_actions: [0; 16],
            watcher: FileWatcherImage {
                kind: FileWatcherImageKind::Notify,
                baseline: FileSystemBaseline::unique().to_image(),
                snapshot: serde_json::from_str(r#"{"started":[0,0],"entries":[]}"#).unwrap(),
                vcs: None,
            },
        };
        let bytes = bincode::serde::encode_to_vec(&file, bincode::config::standard()).unwrap();
        std::fs::write(image_dir.join(IMAGE), &bytes).unwrap();
        let env = env_digest(&identity.config.env_ignore);
        let here = buck2_events::metadata::hostname().unwrap_or_default();
        let manifest = |hostname: &str, portable: bool, run_env: &str| {
            serde_json::to_vec(&Manifest {
                magic: MAGIC.to_owned(),
                format: FORMAT,
                exe: exe_digest().unwrap(),
                startup_config: identity.startup_config.clone(),
                env: env.digest.clone(),
                env_vars: env.vars.clone(),
                root_config: identity.root_config.clone(),
                project_root: identity.project_root.clone(),
                isolation: identity.isolation.clone(),
                host_identity: identity.host_identity.clone(),
                hostname: hostname.to_owned(),
                uid: uid(),
                watcher: identity.watcher.clone(),
                created_at_ms: now_ms(),
                image_len: bytes.len() as u64,
                image: blake3_hex(&bytes),
                dice_state_bytes: 0,
                portable,
                run_env: run_env.to_owned(),
            })
            .unwrap()
        };
        let resume = |manifest: Vec<u8>| -> (bool, bool) {
            let (_, cross_host, dirty) = check(&manifest, &image_dir, cache, &identity)
                .unwrap_or_else(|r| panic!("refused: {r}"));
            (cross_host, dirty)
        };
        assert_eq!(resume(manifest(&here, false, &env.run)), (false, false));
        assert_eq!(resume(manifest(&here, true, &env.run)), (true, true));
        assert_eq!(resume(manifest(&here, false, "other")), (false, true));
        let elsewhere = format!("{here}-elsewhere");
        assert_eq!(resume(manifest(&elsewhere, false, &env.run)), (true, true));

        // An image is resumed once: the start that reads its MANIFEST claims it, and the next
        // start finds none.
        std::fs::write(image_dir.join(MANIFEST), manifest(&here, false, &env.run)).unwrap();
        assert!(matches!(
            prepare_resume(cache, &identity),
            ResumePrep::Ready(_)
        ));
        assert!(!image_dir.join(MANIFEST).exists());
        assert!(matches!(prepare_resume(cache, &identity), ResumePrep::None));
    }

    /// The text of the refusal [`LoadedImage::restore`] returns when the materializer lost
    /// artifacts; that it is returned is checked end to end, by `tests/suspend/e2e-re.sh`'s
    /// non-portable image on a fresh checkout.
    #[test]
    fn a_lost_artifact_refuses_as_not_portable() {
        let text = Refusal::NotPortable { lost: 2 }.to_string();
        assert!(
            text.starts_with("the image is not portable: 2 materialized artifacts"),
            "{text}"
        );
    }

    /// The replay and the graph read of [`overlap_then_install`], as fakes: each records that it
    /// ran to its end, and `install` records what it was handed.
    mod overlap {
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering;

        use buck2_execute::materialize::materializer::ResumedArtifacts;

        use super::super::overlap_then_install;

        fn artifacts(lost: usize) -> ResumedArtifacts {
            ResumedArtifacts {
                reactivated: 3,
                lost,
                ..Default::default()
            }
        }

        async fn slow<T>(done: &AtomicBool, value: T) -> T {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            done.store(true, Ordering::SeqCst);
            value
        }

        /// Runs [`overlap_then_install`] and returns its result and whether `install` ran.
        async fn run(
            replay: impl Future<Output = Result<ResumedArtifacts, String>>,
            read: impl Future<Output = Result<u32, String>>,
        ) -> (Result<(ResumedArtifacts, u32), String>, bool) {
            let installed = AtomicBool::new(false);
            let result = overlap_then_install(replay, read, |p| {
                installed.store(true, Ordering::SeqCst);
                async move { p * 2 }
            })
            .await;
            (result, installed.load(Ordering::SeqCst))
        }

        #[tokio::test]
        async fn both_succeed_and_the_read_is_installed() {
            let (result, installed) = run(async { Ok(artifacts(0)) }, async { Ok(21) }).await;
            assert_eq!(result, Ok((artifacts(0), 42)));
            assert!(installed);
        }

        #[tokio::test]
        async fn a_lost_artifact_refuses_before_the_graph_is_installed() {
            let read_done = AtomicBool::new(false);
            let (result, installed) =
                run(async { Ok(artifacts(1)) }, slow(&read_done, Ok(21))).await;
            let err = result.unwrap_err();
            assert!(
                err.starts_with("the image is not portable: 1 materialized artifacts"),
                "{err}"
            );
            assert!(!installed, "a refused graph must not be installed");
            assert!(
                read_done.load(Ordering::SeqCst),
                "the read is awaited to its end"
            );
        }

        #[tokio::test]
        async fn a_graph_error_refuses_after_the_replay_has_finished() {
            let replay_done = AtomicBool::new(false);
            let (result, installed) = run(slow(&replay_done, Ok(artifacts(0))), async {
                Err("graph: unreadable row".to_owned())
            })
            .await;
            assert_eq!(result, Err("graph: unreadable row".to_owned()));
            assert!(!installed);
            assert!(
                replay_done.load(Ordering::SeqCst),
                "no replay command may still be on its way when the daemon starts cold"
            );
        }

        #[tokio::test]
        async fn a_materializer_error_is_reported_first() {
            let (result, installed) =
                run(async { Err("materializer: closed".to_owned()) }, async {
                    Err("graph: unreadable row".to_owned())
                })
                .await;
            assert_eq!(result, Err("materializer: closed".to_owned()));
            assert!(!installed);
        }

        #[tokio::test]
        async fn a_lost_artifact_is_reported_before_a_graph_error() {
            let (result, installed) = run(async { Ok(artifacts(2)) }, async {
                Err("graph: unreadable row".to_owned())
            })
            .await;
            assert!(
                result
                    .unwrap_err()
                    .starts_with("the image is not portable: 2")
            );
            assert!(!installed);
        }

        /// The two run at once: a replay that waits for the read to start would hang if they
        /// ran one after the other.
        #[tokio::test]
        async fn the_replay_and_the_read_overlap() {
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let replay = async move {
                rx.await.map_err(|e| e.to_string())?;
                Ok(artifacts(0))
            };
            let read = async move {
                drop(tx.send(()));
                Ok(21)
            };
            let (result, installed) =
                tokio::time::timeout(std::time::Duration::from_secs(10), run(replay, read))
                    .await
                    .expect("the replay waited for a read that never started");
            assert_eq!(result, Ok((artifacts(0), 42)));
            assert!(installed);
        }
    }

    /// A store path is valid only if it exists here and the configured check accepts it; a
    /// check that cannot run rejects it.
    #[test]
    fn a_store_path_is_valid_only_if_it_exists_and_passes_the_check() {
        let dir = tempfile::tempdir().unwrap();
        let present = dir.path().join("abc-tool");
        std::fs::write(&present, "tool").unwrap();
        let present = present.to_str().unwrap();
        let missing = dir.path().join("abd-gone");
        let missing = missing.to_str().unwrap();
        let words = |c: &str| c.split_whitespace().map(ToOwned::to_owned).collect::<Vec<_>>();
        assert!(store_path_valid(present, &[]));
        assert!(!store_path_valid(missing, &[]));
        assert!(store_path_valid(present, &words("true")));
        assert!(!store_path_valid(present, &words("false")));
        assert!(!store_path_valid(missing, &words("true")));
        // The path is the check's last argument.
        assert!(store_path_valid(present, &words("test -f")));
        assert!(!store_path_valid(present, &words("test -d")));
        assert!(!store_path_valid(present, &words("/nonexistent/check")));
    }

    /// The dirtying a resume applies to locally executed actions: the dirtied key computes
    /// again in the next transaction, and a key that was not dirtied keeps its value.
    #[tokio::test]
    async fn dirtied_keys_compute_again() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        use allocative::Allocative;
        use async_trait::async_trait;
        use dice::DetectCycles;
        use dice::DiceComputations;
        use dice::DiceKeyDyn;
        use dice::EqualityBehavior;
        use dice::Key;
        use dice_futures::cancellation::CancellationContext;
        use pagable::Pagable;
        use pagable::pagable_typetag;

        static COMPUTES: [AtomicUsize; 2] = [AtomicUsize::new(0), AtomicUsize::new(0)];

        #[derive(Allocative, Clone, Copy, Debug, Eq, PartialEq, Hash, Pagable)]
        #[pagable_typetag(DiceKeyDyn)]
        struct Counted(usize);

        impl std::fmt::Display for Counted {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "Counted({})", self.0)
            }
        }

        #[async_trait]
        impl Key for Counted {
            type Value = usize;

            async fn compute(
                &self,
                _ctx: &mut DiceComputations,
                _cancellations: &CancellationContext,
            ) -> Self::Value {
                COMPUTES[self.0].fetch_add(1, Ordering::SeqCst) + 1
            }

            fn value_serialize() -> impl dice::ValueSerialize<Value = Self::Value> {
                dice::NoValueSerialize::<Self::Value>::new()
            }

            fn equality_behavior() -> EqualityBehavior<Self::Value> {
                EqualityBehavior::Compare(|x, y| x == y)
            }
        }

        let dice = Dice::builder().build(DetectCycles::Enabled);
        let root = dice.new_root().await;
        let ctx = dice.updater(root).commit().await;
        assert_eq!(*ctx.compute(&Counted(0)).await.unwrap(), 1);
        assert_eq!(*ctx.compute(&Counted(1)).await.unwrap(), 1);
        drop(ctx);

        dirty_keys(&dice, root, vec![Counted(0)]).await.unwrap();
        let ctx = dice.updater(root).commit().await;
        assert_eq!(*ctx.compute(&Counted(0)).await.unwrap(), 2);
        assert_eq!(*ctx.compute(&Counted(1)).await.unwrap(), 1);
        assert_eq!(COMPUTES[1].load(Ordering::SeqCst), 1);
        drop(ctx);

        // A key listed twice is dirtied once: DICE refuses a key changed twice in one
        // transaction, and a refusal there would leave a restored graph undirtied.
        assert_eq!(
            dirty_keys(&dice, root, vec![Counted(1), Counted(1)])
                .await
                .unwrap(),
            1
        );
        let ctx = dice.updater(root).commit().await;
        assert_eq!(*ctx.compute(&Counted(1)).await.unwrap(), 2);
        assert_eq!(*ctx.compute(&Counted(0)).await.unwrap(), 2);
    }
}
