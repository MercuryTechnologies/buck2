/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! A suspend image as one file, to carry to another machine: `buck2 suspend --export` writes it
//! after the daemon exits, `buck2 resume --import` unpacks it before a daemon starts.
//!
//! The file is a zstd-compressed tar, written in one pass and hashed while it is written:
//!
//! - `BUNDLE` first, the [`BundleHeader`], so an import refuses a bundle from another binary or
//!   image format before it reads anything else;
//! - `suspend/image.bin`, then every file of `dice_state/`, then `suspend/MANIFEST`;
//! - `BUNDLE.files` last, each member's path, length and blake3, `BUNDLE` included.
//!
//! A bundle is not signed. The per-member digests catch damage in transport; what keeps a
//! deliberately poisoned bundle out is who may write to the store the bundles are kept in.

use std::fs::File;
use std::io;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::path::Path;

use buck2_common::suspend_image::DICE_STATE_DIR;
use buck2_common::suspend_image::FORMAT;
use buck2_common::suspend_image::IMAGE;
use buck2_common::suspend_image::IMAGE_DIR;
use buck2_common::suspend_image::MAGIC;
use buck2_common::suspend_image::MANIFEST;
use buck2_common::suspend_image::ManifestHead;
use buck2_error::BuckErrorContext;
use buck2_error::ErrorTag;
use buck2_error::buck2_error;
use serde::Deserialize;
use serde::Serialize;

const BUNDLE_MAGIC: &str = "BUCK2SUSBUNDLE";
/// Bumped on any change to the bundle's layout, which is independent of the image's format.
const BUNDLE_FORMAT: u32 = 1;
const HEADER: &str = "BUNDLE";
const TRAILER: &str = "BUNDLE.files";
const ZSTD_LEVEL: i32 = 3;

/// The first member of a bundle.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct BundleHeader {
    magic: String,
    bundle_format: u32,
    /// The image's `FORMAT`, from its MANIFEST.
    image_format: u32,
    /// The blake3 of the binary that wrote the image, from its MANIFEST.
    exe: String,
    created_at_ms: u64,
}

/// One member as `BUNDLE.files` lists it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BundleMember {
    path: String,
    len: u64,
    blake3: String,
}

/// The last member of a bundle.
#[derive(Serialize, Deserialize, Debug)]
struct BundleFiles {
    files: Vec<BundleMember>,
}

/// Counts what passes through, and hashes it.
struct Hashing<T> {
    inner: T,
    hasher: blake3::Hasher,
    len: u64,
}

impl<T> Hashing<T> {
    fn new(inner: T) -> Self {
        Self {
            inner,
            hasher: blake3::Hasher::new(),
            len: 0,
        }
    }

    fn member(&self, path: &str) -> BundleMember {
        BundleMember {
            path: path.to_owned(),
            len: self.len,
            blake3: self.hasher.finalize().to_hex().to_string(),
        }
    }
}

impl<R: Read> Read for Hashing<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        self.len += n as u64;
        Ok(n)
    }
}

impl<W: Write> Write for Hashing<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.len += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// What an export wrote.
#[derive(Debug)]
pub(crate) struct ExportSummary {
    pub(crate) members: usize,
    /// The members' bytes before compression.
    pub(crate) bytes: u64,
    /// The bundle's size.
    pub(crate) compressed: u64,
}

/// The files of `dice_state/`, relative to the cache dir with `/` separators, sorted.
fn dice_state_files(cache_dir: &Path) -> buck2_error::Result<Vec<String>> {
    let root = cache_dir.join(DICE_STATE_DIR);
    if !root.is_dir() {
        return Err(buck2_error!(
            ErrorTag::Input,
            "The suspended daemon's paging store `{}` is missing",
            root.display()
        ));
    }
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(&root)
        .follow_links(false)
        .sort_by_file_name()
    {
        let entry = entry
            .map_err(io::Error::from)
            .buck_error_context("Listing the paging store")?;
        let kind = entry.file_type();
        if kind.is_dir() {
            continue;
        }
        if !kind.is_file() {
            return Err(buck2_error!(
                ErrorTag::Input,
                "`{}` in the paging store is not a regular file",
                entry.path().display()
            ));
        }
        let rel = entry
            .path()
            .strip_prefix(cache_dir)
            .buck_error_context("A paging store file outside the cache dir")?;
        let rel = rel.to_str().ok_or_else(|| {
            buck2_error!(
                ErrorTag::Input,
                "`{}` is not a UTF-8 path",
                entry.path().display()
            )
        })?;
        files.push(rel.replace(std::path::MAIN_SEPARATOR, "/"));
    }
    Ok(files)
}

fn tar_header(len: u64) -> tar::Header {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Regular);
    header.set_size(len);
    header.set_mode(0o644);
    header.set_mtime(0);
    header
}

/// Appends `len` bytes of `data` as `path`, and returns the member as `BUNDLE.files` lists it.
fn append<W: Write>(
    tar: &mut tar::Builder<W>,
    path: &str,
    len: u64,
    data: impl Read,
) -> buck2_error::Result<BundleMember> {
    let mut header = tar_header(len);
    let mut data = Hashing::new(data.take(len));
    tar.append_data(&mut header, path, &mut data)
        .with_buck_error_context(|| format!("Writing `{path}` into the bundle"))?;
    if data.len != len {
        return Err(buck2_error!(
            ErrorTag::Environment,
            "`{path}` shrank from {len} to {} bytes while it was bundled",
            data.len
        ));
    }
    Ok(data.member(path))
}

fn append_file<W: Write>(
    tar: &mut tar::Builder<W>,
    cache_dir: &Path,
    path: &str,
) -> buck2_error::Result<BundleMember> {
    let src = cache_dir.join(path);
    let file =
        File::open(&src).with_buck_error_context(|| format!("Opening `{}`", src.display()))?;
    let len = file
        .metadata()
        .with_buck_error_context(|| format!("Reading `{}`", src.display()))?
        .len();
    append(tar, path, len, file)
}

fn append_json<W: Write, T: Serialize>(
    tar: &mut tar::Builder<W>,
    path: &str,
    value: &T,
) -> buck2_error::Result<BundleMember> {
    let bytes = serde_json::to_vec_pretty(value)
        .with_buck_error_context(|| format!("Encoding `{path}`"))?;
    append(tar, path, bytes.len() as u64, bytes.as_slice())
}

fn worker_threads() -> u32 {
    std::thread::available_parallelism().map_or(1, |n| n.get() as u32)
}

/// Writes the image under `cache_dir` to `out` as a bundle. The image is left where it is, so
/// this machine can still resume from it. `out` appears only once it is complete.
pub(crate) fn export(cache_dir: &Path, out: &Path) -> buck2_error::Result<ExportSummary> {
    let manifest_rel = format!("{IMAGE_DIR}/{MANIFEST}");
    let manifest_path = cache_dir.join(&manifest_rel);
    if !manifest_path.exists() {
        return Err(buck2_error!(
            ErrorTag::Input,
            "No suspended state to export: `{}` is missing",
            manifest_path.display()
        ));
    }
    let manifest = ManifestHead::read(&manifest_path)?;
    if manifest.magic != MAGIC || manifest.format != FORMAT {
        return Err(buck2_error!(
            ErrorTag::Input,
            "The suspended state is image format {} {}, and this buck2 writes {MAGIC} {FORMAT}",
            manifest.magic,
            manifest.format
        ));
    }
    let header = BundleHeader {
        magic: BUNDLE_MAGIC.to_owned(),
        bundle_format: BUNDLE_FORMAT,
        image_format: manifest.format,
        exe: manifest.exe,
        created_at_ms: manifest.created_at_ms,
    };
    let dice_state = dice_state_files(cache_dir)?;

    let file_name = out
        .file_name()
        .ok_or_else(|| buck2_error!(ErrorTag::Input, "`{}` names no file", out.display()))?;
    let mut tmp_name = file_name.to_owned();
    tmp_name.push(format!(".tmp.{}", std::process::id()));
    let tmp = out.with_file_name(tmp_name);
    let result = write_bundle(cache_dir, &tmp, &header, &dice_state, &manifest_rel);
    match result {
        Ok(summary) => {
            std::fs::rename(&tmp, out).with_buck_error_context(|| {
                format!("Renaming the bundle to `{}`", out.display())
            })?;
            Ok(summary)
        }
        Err(e) => {
            drop(std::fs::remove_file(&tmp));
            Err(e)
        }
    }
}

fn write_bundle(
    cache_dir: &Path,
    tmp: &Path,
    header: &BundleHeader,
    dice_state: &[String],
    manifest_rel: &str,
) -> buck2_error::Result<ExportSummary> {
    let file =
        File::create(tmp).with_buck_error_context(|| format!("Creating `{}`", tmp.display()))?;
    let mut encoder = zstd::stream::write::Encoder::new(BufWriter::new(file), ZSTD_LEVEL)
        .buck_error_context("Starting the zstd encoder")?;
    encoder
        .include_checksum(true)
        .buck_error_context("Configuring the zstd encoder")?;
    encoder
        .multithread(worker_threads())
        .buck_error_context("Configuring the zstd encoder")?;
    let mut tar = tar::Builder::new(encoder);

    let mut files = vec![append_json(&mut tar, HEADER, header)?];
    files.push(append_file(
        &mut tar,
        cache_dir,
        &format!("{IMAGE_DIR}/{IMAGE}"),
    )?);
    for path in dice_state {
        files.push(append_file(&mut tar, cache_dir, path)?);
    }
    // The MANIFEST after what it vouches for, as the daemon writes it.
    files.push(append_file(&mut tar, cache_dir, manifest_rel)?);
    let members = files.len() + 1;
    let bytes = files.iter().map(|m| m.len).sum();
    append_json(&mut tar, TRAILER, &BundleFiles { files })?;

    let encoder = tar.into_inner().buck_error_context("Finishing the tar")?;
    let mut writer = encoder
        .finish()
        .buck_error_context("Finishing the zstd stream")?;
    writer.flush().buck_error_context("Writing the bundle")?;
    let file = writer
        .into_inner()
        .map_err(|e| e.into_error())
        .buck_error_context("Writing the bundle")?;
    file.sync_all().buck_error_context("Syncing the bundle")?;
    let compressed = file
        .metadata()
        .buck_error_context("Reading the bundle")?
        .len();
    Ok(ExportSummary {
        members,
        bytes,
        compressed,
    })
}

/// The largest `BUNDLE` and `BUNDLE.files` an import reads into memory. A real header is a few
/// hundred bytes and a listing about 150 bytes per paging-store file.
const MAX_HEADER_BYTES: u64 = 64 << 10;
const MAX_TRAILER_BYTES: u64 = 64 << 20;

/// Why an import did not install a bundle. Every refusal leaves the cache dir as it was.
#[derive(Debug)]
pub(crate) enum ImportError {
    DaemonRunning(u32),
    NotABundle(String),
    BundleFormat(u32),
    ImageFormat(u32),
    OtherBinary {
        bundle: String,
        this: String,
    },
    /// A member whose path or kind no bundle has: absolute, `..`, a link, a device, or a name
    /// outside the image.
    BadMember(String),
    Duplicate(String),
    AfterTrailer(String),
    NoTrailer,
    /// A member whose bytes differ from what `BUNDLE.files` says, or that it does not list.
    Mismatch(String),
    /// A member `BUNDLE.files` lists that the bundle does not hold.
    Missing(String),
    /// The compressed stream does not end where the tar does, or fails its checksum there.
    Damaged(String),
    Io {
        what: String,
        error: io::Error,
    },
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImportError::DaemonRunning(pid) => write!(
                f,
                "a buck2 daemon (pid {pid}) is running for this checkout; run `buck2 kill` first"
            ),
            ImportError::NotABundle(why) => write!(f, "not a suspend bundle: {why}"),
            ImportError::BundleFormat(v) => {
                write!(f, "bundle format {v}, and this buck2 reads {BUNDLE_FORMAT}")
            }
            ImportError::ImageFormat(v) => {
                write!(f, "image format {v}, and this buck2 reads {FORMAT}")
            }
            ImportError::OtherBinary { bundle, this } => write!(
                f,
                "the bundle was written by another buck2 binary (blake3 {bundle}, this one is {this})"
            ),
            ImportError::BadMember(p) => write!(f, "member `{p}` has no place in a bundle"),
            ImportError::Duplicate(p) => write!(f, "member `{p}` appears twice"),
            ImportError::AfterTrailer(p) => write!(f, "member `{p}` follows `{TRAILER}`"),
            ImportError::NoTrailer => write!(f, "`{TRAILER}` is missing: the bundle is truncated"),
            ImportError::Mismatch(p) => {
                write!(
                    f,
                    "member `{p}` does not match `{TRAILER}`: the bundle is damaged"
                )
            }
            ImportError::Missing(p) => write!(f, "member `{p}` is missing"),
            ImportError::Damaged(e) => {
                write!(f, "the compressed stream is damaged or truncated: {e}")
            }
            ImportError::Io { what, error } => write!(f, "{what}: {error}"),
        }
    }
}

impl From<ImportError> for buck2_error::Error {
    fn from(e: ImportError) -> Self {
        let tag = match &e {
            ImportError::Io { .. } => ErrorTag::Environment,
            _ => ErrorTag::Input,
        };
        buck2_error!(tag, "Not importing the suspend bundle: {e}")
    }
}

fn io_err(what: impl FnOnce() -> String) -> impl FnOnce(io::Error) -> ImportError {
    move |error| ImportError::Io {
        what: what(),
        error,
    }
}

/// What an import installed.
#[derive(Debug)]
pub(crate) struct ImportSummary {
    pub(crate) members: usize,
    pub(crate) bytes: u64,
    pub(crate) created_at_ms: u64,
}

/// A member's path, if it is one a bundle may hold: relative, plain names only, `suspend/` or
/// `dice_state/`, regular file.
fn member_path<R: Read>(entry: &tar::Entry<'_, R>) -> Result<String, ImportError> {
    let raw = entry.path_bytes();
    let shown = String::from_utf8_lossy(&raw).into_owned();
    let Ok(text) = std::str::from_utf8(&raw) else {
        return Err(ImportError::BadMember(shown));
    };
    let parts: Vec<&str> = text.split('/').collect();
    let plain = parts.iter().all(|p| {
        !p.is_empty() && *p != "." && *p != ".." && !p.contains('\0') && !p.contains('\\')
    });
    if !plain || !entry.header().entry_type().is_file() {
        return Err(ImportError::BadMember(shown));
    }
    let placed = match parts.as_slice() {
        [h] => *h == HEADER || *h == TRAILER,
        [d, f] if *d == IMAGE_DIR => *f == IMAGE || *f == MANIFEST,
        [d, _, ..] => *d == DICE_STATE_DIR,
        _ => false,
    };
    if !placed {
        return Err(ImportError::BadMember(shown));
    }
    Ok(text.to_owned())
}

fn read_small<R: Read>(
    entry: &mut tar::Entry<'_, R>,
    path: &str,
    max: u64,
) -> Result<(Vec<u8>, BundleMember), ImportError> {
    let len = entry
        .header()
        .size()
        .map_err(io_err(|| format!("Reading `{path}`")))?;
    if len > max {
        return Err(ImportError::NotABundle(format!("`{path}` is {len} bytes")));
    }
    let mut reader = Hashing::new(entry);
    let mut bytes = Vec::with_capacity(len as usize);
    reader
        .read_to_end(&mut bytes)
        .map_err(io_err(|| format!("Reading `{path}`")))?;
    let member = reader.member(path);
    Ok((bytes, member))
}

/// Checks the bundle's header against this binary: the refusals that need no unpacking.
fn check_header(header: &BundleHeader, exe: &str) -> Result<(), ImportError> {
    if header.magic != BUNDLE_MAGIC {
        return Err(ImportError::NotABundle(format!("magic {}", header.magic)));
    }
    if header.bundle_format != BUNDLE_FORMAT {
        return Err(ImportError::BundleFormat(header.bundle_format));
    }
    if header.image_format != FORMAT {
        return Err(ImportError::ImageFormat(header.image_format));
    }
    if header.exe != exe {
        return Err(ImportError::OtherBinary {
            bundle: header.exe.clone(),
            this: exe.to_owned(),
        });
    }
    Ok(())
}

/// Unpacks a bundle into `cache_dir`, replacing any image there, and returns what it installed.
///
/// `exe` is the blake3 of the binary the daemon will run, and `running_daemon` the pid of a
/// daemon that is up for this checkout, if any: both are refused before anything is read.
/// Everything is unpacked into a directory of its own and checked against `BUNDLE.files` first;
/// only then does the old image go and the new one move in, the MANIFEST last, so a crash at any
/// point leaves either the old image or no MANIFEST, and the next start is cold.
pub(crate) fn import(
    bundle: &Path,
    cache_dir: &Path,
    exe: &str,
    running_daemon: Option<u32>,
) -> Result<ImportSummary, ImportError> {
    import_with(bundle, cache_dir, exe, running_daemon, &mut |_| Ok(()))
}

/// The steps of installing an unpacked bundle, for tests to fail at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstallStep {
    OldRemoved,
    DiceStateInstalled,
    ImageInstalled,
}

type InstallHook<'a> = dyn FnMut(InstallStep) -> io::Result<()> + 'a;

fn import_with(
    bundle: &Path,
    cache_dir: &Path,
    exe: &str,
    running_daemon: Option<u32>,
    hook: &mut InstallHook<'_>,
) -> Result<ImportSummary, ImportError> {
    if let Some(pid) = running_daemon {
        return Err(ImportError::DaemonRunning(pid));
    }
    let file = File::open(bundle).map_err(io_err(|| format!("Opening `{}`", bundle.display())))?;
    let decoder = zstd::stream::read::Decoder::new(file)
        .map_err(io_err(|| "Starting the zstd decoder".to_owned()))?;
    let mut archive = tar::Archive::new(decoder);
    let mut entries = archive
        .entries()
        .map_err(|e| ImportError::NotABundle(e.to_string()))?;

    let mut first = match entries.next() {
        Some(Ok(entry)) => entry,
        Some(Err(e)) => return Err(ImportError::NotABundle(e.to_string())),
        None => return Err(ImportError::NotABundle("it is empty".to_owned())),
    };
    if member_path(&first).ok().as_deref() != Some(HEADER) {
        return Err(ImportError::NotABundle(format!(
            "its first member is not `{HEADER}`"
        )));
    }
    let (bytes, header_member) = read_small(&mut first, HEADER, MAX_HEADER_BYTES)?;
    drop(first);
    let header: BundleHeader = serde_json::from_slice(&bytes)
        .map_err(|e| ImportError::NotABundle(format!("`{HEADER}`: {e}")))?;
    check_header(&header, exe)?;

    std::fs::create_dir_all(cache_dir)
        .map_err(io_err(|| format!("Creating `{}`", cache_dir.display())))?;
    let staging = cache_dir.join(format!("{IMAGE_DIR}.import.{}", std::process::id()));
    drop(std::fs::remove_dir_all(&staging));
    std::fs::create_dir(&staging)
        .map_err(io_err(|| format!("Creating `{}`", staging.display())))?;

    let result = unpack(entries, &staging, header_member)
        .and_then(|counts| {
            // The tar ends before the zstd frame does: read the rest, so a stream cut inside
            // its last block, or failing its checksum, is not taken for a whole one.
            let mut rest = archive.into_inner();
            io::copy(&mut rest, &mut io::sink())
                .map_err(|e| ImportError::Damaged(e.to_string()))?;
            Ok(counts)
        })
        .and_then(|(members, bytes)| {
            install(&staging, cache_dir, hook)?;
            Ok(ImportSummary {
                members,
                bytes,
                created_at_ms: header.created_at_ms,
            })
        });
    drop(std::fs::remove_dir_all(&staging));
    result
}

/// Unpacks every member after `BUNDLE` into `staging` and checks them against `BUNDLE.files`.
fn unpack<R: Read>(
    entries: tar::Entries<'_, R>,
    staging: &Path,
    header_member: BundleMember,
) -> Result<(usize, u64), ImportError> {
    let mut seen = vec![header_member];
    let mut trailer: Option<BundleFiles> = None;
    for entry in entries {
        let mut entry = entry.map_err(io_err(|| "Reading the bundle".to_owned()))?;
        let path = member_path(&entry)?;
        if trailer.is_some() {
            return Err(ImportError::AfterTrailer(path));
        }
        if path == HEADER {
            return Err(ImportError::Duplicate(path));
        }
        if path == TRAILER {
            let (bytes, _) = read_small(&mut entry, TRAILER, MAX_TRAILER_BYTES)?;
            trailer = Some(
                serde_json::from_slice(&bytes).map_err(|_| ImportError::Mismatch(path.clone()))?,
            );
            continue;
        }
        let dest = staging.join(&path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .map_err(io_err(|| format!("Creating `{}`", parent.display())))?;
        }
        let out = match File::options().write(true).create_new(true).open(&dest) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                return Err(ImportError::Duplicate(path));
            }
            Err(e) => return Err(io_err(|| format!("Creating `{}`", dest.display()))(e)),
        };
        let mut out = Hashing::new(out);
        io::copy(&mut entry, &mut out).map_err(io_err(|| format!("Unpacking `{path}`")))?;
        out.inner
            .sync_all()
            .map_err(io_err(|| format!("Syncing `{}`", dest.display())))?;
        seen.push(out.member(&path));
    }
    let Some(trailer) = trailer else {
        return Err(ImportError::NoTrailer);
    };

    let mut listed = trailer.files;
    listed.sort();
    seen.sort();
    for pair in listed.windows(2) {
        if pair[0].path == pair[1].path {
            return Err(ImportError::Duplicate(pair[0].path.clone()));
        }
    }
    let mut l = listed.iter().peekable();
    let mut s = seen.iter().peekable();
    loop {
        match (l.peek(), s.peek()) {
            (None, None) => break,
            (Some(a), None) => return Err(ImportError::Missing(a.path.clone())),
            (None, Some(b)) => return Err(ImportError::Mismatch(b.path.clone())),
            (Some(a), Some(b)) => match a.path.cmp(&b.path) {
                std::cmp::Ordering::Less => return Err(ImportError::Missing(a.path.clone())),
                std::cmp::Ordering::Greater => return Err(ImportError::Mismatch(b.path.clone())),
                std::cmp::Ordering::Equal if a != b => {
                    return Err(ImportError::Mismatch(a.path.clone()));
                }
                std::cmp::Ordering::Equal => {
                    l.next();
                    s.next();
                }
            },
        }
    }
    for required in [
        format!("{IMAGE_DIR}/{IMAGE}"),
        format!("{IMAGE_DIR}/{MANIFEST}"),
    ] {
        if !seen.iter().any(|m| m.path == required) {
            return Err(ImportError::Missing(required));
        }
    }
    let bytes = seen.iter().map(|m| m.len).sum();
    Ok((seen.len() + 1, bytes))
}

fn sync_dir(dir: &Path) -> Result<(), ImportError> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(io_err(|| format!("Syncing `{}`", dir.display())))
}

fn remove_if_present(path: &Path) -> Result<(), ImportError> {
    let r = if path.is_dir() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    };
    match r {
        Err(e) if e.kind() != io::ErrorKind::NotFound => {
            Err(io_err(|| format!("Removing `{}`", path.display()))(e))
        }
        _ => Ok(()),
    }
}

/// Moves an unpacked bundle into place. The old MANIFEST goes first and the new one comes last,
/// so no crash in between leaves a MANIFEST beside files it does not describe.
fn install(
    staging: &Path,
    cache_dir: &Path,
    hook: &mut InstallHook<'_>,
) -> Result<(), ImportError> {
    let image_dir = cache_dir.join(IMAGE_DIR);
    let dice_state = cache_dir.join(DICE_STATE_DIR);
    let step = |hook: &mut InstallHook<'_>, s: InstallStep| {
        hook(s).map_err(io_err(|| format!("Installing the bundle ({s:?})")))
    };

    remove_if_present(&image_dir.join(MANIFEST))?;
    if image_dir.is_dir() {
        sync_dir(&image_dir)?;
    }
    remove_if_present(&image_dir)?;
    remove_if_present(&dice_state)?;
    step(hook, InstallStep::OldRemoved)?;

    let staged_dice_state = staging.join(DICE_STATE_DIR);
    if staged_dice_state.is_dir() {
        std::fs::rename(&staged_dice_state, &dice_state)
    } else {
        // An empty paging store has no files to carry, but resuming needs the directory.
        std::fs::create_dir(&dice_state)
    }
    .map_err(io_err(|| format!("Installing `{}`", dice_state.display())))?;
    step(hook, InstallStep::DiceStateInstalled)?;

    let staged_manifest = staging.join(MANIFEST);
    std::fs::rename(staging.join(IMAGE_DIR).join(MANIFEST), &staged_manifest)
        .map_err(io_err(|| "Setting the MANIFEST aside".to_owned()))?;
    std::fs::rename(staging.join(IMAGE_DIR), &image_dir)
        .map_err(io_err(|| format!("Installing `{}`", image_dir.display())))?;
    sync_dir(cache_dir)?;
    step(hook, InstallStep::ImageInstalled)?;

    std::fs::rename(&staged_manifest, image_dir.join(MANIFEST))
        .map_err(io_err(|| "Installing the MANIFEST".to_owned()))?;
    sync_dir(&image_dir)?;
    Ok(())
}
