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
//! The file is a prologue and then a sequence of records. Each record is a fixed head and one
//! zstd frame that decodes on its own, so an import decodes, hashes and writes records on every
//! core while it is still reading the stream, whether from a file or from a pipe a download
//! writes into. One frame for the whole bundle, as format 1 had, can only be decoded on one core
//! and only from its start.
//!
//! - the prologue: `BUCK2SUSBUNDLE` and the bundle format, a little-endian u16;
//! - `HEAD`, the [`BundleHeader`]: the binary, the image format, the chunk size and every
//!   member's path and length. It comes first, so an import refuses a bundle from another binary
//!   before it reads anything else, and creates every member at its length before any data;
//! - one `DATA` record per chunk of a member: `chunk_size` bytes at `chunk * chunk_size`, the last
//!   chunk of a member shorter. An export writes them in member and chunk order. The MANIFEST's
//!   come first, and an import reads and checks it before anything after it; the rest it takes in
//!   any order;
//! - `END`, the [`BundleDigests`]: the blake3 of `HEAD`, and each member's digest over the blake3
//!   of its chunks. Nothing may follow it.
//!
//! The members are `suspend/MANIFEST`, `suspend/image.bin`, then every file of `dice_state/`. An
//! import installs the MANIFEST last, and only after every chunk has arrived and every digest
//! matches, so its place in the bundle says nothing about what it vouches for.
//!
//! A bundle is not signed. The digests catch damage in transport; what keeps a deliberately
//! poisoned bundle out is who may write to the store the bundles are kept in.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;

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

const BUNDLE_MAGIC: &[u8; 14] = b"BUCK2SUSBUNDLE";
/// Bumped on any change to the bundle's layout, which is independent of the image's format.
/// 1: one zstd-compressed tar. 2: records of independent frames.
const BUNDLE_FORMAT: u16 = 2;
const PROLOGUE_LEN: usize = 16;
/// How a format 1 bundle starts: a zstd frame.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
const ZSTD_LEVEL: i32 = 3;
/// Big enough that a frame compresses about as well as one stream does at level 3, whose window
/// is smaller, and small enough that a 4-core runner has hundreds of chunks of a real image to
/// share out.
const CHUNK_SIZE: u64 = 8 << 20;
const MIN_CHUNK_SIZE: u64 = 4 << 10;
const MAX_CHUNK_SIZE: u64 = 64 << 20;
/// The largest member an import creates. A real paging store file is a few GB.
const MAX_MEMBER_LEN: u64 = 1 << 40;
/// The largest `HEAD` and `END` an import reads into memory: about 150 bytes per member.
const MAX_JSON_BYTES: u32 = 64 << 20;
/// The largest MANIFEST an import reads before the rest. A real one is a few KB.
const MAX_MANIFEST_BYTES: u64 = 1 << 20;
const HEAD: &str = "HEAD";
const END: &str = "END";
/// What a member's digest is derived under, so it cannot collide with a file's own blake3.
const MEMBER_DIGEST_CONTEXT: &str = "buck2 suspend bundle 2: a member, from its chunks' blake3";

/// The first record of a bundle.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct BundleHeader {
    /// The image's `FORMAT`, from its MANIFEST.
    image_format: u32,
    /// The blake3 of the binary that wrote the image, from its MANIFEST.
    exe: String,
    created_at_ms: u64,
    chunk_size: u64,
    members: Vec<HeaderMember>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct HeaderMember {
    path: String,
    len: u64,
}

/// The last record of a bundle.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct BundleDigests {
    /// The blake3 of `HEAD`'s JSON.
    header: String,
    files: Vec<BundleMember>,
}

/// One member as `END` lists it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct BundleMember {
    path: String,
    len: u64,
    /// [`member_digest`] over the blake3 of each of its chunks.
    blake3: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Kind {
    Head = 1,
    Data = 2,
    End = 3,
}

const RECORD_HEAD_LEN: usize = 24;

/// What precedes every frame: `kind`, three zero bytes, then little-endian `member` (u32),
/// `chunk` (u64), `raw_len` (u32, the frame's decoded length) and `zlen` (u32, the frame's).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordHead {
    kind: Kind,
    member: u32,
    chunk: u64,
    raw_len: u32,
    zlen: u32,
}

impl RecordHead {
    fn encode(&self) -> [u8; RECORD_HEAD_LEN] {
        let mut b = [0u8; RECORD_HEAD_LEN];
        b[0] = self.kind as u8;
        b[4..8].copy_from_slice(&self.member.to_le_bytes());
        b[8..16].copy_from_slice(&self.chunk.to_le_bytes());
        b[16..20].copy_from_slice(&self.raw_len.to_le_bytes());
        b[20..24].copy_from_slice(&self.zlen.to_le_bytes());
        b
    }

    fn decode(b: &[u8; RECORD_HEAD_LEN]) -> Result<Self, ImportError> {
        let kind = match b[0] {
            1 => Kind::Head,
            2 => Kind::Data,
            3 => Kind::End,
            k => return Err(ImportError::Damaged(format!("a record of kind {k}"))),
        };
        if b[1..4] != [0, 0, 0] {
            return Err(ImportError::Damaged(
                "a record head's padding is not zero".to_owned(),
            ));
        }
        let head = RecordHead {
            kind,
            member: u32::from_le_bytes(b[4..8].try_into().unwrap()),
            chunk: u64::from_le_bytes(b[8..16].try_into().unwrap()),
            raw_len: u32::from_le_bytes(b[16..20].try_into().unwrap()),
            zlen: u32::from_le_bytes(b[20..24].try_into().unwrap()),
        };
        // A frame never needs more than this, so a damaged length cannot ask for gigabytes.
        if head.zlen as usize > zstd::zstd_safe::compress_bound(head.raw_len as usize) {
            return Err(ImportError::Damaged(format!(
                "a record of {} bytes claims {} compressed bytes",
                head.raw_len, head.zlen
            )));
        }
        Ok(head)
    }
}

fn prologue() -> [u8; PROLOGUE_LEN] {
    let mut b = [0u8; PROLOGUE_LEN];
    b[..14].copy_from_slice(BUNDLE_MAGIC);
    b[14..].copy_from_slice(&BUNDLE_FORMAT.to_le_bytes());
    b
}

fn chunk_count(len: u64, chunk_size: u64) -> u64 {
    len.div_ceil(chunk_size)
}

fn chunk_len(len: u64, chunk_size: u64, chunk: u64) -> u64 {
    chunk_size.min(len - chunk * chunk_size)
}

/// A member's digest: its length, the chunk size and the blake3 of each chunk in order. Any
/// change to a byte, to where a chunk sits or to the member's length changes it.
fn member_digest(len: u64, chunk_size: u64, chunks: impl IntoIterator<Item = [u8; 32]>) -> String {
    let mut hasher = blake3::Hasher::new_derive_key(MEMBER_DIGEST_CONTEXT);
    hasher.update(&len.to_le_bytes());
    hasher.update(&chunk_size.to_le_bytes());
    for c in chunks {
        hasher.update(&c);
    }
    hasher.finalize().to_hex().to_string()
}

fn worker_threads() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
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

/// Writes the image under `cache_dir` to `out` as a bundle. The image is left where it is, so
/// this machine can still resume from it. `out` appears only once it is complete.
pub(crate) fn export(cache_dir: &Path, out: &Path) -> buck2_error::Result<ExportSummary> {
    export_with(cache_dir, out, CHUNK_SIZE, worker_threads())
}

fn export_with(
    cache_dir: &Path,
    out: &Path,
    chunk_size: u64,
    threads: usize,
) -> buck2_error::Result<ExportSummary> {
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
    // A bundle is resumed on a machine with none of this one's buck-out, and by a daemon whose
    // environment differs in the per-run variables the MANIFEST leaves out: that is sound only
    // for an image that has every artifact in the CAS and has its local actions run again.
    if !manifest.portable {
        return Err(buck2_error!(
            ErrorTag::Input,
            "The suspended state is not portable: export it with `buck2 suspend --export`, \
             which writes a portable image"
        ));
    }
    // The MANIFEST first, for an import to check before the gigabytes after it.
    let mut paths = vec![manifest_rel, format!("{IMAGE_DIR}/{IMAGE}")];
    paths.extend(dice_state_files(cache_dir)?);
    let mut members = Vec::with_capacity(paths.len());
    for path in paths {
        let src = cache_dir.join(&path);
        let len = std::fs::metadata(&src)
            .with_buck_error_context(|| format!("Reading `{}`", src.display()))?
            .len();
        members.push(HeaderMember { path, len });
    }
    let header = BundleHeader {
        image_format: manifest.format,
        exe: manifest.exe,
        created_at_ms: manifest.created_at_ms,
        chunk_size,
        members,
    };

    let file_name = out
        .file_name()
        .ok_or_else(|| buck2_error!(ErrorTag::Input, "`{}` names no file", out.display()))?;
    let mut tmp_name = file_name.to_owned();
    tmp_name.push(format!(".tmp.{}", std::process::id()));
    let tmp = out.with_file_name(tmp_name);
    let result = write_bundle(cache_dir, &tmp, &header, threads);
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
    threads: usize,
) -> buck2_error::Result<ExportSummary> {
    let file =
        File::create(tmp).with_buck_error_context(|| format!("Creating `{}`", tmp.display()))?;
    let mut writer = BufWriter::with_capacity(1 << 20, file);
    encode(cache_dir, header, &mut writer, threads)?;
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
        members: header.members.len(),
        bytes: header.members.iter().map(|m| m.len).sum(),
        compressed,
    })
}

fn write_record<W: Write>(
    out: &mut W,
    kind: Kind,
    member: u32,
    chunk: u64,
    raw_len: usize,
    frame: &[u8],
) -> buck2_error::Result<()> {
    let head = RecordHead {
        kind,
        member,
        chunk,
        raw_len: raw_len as u32,
        zlen: frame.len() as u32,
    };
    out.write_all(&head.encode())
        .and_then(|()| out.write_all(frame))
        .buck_error_context("Writing the bundle")
}

fn write_json<W: Write, T: Serialize>(
    out: &mut W,
    kind: Kind,
    value: &T,
) -> buck2_error::Result<Vec<u8>> {
    let json = serde_json::to_vec(value).buck_error_context("Encoding a bundle record")?;
    let frame = zstd::bulk::compress(&json, ZSTD_LEVEL).buck_error_context("Compressing")?;
    write_record(out, kind, 0, 0, json.len(), &frame)?;
    Ok(json)
}

/// The next item from a receiver the workers share, or `None` once every sender is gone.
fn next_shared<T>(rx: &Mutex<mpsc::Receiver<T>>) -> Option<T> {
    rx.lock().ok()?.recv().ok()
}

/// A chunk read from a member, on its way to be compressed.
struct RawChunk {
    seq: u64,
    member: u32,
    chunk: u64,
    data: Vec<u8>,
}

/// A chunk compressed, on its way to be written.
struct PackedChunk {
    seq: u64,
    member: u32,
    chunk: u64,
    raw_len: usize,
    frame: Vec<u8>,
    digest: [u8; 32],
}

fn pack(
    compressor: &mut zstd::bulk::Compressor<'static>,
    raw: RawChunk,
) -> buck2_error::Result<PackedChunk> {
    let digest = *blake3::hash(&raw.data).as_bytes();
    let frame = compressor
        .compress(&raw.data)
        .buck_error_context("Compressing a chunk")?;
    Ok(PackedChunk {
        seq: raw.seq,
        member: raw.member,
        chunk: raw.chunk,
        raw_len: raw.data.len(),
        frame,
        digest,
    })
}

fn compressor() -> buck2_error::Result<zstd::bulk::Compressor<'static>> {
    let mut c =
        zstd::bulk::Compressor::new(ZSTD_LEVEL).buck_error_context("Starting the zstd encoder")?;
    c.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))
        .buck_error_context("Configuring the zstd encoder")?;
    Ok(c)
}

/// Reads `len` bytes of `data` as chunks of `chunk_size`, handing each to `each`, and fails if
/// `data` yields fewer: a member cut short while it is bundled.
fn read_chunks(
    path: &str,
    len: u64,
    chunk_size: u64,
    mut data: impl Read,
    mut each: impl FnMut(u64, Vec<u8>) -> buck2_error::Result<()>,
) -> buck2_error::Result<()> {
    let mut done = 0;
    let mut chunk = 0;
    while done < len {
        let want = chunk_len(len, chunk_size, chunk);
        let mut buf = Vec::with_capacity(want as usize);
        let got = (&mut data)
            .take(want)
            .read_to_end(&mut buf)
            .with_buck_error_context(|| format!("Reading `{path}`"))? as u64;
        if got < want {
            return Err(buck2_error!(
                ErrorTag::Environment,
                "`{path}` shrank from {len} to {} bytes while it was bundled",
                done + got
            ));
        }
        each(chunk, buf)?;
        done += want;
        chunk += 1;
    }
    Ok(())
}

/// Writes the prologue and every record of a bundle of `header`'s members, read from under
/// `cache_dir`. This thread reads, `threads` workers hash and compress, and one more writes in
/// member and chunk order, so the bundle's bytes depend only on the image.
fn encode<W: Write + Send>(
    cache_dir: &Path,
    header: &BundleHeader,
    out: &mut W,
    threads: usize,
) -> buck2_error::Result<()> {
    let threads = threads.max(1);
    out.write_all(&prologue())
        .buck_error_context("Writing the bundle")?;
    let head_json = write_json(out, Kind::Head, header)?;
    let counts: Vec<u64> = header
        .members
        .iter()
        .map(|m| chunk_count(m.len, header.chunk_size))
        .collect();

    let (read, written) = std::thread::scope(|s| {
        let (raw_tx, raw_rx) = mpsc::sync_channel::<RawChunk>(threads * 2);
        let raw_rx = Arc::new(Mutex::new(raw_rx));
        let (packed_tx, packed_rx) =
            mpsc::sync_channel::<buck2_error::Result<PackedChunk>>(threads * 2);
        // One ticket per chunk between its read and its write, so a slow chunk cannot leave an
        // unbounded backlog of later ones waiting in memory.
        let (ticket_tx, ticket_rx) = mpsc::sync_channel::<()>(threads * 4);
        for _ in 0..threads {
            let raw_rx = raw_rx.clone();
            let packed_tx = packed_tx.clone();
            s.spawn(move || {
                let mut c = compressor();
                while let Some(raw) = next_shared(&raw_rx) {
                    let packed = match &mut c {
                        Ok(c) => pack(c, raw),
                        Err(e) => Err(e.clone()),
                    };
                    if packed_tx.send(packed).is_err() {
                        break;
                    }
                }
            });
        }
        drop(raw_rx);
        drop(packed_tx);

        let counts = &counts;
        let out = &mut *out;
        let writer = s.spawn(move || -> buck2_error::Result<(u64, Vec<Vec<[u8; 32]>>)> {
            let mut digests: Vec<Vec<[u8; 32]>> =
                counts.iter().map(|&n| vec![[0; 32]; n as usize]).collect();
            let mut pending = BTreeMap::new();
            let mut next = 0;
            for packed in packed_rx {
                let packed = packed?;
                pending.insert(packed.seq, packed);
                while let Some(p) = pending.remove(&next) {
                    write_record(out, Kind::Data, p.member, p.chunk, p.raw_len, &p.frame)?;
                    digests[p.member as usize][p.chunk as usize] = p.digest;
                    next += 1;
                    ticket_rx.recv().ok();
                }
            }
            if !pending.is_empty() {
                return Err(buck2_error!(
                    ErrorTag::Tier0,
                    "A chunk of the bundle was compressed but never written"
                ));
            }
            Ok((next, digests))
        });

        let mut seq = 0;
        let read = header
            .members
            .iter()
            .enumerate()
            .try_for_each(|(i, m)| {
                let src = cache_dir.join(&m.path);
                let file = File::open(&src)
                    .with_buck_error_context(|| format!("Opening `{}`", src.display()))?;
                read_chunks(&m.path, m.len, header.chunk_size, file, |chunk, data| {
                    let stopped =
                        || buck2_error!(ErrorTag::Tier0, "The bundle's writer stopped early");
                    ticket_tx.send(()).map_err(|_| stopped())?;
                    raw_tx
                        .send(RawChunk {
                            seq,
                            member: i as u32,
                            chunk,
                            data,
                        })
                        .map_err(|_| stopped())?;
                    seq += 1;
                    Ok(())
                })
            })
            .map(|()| seq);
        drop(raw_tx);
        drop(ticket_tx);
        let written = writer.join().expect("the bundle writer panicked");
        (read, written)
    });
    // The writer's error is the cause when both fail: the reader stops because the writer did.
    let (written, digests) = written?;
    let read = read?;
    if read != written {
        return Err(buck2_error!(
            ErrorTag::Tier0,
            "Read {read} chunks into the bundle and wrote {written}"
        ));
    }

    let files = header
        .members
        .iter()
        .zip(digests)
        .map(|(m, d)| BundleMember {
            path: m.path.clone(),
            len: m.len,
            blake3: member_digest(m.len, header.chunk_size, d),
        })
        .collect();
    write_json(
        out,
        Kind::End,
        &BundleDigests {
            header: blake3::hash(&head_json).to_hex().to_string(),
            files,
        },
    )?;
    out.flush().buck_error_context("Writing the bundle")
}

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
    /// A member whose path no bundle has: absolute, `..`, or a name outside the image; or a
    /// record for a member `HEAD` does not list.
    BadMember(String),
    Duplicate(String),
    AfterTrailer(String),
    NoTrailer,
    /// A member whose bytes differ from what `END` says, or that `END` does not list.
    Mismatch(String),
    /// A member `END` lists that `HEAD` does not, or one some of whose chunks never came.
    Missing(String),
    /// A frame that does not decode, or a stream cut inside a record.
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
            ImportError::Duplicate(p) => write!(f, "`{p}` appears twice"),
            ImportError::AfterTrailer(p) => write!(f, "{p} follows `{END}`"),
            ImportError::NoTrailer => write!(f, "`{END}` is missing: the bundle is truncated"),
            ImportError::Mismatch(p) => {
                write!(
                    f,
                    "member `{p}` does not match `{END}`: the bundle is damaged"
                )
            }
            ImportError::Missing(p) => write!(f, "member `{p}` is missing"),
            ImportError::Damaged(e) => {
                write!(f, "the bundle is damaged or truncated: {e}")
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

/// Where an import reads a bundle from.
#[derive(Debug, Clone)]
pub(crate) enum BundleSource {
    File(PathBuf),
    /// Standard input, so that a download can pipe into the import and the two overlap.
    Stdin,
}

impl std::fmt::Display for BundleSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BundleSource::File(p) => write!(f, "{}", p.display()),
            BundleSource::Stdin => write!(f, "standard input"),
        }
    }
}

/// What an import installed.
#[derive(Debug)]
pub(crate) struct ImportSummary {
    pub(crate) members: usize,
    pub(crate) bytes: u64,
    pub(crate) created_at_ms: u64,
    pub(crate) threads: usize,
}

/// Checks a member's path: relative, plain names only, `suspend/image.bin`,
/// `suspend/MANIFEST` or under `dice_state/`.
fn check_member_path(text: &str) -> Result<(), ImportError> {
    let parts: Vec<&str> = text.split('/').collect();
    let plain = parts.iter().all(|p| {
        !p.is_empty() && *p != "." && *p != ".." && !p.contains('\0') && !p.contains('\\')
    });
    let placed = match parts.as_slice() {
        [d, f] if *d == IMAGE_DIR => *f == IMAGE || *f == MANIFEST,
        [d, _, ..] => *d == DICE_STATE_DIR,
        _ => false,
    };
    if !plain || !placed {
        return Err(ImportError::BadMember(text.to_owned()));
    }
    Ok(())
}

/// Checks the bundle's header against this binary, and its member list: the refusals that need
/// no unpacking.
fn check_header(header: &BundleHeader, exe: &str) -> Result<(), ImportError> {
    if header.image_format != FORMAT {
        return Err(ImportError::ImageFormat(header.image_format));
    }
    if header.exe != exe {
        return Err(ImportError::OtherBinary {
            bundle: header.exe.clone(),
            this: exe.to_owned(),
        });
    }
    if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&header.chunk_size) {
        return Err(ImportError::NotABundle(format!(
            "chunk size {}",
            header.chunk_size
        )));
    }
    let mut paths = BTreeSet::new();
    for m in &header.members {
        check_member_path(&m.path)?;
        if !paths.insert(m.path.as_str()) {
            return Err(ImportError::Duplicate(m.path.clone()));
        }
        if m.len > MAX_MEMBER_LEN {
            return Err(ImportError::NotABundle(format!(
                "member `{}` is {} bytes",
                m.path, m.len
            )));
        }
    }
    for required in [
        format!("{IMAGE_DIR}/{IMAGE}"),
        format!("{IMAGE_DIR}/{MANIFEST}"),
    ] {
        if !paths.contains(required.as_str()) {
            return Err(ImportError::Missing(required));
        }
    }
    Ok(())
}

/// Fills `buf` from `r` until it is full or the stream ends, and returns how much it read.
fn fill<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

fn read_prologue<R: Read>(r: &mut R) -> Result<(), ImportError> {
    let mut b = [0u8; PROLOGUE_LEN];
    let n = fill(r, &mut b).map_err(io_err(|| "Reading the bundle".to_owned()))?;
    if n >= 4 && b[..4] == ZSTD_MAGIC {
        return Err(ImportError::BundleFormat(1));
    }
    if n < PROLOGUE_LEN || &b[..14] != BUNDLE_MAGIC {
        return Err(ImportError::NotABundle(
            "it does not start with a bundle prologue".to_owned(),
        ));
    }
    let format = u16::from_le_bytes([b[14], b[15]]);
    if format != BUNDLE_FORMAT {
        return Err(ImportError::BundleFormat(format.into()));
    }
    Ok(())
}

/// The next record's head, or `None` where the stream ends between two records.
fn next_head<R: Read>(r: &mut R) -> Result<Option<RecordHead>, ImportError> {
    let mut b = [0u8; RECORD_HEAD_LEN];
    match fill(r, &mut b).map_err(io_err(|| "Reading the bundle".to_owned()))? {
        0 => Ok(None),
        RECORD_HEAD_LEN => RecordHead::decode(&b).map(Some),
        _ => Err(ImportError::Damaged(
            "the stream ends inside a record head".to_owned(),
        )),
    }
}

fn read_frame<R: Read>(r: &mut R, head: &RecordHead) -> Result<Vec<u8>, ImportError> {
    let mut frame = vec![0; head.zlen as usize];
    let n = fill(r, &mut frame).map_err(io_err(|| "Reading the bundle".to_owned()))?;
    if n < frame.len() {
        return Err(ImportError::Damaged(
            "the stream ends inside a record".to_owned(),
        ));
    }
    Ok(frame)
}

fn decode_frame(
    d: &mut zstd::bulk::Decompressor<'static>,
    frame: &[u8],
    raw_len: u32,
) -> Result<Vec<u8>, ImportError> {
    let data = d
        .decompress(frame, raw_len as usize)
        .map_err(|e| ImportError::Damaged(e.to_string()))?;
    if data.len() != raw_len as usize {
        return Err(ImportError::Damaged(format!(
            "a frame decodes to {} bytes, and its record says {raw_len}",
            data.len()
        )));
    }
    Ok(data)
}

fn read_json<R: Read, T: for<'de> Deserialize<'de>>(
    r: &mut R,
    head: &RecordHead,
    name: &str,
) -> Result<(Vec<u8>, T), ImportError> {
    if head.raw_len > MAX_JSON_BYTES {
        return Err(ImportError::NotABundle(format!(
            "`{name}` is {} bytes",
            head.raw_len
        )));
    }
    let frame = read_frame(r, head)?;
    let mut d = zstd::bulk::Decompressor::new()
        .map_err(io_err(|| "Starting the zstd decoder".to_owned()))?;
    let json = decode_frame(&mut d, &frame, head.raw_len)?;
    let value = serde_json::from_slice(&json)
        .map_err(|e| ImportError::NotABundle(format!("`{name}`: {e}")))?;
    Ok((json, value))
}

/// Unpacks a bundle into `cache_dir`, replacing any image there, and returns what it installed.
///
/// `exe` is the blake3 of the binary the daemon will run, and `running_daemon` the pid of a
/// daemon that is up for this checkout, if any: both are refused before anything is read.
/// Everything is unpacked into a directory of its own and checked against `END` first; only
/// then does the old image go and the new one move in, the MANIFEST last, so an import that
/// fails or is killed at any point leaves either the old image or no MANIFEST, and the next start
/// is cold.
///
/// Nothing is synced to disk. A runner that loses power between the install and the daemon's
/// start, which consumes the MANIFEST, is not one a job resumes on.
pub(crate) fn import(
    source: &BundleSource,
    cache_dir: &Path,
    exe: &str,
    running_daemon: Option<u32>,
) -> Result<ImportSummary, ImportError> {
    if let Some(pid) = running_daemon {
        return Err(ImportError::DaemonRunning(pid));
    }
    let threads = worker_threads();
    match source {
        BundleSource::File(path) => {
            let file =
                File::open(path).map_err(io_err(|| format!("Opening `{}`", path.display())))?;
            import_from(
                BufReader::with_capacity(1 << 20, file),
                cache_dir,
                exe,
                threads,
                &mut |_| Ok(()),
            )
        }
        BundleSource::Stdin => import_from(
            BufReader::with_capacity(1 << 20, io::stdin().lock()),
            cache_dir,
            exe,
            threads,
            &mut |_| Ok(()),
        ),
    }
}

/// The steps of installing an unpacked bundle, for tests to fail at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstallStep {
    OldRemoved,
    DiceStateInstalled,
    ImageInstalled,
}

type InstallHook<'a> = dyn FnMut(InstallStep) -> io::Result<()> + 'a;

fn import_from<R: Read>(
    mut input: R,
    cache_dir: &Path,
    exe: &str,
    threads: usize,
    hook: &mut InstallHook<'_>,
) -> Result<ImportSummary, ImportError> {
    read_prologue(&mut input)?;
    let head = match next_head(&mut input)? {
        Some(head) if head.kind == Kind::Head && head.member == 0 && head.chunk == 0 => head,
        Some(_) => {
            return Err(ImportError::NotABundle(format!(
                "its first record is not `{HEAD}`"
            )));
        }
        None => return Err(ImportError::NotABundle("it holds no records".to_owned())),
    };
    let (head_json, header): (_, BundleHeader) = read_json(&mut input, &head, HEAD)?;
    check_header(&header, exe)?;
    let head_digest = blake3::hash(&head_json).to_hex().to_string();
    let manifest = read_manifest(&mut input, &header)?;

    std::fs::create_dir_all(cache_dir)
        .map_err(io_err(|| format!("Creating `{}`", cache_dir.display())))?;
    let staging = cache_dir.join(format!("{IMAGE_DIR}.import.{}", std::process::id()));
    drop(std::fs::remove_dir_all(&staging));
    std::fs::create_dir(&staging)
        .map_err(io_err(|| format!("Creating `{}`", staging.display())))?;

    let result = unpack(
        input,
        &staging,
        &header,
        &head_digest,
        &manifest,
        threads.max(1),
    )
    .and_then(|bytes| {
        install(&staging, cache_dir, hook)?;
        Ok(ImportSummary {
            members: header.members.len(),
            bytes,
            created_at_ms: header.created_at_ms,
            threads: threads.max(1),
        })
    });
    drop(std::fs::remove_dir_all(&staging));
    result
}

/// The MANIFEST, read whole before anything after it.
struct EarlyMember {
    member: u32,
    data: Vec<u8>,
    chunks: Vec<[u8; 32]>,
}

/// Reads the MANIFEST's records, which follow `HEAD`, and checks the MANIFEST against `HEAD`:
/// what an import can refuse having read a few KB rather than gigabytes.
fn read_manifest<R: Read>(
    input: &mut R,
    header: &BundleHeader,
) -> Result<EarlyMember, ImportError> {
    let manifest_rel = format!("{IMAGE_DIR}/{MANIFEST}");
    let not_first = || {
        ImportError::NotABundle(format!(
            "the records after `{HEAD}` are not `{manifest_rel}`'s"
        ))
    };
    let Some(i) = header.members.iter().position(|m| m.path == manifest_rel) else {
        return Err(ImportError::Missing(manifest_rel));
    };
    let len = header.members[i].len;
    if len == 0 || len > MAX_MANIFEST_BYTES {
        return Err(ImportError::NotABundle(format!(
            "`{manifest_rel}` is {len} bytes"
        )));
    }
    let mut d = zstd::bulk::Decompressor::new()
        .map_err(io_err(|| "Starting the zstd decoder".to_owned()))?;
    let mut data = Vec::with_capacity(len as usize);
    let mut chunks = Vec::new();
    for chunk in 0..chunk_count(len, header.chunk_size) {
        let Some(head) = next_head(input)? else {
            return Err(ImportError::NoTrailer);
        };
        if head.kind != Kind::Data
            || head.member as usize != i
            || head.chunk != chunk
            || u64::from(head.raw_len) != chunk_len(len, header.chunk_size, chunk)
        {
            return Err(not_first());
        }
        let bytes = decode_frame(&mut d, &read_frame(input, &head)?, head.raw_len)?;
        chunks.push(*blake3::hash(&bytes).as_bytes());
        data.extend_from_slice(&bytes);
    }
    let manifest: ManifestHead = serde_json::from_slice(&data)
        .map_err(|e| ImportError::NotABundle(format!("`{manifest_rel}`: {e}")))?;
    if manifest.magic != MAGIC
        || manifest.format != header.image_format
        || manifest.exe != header.exe
    {
        return Err(ImportError::NotABundle(format!(
            "`{manifest_rel}` is image format {} {} by {}, and `{HEAD}` says {MAGIC} {} by {}",
            manifest.magic, manifest.format, manifest.exe, header.image_format, header.exe
        )));
    }
    Ok(EarlyMember {
        member: i as u32,
        data,
        chunks,
    })
}

/// A record on its way from the reader to a worker.
struct Job {
    member: u32,
    chunk: u64,
    raw_len: u32,
    frame: Vec<u8>,
}

fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::write_all_at(file, buf, offset)
    }
    #[cfg(windows)]
    {
        let mut done = 0;
        while done < buf.len() {
            let n = std::os::windows::fs::FileExt::seek_write(
                file,
                &buf[done..],
                offset + done as u64,
            )?;
            if n == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            done += n;
        }
        Ok(())
    }
}

/// Unpacks every record after `HEAD` into `staging` and checks the members against `END`, and
/// returns the members' bytes. This thread reads the records in turn; `threads` workers decode,
/// hash and write each `DATA` record where it belongs.
fn unpack<R: Read>(
    mut input: R,
    staging: &Path,
    header: &BundleHeader,
    head_digest: &str,
    early: &EarlyMember,
    threads: usize,
) -> Result<u64, ImportError> {
    let chunk_size = header.chunk_size;
    let mut files = Vec::with_capacity(header.members.len());
    for m in &header.members {
        let dest = staging.join(&m.path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .map_err(io_err(|| format!("Creating `{}`", parent.display())))?;
        }
        let file = File::options()
            .write(true)
            .create_new(true)
            .open(&dest)
            .map_err(io_err(|| format!("Creating `{}`", dest.display())))?;
        file.set_len(m.len)
            .map_err(io_err(|| format!("Sizing `{}`", dest.display())))?;
        files.push(file);
    }
    let counts: Vec<u64> = header
        .members
        .iter()
        .map(|m| chunk_count(m.len, chunk_size))
        .collect();

    write_at(&files[early.member as usize], &early.data, 0)
        .map_err(io_err(|| format!("Writing `{IMAGE_DIR}/{MANIFEST}`")))?;
    let early_chunks: Vec<((u32, u64), [u8; 32])> = (0u64..)
        .zip(&early.chunks)
        .map(|(c, d)| ((early.member, c), *d))
        .collect();

    let failed = AtomicBool::new(false);
    let first_err: Mutex<Option<ImportError>> = Mutex::new(None);
    let digests: Mutex<HashMap<(u32, u64), [u8; 32]>> =
        Mutex::new(early_chunks.iter().copied().collect());
    let fail = |e: ImportError| {
        if let Ok(mut slot) = first_err.lock() {
            slot.get_or_insert(e);
        }
        failed.store(true, Ordering::Relaxed);
    };

    let read = std::thread::scope(|s| {
        let (tx, rx) = mpsc::sync_channel::<Job>(threads * 2);
        let rx = Arc::new(Mutex::new(rx));
        for _ in 0..threads {
            let rx = rx.clone();
            let (files, digests, fail, failed) = (&files, &digests, &fail, &failed);
            s.spawn(move || {
                let mut d = zstd::bulk::Decompressor::new()
                    .map_err(io_err(|| "Starting the zstd decoder".to_owned()));
                while let Some(job) = next_shared(&rx) {
                    // After a failure, drain without work, so the reader is never left blocked.
                    if failed.load(Ordering::Relaxed) {
                        continue;
                    }
                    let placed = match &mut d {
                        Ok(d) => place(d, files, chunk_size, &job),
                        Err(e) => Err(ImportError::Damaged(e.to_string())),
                    };
                    match placed {
                        Ok(digest) => {
                            if let Ok(mut map) = digests.lock() {
                                map.insert((job.member, job.chunk), digest);
                            }
                        }
                        Err(e) => fail(e),
                    }
                }
            });
        }
        drop(rx);
        let seen = early_chunks.iter().map(|(k, _)| *k).collect();
        let read = read_records(&mut input, header, &counts, seen, &failed, |job| {
            tx.send(job).is_ok()
        });
        drop(tx);
        read
    });
    let worker_err = first_err.into_inner().ok().flatten();
    let trailer = match (read, worker_err) {
        (Err(e), _) => return Err(e),
        (Ok(_), Some(e)) => return Err(e),
        (Ok(Some(trailer)), None) => trailer,
        (Ok(None), None) => {
            return Err(ImportError::Damaged(
                "the import stopped before the end".to_owned(),
            ));
        }
    };
    let digests = digests.into_inner().unwrap_or_default();
    verify(header, head_digest, &trailer, &counts, &digests)
}

/// Decodes one `DATA` record, writes it where it belongs, and returns its blake3.
fn place(
    d: &mut zstd::bulk::Decompressor<'static>,
    files: &[File],
    chunk_size: u64,
    job: &Job,
) -> Result<[u8; 32], ImportError> {
    let data = decode_frame(d, &job.frame, job.raw_len)?;
    let digest = *blake3::hash(&data).as_bytes();
    write_at(&files[job.member as usize], &data, job.chunk * chunk_size)
        .map_err(io_err(|| "Unpacking a chunk".to_owned()))?;
    Ok(digest)
}

/// Reads every record after `HEAD`, hands each `DATA` record to `dispatch`, and returns `END`;
/// `None` if it stopped because a worker failed.
fn read_records<R: Read>(
    input: &mut R,
    header: &BundleHeader,
    counts: &[u64],
    mut seen: HashSet<(u32, u64)>,
    failed: &AtomicBool,
    mut dispatch: impl FnMut(Job) -> bool,
) -> Result<Option<BundleDigests>, ImportError> {
    let mut trailer = None;
    loop {
        if failed.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let Some(head) = next_head(input)? else {
            return trailer.map(Some).ok_or(ImportError::NoTrailer);
        };
        if trailer.is_some() {
            return Err(ImportError::AfterTrailer(format!(
                "a {:?} record",
                head.kind
            )));
        }
        match head.kind {
            Kind::Head => return Err(ImportError::Duplicate(HEAD.to_owned())),
            Kind::End => {
                let (_, digests): (_, BundleDigests) = read_json(input, &head, END)?;
                trailer = Some(digests);
            }
            Kind::Data => {
                let i = head.member as usize;
                let Some(member) = header.members.get(i) else {
                    return Err(ImportError::BadMember(format!("#{i}")));
                };
                if head.chunk >= counts[i]
                    || u64::from(head.raw_len)
                        != chunk_len(member.len, header.chunk_size, head.chunk)
                {
                    return Err(ImportError::Mismatch(member.path.clone()));
                }
                if !seen.insert((head.member, head.chunk)) {
                    return Err(ImportError::Duplicate(format!(
                        "chunk {} of `{}`",
                        head.chunk, member.path
                    )));
                }
                let frame = read_frame(input, &head)?;
                let job = Job {
                    member: head.member,
                    chunk: head.chunk,
                    raw_len: head.raw_len,
                    frame,
                };
                if !dispatch(job) {
                    return Ok(None);
                }
            }
        }
    }
}

/// Checks what arrived against `HEAD` and `END`, and returns the members' bytes.
fn verify(
    header: &BundleHeader,
    head_digest: &str,
    trailer: &BundleDigests,
    counts: &[u64],
    digests: &HashMap<(u32, u64), [u8; 32]>,
) -> Result<u64, ImportError> {
    if trailer.header != head_digest {
        return Err(ImportError::Mismatch(HEAD.to_owned()));
    }
    let mut listed = HashMap::new();
    for f in &trailer.files {
        if listed.insert(f.path.as_str(), f).is_some() {
            return Err(ImportError::Duplicate(f.path.clone()));
        }
    }
    let held: HashSet<&str> = header.members.iter().map(|m| m.path.as_str()).collect();
    if let Some(f) = trailer
        .files
        .iter()
        .find(|f| !held.contains(f.path.as_str()))
    {
        return Err(ImportError::Missing(f.path.clone()));
    }
    for (i, m) in header.members.iter().enumerate() {
        let Some(listed) = listed.get(m.path.as_str()) else {
            return Err(ImportError::Mismatch(m.path.clone()));
        };
        let mut chunks = Vec::with_capacity(counts[i] as usize);
        for c in 0..counts[i] {
            match digests.get(&(i as u32, c)) {
                Some(d) => chunks.push(*d),
                None => return Err(ImportError::Missing(m.path.clone())),
            }
        }
        if listed.len != m.len || listed.blake3 != member_digest(m.len, header.chunk_size, chunks) {
            return Err(ImportError::Mismatch(m.path.clone()));
        }
    }
    Ok(header.members.iter().map(|m| m.len).sum())
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
/// so no failure in between leaves a MANIFEST beside files it does not describe.
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
    step(hook, InstallStep::ImageInstalled)?;

    std::fs::rename(&staged_manifest, image_dir.join(MANIFEST))
        .map_err(io_err(|| "Installing the MANIFEST".to_owned()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use super::*;

    const EXE: &str = "e1e1";
    /// Small enough that the fixture's members span several chunks, the last one short.
    const TEST_CHUNK: u64 = 64 << 10;

    /// A suspended image as the daemon leaves it, with `seed` in every file's bytes.
    fn write_image(cache_dir: &Path, seed: u8) {
        let image_dir = cache_dir.join(IMAGE_DIR);
        let dice_state = cache_dir.join(DICE_STATE_DIR);
        std::fs::create_dir_all(&image_dir).unwrap();
        std::fs::create_dir_all(&dice_state).unwrap();
        let bytes = |n: usize| -> Vec<u8> {
            (0..n)
                .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed) ^ (i >> 9) as u8)
                .collect()
        };
        std::fs::write(image_dir.join(IMAGE), bytes(70_000)).unwrap();
        std::fs::write(dice_state.join("pagable.0.db"), bytes(200_000)).unwrap();
        std::fs::write(dice_state.join("pagable.1.db"), bytes(3)).unwrap();
        std::fs::write(dice_state.join("pagable.2.db"), b"").unwrap();
        // The daemon's MANIFEST has more fields than the client reads.
        let manifest = serde_json::json!({
            "magic": MAGIC,
            "format": FORMAT,
            "exe": EXE,
            "created_at_ms": 1000 + seed as u64,
            "portable": true,
            "hostname": "h",
            "uid": 1,
        });
        std::fs::write(
            image_dir.join(MANIFEST),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
    }

    /// Every file under the cache dir, by path.
    fn snapshot(cache_dir: &Path) -> BTreeMap<String, Vec<u8>> {
        let mut out = BTreeMap::new();
        if !cache_dir.exists() {
            return out;
        }
        for e in walkdir::WalkDir::new(cache_dir) {
            let e = e.unwrap();
            if e.file_type().is_file() {
                let rel = e.path().strip_prefix(cache_dir).unwrap();
                out.insert(
                    rel.to_str().unwrap().to_owned(),
                    std::fs::read(e.path()).unwrap(),
                );
            }
        }
        out
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        /// The suspending machine's cache dir.
        src: PathBuf,
        /// The importing machine's cache dir, which already holds another image.
        dst: PathBuf,
        bundle: PathBuf,
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("a/buck-out/v2/cache");
        let dst = dir.path().join("b/buck-out/v2/cache");
        write_image(&src, 1);
        write_image(&dst, 2);
        let bundle = dir.path().join("image.bundle");
        export_with(&src, &bundle, TEST_CHUNK, 4).unwrap();
        Fixture {
            _dir: dir,
            src,
            dst,
            bundle,
        }
    }

    fn import_file(
        bundle: &Path,
        cache_dir: &Path,
        exe: &str,
    ) -> Result<ImportSummary, ImportError> {
        import(&BundleSource::File(bundle.to_owned()), cache_dir, exe, None)
    }

    /// One record, its frame decoded.
    #[derive(Clone, Debug)]
    struct Rec {
        kind: Kind,
        member: u32,
        chunk: u64,
        raw: Vec<u8>,
    }

    /// The bundle's records, decoded, and where each one starts in the file.
    fn records_at(bundle: &Path) -> Vec<(usize, Rec)> {
        let bytes = std::fs::read(bundle).unwrap();
        assert_eq!(&bytes[..PROLOGUE_LEN], &prologue());
        let mut at = PROLOGUE_LEN;
        let mut out = Vec::new();
        while at < bytes.len() {
            let head =
                RecordHead::decode(bytes[at..at + RECORD_HEAD_LEN].try_into().unwrap()).unwrap();
            let start = at;
            at += RECORD_HEAD_LEN;
            let frame = &bytes[at..at + head.zlen as usize];
            at += head.zlen as usize;
            let raw = zstd::bulk::decompress(frame, head.raw_len as usize).unwrap();
            assert_eq!(raw.len(), head.raw_len as usize);
            out.push((
                start,
                Rec {
                    kind: head.kind,
                    member: head.member,
                    chunk: head.chunk,
                    raw,
                },
            ));
        }
        out
    }

    fn records(bundle: &Path) -> Vec<Rec> {
        records_at(bundle).into_iter().map(|(_, r)| r).collect()
    }

    fn encode_records(recs: &[Rec]) -> Vec<u8> {
        let mut out = prologue().to_vec();
        for r in recs {
            let frame = zstd::bulk::compress(&r.raw, 3).unwrap();
            write_record(&mut out, r.kind, r.member, r.chunk, r.raw.len(), &frame).unwrap();
        }
        out
    }

    fn write_records(bundle: &Path, recs: &[Rec]) {
        std::fs::write(bundle, encode_records(recs)).unwrap();
    }

    fn header_of(recs: &[Rec]) -> BundleHeader {
        serde_json::from_slice(&recs[0].raw).unwrap()
    }

    fn digests_of(recs: &[Rec]) -> BundleDigests {
        serde_json::from_slice(&recs.last().unwrap().raw).unwrap()
    }

    /// Rewrites `HEAD`, and `END`'s digest of it, so only the edit differs.
    fn edit_header(recs: &mut [Rec], edit: impl FnOnce(&mut BundleHeader)) {
        let mut header = header_of(recs);
        edit(&mut header);
        recs[0].raw = serde_json::to_vec(&header).unwrap();
        let digest = blake3::hash(&recs[0].raw).to_hex().to_string();
        edit_digests(recs, |d| d.header = digest);
    }

    fn edit_digests(recs: &mut [Rec], edit: impl FnOnce(&mut BundleDigests)) {
        let mut digests = digests_of(recs);
        edit(&mut digests);
        recs.last_mut().unwrap().raw = serde_json::to_vec(&digests).unwrap();
    }

    /// Imports `f.bundle` into `f.dst` and expects a refusal that leaves `f.dst` as it was.
    fn refused(f: &Fixture, exe: &str) -> ImportError {
        let before = snapshot(&f.dst);
        let err = import_file(&f.bundle, &f.dst, exe).unwrap_err();
        assert_eq!(
            snapshot(&f.dst),
            before,
            "a refusal changed the cache dir: {err}"
        );
        err
    }

    #[test]
    fn round_trip_is_byte_identical() {
        let f = fixture();
        let before = snapshot(&f.src);
        let summary = import_file(&f.bundle, &f.dst, EXE).unwrap();
        assert_eq!(snapshot(&f.dst), before);
        // The export leaves the image where it was.
        assert_eq!(snapshot(&f.src), before);
        // image.bin, three paging files and the MANIFEST.
        assert_eq!(summary.members, 5);
        assert_eq!(summary.created_at_ms, 1001);
    }

    #[test]
    fn round_trip_into_a_fresh_checkout() {
        let f = fixture();
        let fresh = f.dst.parent().unwrap().join("other/buck-out/v2/cache");
        import_file(&f.bundle, &fresh, EXE).unwrap();
        assert_eq!(snapshot(&fresh), snapshot(&f.src));
    }

    /// A pipe hands over a few bytes at a time, never a whole record.
    struct Trickle<R>(R, usize);

    impl<R: Read> Read for Trickle<R> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.1 = self.1 % 7 + 1;
            let n = buf.len().min(self.1);
            self.0.read(&mut buf[..n])
        }
    }

    #[test]
    fn round_trip_from_a_stream_on_any_number_of_threads() {
        let f = fixture();
        for threads in [1, 2, 16] {
            let dst = f.dst.parent().unwrap().join(format!("t{threads}/cache"));
            let input = Trickle(File::open(&f.bundle).unwrap(), 0);
            let summary = import_from(input, &dst, EXE, threads, &mut |_| Ok(())).unwrap();
            assert_eq!(summary.threads, threads);
            assert_eq!(snapshot(&dst), snapshot(&f.src), "{threads} threads");
        }
    }

    #[test]
    fn layout_is_head_chunks_in_order_end() {
        let f = fixture();
        let recs = records(&f.bundle);
        let header = header_of(&recs);
        let paths: Vec<&str> = header.members.iter().map(|m| m.path.as_str()).collect();
        assert_eq!(
            paths,
            [
                "suspend/MANIFEST",
                "suspend/image.bin",
                "dice_state/pagable.0.db",
                "dice_state/pagable.1.db",
                "dice_state/pagable.2.db",
            ]
        );
        assert_eq!(header.chunk_size, TEST_CHUNK);
        let kinds: Vec<(Kind, u32, u64)> =
            recs.iter().map(|r| (r.kind, r.member, r.chunk)).collect();
        assert_eq!(
            kinds,
            [
                (Kind::Head, 0, 0),
                (Kind::Data, 0, 0),
                (Kind::Data, 1, 0),
                (Kind::Data, 1, 1),
                (Kind::Data, 2, 0),
                (Kind::Data, 2, 1),
                (Kind::Data, 2, 2),
                (Kind::Data, 2, 3),
                (Kind::Data, 3, 0),
                (Kind::End, 0, 0),
            ]
        );
        // The export writes the same bytes on any number of threads.
        let again = f.bundle.with_file_name("again.bundle");
        export_with(&f.src, &again, TEST_CHUNK, 1).unwrap();
        assert_eq!(
            std::fs::read(&again).unwrap(),
            std::fs::read(&f.bundle).unwrap()
        );
    }

    /// What lets an import decode on every core, and start before the download ends: each
    /// chunk's frame decodes on its own, from the bundle's bytes alone, to the member's bytes.
    #[test]
    fn every_chunk_decodes_on_its_own() {
        let f = fixture();
        let bytes = std::fs::read(&f.bundle).unwrap();
        let header = header_of(&records(&f.bundle));
        let mut chunks = 0;
        for (at, rec) in records_at(&f.bundle) {
            if rec.kind != Kind::Data {
                continue;
            }
            let head =
                RecordHead::decode(bytes[at..at + RECORD_HEAD_LEN].try_into().unwrap()).unwrap();
            let frame = &bytes[at + RECORD_HEAD_LEN..at + RECORD_HEAD_LEN + head.zlen as usize];
            let mut d = zstd::bulk::Decompressor::new().unwrap();
            let decoded = d.decompress(frame, head.raw_len as usize).unwrap();
            let member = &header.members[head.member as usize];
            let file = std::fs::read(f.src.join(&member.path)).unwrap();
            let start = (head.chunk * TEST_CHUNK) as usize;
            assert_eq!(decoded, file[start..start + decoded.len()]);
            chunks += 1;
        }
        assert_eq!(chunks, 8);
    }

    #[test]
    /// After the MANIFEST, which an import reads first.
    fn chunks_in_any_order_are_accepted() {
        let f = fixture();
        let mut recs = records(&f.bundle);
        let last = recs.len() - 1;
        recs[2..last].reverse();
        write_records(&f.bundle, &recs);
        import_file(&f.bundle, &f.dst, EXE).unwrap();
        assert_eq!(snapshot(&f.dst), snapshot(&f.src));
    }

    #[test]
    fn a_chunk_at_another_offset_is_refused() {
        let f = fixture();
        let mut recs = records(&f.bundle);
        // Two whole chunks of pagable.0.db trade places but keep their data.
        let a = recs
            .iter()
            .position(|r| (r.member, r.chunk) == (2, 0))
            .unwrap();
        let b = recs
            .iter()
            .position(|r| (r.member, r.chunk) == (2, 1))
            .unwrap();
        recs[a].chunk = 1;
        recs[b].chunk = 0;
        write_records(&f.bundle, &recs);
        assert!(matches!(
            refused(&f, EXE),
            ImportError::Mismatch(p) if p == "dice_state/pagable.0.db"
        ));
    }

    #[test]
    fn flipped_byte_in_any_record_is_refused() {
        let f = fixture();
        let original = records(&f.bundle);
        for i in 0..original.len() {
            let mut changed = original.clone();
            let data = &mut changed[i].raw;
            let at = data.len() / 2;
            data[at] ^= 0x01;
            write_records(&f.bundle, &changed);
            refused(&f, EXE);
        }
    }

    #[test]
    fn flipped_byte_in_the_compressed_stream_is_refused() {
        let f = fixture();
        let original = std::fs::read(&f.bundle).unwrap();
        let mut bytes = original.clone();
        let at = bytes.len() / 2;
        bytes[at] ^= 0x10;
        std::fs::write(&f.bundle, &bytes).unwrap();
        refused(&f, EXE);
        // Anywhere: a flip is refused, or (in a frame's header, say a larger window) changes
        // nothing that decodes, so the import is the original's. Never anything else.
        let want = snapshot(&f.src);
        for at in (0..original.len()).step_by(97) {
            let mut bytes = original.clone();
            bytes[at] ^= 0x04;
            std::fs::write(&f.bundle, &bytes).unwrap();
            let before = snapshot(&f.dst);
            match import_file(&f.bundle, &f.dst, EXE) {
                Ok(_) => assert_eq!(snapshot(&f.dst), want, "flip at {at} accepted"),
                Err(_) => assert_eq!(snapshot(&f.dst), before, "flip at {at}"),
            }
        }
    }

    #[test]
    fn truncation_is_refused() {
        let f = fixture();
        let bytes = std::fs::read(&f.bundle).unwrap();
        let starts: Vec<usize> = records_at(&f.bundle).iter().map(|(at, _)| *at).collect();
        for keep in [
            0,
            10,
            PROLOGUE_LEN,
            PROLOGUE_LEN + 5,
            bytes.len() / 3,
            bytes.len() / 2,
            bytes.len() - 4,
            bytes.len() - 1,
        ] {
            std::fs::write(&f.bundle, &bytes[..keep]).unwrap();
            refused(&f, EXE);
        }
        // A stream cut cleanly after a whole record, so only the missing `END` tells.
        for &keep in &starts[1..] {
            std::fs::write(&f.bundle, &bytes[..keep]).unwrap();
            let err = refused(&f, EXE);
            assert!(matches!(err, ImportError::NoTrailer), "kept {keep}: {err}");
        }
    }

    /// A download that dies part way, or an import killed while it unpacks, leaves the image
    /// the cache dir had and nothing of the bundle.
    #[test]
    fn an_interrupted_stream_leaves_the_old_image() {
        struct Dies<R>(R, usize);
        impl<R: Read> Read for Dies<R> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.1 == 0 {
                    return Err(io::Error::other("connection reset"));
                }
                let n = buf.len().min(self.1);
                let n = self.0.read(&mut buf[..n])?;
                self.1 -= n;
                Ok(n)
            }
        }
        let f = fixture();
        let len = std::fs::metadata(&f.bundle).unwrap().len() as usize;
        for keep in [len / 4, len / 2, len - 30] {
            let before = snapshot(&f.dst);
            let input = Dies(File::open(&f.bundle).unwrap(), keep);
            let err = import_from(input, &f.dst, EXE, 4, &mut |_| Ok(())).unwrap_err();
            assert!(matches!(err, ImportError::Io { .. }), "{keep}: {err}");
            assert_eq!(snapshot(&f.dst), before);
            let left: Vec<_> = std::fs::read_dir(&f.dst)
                .unwrap()
                .map(|e| e.unwrap().file_name().into_string().unwrap())
                .filter(|n| n.contains(".import."))
                .collect();
            assert!(left.is_empty(), "{keep}: {left:?}");
        }
    }

    #[test]
    fn a_wrong_digest_is_refused() {
        let f = fixture();
        let original = records(&f.bundle);
        for path in [
            "suspend/image.bin",
            "dice_state/pagable.2.db",
            "suspend/MANIFEST",
        ] {
            let mut recs = original.clone();
            edit_digests(&mut recs, |d| {
                let m = d.files.iter_mut().find(|m| m.path == path).unwrap();
                m.blake3 = blake3::hash(b"other").to_hex().to_string();
            });
            write_records(&f.bundle, &recs);
            assert!(matches!(refused(&f, EXE), ImportError::Mismatch(p) if p == path));
        }
        let mut recs = original.clone();
        edit_digests(&mut recs, |d| {
            d.header = blake3::hash(b"other").to_hex().to_string()
        });
        write_records(&f.bundle, &recs);
        assert!(matches!(refused(&f, EXE), ImportError::Mismatch(p) if p == HEAD));
        let mut recs = original.clone();
        edit_digests(&mut recs, |d| d.files[1].len += 1);
        write_records(&f.bundle, &recs);
        assert!(matches!(refused(&f, EXE), ImportError::Mismatch(_)));
    }

    /// A member `HEAD` says is longer than its data: its last chunks never come.
    #[test]
    fn a_missing_chunk_is_refused() {
        let f = fixture();
        let mut recs = records(&f.bundle);
        recs.retain(|r| (r.kind, r.member, r.chunk) != (Kind::Data, 2, 2));
        write_records(&f.bundle, &recs);
        assert!(matches!(
            refused(&f, EXE),
            ImportError::Missing(p) if p == "dice_state/pagable.0.db"
        ));
    }

    #[test]
    fn a_chunk_of_the_wrong_length_is_refused() {
        let f = fixture();
        let mut recs = records(&f.bundle);
        let i = recs
            .iter()
            .position(|r| (r.member, r.chunk) == (1, 1))
            .unwrap();
        recs[i].raw.push(0);
        write_records(&f.bundle, &recs);
        assert!(matches!(refused(&f, EXE), ImportError::Mismatch(p) if p == "suspend/image.bin"));
    }

    /// Keeps the bundle up to the MANIFEST and makes everything after it bytes that are no
    /// record: an import that reads past the MANIFEST fails on them, not on the MANIFEST.
    fn cut_after_the_manifest(f: &Fixture) {
        let starts: Vec<usize> = records_at(&f.bundle).iter().map(|(at, _)| *at).collect();
        let mut bytes = std::fs::read(&f.bundle).unwrap();
        bytes.truncate(starts[2]);
        bytes.extend(vec![0xA5u8; 1 << 20]);
        std::fs::write(&f.bundle, bytes).unwrap();
    }

    #[test]
    fn a_manifest_at_odds_with_head_is_refused_before_the_rest_is_read() {
        let f = fixture();
        let mut recs = records(&f.bundle);
        let mut manifest: serde_json::Value = serde_json::from_slice(&recs[1].raw).unwrap();
        manifest["exe"] = serde_json::Value::String("another".to_owned());
        recs[1].raw = serde_json::to_vec_pretty(&manifest).unwrap();
        let len = recs[1].raw.len() as u64;
        edit_header(&mut recs, |h| h.members[0].len = len);
        write_records(&f.bundle, &recs);
        cut_after_the_manifest(&f);
        let err = refused(&f, EXE);
        assert!(
            matches!(&err, ImportError::NotABundle(why) if why.contains("by another")),
            "{err}"
        );
        // The same cut bundle with a good MANIFEST fails only on what follows it.
        let f = fixture();
        cut_after_the_manifest(&f);
        assert!(matches!(refused(&f, EXE), ImportError::Damaged(_)));
    }

    #[test]
    fn a_manifest_that_does_not_follow_head_is_refused() {
        let f = fixture();
        let mut recs = records(&f.bundle);
        let manifest = recs.remove(1);
        let last = recs.len() - 1;
        recs.insert(last, manifest);
        write_records(&f.bundle, &recs);
        assert!(matches!(refused(&f, EXE), ImportError::NotABundle(_)));
    }

    #[test]
    fn another_image_format_is_refused() {
        let f = fixture();
        let mut recs = records(&f.bundle);
        edit_header(&mut recs, |h| h.image_format = FORMAT + 1);
        write_records(&f.bundle, &recs);
        assert!(matches!(refused(&f, EXE), ImportError::ImageFormat(v) if v == FORMAT + 1));
    }

    #[test]
    fn another_bundle_format_is_refused() {
        let f = fixture();
        let mut bytes = std::fs::read(&f.bundle).unwrap();
        bytes[14..16].copy_from_slice(&(BUNDLE_FORMAT + 1).to_le_bytes());
        std::fs::write(&f.bundle, &bytes).unwrap();
        assert!(matches!(refused(&f, EXE), ImportError::BundleFormat(v) if v == 3));
        // Format 1 was one zstd frame of a tar.
        std::fs::write(
            &f.bundle,
            zstd::stream::encode_all(&b"BUNDLE"[..], 3).unwrap(),
        )
        .unwrap();
        assert!(matches!(refused(&f, EXE), ImportError::BundleFormat(1)));
        std::fs::write(&f.bundle, b"not a bundle at all").unwrap();
        assert!(matches!(refused(&f, EXE), ImportError::NotABundle(_)));
    }

    #[test]
    fn another_binary_is_refused() {
        let f = fixture();
        assert!(matches!(
            refused(&f, "another"),
            ImportError::OtherBinary { .. }
        ));
    }

    #[test]
    fn header_must_come_first() {
        let f = fixture();
        let mut recs = records(&f.bundle);
        recs.swap(0, 1);
        write_records(&f.bundle, &recs);
        assert!(matches!(refused(&f, EXE), ImportError::NotABundle(_)));
    }

    #[test]
    fn a_chunk_size_out_of_bounds_is_refused() {
        for size in [0, MIN_CHUNK_SIZE - 1, MAX_CHUNK_SIZE + 1] {
            let f = fixture();
            let mut recs = records(&f.bundle);
            edit_header(&mut recs, |h| h.chunk_size = size);
            write_records(&f.bundle, &recs);
            assert!(
                matches!(refused(&f, EXE), ImportError::NotABundle(_)),
                "{size}"
            );
        }
    }

    /// A bundle without the image or without the MANIFEST is refused even when its listing
    /// agrees, and the cache dir keeps the image it had.
    #[test]
    fn a_bundle_without_the_image_or_the_manifest_is_refused() {
        for gone in [
            format!("{IMAGE_DIR}/{IMAGE}"),
            format!("{IMAGE_DIR}/{MANIFEST}"),
        ] {
            let f = fixture();
            let mut recs = records(&f.bundle);
            let index = header_of(&recs)
                .members
                .iter()
                .position(|m| m.path == gone)
                .unwrap() as u32;
            recs.retain(|r| r.kind != Kind::Data || r.member != index);
            for r in &mut recs {
                if r.kind == Kind::Data && r.member > index {
                    r.member -= 1;
                }
            }
            edit_header(&mut recs, |h| h.members.retain(|m| m.path != gone));
            edit_digests(&mut recs, |d| d.files.retain(|m| m.path != gone));
            write_records(&f.bundle, &recs);
            let err = refused(&f, EXE);
            assert!(
                matches!(&err, ImportError::Missing(p) if *p == gone),
                "{gone}: {err}"
            );
        }
    }

    /// A file that yields fewer bytes than its length said, as one cut short while it is
    /// bundled, fails the export.
    #[test]
    fn a_member_that_shrinks_while_it_is_bundled_fails_the_export() {
        let err = read_chunks("suspend/image.bin", 10, 4, &b"abc"[..], |_, _| Ok(())).unwrap_err();
        assert!(
            format!("{err:#}").contains("shrank from 10 to 3 bytes"),
            "{err:#}"
        );
        let err =
            read_chunks("suspend/image.bin", 10, 4, &b"abcdef"[..], |_, _| Ok(())).unwrap_err();
        assert!(
            format!("{err:#}").contains("shrank from 10 to 6 bytes"),
            "{err:#}"
        );
    }

    /// A member `HEAD` holds that `END` does not list, and one `END` lists that `HEAD` lacks.
    #[test]
    fn unlisted_member_is_refused() {
        let f = fixture();
        let mut recs = records(&f.bundle);
        let extra = header_of(&recs).members.len() as u32;
        edit_header(&mut recs, |h| {
            h.members.push(HeaderMember {
                path: "dice_state/extra.db".to_owned(),
                len: 4,
            })
        });
        let end = recs.len() - 1;
        recs.insert(
            end,
            Rec {
                kind: Kind::Data,
                member: extra,
                chunk: 0,
                raw: b"evil".to_vec(),
            },
        );
        write_records(&f.bundle, &recs);
        assert!(matches!(
            refused(&f, EXE),
            ImportError::Mismatch(p) if p == "dice_state/extra.db"
        ));

        let f = fixture();
        let mut recs = records(&f.bundle);
        edit_digests(&mut recs, |d| {
            d.files.push(BundleMember {
                path: "dice_state/extra.db".to_owned(),
                len: 4,
                blake3: String::new(),
            })
        });
        write_records(&f.bundle, &recs);
        assert!(matches!(
            refused(&f, EXE),
            ImportError::Missing(p) if p == "dice_state/extra.db"
        ));
    }

    #[test]
    fn member_outside_the_image_is_refused() {
        for name in [
            "../x",
            "dice_state/../../x",
            "/etc/passwd",
            "suspend/other",
            "materializer_state/db.sqlite",
            "x",
            "dice_state//x",
            "./dice_state/x",
            "dice_state",
            "BUNDLE",
        ] {
            let f = fixture();
            let mut recs = records(&f.bundle);
            edit_header(&mut recs, |h| {
                h.members.push(HeaderMember {
                    path: name.to_owned(),
                    len: 0,
                })
            });
            write_records(&f.bundle, &recs);
            let err = refused(&f, EXE);
            assert!(matches!(err, ImportError::BadMember(_)), "{name}: {err}");
            // Nothing was written outside the cache dir either.
            assert!(!f.dst.parent().unwrap().join("x").exists());
        }
    }

    #[test]
    fn a_chunk_of_no_member_is_refused() {
        let f = fixture();
        let mut recs = records(&f.bundle);
        let end = recs.len() - 1;
        recs.insert(
            end,
            Rec {
                kind: Kind::Data,
                member: 99,
                chunk: 0,
                raw: b"evil".to_vec(),
            },
        );
        write_records(&f.bundle, &recs);
        assert!(matches!(refused(&f, EXE), ImportError::BadMember(_)));
    }

    #[test]
    fn duplicate_member_or_chunk_is_refused() {
        let f = fixture();
        let mut recs = records(&f.bundle);
        edit_header(&mut recs, |h| {
            let first = h.members[1].clone();
            h.members.push(first);
        });
        write_records(&f.bundle, &recs);
        assert!(matches!(refused(&f, EXE), ImportError::Duplicate(_)));

        let f = fixture();
        let mut recs = records(&f.bundle);
        let again = recs[2].clone();
        recs.insert(3, again);
        write_records(&f.bundle, &recs);
        assert!(matches!(refused(&f, EXE), ImportError::Duplicate(_)));

        let f = fixture();
        let mut recs = records(&f.bundle);
        let head = recs[0].clone();
        recs.insert(2, head);
        write_records(&f.bundle, &recs);
        assert!(matches!(refused(&f, EXE), ImportError::Duplicate(_)));
    }

    #[test]
    fn anything_after_the_end_is_refused() {
        let f = fixture();
        let mut recs = records(&f.bundle);
        recs.push(recs[1].clone());
        write_records(&f.bundle, &recs);
        assert!(matches!(refused(&f, EXE), ImportError::AfterTrailer(_)));

        let f = fixture();
        let mut bytes = std::fs::read(&f.bundle).unwrap();
        bytes.extend_from_slice(b"junk");
        std::fs::write(&f.bundle, &bytes).unwrap();
        refused(&f, EXE);
    }

    #[test]
    fn running_daemon_is_refused_before_anything_is_read() {
        let f = fixture();
        std::fs::write(&f.bundle, b"not even a bundle").unwrap();
        let before = snapshot(&f.dst);
        let err = import(
            &BundleSource::File(f.bundle.clone()),
            &f.dst,
            EXE,
            Some(4242),
        )
        .unwrap_err();
        assert!(matches!(err, ImportError::DaemonRunning(4242)));
        assert_eq!(snapshot(&f.dst), before);
    }

    #[test]
    fn failure_after_the_paging_store_moves_leaves_no_manifest() {
        for fail_at in [
            InstallStep::OldRemoved,
            InstallStep::DiceStateInstalled,
            InstallStep::ImageInstalled,
        ] {
            let f = fixture();
            let input = File::open(&f.bundle).unwrap();
            let err = import_from(input, &f.dst, EXE, 4, &mut |s| {
                if s == fail_at {
                    Err(io::Error::other("injected"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
            assert!(matches!(err, ImportError::Io { .. }), "{fail_at:?}: {err}");
            assert!(
                !f.dst.join(IMAGE_DIR).join(MANIFEST).exists(),
                "{fail_at:?} left a MANIFEST"
            );
            // The unpacked copy is cleaned up; the next daemon start wipes the rest.
            let left: Vec<_> = std::fs::read_dir(&f.dst)
                .unwrap()
                .map(|e| e.unwrap().file_name().into_string().unwrap())
                .filter(|n| n.contains(".import."))
                .collect();
            assert!(left.is_empty(), "{fail_at:?}: {left:?}");
        }
    }

    #[test]
    fn export_without_an_image_fails_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        std::fs::create_dir_all(cache.join(DICE_STATE_DIR)).unwrap();
        let out = dir.path().join("out.bundle");
        assert!(export(&cache, &out).is_err());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn export_refuses_a_link_in_the_paging_store() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        write_image(&cache, 1);
        std::os::unix::fs::symlink("/etc/passwd", cache.join(DICE_STATE_DIR).join("l")).unwrap();
        let out = dir.path().join("out.bundle");
        assert!(export(&cache, &out).is_err());
        assert!(!out.exists());
    }

    #[test]
    fn export_refuses_an_image_that_is_not_portable() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        write_image(&cache, 1);
        let manifest_path = cache.join(IMAGE_DIR).join(MANIFEST);
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        manifest["portable"] = serde_json::Value::Bool(false);
        std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        let before = snapshot(&cache);
        let out = dir.path().join("out.bundle");
        let err = format!("{:#}", export(&cache, &out).unwrap_err());
        assert!(err.contains("not portable"), "{err}");
        assert!(!out.exists());
        assert_eq!(snapshot(&cache), before);
        // A MANIFEST from before the field existed reads as not portable.
        manifest.as_object_mut().unwrap().remove("portable");
        std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert!(export(&cache, &out).is_err());
    }
}
