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
