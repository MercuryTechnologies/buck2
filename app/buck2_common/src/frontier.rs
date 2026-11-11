/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! The leaves of a frontier record: the DICE keys whose values a record names as its premises,
//! and the canonical form in which it names them.
//!
//! A record says "if every leaf in this tree has the value it had on the writer, the key's value
//! is these rows". A leaf's canonical value is a blake3 digest of exactly what its key's equality
//! compares, never of its value's serialized bytes, which may carry a Starlark heap's identity.
//! A key type this module cannot canonicalize is a barrier: nothing above it is shared.

use std::any::Any;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use buck2_core::cells::cell_path::CellPath;
use serde::Deserialize;
use serde::Serialize;

use crate::file_ops::metadata::RawPathMetadata;
use crate::file_ops::metadata::RawSymlink;
use crate::file_ops::metadata::ReadDirOutput;

/// Set once at daemon start from `frontier.mode`: whether anything other than `off` is on. Read by
/// keys whose shape changes with the frontier (the section projections), so that with the mode
/// off the graph is the one an unmodified binary builds.
static FRONTIER_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn frontier_enabled() -> bool {
    FRONTIER_ENABLED.load(Ordering::Relaxed)
}

pub fn set_frontier_enabled(enabled: bool) {
    FRONTIER_ENABLED.store(enabled, Ordering::Relaxed);
}

pub use blake3::Hasher as FrontierHasher;

/// A deterministic digest of a value set from outside DICE. An implementation destructures the
/// value with no `..`, so that a new field fails to compile until someone decides how it enters
/// the digest.
pub trait FrontierLeaf {
    fn frontier_digest(&self, h: &mut blake3::Hasher);
}

/// What a leaf is, by name: enough for a reader to compute the same leaf through its own DICE.
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize
)]
pub enum LeafId {
    /// `PathMetadataKey`, or the path a `ReadFileKey` reads (whose own value is a handle).
    Path { cell: String, path: String },
    /// `ReadDirKey`.
    Listing {
        cell: String,
        path: String,
        check_ignores: bool,
    },
    /// `ExistsMatchingExactCaseKey`.
    Exists { cell: String, path: String },
    /// `LegacyBuckConfigPropertyProjectionKey`.
    Config {
        cell: String,
        section: String,
        property: String,
    },
    /// `LegacyBuckConfigErrorKey`: whether the cell's config parsed.
    ConfigError { cell: String },
    /// The section projection the frontier adds for whole-section readers.
    Sections { cell: String, sections: Vec<String> },
    /// An injected key, by type.
    Injected { key_type: String },
    /// A computed key whose value this record trusts by its value row, not by walking into it:
    /// a locally executed action.
    LocalOut { key_type: String, key_id: String },
    /// The frontier version (root bodies only).
    Version,
}

impl LeafId {
    pub fn kind(&self) -> &'static str {
        match self {
            LeafId::Path { .. } => "path",
            LeafId::Listing { .. } => "listing",
            LeafId::Exists { .. } => "exists",
            LeafId::Config { .. } => "config",
            LeafId::ConfigError { .. } => "config-error",
            LeafId::Sections { .. } => "section",
            LeafId::Injected { .. } => "injected",
            LeafId::LocalOut { .. } => "local-out",
            LeafId::Version => "version",
        }
    }

    pub fn path_of(cell_path: &CellPath) -> (String, String) {
        (
            cell_path.cell().as_str().to_owned(),
            cell_path.path().as_str().to_owned(),
        )
    }
}

/// A canonical leaf value: a blake3 digest, hex.
pub type Canon = String;

pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(s, "{b:02x}").unwrap();
    }
    s
}

fn finish(h: blake3::Hasher) -> Canon {
    hex(h.finalize().as_bytes())
}

fn field(h: &mut blake3::Hasher, s: &str) {
    h.update(&(s.len() as u64).to_le_bytes());
    h.update(s.as_bytes());
}

/// What a frontier walk learnt about one node.
#[derive(Clone, Debug)]
pub enum Inspected {
    /// A leaf. `canon` is `None` when the node's own value is not the leaf's (a `ReadFileKey`,
    /// whose path leaf the walk computes through `PathMetadataKey`).
    Leaf { id: LeafId, canon: Option<Canon> },
    /// Nothing above this node can be shared.
    Barrier(String),
    /// A premise whose role across daemons the leaves take over (`FileSystemBaselineKey`).
    Skip,
}

impl Inspected {
    pub fn boxed(self) -> Box<dyn Any + Send + Sync> {
        Box::new(self)
    }
}

/// `PathMetadataKey`'s value: its kind, and for a file its digest and exec bit, so that a mode
/// change and a type change both change the leaf.
pub fn canonical_path_metadata(value: &buck2_error::Result<Option<RawPathMetadata>>) -> Canon {
    let mut h = blake3::Hasher::new();
    h.update(b"path-v1");
    match value {
        Err(e) => {
            field(&mut h, "error");
            field(&mut h, &format!("{e}"));
        }
        Ok(None) => field(&mut h, "absent"),
        Ok(Some(RawPathMetadata::Directory)) => field(&mut h, "dir"),
        Ok(Some(RawPathMetadata::File(meta))) => {
            let crate::file_ops::metadata::FileMetadata {
                digest,
                is_executable,
            } = meta;
            field(&mut h, "file");
            field(&mut h, &format!("{digest}"));
            h.update(&[*is_executable as u8]);
        }
        Ok(Some(RawPathMetadata::Symlink { at, to })) => {
            field(&mut h, "symlink");
            field(&mut h, &format!("{at}"));
            match to {
                RawSymlink::Relative(target, link) => {
                    field(&mut h, "relative");
                    field(&mut h, &format!("{target}"));
                    field(&mut h, &format!("{link:?}"));
                }
                RawSymlink::External(ext) => {
                    field(&mut h, "external");
                    field(&mut h, &format!("{ext:?}"));
                }
            }
        }
    }
    finish(h)
}

/// `ReadDirKey`'s value: the sorted `(name, type)` entries, so that an untracked file appearing
/// in a globbed directory changes the leaf.
pub fn canonical_listing(value: &buck2_error::Result<ReadDirOutput>) -> Canon {
    let mut h = blake3::Hasher::new();
    h.update(b"listing-v1");
    match value {
        Err(e) => {
            field(&mut h, "error");
            field(&mut h, &format!("{e}"));
        }
        Ok(out) => {
            field(&mut h, "ok");
            for entry in out.included.iter() {
                field(&mut h, entry.file_name.as_str());
                field(&mut h, &format!("{:?}", entry.file_type));
            }
        }
    }
    finish(h)
}

pub fn canonical_exists(value: &buck2_error::Result<bool>) -> Canon {
    let mut h = blake3::Hasher::new();
    h.update(b"exists-v1");
    match value {
        Err(e) => {
            field(&mut h, "error");
            field(&mut h, &format!("{e}"));
        }
        Ok(b) => {
            field(&mut h, "ok");
            h.update(&[*b as u8]);
        }
    }
    finish(h)
}

/// A config property's value. Only called for a value whose origin is entirely in tracked files:
/// see [`ConfigOrigin`].
pub fn canonical_config_value(value: Option<&str>) -> Canon {
    let mut h = blake3::Hasher::new();
    h.update(b"config-v1");
    match value {
        None => field(&mut h, "absent"),
        Some(v) => {
            field(&mut h, "value");
            field(&mut h, v);
        }
    }
    finish(h)
}

/// A projection of whole sections: the sorted `(section, property, value)` triples.
pub fn canonical_sections<'a>(entries: impl IntoIterator<Item = (&'a str, &'a str, &'a str)>) -> Canon {
    let mut entries: Vec<_> = entries.into_iter().collect();
    entries.sort();
    let mut h = blake3::Hasher::new();
    h.update(b"sections-v1");
    for (s, p, v) in entries {
        field(&mut h, s);
        field(&mut h, p);
        field(&mut h, v);
    }
    finish(h)
}

pub fn canonical_config_error(is_error: bool) -> Canon {
    let mut h = blake3::Hasher::new();
    h.update(b"config-error-v1");
    h.update(&[is_error as u8]);
    finish(h)
}

pub fn canonical_injected(key_type: &str, leaf: &dyn FrontierLeaf) -> Canon {
    let mut h = blake3::Hasher::new();
    h.update(b"injected-v1");
    field(&mut h, key_type);
    leaf.frontier_digest(&mut h);
    finish(h)
}

/// Feeds a string into a [`FrontierLeaf::frontier_digest`].
pub fn digest_str(h: &mut blake3::Hasher, s: &str) {
    field(h, s);
}

/// Where a config value came from. Only a value read entirely from files the writer's checkout
/// tracks may be named in a record by its digest; any other is named by property alone, and makes
/// the record unshareable, so that no credential (from `~/.buckconfig.d`, `/etc/buckconfig.d`, an
/// untracked `.buckconfig.local` or a `-c`) reaches a store, even hashed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigOrigin {
    Absent,
    /// The files of the include chain, as the config parser recorded them.
    Files(Vec<String>),
    /// A `-c` on the command line, or a value set by a `--config-file` the parser could not
    /// place.
    CommandLine,
    /// The value interpolates another (`$(config ...)`), so its origin is not its own.
    Interpolated,
}

impl FrontierLeaf for std::collections::BTreeSet<String> {
    fn frontier_digest(&self, h: &mut blake3::Hasher) {
        h.update(&(self.len() as u64).to_le_bytes());
        for s in self {
            field(h, s);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use buck2_core::cells::cell_path::CellPath;
    use buck2_core::cells::name::CellName;
    use buck2_core::cells::paths::CellRelativePathBuf;
    use buck2_fs::paths::file_name::FileNameBuf;

    use super::*;
    use crate::cas_digest::CasDigestConfig;
    use crate::file_ops::metadata::FileMetadata;
    use crate::file_ops::metadata::FileType;
    use crate::file_ops::metadata::SimpleDirEntry;
    use crate::file_ops::metadata::Symlink;
    use crate::file_ops::metadata::TrackedFileDigest;

    fn file(content: &[u8], exec: bool) -> buck2_error::Result<Option<RawPathMetadata>> {
        let config = CasDigestConfig::testing_default();
        let digest = TrackedFileDigest::from_content(content, config);
        Ok(Some(RawPathMetadata::File(FileMetadata {
            digest,
            is_executable: exec,
        })))
    }

    fn cell_path(p: &str) -> CellPath {
        CellPath::new(
            CellName::testing_new("root"),
            &CellRelativePathBuf::unchecked_new(p.to_owned()),
        )
    }

    #[test]
    fn content_change_changes_the_path_leaf() {
        assert_ne!(
            canonical_path_metadata(&file(b"a", false)),
            canonical_path_metadata(&file(b"b", false))
        );
        assert_eq!(
            canonical_path_metadata(&file(b"a", false)),
            canonical_path_metadata(&file(b"a", false))
        );
    }

    #[test]
    fn mode_change_changes_the_path_leaf() {
        // `chmod +x` with the content unchanged: git shows a mode change, a blob-only check does
        // not.
        assert_ne!(
            canonical_path_metadata(&file(b"a", false)),
            canonical_path_metadata(&file(b"a", true))
        );
    }

    #[test]
    fn type_change_changes_the_path_leaf() {
        let as_file = canonical_path_metadata(&file(b"a", false));
        let as_symlink = canonical_path_metadata(&Ok(Some(RawPathMetadata::Symlink {
            at: cell_path("x"),
            to: RawSymlink::Relative(cell_path("a"), Arc::new(Symlink::new(buck2_fs::paths::RelativePathBuf::from("a")))),
        })));
        let as_dir = canonical_path_metadata(&Ok(Some(RawPathMetadata::Directory)));
        let absent = canonical_path_metadata(&Ok(None));
        let all = [&as_file, &as_symlink, &as_dir, &absent];
        for (i, a) in all.iter().enumerate() {
            for b in &all[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    fn listing(names: &[(&str, FileType)]) -> buck2_error::Result<ReadDirOutput> {
        let mut v: Vec<SimpleDirEntry> = names
            .iter()
            .map(|(n, t)| SimpleDirEntry {
                file_name: FileNameBuf::unchecked_new(*n),
                file_type: t.clone(),
            })
            .collect();
        v.sort();
        Ok(ReadDirOutput {
            included: v.into(),
        })
    }

    #[test]
    fn untracked_file_changes_the_listing_leaf() {
        let before = canonical_listing(&listing(&[("a.txt", FileType::File)]));
        let after = canonical_listing(&listing(&[
            ("a.txt", FileType::File),
            ("untracked.txt", FileType::File),
        ]));
        assert_ne!(before, after);
        // A directory and a file of one name are different listings.
        assert_ne!(
            canonical_listing(&listing(&[("a", FileType::File)])),
            canonical_listing(&listing(&[("a", FileType::Directory)]))
        );
    }

    #[test]
    fn config_projection_change_changes_the_config_leaf() {
        assert_ne!(
            canonical_config_value(Some("1")),
            canonical_config_value(Some("2"))
        );
        assert_ne!(canonical_config_value(None), canonical_config_value(Some("")));
        assert_ne!(
            canonical_sections([("cell_aliases", "a", "root")]),
            canonical_sections([("cell_aliases", "a", "other")])
        );
        // Order of entries does not matter, their content does.
        assert_eq!(
            canonical_sections([("s", "a", "1"), ("s", "b", "2")]),
            canonical_sections([("s", "b", "2"), ("s", "a", "1")])
        );
    }

    #[test]
    fn exists_leaf() {
        assert_ne!(canonical_exists(&Ok(true)), canonical_exists(&Ok(false)));
    }
}
