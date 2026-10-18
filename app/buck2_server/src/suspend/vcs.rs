/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! What git tells a suspend and a resume about the checkout, so that a resume on a fresh clone
//! need not read every file. Git only ever narrows the files a resume reads: whenever its
//! answer could be wrong, it vouches for nothing, and the resume compares contents instead.
//!
//! One known limit. A file's stat in the index was recorded when git last wrote the file, under
//! the attributes of that checkout, and the resume sees only the attributes in HEAD, the index
//! and the working tree. An attribute that applied at an earlier checkout and is gone from all
//! three can let git vouch for converted bytes: check out C, whose `.gitattributes` gives
//! `*.txt filter=upper`, so that `f.txt` is written upper-cased; then check out D, which has no
//! `.gitattributes` and the same blob for `f.txt`, so git keeps the file and its stat. The index
//! now says `f.txt` is its blob, unconverted, and the resume believes it, as `git status` does.
//! A conversion that changes the size is still caught by the stat; a filter that keeps it is
//! not. A fresh clone of a single commit, which is
//! what a CI runner resuming an image checks out, has no earlier checkout and cannot reach this;
//! a runner that reuses one checkout across commits whose attributes differ can, and should
//! clone afresh.

use std::collections::HashMap;
use std::collections::HashSet;

use buck2_core::fs::project_rel_path::ProjectRelativePath;
use buck2_core::fs::project_rel_path::ProjectRelativePathBuf;
use buck2_error::ErrorTag;
use buck2_error::buck2_error;
use buck2_file_watcher::file_watcher::IndexEntry;
use buck2_file_watcher::file_watcher::IndexStat;
use buck2_file_watcher::file_watcher::VcsDelta;
use buck2_file_watcher::file_watcher::VcsImage;
use buck2_file_watcher::file_watcher::VcsTree;
use buck2_fs::paths::abs_norm_path::AbsNormPath;

use crate::git::run_git_blocking;

/// The attributes that let a checked-out file's bytes differ from its blob's. `text` (and `crlf`,
/// its old name) normalises line endings when git hashes a file, so a file with CRLF on disk
/// and LF in its blob is clean to git.
const CONVERSION_ATTRIBUTES: &[&str] = &[
    "filter",
    "eol",
    "text",
    "crlf",
    "ident",
    "working-tree-encoding",
];

/// The commit checked out now and the blob id of every regular file it holds under the project,
/// read after a suspend has read the files. `None` when git cannot say, which leaves a resume
/// to compare contents.
pub(crate) fn suspend_tree(root: &AbsNormPath) -> Option<VcsTree> {
    match suspend_tree_inner(root) {
        Ok(tree) => Some(tree),
        Err(e) => {
            tracing::info!("Suspend records no git commit, so a resume will hash: {e:#}");
            None
        }
    }
}

fn suspend_tree_inner(root: &AbsNormPath) -> buck2_error::Result<VcsTree> {
    let head = text(run_git_blocking(
        root,
        &["rev-parse", "--verify", "HEAD"],
        None,
    )?)?
    .trim()
    .to_owned();
    if head.len() != 40 {
        // A SHA-256 repository: its blob ids are not the sha1 the suspend computes.
        return Err(buck2_error!(
            ErrorTag::Input,
            "HEAD `{head}` is not a sha1 object id"
        ));
    }
    let prefix = toplevel_to_project(root)?;
    let tree = run_git_blocking(root, &["ls-tree", "-r", "-z", "--full-tree", &head], None)?;
    let mut blobs = HashMap::new();
    for record in records(&tree) {
        // `<mode> SP <type> SP <object> TAB <path>`
        let Some((meta, path)) = record.split_once('\t') else {
            continue;
        };
        let mut meta = meta.split(' ');
        let (Some(mode), Some(kind), Some(object)) = (meta.next(), meta.next(), meta.next()) else {
            continue;
        };
        if kind != "blob" || !(mode == "100644" || mode == "100755") {
            continue;
        }
        if let (Some(path), Some(id)) = (project_path(&prefix, path), sha1_id(object)) {
            blobs.insert(path, id);
        }
    }
    Ok(VcsTree {
        image: VcsImage {
            head,
            toplevel_to_project: prefix,
        },
        blobs,
    })
}

/// The files git vouches for now, called by a resuming watcher after it has statted the files.
/// Not usable, so that git vouches for nothing, when any git call fails or a setting converts
/// every checkout. A file git cannot vouch for on its own (one git does not look at, one an
/// attribute may convert, or one racily clean) is left out, and the resume hashes it, as it
/// hashes a file whose stat is not the one the index recorded.
///
/// The index's blob ids are compared with the ones the suspend read, so the suspended commit
/// need not be in the clone: a shallow clone, or one fetched without the suspended commit,
/// still has git's help. No git command here reads a file's contents.
pub(crate) fn resume_delta(root: &AbsNormPath, image: &VcsImage) -> VcsDelta {
    resume_delta_racing(root, image, || {})
}

/// [`resume_delta`], with `during` run after the index is first statted and before git reads
/// it, where a test puts a concurrent write of the index.
fn resume_delta_racing(root: &AbsNormPath, image: &VcsImage, during: impl FnOnce()) -> VcsDelta {
    match resume_delta_inner(root, image, during) {
        Ok(delta) => delta,
        Err(e) => {
            tracing::info!("Resume does not use git, and hashes instead: {e:#}");
            VcsDelta::default()
        }
    }
}

fn resume_delta_inner(
    root: &AbsNormPath,
    image: &VcsImage,
    during: impl FnOnce(),
) -> buck2_error::Result<VcsDelta> {
    let prefix = toplevel_to_project(root)?;
    if prefix != image.toplevel_to_project {
        return Err(buck2_error!(
            ErrorTag::Input,
            "the project is at `{prefix}` in the repository, and was at `{}`",
            image.toplevel_to_project
        ));
    }
    // Line-ending conversion on checkout makes a file's bytes differ from its blob's.
    for (key, allowed) in [("core.autocrlf", "false"), ("core.eol", "lf")] {
        let value = text(run_git_blocking(
            root,
            &["config", "--default", "", "--get", key],
            None,
        )?)?
        .trim()
        .to_ascii_lowercase();
        if !value.is_empty() && value != allowed {
            return Err(buck2_error!(ErrorTag::Input, "`{key}` is `{value}`"));
        }
    }

    // The index's modification time, read before the index itself: an entry the index got
    // after this read has a modification time no earlier, so it counts as racily clean.
    let index_path = text(run_git_blocking(
        root,
        &["rev-parse", "--path-format=absolute", "--git-path", "index"],
        None,
    )?)?;
    let index_path = index_path.trim_end_matches('\n');
    // A split index keeps most entries in a shared file written at another time than the
    // index, so the index's modification time does not tell which of them are racily clean.
    let index_dir = std::path::Path::new(index_path).parent().ok_or_else(|| {
        buck2_error!(ErrorTag::Input, "the index `{index_path}` has no directory")
    })?;
    let split = std::fs::read_dir(index_dir)
        .map_err(|e| {
            buck2_error!(
                ErrorTag::Input,
                "`{}` cannot be listed: {e}",
                index_dir.display()
            )
        })?
        .filter_map(|entry| entry.ok())
        .any(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("sharedindex.")
        });
    if split {
        return Err(buck2_error!(
            ErrorTag::Input,
            "the index is split (`core.splitIndex`)"
        ));
    }
    let index_before = IndexStamp::of(std::path::Path::new(index_path))?;
    let index_written = index_before
        .modified
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| buck2_error!(ErrorTag::Input, "the index predates 1970: {e}"))?
        .as_secs() as u32;
    during();

    // The index's `.gitattributes` must be HEAD's, the ones the checkout converted files with,
    // for `check-attr --cached` to stand for them: one staged since would hide a conversion the
    // checkout applied. Cheap, as no file is read.
    run_git_blocking(
        root,
        &[
            "diff-index",
            "--cached",
            "--quiet",
            "HEAD",
            "--",
            ":(top,glob)**/.gitattributes",
        ],
        None,
    )
    .map_err(|_| {
        buck2_error!(
            ErrorTag::Input,
            "a `.gitattributes` is staged that HEAD does not hold, or HEAD cannot be read"
        )
    })?;

    // The attributes are asked for beside the listing, of a listing of their own; an entry the
    // attribute pass did not see, should the index change between the two, is not vouched for.
    let (listing, attributes) = std::thread::scope(|scope| {
        let attributes = scope.spawn(|| conversions(root));
        let listing = run_git_blocking(
            root,
            &["ls-files", "-z", "-v", "-s", "--debug", "--full-name"],
            None,
        );
        (listing, attributes.join())
    });
    let listing = listing?;
    let (checked, converts) =
        attributes.map_err(|_| buck2_error!(ErrorTag::Tier0, "the attribute check panicked"))??;
    // The racy rule holds only for the index whose time was read above. A write of the index
    // since, by a git command running beside the resume, would leave the listing describing an
    // index of another time (one copied back with an older time makes entries look settled
    // that are not), so git vouches for nothing. git writes an index to `index.lock` and
    // renames it, so any write moves the inode as well as the time or size.
    let index_after = IndexStamp::of(std::path::Path::new(index_path))?;
    if index_after != index_before {
        return Err(buck2_error!(
            ErrorTag::Input,
            "the index changed while git read it ({index_before:?} then {index_after:?})"
        ));
    }
    let mut clean = HashMap::new();
    let mut hidden = 0;
    let mut racy = 0;
    let mut converted = 0;
    for (record, stat) in index_records(&listing)? {
        // `<tag> SP <mode> SP <object> SP <stage> TAB <path>`; a lowercase tag is
        // assume-unchanged and `S` skip-worktree, for both of which git stops keeping the
        // entry in step with the file. A file system monitor's marks do not matter: the stat is
        // compared here, not by git.
        let Some((meta, path)) = record.split_once('\t') else {
            continue;
        };
        let mut meta = meta.split(' ');
        let (Some(tag), Some(mode), Some(object), Some(stage)) =
            (meta.next(), meta.next(), meta.next(), meta.next())
        else {
            continue;
        };
        if tag.chars().any(|c| c.is_ascii_lowercase()) || tag == "S" {
            hidden += 1;
            continue;
        }
        if stage != "0" || !(mode == "100644" || mode == "100755") {
            continue;
        }
        // Racily clean: written in the second the index was, or later, so a rewrite of the same
        // size in that second leaves the stat git recorded. git compares such a file's
        // contents; the resume hashes it. Whole seconds, as a git built without `USE_NSEC`
        // compares them.
        if stat.mtime.0 >= index_written {
            racy += 1;
            continue;
        }
        if let (Some(path), Some(blob)) = (project_path(&prefix, path), sha1_id(object)) {
            if !checked.contains(&path) || converts.contains(&path) {
                converted += 1;
                continue;
            }
            clean.insert(
                path,
                IndexEntry {
                    blob,
                    stat: IndexStat {
                        executable: mode == "100755",
                        ..stat
                    },
                },
            );
        }
    }

    tracing::info!(
        "Resume: git vouches for {} files whose stat is unchanged; hashes {} an attribute may \
         convert ({}), {} racily clean and {} git does not look at",
        clean.len(),
        converted,
        CONVERSION_ATTRIBUTES.join(", "),
        racy,
        hidden,
    );
    Ok(VcsDelta {
        clean,
        usable: true,
    })
}

/// What a write of the index changes, compared before and after git reads it.
#[derive(Debug, PartialEq, Eq)]
struct IndexStamp {
    modified: std::time::SystemTime,
    size: u64,
    /// Device, inode and ctime, where the platform has them.
    #[cfg(unix)]
    identity: (u64, u64, i64, i64),
}

impl IndexStamp {
    fn of(path: &std::path::Path) -> buck2_error::Result<Self> {
        let meta = std::fs::metadata(path)
            .map_err(|e| buck2_error!(ErrorTag::Input, "the index cannot be statted: {e}"))?;
        Ok(Self {
            modified: meta
                .modified()
                .map_err(|e| buck2_error!(ErrorTag::Input, "the index has no mtime: {e}"))?,
            size: meta.len(),
            #[cfg(unix)]
            identity: {
                use std::os::unix::fs::MetadataExt;
                (meta.dev(), meta.ino(), meta.ctime(), meta.ctime_nsec())
            },
        })
    }
}

/// The tracked files under the project, and those of them whose checkout an attribute may
/// convert, as paths from the project.
///
/// A file whose stat moved since the index recorded it is hashed by the resume, so no git
/// command rehashes it. A file whose stat did not move is clean to git; that leaves the
/// conversions, which let git call a file clean whose bytes are not the blob's. Two sets of
/// attributes matter. git checks a file in with the working tree's `.gitattributes` over the
/// index's, which `check-attr` reads by default. It checked the file out, when it recorded the
/// stat, with the index's over the working tree's, which `check-attr --cached` reads; the index
/// must hold HEAD's `.gitattributes` for that, see [`resume_delta`]. A `.gitattributes` edited,
/// emptied or added in the working tree after the checkout changes only the first, so a file a
/// smudge filter converted on checkout would otherwise pass as its blob. Attributes of the
/// suspended commit do not matter: the suspend kept a blob id only for bytes that hash to it,
/// and the index's blob is compared with it directly.
fn conversions(
    root: &AbsNormPath,
) -> buck2_error::Result<(
    HashSet<ProjectRelativePathBuf>,
    HashSet<ProjectRelativePathBuf>,
)> {
    // Without `--full-name`, paths are relative to the project, as `check-attr` reads them.
    let paths = run_git_blocking(root, &["ls-files", "-z"], None)?;
    let check = |cached: bool| {
        let mut args = vec!["check-attr", "-z", "--stdin"];
        if cached {
            args.push("--cached");
        }
        args.extend_from_slice(CONVERSION_ATTRIBUTES);
        run_git_blocking(root, &args, Some(paths.clone()))
    };
    let (working_tree, index) = std::thread::scope(|scope| {
        let index = scope.spawn(|| check(true));
        (check(false), index.join())
    });
    let index =
        index.map_err(|_| buck2_error!(ErrorTag::Tier0, "the attribute check panicked"))?;
    let checked = records(&paths)
        .filter_map(|path| project_path("", path))
        .collect();
    let mut converts = HashSet::new();
    for attributes in [working_tree?, index?] {
        let fields: Vec<&str> = records(&attributes).collect();
        converts.extend(fields.chunks(3).filter_map(|triple| match triple {
            [path, _attribute, value] if *value != "unspecified" && *value != "unset" => {
                project_path("", path)
            }
            _ => None,
        }));
    }
    Ok((checked, converts))
}

fn toplevel_to_project(root: &AbsNormPath) -> buck2_error::Result<String> {
    Ok(text(run_git_blocking(
        root,
        &["rev-parse", "--show-prefix"],
        None,
    )?)?
    .trim_end_matches('\n')
    .to_owned())
}

fn text(bytes: Vec<u8>) -> buck2_error::Result<String> {
    String::from_utf8(bytes)
        .map_err(|e| buck2_error!(ErrorTag::Input, "git printed a path that is not utf8: {e}"))
}

/// The NUL-terminated records of a `-z` output. Paths buck2 cannot name are not utf8; they are
/// skipped, and a file buck2 cannot name is in no snapshot either.
fn records(output: &[u8]) -> impl Iterator<Item = &str> {
    output
        .split(|b| *b == 0)
        .filter(|record| !record.is_empty())
        .filter_map(|record| std::str::from_utf8(record).ok())
}

/// The records of `git ls-files -z --debug`: each is a NUL-terminated `-s` record followed by
/// five newline-terminated lines of the stat the index keeps,
/// `  ctime: S:N`, `  mtime: S:N`, `  dev: D<TAB>ino: I`, `  uid: U<TAB>gid: G`,
/// `  size: Z<TAB>flags: F`. A record whose path is not utf8 is skipped.
fn index_records(output: &[u8]) -> buck2_error::Result<Vec<(&str, IndexStat)>> {
    fn malformed() -> buck2_error::Error {
        buck2_error!(
            ErrorTag::Input,
            "`git ls-files --debug` printed a malformed record"
        )
    }
    /// `<key>: <number>` at the start of `field`, and what follows the number.
    fn number<'a>(field: &'a [u8], key: &[u8]) -> Option<(u32, &'a [u8])> {
        let rest = field.strip_prefix(key)?.strip_prefix(b": ")?;
        let end = rest
            .iter()
            .position(|b| !b.is_ascii_digit())
            .unwrap_or(rest.len());
        let value = std::str::from_utf8(&rest[..end]).ok()?.parse().ok()?;
        Some((value, &rest[end..]))
    }
    fn time(line: &[u8], key: &[u8]) -> Option<(u32, u32)> {
        let (sec, rest) = number(line, key)?;
        let rest = rest.strip_prefix(b":")?;
        let nsec = std::str::from_utf8(rest).ok()?.parse().ok()?;
        Some((sec, nsec))
    }
    fn pair(line: &[u8], first: &[u8], second: &[u8]) -> Option<(u32, u32)> {
        let (a, rest) = number(line, first)?;
        let (b, _) = number(rest.strip_prefix(b"\t")?, second)?;
        Some((a, b))
    }
    let mut out = Vec::new();
    let mut rest = output;
    while !rest.is_empty() {
        let end = rest.iter().position(|b| *b == 0).ok_or_else(malformed)?;
        let record = std::str::from_utf8(&rest[..end]).ok();
        rest = &rest[end + 1..];
        let mut lines = [&b""[..]; 5];
        for line in &mut lines {
            let end = rest
                .iter()
                .position(|b| *b == b'\n')
                .ok_or_else(malformed)?;
            *line = rest[..end].trim_ascii_start();
            rest = &rest[end + 1..];
        }
        let parsed = (|| {
            let (dev, ino) = pair(lines[2], b"dev", b"ino")?;
            // The flags that follow are hexadecimal, and not needed.
            let (size, _) = number(lines[4], b"size")?;
            Some(IndexStat {
                ctime: time(lines[0], b"ctime")?,
                mtime: time(lines[1], b"mtime")?,
                dev,
                ino,
                size,
                executable: false,
            })
        })();
        let stat = parsed.ok_or_else(malformed)?;
        if let Some(record) = record {
            out.push((record, stat));
        }
    }
    Ok(out)
}

/// A path relative to the repository's top level, relative to the project instead, or `None`
/// outside the project.
fn project_path(prefix: &str, toplevel_path: &str) -> Option<ProjectRelativePathBuf> {
    let rest = toplevel_path.strip_prefix(prefix)?;
    ProjectRelativePath::new(rest)
        .ok()
        .map(|path| path.to_buf())
}

fn sha1_id(hex: &str) -> Option<[u8; 20]> {
    if hex.len() != 40 {
        return None;
    }
    let mut id = [0u8; 20];
    for (i, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(hex.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(id)
}

/// T0 test 4 of the suspend-b test plan: a real git repository, suspended by the crawler and
/// resumed by [`resume_scan`] with what git says.
#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::Path;
    use std::path::PathBuf;
    use std::time::Duration;
    use std::time::SystemTime;

    use buck2_common::ignores::ignore_set::IgnoreSet;
    use buck2_core::cells::CellResolver;
    use buck2_core::cells::cell_root_path::CellRootPathBuf;
    use buck2_core::cells::name::CellName;
    use buck2_core::fs::project::ProjectRoot;
    use buck2_data::FileWatcherEventType;
    use buck2_file_watcher::file_watcher::FileWatcher;
    use buck2_file_watcher::file_watcher::FileWatcherImage;
    use buck2_file_watcher::file_watcher::FsHashCrawler;
    use buck2_file_watcher::file_watcher::ResumeInput;
    use buck2_file_watcher::file_watcher::ResumeScan;
    use buck2_file_watcher::file_watcher::VcsDelta;
    use buck2_file_watcher::file_watcher::resume_scan;
    use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;
    use buck2_hash::StdBuckHashMap;
    use dupe::Dupe;

    use super::*;

    const M: FileWatcherEventType = FileWatcherEventType::Modify;
    const C: FileWatcherEventType = FileWatcherEventType::Create;
    const D: FileWatcherEventType = FileWatcherEventType::Delete;

    type Events = BTreeSet<(String, FileWatcherEventType)>;

    /// A repository at `<tempdir>/repo`, a path a fresh clone can take over.
    struct Repo {
        tempdir: tempfile::TempDir,
        root: ProjectRoot,
        cells: CellResolver,
        ignores: StdBuckHashMap<CellName, IgnoreSet>,
    }

    impl Repo {
        fn new() -> Self {
            let tempdir = tempfile::tempdir().unwrap();
            let dir = std::fs::canonicalize(tempdir.path()).unwrap().join("repo");
            std::fs::create_dir(&dir).unwrap();
            let root = ProjectRoot::new(AbsNormPathBuf::new(dir).unwrap()).unwrap();
            let repo = Self {
                tempdir,
                root,
                cells: CellResolver::testing_with_name_and_path(
                    CellName::testing_new("root"),
                    CellRootPathBuf::testing_new(""),
                ),
                ignores: StdBuckHashMap::default(),
            };
            repo.git(&["init", "-q", "-b", "main"]);
            repo
        }

        fn dir(&self) -> PathBuf {
            self.root.root().as_path().to_path_buf()
        }

        fn git(&self, args: &[&str]) -> String {
            let mut full = vec![
                "-c",
                "user.name=Buck Test",
                "-c",
                "user.email=buck-test@example.com",
                "-c",
                "commit.gpgsign=false",
            ];
            full.extend_from_slice(args);
            String::from_utf8(run_git_blocking(self.root.root(), &full, None).unwrap()).unwrap()
        }

        fn commit(&self, message: &str) -> String {
            self.git(&["add", "-A"]);
            self.git(&["commit", "-q", "-m", message]);
            self.git(&["rev-parse", "HEAD"]).trim().to_owned()
        }

        /// Writes a file an hour old, so no snapshot taken now finds it racily clean.
        fn write(&self, path: &str, contents: &str) {
            let path = self.dir().join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, contents).unwrap();
            age(&path);
        }

        /// Ages every file of the working tree, as a checkout made long before a build.
        fn age_all(&self) {
            fn walk(dir: &Path) {
                for entry in std::fs::read_dir(dir).unwrap() {
                    let entry = entry.unwrap();
                    if entry.file_name() == ".git" {
                        continue;
                    }
                    let kind = entry.file_type().unwrap();
                    if kind.is_dir() {
                        walk(&entry.path());
                    } else if kind.is_file() {
                        age(&entry.path());
                    }
                }
            }
            walk(&self.dir());
        }

        /// Moves the repository aside and clones it, `args` given, into the same path: a
        /// fresh runner's checkout, where every file has a new inode. The checkout is aged and
        /// the index refreshed, as one made long before the resume, so no entry is racily
        /// clean; [`Self::reclone_raw`] leaves it as the clone wrote it.
        fn reclone(&self, args: &[&str]) {
            self.reclone_raw(args);
            self.age_all();
            self.git(&["update-index", "-q", "--refresh"]);
        }

        fn reclone_raw(&self, args: &[&str]) {
            let aside = self.tempdir.path().join("aside");
            drop(std::fs::remove_dir_all(&aside));
            std::fs::rename(self.dir(), &aside).unwrap();
            let url = format!(
                "file://{}",
                std::fs::canonicalize(&aside).unwrap().display()
            );
            let mut command = std::process::Command::new("git");
            command
                .arg("clone")
                .arg("-q")
                .args(args)
                .arg(&url)
                .arg(self.dir());
            assert!(command.status().unwrap().success());
        }

        fn crawler(&self) -> FsHashCrawler {
            FsHashCrawler::new(&self.root, self.cells.dupe(), self.ignores.clone()).unwrap()
        }

        async fn suspend(&self, crawler: &FsHashCrawler) -> FileWatcherImage {
            let root = self.root.root();
            crawler
                .suspend_image(&self.root, Box::new(move || suspend_tree(root)))
                .await
                .unwrap()
        }

        fn scan(
            &self,
            image: &FileWatcherImage,
            vcs: impl FnOnce() -> Option<VcsDelta>,
        ) -> ResumeScan {
            resume_scan(
                &self.root,
                &self.cells,
                &self.ignores,
                ResumeInput {
                    image,
                    cross_host: false,
                    vcs: Box::new(vcs),
                },
            )
            .unwrap()
        }

        fn scan_git(&self, image: &FileWatcherImage) -> ResumeScan {
            let head = image.vcs.clone().expect("the suspend recorded a commit");
            self.scan(image, || Some(resume_delta(self.root.root(), &head)))
        }

        fn delta(&self, image: &FileWatcherImage) -> VcsDelta {
            resume_delta(self.root.root(), image.vcs.as_ref().unwrap())
        }
    }

    fn age(path: &Path) {
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(3600))
            .unwrap();
    }

    fn events(scan: &ResumeScan) -> Events {
        scan.events
            .iter()
            .map(|e| (e.cell_path.path().to_string(), e.event))
            .collect()
    }

    fn set(items: &[(&str, FileWatcherEventType)]) -> Events {
        items.iter().map(|(p, e)| ((*p).to_owned(), *e)).collect()
    }

    fn lines(output: &str) -> BTreeSet<String> {
        output
            .split(['\n', '\0'])
            .filter(|l| !l.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    }

    /// Commit B on a fresh clone, a dirty tracked file and an untracked one: exactly
    /// `git diff --name-only A B` and the dirty files, and git vouches for the rest.
    #[tokio::test(flavor = "multi_thread")]
    async fn reports_exactly_the_diff_and_the_dirty_files() {
        let repo = Repo::new();
        for name in ["keep1", "keep2", "keep3", "d/keep4", "d/keep5"] {
            repo.write(name, name);
        }
        repo.write("edit", "before");
        repo.write("del", "gone");
        repo.write("d/ren_old", "moved");
        repo.write("dirty", "clean");
        let a = repo.commit("A");
        repo.age_all();
        let image = repo.suspend(&repo.crawler()).await;
        assert_eq!(image.vcs.as_ref().unwrap().head, a);

        repo.reclone(&[]);
        // Not aged: written in the second the reclone refreshed the index, an aged rewrite of
        // the same size would look unchanged to a git that compares whole seconds.
        std::fs::write(repo.dir().join("edit"), "after!").unwrap();
        repo.write("added", "new");
        std::fs::remove_file(repo.dir().join("del")).unwrap();
        std::fs::rename(repo.dir().join("d/ren_old"), repo.dir().join("d/ren_new")).unwrap();
        let b = repo.commit("B");
        repo.write("dirty", "DIRTY");
        repo.write("untracked", "u");

        let scan = repo.scan_git(&image);
        let mut expected = lines(&repo.git(&["diff", "--name-only", "--no-renames", &a, &b]));
        expected.extend(["dirty".to_owned(), "untracked".to_owned()]);
        let reported: BTreeSet<String> = events(&scan).into_iter().map(|(p, _)| p).collect();
        assert_eq!(reported, expected);
        assert_eq!(
            events(&scan),
            set(&[
                ("added", C),
                ("d/ren_new", C),
                ("d/ren_old", D),
                ("del", D),
                ("dirty", M),
                ("edit", M),
                ("untracked", C),
            ])
        );
        assert_eq!((scan.vouched, scan.hashed), (5, 2));
    }

    /// An edit written back to the suspended bytes, with a new modification time, is no change,
    /// with git and without. Its stat no longer matches the index, so git names it and the hash
    /// finds the suspended bytes.
    #[tokio::test(flavor = "multi_thread")]
    async fn aba_after_suspend_is_no_change() {
        let repo = Repo::new();
        repo.write("f", "one");
        repo.write("g", "two");
        repo.commit("A");
        repo.age_all();
        let image = repo.suspend(&repo.crawler()).await;
        std::fs::write(repo.dir().join("f"), "ONE").unwrap();
        std::fs::write(repo.dir().join("f"), "one").unwrap();

        let scan = repo.scan_git(&image);
        assert_eq!(events(&scan), Events::new());
        assert_eq!((scan.stat_equal, scan.vouched, scan.hashed), (1, 0, 1));
        let scan = repo.scan(&image, || None);
        assert_eq!(events(&scan), Events::new());
        assert_eq!((scan.stat_equal, scan.hashed), (1, 1));
    }

    /// A file dirty when the crawler's snapshot was taken and reverted before the suspend read
    /// it: the graph may hold the dirty bytes, so it is reported although git calls it clean.
    #[tokio::test(flavor = "multi_thread")]
    async fn aba_at_suspend_dirty_at_snapshot() {
        let repo = Repo::new();
        repo.write("f", "committed");
        repo.write("g", "other");
        repo.commit("A");
        repo.age_all();
        repo.write("f", "dirty!!!!");
        let crawler = repo.crawler();
        repo.write("f", "committed");
        let image = repo.suspend(&crawler).await;
        repo.reclone(&[]);
        assert_eq!(events(&repo.scan_git(&image)), set(&[("f", M)]));
    }

    /// The crawler's case: a sync, then an edit a build read without a sync, reverted before
    /// the suspend. The snapshot's stat no longer matches, so the file has no digest.
    #[tokio::test(flavor = "multi_thread")]
    async fn aba_at_suspend_edit_after_last_sync() {
        let repo = Repo::new();
        repo.write("f", "committed");
        repo.write("g", "other");
        repo.commit("A");
        repo.age_all();
        let crawler = repo.crawler();
        std::thread::sleep(Duration::from_millis(20));
        repo.write("f", "edited!!!");
        std::thread::sleep(Duration::from_millis(20));
        repo.write("f", "committed");
        let image = repo.suspend(&crawler).await;
        repo.reclone(&[]);
        let scan = repo.scan_git(&image);
        assert_eq!(events(&scan), set(&[("f", M)]));
        assert_eq!(scan.vouched, 1);
    }

    /// An edit after the files were statted and before git compares them is dirty to git; one
    /// after moves the stat the watcher's next sync compares with.
    #[tokio::test(flavor = "multi_thread")]
    async fn edits_racing_git() {
        let repo = Repo::new();
        repo.write("f", "aaaa");
        repo.write("g", "gggg");
        repo.commit("A");
        repo.age_all();
        let image = repo.suspend(&repo.crawler()).await;
        repo.reclone(&[]);
        // A checkout made long before: only the edits below are racily clean afterwards.
        repo.age_all();
        let head = image.vcs.clone().unwrap();
        let f = repo.dir().join("f");

        let scan = repo.scan(&image, || {
            std::fs::write(&f, "bbbb").unwrap();
            Some(resume_delta(repo.root.root(), &head))
        });
        assert_eq!(events(&scan), set(&[("f", M)]));

        // Two hours old, a time no earlier write gave it, so git sees the stat move and records
        // the new one; the index is written now, so the entry is not racily clean.
        std::fs::write(&f, "aaaa").unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&f)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(7200))
            .unwrap();
        repo.git(&["update-index", "-q", "--refresh"]);
        let scan = repo.scan(&image, || {
            let delta = resume_delta(repo.root.root(), &head);
            std::thread::sleep(Duration::from_millis(20));
            std::fs::write(&f, "cccc").unwrap();
            Some(delta)
        });
        assert_eq!(events(&scan), Events::new());
        let later: Events = scan
            .testing_changed_since(&repo.root, &repo.cells, &repo.ignores)
            .unwrap()
            .into_iter()
            .map(|e| (e.cell_path.path().to_string(), e.event))
            .collect();
        assert_eq!(later, set(&[("f", M)]));
    }

    /// A fresh clone at the suspended commit: nothing changed, and no file is read.
    #[tokio::test(flavor = "multi_thread")]
    async fn fresh_clone_at_the_same_commit_reads_nothing() {
        let repo = Repo::new();
        for name in ["a", "b", "c/d", "c/e/f"] {
            repo.write(name, name);
        }
        repo.commit("A");
        repo.age_all();
        let image = repo.suspend(&repo.crawler()).await;
        repo.reclone(&[]);
        let scan = repo.scan_git(&image);
        assert_eq!(events(&scan), Events::new());
        assert_eq!((scan.stat_equal, scan.vouched, scan.hashed), (0, 4, 0));
    }

    /// A shallow clone that lacks the suspended commit, as a CI checkout of the PR's commit at
    /// depth 1: the index's blob ids are compared with the suspended ones, so git still vouches
    /// for the unchanged file, and the changed one is hashed and reported.
    #[tokio::test(flavor = "multi_thread")]
    async fn shallow_clone_without_the_suspended_commit_still_uses_git() {
        let repo = Repo::new();
        repo.write("a", "a");
        repo.write("b", "b");
        let a = repo.commit("A");
        repo.age_all();
        let image = repo.suspend(&repo.crawler()).await;
        repo.write("b", "B!");
        repo.commit("B");
        repo.reclone(&["--depth", "1"]);
        let commit = format!("{a}^{{commit}}");
        assert!(run_git_blocking(repo.root.root(), &["cat-file", "-e", &commit], None).is_err());

        assert!(repo.delta(&image).usable);
        let with_git = repo.scan_git(&image);
        let without = repo.scan(&image, || None);
        assert_eq!(events(&with_git), set(&[("b", M)]));
        assert_eq!(events(&with_git), events(&without));
        // `b`'s size moved, which is a change before any hash.
        assert_eq!((with_git.vouched, with_git.hashed), (1, 0));
        assert_eq!((without.vouched, without.hashed), (0, 1));
    }

    /// Settings that convert every checkout make git unusable. A file git does not look at, or
    /// one an attribute may convert, is left to the hash, and git vouches for the rest.
    #[tokio::test(flavor = "multi_thread")]
    async fn conversions_and_hidden_files_are_hashed() {
        let repo = Repo::new();
        repo.write("a.txt", "a");
        repo.write("b", "b");
        repo.commit("A");
        let image = repo.suspend(&repo.crawler()).await;
        let clean = |repo: &Repo| -> Option<BTreeSet<String>> {
            let delta = repo.delta(&image);
            delta
                .usable
                .then(|| delta.clean.keys().map(|p| p.to_string()).collect())
        };
        assert_eq!(clean(&repo), Some(lines("a.txt\nb")));

        repo.git(&["config", "core.autocrlf", "true"]);
        assert_eq!(clean(&repo), None);
        repo.git(&["config", "core.autocrlf", "false"]);
        assert_eq!(clean(&repo), Some(lines("a.txt\nb")));

        repo.git(&["config", "core.eol", "crlf"]);
        assert_eq!(clean(&repo), None);
        repo.git(&["config", "--unset", "core.eol"]);

        repo.git(&["update-index", "--skip-worktree", "b"]);
        assert_eq!(clean(&repo), Some(lines("a.txt")));
        repo.git(&["update-index", "--no-skip-worktree", "b"]);

        repo.git(&["update-index", "--assume-unchanged", "b"]);
        assert_eq!(clean(&repo), Some(lines("a.txt")));
        repo.git(&["update-index", "--no-assume-unchanged", "b"]);
        assert_eq!(clean(&repo), Some(lines("a.txt\nb")));

        // Uncommitted, in the working tree only: git applies it all the same. The
        // `.gitattributes` file itself is untracked, then committed and clean.
        for attributes in [
            "*.txt filter=fake",
            "*.txt eol=lf",
            "*.txt text=auto",
            "*.txt ident",
        ] {
            repo.write(".gitattributes", &format!("{attributes}\n"));
            assert_eq!(clean(&repo), Some(lines("b")), "{attributes}");
        }
        repo.commit("attributes");
        assert_eq!(clean(&repo), Some(lines(".gitattributes\nb")));
    }

    /// Lab's tree: one vendored directory's `.gitattributes` sets `eol=lf`. Only the files it
    /// matches are hashed; git vouches for every other file.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_vendored_gitattributes_hashes_only_its_own_files() {
        let repo = Repo::new();
        repo.write("vendor/winio/.gitattributes", "* text=auto eol=lf\n");
        repo.write("vendor/winio/pipe.go", "package winio\n");
        repo.write("vendor/winio/sub/file.go", "package sub\n");
        repo.write("vendor/other/x.go", "package other\n");
        repo.write("src/main.rs", "fn main() {}\n");
        repo.write("README", "readme\n");
        repo.commit("A");
        repo.age_all();
        let image = repo.suspend(&repo.crawler()).await;
        repo.reclone(&[]);
        let scan = repo.scan_git(&image);
        assert_eq!(events(&scan), Events::new());
        assert_eq!((scan.vouched, scan.hashed), (3, 3));
    }

    /// A clean filter that maps different bytes of the same size to the blob: once the index has
    /// been refreshed, git calls the edited file clean, so git must not vouch for it, and the
    /// hash reports it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_file_a_filter_makes_clean_to_git_is_reported() {
        let repo = Repo::new();
        repo.write(".gitattributes", "*.txt filter=lower\n");
        repo.write("f.txt", "abc\n");
        repo.write("g", "g");
        repo.commit("A");
        repo.age_all();
        let image = repo.suspend(&repo.crawler()).await;
        repo.reclone(&[]);
        repo.git(&["config", "filter.lower.clean", "tr A-Z a-z"]);
        let f = repo.dir().join("f.txt");
        std::fs::write(&f, "ABC\n").unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&f)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(7200))
            .unwrap();
        // A refresh hashes the edit through the filter, finds the blob, and records the new stat.
        repo.git(&["update-index", "-q", "--refresh"]);
        assert_eq!(
            repo.git(&["diff-files", "--name-only"]),
            "",
            "git calls the edit clean"
        );
        let scan = repo.scan_git(&image);
        assert_eq!(events(&scan), set(&[("f.txt", M)]));
        // `g` and `.gitattributes`.
        assert_eq!((scan.vouched, scan.hashed), (2, 1));
    }

    /// The resume compares the index's blob with the suspended one: a commit that changes a file
    /// to a blob of the same size is no file git vouches for.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_same_size_change_in_a_new_commit_is_reported() {
        let repo = Repo::new();
        repo.write("f", "aaaa");
        repo.write("g", "gggg");
        repo.commit("A");
        repo.age_all();
        let image = repo.suspend(&repo.crawler()).await;
        // Not aged: the index's stat for `f` is an hour old, so `git add` sees the edit.
        std::fs::write(repo.dir().join("f"), "bbbb").unwrap();
        repo.commit("B");
        repo.reclone(&[]);
        let delta = repo.delta(&image);
        assert!(delta.usable);
        let f = ProjectRelativePath::new("f").unwrap().to_buf();
        assert!(delta.clean.contains_key(&f), "the index tracks f at B");
        let scan = repo.scan_git(&image);
        assert_eq!(events(&scan), set(&[("f", M)]));
        assert_eq!((scan.vouched, scan.hashed), (1, 1));
    }

    /// Outside a git repository, or with a broken index, git vouches for nothing and the scan is
    /// the one without git.
    #[tokio::test(flavor = "multi_thread")]
    async fn git_errors_make_git_unusable() {
        let repo = Repo::new();
        repo.write("a", "a");
        repo.write("b", "b");
        repo.commit("A");
        repo.age_all();
        let image = repo.suspend(&repo.crawler()).await;
        repo.reclone(&[]);
        repo.write("b", "B");

        std::fs::write(repo.dir().join(".git/index"), "not an index").unwrap();
        assert!(!repo.delta(&image).usable);
        let broken = repo.scan_git(&image);
        assert_eq!(events(&broken), set(&[("b", M)]));
        assert_eq!((broken.vouched, broken.hashed), (0, 2));

        std::fs::remove_dir_all(repo.dir().join(".git")).unwrap();
        assert!(!repo.delta(&image).usable);
        let none = repo.scan_git(&image);
        assert_eq!(events(&none), set(&[("b", M)]));
        assert_eq!((none.vouched, none.hashed), (0, 2));
    }

    /// A same-size rewrite in place that puts the modification time back leaves only the change
    /// time moved. A repository that tells git not to trust it (`core.trustctime=false`, or
    /// `core.checkStat=minimal`) would call the file clean; the resume reports it all the same.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_rewrite_with_the_old_mtime_is_a_change_whatever_git_trusts() {
        for setting in [("core.trustctime", "false"), ("core.checkStat", "minimal")] {
            let repo = Repo::new();
            repo.write("f", "aaaa");
            repo.write("g", "gggg");
            repo.commit("A");
            repo.age_all();
            let image = repo.suspend(&repo.crawler()).await;
            repo.reclone(&[]);
            repo.age_all();
            // The index records the aged stat, so only the rewrite below can move it.
            repo.git(&["update-index", "-q", "--refresh"]);
            repo.git(&["config", setting.0, setting.1]);
            let f = repo.dir().join("f");
            let modified = std::fs::metadata(&f).unwrap().modified().unwrap();
            // git compares whole seconds of the change time.
            std::thread::sleep(Duration::from_millis(1100));
            std::fs::write(&f, "bbbb").unwrap();
            std::fs::OpenOptions::new()
                .write(true)
                .open(&f)
                .unwrap()
                .set_modified(modified)
                .unwrap();
            let scan = repo.scan_git(&image);
            assert_eq!(events(&scan), set(&[("f", M)]), "{setting:?}");
        }
    }

    /// An attribute only the suspended commit set does not stop git: the suspend kept a blob id
    /// only for bytes that hash to it, and the index's blob is compared with it directly.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_attribute_of_the_suspended_commit_alone_does_not_stop_git() {
        let repo = Repo::new();
        repo.write("a.txt", "a");
        repo.write(".gitattributes", "*.txt filter=fake\n");
        repo.commit("A");
        repo.age_all();
        let image = repo.suspend(&repo.crawler()).await;
        std::fs::remove_file(repo.dir().join(".gitattributes")).unwrap();
        repo.commit("B");
        repo.reclone(&[]);
        let scan = repo.scan_git(&image);
        assert_eq!(events(&scan), set(&[(".gitattributes", D)]));
        assert_eq!((scan.vouched, scan.hashed), (1, 0));
    }

    /// A fresh clone's files written no earlier than its index are racily clean: git vouches
    /// for none of them, whatever their stat, and the resume hashes them.
    #[tokio::test(flavor = "multi_thread")]
    async fn racily_clean_entries_are_hashed() {
        let repo = Repo::new();
        for name in ["a", "b", "c/d"] {
            repo.write(name, name);
        }
        repo.commit("A");
        repo.age_all();
        let image = repo.suspend(&repo.crawler()).await;
        repo.reclone_raw(&[]);
        // The index's time put back past every file's: each entry is written no earlier.
        let index = repo.dir().join(".git/index");
        std::fs::OpenOptions::new()
            .write(true)
            .open(&index)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(7200))
            .unwrap();
        let delta = repo.delta(&image);
        assert!(delta.usable);
        assert_eq!(delta.clean.len(), 0);
        let scan = repo.scan_git(&image);
        assert_eq!(events(&scan), Events::new());
        assert_eq!((scan.vouched, scan.hashed), (0, 3));

        // Files an hour old, recorded by an index written now: the same entries are clean.
        repo.age_all();
        repo.git(&["update-index", "-q", "--refresh"]);
        let scan = repo.scan_git(&image);
        assert_eq!((scan.vouched, scan.hashed), (3, 0));
    }

    /// The racy rule reads the index's time before git lists the index; an index written in
    /// between (a `git add` beside the resume, an index put back with an older time, or one
    /// replaced by a copy of itself) leaves the listing describing another index than the one
    /// statted, so git vouches for nothing and the resume hashes every file.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_index_written_while_git_reads_it_makes_git_unusable() {
        let repo = Repo::new();
        for name in ["a", "b", "c/d"] {
            repo.write(name, name);
        }
        repo.commit("A");
        repo.age_all();
        let image = repo.suspend(&repo.crawler()).await;
        repo.reclone(&[]);
        let head = image.vcs.clone().unwrap();
        let index = repo.dir().join(".git/index");

        let untouched = resume_delta_racing(repo.root.root(), &head, || {});
        assert!(untouched.usable);
        assert_eq!(untouched.clean.len(), 3);

        let added = resume_delta_racing(repo.root.root(), &head, || {
            repo.write("e", "e");
            repo.git(&["add", "e"]);
        });
        assert!(!added.usable, "an index written by `git add` was used");
        assert!(added.clean.is_empty());

        let older = resume_delta_racing(repo.root.root(), &head, || {
            std::fs::OpenOptions::new()
                .write(true)
                .open(&index)
                .unwrap()
                .set_modified(SystemTime::now() - Duration::from_secs(7200))
                .unwrap();
        });
        assert!(!older.usable, "an index whose time went back was used");

        let replaced = resume_delta_racing(repo.root.root(), &head, || {
            let copy = repo.dir().join(".git/index.copy");
            std::fs::copy(&index, &copy).unwrap();
            let modified = std::fs::metadata(&index).unwrap().modified().unwrap();
            std::fs::OpenOptions::new()
                .write(true)
                .open(&copy)
                .unwrap()
                .set_modified(modified)
                .unwrap();
            std::fs::rename(&copy, &index).unwrap();
        });
        assert!(
            !replaced.usable,
            "an index replaced by a copy with the same time and size was used"
        );

        // Nothing writing the index: git's help is back.
        let settled = resume_delta_racing(repo.root.root(), &head, || {});
        assert!(settled.usable);
    }

    /// A clone whose checkout a smudge filter converted (to upper case, the same size), its
    /// `.gitattributes` then emptied in the working tree or emptied and staged: the file's stat
    /// is the index's and no attribute applies in the working tree, but its bytes are not the
    /// blob's, so it must be hashed. Without the index's attributes git vouched for it; without
    /// the staged check, for the staged case.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_conversion_the_checkout_applied_is_hashed_after_its_attribute_goes() {
        for staged in [false, true] {
            let repo = Repo::new();
            repo.write("f.txt", "abc\ndef\n");
            repo.write("g", "g");
            repo.commit("A");
            repo.age_all();
            let image = repo.suspend(&repo.crawler()).await;
            repo.write(".gitattributes", "*.txt filter=upper\n");
            repo.commit("C");
            repo.reclone(&[
                "-c",
                "filter.upper.smudge=tr a-z A-Z",
                "-c",
                "filter.upper.clean=tr A-Z a-z",
            ]);
            assert_eq!(
                std::fs::read(repo.dir().join("f.txt")).unwrap(),
                b"ABC\nDEF\n"
            );
            std::fs::write(repo.dir().join(".gitattributes"), "# none\n").unwrap();
            if staged {
                repo.git(&["add", ".gitattributes"]);
                assert!(!repo.delta(&image).usable, "staged");
            }
            let without = repo.scan(&image, || None);
            assert!(
                events(&without).contains(&("f.txt".to_owned(), M)),
                "staged {staged}"
            );
            let with_git = repo.scan_git(&image);
            assert_eq!(events(&with_git), events(&without), "staged {staged}");
        }
    }

    /// A split index keeps entries in a file written at another time than the index, so git
    /// vouches for nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_split_index_makes_git_unusable() {
        let repo = Repo::new();
        repo.write("a", "a");
        repo.commit("A");
        let image = repo.suspend(&repo.crawler()).await;
        assert!(repo.delta(&image).usable);
        repo.git(&["update-index", "--split-index"]);
        assert!(!repo.delta(&image).usable);
    }

    /// A project below the repository's top level: paths are relative to the project, files
    /// outside it are left out, and a resume whose project sits elsewhere in the repository
    /// does not use git.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_project_below_the_top_level_names_paths_from_the_project() {
        let repo = Repo::new();
        repo.write("outside", "o");
        repo.write("sub/inside", "i");
        repo.write("sub/d/f", "f");
        repo.commit("A");
        let sub = ProjectRoot::new(AbsNormPathBuf::new(repo.dir().join("sub")).unwrap()).unwrap();

        let tree = suspend_tree(sub.root()).unwrap();
        assert_eq!(tree.image.toplevel_to_project, "sub/");
        let blobs: BTreeSet<String> = tree.blobs.keys().map(|p| p.to_string()).collect();
        assert_eq!(blobs, lines("inside\nd/f"));

        repo.write("sub/inside", "edited");
        repo.write("outside", "edited");
        let delta = resume_delta(sub.root(), &tree.image);
        assert!(delta.usable);
        // `inside`'s new stat is the scan's to compare; git holds the entry, not the edit.
        let clean: BTreeSet<String> = delta.clean.keys().map(|p| p.to_string()).collect();
        assert_eq!(clean, lines("inside\nd/f"));

        let at_top = VcsImage {
            toplevel_to_project: String::new(),
            ..tree.image
        };
        assert!(!resume_delta(sub.root(), &at_top).usable);
    }
}
