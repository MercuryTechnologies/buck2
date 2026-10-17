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

use std::collections::HashMap;
use std::collections::HashSet;

use buck2_core::fs::project_rel_path::ProjectRelativePath;
use buck2_core::fs::project_rel_path::ProjectRelativePathBuf;
use buck2_error::ErrorTag;
use buck2_error::buck2_error;
use buck2_file_watcher::file_watcher::VcsDelta;
use buck2_file_watcher::file_watcher::VcsImage;
use buck2_file_watcher::file_watcher::VcsTree;
use buck2_fs::paths::abs_norm_path::AbsNormPath;

use crate::git::run_git_blocking;

/// The attributes that let a checked-out file's bytes differ from its blob's.
const CONVERSION_ATTRIBUTES: &[&str] = &["filter", "eol", "ident", "working-tree-encoding"];

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

/// What git says changed since `image`'s commit, called by a resuming watcher after it has
/// statted the files. Not usable, so that git vouches for nothing, when any git call fails or
/// when anything lets git call a file clean whose bytes are not its blob's.
pub(crate) fn resume_delta(root: &AbsNormPath, image: &VcsImage) -> VcsDelta {
    match resume_delta_inner(root, image) {
        Ok(delta) => delta,
        Err(e) => {
            tracing::info!("Resume does not use git, and hashes instead: {e:#}");
            VcsDelta::default()
        }
    }
}

fn resume_delta_inner(root: &AbsNormPath, image: &VcsImage) -> buck2_error::Result<VcsDelta> {
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

    let listing = run_git_blocking(root, &["ls-files", "-z", "-v", "--full-name"], None)?;
    let mut tracked = HashSet::new();
    let mut paths_input = Vec::new();
    for record in records(&listing) {
        // `<tag> SP <path>`; a lowercase tag is assume-unchanged and `S` skip-worktree, for
        // both of which `git status` does not look at the file.
        let (Some(tag), Some(path)) = (record.chars().next(), record.get(2..)) else {
            continue;
        };
        if tag.is_ascii_lowercase() || tag == 'S' {
            return Err(buck2_error!(
                ErrorTag::Input,
                "`{path}` is marked assume-unchanged or skip-worktree"
            ));
        }
        if let Some(path) = project_path(&prefix, path) {
            paths_input.extend_from_slice(path.as_str().as_bytes());
            paths_input.push(0);
            tracked.insert(path);
        }
    }

    // `git status` converts a file it rehashes by the attributes of the working tree, which an
    // uncommitted `.gitattributes` changes; the commits' attributes are checked as well.
    for source in [None, Some("HEAD"), Some(image.head.as_str())] {
        let mut args = vec!["check-attr", "-z", "--stdin"];
        if let Some(source) = source {
            args.extend_from_slice(&["--source", source]);
        }
        args.extend_from_slice(CONVERSION_ATTRIBUTES);
        let attributes = run_git_blocking(root, &args, Some(paths_input.clone()))?;
        let fields: Vec<&str> = records(&attributes).collect();
        for triple in fields.chunks(3) {
            if let [path, attribute, value] = triple
                && *value != "unspecified"
                && *value != "unset"
            {
                return Err(buck2_error!(
                    ErrorTag::Input,
                    "`{path}` has `{attribute}: {value}` at {}",
                    source.unwrap_or("the working tree")
                ));
            }
        }
    }

    let mut changed = HashSet::new();
    let diff = run_git_blocking(
        root,
        &[
            "-c",
            "diff.relative=false",
            "diff",
            "--name-only",
            "-z",
            "--no-renames",
            "--no-ext-diff",
            &image.head,
            "HEAD",
            "--",
        ],
        None,
    )?;
    changed.extend(records(&diff).filter_map(|path| project_path(&prefix, path)));
    // A file system monitor or untracked cache answers from its own state; read the files.
    // `core.trustctime=false` or `core.checkStat=minimal` lets a same-size rewrite that puts
    // the modification time back pass as clean, since only the change time moved.
    let status = run_git_blocking(
        root,
        &[
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.untrackedCache=false",
            "-c",
            "core.trustctime=true",
            "-c",
            "core.checkStat=default",
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--no-renames",
            "--ignore-submodules=none",
        ],
        None,
    )?;
    // `XY SP <path>`, the path relative to the repository's top level.
    changed.extend(
        records(&status)
            .filter_map(|record| record.get(3..))
            .filter_map(|path| project_path(&prefix, path)),
    );
    Ok(VcsDelta {
        tracked,
        changed,
        usable: true,
    })
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
        /// fresh runner's checkout, where every file has a new inode.
        fn reclone(&self, args: &[&str]) {
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
        repo.write("edit", "after!");
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
    /// with git and without.
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
        assert_eq!((scan.stat_equal, scan.vouched, scan.hashed), (1, 1, 0));
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

    /// An edit after the files were statted and before `git status` is dirty to git; one after
    /// `git status` moves the stat the watcher's next sync compares with.
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

        repo.write("f", "aaaa");
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

    /// A shallow clone that lacks the suspended commit: git is unusable, and the scan is the
    /// one without git.
    #[tokio::test(flavor = "multi_thread")]
    async fn shallow_clone_without_the_suspended_commit_hashes() {
        let repo = Repo::new();
        repo.write("a", "a");
        repo.write("b", "b");
        repo.commit("A");
        repo.age_all();
        let image = repo.suspend(&repo.crawler()).await;
        repo.write("b", "B!");
        repo.commit("B");
        repo.reclone(&["--depth", "1"]);

        assert!(!repo.delta(&image).usable);
        let with_git = repo.scan_git(&image);
        let without = repo.scan(&image, || None);
        assert_eq!(events(&with_git), set(&[("b", M)]));
        assert_eq!(events(&with_git), events(&without));
        assert_eq!((with_git.vouched, with_git.hashed), (0, 1));
    }

    /// Settings and attributes that let a checkout's bytes differ from its blob's, and a file
    /// git does not look at, each make git unusable.
    #[tokio::test(flavor = "multi_thread")]
    async fn conversions_and_hidden_files_make_git_unusable() {
        let repo = Repo::new();
        repo.write("a.txt", "a");
        repo.write("b", "b");
        repo.commit("A");
        let image = repo.suspend(&repo.crawler()).await;
        assert!(repo.delta(&image).usable);

        repo.git(&["config", "core.autocrlf", "true"]);
        assert!(!repo.delta(&image).usable);
        repo.git(&["config", "core.autocrlf", "false"]);
        assert!(repo.delta(&image).usable);

        repo.git(&["update-index", "--skip-worktree", "b"]);
        assert!(!repo.delta(&image).usable);
        repo.git(&["update-index", "--no-skip-worktree", "b"]);
        assert!(repo.delta(&image).usable);

        // Uncommitted, in the working tree only: `git status` applies it all the same.
        repo.write(".gitattributes", "*.txt filter=fake\n");
        assert!(!repo.delta(&image).usable);
        repo.commit("attributes");
        assert!(!repo.delta(&image).usable);
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
            // The index records the aged stat.
            repo.git(&["status", "--porcelain"]);
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
}
