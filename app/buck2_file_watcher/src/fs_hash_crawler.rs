/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::fs::Metadata;
use std::mem;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;

use allocative::Allocative;
use async_trait::async_trait;
use buck2_common::file_ops::baseline::FileSystemBaseline;
use buck2_common::file_ops::baseline::SetFileSystemBaseline;
use buck2_common::file_ops::dice::FileChangeTracker;
use buck2_common::file_ops::metadata::FileType;
use buck2_common::ignores::ignore_set::IgnoreSet;
use buck2_common::invocation_paths::InvocationPaths;
use buck2_core::cells::CellResolver;
use buck2_core::cells::cell_path::CellPath;
use buck2_core::cells::name::CellName;
use buck2_core::fs::project::ProjectRoot;
use buck2_core::fs::project_rel_path::ProjectRelativePath;
use buck2_core::fs::project_rel_path::ProjectRelativePathBuf;
use buck2_data::FileWatcherEventType;
use buck2_data::FileWatcherKind;
use buck2_error::BuckErrorContext;
use buck2_error::BuckErrorOptionContext;
use buck2_events::dispatch::span_async;
use buck2_fs::async_fs_util::spawn_blocking;
use buck2_fs::error::IoResultExt;
use buck2_fs::fs_util;
use buck2_fs::paths::abs_norm_path::AbsNormPath;
use buck2_fs::paths::file_name::FileNameBuf;
use buck2_hash::BuckMutMap;
use buck2_hash::StdBuckHashMap;
use compact_str::CompactString;
use dice::DiceTransactionUpdater;
use dupe::Dupe;

use crate::file_watcher::FileWatcher;
use crate::file_watcher::FileWatcherImage;
use crate::file_watcher::FileWatcherImageKind;
use crate::file_watcher::SyncOutcome;
use crate::mergebase::Mergebase;
use crate::stats::FileWatcherStats;

/// A file modified this close to the start of the previous crawl, or after it, is reported as
/// changed even when its stat is the same. Filesystems stamp files from a coarse clock, so a
/// file the previous crawl read can be rewritten, at the same size, within the same timestamp;
/// git's index treats such entries as "racily clean" for the same reason. A file in the window
/// costs a spurious report, which the DICE keys of most paths cut off.
const RACY_WINDOW: Duration = Duration::from_secs(2);

// On each sync, walks the repository and reports every path whose stat changed since the
// previous sync. It reads no file contents. Useful for tests on unreliable filesystems and
// where neither watchman nor inotify can watch the tree.
#[derive(Allocative)]
pub struct FsHashCrawler {
    root: ProjectRoot,
    cells: CellResolver,
    ignore_specs: Arc<StdBuckHashMap<CellName, IgnoreSet>>,
    snapshot: Arc<Mutex<FsSnapshot>>,
    /// The crawler never loses track of a change, so one baseline serves for its whole life.
    baseline: FileSystemBaseline,
}

impl FsHashCrawler {
    pub fn new(
        root: &ProjectRoot,
        cells: CellResolver,
        ignore_specs: StdBuckHashMap<CellName, IgnoreSet>,
    ) -> buck2_error::Result<Self> {
        let snapshot = Arc::new(Mutex::new(FsSnapshot::build(root, &cells, &ignore_specs)?));
        Ok(Self {
            root: root.dupe(),
            cells,
            ignore_specs: Arc::new(ignore_specs),
            snapshot,
            baseline: FileSystemBaseline::unique(),
        })
    }

    /// A crawler for a resumed daemon: its first sync reports every path whose stat differs
    /// from `image`'s snapshot, the one the suspended daemon's last sync took, so what changed
    /// while no daemon ran is recorded like any other change. It keeps the suspended daemon's
    /// baseline, which the restored file reads are relative to.
    pub fn resume(
        root: &ProjectRoot,
        cells: CellResolver,
        ignore_specs: StdBuckHashMap<CellName, IgnoreSet>,
        image: &FileWatcherImage,
    ) -> buck2_error::Result<Self> {
        let snapshot = FsSnapshot::from_image(&image.snapshot, &cells)?;
        Ok(Self {
            root: root.dupe(),
            cells,
            ignore_specs: Arc::new(ignore_specs),
            snapshot: Arc::new(Mutex::new(snapshot)),
            baseline: FileSystemBaseline::from_image(&image.baseline),
        })
    }

    async fn update(
        &self,
        mut dice: DiceTransactionUpdater,
    ) -> buck2_error::Result<(buck2_data::FileWatcherStats, DiceTransactionUpdater)> {
        let root = self.root.dupe();
        let cells = self.cells.dupe();
        let ignore_specs = self.ignore_specs.dupe();
        let new_snapshot =
            spawn_blocking(move || FsSnapshot::build(&root, &cells, &ignore_specs)).await??;
        let mut guard = self.snapshot.lock().unwrap();
        let old_snapshot = mem::replace(&mut *guard, new_snapshot);
        let (stats, changes) = old_snapshot.get_updates_for_dice(&guard, &self.ignore_specs)?;
        changes.write_to_dice(&mut dice)?;
        dice.set_file_system_baseline(self.baseline.dupe())?;
        Ok((stats, dice))
    }
}

#[async_trait]
impl FileWatcher for FsHashCrawler {
    /// The snapshot of the last sync is `S`: an edit made before it statted a file was
    /// recorded by that sync, and one made after differs from `S`, so the resumed crawler
    /// reports it.
    async fn suspend_image(
        &self,
        _project_root: &ProjectRoot,
    ) -> buck2_error::Result<FileWatcherImage> {
        Ok(FileWatcherImage {
            kind: FileWatcherImageKind::FsHashCrawler,
            baseline: self.baseline.to_image(),
            snapshot: self.snapshot.lock().unwrap().to_image(),
        })
    }

    async fn sync(
        &self,
        dice: DiceTransactionUpdater,
    ) -> buck2_error::Result<(DiceTransactionUpdater, Mergebase, SyncOutcome)> {
        span_async(
            buck2_data::FileWatcherStart {
                provider: buck2_data::FileWatcherProvider::FsHashCrawler as i32,
            },
            async {
                let (stats, res) = match self.update(dice).await {
                    Ok((stats, dice)) => {
                        let mergebase = Mergebase(Arc::new(stats.branched_from_revision.clone()));
                        let outcome = SyncOutcome::reported_by(&stats);
                        ((Some(stats)), Ok((dice, mergebase, outcome)))
                    }
                    Err(e) => (None, Err(e)),
                };
                (res, buck2_data::FileWatcherEnd { stats })
            },
        )
        .await
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
struct FsEvent {
    cell_path: CellPath,
    event: FileWatcherEventType,
    kind: FileWatcherKind,
}

/// What a file's stat says about its contents: any write changes the modification or change
/// time, and a replacement also changes the inode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileStat {
    modified: Option<SystemTime>,
    len: u64,
    #[cfg(unix)]
    changed: (i64, i64),
    #[cfg(unix)]
    ino: u64,
    #[cfg(unix)]
    dev: u64,
}

impl FileStat {
    fn from_metadata(metadata: &Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Self {
            modified: metadata.modified().ok(),
            len: metadata.len(),
            #[cfg(unix)]
            changed: (metadata.ctime(), metadata.ctime_nsec()),
            #[cfg(unix)]
            ino: metadata.ino(),
            #[cfg(unix)]
            dev: metadata.dev(),
        }
    }
}

#[derive(Allocative)]
enum EntryInfo {
    #[allocative(skip)]
    File(FileStat),
    Directory,
    /// The link's target, or `None` if it could not be read.
    Symlink(Option<String>),
}

impl EntryInfo {
    fn to_file_watcher_kind(&self) -> FileWatcherKind {
        match self {
            EntryInfo::File(_) => FileWatcherKind::File,
            EntryInfo::Directory => FileWatcherKind::Directory,
            EntryInfo::Symlink(_) => FileWatcherKind::Symlink,
        }
    }
}

#[derive(Allocative)]
pub(crate) struct FsSnapshot {
    entries: BuckMutMap<CellPath, EntryInfo>,
    /// When the walk that took this snapshot began.
    #[allocative(skip)]
    started: SystemTime,
}

/// [`FsSnapshot`] as `buck2 suspend` writes it: every path by cell and cell-relative path.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FsSnapshotImage {
    /// When the walk began, as seconds and nanoseconds since the epoch.
    started: (u64, u32),
    entries: Vec<(String, String, EntryImage)>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum EntryImage {
    File {
        modified: Option<(u64, u32)>,
        len: u64,
        changed: (i64, i64),
        ino: u64,
        dev: u64,
    },
    Directory,
    Symlink(Option<String>),
}

/// What the walk needs besides the directory it is in.
struct Walk<'a> {
    root: &'a ProjectRoot,
    cells: &'a CellResolver,
    ignore_specs: &'a StdBuckHashMap<CellName, IgnoreSet>,
    /// Every cell root but the project root's, since an ignored directory that holds one is
    /// still walked: the inner cell's own ignores decide what under it is ignored.
    cell_roots: Vec<ProjectRelativePathBuf>,
}

impl Walk<'_> {
    /// Whether this entry, and everything under it, can produce no event that survives the
    /// ignores.
    fn skips(&self, rel_path: &ProjectRelativePath, cell_path: &CellPath) -> bool {
        // A worktree's `.git` is a file, a checkout's a directory; either way its events are
        // git's own bookkeeping, like `.hg`.
        if rel_path.starts_with(ProjectRelativePath::unchecked_new(".git")) {
            return true;
        }
        let ignored = self
            .ignore_specs
            .get(&cell_path.cell())
            .is_some_and(|ignores| ignores.ignores_subtree(cell_path.path()));
        ignored
            && !self
                .cell_roots
                .iter()
                .any(|cell_root| cell_root.starts_with(rel_path) && &**cell_root != rel_path)
    }
}

impl FsSnapshot {
    pub(crate) fn to_image(&self) -> FsSnapshotImage {
        let started = self
            .started
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let mut entries: Vec<(String, String, EntryImage)> = self
            .entries
            .iter()
            .map(|(cell_path, info)| {
                (
                    cell_path.cell().as_str().to_owned(),
                    cell_path.path().as_str().to_owned(),
                    match info {
                        EntryInfo::File(stat) => EntryImage::File {
                            modified: stat.modified.and_then(|m| {
                                m.duration_since(SystemTime::UNIX_EPOCH)
                                    .ok()
                                    .map(|d| (d.as_secs(), d.subsec_nanos()))
                            }),
                            len: stat.len,
                            #[cfg(unix)]
                            changed: stat.changed,
                            #[cfg(not(unix))]
                            changed: (0, 0),
                            #[cfg(unix)]
                            ino: stat.ino,
                            #[cfg(not(unix))]
                            ino: 0,
                            #[cfg(unix)]
                            dev: stat.dev,
                            #[cfg(not(unix))]
                            dev: 0,
                        },
                        EntryInfo::Directory => EntryImage::Directory,
                        EntryInfo::Symlink(target) => EntryImage::Symlink(target.clone()),
                    },
                )
            })
            .collect();
        entries.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        FsSnapshotImage {
            started: (started.as_secs(), started.subsec_nanos()),
            entries,
        }
    }

    pub(crate) fn from_image(
        image: &FsSnapshotImage,
        cells: &CellResolver,
    ) -> buck2_error::Result<Self> {
        let mut entries = BuckMutMap::default();
        for (cell, path, entry) in &image.entries {
            let cell = cells
                .cells()
                .map(|(name, _)| name)
                .find(|name| name.as_str() == cell)
                .ok_or_else(|| {
                    buck2_error::buck2_error!(
                        buck2_error::ErrorTag::Input,
                        "the suspended snapshot names cell `{cell}`, which no longer exists"
                    )
                })?;
            let cell_path = CellPath::new(
                cell,
                &buck2_core::cells::paths::CellRelativePathBuf::try_from(path.clone())?,
            );
            let info = match entry {
                EntryImage::File {
                    modified,
                    len,
                    changed,
                    ino,
                    dev,
                } => {
                    let _unused = (changed, ino, dev);
                    EntryInfo::File(FileStat {
                        modified: modified.map(|(secs, nanos)| {
                            SystemTime::UNIX_EPOCH + Duration::new(secs, nanos)
                        }),
                        len: *len,
                        #[cfg(unix)]
                        changed: *changed,
                        #[cfg(unix)]
                        ino: *ino,
                        #[cfg(unix)]
                        dev: *dev,
                    })
                }
                EntryImage::Directory => EntryInfo::Directory,
                EntryImage::Symlink(target) => EntryInfo::Symlink(target.clone()),
            };
            entries.insert(cell_path, info);
        }
        Ok(FsSnapshot {
            entries,
            started: SystemTime::UNIX_EPOCH + Duration::new(image.started.0, image.started.1),
        })
    }

    pub(crate) fn build(
        root: &ProjectRoot,
        cells: &CellResolver,
        ignore_specs: &StdBuckHashMap<CellName, IgnoreSet>,
    ) -> buck2_error::Result<Self> {
        let started = SystemTime::now();
        let walk = Walk {
            root,
            cells,
            ignore_specs,
            cell_roots: cells
                .cells()
                .map(|(_, instance)| instance.path().as_project_relative_path().to_buf())
                .filter(|path| !path.is_empty())
                .collect(),
        };
        let mut snapshot = FsSnapshot {
            entries: BuckMutMap::default(),
            started,
        };
        snapshot.build_fs_snapshot(&walk, root.root())?;
        Ok(snapshot)
    }

    fn add_entry(&mut self, cell: CellPath, info: EntryInfo) {
        self.entries.insert(cell, info);
    }

    /// Whether a file this snapshot saw unchanged may still have changed since: it was
    /// modified within `RACY_WINDOW` of this snapshot's walk, or after it, or has no time.
    fn racily_clean(&self, stat: &FileStat) -> bool {
        stat.modified
            .is_none_or(|modified| modified + RACY_WINDOW >= self.started)
    }

    fn get_updates(&self, new_snapshot: &FsSnapshot) -> buck2_error::Result<Vec<FsEvent>> {
        let mut events = Vec::new();
        for (cell_path, prev_info) in self.entries.iter() {
            if let Some(current_info) = new_snapshot.entries.get(cell_path) {
                match (current_info, prev_info) {
                    (EntryInfo::File(cur), EntryInfo::File(prev)) => {
                        if cur != prev || self.racily_clean(prev) {
                            events.push(FsEvent {
                                cell_path: cell_path.to_owned(),
                                event: FileWatcherEventType::Modify,
                                kind: prev_info.to_file_watcher_kind(),
                            });
                        }
                    }
                    (EntryInfo::Directory, EntryInfo::Directory) => (),
                    (EntryInfo::Symlink(cur), EntryInfo::Symlink(prev)) => {
                        // A retargeted link reads as different contents.
                        if cur != prev || cur.is_none() {
                            events.push(FsEvent {
                                cell_path: cell_path.to_owned(),
                                event: FileWatcherEventType::Modify,
                                kind: prev_info.to_file_watcher_kind(),
                            });
                        }
                    }
                    (current_info, prev_info) => {
                        events.push(FsEvent {
                            cell_path: cell_path.to_owned(),
                            event: FileWatcherEventType::Delete,
                            kind: prev_info.to_file_watcher_kind(),
                        });
                        events.push(FsEvent {
                            cell_path: cell_path.to_owned(),
                            event: FileWatcherEventType::Create,
                            kind: current_info.to_file_watcher_kind(),
                        });
                    }
                }
            } else {
                events.push(FsEvent {
                    cell_path: cell_path.to_owned(),
                    event: FileWatcherEventType::Delete,
                    kind: prev_info.to_file_watcher_kind(),
                });
            }
        }
        let new_entries = new_snapshot
            .entries
            .iter()
            .filter(|(path, _)| !self.entries.contains_key(*path));
        for (cell_path, info) in new_entries {
            events.push(FsEvent {
                cell_path: cell_path.to_owned(),
                event: FileWatcherEventType::Create,
                kind: info.to_file_watcher_kind(),
            });
        }
        Ok(events)
    }

    pub(crate) fn get_updates_for_dice(
        &self,
        new_snapshot: &FsSnapshot,
        ignore_specs: &StdBuckHashMap<CellName, IgnoreSet>,
    ) -> buck2_error::Result<(buck2_data::FileWatcherStats, FileChangeTracker)> {
        let events = self.get_updates(new_snapshot)?;
        let mut changed = FileChangeTracker::new();
        let mut stats = FileWatcherStats::new(Default::default(), events.len());
        let mut ignored = 0;
        for event in events.into_iter() {
            let ignore = ignore_specs
                .get(&event.cell_path.cell())
                .is_some_and(|i| i.is_match(event.cell_path.path()));

            if ignore {
                ignored += 1;
                continue;
            }

            stats.add(event.cell_path.to_string(), event.event, event.kind);
            match (event.event, event.kind) {
                (
                    FileWatcherEventType::Create,
                    FileWatcherKind::File | FileWatcherKind::Symlink,
                ) => {
                    changed.file_added_or_removed(event.cell_path);
                }
                (FileWatcherEventType::Create, FileWatcherKind::Directory) => {
                    changed.dir_added_or_removed(event.cell_path);
                }
                (
                    FileWatcherEventType::Modify,
                    FileWatcherKind::File | FileWatcherKind::Symlink,
                ) => {
                    changed.file_contents_changed(event.cell_path);
                }
                (FileWatcherEventType::Modify, FileWatcherKind::Directory) => {
                    // FIXME(JakobDegen): This should not be needed
                    changed.dir_entries_changed_force_invalidate(event.cell_path);
                }
                (
                    FileWatcherEventType::Delete,
                    FileWatcherKind::File | FileWatcherKind::Symlink,
                ) => {
                    changed.file_added_or_removed(event.cell_path);
                }
                (FileWatcherEventType::Delete, FileWatcherKind::Directory) => {
                    changed.dir_added_or_removed(event.cell_path);
                }
            }
        }
        stats.add_ignored(ignored);
        Ok((stats.finish(), changed))
    }

    fn build_fs_snapshot(
        &mut self,
        walk: &Walk,
        disk_path: &AbsNormPath,
    ) -> buck2_error::Result<()> {
        for file in fs_util::read_dir(disk_path).categorize_internal()? {
            let file = file?;
            let filetype = file.file_type()?;
            let filename = file.file_name();

            let filename = FileNameBuf::try_from(CompactString::new(
                filename.to_str().internal_error("Filename is not UTF-8")?,
            ))
            .with_buck_error_context(|| format!("Invalid filename: {}", disk_path.display()))?;

            let disk_path = disk_path.join(filename);
            let rel_path = walk.root.relativize(&disk_path)?;
            let cell_path = walk.cells.get_cell_path(&rel_path);

            // We ignore buck-out and .hg dirs, as those are uninteresting events caused by us.
            if rel_path.starts_with(InvocationPaths::buck_out_dir_prefix())
                || rel_path.starts_with(ProjectRelativePath::unchecked_new(".hg"))
                || walk.skips(&rel_path, &cell_path)
            {
                continue;
            }

            let filetype = FileType::from(filetype);
            match filetype {
                FileType::File => {
                    // `DirEntry::metadata` does not follow symlinks, and this entry is a file.
                    let metadata = file.metadata()?;
                    self.add_entry(
                        cell_path,
                        EntryInfo::File(FileStat::from_metadata(&metadata)),
                    );
                }
                FileType::Directory => {
                    self.build_fs_snapshot(walk, &disk_path)?;
                    self.add_entry(cell_path, EntryInfo::Directory);
                }
                FileType::Symlink => {
                    let target = std::fs::read_link(disk_path.as_path())
                        .ok()
                        .and_then(|t| t.to_str().map(ToOwned::to_owned));
                    self.add_entry(cell_path, EntryInfo::Symlink(target));
                }
                FileType::Unknown => (),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;
    use std::time::SystemTime;

    use buck2_common::ignores::ignore_set::IgnoreSet;
    use buck2_core::cells::CellResolver;
    use buck2_core::cells::cell_path::CellPath;
    use buck2_core::cells::cell_root_path::CellRootPathBuf;
    use buck2_core::cells::name::CellName;
    use buck2_core::fs::project::ProjectRoot;
    use buck2_core::fs::project_rel_path::ProjectRelativePath;
    use buck2_data::FileWatcherEventType;
    use buck2_data::FileWatcherKind;
    use buck2_fs::fs_util::uncategorized as fs_util;
    use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;
    use buck2_fs::paths::abs_path::AbsPathBuf;
    use buck2_hash::StdBuckHashMap;

    use crate::fs_hash_crawler::FsEvent;
    use crate::fs_hash_crawler::FsSnapshot;

    fn project(
        cells: &[(&str, &str)],
    ) -> buck2_error::Result<(tempfile::TempDir, ProjectRoot, CellResolver)> {
        let cell_resolver = CellResolver::testing_with_names_and_paths(
            &cells
                .iter()
                .map(|(name, path)| {
                    (
                        CellName::testing_new(name),
                        CellRootPathBuf::testing_new(path),
                    )
                })
                .collect::<Vec<_>>(),
        );
        let tempdir = tempfile::tempdir()?;
        let root_path = fs_util::canonicalize(AbsNormPathBuf::new(tempdir.path().to_owned())?)?;
        Ok((tempdir, ProjectRoot::new(root_path)?, cell_resolver))
    }

    fn write_aged(path: &AbsPathBuf, contents: &str) -> buck2_error::Result<()> {
        fs_util::write(path, contents)?;
        let file = std::fs::OpenOptions::new().write(true).open(path)?;
        file.set_modified(SystemTime::now() - Duration::from_secs(3600))?;
        Ok(())
    }

    fn file_events(
        old: &FsSnapshot,
        new: &FsSnapshot,
    ) -> buck2_error::Result<BTreeSet<(String, FileWatcherEventType)>> {
        Ok(old
            .get_updates(new)?
            .into_iter()
            .filter(|event| event.kind == FileWatcherKind::File)
            .map(|event| (event.cell_path.path().to_string(), event.event))
            .collect())
    }

    /// A file rewritten at the same size and given back its modification time still changed:
    /// its change time moved. A file left alone long enough is not reported.
    #[tokio::test]
    async fn test_fs_snapshot_reports_a_same_size_rewrite_and_not_an_aged_file()
    -> buck2_error::Result<()> {
        let (_tempdir, proj_root, cells) = project(&[("root", "")])?;
        let ignores = StdBuckHashMap::default();
        let same = proj_root
            .resolve(ProjectRelativePath::new("same")?)
            .into_abs_path_buf();
        let rewritten = proj_root
            .resolve(ProjectRelativePath::new("rewritten")?)
            .into_abs_path_buf();
        write_aged(&same, "aaaa")?;
        write_aged(&rewritten, "aaaa")?;
        let modified = std::fs::metadata(&rewritten)?.modified()?;

        let old = FsSnapshot::build(&proj_root, &cells, &ignores)?;
        // Change time has a coarse clock too; step past it.
        std::thread::sleep(Duration::from_millis(20));
        fs_util::write(&rewritten, "bbbb")?;
        std::fs::OpenOptions::new()
            .write(true)
            .open(&rewritten)?
            .set_modified(modified)?;
        let new = FsSnapshot::build(&proj_root, &cells, &ignores)?;

        assert_eq!(
            file_events(&old, &new)?,
            BTreeSet::from([("rewritten".to_owned(), FileWatcherEventType::Modify)])
        );
        Ok(())
    }

    /// A same-size edit within one second of the walk before it is reported twice over: its
    /// stat moved (the change time, and the modification time unless a tool restores it), and
    /// it is racily clean. With the racily clean rule out of the way, the stat alone still
    /// reports it, including when the modification time is put back as `git checkout` or
    /// `touch -r` would.
    #[tokio::test]
    async fn test_fs_snapshot_reports_a_same_size_edit_within_one_second() -> buck2_error::Result<()>
    {
        let (_tempdir, proj_root, cells) = project(&[("root", "")])?;
        let ignores = StdBuckHashMap::default();
        let file = proj_root
            .resolve(ProjectRelativePath::new("file")?)
            .into_abs_path_buf();
        let expected = BTreeSet::from([("file".to_owned(), FileWatcherEventType::Modify)]);

        fs_util::write(&file, "aaaa")?;
        let modified = std::fs::metadata(&file)?.modified()?;
        let mut old = FsSnapshot::build(&proj_root, &cells, &ignores)?;
        // Within one tick of a coarse timestamp clock the stat cannot move, which is what the
        // racily clean rule is for; step past a tick so that the stat comparison is tested.
        std::thread::sleep(Duration::from_millis(20));
        fs_util::write(&file, "bbbb")?;
        let new = FsSnapshot::build(&proj_root, &cells, &ignores)?;
        assert_eq!(file_events(&old, &new)?, expected);

        // As if the previous walk began long after the file's modification time, so only the
        // stat comparison can report it.
        old.started = SystemTime::now() + Duration::from_secs(3600);
        assert_eq!(file_events(&old, &new)?, expected);

        std::thread::sleep(Duration::from_millis(20));
        fs_util::write(&file, "cccc")?;
        std::fs::OpenOptions::new()
            .write(true)
            .open(&file)?
            .set_modified(modified)?;
        let mut restored = FsSnapshot::build(&proj_root, &cells, &ignores)?;
        restored.started = old.started;
        let mut new = new;
        new.started = old.started;
        assert_eq!(file_events(&new, &restored)?, expected);
        Ok(())
    }

    /// A file modified around the previous walk is reported even with its stat unchanged, since
    /// a rewrite within one timestamp tick of that walk leaves the stat the same.
    #[tokio::test]
    async fn test_fs_snapshot_reports_a_racily_clean_file() -> buck2_error::Result<()> {
        let (_tempdir, proj_root, cells) = project(&[("root", "")])?;
        let ignores = StdBuckHashMap::default();
        let fresh = proj_root
            .resolve(ProjectRelativePath::new("fresh")?)
            .into_abs_path_buf();
        fs_util::write(&fresh, "x")?;

        let old = FsSnapshot::build(&proj_root, &cells, &ignores)?;
        let new = FsSnapshot::build(&proj_root, &cells, &ignores)?;

        assert_eq!(
            file_events(&old, &new)?,
            BTreeSet::from([("fresh".to_owned(), FileWatcherEventType::Modify)])
        );
        Ok(())
    }

    /// The walk skips `.git` and every directory a pattern without globs ignores (`skipped`),
    /// unless a cell lives under it (`ignored`), and walks a directory a glob matches (`gen/x`),
    /// since the glob may not match its children.
    #[tokio::test]
    async fn test_fs_snapshot_skips_ignored_subtrees() -> buck2_error::Result<()> {
        let (_tempdir, proj_root, cells) = project(&[("root", ""), ("inner", "ignored/inner")])?;
        let ignores = StdBuckHashMap::from_iter([(
            CellName::testing_new("root"),
            IgnoreSet::from_ignore_spec("ignored, skipped, gen/*", true)?,
        )]);
        for path in [
            ".git/HEAD",
            "skipped/file",
            "ignored/file",
            "ignored/inner/file",
            "gen/x/file",
            "src/file",
        ] {
            let path = proj_root
                .resolve(ProjectRelativePath::new(path)?)
                .into_abs_path_buf();
            fs_util::create_dir_all(path.parent().unwrap())?;
            fs_util::write(&path, "x")?;
        }

        let snapshot = FsSnapshot::build(&proj_root, &cells, &ignores)?;
        let mut files = snapshot
            .entries
            .keys()
            .map(|path| path.to_string())
            .collect::<Vec<_>>();
        files.sort();

        assert_eq!(
            files,
            [
                "inner//",
                "inner//file",
                "root//gen",
                "root//gen/x",
                "root//gen/x/file",
                "root//ignored",
                "root//src",
                "root//src/file",
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_fs_snapshot() -> buck2_error::Result<()> {
        let cell_resolver = CellResolver::testing_with_name_and_path(
            CellName::testing_new("root"),
            CellRootPathBuf::testing_new(""),
        );
        let tempdir = tempfile::tempdir()?;
        let root_path = fs_util::canonicalize(AbsNormPathBuf::new(tempdir.path().to_owned())?)?;
        let proj_root = ProjectRoot::new(root_path)?;

        let get_path = |path| -> buck2_error::Result<(AbsPathBuf, CellPath)> {
            let path = ProjectRelativePath::new(path).unwrap();
            let cell_path = cell_resolver.get_cell_path(path);
            Ok((proj_root.resolve(path).into_abs_path_buf(), cell_path))
        };
        let dir1 = proj_root.resolve(ProjectRelativePath::new("dir1")?);
        let (file1, file1_cell) = get_path("dir1/file1")?;
        let (dir2, dir2_cell) = get_path("dir2")?;
        let (file2, file2_cell) = get_path("dir2/file2")?;
        let (file3, file3_cell) = get_path("dir1/file3")?;
        fs_util::create_dir_all(dir1)?;
        fs_util::write(&file1, "old content")?;
        fs_util::create_dir_all(&dir2)?;
        fs_util::write(file2, "old content")?;

        let old_snapshot =
            FsSnapshot::build(&proj_root, &cell_resolver, &StdBuckHashMap::default())?;
        fs_util::write(file1, "new content")?;
        fs_util::remove_all(dir2)?;
        fs_util::write(file3, "new content")?;
        let new_snapshot =
            FsSnapshot::build(&proj_root, &cell_resolver, &StdBuckHashMap::default())?;
        let events = old_snapshot.get_updates(&new_snapshot)?;

        let expected = [
            FsEvent {
                cell_path: file1_cell,
                event: FileWatcherEventType::Modify,
                kind: FileWatcherKind::File,
            },
            FsEvent {
                cell_path: file3_cell,
                event: FileWatcherEventType::Create,
                kind: FileWatcherKind::File,
            },
            FsEvent {
                cell_path: dir2_cell,
                event: FileWatcherEventType::Delete,
                kind: FileWatcherKind::Directory,
            },
            FsEvent {
                cell_path: file2_cell,
                event: FileWatcherEventType::Delete,
                kind: FileWatcherKind::File,
            },
        ];

        let events = events.iter().collect::<BTreeSet<_>>();
        let expected = expected.iter().collect::<BTreeSet<_>>();
        assert_eq!(events, expected);
        Ok(())
    }
}
