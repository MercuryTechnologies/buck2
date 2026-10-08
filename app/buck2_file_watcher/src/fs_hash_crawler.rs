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
use buck2_common::file_ops::io::FileChangeTracker;
use buck2_common::file_ops::metadata::FileType;
use buck2_common::invocation_paths::InvocationPaths;
use buck2_core::fs::i_project_rel_path::IProjectRelativePath;
use buck2_core::fs::project::ProjectRoot;
use buck2_core::fs::project_rel_path::ProjectRelativePath;
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
use compact_str::CompactString;
use dice::DiceTransactionUpdater;
use dupe::Dupe;

use crate::file_watcher::FileWatcher;
use crate::file_watcher::SyncOutcome;
use crate::ignores::FileWatcherIgnores;
use crate::mergebase::Mergebase;
use crate::stats::FileWatcherStats;

/// A file modified this close to the start of the previous crawl, or after it, is reported as
/// changed even when its stat is the same. Filesystems stamp files from a coarse clock, so a
/// file the previous crawl saw can be rewritten, at the same size, within the same timestamp;
/// git's index treats such entries as "racily clean" for the same reason. A file in the window
/// costs a spurious report, which the DICE keys of most paths cut off.
const RACY_WINDOW: Duration = Duration::from_secs(2);

// On each sync, walks the repository and reports every path whose stat changed since the
// previous sync. It reads no file contents. Useful for tests on unreliable filesystems and
// where neither watchman nor inotify can watch the tree.
#[derive(Allocative)]
pub struct FsHashCrawler {
    root: ProjectRoot,
    ignores: FileWatcherIgnores,
    snapshot: Arc<Mutex<FsSnapshot>>,
    /// The crawler never loses track of a change, so one baseline serves for its whole life.
    baseline: FileSystemBaseline,
}

impl FsHashCrawler {
    pub fn new(root: &ProjectRoot, ignores: FileWatcherIgnores) -> buck2_error::Result<Self> {
        let snapshot = Arc::new(Mutex::new(FsSnapshot::build(root)?));
        Ok(Self {
            root: root.dupe(),
            ignores,
            snapshot,
            baseline: FileSystemBaseline::unique(),
        })
    }

    async fn update(
        &self,
        mut dice: DiceTransactionUpdater,
    ) -> buck2_error::Result<(buck2_data::FileWatcherStats, DiceTransactionUpdater)> {
        let root = self.root.dupe();
        let new_snapshot = spawn_blocking(move || FsSnapshot::build(&root)).await??;
        let mut guard = self.snapshot.lock().unwrap();
        let old_snapshot = mem::replace(&mut *guard, new_snapshot);
        let (stats, changes) = old_snapshot.get_updates_for_dice(&guard, &self.ignores)?;
        changes.write_to_dice(&mut dice)?;
        dice.set_file_system_baseline(self.baseline.dupe())?;
        Ok((stats, dice))
    }
}

#[async_trait]
impl FileWatcher for FsHashCrawler {
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
    path: IProjectRelativePath,
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
    Symlink,
}

impl EntryInfo {
    fn to_file_watcher_kind(&self) -> FileWatcherKind {
        match self {
            EntryInfo::File(_) => FileWatcherKind::File,
            EntryInfo::Directory => FileWatcherKind::Directory,
            EntryInfo::Symlink => FileWatcherKind::Symlink,
        }
    }
}

#[derive(Allocative)]
struct FsSnapshot {
    entries: BuckMutMap<IProjectRelativePath, EntryInfo>,
    /// When the walk that took this snapshot began.
    #[allocative(skip)]
    started: SystemTime,
}

impl FsSnapshot {
    fn build(root: &ProjectRoot) -> buck2_error::Result<Self> {
        let mut snapshot = FsSnapshot {
            entries: BuckMutMap::default(),
            started: SystemTime::now(),
        };
        snapshot.build_fs_snapshot(root, root.root())?;
        Ok(snapshot)
    }

    fn add_entry(&mut self, path: IProjectRelativePath, info: EntryInfo) {
        self.entries.insert(path, info);
    }

    /// Whether a file this snapshot saw unchanged may still have changed since: it was
    /// modified within `RACY_WINDOW` of this snapshot's walk, or after it, or has no time.
    fn racily_clean(&self, stat: &FileStat) -> bool {
        stat.modified
            .is_none_or(|modified| modified + RACY_WINDOW >= self.started)
    }

    fn get_updates(&self, new_snapshot: &FsSnapshot) -> buck2_error::Result<Vec<FsEvent>> {
        let mut events = Vec::new();
        for (path, prev_info) in self.entries.iter() {
            if let Some(current_info) = new_snapshot.entries.get(path) {
                match (current_info, prev_info) {
                    (EntryInfo::File(cur), EntryInfo::File(prev)) => {
                        if cur != prev || self.racily_clean(prev) {
                            events.push(FsEvent {
                                path: *path,
                                event: FileWatcherEventType::Modify,
                                kind: prev_info.to_file_watcher_kind(),
                            });
                        }
                    }
                    (EntryInfo::Directory, EntryInfo::Directory) => (),
                    // FIXME(JakobDegen): Track symlink targets
                    (EntryInfo::Symlink, EntryInfo::Symlink) => (),
                    (current_info, prev_info) => {
                        events.push(FsEvent {
                            path: *path,
                            event: FileWatcherEventType::Delete,
                            kind: prev_info.to_file_watcher_kind(),
                        });
                        events.push(FsEvent {
                            path: *path,
                            event: FileWatcherEventType::Create,
                            kind: current_info.to_file_watcher_kind(),
                        });
                    }
                }
            } else {
                events.push(FsEvent {
                    path: *path,
                    event: FileWatcherEventType::Delete,
                    kind: prev_info.to_file_watcher_kind(),
                });
            }
        }
        let new_entries = new_snapshot
            .entries
            .iter()
            .filter(|(path, _)| !self.entries.contains_key(*path));
        for (path, info) in new_entries {
            events.push(FsEvent {
                path: *path,
                event: FileWatcherEventType::Create,
                kind: info.to_file_watcher_kind(),
            });
        }
        Ok(events)
    }

    fn get_updates_for_dice(
        &self,
        new_snapshot: &FsSnapshot,
        ignores: &FileWatcherIgnores,
    ) -> buck2_error::Result<(buck2_data::FileWatcherStats, FileChangeTracker)> {
        let events = self.get_updates(new_snapshot)?;
        let mut changed = FileChangeTracker::new();
        let mut stats = FileWatcherStats::new(Default::default(), events.len());
        let mut ignored = 0;
        for event in events.into_iter() {
            let ignore = ignores.is_ignored(event.path);

            if ignore {
                ignored += 1;
                continue;
            }

            stats.add(event.path, event.event, event.kind);
            match (event.event, event.kind) {
                (
                    FileWatcherEventType::Create,
                    FileWatcherKind::File | FileWatcherKind::Symlink,
                ) => {
                    changed.file_added_or_removed(event.path);
                }
                (FileWatcherEventType::Create, FileWatcherKind::Directory) => {
                    changed.dir_added_or_removed(event.path);
                }
                (
                    FileWatcherEventType::Modify,
                    FileWatcherKind::File | FileWatcherKind::Symlink,
                ) => {
                    changed.file_contents_changed(event.path);
                }
                (FileWatcherEventType::Modify, FileWatcherKind::Directory) => {
                    // FIXME(JakobDegen): This should not be needed
                    changed.dir_entries_changed_force_invalidate(event.path);
                }
                (
                    FileWatcherEventType::Delete,
                    FileWatcherKind::File | FileWatcherKind::Symlink,
                ) => {
                    changed.file_added_or_removed(event.path);
                }
                (FileWatcherEventType::Delete, FileWatcherKind::Directory) => {
                    changed.dir_added_or_removed(event.path);
                }
            }
        }
        stats.add_ignored(ignored);
        Ok((stats.finish(), changed))
    }

    fn build_fs_snapshot(
        &mut self,
        root: &ProjectRoot,
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
            let rel_path = root.relativize(&disk_path)?;
            let path = IProjectRelativePath::intern_directly(rel_path.as_ref());

            // We ignore buck-out and .hg dirs, as those are uninteresting events caused by us. A
            // worktree's `.git` is a file, a checkout's a directory; either way its events are
            // git's own bookkeeping, like `.hg`.
            if rel_path.starts_with(InvocationPaths::buck_out_dir_prefix())
                || rel_path.starts_with(ProjectRelativePath::unchecked_new(".hg"))
                || rel_path.starts_with(ProjectRelativePath::unchecked_new(".git"))
            {
                continue;
            }

            let filetype = FileType::from(filetype);
            match filetype {
                FileType::File => {
                    // `DirEntry::metadata` does not follow symlinks, and this entry is a file.
                    let metadata = file.metadata()?;
                    self.add_entry(path, EntryInfo::File(FileStat::from_metadata(&metadata)));
                }
                FileType::Directory => {
                    self.build_fs_snapshot(root, &disk_path)?;
                    self.add_entry(path, EntryInfo::Directory);
                }
                FileType::Symlink => {
                    self.add_entry(path, EntryInfo::Symlink);
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

    use buck2_core::fs::i_project_rel_path::IProjectRelativePath;
    use buck2_core::fs::project::ProjectRoot;
    use buck2_core::fs::project_rel_path::ProjectRelativePath;
    use buck2_data::FileWatcherEventType;
    use buck2_data::FileWatcherKind;
    use buck2_fs::fs_util::uncategorized as fs_util;
    use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;
    use buck2_fs::paths::abs_path::AbsPathBuf;

    use crate::fs_hash_crawler::FsEvent;
    use crate::fs_hash_crawler::FsSnapshot;

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
            .map(|event| (event.path.to_string(), event.event))
            .collect())
    }

    /// A file rewritten at the same size and given back its modification time still changed:
    /// its change time moved. A file left alone long enough is not reported.
    #[tokio::test]
    async fn test_fs_snapshot_reports_a_same_size_rewrite_and_not_an_aged_file()
    -> buck2_error::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root_path = fs_util::canonicalize(AbsNormPathBuf::new(tempdir.path().to_owned())?)?;
        let proj_root = ProjectRoot::new(root_path)?;
        let same = proj_root
            .resolve(ProjectRelativePath::new("same")?)
            .into_abs_path_buf();
        let rewritten = proj_root
            .resolve(ProjectRelativePath::new("rewritten")?)
            .into_abs_path_buf();
        write_aged(&same, "aaaa")?;
        write_aged(&rewritten, "aaaa")?;
        let modified = std::fs::metadata(&rewritten)?.modified()?;

        let old = FsSnapshot::build(&proj_root)?;
        // Change time has a coarse clock too; step past it.
        std::thread::sleep(Duration::from_millis(20));
        fs_util::write(&rewritten, "bbbb")?;
        std::fs::OpenOptions::new()
            .write(true)
            .open(&rewritten)?
            .set_modified(modified)?;
        let new = FsSnapshot::build(&proj_root)?;

        assert_eq!(
            file_events(&old, &new)?,
            BTreeSet::from([("rewritten".to_owned(), FileWatcherEventType::Modify)])
        );
        Ok(())
    }

    /// A same-size edit whose modification time is put back, as `git checkout` or `touch -r`
    /// would, is reported by its stat alone even when the walk before it began long after.
    #[tokio::test]
    async fn test_fs_snapshot_reports_a_same_size_edit_by_its_stat() -> buck2_error::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root_path = fs_util::canonicalize(AbsNormPathBuf::new(tempdir.path().to_owned())?)?;
        let proj_root = ProjectRoot::new(root_path)?;
        let file = proj_root
            .resolve(ProjectRelativePath::new("file")?)
            .into_abs_path_buf();
        write_aged(&file, "aaaa")?;
        let modified = std::fs::metadata(&file)?.modified()?;
        let mut old = FsSnapshot::build(&proj_root)?;
        old.started = SystemTime::now() + Duration::from_secs(3600);
        std::thread::sleep(Duration::from_millis(20));
        fs_util::write(&file, "bbbb")?;
        std::fs::OpenOptions::new()
            .write(true)
            .open(&file)?
            .set_modified(modified)?;
        let new = FsSnapshot::build(&proj_root)?;
        assert_eq!(
            file_events(&old, &new)?,
            BTreeSet::from([("file".to_owned(), FileWatcherEventType::Modify)])
        );
        Ok(())
    }

    /// A walk never descends into `.git`.
    #[tokio::test]
    async fn test_fs_snapshot_skips_git() -> buck2_error::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root_path = fs_util::canonicalize(AbsNormPathBuf::new(tempdir.path().to_owned())?)?;
        let proj_root = ProjectRoot::new(root_path)?;
        let git = proj_root.resolve(ProjectRelativePath::new(".git")?);
        fs_util::create_dir_all(&git)?;
        fs_util::write(
            proj_root
                .resolve(ProjectRelativePath::new(".git/HEAD")?)
                .into_abs_path_buf(),
            "ref",
        )?;
        let snapshot = FsSnapshot::build(&proj_root)?;
        assert!(snapshot.entries.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_fs_snapshot() -> buck2_error::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let root_path = fs_util::canonicalize(AbsNormPathBuf::new(tempdir.path().to_owned())?)?;
        let proj_root = ProjectRoot::new(root_path)?;

        let get_path = |path| -> buck2_error::Result<(AbsPathBuf, IProjectRelativePath)> {
            let path =
                IProjectRelativePath::intern_directly(ProjectRelativePath::new(path).unwrap());
            Ok((proj_root.resolve(path).into_abs_path_buf(), path))
        };
        let dir1 = proj_root.resolve(ProjectRelativePath::new("dir1")?);
        let (file1, file1_project) = get_path("dir1/file1")?;
        let (dir2, dir2_project) = get_path("dir2")?;
        let (file2, file2_project) = get_path("dir2/file2")?;
        let (file3, file3_project) = get_path("dir1/file3")?;
        fs_util::create_dir_all(dir1)?;
        fs_util::write(&file1, "old content")?;
        fs_util::create_dir_all(&dir2)?;
        fs_util::write(file2, "old content")?;

        let old_snapshot = FsSnapshot::build(&proj_root)?;
        fs_util::write(file1, "new content")?;
        fs_util::remove_all(dir2)?;
        fs_util::write(file3, "new content")?;
        let new_snapshot = FsSnapshot::build(&proj_root)?;
        let events = old_snapshot.get_updates(&new_snapshot)?;

        let expected = [
            FsEvent {
                path: file1_project,
                event: FileWatcherEventType::Modify,
                kind: FileWatcherKind::File,
            },
            FsEvent {
                path: file3_project,
                event: FileWatcherEventType::Create,
                kind: FileWatcherKind::File,
            },
            FsEvent {
                path: dir2_project,
                event: FileWatcherEventType::Delete,
                kind: FileWatcherKind::Directory,
            },
            FsEvent {
                path: file2_project,
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
