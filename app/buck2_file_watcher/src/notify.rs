/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::mem;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use allocative::Allocative;
use async_trait::async_trait;
use buck2_common::file_ops::baseline::FileSystemBaseline;
use buck2_common::file_ops::baseline::SetFileSystemBaseline;
use buck2_common::file_ops::dice::FileChangeTracker;
use buck2_common::ignores::ignore_set::IgnoreSet;
use buck2_common::invocation_paths::InvocationPaths;
use buck2_core::cells::CellResolver;
use buck2_core::cells::cell_path::CellPath;
use buck2_core::cells::name::CellName;
use buck2_core::fs::project::ProjectRoot;
use buck2_data::FileWatcherEventType;
use buck2_data::FileWatcherKind;
use buck2_error::conversion::from_any_with_tag;
use buck2_events::dispatch::span_async;
use buck2_fs::paths::abs_norm_path::AbsNormPath;
use buck2_hash::StdBuckHashMap;
use dice::DiceTransactionUpdater;
use dupe::Dupe;
use notify::EventKind;
use notify::RecommendedWatcher;
use notify::Watcher;
use notify::event::CreateKind;
use notify::event::MetadataKind;
use notify::event::ModifyKind;
use notify::event::RemoveKind;
use starlark_map::ordered_set::OrderedSet;
use tracing::debug;
use tracing::info;

use crate::file_watcher::FileWatcher;
use crate::file_watcher::FileWatcherImage;
use crate::file_watcher::FileWatcherImageKind;
use crate::file_watcher::ResumeInput;
use crate::file_watcher::SuspendVcs;
use crate::file_watcher::SyncOutcome;
use crate::fs_hash_crawler::FsSnapshot;
use crate::fs_hash_crawler::resume_scan;
use crate::mergebase::Mergebase;
use crate::stats::FileWatcherStats;

fn ignore_event_kind(event_kind: EventKind) -> bool {
    match event_kind {
        EventKind::Access(_) => true,
        // Only the access time can change without anything buck2 reads changing. `chmod` arrives
        // as `Metadata(Any)` from every notify backend, never as `Permissions`.
        EventKind::Modify(ModifyKind::Metadata(MetadataKind::AccessTime)) => true,
        _ => false,
    }
}

/// Buffer containing the events that have happened since we last got a message.
/// Used to dedupe events, since notify sends a notification on every change.
#[derive(Allocative)]
struct NotifyFileData {
    ignored: u64,
    #[allocative(skip)]
    events: OrderedSet<(CellPath, EventKind)>,
    /// Whether file system changes were missed
    missed_events: bool,
}

impl NotifyFileData {
    fn new() -> Self {
        Self {
            ignored: 0,
            events: OrderedSet::new(),
            missed_events: false,
        }
    }

    fn process(
        &mut self,
        event: notify::Result<notify::Event>,
        root: &ProjectRoot,
        cells: &CellResolver,
        ignore_specs: &StdBuckHashMap<CellName, IgnoreSet>,
    ) -> buck2_error::Result<()> {
        let event =
            event.map_err(|e| from_any_with_tag(e, buck2_error::ErrorTag::NotifyWatcher))?;

        // Checked before the path loop: the kernel-overflow rescan signal can arrive with no
        // paths attached (platform dependent), and a missed rescan means the daemon keeps
        // answering from a stale graph.
        if event.need_rescan() {
            self.missed_events = true;
            debug!("FileWatcher: File change events were missed");
        }

        for path in &event.paths {
            // Testing shows that we get absolute paths back from the `notify` library.
            // It's not documented though.
            let path = root.relativize(AbsNormPath::new(&path)?)?;

            // We ignore the buck-out prefix, as those are uninteresting events caused by us.
            // We also ignore other buck-out directories, as if you have two isolation dirs running at once, they are not interesting.
            // We do this in the notify-watcher, rather than a generic layer, as watchman users should configure
            // to ignore buck-out, to reduce the number of events, rather than hiding them later.
            if path.starts_with(InvocationPaths::buck_out_dir_prefix()) {
                // We don't want to event add them as ignored events, since they are super common
                // and very boring
                continue;
            }

            let cell_path = cells.get_cell_path(&path);
            let ignore = ignore_specs
                .get(&cell_path.cell())
                // See the comment on the analogous code in `watchman/interface.rs`
                .is_some_and(|ignore| ignore.is_match(cell_path.path()));

            info!(
                "FileWatcher: {:?} {:?} (ignore = {})",
                path, &event.kind, ignore
            );

            if ignore || ignore_event_kind(event.kind) {
                self.ignored += 1;
            } else {
                self.events.insert((cell_path, event.kind));
            }
        }
        Ok(())
    }

    fn sync(self) -> (buck2_data::FileWatcherStats, Option<FileChangeTracker>) {
        // The changes that go into the DICE transaction
        let mut changed = FileChangeTracker::new();
        // If we missed events, the sync reports a fresh instance and the daemon starts over.
        // Surface that to telemetry/UI the same way the watchman path does.
        let base = if self.missed_events {
            buck2_data::FileWatcherStats {
                fresh_instance: true,
                fresh_instance_data: Some(buck2_data::FreshInstance {
                    new_mergebase: false,
                    cleared_dice: true,
                    cleared_dep_files: false,
                }),
                incomplete_events_reason: Some(
                    "notify dropped events (kernel queue overflow)".to_owned(),
                ),
                ..Default::default()
            }
        } else {
            Default::default()
        };
        let mut stats = FileWatcherStats::new(base, self.events.len());
        stats.add_ignored(self.ignored);

        for (cell_path, event_kind) in self.events {
            let cell_path_str = cell_path.to_string();
            match event_kind {
                EventKind::Create(create_kind) => match create_kind {
                    CreateKind::File => {
                        changed.file_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str,
                            FileWatcherEventType::Create,
                            FileWatcherKind::File,
                        );
                    }
                    CreateKind::Folder => {
                        changed.dir_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str,
                            FileWatcherEventType::Create,
                            FileWatcherKind::Directory,
                        );
                    }
                    CreateKind::Any | CreateKind::Other => {
                        changed.file_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str.clone(),
                            FileWatcherEventType::Create,
                            FileWatcherKind::File,
                        );
                        changed.dir_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str,
                            FileWatcherEventType::Create,
                            FileWatcherKind::Directory,
                        );
                    }
                },
                EventKind::Modify(modify_kind) => match modify_kind {
                    ModifyKind::Data(_) | ModifyKind::Metadata(_) => {
                        changed.file_contents_changed(cell_path);
                        stats.add(
                            cell_path_str,
                            FileWatcherEventType::Modify,
                            FileWatcherKind::File,
                        );
                    }
                    ModifyKind::Name(_) | ModifyKind::Any | ModifyKind::Other => {
                        changed.file_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str.clone(),
                            FileWatcherEventType::Create,
                            FileWatcherKind::File,
                        );
                        stats.add(
                            cell_path_str.clone(),
                            FileWatcherEventType::Delete,
                            FileWatcherKind::File,
                        );
                        changed.dir_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str.clone(),
                            FileWatcherEventType::Create,
                            FileWatcherKind::Directory,
                        );
                        stats.add(
                            cell_path_str.clone(),
                            FileWatcherEventType::Delete,
                            FileWatcherKind::Directory,
                        );
                    }
                },
                EventKind::Remove(remove_kind) => match remove_kind {
                    RemoveKind::File => {
                        changed.file_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str,
                            FileWatcherEventType::Delete,
                            FileWatcherKind::File,
                        );
                    }
                    RemoveKind::Folder => {
                        changed.dir_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str,
                            FileWatcherEventType::Delete,
                            FileWatcherKind::Directory,
                        );
                    }
                    RemoveKind::Any | RemoveKind::Other => {
                        changed.file_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str.clone(),
                            FileWatcherEventType::Delete,
                            FileWatcherKind::File,
                        );
                        changed.dir_added_or_removed(cell_path);
                        stats.add(
                            cell_path_str,
                            FileWatcherEventType::Delete,
                            FileWatcherKind::Directory,
                        );
                    }
                },
                _ => {}
            }
        }

        let stats = stats.finish();
        let changed = if self.missed_events {
            None
        } else {
            Some(changed)
        };

        (stats, changed)
    }
}

#[derive(Allocative)]
pub struct NotifyFileWatcher {
    #[allocative(skip)]
    #[expect(unused)]
    // FIXME(JakobDegen): Clarify if this just needs to be kept alive or can be removed?
    watcher: RecommendedWatcher,
    data: Arc<Mutex<buck2_error::Result<NotifyFileData>>>,
    /// What the changes recorded on the updater are relative to.
    baseline: Mutex<FileSystemBaseline>,
    root: ProjectRoot,
    cells: CellResolver,
    #[allocative(skip)]
    ignore_specs: Arc<StdBuckHashMap<CellName, IgnoreSet>>,
    /// For a resumed daemon, what changed while no daemon ran, until the first sync records it.
    #[allocative(skip)]
    pending: Mutex<Option<FileChangeTracker>>,
    /// The cookie a suspend waits for: once its event arrives, every event the kernel queued
    /// before it has been processed.
    #[allocative(skip)]
    cookie: Arc<Mutex<Option<(PathBuf, std::sync::mpsc::Sender<()>)>>>,
}

impl NotifyFileWatcher {
    pub fn new(
        root: &ProjectRoot,
        cells: CellResolver,
        ignore_specs: StdBuckHashMap<CellName, IgnoreSet>,
    ) -> buck2_error::Result<Self> {
        Self::with_baseline(root, cells, ignore_specs, FileSystemBaseline::unique())
    }

    fn with_baseline(
        root: &ProjectRoot,
        cells: CellResolver,
        ignore_specs: StdBuckHashMap<CellName, IgnoreSet>,
        baseline: FileSystemBaseline,
    ) -> buck2_error::Result<Self> {
        let data = Arc::new(Mutex::new(Ok(NotifyFileData::new())));
        let data2 = data.dupe();
        let root2 = root.dupe();
        let cells2 = cells.dupe();
        let ignore_specs = Arc::new(ignore_specs);
        let ignore_specs2 = ignore_specs.dupe();
        let cookie: Arc<Mutex<Option<(PathBuf, std::sync::mpsc::Sender<()>)>>> =
            Arc::new(Mutex::new(None));
        let cookie2 = cookie.dupe();
        let mut watcher =
            notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
                if let Ok(e) = &event {
                    let mut cookie = cookie2.lock().unwrap();
                    if let Some((path, _)) = &*cookie
                        && e.paths.iter().any(|p| p == path)
                    {
                        let (_, sender) = cookie.take().expect("checked above");
                        drop(sender.send(()));
                    }
                }
                let mut guard = data2.lock().unwrap();
                if let Ok(state) = &mut *guard {
                    if let Err(e) = state.process(event, &root2, &cells2, &ignore_specs2) {
                        *guard = Err(e);
                    }
                }
            })
            .map_err(|e| from_any_with_tag(e, buck2_error::ErrorTag::NotifyWatcher))?;
        watcher
            .watch(root.root().as_path(), notify::RecursiveMode::Recursive)
            .map_err(|e| from_any_with_tag(e, buck2_error::ErrorTag::NotifyWatcher))?;
        Ok(Self {
            watcher,
            data,
            baseline: Mutex::new(baseline),
            root: root.dupe(),
            cells,
            ignore_specs,
            pending: Mutex::new(None),
            cookie,
        })
    }

    /// A watcher for a resumed daemon. The watch is registered before [`resume_scan`] stats
    /// the files and asks git, so a change made during the comparison is reported by the
    /// comparison, by an event, or by both, and never by neither. The first sync records both.
    pub fn resume(
        root: &ProjectRoot,
        cells: CellResolver,
        ignore_specs: StdBuckHashMap<CellName, IgnoreSet>,
        input: ResumeInput<'_>,
    ) -> buck2_error::Result<Self> {
        let watcher = Self::with_baseline(
            root,
            cells.dupe(),
            ignore_specs,
            FileSystemBaseline::from_image(&input.image.baseline),
        )?;
        let scan = resume_scan(root, &cells, &watcher.ignore_specs, input)?;
        let (_now, changes) = scan.into_dice(&watcher.ignore_specs);
        *watcher.pending.lock().unwrap() = Some(changes);
        Ok(watcher)
    }

    fn sync2(
        &self,
        mut dice: DiceTransactionUpdater,
    ) -> buck2_error::Result<(buck2_data::FileWatcherStats, DiceTransactionUpdater)> {
        let old = {
            let mut guard = self.data.lock().unwrap();
            mem::replace(&mut *guard, Ok(NotifyFileData::new()))
        };
        let (stats, changes) = old?.sync();
        let pending = self.pending.lock().unwrap().take();
        let mut baseline = self.baseline.lock().unwrap();
        if let Some(mut changes) = changes {
            if let Some(pending) = pending {
                changes.merge(pending);
            }
            changes.write_to_dice(&mut dice)?;
        } else {
            // Events were missed; the fresh instance reported in `stats` has the daemon start
            // over.
            *baseline = FileSystemBaseline::unique();
        }
        dice.set_file_system_baseline(baseline.dupe())?;
        Ok((stats, dice))
    }

    /// Waits until every event queued before now has been processed: writes a cookie file and
    /// waits for its event, which inotify delivers after every earlier one on the same queue.
    fn barrier(&self, project_root: &ProjectRoot) -> buck2_error::Result<()> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = project_root
            .root()
            .as_path()
            .join(InvocationPaths::buck_out_dir_prefix().as_str());
        let path = dir.join(format!(
            ".suspend-cookie-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let (sender, receiver) = std::sync::mpsc::channel();
        *self.cookie.lock().unwrap() = Some((path.clone(), sender));
        std::fs::write(&path, b"")
            .map_err(|e| from_any_with_tag(e, buck2_error::ErrorTag::NotifyWatcher))?;
        let arrived = receiver.recv_timeout(std::time::Duration::from_secs(30));
        drop(std::fs::remove_file(&path));
        *self.cookie.lock().unwrap() = None;
        arrived.map_err(|_| {
            buck2_error::buck2_error!(
                buck2_error::ErrorTag::NotifyWatcher,
                "the file watcher did not see its cookie `{}` within 30s",
                path.display()
            )
        })
    }
}

#[async_trait]
impl FileWatcher for NotifyFileWatcher {
    /// Takes the snapshot `S` first, then waits for every event queued so far. An edit made
    /// before `S` statted a file produced such an event; since the last sync's changes are
    /// committed, any event means `S` and the graph disagree, and the suspend fails rather than
    /// lose it. An edit made after `S` statted a file differs from `S`, so the resumed watcher's
    /// comparison reports it.
    async fn suspend_image(
        &self,
        project_root: &ProjectRoot,
        vcs: SuspendVcs<'_>,
    ) -> buck2_error::Result<FileWatcherImage> {
        let snapshot = tokio::task::block_in_place(|| {
            let snapshot = FsSnapshot::build(&self.root, &self.cells, &self.ignore_specs)?;
            self.barrier(project_root)?;
            buck2_error::Ok(snapshot)
        })?;
        {
            let guard = self.data.lock().unwrap();
            let data = match &*guard {
                Ok(data) => data,
                Err(e) => return Err(e.clone()),
            };
            if data.missed_events {
                return Err(buck2_error::buck2_error!(
                    buck2_error::ErrorTag::NotifyWatcher,
                    "the file watcher missed events since the last command; run a build, then \
                     `buck2 suspend` again"
                ));
            }
            if !data.events.is_empty() {
                let paths: Vec<String> = data
                    .events
                    .iter()
                    .take(5)
                    .map(|(p, _)| p.to_string())
                    .collect();
                return Err(buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Input,
                    "{} file(s) changed since the last command ({}); run a build, then \
                     `buck2 suspend` again",
                    data.events.len(),
                    paths.join(", ")
                ));
            }
        }
        // The contents after the check: a suspend that fails it reads nothing. A file edited
        // since `S` statted it gets no digest, which the read's stat comparison decides.
        let (snapshot, vcs) =
            tokio::task::block_in_place(|| snapshot.suspend_image(&self.root, &self.cells, vcs))?;
        Ok(FileWatcherImage {
            kind: FileWatcherImageKind::Notify,
            baseline: self.baseline.lock().unwrap().to_image(),
            snapshot,
            vcs,
        })
    }

    async fn sync(
        &self,
        dice: DiceTransactionUpdater,
    ) -> buck2_error::Result<(DiceTransactionUpdater, Mergebase, SyncOutcome)> {
        span_async(
            buck2_data::FileWatcherStart {
                provider: buck2_data::FileWatcherProvider::RustNotify as i32,
            },
            async {
                let (stats, res) = match self.sync2(dice) {
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

#[cfg(test)]
mod tests {
    use buck2_core::cells::cell_root_path::CellRootPathBuf;
    use buck2_core::cells::name::CellName;
    use buck2_core::fs::project_rel_path::ProjectRelativePath;
    use buck2_fs::fs_util::uncategorized as fs_util;
    use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;
    use notify::event::CreateKind;
    use notify::event::Flag;

    use super::*;

    fn fixture() -> (
        ProjectRoot,
        CellResolver,
        StdBuckHashMap<CellName, IgnoreSet>,
        tempfile::TempDir,
    ) {
        let cells = CellResolver::testing_with_name_and_path(
            CellName::testing_new("root"),
            CellRootPathBuf::testing_new(""),
        );
        let tempdir = tempfile::tempdir().unwrap();
        let root_path =
            fs_util::canonicalize(AbsNormPathBuf::new(tempdir.path().to_owned()).unwrap()).unwrap();
        let root = ProjectRoot::new(root_path).unwrap();
        (root, cells, StdBuckHashMap::default(), tempdir)
    }

    /// The kernel-overflow signal can arrive as a rescan-flagged event with no paths attached
    /// (platform dependent). The flag must still be recorded, otherwise lost notifications go
    /// undetected and the daemon keeps answering from a stale graph, which is the cache
    /// inconsistency the missed-events handling exists to close.
    #[test]
    fn rescan_event_with_no_paths_sets_missed_events() {
        let (root, cells, ignores, _t) = fixture();
        let mut state = NotifyFileData::new();
        let event = notify::Event::new(EventKind::Any).set_flag(Flag::Rescan);
        state.process(Ok(event), &root, &cells, &ignores).unwrap();
        assert!(
            state.missed_events,
            "a pathless rescan event must set missed_events"
        );
    }

    #[test]
    fn rescan_event_with_paths_sets_missed_events() {
        let (root, cells, ignores, _t) = fixture();
        let mut state = NotifyFileData::new();
        let path = root.resolve(ProjectRelativePath::new("f").unwrap());
        let event = notify::Event::new(EventKind::Create(CreateKind::File))
            .add_path(path.into_abs_path_buf().into_path_buf())
            .set_flag(Flag::Rescan);
        state.process(Ok(event), &root, &cells, &ignores).unwrap();
        assert!(state.missed_events);
    }

    /// Missed events: the sync result carries no tracker (the daemon starts over) and the stats
    /// surface the fresh instance the same way watchman's path does.
    #[test]
    fn sync_with_missed_events_reports_fresh_instance_and_drops_changes() {
        let mut state = NotifyFileData::new();
        state.missed_events = true;
        let (stats, changes) = state.sync();
        assert!(changes.is_none(), "missed events must drop the tracker");
        assert!(stats.fresh_instance);
        let fresh = stats
            .fresh_instance_data
            .expect("fresh instance data populated");
        assert!(fresh.cleared_dice);
        assert!(stats.incomplete_events_reason.is_some());
    }

    #[test]
    fn sync_without_missed_events_returns_changes() {
        let (stats, changes) = NotifyFileData::new().sync();
        assert!(
            changes.is_some(),
            "no missed events: incremental tracker must survive"
        );
        assert!(!stats.fresh_instance);
        assert!(stats.incomplete_events_reason.is_none());
    }

    /// `chmod` arrives from every notify backend as `Modify(Metadata(Any))` (inotify
    /// `IN_ATTRIB`, FSEvents `INODE_META_MOD`, kqueue `NOTE_ATTRIB`); no backend emits
    /// `Permissions`. Only access events and access-time changes can be dropped.
    #[test]
    fn test_metadata_any_is_not_ignored() {
        assert!(!ignore_event_kind(EventKind::Modify(ModifyKind::Metadata(
            MetadataKind::Any
        ))));
        assert!(!ignore_event_kind(EventKind::Modify(ModifyKind::Metadata(
            MetadataKind::Permissions
        ))));
        assert!(ignore_event_kind(EventKind::Modify(ModifyKind::Metadata(
            MetadataKind::AccessTime
        ))));
        assert!(ignore_event_kind(EventKind::Access(
            notify::event::AccessKind::Read
        )));
    }
}
