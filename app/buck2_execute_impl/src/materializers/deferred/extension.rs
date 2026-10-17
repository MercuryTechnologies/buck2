/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Write;
use std::sync::Arc;
use std::time::Instant;

use allocative::Allocative;
use allocative::FlameGraphBuilder;
use allocative::Visitor;
use allocative::ident_key;
use buck2_core::execution_types::executor_config::RemoteExecutorUseCase;
use buck2_core::fs::project_rel_path::ProjectRelativePathBuf;
use buck2_directory::directory::immutable_directory::ImmutableDirectory;
use buck2_error::BuckErrorContext;
use buck2_events::dispatch::EventDispatcher;
use buck2_events::dispatch::get_dispatcher;
use buck2_events::dispatch::get_dispatcher_opt;
use buck2_execute::artifact_value::ArtifactValue;
use buck2_execute::materialize::materializer::CasDownloadInfo;
use buck2_execute::materialize::materializer::CleanStaleArtifactsArgs;
use buck2_execute::materialize::materializer::CleanStaleArtifactsPolicy;
use buck2_execute::materialize::materializer::CopiedArtifact;
use buck2_execute::materialize::materializer::MaterializerEntry;
use buck2_execute::materialize::materializer::MaterializerIterItem;
use buck2_execute::materialize::materializer::ResumedArtifacts;
use buck2_execute::materialize::materializer::SuspendedArtifact;
use buck2_execute::materialize::materializer::SuspendedArtifactState;
use buck2_execute::materialize::materializer::SuspendedCopy;
use buck2_fs::error::IoResultExt;
use buck2_fs::fs_util;
use derivative::Derivative;
use dupe::Dupe;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::stream::TryStreamExt;
use jiff::SignedDuration;
use jiff::Timestamp;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::materializers::deferred::ArtifactMaterializationMethod;
use crate::materializers::deferred::ArtifactMaterializationStage;
use crate::materializers::deferred::DeferredMaterializerAccessor;
use crate::materializers::deferred::DeferredMaterializerCommandProcessor;
use crate::materializers::deferred::MaterializerCommand;
use crate::materializers::deferred::ProcessingFuture;
use crate::materializers::deferred::WriteFile;
use crate::materializers::deferred::artifact_tree::ArtifactTree;
use crate::materializers::deferred::artifact_tree::artifact_metadata_matches_entry;
use crate::materializers::deferred::artifact_tree::artifact_metadata_size;
use crate::materializers::deferred::clean_stale::CleanScratchExtensionCommand;
use crate::materializers::deferred::clean_stale::CleanStaleArtifactsCommand;
use crate::materializers::deferred::clean_stale::CleanStaleArtifactsExtensionCommand;
use crate::materializers::deferred::clean_stale::CleanStaleArtifactsExtensionCommandKind;
use crate::materializers::deferred::command_processor::ResumeDiskProbe;
use crate::materializers::deferred::io_handler::IoHandler;
use crate::materializers::deferred::io_handler::create_ttl_refresh;

pub(super) trait ExtensionCommand<T>: Debug + Sync + Send + 'static {
    fn execute(self: Box<Self>, processor: &mut DeferredMaterializerCommandProcessor<T>);
}

struct AllocativeProfileRoot<'a> {
    artifact_tree: &'a ArtifactTree,
}

impl Allocative for AllocativeProfileRoot<'_> {
    fn visit<'a, 'b: 'a>(&self, visitor: &'a mut Visitor<'b>) {
        let mut visitor = visitor.enter_self_sized::<Self>();
        visitor.visit_field(
            ident_key!(artifact_tree),
            &self.artifact_tree.allocative_dfs(),
        );
        visitor.exit();
    }
}

#[derive(Debug)]
struct PathData {
    stage: PathStage,
    processing: PathProcessing,
}

#[derive(Debug)]
enum PathStage {
    Materialized { ts: Timestamp, size: Option<u64> },
    Declared(Arc<ArtifactMaterializationMethod>),
}

#[derive(Debug)]
enum PathProcessing {
    Done,
    Materializing,
    Cleaning,
}

impl Display for PathData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.stage {
            PathStage::Materialized { ts, size } => {
                if let Some(size) = size {
                    write!(f, "materialized (ts={ts:?}, size={size})")?;
                } else {
                    write!(f, "materialized (ts={ts:?})")?;
                }
            }
            PathStage::Declared(method) => {
                write!(f, "declared: {method}")?;
            }
        }

        match &self.processing {
            PathProcessing::Done => {}
            PathProcessing::Materializing => {
                write!(f, " (materializing")?;
            }
            PathProcessing::Cleaning => {
                write!(f, " (cleaning)")?;
            }
        }

        Ok(())
    }
}

impl MaterializerEntry for PathData {}

#[derive(Derivative)]
#[derivative(Debug)]
struct Iterate {
    /// This is for debug commands so we use an unbounded channel to avoid locking up the
    /// materializer command thread.
    #[derivative(Debug = "ignore")]
    sender: UnboundedSender<MaterializerIterItem>,
}

impl<T: IoHandler> ExtensionCommand<T> for Iterate {
    fn execute(self: Box<Self>, processor: &mut DeferredMaterializerCommandProcessor<T>) {
        // Ensure up to date access times
        processor.flush_access_times();
        for (path, data) in processor.tree.iter_with_paths() {
            let stage = match &data.stage {
                ArtifactMaterializationStage::Declared { method, .. } => {
                    PathStage::Declared(method.dupe())
                }
                ArtifactMaterializationStage::Materialized {
                    last_access_time,
                    metadata,
                    ..
                } => {
                    // drop nano-seconds
                    let ts = Timestamp::from_second(last_access_time.as_second())
                        .expect("whole seconds of a valid timestamp are in range");
                    PathStage::Materialized {
                        ts,
                        size: Some(artifact_metadata_size(metadata)),
                    }
                }
            };

            let processing = match data.processing.active_ref() {
                None => PathProcessing::Done,
                Some(active) => match &active.future {
                    ProcessingFuture::Materializing(..) => PathProcessing::Materializing,
                    ProcessingFuture::Cleaning(..) => PathProcessing::Cleaning,
                },
            };

            let path_data = PathData { stage, processing };

            let path = ProjectRelativePathBuf::from(path);

            let deps = match &data.deps {
                Some(deps) => processor.tree.find_artifacts_for_debug(deps),
                None => Vec::new(),
            };

            match self.sender.send(MaterializerIterItem {
                artifact_path: path,
                artifact_display: Box::new(path_data) as _,
                deps,
            }) {
                Ok(..) => {}
                Err(..) => break, // No use sending more if the client disconnected.
            }
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
struct AllocativeProfile {
    #[derivative(Debug = "ignore")]
    sender: Sender<allocative::FlameGraphOutput>,
}

impl<T> ExtensionCommand<T> for AllocativeProfile {
    fn execute(self: Box<Self>, processor: &mut DeferredMaterializerCommandProcessor<T>) {
        let mut graph = FlameGraphBuilder::default();
        graph.visit_root(&AllocativeProfileRoot {
            artifact_tree: &processor.tree,
        });
        let _ignored = self.sender.send(graph.finish());
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
struct Fsck {
    /// This is for debug commands so we use an unbounded channel to avoid locking up the
    /// materializer command thread.
    #[derivative(Debug = "ignore")]
    sender: UnboundedSender<(ProjectRelativePathBuf, buck2_error::Error)>,
}

impl<T: IoHandler> ExtensionCommand<T> for Fsck {
    fn execute(self: Box<Self>, processor: &mut DeferredMaterializerCommandProcessor<T>) {
        for (path, data) in processor.tree.iter_with_paths() {
            match &data.stage {
                ArtifactMaterializationStage::Declared { .. } => {
                    continue;
                }
                ArtifactMaterializationStage::Materialized { .. } => {}
            };

            // We actually block the thread here. This is to ensure we don't try to delete things
            // when we check them. This is primarily a debug command. We don't run this while
            // actual things are in flight.

            let path = ProjectRelativePathBuf::from(path);
            let res =
                fs_util::symlink_metadata(processor.io.fs().resolve(&path)).categorize_internal();
            match res {
                Ok(..) => {}
                Err(e) => {
                    let _ignored = self.sender.send((path, e.into()));
                }
            }
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
struct RefreshTtls {
    sender: Sender<Option<JoinHandle<buck2_error::Result<()>>>>,
    min_ttl: i64,
}

impl<T: IoHandler> ExtensionCommand<T> for RefreshTtls {
    fn execute(self: Box<Self>, processor: &mut DeferredMaterializerCommandProcessor<T>) {
        let dispatcher = get_dispatcher_opt().unwrap_or_else(EventDispatcher::error_on_event);
        let task = create_ttl_refresh(
            &processor.tree,
            processor.io.re_client_manager(),
            SignedDuration::from_secs(self.min_ttl),
            processor.io.digest_config(),
        )
        .map(|f| processor.spawn(&dispatcher, f));
        let _ignored = self.sender.send(task);
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
struct GetTtlRefreshLog {
    sender: Sender<String>,
}

impl<T: IoHandler> ExtensionCommand<T> for GetTtlRefreshLog {
    fn execute(self: Box<Self>, processor: &mut DeferredMaterializerCommandProcessor<T>) {
        // We normally poll this very lazily, so actually force it to happen here.
        processor.poll_current_ttl_refresh();

        let mut out = String::new();

        for entry in &processor.ttl_refresh_history {
            write!(&mut out, "{:?}\t", entry.at).unwrap();
            match &entry.outcome {
                None => {
                    writeln!(&mut out, "SKIP").unwrap();
                }
                Some(Ok(())) => {
                    writeln!(&mut out, "OK").unwrap();
                }
                Some(Err(e)) => {
                    writeln!(&mut out, "ERR\t{e:#}").unwrap();
                }
            }
        }

        let _ignored = self.sender.send(out);
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
struct TestIter {
    sender: Sender<String>,
    count: usize,
}

impl<T> ExtensionCommand<T> for TestIter {
    fn execute(self: Box<Self>, processor: &mut DeferredMaterializerCommandProcessor<T>) {
        let mut out = String::new();

        let now = std::time::Instant::now();

        for _i in 0..self.count {
            let it = processor.tree.iter_without_paths();

            for e in it {
                let _e = e;
            }
        }

        writeln!(
            &mut out,
            "Elapsed for iter() ({} times): {:?}",
            self.count,
            Instant::now() - now
        )
        .unwrap();

        let now = std::time::Instant::now();

        for _i in 0..self.count {
            let it = processor.tree.iter_with_paths();

            for e in it {
                let _e = e;
            }
        }

        writeln!(
            &mut out,
            "Elapsed for iter().with_paths() ({} times): {:?}",
            self.count,
            Instant::now() - now
        )
        .unwrap();

        let _ignored = self.sender.send(out);
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
struct FlushAccessTimes {
    sender: Sender<String>,
}

impl<T: IoHandler> ExtensionCommand<T> for FlushAccessTimes {
    fn execute(self: Box<Self>, processor: &mut DeferredMaterializerCommandProcessor<T>) {
        let mut out = String::new();

        writeln!(&mut out, "{}", processor.flush_access_times()).unwrap();
        let _ignored = self.sender.send(out);
    }
}

/// Every artifact the running daemon declared, for `buck2 suspend`.
#[derive(Derivative)]
#[derivative(Debug)]
struct SuspendArtifacts {
    #[derivative(Debug = "ignore")]
    sender: Sender<buck2_error::Result<Vec<SuspendedArtifact>>>,
}

impl<T: IoHandler> ExtensionCommand<T> for SuspendArtifacts {
    fn execute(self: Box<Self>, processor: &mut DeferredMaterializerCommandProcessor<T>) {
        let _ignored = self.sender.send(suspend_artifacts(processor));
    }
}

fn suspend_artifacts<T: IoHandler>(
    processor: &mut DeferredMaterializerCommandProcessor<T>,
) -> buck2_error::Result<Vec<SuspendedArtifact>> {
    if processor.sqlite_db.is_none() {
        return Err(buck2_error::buck2_error!(
            buck2_error::ErrorTag::Input,
            "`buck2 suspend` needs `buck2.sqlite_materializer_state = true`: without it a \
             restarted materializer knows nothing of what is on disk"
        ));
    }
    // What a resumed materializer reads back from sqlite carries the access times as of now.
    processor.flush_access_times();
    let mut out = Vec::new();
    for (path, data) in processor.tree.iter_with_paths() {
        let path = ProjectRelativePathBuf::from(path);
        let state = match &data.stage {
            ArtifactMaterializationStage::Materialized { active: false, .. } => continue,
            ArtifactMaterializationStage::Materialized {
                active: true,
                metadata,
                rematerialization_method,
                ..
            } => SuspendedArtifactState::Materialized {
                entry: metadata.dupe(),
                cas: rematerialization_method.as_ref().and_then(|m| {
                    match m.materialization_method().as_ref() {
                        ArtifactMaterializationMethod::CasDownload { info } => {
                            Some(info.to_suspended())
                        }
                        _ => None,
                    }
                }),
            },
            ArtifactMaterializationStage::Declared { entry, method } => match method.as_ref() {
                ArtifactMaterializationMethod::CasDownload { info } => {
                    let (origin, re_use_case) = info.to_suspended();
                    SuspendedArtifactState::CasDownload {
                        entry: entry.dupe(),
                        origin,
                        re_use_case,
                    }
                }
                ArtifactMaterializationMethod::LocalCopy(_, copied) => {
                    SuspendedArtifactState::LocalCopy {
                        entry: entry.dupe(),
                        srcs: copied
                            .iter()
                            .map(|c| SuspendedCopy {
                                src: c.src.clone(),
                                dest: c.dest.clone(),
                                dest_entry: c
                                    .dest_entry
                                    .clone()
                                    .map_dir(|d| d.shared(&*buck2_execute::directory::INTERNER)),
                                executable_bit_override: c.executable_bit_override,
                            })
                            .collect(),
                    }
                }
                ArtifactMaterializationMethod::Write(write) => SuspendedArtifactState::Write {
                    entry: entry.dupe(),
                    compressed: write.compressed_data.to_vec(),
                    decompressed_size: write.decompressed_size as u64,
                    is_executable: write.is_executable,
                },
                #[cfg(test)]
                ArtifactMaterializationMethod::Test => continue,
            },
        };
        out.push(SuspendedArtifact {
            path,
            deps: data.deps.dupe(),
            state,
        });
    }
    Ok(out)
}

/// What a suspended daemon's materializer tracked, replayed at startup: materialized artifacts
/// marked active again, declared ones declared again, through the same `declare` a running
/// daemon uses, which keeps an artifact already on disk as it is.
#[derive(Derivative)]
#[derivative(Debug)]
struct ResumeArtifacts {
    #[derivative(Debug = "ignore")]
    artifacts: Vec<SuspendedArtifact>,
    #[derivative(Debug = "ignore")]
    sender: Sender<buck2_error::Result<ResumedArtifacts>>,
}

impl<T: IoHandler> ExtensionCommand<T> for ResumeArtifacts {
    fn execute(self: Box<Self>, processor: &mut DeferredMaterializerCommandProcessor<T>) {
        let result = resume_artifacts(processor, self.artifacts);
        let _ignored = self.sender.send(result);
    }
}

fn resume_artifacts<T: IoHandler>(
    processor: &mut DeferredMaterializerCommandProcessor<T>,
    artifacts: Vec<SuspendedArtifact>,
) -> buck2_error::Result<ResumedArtifacts> {
    let dispatcher = EventDispatcher::null();
    let mut resumed = ResumedArtifacts::default();
    let mut probe = ResumeDiskProbe::default();
    for SuspendedArtifact { path, deps, state } in artifacts {
        let (entry, method) = match state {
            SuspendedArtifactState::Materialized { entry, cas } => {
                // The persisted state vouches for what is on disk: the artifact is still there
                // only if the state has this very entry at this path.
                let mut path_iter = path.iter();
                if let Some(data) = processor.tree.prefix_get_mut(&mut path_iter)
                    && path_iter.next().is_none()
                    && let ArtifactMaterializationStage::Materialized {
                        active, metadata, ..
                    } = &mut data.stage
                    && artifact_metadata_matches_entry(metadata, &entry)
                {
                    *active = true;
                    data.deps = deps;
                    resumed.reactivated += 1;
                    continue;
                }
                let Some((origin, re_use_case)) = cas else {
                    resumed.lost += 1;
                    continue;
                };
                resumed.refetch += 1;
                (
                    entry,
                    ArtifactMaterializationMethod::CasDownload {
                        info: Arc::new(CasDownloadInfo::from_suspended(&origin, re_use_case)?),
                    },
                )
            }
            SuspendedArtifactState::CasDownload {
                entry,
                origin,
                re_use_case,
            } => {
                resumed.redeclared += 1;
                (
                    entry,
                    ArtifactMaterializationMethod::CasDownload {
                        info: Arc::new(CasDownloadInfo::from_suspended(&origin, re_use_case)?),
                    },
                )
            }
            SuspendedArtifactState::LocalCopy { entry, srcs } => {
                let copied: Vec<CopiedArtifact> = srcs
                    .into_iter()
                    .map(|c| CopiedArtifact {
                        src: c.src,
                        dest: c.dest,
                        dest_entry: c.dest_entry.map_dir(ImmutableDirectory::Shared),
                        executable_bit_override: c.executable_bit_override,
                    })
                    .collect();
                let tree = crate::materializers::deferred::copy_srcs_tree(&path, &copied)?;
                resumed.redeclared += 1;
                (
                    entry,
                    ArtifactMaterializationMethod::LocalCopy(tree, copied),
                )
            }
            SuspendedArtifactState::Write {
                entry,
                compressed,
                decompressed_size,
                is_executable,
            } => {
                resumed.redeclared += 1;
                (
                    entry,
                    ArtifactMaterializationMethod::Write(Arc::new(WriteFile {
                        compressed_data: compressed.into_boxed_slice(),
                        decompressed_size: decompressed_size as usize,
                        is_executable,
                    })),
                )
            }
        };
        processor.declare_for_resume(
            &path,
            ArtifactValue::new(entry, deps),
            Box::new(method),
            &dispatcher,
            &mut probe,
        );
    }
    Ok(resumed)
}

impl<T: IoHandler> DeferredMaterializerAccessor<T> {
    pub(super) async fn suspend_artifacts_impl(
        &self,
        portable: bool,
    ) -> buck2_error::Result<Vec<SuspendedArtifact>> {
        let (sender, receiver) = oneshot::channel();
        self.command_sender.send(MaterializerCommand::Extension(
            Box::new(SuspendArtifacts { sender }) as _,
            get_dispatcher_opt(),
        ))?;
        let mut artifacts = receiver
            .await
            .buck_error_context("No response from materializer")??;
        if portable {
            self.upload_for_portable_suspend(&mut artifacts).await?;
        }
        Ok(artifacts)
    }

    /// Uploads every materialized artifact that has no CAS recipe, verifying each file, and
    /// records the upload as its recipe. Nothing runs while a suspend does, so the artifacts
    /// stay as the materializer listed them.
    async fn upload_for_portable_suspend(
        &self,
        artifacts: &mut [SuspendedArtifact],
    ) -> buck2_error::Result<()> {
        const CONCURRENT_UPLOADS: usize = 32;
        let info = Arc::new(CasDownloadInfo::new_uploaded(
            RemoteExecutorUseCase::buck2_default(),
        ));
        let uploads: Vec<_> = artifacts
            .iter()
            .enumerate()
            .filter_map(|(i, a)| match &a.state {
                SuspendedArtifactState::Materialized { entry, cas: None } => {
                    Some((i, a.path.clone(), entry.dupe()))
                }
                _ => None,
            })
            .collect();
        let uploaded: Vec<usize> =
            futures::stream::iter(uploads.into_iter().map(|(i, path, entry)| {
                let io = self.io.dupe();
                let info = info.dupe();
                async move {
                    io.upload_materialized_artifact(path.clone(), entry, info, true)
                        .await
                        .with_buck_error_context(|| {
                            format!("uploading `{path}` for `buck2 suspend --portable`")
                        })?;
                    buck2_error::Ok(i)
                }
            }))
            .buffer_unordered(CONCURRENT_UPLOADS)
            .try_collect()
            .await?;
        for i in uploaded {
            if let SuspendedArtifactState::Materialized { cas, .. } = &mut artifacts[i].state {
                *cas = Some(info.to_suspended());
            }
        }
        Ok(())
    }

    pub(super) async fn resume_artifacts_impl(
        &self,
        artifacts: Vec<SuspendedArtifact>,
    ) -> buck2_error::Result<ResumedArtifacts> {
        let (sender, receiver) = oneshot::channel();
        self.command_sender.send(MaterializerCommand::Extension(
            Box::new(ResumeArtifacts { artifacts, sender }) as _,
            get_dispatcher_opt(),
        ))?;
        receiver
            .await
            .buck_error_context("No response from materializer")?
    }

    pub(super) fn iterate_impl(
        &self,
    ) -> buck2_error::Result<BoxStream<'static, MaterializerIterItem>> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.command_sender.send(MaterializerCommand::Extension(
            Box::new(Iterate { sender }) as _,
            get_dispatcher_opt(),
        ))?;
        Ok(UnboundedReceiverStream::new(receiver).boxed())
    }

    pub(super) async fn allocative_impl(
        &self,
    ) -> buck2_error::Result<allocative::FlameGraphOutput> {
        let (sender, receiver) = oneshot::channel();
        self.command_sender.send(MaterializerCommand::Extension(
            Box::new(AllocativeProfile { sender }) as _,
            get_dispatcher_opt(),
        ))?;
        receiver
            .await
            .buck_error_context("No response from materializer")
    }

    pub(super) fn fsck_impl(
        &self,
    ) -> buck2_error::Result<BoxStream<'static, (ProjectRelativePathBuf, buck2_error::Error)>> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.command_sender.send(MaterializerCommand::Extension(
            Box::new(Fsck { sender }) as _,
            get_dispatcher_opt(),
        ))?;
        Ok(UnboundedReceiverStream::new(receiver).boxed())
    }

    pub(super) async fn refresh_ttls_impl(&self, min_ttl: i64) -> buck2_error::Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.command_sender.send(MaterializerCommand::Extension(
            Box::new(RefreshTtls { sender, min_ttl }) as _,
            get_dispatcher_opt(),
        ))?;
        if let Some(task) = receiver
            .await
            .buck_error_context("No response from materializer")?
        {
            task.await
                .buck_error_context("Refresh task aborted")?
                .buck_error_context("Refresh failed")?;
        }
        Ok(())
    }

    pub(super) async fn get_ttl_refresh_log_impl(&self) -> buck2_error::Result<String> {
        let (sender, receiver) = oneshot::channel();
        self.command_sender.send(MaterializerCommand::Extension(
            Box::new(GetTtlRefreshLog { sender }) as _,
            get_dispatcher_opt(),
        ))?;
        receiver
            .await
            .buck_error_context("No response from materializer")
    }

    pub(super) async fn clean_stale_artifacts_impl(
        &self,
        args: CleanStaleArtifactsArgs,
    ) -> buck2_error::Result<buck2_cli_proto::CleanStaleResponse> {
        let dispatcher = get_dispatcher();
        let kind = match args.policy {
            CleanStaleArtifactsPolicy::Configured => {
                CleanStaleArtifactsExtensionCommandKind::Configured {
                    dispatcher,
                    dry_run: args.dry_run,
                    tracked_only: args.tracked_only,
                }
            }
            CleanStaleArtifactsPolicy::Explicit {
                keep_since_time,
                adaptive_low_disk_threshold,
                adaptive_min_ttl,
                adaptive_unmaterialize_active,
            } => CleanStaleArtifactsExtensionCommandKind::Explicit(
                CleanStaleArtifactsCommand::explicit(
                    keep_since_time,
                    args.dry_run,
                    args.tracked_only,
                    adaptive_low_disk_threshold,
                    adaptive_min_ttl,
                    adaptive_unmaterialize_active,
                    dispatcher,
                ),
            ),
        };
        let (sender, recv) = oneshot::channel();
        self.command_sender.send(MaterializerCommand::Extension(
            Box::new(CleanStaleArtifactsExtensionCommand { kind, sender }),
            get_dispatcher_opt(),
        ))?;
        recv.await?
            .await
            .map(|res| res.into())
            .map_err(|failure| failure.into_error())
    }

    pub(super) async fn clean_scratch_impl(
        &self,
    ) -> buck2_error::Result<buck2_cli_proto::CleanStaleResponse> {
        let (sender, recv) = oneshot::channel();
        self.command_sender.send(MaterializerCommand::Extension(
            Box::new(CleanScratchExtensionCommand { sender }),
            get_dispatcher_opt(),
        ))?;
        recv.await?.await.map(|res| res.into())
    }

    pub(super) async fn test_iter_impl(&self, count: usize) -> buck2_error::Result<String> {
        let (sender, receiver) = oneshot::channel();
        self.command_sender.send(MaterializerCommand::Extension(
            Box::new(TestIter { sender, count }) as _,
            get_dispatcher_opt(),
        ))?;
        receiver
            .await
            .buck_error_context("No response from materializer")
    }

    pub(super) async fn flush_all_access_times_impl(&self) -> buck2_error::Result<String> {
        let (sender, receiver) = oneshot::channel();
        self.command_sender.send(MaterializerCommand::Extension(
            Box::new(FlushAccessTimes { sender }) as _,
            get_dispatcher_opt(),
        ))?;
        receiver
            .await
            .buck_error_context("No response from materializer")
    }
}
