/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

mod bundle;

use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use buck2_cli_proto::HydrationRequest;
use buck2_cli_proto::HydrationSubcommand;
use buck2_client_ctx::client_ctx::ClientCommandContext;
use buck2_client_ctx::common::BuckArgMatches;
use buck2_client_ctx::common::CommonBuildConfigurationOptions;
use buck2_client_ctx::common::CommonEventLogOptions;
use buck2_client_ctx::common::CommonStarlarkOptions;
use buck2_client_ctx::common::ui::CommonConsoleOptions;
use buck2_client_ctx::daemon::client::BuckdClientConnector;
use buck2_client_ctx::daemon::client::BuckdLifecycleLock;
use buck2_client_ctx::daemon::client::NoPartialResultHandler;
use buck2_client_ctx::daemon::client::connect::BuckdProcessInfo;
use buck2_client_ctx::daemon::client::connect::get_daemon_exe;
use buck2_client_ctx::events_ctx::EventsCtx;
use buck2_client_ctx::exit_result::ExitResult;
use buck2_client_ctx::path_arg::PathArg;
use buck2_client_ctx::startup_deadline::StartupDeadline;
use buck2_client_ctx::streaming::StreamingCommand;
use buck2_common::daemon_dir::DaemonDir;
use buck2_common::suspend_image::HeadObserved;
use buck2_common::suspend_image::exe_digest;
use buck2_common::suspend_image::host_identity;
use buck2_common::suspend_image::system_fingerprint;
use buck2_error::ExitCode;
use buck2_wrapper_common::kill::process_exists;

/// Write the daemon's state to disk and stop the daemon, for a later daemon to resume.
///
/// Needs `hydration.suspend = true` (or `buck2_hydration.suspend = true`), paging, the deferred
/// materializer with `buck2.sqlite_materializer_state = true`, and the `notify` or
/// `fs_hash_crawler` file watcher. The next daemon started with the same binary, startup config,
/// environment and root buckconfig resumes from what this writes; `buck2 resume` says whether it
/// did. If the suspend fails, the daemon keeps running: `buck2 suspend || buck2 kill` leaves no
/// daemon behind either way.
///
/// Give it, and `buck2 resume`, the same `-c` and `--config-file` flags as the builds: each
/// command injects its own buckconfig into the graph, so a suspend without the builds' flags
/// records other settings, and the first build after the resume recomputes what reads them.
#[derive(Debug, clap::Parser)]
pub struct SuspendCommand {
    #[clap(flatten)]
    event_log_opts: CommonEventLogOptions,

    // Pass the same `-c`, `--config-file` and host flags as the builds this daemon ran: the
    // suspend runs in a DICE transaction of its own, and buckconfig it injects that differs
    // from theirs is what the image records, so the first build after the resume would
    // invalidate what depends on it.
    #[clap(flatten)]
    config_opts: CommonBuildConfigurationOptions,

    #[clap(flatten)]
    starlark_opts: CommonStarlarkOptions,

    /// Before the daemon exits, wait up to this long for the BES server to acknowledge its
    /// build events, as `buck2 kill --bes-drain-timeout` does.
    #[clap(long, value_name = "DURATION", default_value = "0s")]
    bes_drain_timeout: humantime::Duration,

    /// Make the image resumable on another machine, which has none of this one's `buck-out`:
    /// upload every built output the remote cache cannot already serve (outputs of local
    /// actions), hashing each file again first, and have the resumed daemon run every action
    /// that ran locally again. Needs a remote execution backend to upload to.
    #[clap(long)]
    portable: bool,

    /// After the daemon exits, also write its state to this file, a sequence of zstd frames, for
    /// `buck2 resume --import` to unpack on another checkout at the same path. The state stays
    /// under buck-out as well. Implies `--portable`, since the importing machine has none of
    /// this one's `buck-out`.
    #[clap(long, value_name = "FILE")]
    export: Option<PathArg>,
}

#[async_trait(?Send)]
impl StreamingCommand for SuspendCommand {
    const COMMAND_NAME: &'static str = "suspend";

    fn existing_only() -> bool {
        true
    }

    async fn exec_impl(
        self,
        buckd: &mut BuckdClientConnector,
        matches: BuckArgMatches<'_>,
        ctx: &mut ClientCommandContext<'_>,
        events_ctx: &mut EventsCtx,
    ) -> ExitResult {
        let start = Instant::now();
        let context = ctx.client_context(matches, &self)?;
        let response = buckd
            .with_flushing()
            .hydration(
                HydrationRequest {
                    context: Some(context),
                    subcommand: HydrationSubcommand::Suspend.into(),
                    wait: false,
                    portable: Some(self.portable || self.export.is_some()),
                },
                events_ctx,
                ctx.console_interaction_stream(self.console_opts()),
                &mut NoPartialResultHandler,
            )
            .await??;
        let written = start.elapsed();
        if let Some(summary) = response.summary {
            buck2_client_ctx::println!("{}", summary.trim_end())?;
        }

        let lifecycle_lock = BuckdLifecycleLock::lock_with_timeout(
            ctx.paths()?.daemon_dir()?,
            StartupDeadline::duration_from_now(Duration::from_secs(10))?,
        )
        .await?;
        buck2_client_ctx::daemon::client::kill::kill_command_impl(
            &lifecycle_lock,
            "`buck2 suspend` wrote the daemon's state",
            self.bes_drain_timeout.into(),
        )
        .await?;
        buck2_client_ctx::println!(
            "suspend took {:.3}s, of which writing {:.3}s",
            start.elapsed().as_secs_f64(),
            written.as_secs_f64()
        )?;

        // Still under the lifecycle lock, so no daemon starts and consumes the image while it
        // is read.
        if let Some(out) = &self.export {
            let out = out.resolve(&ctx.working_dir);
            let cache_dir = ctx.paths()?.cache_dir_path();
            let t = Instant::now();
            let summary = tokio::task::spawn_blocking({
                let out = out.clone();
                move || bundle::export(cache_dir.as_path(), out.as_path())
            })
            .await??;
            buck2_client_ctx::println!(
                "exported {}: {} files, {} bytes compressed to {} in {:.3}s",
                out.display(),
                summary.members,
                summary.bytes,
                summary.compressed,
                t.elapsed().as_secs_f64()
            )?;
        }
        drop(lifecycle_lock);
        ExitResult::success()
    }

    fn console_opts(&self) -> &CommonConsoleOptions {
        CommonConsoleOptions::default_ref()
    }

    fn event_log_opts(&self) -> &CommonEventLogOptions {
        &self.event_log_opts
    }

    fn build_config_opts(&self) -> &CommonBuildConfigurationOptions {
        &self.config_opts
    }

    fn starlark_opts(&self) -> &CommonStarlarkOptions {
        &self.starlark_opts
    }
}

/// Start the daemon and report whether it resumed the state `buck2 suspend` wrote.
///
/// Prints `resumed: ...` or `cold: <reason>`. Any command that starts a daemon resumes the same
/// way; this one only makes the outcome visible, and with `--require` fails on a cold start.
/// With `--import`, it first unpacks a bundle `buck2 suspend --export` wrote, replacing any
/// suspended state under buck-out.
#[derive(Debug, clap::Parser)]
pub struct ResumeCommand {
    #[clap(flatten)]
    event_log_opts: CommonEventLogOptions,

    // Pass the same `-c`, `--config-file` and host flags as the build that follows: this
    // command injects its buckconfig into the resumed graph like any other.
    #[clap(flatten)]
    config_opts: CommonBuildConfigurationOptions,

    #[clap(flatten)]
    starlark_opts: CommonStarlarkOptions,

    /// Exit with code 3 if the daemon did not resume, so a job never mistakes a cold start for
    /// a warm one.
    #[clap(long)]
    require: bool,

    /// Before starting the daemon, unpack this bundle from `buck2 suspend --export`, or read it
    /// from standard input when FILE is `-`, so a download can stream into the import. Refused
    /// while a daemon runs for this checkout, and, having read only the bundle's MANIFEST and
    /// starting no daemon, for a bundle another buck2 binary, host identity, user or system
    /// wrote: the daemon would refuse it with the same reason. The daemon still checks
    /// everything when it starts.
    #[clap(long, value_name = "FILE")]
    import: Option<PathArg>,
}

/// How long `buck2 resume` waits for the daemon it starts, unless
/// `buck2_hydration.resume_timeout_s` says otherwise. A daemon restores a suspended state before
/// it answers, and a large graph takes far longer than the 90 s an ordinary start is given.
const DEFAULT_RESUME_TIMEOUT_S: u64 = 600;

/// The pid of a live daemon for this checkout, from `buckd.info`.
fn running_daemon(daemon_dir: &DaemonDir) -> buck2_error::Result<Option<u32>> {
    let Some(info) = BuckdProcessInfo::load_if_exists(daemon_dir)? else {
        return Ok(None);
    };
    let pid = info.pid()?;
    Ok(process_exists(pid)?.then(|| pid.to_u32()))
}

#[async_trait(?Send)]
impl StreamingCommand for ResumeCommand {
    const COMMAND_NAME: &'static str = "resume";

    async fn before_connect(&self, ctx: &ClientCommandContext<'_>) -> buck2_error::Result<()> {
        let Some(bundle) = &self.import else {
            return Ok(());
        };
        let source = if bundle.path() == std::path::Path::new("-") {
            bundle::BundleSource::Stdin
        } else {
            bundle::BundleSource::File(bundle.resolve(&ctx.working_dir).into_path_buf())
        };
        let paths = ctx.paths()?;
        let daemon_dir = paths.daemon_dir()?;
        // Held until the bundle is in place, so no daemon starts on half an image.
        let lifecycle_lock = BuckdLifecycleLock::lock_with_timeout(
            daemon_dir.clone(),
            StartupDeadline::duration_from_now(Duration::from_secs(10))?,
        )
        .await?;
        let running = running_daemon(&daemon_dir)?;
        let cache_dir = paths.cache_dir_path();
        // What the daemon this command starts will check the MANIFEST against: its identity
        // comes from the startup config the client hands it.
        let identity = host_identity(ctx.immediate_config.daemon_startup_config()?);
        let t = Instant::now();
        let summary = tokio::task::spawn_blocking({
            let source = source.clone();
            move || -> buck2_error::Result<bundle::ImportSummary> {
                // Only the binary the daemon runs is hashed, and only when no daemon runs.
                let exe = match running {
                    Some(_) => String::new(),
                    None => exe_digest(&get_daemon_exe()?)?,
                };
                let here = HeadObserved {
                    exe,
                    host_identity: identity,
                    system: system_fingerprint(),
                    // SAFETY: getuid cannot fail.
                    uid: unsafe { libc::getuid() },
                };
                Ok(bundle::import(&source, cache_dir.as_path(), &here, running)?)
            }
        })
        .await??;
        drop(lifecycle_lock);
        buck2_client_ctx::eprintln!(
            "imported {}: {} files, {} bytes, suspended at {} ms, in {:.3}s on {} threads",
            source,
            summary.members,
            summary.bytes,
            summary.created_at_ms,
            t.elapsed().as_secs_f64(),
            summary.threads
        )?;
        Ok(())
    }

    fn daemon_startup_timeout_floor(
        &self,
        ctx: &ClientCommandContext<'_>,
    ) -> buck2_error::Result<Option<Duration>> {
        let secs = ctx
            .immediate_config
            .hydration_resume_timeout_s()?
            .unwrap_or(DEFAULT_RESUME_TIMEOUT_S);
        Ok(Some(Duration::from_secs(secs)))
    }

    async fn exec_impl(
        self,
        buckd: &mut BuckdClientConnector,
        matches: BuckArgMatches<'_>,
        ctx: &mut ClientCommandContext<'_>,
        events_ctx: &mut EventsCtx,
    ) -> ExitResult {
        let context = ctx.client_context(matches, &self)?;
        let response = buckd
            .with_flushing()
            .hydration(
                HydrationRequest {
                    context: Some(context),
                    subcommand: HydrationSubcommand::ResumeStatus.into(),
                    wait: false,
                    portable: None,
                },
                events_ctx,
                ctx.console_interaction_stream(self.console_opts()),
                &mut NoPartialResultHandler,
            )
            .await??;
        if let Some(summary) = response.summary {
            buck2_client_ctx::println!("{}", summary.trim_end())?;
        }
        if self.require && response.resumed != Some(true) {
            return ExitResult::status_with_emitted_errors(ExitCode::UserError, Vec::new());
        }
        ExitResult::success()
    }

    fn console_opts(&self) -> &CommonConsoleOptions {
        CommonConsoleOptions::default_ref()
    }

    fn event_log_opts(&self) -> &CommonEventLogOptions {
        &self.event_log_opts
    }

    fn build_config_opts(&self) -> &CommonBuildConfigurationOptions {
        &self.config_opts
    }

    fn starlark_opts(&self) -> &CommonStarlarkOptions {
        &self.starlark_opts
    }
}

#[cfg(test)]
mod tests {
    use buck2_cli_proto::DaemonProcessInfo;
    use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;

    use super::*;

    fn daemon_dir_with_pid(pid: Option<u32>) -> (tempfile::TempDir, DaemonDir) {
        let dir = tempfile::tempdir().unwrap();
        let daemon_dir = DaemonDir {
            path: AbsNormPathBuf::new(dir.path().to_path_buf()).unwrap(),
        };
        if let Some(pid) = pid {
            let info = DaemonProcessInfo {
                pid: pid as i64,
                endpoint: "tcp:1".to_owned(),
                version: "v".to_owned(),
                auth_token: "t".to_owned(),
            };
            std::fs::write(daemon_dir.buckd_info(), serde_json::to_vec(&info).unwrap()).unwrap();
        }
        (dir, daemon_dir)
    }

    #[test]
    fn a_live_daemon_pid_counts_as_running() {
        let me = std::process::id();
        let (_d, dir) = daemon_dir_with_pid(Some(me));
        assert_eq!(running_daemon(&dir).unwrap(), Some(me));
    }

    #[test]
    fn no_info_or_a_dead_pid_is_not_running() {
        let (_d, dir) = daemon_dir_with_pid(None);
        assert_eq!(running_daemon(&dir).unwrap(), None);

        let mut child = std::process::Command::new("true").spawn().unwrap();
        let dead = child.id();
        child.wait().unwrap();
        let (_d, dir) = daemon_dir_with_pid(Some(dead));
        assert_eq!(running_daemon(&dir).unwrap(), None);
    }
}
