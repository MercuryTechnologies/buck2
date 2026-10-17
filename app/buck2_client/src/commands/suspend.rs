/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

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
use buck2_client_ctx::events_ctx::EventsCtx;
use buck2_client_ctx::exit_result::ExitResult;
use buck2_client_ctx::startup_deadline::StartupDeadline;
use buck2_client_ctx::streaming::StreamingCommand;
use buck2_error::ExitCode;

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
}

#[async_trait(?Send)]
impl StreamingCommand for ResumeCommand {
    const COMMAND_NAME: &'static str = "resume";

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
