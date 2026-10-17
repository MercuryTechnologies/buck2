/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::os::unix::process::CommandExt;
use std::process::Command as StdCommand;
use std::process::ExitStatus;
use std::time::Duration;

use buck2_common::kill_util::try_terminate_process_group_gracefully;
use buck2_error::BuckErrorContext;
use buck2_error::BuckErrorOptionContext;
use buck2_resource_control::ActionFreezeEvent;
use buck2_resource_control::ActionFreezeEventReceiver;
use buck2_resource_control::OrphanProcessInfo;
use buck2_resource_control::cgroup::Cgroup;
use buck2_resource_control::cgroup::CgroupKindLeaf;
use buck2_resource_control::cgroup::CgroupMinimal;
use buck2_resource_control::cgroup::NoMemoryMonitoring;
use buck2_resource_control::path::CgroupPathBuf;
use futures::StreamExt;
use futures::pin_mut;
use nix::sys::signal;
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use tokio::pin;
use tokio::process::Child;
use tokio::process::ChildStderr;
use tokio::process::ChildStdout;
use tokio::process::Command;

use crate::process_group::SpawnError;

pub(crate) struct ProcessCommandImpl {
    inner: Command,
    cgroup: Option<Cgroup<NoMemoryMonitoring, CgroupKindLeaf>>,
}

impl ProcessCommandImpl {
    pub(crate) async fn new(
        mut cmd: StdCommand,
        cgroup: Option<CgroupPathBuf>,
    ) -> buck2_error::Result<Self> {
        cmd.process_group(0);

        let cgroup = if let Some(cgroup) = cgroup {
            let cgroup = CgroupMinimal::try_from_path(cgroup.clone())
                .await?
                .into_leaf()
                .await?;
            cgroup.setup_command(&mut cmd);
            Some(cgroup)
        } else {
            None
        };

        Ok(Self {
            inner: cmd.into(),
            cgroup,
        })
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn spawn(mut self) -> Result<ProcessGroupImpl, (SpawnError, Self)> {
        match self.inner.spawn() {
            Ok(inner) => Ok(ProcessGroupImpl {
                inner,
                cgroup: self.cgroup,
            }),
            Err(e) => Err((e.into(), self)),
        }
    }
}

pub(crate) struct ProcessGroupImpl {
    inner: Child,
    cgroup: Option<Cgroup<NoMemoryMonitoring, CgroupKindLeaf>>,
}

impl ProcessGroupImpl {
    pub(crate) fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.inner.stdout.take()
    }

    pub(crate) fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.inner.stderr.take()
    }

    /// It might seem surprising that we associate the freezing/unfreezing with the lifetime of a
    /// `wait` future instead of with the lifetime of some higher level thing such as the process,
    /// the command, the action, etc.
    ///
    /// But this is intentional and important; it ensures that we only freeze cgroups while we know
    /// exactly what's going on in them. Any other choice risks leaving behind frozen cgroups or
    /// freezing cgroups during critical times like when we're spawning into them or killing them.
    pub(crate) async fn wait(
        &mut self,
        freeze_rx: impl ActionFreezeEventReceiver,
    ) -> buck2_error::Result<(ExitStatus, Vec<OrphanProcessInfo>)> {
        let child = self.inner.wait();
        pin!(child);
        pin_mut!(freeze_rx);
        let mut freeze_guard = None;
        loop {
            tokio::select! {
                res = &mut child => {
                    // Kill any remaining PIDs in the cgroup that outlived the main process.
                    let orphans = if let Some(cgroup) = self.cgroup.as_ref() {
                        cgroup.kill_remaining_pids().await?
                    } else {
                        Vec::new()
                    };
                    break Ok((res?, orphans));
                },
                Some(freeze_op) = freeze_rx.next() => {
                    match freeze_op {
                        ActionFreezeEvent::Unfreeze => {
                            drop(freeze_guard.take());
                        }
                        ActionFreezeEvent::Freeze => {
                            if let Some(cgroup) = self.cgroup.as_ref() {
                                freeze_guard = cgroup.freeze().await.ok();
                            }
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn id(&self) -> Option<u32> {
        self.inner.id()
    }

    // Kill the process tree. Uses process-group-based signaling (killpg or
    // graceful SIGTERM+SIGKILL).
    pub(crate) async fn kill(
        &self,
        graceful_shutdown_timeout_s: Option<u32>,
    ) -> buck2_error::Result<()> {
        let pid: i32 = self
            .inner
            .id()
            .and_then(|id| id.try_into().ok())
            .internal_error("PID does not fit a i32")?;

        // The command leads its own process group (`process_group(0)` above),
        // and a wrapper that forks rather than execs leaves its real work in
        // that group, so both legs signal the group, not the pid.
        if let Some(graceful_shutdown_timeout_s) = graceful_shutdown_timeout_s {
            try_terminate_process_group_gracefully(
                pid,
                Duration::from_secs(graceful_shutdown_timeout_s as u64),
            )
            .await
            .with_buck_error_context(|| format!("Failed to terminate process {pid} gracefully"))?;
        } else {
            signal::killpg(Pid::from_raw(pid), Signal::SIGKILL)
                .with_buck_error_context(|| format!("Failed to kill process {pid}"))?;
        }

        Ok(())
    }
}

impl Drop for ProcessGroupImpl {
    /// A command whose handle goes before it has been waited for has nobody
    /// left to wait for it or kill it: the stream that owned it was dropped,
    /// for instance because the forkserver's connection to the daemon ended
    /// and the forkserver is exiting. Kill its group rather than orphan it.
    /// `id()` is `None` once the leader has been reaped, so this never
    /// signals a group whose pgid could have been reused.
    fn drop(&mut self) {
        if let Some(pid) = self.inner.id().and_then(|id| i32::try_from(id).ok()) {
            tracing::info!("Killing process group {pid}, whose handle was dropped");
            // ESRCH: the group has already gone.
            signal::killpg(Pid::from_raw(pid), Signal::SIGKILL).ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::process::Stdio;
    use std::time::Duration;

    use buck2_util::process::background_command;
    use tokio::io::AsyncBufReadExt;
    use tokio::io::BufReader;

    use crate::process_group::ProcessCommand;
    use crate::process_group::ProcessGroup;

    /// Spawns a wrapper that forks a long-lived child and waits for it, as a
    /// worker started through `sh wrap.sh` without `exec` does, and returns
    /// the group and the child's pid.
    async fn spawn_forking_wrapper() -> buck2_error::Result<(ProcessGroup, i32)> {
        let mut cmd = background_command("sh");
        // Longer than any wait below, so a child that was not killed is
        // still there when the test looks.
        cmd.args(["-c", "sleep 1000 & echo $!; wait"]);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut group = ProcessCommand::new(cmd, None)
            .await?
            .spawn()
            .map_err(|x| x.0)
            .unwrap();
        let mut lines = BufReader::new(group.take_stdout().unwrap()).lines();
        let child = tokio::time::timeout(Duration::from_secs(60), lines.next_line())
            .await
            .expect("the wrapper should print its child's pid within a minute")?
            .unwrap()
            .parse()
            .unwrap();
        Ok((group, child))
    }

    /// Whether `pid` is gone (or a zombie nobody but init will reap), waiting
    /// up to 30 seconds for the kernel to deliver the kill.
    async fn is_gone(pid: i32) -> bool {
        let pid = nix::unistd::Pid::from_raw(pid);
        for _ in 0..300 {
            let zombie = std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
                stat.rsplit_once(')')
                    .is_some_and(|(_, rest)| rest.trim_start().starts_with('Z'))
            });
            if zombie || nix::sys::signal::kill(pid, None) == Err(nix::errno::Errno::ESRCH) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    #[tokio::test]
    async fn test_graceful_kill_reaches_a_forked_child() -> buck2_error::Result<()> {
        let (mut group, child) = spawn_forking_wrapper().await?;
        group.kill(Some(10)).await?;
        group.wait(futures::stream::pending()).await?;
        assert!(
            is_gone(child).await,
            "the wrapper's child survived its kill"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_dropping_a_running_group_kills_it() -> buck2_error::Result<()> {
        let (group, child) = spawn_forking_wrapper().await?;
        drop(group);
        assert!(
            is_gone(child).await,
            "the wrapper's child survived its handle"
        );
        Ok(())
    }
}
