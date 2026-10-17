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

#[allow(unused_variables)]
pub async fn try_terminate_process_gracefully(
    pid: i32,
    timeout: Duration,
) -> buck2_error::Result<()> {
    #[cfg(unix)]
    {
        unix::try_terminate_process_gracefully(pid, timeout).await
    }

    #[cfg(not(unix))]
    {
        Err(buck2_error::buck2_error!(
            buck2_error::ErrorTag::Unimplemented,
            "Graceful process termination is not implemented for non-unix target family."
        ))
    }
}

/// Like [`try_terminate_process_gracefully`], for the process group that `pgid`
/// leads: SIGTERM goes to every process in the group, the grace is measured on
/// the leader, and SIGKILL then goes to the whole group in any case, so a member
/// the leader forked and left behind, or one that ignores SIGTERM, does not
/// outlive it. The leader must not have been reaped yet: until it is, its pid
/// cannot name another group.
#[allow(unused_variables)]
pub async fn try_terminate_process_group_gracefully(
    pgid: i32,
    timeout: Duration,
) -> buck2_error::Result<()> {
    #[cfg(unix)]
    {
        unix::try_terminate_process_group_gracefully(pgid, timeout).await
    }

    #[cfg(not(unix))]
    {
        Err(buck2_error::buck2_error!(
            buck2_error::ErrorTag::Unimplemented,
            "Graceful process termination is not implemented for non-unix target family."
        ))
    }
}

#[cfg(unix)]
mod unix {
    use std::ops::ControlFlow;
    use std::time::Duration;
    use std::time::Instant;

    use nix::sys::signal::Signal;
    use tracing::info;
    use tracing::warn;

    pub(crate) async fn try_terminate_process_gracefully(
        pid: i32,
        timeout: Duration,
    ) -> buck2_error::Result<()> {
        let pid = nix::unistd::Pid::from_raw(pid);
        match send_signal_and_wait_for_shutdown(
            Signal::SIGTERM,
            Target::Process,
            pid,
            Some(timeout),
        )
        .await
        {
            ControlFlow::Continue(_) => {}
            ControlFlow::Break(StoppedWaiting::Success) => {
                return Ok(());
            }
            ControlFlow::Break(StoppedWaiting::UnexpectedError(e)) => {
                return Err(e);
            }
        }
        warn!(
            "Failed to gracefully terminate process `{}`, sending SIGKILL.",
            pid,
        );
        match send_signal_and_wait_for_shutdown(Signal::SIGKILL, Target::Process, pid, None).await {
            ControlFlow::Continue(_) => {
                unreachable!("Not expected when there is no timeout specified.")
            }
            ControlFlow::Break(StoppedWaiting::Success) => Ok(()),
            ControlFlow::Break(StoppedWaiting::UnexpectedError(e)) => Err(e),
        }
    }

    pub(crate) async fn try_terminate_process_group_gracefully(
        pgid: i32,
        timeout: Duration,
    ) -> buck2_error::Result<()> {
        let pgid = nix::unistd::Pid::from_raw(pgid);
        match send_signal_and_wait_for_shutdown(Signal::SIGTERM, Target::Group, pgid, Some(timeout))
            .await
        {
            ControlFlow::Continue(_) => {
                warn!(
                    "Failed to gracefully terminate process group `{}`, sending SIGKILL.",
                    pgid,
                );
            }
            ControlFlow::Break(StoppedWaiting::Success) => {}
            ControlFlow::Break(StoppedWaiting::UnexpectedError(e)) => return Err(e),
        }
        // Whether or not the leader went within the grace, whatever it left
        // in its group has nobody left to stop it.
        match send_signal_and_wait_for_shutdown(Signal::SIGKILL, Target::Group, pgid, None).await {
            ControlFlow::Continue(_) => {
                unreachable!("Not expected when there is no timeout specified.")
            }
            ControlFlow::Break(StoppedWaiting::Success) => Ok(()),
            ControlFlow::Break(StoppedWaiting::UnexpectedError(e)) => Err(e),
        }
    }

    #[derive(Clone, Copy)]
    enum Target {
        /// `kill(pid)`: the one process.
        Process,
        /// `killpg(pid)`: every process in the group `pid` leads. The wait
        /// is still on the leader, because a zombie leader keeps the group
        /// non-empty for `killpg(pgid, 0)` until it is reaped.
        Group,
    }

    enum StoppedWaiting {
        Success,
        UnexpectedError(buck2_error::Error),
    }

    /// Whether the process has terminated but nobody has reaped it yet. Only
    /// its parent can, and until it does `kill(pid, 0)` still finds the
    /// process; for anyone else, a zombie is as gone as a process gets.
    fn is_zombie(pid: nix::unistd::Pid) -> bool {
        #[cfg(target_os = "linux")]
        {
            // `/proc/<pid>/stat`: `<pid> (<comm>) <state> ...`, where `comm`
            // may contain anything, so look past the last `)`.
            std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
                stat.rsplit_once(')')
                    .is_some_and(|(_, rest)| rest.trim_start().starts_with('Z'))
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = pid;
            false
        }
    }

    #[cfg(test)]
    pub(super) fn is_zombie_for_tests(pid: nix::unistd::Pid) -> bool {
        is_zombie(pid)
    }

    fn check_result(pid: nix::unistd::Pid, result: nix::Result<()>) -> ControlFlow<StoppedWaiting> {
        match result {
            Ok(_) if is_zombie(pid) => ControlFlow::Break(StoppedWaiting::Success),
            // Signal is sent successfully, now we need to wait for a process to terminate.
            Ok(_) => ControlFlow::Continue(()),
            // There is no such process, our desired outcome.
            Err(nix::errno::Errno::ESRCH) => ControlFlow::Break(StoppedWaiting::Success),
            Err(e) => {
                ControlFlow::Break(StoppedWaiting::UnexpectedError(buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Tier0,
                    "Unexpected error while waiting for process `{}` to terminate (`{}`)",
                    pid,
                    e
                )))
            }
        }
    }

    async fn send_signal_and_wait_for_shutdown(
        signal: nix::sys::signal::Signal,
        target: Target,
        pid: nix::unistd::Pid,
        timeout: Option<Duration>,
    ) -> ControlFlow<StoppedWaiting> {
        let start = Instant::now();
        // Actually send a signal.
        match target {
            Target::Process => {
                info!("Sending signal `{}` to process `{}`.", signal, pid);
                check_result(pid, nix::sys::signal::kill(pid, signal))?;
            }
            Target::Group => {
                info!("Sending signal `{}` to process group `{}`.", signal, pid);
                // ESRCH: the group is empty, so the leader is gone too.
                check_result(pid, nix::sys::signal::killpg(pid, signal))?;
            }
        }
        loop {
            if let Some(timeout) = timeout {
                if Instant::now() - start > timeout {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            // Just check the process, signal already sent.
            check_result(pid, nix::sys::signal::kill(pid, None))?;
        }
        ControlFlow::Continue(())
    }
}

#[cfg(unix)]
#[cfg(test)]
mod tests {
    use std::process::Stdio;

    use tokio::fs::File;
    use tokio::io::AsyncBufReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::io::BufReader;

    use super::*;

    #[tokio::test]
    async fn test_graceful_termination() -> buck2_error::Result<()> {
        let dir = tempfile::tempdir()?;

        // We cannot start child script directly else it will enter a zombie state
        // when trying to kill it and termination would be stuck.
        let mut parent_file = File::create(dir.path().join("parent.sh")).await?;
        let parent_script = r#"
            sh child.sh &
            while [ ! -f trap_registered ]; do sleep 1; done # wait for a child script to register a trap
            echo $!
        "#;
        parent_file.write_all(parent_script.as_bytes()).await?;

        let mut child_file = File::create(dir.path().join("child.sh")).await?;
        let child_script = r#"
            sigterm()
            {
                touch gracefully_terminated
            }
            trap sigterm TERM
            touch trap_registered

            sleep 100 &
            wait
        "#;
        child_file.write_all(child_script.as_bytes()).await?;

        // ast-grep-ignore: rust/buck2-no-command-new
        let mut command = tokio::process::Command::new("sh");
        command
            .args(["parent.sh"])
            .current_dir(dir.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());

        let mut parent = command.spawn()?;
        parent.wait().await?;

        let pid = {
            let stdout = parent.stdout.take().unwrap();
            let mut reader = BufReader::new(stdout).lines();
            let pid_str = reader.next_line().await?;
            pid_str.unwrap().parse().unwrap()
        };

        // Generous: on a loaded machine the trap can take more than a second,
        // and a SIGKILL mid-trap fails the test. The wait ends as soon as the
        // child is gone, so this costs nothing when the trap is quick.
        try_terminate_process_gracefully(pid, Duration::from_secs(60)).await?;

        assert!(tokio::fs::try_exists(dir.path().join("gracefully_terminated")).await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_sigkill_when_graceful_termination_fails() -> buck2_error::Result<()> {
        let dir = tempfile::tempdir()?;

        // We cannot start child script directly else it will enter a zombie state
        // when trying to kill it and termination would be stuck.
        let mut parent_file = File::create(dir.path().join("parent.sh")).await?;
        let parent_script = r#"
            sh child.sh &
            while [ ! -f trap_registered ]; do sleep 1; done # wait for a child script to register a trap
            echo $!
        "#;
        parent_file.write_all(parent_script.as_bytes()).await?;

        let mut child_file = File::create(dir.path().join("child.sh")).await?;
        let child_script = r#"
            sigterm()
            {
                sleep 3  # block termination
                touch gracefully_terminated
            }
            trap sigterm TERM
            touch trap_registered

            sleep 100 &
            wait
        "#;
        child_file.write_all(child_script.as_bytes()).await?;

        // ast-grep-ignore: rust/buck2-no-command-new
        let mut command = tokio::process::Command::new("sh");
        command
            .args(["parent.sh"])
            .current_dir(dir.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());

        let mut parent = command.spawn()?;
        // Bounded: the parent script spins until the child has registered its
        // trap, and the SIGKILL leg waits for the child to be gone with no
        // timeout of its own. A hang here would otherwise be the suite's.
        tokio::time::timeout(Duration::from_secs(60), parent.wait())
            .await
            .expect("the parent script should finish within a minute")?;

        let pid = {
            let stdout = parent.stdout.take().unwrap();
            let mut reader = BufReader::new(stdout).lines();
            let pid_str = reader.next_line().await?;
            pid_str.unwrap().parse().unwrap()
        };

        tokio::time::timeout(
            Duration::from_secs(60),
            try_terminate_process_gracefully(pid, Duration::from_secs(1)),
        )
        .await
        .expect("the child should be gone well within a minute of the SIGKILL")?;

        assert!(!tokio::fs::try_exists(dir.path().join("gracefully_terminated")).await?);

        Ok(())
    }

    /// Starts `script` under `sh` as the leader of a new process group, the
    /// way the forkserver starts a command, and returns the leader's pid and
    /// the first line the script prints.
    async fn spawn_group_leader(
        dir: &std::path::Path,
        script: &str,
    ) -> buck2_error::Result<(tokio::process::Child, i32, String)> {
        // ast-grep-ignore: rust/buck2-no-command-new
        let mut command = tokio::process::Command::new("sh");
        command
            .args(["-c", script])
            .current_dir(dir)
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut leader = command.spawn()?;
        let pid = leader.id().unwrap().try_into().unwrap();
        let mut lines = BufReader::new(leader.stdout.take().unwrap()).lines();
        let line = tokio::time::timeout(Duration::from_secs(60), lines.next_line())
            .await
            .expect("the script should print within a minute")?
            .unwrap();
        Ok((leader, pid, line))
    }

    /// Whether `pid` is gone, waiting up to a minute for init (or a
    /// subreaper) to reap it once it has been reparented.
    async fn is_gone(pid: i32) -> bool {
        let pid = nix::unistd::Pid::from_raw(pid);
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(60) {
            match nix::sys::signal::kill(pid, None) {
                Err(nix::errno::Errno::ESRCH) => return true,
                _ if super::unix::is_zombie_for_tests(pid) => return true,
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
        false
    }

    #[tokio::test]
    async fn test_group_termination_reaches_a_forked_child() -> buck2_error::Result<()> {
        let dir = tempfile::tempdir()?;
        // A wrapper that forks its worker and waits for it, as `sh wrap.sh`
        // does when the worker is not exec'd: SIGTERM to the wrapper alone
        // ends the wrapper and orphans the child.
        let (mut leader, pgid, child) =
            spawn_group_leader(dir.path(), "sleep 1000 & echo $!; wait").await?;
        let child: i32 = child.parse().unwrap();

        tokio::time::timeout(
            Duration::from_secs(60),
            try_terminate_process_group_gracefully(pgid, Duration::from_secs(10)),
        )
        .await
        .expect("the group should be gone well within a minute")?;
        leader.wait().await?;

        assert!(is_gone(child).await, "the leader's child survived");
        Ok(())
    }

    #[tokio::test]
    async fn test_group_termination_kills_a_member_that_ignores_sigterm() -> buck2_error::Result<()>
    {
        let dir = tempfile::tempdir()?;
        // The leader exits on SIGTERM at once; its child ignores SIGTERM, as
        // a worker whose handler does not end it does.
        let (mut leader, pgid, child) = spawn_group_leader(
            dir.path(),
            "(trap '' TERM; exec sleep 1000) & echo $!; wait",
        )
        .await?;
        let child: i32 = child.parse().unwrap();

        tokio::time::timeout(
            Duration::from_secs(60),
            try_terminate_process_group_gracefully(pgid, Duration::from_secs(1)),
        )
        .await
        .expect("the group should be gone well within a minute")?;
        leader.wait().await?;

        assert!(is_gone(child).await, "the SIGTERM-ignoring member survived");
        Ok(())
    }
}
