/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Running `git` in the project: for the version control event, and for what a suspend image
//! records of the checkout.

use std::io::Write;
use std::iter;
use std::process::Stdio;

use buck2_error::ErrorTag;
use buck2_fs::paths::abs_norm_path::AbsNormPath;
use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;
use buck2_util::properly_reaped_child::reap_on_drop_command;

/// `GIT_OPTIONAL_LOCKS=0` prevents git from taking the index lock or refreshing the index on
/// disk, so we don't contend with a concurrent user `git` invocation (e.g. for `git status`).
const GIT_ENV: &[(&str, &str)] = &[("GIT_OPTIONAL_LOCKS", "0")];

pub(crate) fn display_command(program: &str, args: &[&str]) -> String {
    shlex::try_join(iter::once(program).chain(args.iter().copied()))
        .unwrap_or_else(|_| format!("{program} <argument contains NUL>"))
}

/// `-C <repo_root>` makes git operate on the repository regardless of the daemon's current
/// working directory.
fn git_args<'a>(repo_root: &'a AbsNormPath, args: &[&'a str]) -> buck2_error::Result<Vec<&'a str>> {
    let Some(repo_root) = repo_root.as_path().to_str() else {
        return Err(buck2_error::buck2_error!(
            ErrorTag::Input,
            "Repository root is not valid utf8: {}",
            repo_root.as_path().display()
        ));
    };
    let mut full_args = vec!["-C", repo_root];
    full_args.extend_from_slice(args);
    Ok(full_args)
}

/// Run a `git` command rooted at `repo_root`, returning its stdout on success or
/// an error message describing the failure.
pub(crate) async fn run_git(
    repo_root: &AbsNormPathBuf,
    args: &[&str],
) -> buck2_error::Result<String> {
    let full_args = git_args(repo_root, args)?;
    let command = display_command("git", &full_args);

    let output = match reap_on_drop_command("git", &full_args, Some(GIT_ENV)) {
        Ok(command) => command.output().await,
        Err(e) => {
            return Err(buck2_error::buck2_error!(
                ErrorTag::Tier0,
                "reap_on_drop_command for `{command}` failed: {e}"
            ));
        }
    };

    let result = match output {
        Ok(result) => result,
        Err(e) => {
            return Err(buck2_error::buck2_error!(
                ErrorTag::Tier0,
                "Command `{command}` failed with error: {e:?}"
            ));
        }
    };

    if !result.status.success() {
        let stderr = match std::str::from_utf8(&result.stderr) {
            Ok(s) => s,
            Err(e) => {
                return Err(buck2_error::buck2_error!(
                    ErrorTag::Tier0,
                    "`{command}` stderr is not utf8: {e}"
                ));
            }
        };
        return Err(buck2_error::buck2_error!(
            ErrorTag::Tier0,
            "Command `{command}` failed with error code {}; stderr: {stderr}",
            result.status
        ));
    }

    match std::str::from_utf8(&result.stdout) {
        Ok(s) => Ok(s.to_owned()),
        Err(e) => Err(buck2_error::buck2_error!(
            ErrorTag::Tier0,
            "`{command}` stdout is not utf8: {e}"
        )),
    }
}

/// [`run_git`] for a caller that is not async, such as the file watcher's resume: waits for
/// git, writing `stdin` to it from another thread, and returns its stdout bytes.
pub(crate) fn run_git_blocking(
    repo_root: &AbsNormPath,
    args: &[&str],
    stdin: Option<Vec<u8>>,
) -> buck2_error::Result<Vec<u8>> {
    let full_args = git_args(repo_root, args)?;
    let command = display_command("git", &full_args);
    let mut child = std::process::Command::new("git")
        .args(&full_args)
        .envs(GIT_ENV.iter().copied())
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            buck2_error::buck2_error!(ErrorTag::Tier0, "Command `{command}` failed to start: {e}")
        })?;
    let writer = match (stdin, child.stdin.take()) {
        (Some(bytes), Some(mut pipe)) => Some(std::thread::spawn(move || {
            // A git that exits early closes the pipe; its exit status reports why.
            drop(pipe.write_all(&bytes));
        })),
        _ => None,
    };
    let output = child.wait_with_output().map_err(|e| {
        buck2_error::buck2_error!(
            ErrorTag::Tier0,
            "Command `{command}` failed with error: {e}"
        )
    })?;
    if let Some(writer) = writer {
        drop(writer.join());
    }
    if !output.status.success() {
        return Err(buck2_error::buck2_error!(
            ErrorTag::Tier0,
            "Command `{command}` failed with error code {}; stderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(output.stdout)
}
