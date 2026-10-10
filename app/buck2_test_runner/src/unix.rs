/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::os::unix::io::FromRawFd;
use std::os::unix::io::RawFd;
use std::os::unix::net::UnixStream as StdUnixStream;

use buck2_error::BuckErrorContext;
use buck2_grpc::DuplexChannel;
use clap::Parser;
use tokio::net::UnixStream;

#[derive(Debug, Parser)]
pub struct Buck2TestRunnerUnix {
    #[clap(long)]
    executor_fd: RawFd,

    #[clap(long)]
    orchestrator_fd: RawFd,

    args: Vec<String>,
}

impl Buck2TestRunnerUnix {
    pub async fn run(self) -> buck2_error::Result<()> {
        // NOTE: We assume the parameters we received from the caller are correct here. If
        // they're not, things are probably going to go wrong but that's on our caller.
        //
        // There are a few ways in which the params could be incorrect:
        // - The FDs don't exist
        // - The FDs aren't streams
        // - The FDs are the same
        //
        // In all those cases though, the unsafety below is going to result in bad file
        // descriptors at worse, which is basically the best we can do anyway.
        let orchestrator_io =
            UnixStream::from_std(unsafe { StdUnixStream::from_raw_fd(self.orchestrator_fd) })
                .buck_error_context("Failed to create orchestrator_io")?;

        let executor_io =
            UnixStream::from_std(unsafe { StdUnixStream::from_raw_fd(self.executor_fd) })
                .buck_error_context("Failed to create executor_io")?;

        let executor_io = {
            let (read, write) = tokio::io::split(executor_io);
            DuplexChannel::new(read, write)
        };

        crate::service::run(orchestrator_io, executor_io, self.args).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    /// The command line `buck2 test` gives the internal runner when
    /// `[test] undeclared_outputs_dir` is set: the runner's own flags come
    /// after the executor's file descriptors and `--`, first among them.
    #[test]
    fn the_outputs_dir_flag_reaches_the_runner_s_config() {
        let runner = Buck2TestRunnerUnix::try_parse_from([
            "internal-test-runner",
            "--executor-fd",
            "3",
            "--orchestrator-fd",
            "4",
            "--",
            "ignored",
            "--buck-test-info",
            "ignored",
            "--undeclared-outputs-dir",
            "--timeout",
            "3600",
            "--test-arg",
            "--match",
            "x",
        ])
        .expect("the runner command accepts its arguments");
        let config = Config::try_parse_from(&runner.args).expect("the config parses");
        assert!(config.undeclared_outputs_dir);
        assert_eq!(config.timeout, 3600);
        assert_eq!(config.test_arg, vec!["--match", "x"]);
    }

    #[test]
    fn the_flag_before_the_file_descriptors_is_refused() {
        assert!(
            Buck2TestRunnerUnix::try_parse_from([
                "internal-test-runner",
                "--undeclared-outputs-dir",
                "--executor-fd",
                "3",
                "--orchestrator-fd",
                "4",
            ])
            .is_err()
        );
    }
}
