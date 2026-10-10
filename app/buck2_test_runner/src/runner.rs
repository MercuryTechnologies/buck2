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

use buck2_error::BuckErrorContext;
use buck2_error::internal_error;
use buck2_test_api::environment::build_test_env;
use buck2_test_api::data::ArgValue;
use buck2_test_api::data::ArgValueContent;
use buck2_test_api::data::ConfiguredTargetHandle;
use buck2_test_api::data::DeclaredOutput;
use buck2_test_api::data::ExecuteResponse;
use buck2_test_api::data::ExecutionResult2;
use buck2_test_api::data::ExecutionStatus;
use buck2_test_api::data::ExternalRunnerSpec;
use buck2_test_api::data::ExternalRunnerSpecValue;
use buck2_test_api::data::OutputName;
use buck2_test_api::data::RemoteStorageConfig;
use buck2_test_api::data::RequiredLocalResources;
use buck2_test_api::data::TEST_OUTPUTS_DIR_NAME;
use buck2_test_api::data::TestResult;
use buck2_test_api::data::TestStage;
use buck2_test_api::data::TestStatus;
use buck2_test_api::grpc::TestOrchestratorClient;
use clap::Parser;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::channel::mpsc::UnboundedReceiver;
use host_sharing::HostSharingRequirements;
use parking_lot::Mutex;
use sorted_vector_map::SortedVectorMap;

use crate::config::Config;
use crate::config::EnvValue;

pub type SpecReceiver = UnboundedReceiver<ExternalRunnerSpec>;

/// Internal test runner implementation for Buck2.
///
/// This is a basic test runner intended to be used by the open-source Buck2 build
/// if no external test runner is provided. This ensures that `buck2 test` works
/// out-of-the-box for open-source users.
///
/// **This is intended for open-source use only.**
pub struct Buck2TestRunner {
    orchestrator_client: TestOrchestratorClient,
    spec_receiver: Mutex<Option<SpecReceiver>>,
    config: Config,
}

impl Buck2TestRunner {
    pub fn new(
        orchestrator_client: TestOrchestratorClient,
        spec_receiver: SpecReceiver,
        args: Vec<String>,
    ) -> buck2_error::Result<Self> {
        let config = Config::try_parse_from(args)
            .buck_error_context("Error parsing test runner arguments")?;
        Ok(Self {
            orchestrator_client,
            spec_receiver: Mutex::new(Some(spec_receiver)),
            config,
        })
    }

    pub async fn run_all_tests(&self) -> buck2_error::Result<()> {
        let receiver;
        {
            let mut maybe_receiver = self.spec_receiver.lock();
            receiver = maybe_receiver
                .take()
                .ok_or_else(|| internal_error!("Spec channel has already been consumed"))?;
            drop(maybe_receiver);
        }
        let run_verdict = receiver
            .map(|spec| async move {
                let name = format!(
                    "{}//{}:{}",
                    spec.target.cell, spec.target.package, spec.target.target
                );
                let target_handle = spec.target.handle.to_owned();

                let execution_response = self
                    .execute_test_from_spec(spec)
                    .await
                    .buck_error_context("Test execution request failed")?;

                let execution_result = match execution_response {
                    ExecuteResponse::Result(r) => r,
                    ExecuteResponse::Cancelled(_) => return Ok(TestStatus::OMITTED),
                };

                let test_result = get_test_result(name, target_handle, execution_result);
                let test_status = test_result.status.clone();

                self.report_test_result(test_result)
                    .await
                    .buck_error_context("Test result reporting failed")?;

                Ok(test_status)
            })
            // Use an arbitrarily large buffer -- execution throttling will be handled by the Buck2
            // executor, so no need to hold back on requests here.
            .buffer_unordered(10000)
            // If any individual test failed, consider the entire run to have failed.
            .try_fold(
                RunVerdict::Pass,
                |mut run_verdict, test_status| async move {
                    if test_status != TestStatus::PASS {
                        run_verdict = RunVerdict::Fail;
                    }
                    buck2_error::Ok(run_verdict)
                },
            )
            .await;

        self.orchestrator_client
            .end_of_test_results(run_verdict?.exit_code())
            .await
    }

    async fn execute_test_from_spec(
        &self,
        spec: ExternalRunnerSpec,
    ) -> buck2_error::Result<ExecuteResponse> {
        let stage = TestStage::Testing {
            suite: spec.target.target,
            testcases: Vec::new(),
            variant: None,
            repeat_count: None,
        };

        let config_args = self.config.test_arg.iter().map(|arg| ArgValue {
            content: ArgValueContent::ExternalRunnerSpecValue(ExternalRunnerSpecValue::Verbatim(
                arg.to_owned(),
            )),
            format: None,
        });

        let command = spec
            .command
            .into_iter()
            .map(|spec_value| ArgValue {
                content: ArgValueContent::ExternalRunnerSpecValue(spec_value),
                format: None,
            })
            .chain(config_args)
            .collect();

        let config_env: Vec<_> = self
            .config
            .env
            .iter()
            .map(|s| s.parse())
            .collect::<buck2_error::Result<_>>()?;
        let config_env = config_env.iter().map(|EnvValue { name, value }| {
            (
                name.to_owned(),
                ArgValue {
                    content: ArgValueContent::ExternalRunnerSpecValue(
                        ExternalRunnerSpecValue::Verbatim(value.to_owned()),
                    ),
                    format: None,
                },
            )
        });

        let (test_output_dir, env) =
            test_env(spec.env, config_env, self.config.undeclared_outputs_dir);

        let target_handle = spec.target.handle;
        let host_sharing_requirements = HostSharingRequirements::default();
        let pre_create_dirs = test_output_dir.into_iter().collect();
        let executor_override = None;

        self.orchestrator_client
            .execute2(
                stage,
                target_handle,
                command,
                env,
                Duration::from_secs(self.config.timeout),
                host_sharing_requirements,
                pre_create_dirs,
                executor_override,
                RequiredLocalResources { resources: vec![] },
                false,
            )
            .await
    }

    async fn report_test_result(&self, test_result: TestResult) -> buck2_error::Result<()> {
        self.orchestrator_client
            .report_test_result(test_result)
            .await
    }
}

/// The variable that names a directory a test may write files into, collected
/// with its result. It is Bazel's name for the same directory, so a test
/// written for either finds it; Bazel gives an absolute path, and this one is
/// relative to the test's working directory, like every output path buck2
/// passes a test.
/// https://bazel.build/reference/test-encyclopedia#initial-conditions
const TEST_OUTPUT_DIR_ENV: &str = "TEST_UNDECLARED_OUTPUTS_DIR";

/// The output directory every test gets, and the variable naming its path.
///
/// It comes first in the environment, so a target's own `env` or the runner's
/// `--env` can still set the variable. `supports_remote` leaves a remote test's
/// directory in the CAS, listed in its action result, rather than downloading
/// every test's files; a local test's directory is written under buck-out.
fn test_output_dir() -> (DeclaredOutput, (String, ArgValue)) {
    let name = OutputName::unchecked_new(TEST_OUTPUTS_DIR_NAME.to_owned());
    (
        DeclaredOutput {
            name: name.clone(),
            remote_storage_config: RemoteStorageConfig::new(true),
        },
        (
            TEST_OUTPUT_DIR_ENV.to_owned(),
            ArgValue {
                content: ArgValueContent::DeclaredOutput(name),
                format: None,
            },
        ),
    )
}

/// A test's environment: its output directory's variable when the directory is
/// asked for, then the target's `env` with `LC_CTYPE` defaulted as mercury-head's
/// runner does it (`build_test_env`), then the runner's `--env`, each later one
/// winning over an earlier one of the same name.
///
/// The directory is an extra output and the variable an extra environment entry,
/// so either changes every test's action digest. Without them a test's action is
/// the one upstream buck2 runs, and can share cached results with it.
fn test_env(
    spec_env: impl IntoIterator<Item = (String, ExternalRunnerSpecValue)>,
    config_env: impl IntoIterator<Item = (String, ArgValue)>,
    undeclared_outputs_dir: bool,
) -> (Option<DeclaredOutput>, SortedVectorMap<String, ArgValue>) {
    let (test_output_dir, test_output_env) = if undeclared_outputs_dir {
        let (dir, env) = test_output_dir();
        (Some(dir), Some(env))
    } else {
        (None, None)
    };
    let env = test_output_env
        .into_iter()
        .chain(build_test_env(spec_env))
        .chain(config_env)
        .collect();
    (test_output_dir, env)
}

fn get_test_result(
    name: String,
    target: ConfiguredTargetHandle,
    execution_result: ExecutionResult2,
) -> TestResult {
    let status = match execution_result.status {
        ExecutionStatus::Finished { exitcode } => match exitcode {
            0 => TestStatus::PASS,
            _ => TestStatus::FAIL,
        },
        ExecutionStatus::TimedOut { .. } => TestStatus::TIMEOUT,
    };
    TestResult {
        target,
        name,
        status,
        msg: None,
        duration: Some(execution_result.execution_time),
        details: format!(
            "---- STDOUT ----\n{:?}\n---- STDERR ----\n{:?}\n",
            execution_result.stdout, execution_result.stderr
        ),
        max_memory_used_bytes: execution_result.max_memory_used_bytes,
    }
}

#[derive(Debug)]
enum RunVerdict {
    Pass,
    Fail,
}

impl RunVerdict {
    fn exit_code(&self) -> i32 {
        match self {
            RunVerdict::Pass => 0,
            RunVerdict::Fail => 32,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verbatim(value: &str) -> ExternalRunnerSpecValue {
        ExternalRunnerSpecValue::Verbatim(value.to_owned())
    }

    #[test]
    fn every_test_gets_one_output_dir_named_by_the_variable() {
        let (declared, env) = test_env(Vec::new(), Vec::new(), true);
        let declared = declared.expect("asked for an output dir");
        assert_eq!(declared.name.as_str(), "test_outputs");
        assert!(declared.remote_storage_config.supports_remote);
        match &env["TEST_UNDECLARED_OUTPUTS_DIR"].content {
            ArgValueContent::DeclaredOutput(output) => assert_eq!(output, &declared.name),
            other => panic!("the variable should name the declared output, got {other}"),
        }
    }

    #[test]
    fn a_target_s_own_value_wins() {
        let (_, env) = test_env(
            vec![(
                "TEST_UNDECLARED_OUTPUTS_DIR".to_owned(),
                verbatim("/elsewhere"),
            )],
            Vec::new(),
            true,
        );
        match &env["TEST_UNDECLARED_OUTPUTS_DIR"].content {
            ArgValueContent::ExternalRunnerSpecValue(ExternalRunnerSpecValue::Verbatim(v)) => {
                assert_eq!(v, "/elsewhere")
            }
            other => panic!("the target's value should win, got {other}"),
        }
    }

    #[test]
    fn unless_asked_a_test_gets_no_output_dir_and_no_variable() {
        let (declared, env) = test_env(vec![("A".to_owned(), verbatim("1"))], Vec::new(), false);
        assert!(declared.is_none());
        assert!(!env.contains_key("TEST_UNDECLARED_OUTPUTS_DIR"));
        // The target's own variable, and the LC_CTYPE default mercury-head's runner adds.
        assert_eq!(env.len(), 2);
        assert!(env.contains_key("LC_CTYPE"));
    }
}
