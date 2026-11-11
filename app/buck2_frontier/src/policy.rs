/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Which node kinds get records, and how each type of per-command data enters the frontier.

/// Kinds that get records, and why. A kind joins only when the determinism gate found its value
/// rows identical in every arm; analysis, `.bzl` loads and interpreter results stay out until the
/// heap-identity audit lands.
pub const RECORD_KINDS: &[(&str, &str)] = &[
    ("BuildKey", "records; the gate decides"),
    ("ConfiguredTargetNodeKey", "harness 13,720/13,720"),
    ("ConfigurationNodeKey", "harness 44/44"),
    ("MatchedConfigurationSettingKeysKey", "harness 28/28"),
    ("ExecutionPlatformResolutionKey", "harness 13/13"),
    ("ToolchainExecutionPlatformCompatibilityKey", "harness 7/7"),
    ("ExecDepCfgKey", "harness 4/4"),
    ("TransitionKey", "harness 2/2"),
    ("CfgConstructorInvocationKey", "harness 3/3"),
    ("PackageListingKey", "harness 7,800/7,800"),
    ("PackageFileLookupKey", "harness 2,779/2,779"),
    ("EnsureProjectedArtifactKey", "shadow only"),
    ("EnsureTransitiveSetProjectionKey", "shadow only"),
];

pub fn is_record_kind(kind: &str, narrowed: Option<&[String]>) -> bool {
    RECORD_KINDS.iter().any(|(k, _)| *k == kind)
        && narrowed.is_none_or(|n| n.iter().any(|k| k == kind))
}

/// How a type of per-command data (`UserComputationData`'s type map) enters the frontier.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DataClass {
    /// Its value changes what nodes compute: in the version.
    Version,
    /// A command that sets it must not read records (`--no-remote-cache`).
    NoRead,
    /// A command that sets it must not write records.
    NoWrite,
    /// A handle or tracker: what it reaches is configured at startup, so in the startup config.
    Handle,
}

/// Every type `ServerCommandDiceContext` puts in a command's data, by the path prefix of its
/// type name. A type this table does not classify turns the frontier off for the command; the
/// test in the server builds the data as `ctx.rs` does and fails on any unclassified type, so an
/// upstream merge that adds one fails in the fork's CI rather than in a job.
///
/// The flags inside `CommandExecutorFactory` (`skip_cache_read`, `skip_cache_write`, the
/// execution strategy, upload-all) are not types of their own: the server records them beside
/// the type names (see `buck2_server::frontier::CommandData`).
pub const FRONTIER_DATA_POLICY: &[(&str, DataClass)] = &[
    // Executor configuration: execution strategy, executor config, upload-all and the cache
    // flags live here; the flags are recorded separately.
    ("buck2_execute::execute::dice_data::", DataClass::Version),
    ("buck2_build_api::actions::execute::dice_data::", DataClass::Version),
    ("buck2_execute::re::manager::", DataClass::Handle),
    ("buck2_execute::materialize::", DataClass::Handle),
    ("buck2_execute::execute::blocking::", DataClass::Handle),
    ("buck2_common::http::", DataClass::Handle),
    ("buck2_common::package_listing::", DataClass::Handle),
    ("buck2_common::dice::", DataClass::Handle),
    ("buck2_events::", DataClass::Handle),
    ("buck2_build_signals::", DataClass::Handle),
    ("buck2_build_api::build_signals::", DataClass::Handle),
    ("buck2_build_api::keep_going::", DataClass::Version),
    ("buck2_build_api::actions::execute::", DataClass::Handle),
    ("buck2_build_api::materialize::", DataClass::Handle),
    ("buck2_build_api::actions::impls::run::", DataClass::Handle),
    ("buck2_build_api::deferred::", DataClass::Handle),
    ("buck2_build_api::bxl::", DataClass::Handle),
    ("buck2_build_api::interpreter::", DataClass::Handle),
    ("buck2_build_api::actions::calculation::", DataClass::Handle),
    ("buck2_build_api::actions::rewind::", DataClass::Handle),
    ("buck2_build_api::actions::local_actions::", DataClass::Handle),
    ("buck2_execute::digest_config::", DataClass::Version),
    ("buck2_interpreter::", DataClass::Handle),
    ("buck2_node::visibility::", DataClass::Version),
    ("buck2_critical_path::", DataClass::Handle),
    ("buck2_server_starlark_debug::", DataClass::Handle),
    ("buck2_server::", DataClass::Handle),
    ("buck2_execute::execute::", DataClass::Handle),
    ("buck2_execute_impl::", DataClass::Handle),
    ("buck2_common::io::", DataClass::Handle),
    ("buck2_util::", DataClass::Handle),
    ("buck2_file_watcher::", DataClass::Handle),
    ("buck2_common::local_resource_state::", DataClass::Handle),
    ("buck2_common::starlark_profiler::", DataClass::Handle),
    ("buck2_common::legacy_configs::", DataClass::Handle),
    ("buck2_common::file_ops::", DataClass::Handle),
    ("buck2_common::invocation_paths::", DataClass::Handle),
    ("buck2_common::memory::", DataClass::Handle),
    ("buck2_resource_control::", DataClass::Handle),
    ("buck2_execute::", DataClass::Handle),
    ("buck2_bxl::", DataClass::Handle),
    ("buck2_analysis::", DataClass::Handle),
    ("buck2_test::", DataClass::Handle),
    ("buck2_core::", DataClass::Handle),
    ("buck2_build_api::", DataClass::Handle),
    ("buck2_common::", DataClass::Handle),
    ("dice::", DataClass::Handle),
    ("alloc::sync::Arc<buck2_", DataClass::Handle),
];

pub fn classify(type_name: &str) -> Option<DataClass> {
    FRONTIER_DATA_POLICY
        .iter()
        .find(|(prefix, _)| type_name.starts_with(prefix))
        .map(|(_, c)| *c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_types_are_unclassified() {
        assert_eq!(classify("some_crate::Thing"), None);
        assert_eq!(classify("std::string::String"), None);
    }

    #[test]
    fn narrowing() {
        assert!(is_record_kind("BuildKey", None));
        assert!(!is_record_kind("AnalysisKey", None));
        let n = vec!["ConfiguredTargetNodeKey".to_owned()];
        assert!(!is_record_kind("BuildKey", Some(&n)));
        assert!(is_record_kind("ConfiguredTargetNodeKey", Some(&n)));
    }
}
