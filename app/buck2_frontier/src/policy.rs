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

/// Every type `ServerCommandDiceContext` (`buck2_server::ctx`) puts in a command's data, by its
/// full type name. A type this table does not list turns the frontier off for the command, so an
/// upstream merge that adds one fails closed; the frontier commands name the type.
///
/// The flags inside `CommandExecutorFactory` (`skip_cache_read`, `skip_cache_write`, the
/// execution strategy, upload-all) are not types of their own: the server records them beside
/// the type names (`buck2_server::frontier::CommandData`), and `skip_cache_read` turns the shadow
/// reader off, `skip_cache_write` the primer.
pub const FRONTIER_DATA_POLICY: &[(&str, DataClass)] = &[
    // The executor factory: its strategy and upload-all are recorded and in the version.
    (
        "buck2_build_api::actions::execute::dice_data::HasCommandExecutorHolder",
        DataClass::Version,
    ),
    // The fallback executor config, from the root config: in the startup-config component.
    (
        "alloc::sync::Arc<buck2_core::execution_types::executor_config::CommandExecutorConfig>",
        DataClass::Version,
    ),
    ("buck2_build_api::actions::impls::run_action_knobs::RunActionKnobs", DataClass::Version),
    // From the daemon's settings: in the daemon-settings component.
    (
        "buck2_common::settings::package_visibility::PackageVisibilityDefaultIntersection",
        DataClass::Version,
    ),
    // Whether a failure stops the build: not the value of any node that succeeded.
    ("buck2_build_api::keep_going::KeepGoingHolder", DataClass::Handle),
    ("buck2_execute::re::invocation_re_settings::InvocationReSettings", DataClass::Handle),
    ("buck2_execute::re::manager::UnconfiguredRemoteExecutionClient", DataClass::Handle),
    ("alloc::sync::Arc<buck2_execute::execute::low_pass_filter::LowPassFilter>", DataClass::Handle),
    ("alloc::sync::Arc<buck2_server_ctx::bxl::BxlStreamingTracker>", DataClass::Handle),
    (
        "alloc::sync::Arc<buck2_test::local_resource_registry::LocalResourceRegistry>",
        DataClass::Handle,
    ),
    ("alloc::sync::Arc<dyn buck2_build_api::build_signals::BuildSignals>", DataClass::Handle),
    (
        "alloc::sync::Arc<dyn buck2_execute::execute::blocking::BlockingExecutor>",
        DataClass::Handle,
    ),
    (
        "alloc::sync::Arc<dyn buck2_execute::materialize::materializer::Materializer>",
        DataClass::Handle,
    ),
    // The unhashed-symlink lock.
    ("alloc::sync::Arc<tokio::sync::mutex::Mutex<()>>", DataClass::Handle),
    ("buck2_build_api::actions::rewind::ActionRewindTrackerHolder", DataClass::Handle),
    (
        "buck2_build_api::build::detailed_aggregated_metrics::events::DetailedAggregatedMetricsPerBuildEventsHolder",
        DataClass::Handle,
    ),
    ("buck2_build_api::materialize::MaterializationQueueTrackerHolder", DataClass::Handle),
    ("buck2_build_signals::env::CriticalPathBackendName", DataClass::Handle),
    ("buck2_common::file_ops::io::ReadDirCache", DataClass::Handle),
    ("buck2_events::dispatch::EventDispatcher", DataClass::Handle),
    ("buck2_execute::dep_file_state::DepFileStoreHolder", DataClass::Handle),
    ("buck2_file_watcher::dep_files::DepFileCacheHolder", DataClass::Handle),
    ("buck2_http::client::HttpClient", DataClass::Handle),
    ("buck2_interpreter::dice::starlark_debug::StarlarkDebuggerHookHolder", DataClass::Handle),
    ("buck2_resource_control::HasResourceControl", DataClass::Handle),
];

pub fn classify(type_name: &str) -> Option<DataClass> {
    FRONTIER_DATA_POLICY
        .iter()
        .find(|(name, _)| *name == type_name)
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
