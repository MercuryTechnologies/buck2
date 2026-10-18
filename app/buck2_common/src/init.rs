/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::str::FromStr;
use std::time::Duration;

use allocative::Allocative;
use buck2_core::buck2_env;
use buck2_error::BuckErrorContext;
#[cfg(unix)]
use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;
use buck2_fs::paths::file_name::FileName;
#[cfg(fbcode_build)]
use buck2_http::ProxyHostAllowlist;
use dice::EvictionPolicy;
use dice::PagableStorageBackend;
use dice::PageOutPolicy;
use dupe::Dupe;
use serde::Deserialize;
use serde::Serialize;

use crate::legacy_configs::configs::LegacyBuckConfig;
use crate::legacy_configs::key::BuckconfigKeyRef;
use crate::settings::BuckSettings;

pub const DEFAULT_RETAINED_EVENT_LOGS: usize = 12;

/// Helper enum to categorize the kind of timeout we get from the startup config.
#[derive(Clone, Debug)]
pub enum Timeout {
    /// Timeout value is set in the config, use that.
    Value(Duration),
    /// Timeout value was not set in config, apply the default.
    Default,
    /// Timeout value was explicitly set to 0, meaning we shouldn't use a timeout.
    NoTimeout,
}

impl Timeout {
    pub fn new(value: Option<Duration>) -> Self {
        match value {
            Some(Duration::ZERO) => Self::NoTimeout,
            Some(value) => Self::Value(value),
            None => Self::Default,
        }
    }
}

#[derive(
    Allocative,
    Clone,
    Debug,
    Default,
    Serialize,
    Deserialize,
    PartialEq,
    Eq
)]
pub struct HttpConfig {
    #[cfg(fbcode_build)]
    pub proxy_env_allowlist: ProxyHostAllowlist,
    connect_timeout_ms: Option<u64>,
    read_timeout_ms: Option<u64>,
    write_timeout_ms: Option<u64>,
    pub http2: bool,
    pub max_redirects: Option<usize>,
    pub max_concurrent_requests: Option<usize>,
}

impl HttpConfig {
    pub fn from_config(config: &LegacyBuckConfig) -> buck2_error::Result<Self> {
        let connect_timeout_ms = config.parse(BuckconfigKeyRef {
            section: "http",
            property: "connect_timeout_ms",
        })?;
        let read_timeout_ms = config.parse(BuckconfigKeyRef {
            section: "http",
            property: "read_timeout_ms",
        })?;
        let write_timeout_ms = config.parse(BuckconfigKeyRef {
            section: "http",
            property: "write_timeout_ms",
        })?;
        let max_redirects = config.parse(BuckconfigKeyRef {
            section: "http",
            property: "max_redirects",
        })?;
        let http2 = config
            .parse(BuckconfigKeyRef {
                section: "http",
                property: "http2",
            })?
            .unwrap_or(true);
        let max_concurrent_requests = config.parse(BuckconfigKeyRef {
            section: "http",
            property: "max_concurrent_requests",
        })?;

        Ok(Self {
            #[cfg(fbcode_build)]
            proxy_env_allowlist: config
                .parse_list::<String>(BuckconfigKeyRef {
                    section: "http",
                    property: "proxy_env_allowlist",
                })?
                .unwrap_or_default()
                .try_into()?,
            connect_timeout_ms,
            read_timeout_ms,
            write_timeout_ms,
            http2,
            max_redirects,
            max_concurrent_requests,
        })
    }

    pub fn connect_timeout(&self) -> Timeout {
        match self.connect_timeout_ms.map(Duration::from_millis) {
            Some(Duration::ZERO) => Timeout::NoTimeout,
            Some(value) => Timeout::Value(value),
            None => Timeout::Default,
        }
    }

    pub fn read_timeout(&self) -> Timeout {
        match self.read_timeout_ms.map(Duration::from_millis) {
            Some(Duration::ZERO) => Timeout::NoTimeout,
            Some(value) => Timeout::Value(value),
            None => Timeout::Default,
        }
    }

    pub fn write_timeout(&self) -> Timeout {
        match self.write_timeout_ms.map(Duration::from_millis) {
            Some(Duration::ZERO) => Timeout::NoTimeout,
            Some(value) => Timeout::Value(value),
            None => Timeout::Default,
        }
    }
}

#[derive(
    Allocative,
    Clone,
    Debug,
    Default,
    Serialize,
    Deserialize,
    PartialEq,
    Eq
)]
pub struct SystemWarningConfig {
    /// A threshold that is used to determine the percent of memory buck2 uses to display memory pressure warnings.
    /// If None, we don't warn the user.
    /// The corresponding buckconfig is `buck2_system_warning.memory_pressure_threshold_percent`.
    pub memory_pressure_threshold_percent: Option<u64>,
    /// A threshold that is used to determine remaining disk space buck2 uses to display disk space warnings.
    /// If None, we don't warn the user.
    /// The corresponding buckconfig is `buck2_system_warning.remaining_disk_space_threshold`.
    pub remaining_disk_space_threshold_gb: Option<u64>,
    /// Minimum number of bytes downloaded to measure average download speed.
    /// If None, we don't warn the user.
    /// The corresponding buckconfig is `buck2_system_warning.min_re_download_bytes_threshold`.
    pub min_re_download_bytes_threshold: Option<u64>,
    /// A threshold that is used to determine if download speed is too low and display a warning.
    /// If None, we don't warn the user.
    /// The corresponding buckconfig is `buck2_system_warning.avg_re_download_bytes_per_sec_threshold`.
    pub avg_re_download_bytes_per_sec_threshold: Option<u64>,
    /// A regex that controls which targets are opted into the vpn check.
    /// The corresponding buckconfig is `buck2_health_check.optin_vpn_check_targets_regex`.
    pub optin_vpn_check_targets_regex: Option<String>,
    /// Whether to enable the stable revision check.
    pub enable_stable_revision_check: Option<bool>,
    /// Run the health checks in a separate process.
    pub enable_health_check_process_isolation: Option<bool>,
}

impl SystemWarningConfig {
    pub fn from_config(config: &LegacyBuckConfig) -> buck2_error::Result<Self> {
        let memory_pressure_threshold_percent = config.parse(BuckconfigKeyRef {
            section: "buck2_system_warning",
            property: "memory_pressure_threshold_percent",
        })?;
        let remaining_disk_space_threshold_gb = config.parse(BuckconfigKeyRef {
            section: "buck2_system_warning",
            property: "remaining_disk_space_threshold_gb",
        })?;
        let min_re_download_bytes_threshold = config.parse(BuckconfigKeyRef {
            section: "buck2_system_warning",
            property: "min_re_download_bytes_threshold",
        })?;
        let avg_re_download_bytes_per_sec_threshold = config.parse(BuckconfigKeyRef {
            section: "buck2_system_warning",
            property: "avg_re_download_bytes_per_sec_threshold",
        })?;
        let optin_vpn_check_targets_regex = config.parse(BuckconfigKeyRef {
            section: "buck2_health_check",
            property: "optin_vpn_check_targets_regex",
        })?;
        let enable_stable_revision_check = config.parse(BuckconfigKeyRef {
            section: "buck2_health_check",
            property: "enable_stable_revision_check",
        })?;
        let enable_health_check_process_isolation = config.parse(BuckconfigKeyRef {
            section: "buck2_health_check",
            property: "enable_health_check_process_isolation",
        })?;
        Ok(Self {
            memory_pressure_threshold_percent,
            remaining_disk_space_threshold_gb,
            min_re_download_bytes_threshold,
            avg_re_download_bytes_per_sec_threshold,
            optin_vpn_check_targets_regex,
            enable_stable_revision_check,
            enable_health_check_process_isolation,
        })
    }

    pub fn serialize(&self) -> buck2_error::Result<String> {
        serde_json::to_string(&self).buck_error_context("Error serializing SystemWarningConfig")
    }

    pub fn deserialize(s: &str) -> buck2_error::Result<Self> {
        serde_json::from_str::<Self>(s)
            .buck_error_context("Error deserializing SystemWarningConfig")
    }
}

#[derive(Allocative, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceControlConfig {
    /// A config to determine if the resource control should be activated or not.
    /// The corresponding buckconfig is `buck2_resource_control.status` that can take
    /// one of `{off | if_available | required}`.
    pub status: ResourceControlStatus,
    /// If resource control is enabled, buck needs to get a cgroup to run in from somewhere - this is
    /// where.
    pub init: ResourceControlInit,
    /// Maximum allowed memory usage for all work buck2 manages.
    ///
    /// Accepts either a number of bytes or a percentage of the available resources.
    ///
    /// The corresponding buckconfig is `buck2_resource_control.memory_max`.
    pub memory_max: Option<String>,
    /// Like `memory_max`, but controls cgroupv2's `memory.high`
    ///
    /// The corresponding buckconfig is `buck2_resource_control.memory_high`.
    pub memory_high: Option<String>,
    /// A memory threshold that any action is allowed to allocate.
    pub memory_max_per_action: Option<String>,
    /// A memory threshold that any action is allowed to reach before being throttled.
    pub memory_high_per_action: Option<String>,
    /// Memory high limit for all actions.
    ///
    /// Mainly for testing purpose.
    pub memory_high_actions: Option<String>,
    /// Memory max limit for all actions.
    ///
    /// Mainly for testing purpose.
    pub memory_max_actions: Option<String>,
    /// Value buck2 writes to `memory.swap.max` on the actions-pool cgroup (the cgroup shared by all
    /// running actions); the kernel enforces it from there. Accepts a byte count or `max`.
    ///
    /// Setting it to `0` disables swap for actions, so once `memory_high_actions` /
    /// `memory_max_actions` is reached the kernel must reclaim by stalling the action rather than
    /// paging its pages out. That stall is what produces the sustained memory pressure the
    /// scheduler needs to trigger suspension.
    ///
    /// Mainly for testing purpose.
    #[serde(default)]
    pub memory_swap_max_actions: Option<String>,
    /// Enable suspension when memory pressure is high.
    pub enable_suspension: bool,
    pub experimental_suspension_algo_variant: Option<u8>,
    pub preferred_action_suspend_strategy: ActionSuspendStrategy,
}

impl ResourceControlConfig {
    pub fn testing_default() -> Self {
        Self::from_config(&LegacyBuckConfig::empty()).unwrap()
    }
}

#[derive(Allocative, Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ActionSuspendStrategy {
    CgroupFreeze,
    KillAndRetry,
}

impl FromStr for ActionSuspendStrategy {
    type Err = buck2_error::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "kill_and_retry" => Ok(Self::KillAndRetry),
            "cgroup_freeze" => Ok(Self::CgroupFreeze),
            _ => Err(buck2_error::buck2_error!(
                buck2_error::ErrorTag::Input,
                "Invalid suspend strategy: `{}`",
                s
            )),
        }
    }
}

#[derive(
    Allocative,
    Clone,
    Copy,
    Debug,
    Default,
    Serialize,
    Deserialize,
    PartialEq,
    Eq
)]
pub enum ResourceControlStatus {
    #[default]
    /// The resource is not controlled or limited.
    Off,
    /// The resource is controlled by `systemd` if it's available on the system, otherwise off.
    IfAvailable,
    /// The resource is controlled by `systemd`. If it is not available on the system,
    /// buck2 errors it out and the command returns with an error exit code.
    Required,
}

impl FromStr for ResourceControlStatus {
    type Err = buck2_error::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "off" => Ok(Self::Off),
            "if_available" => Ok(Self::IfAvailable),
            "required" => Ok(Self::Required),
            _ => Err(buck2_error::buck2_error!(
                buck2_error::ErrorTag::Input,
                "Invalid resource control status: `{}`",
                s
            )),
        }
    }
}

#[derive(Allocative, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ResourceControlInit {
    Systemd,
    #[cfg(unix)]
    Cgroup(AbsNormPathBuf),
}

impl FromStr for ResourceControlInit {
    type Err = buck2_error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "systemd" {
            return Ok(ResourceControlInit::Systemd);
        }
        #[cfg(unix)]
        if let Some(p) = s.strip_prefix("cgroup:") {
            return Ok(ResourceControlInit::Cgroup(AbsNormPathBuf::from(
                p.to_owned(),
            )?));
        }
        Err(buck2_error::buck2_error!(
            buck2_error::ErrorTag::Input,
            "Unknown resource control initializer: `{}`",
            s
        ))
    }
}

/// The current version of the resource control algorithm. Say you have some important change to the
/// algo that fixes a bug. Incrementing this to `N + 1` and setting the
/// `buck2_resource_control.enable_suspension_if_min_algo_version` buckconfig to `N + 1` enables
/// suspension only if your bug fix is actually included in the version of buck in use
const RESOURCE_CONTROL_ALGO_VERSION: u32 = 6;

/// The current version of the daemon cgroup wrapping logic. Incrementing this to `N + 1` and
/// setting `buck2_resource_control.min_version_for_gated_status` buckconfig to `N + 1` enables the
/// gated default status (`buck2_resource_control.version_gated_default_status`, defaulting to
/// `if_available`) only if the bug fix is included in the version of buck in use.
const DAEMON_CGROUP_VERSION: u32 = 1;

impl ResourceControlConfig {
    pub fn from_config(config: &LegacyBuckConfig) -> buck2_error::Result<Self> {
        if let Some(env_conf) = buck2_env!(
            "BUCK2_TEST_RESOURCE_CONTROL_CONFIG",
            applicability = testing,
        )? {
            Self::deserialize(env_conf)
        } else {
            let status: Option<ResourceControlStatus> = config.parse(BuckconfigKeyRef {
                section: "buck2_resource_control",
                property: "status",
            })?;
            let status = if let Some(status) = status {
                status
            } else {
                let min_version_for_gated_status: Option<u32> = config.parse(BuckconfigKeyRef {
                    section: "buck2_resource_control",
                    property: "min_version_for_gated_status",
                })?;
                if min_version_for_gated_status
                    .is_some_and(|min_version| DAEMON_CGROUP_VERSION >= min_version)
                {
                    config
                        .parse(BuckconfigKeyRef {
                            section: "buck2_resource_control",
                            property: "version_gated_default_status",
                        })?
                        .unwrap_or(ResourceControlStatus::IfAvailable)
                } else {
                    ResourceControlStatus::Off
                }
            };
            let init = config
                .parse(BuckconfigKeyRef {
                    section: "buck2_resource_control",
                    property: "init",
                })?
                .unwrap_or(ResourceControlInit::Systemd);
            let memory_max = config.parse(BuckconfigKeyRef {
                section: "buck2_resource_control",
                property: "memory_max",
            })?;
            let memory_high = config.parse(BuckconfigKeyRef {
                section: "buck2_resource_control",
                property: "memory_high",
            })?;
            let memory_max_per_action = config.parse(BuckconfigKeyRef {
                section: "buck2_resource_control",
                property: "memory_max_per_action",
            })?;
            let memory_high_per_action = config.parse(BuckconfigKeyRef {
                section: "buck2_resource_control",
                property: "memory_high_per_action",
            })?;
            let memory_high_actions = config.parse(BuckconfigKeyRef {
                section: "buck2_resource_control",
                property: "memory_high_actions",
            })?;
            let memory_max_actions = config.parse(BuckconfigKeyRef {
                section: "buck2_resource_control",
                property: "memory_max_actions",
            })?;
            let memory_swap_max_actions = config.parse(BuckconfigKeyRef {
                section: "buck2_resource_control",
                property: "memory_swap_max_actions",
            })?;
            let enable_suspension = config.parse(BuckconfigKeyRef {
                section: "buck2_resource_control",
                property: "enable_suspension",
            })?;
            let enable_suspension_if_min_algo_version: Option<u32> =
                config.parse(BuckconfigKeyRef {
                    section: "buck2_resource_control",
                    property: "enable_suspension_if_min_algo_version",
                })?;
            let enable_suspension = enable_suspension.unwrap_or(false)
                || enable_suspension_if_min_algo_version
                    .is_some_and(|min_version| RESOURCE_CONTROL_ALGO_VERSION >= min_version);
            let experimental_suspension_algo_variant = config.parse(BuckconfigKeyRef {
                section: "buck2_resource_control",
                property: "experimental_suspension_algo_variant",
            })?;
            let preferred_action_suspend_strategy = config
                .parse(BuckconfigKeyRef {
                    section: "buck2_resource_control",
                    property: "preferred_action_suspend_strategy",
                })?
                .unwrap_or(ActionSuspendStrategy::KillAndRetry);
            Ok(Self {
                status,
                init,
                memory_max,
                memory_high,
                memory_max_per_action,
                memory_high_per_action,
                memory_high_actions,
                memory_max_actions,
                memory_swap_max_actions,
                enable_suspension,
                experimental_suspension_algo_variant,
                preferred_action_suspend_strategy,
            })
        }
    }

    pub fn serialize(&self) -> buck2_error::Result<String> {
        serde_json::to_string(&self).buck_error_context("Error serializing ResourceControlConfig")
    }

    pub fn deserialize(s: &str) -> buck2_error::Result<Self> {
        serde_json::from_str::<Self>(s)
            .buck_error_context("Error deserializing ResourceControlConfig")
    }
}

#[derive(Allocative, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogDownloadMethod {
    Manifold,
    Curl(String),
    None,
}

#[derive(
    Allocative,
    Clone,
    Debug,
    Default,
    Serialize,
    Deserialize,
    PartialEq,
    Eq
)]
pub struct HealthCheckConfig {
    pub enable_health_checks: bool,
    pub disabled_health_check_names: Option<String>,
}

impl HealthCheckConfig {
    pub fn from_config(config: &LegacyBuckConfig) -> buck2_error::Result<Self> {
        let enable_health_checks = config.parse(BuckconfigKeyRef {
            section: "buck2_health_check",
            property: "enable_health_checks",
        })?;
        let disabled_health_check_names = config.parse(BuckconfigKeyRef {
            section: "buck2_health_check",
            property: "disabled_health_check_names",
        })?;

        Ok(Self {
            // TODO(rajneeshl): When the rollout is successful, change this to default to true.
            enable_health_checks: enable_health_checks.unwrap_or(false),
            disabled_health_check_names,
        })
    }
}

/// Pagable DICE storage settings, present (`Some`) only when paging is enabled by
/// the `hydration` Buck settings or their legacy `buck2_hydration` fallbacks.
/// `page_out_on_idle` also implies paging is enabled. When present, the daemon
/// sets up on-disk storage during construction so `buck2 debug hydration` can
/// page node values out to / in from disk. Read at startup because it gates that
/// setup.
#[derive(Allocative, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HydrationConfig {
    /// On-disk backend for pagable storage.
    pub pagable_storage_backend: PagableStorageBackend,
    /// Automatically page the graph out to disk when the daemon goes idle.
    pub page_out_on_idle: bool,
    /// Idle page-out only runs when at least this many GiB of disk are free to
    /// write the paged-out values to.
    pub page_out_min_free_disk_gb: u64,
    /// Which values a page-out writes; see [`PageOutPolicy`].
    pub page_out_policy: PageOutPolicy,
    /// When the values a page-out writes leave memory; see [`EvictionPolicy`].
    pub eviction_policy: EvictionPolicy,
    /// `buck2 suspend` and resume, `Some` when `hydration.suspend` (legacy
    /// `buck2_hydration.suspend`) is true. Part of the startup config, so turning it on or off
    /// restarts the daemon.
    pub suspend: Option<SuspendConfig>,
}

/// Settings for `buck2 suspend` and resuming from what it wrote.
#[derive(Allocative, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SuspendConfig {
    /// A suspended daemon whose paging store is larger than this many GiB is not resumed
    /// (`buck2_hydration.suspend_max_db_gb`): the store only grows, and a cold start is how it
    /// is reclaimed.
    pub max_db_gb: u64,
    /// Environment variables, beyond the built-in per-run ones and credentials, whose values may
    /// differ between the suspended daemon and the resumed one
    /// (`buck2_hydration.suspend_env_ignore`, comma-separated; a name ending in `*` is a prefix
    /// and one starting with `*` a suffix). Unset, it is [`DEFAULT_SUSPEND_ENV_IGNORE`]; set, it
    /// replaces that list, and set empty it ignores nothing more.
    pub env_ignore: Vec<String>,
    /// The command a resume runs, with one store path appended, to ask whether that path is
    /// valid on this machine (`buck2_hydration.suspend_store_check`, split on whitespace, such as
    /// `nix-store --check-validity`); exit status 0 means valid. A locally executed action that
    /// declared `store_path_outputs` keeps its value across a resume on another machine only
    /// when each of its paths exists and, if this is set, passes it. Unset, existence is enough.
    pub store_check: Vec<String>,
}

/// The variables a CI runner sets differently on every run of one job, beyond those the daemon
/// always ignores (`GITHUB_*`, `RUNNER_*` and the like): the default of
/// `buck2_hydration.suspend_env_ignore`. Leaving them out is sound only because the actions that
/// inherit the daemon's environment, the local ones, run again on a resume that sees other values
/// for them, or another host, or a portable image.
pub const DEFAULT_SUSPEND_ENV_IGNORE: &[&str] = &[
    "ACTIONS_*",
    "GIT_BASE_BRANCH",
    "GIT_BRANCH",
    "GIT_COMMIT",
    "GIT_PR_NUMBER",
    "ImageOS",
    "ImageVersion",
];

impl HydrationConfig {
    /// Returns `None` when neither the Buck settings nor their legacy fallbacks
    /// enable paging.
    fn from_config(
        config: &LegacyBuckConfig,
        settings: &BuckSettings,
    ) -> buck2_error::Result<Option<Self>> {
        fn resolve_bool(
            setting: Option<bool>,
            config: &LegacyBuckConfig,
            property: &'static str,
        ) -> buck2_error::Result<bool> {
            if let Some(setting) = setting {
                return Ok(setting);
            }

            Ok(config
                .parse(BuckconfigKeyRef {
                    section: "buck2_hydration",
                    property,
                })?
                .unwrap_or(false))
        }

        let page_out_on_idle = resolve_bool(
            settings.hydration.page_out_on_idle(),
            config,
            "page_out_on_idle",
        )?;
        // `page_out_on_idle` implies pagable storage, so it enables it too.
        let enabled = page_out_on_idle
            || resolve_bool(settings.hydration.enable_paging(), config, "enable_paging")?;
        let suspend = resolve_bool(settings.hydration.suspend(), config, "suspend")?;
        let page_out_policy = settings.hydration.page_out_policy();
        if suspend {
            // A suspend writes what paging does not; it needs paging to have written the rest,
            // and every value it reads back again to have a row.
            if !enabled {
                return Err(buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Input,
                    "`hydration.suspend` needs `hydration.enable_paging = true`"
                ));
            }
            if page_out_policy == PageOutPolicy::OncePerKey {
                return Err(buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Input,
                    "`hydration.suspend` needs `hydration.page_out_policy = once_per_value`: \
                     under `once_per_key` a value recomputed after a page-out never gets a row"
                ));
            }
        }
        if !enabled {
            return Ok(None);
        }
        let suspend = if suspend {
            Some(SuspendConfig {
                max_db_gb: config
                    .parse(BuckconfigKeyRef {
                        section: "buck2_hydration",
                        property: "suspend_max_db_gb",
                    })?
                    .unwrap_or(50),
                env_ignore: match config.get(BuckconfigKeyRef {
                    section: "buck2_hydration",
                    property: "suspend_env_ignore",
                }) {
                    Some(list) => list
                        .split(',')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(ToOwned::to_owned)
                        .collect(),
                    None => DEFAULT_SUSPEND_ENV_IGNORE
                        .iter()
                        .map(|s| (*s).to_owned())
                        .collect(),
                },
                store_check: config
                    .get(BuckconfigKeyRef {
                        section: "buck2_hydration",
                        property: "suspend_store_check",
                    })
                    .map(|c| c.split_whitespace().map(ToOwned::to_owned).collect())
                    .unwrap_or_default(),
            })
        } else {
            None
        };
        Ok(Some(Self {
            pagable_storage_backend: config
                .parse::<PagableStorageBackend>(BuckconfigKeyRef {
                    section: "buck2_hydration",
                    property: "pagable_storage_backend",
                })?
                .unwrap_or_default(),
            page_out_on_idle,
            page_out_min_free_disk_gb: config
                .parse(BuckconfigKeyRef {
                    section: "buck2_hydration",
                    property: "page_out_min_free_disk_gb",
                })?
                .unwrap_or(100),
            page_out_policy,
            eviction_policy: settings.hydration.eviction_policy(),
            suspend,
        }))
    }
}

/// The legacy buckconfig key for the host identity, in `buck2_hydration`. A digest of the root
/// buckconfig leaves it out, as [`DaemonStartupConfig::without_host_identity`] does for the
/// startup config.
pub const HOST_IDENTITY_PROPERTY: &str = "host_identity";

/// Configurations that are used at startup by the daemon. Those are actually read by the client,
/// and passed on to the daemon.
///
/// The fields here are often raw String we get from the buckconfig, the daemon will do
/// deserialization once it receives them. That said, this is not a requirement.
///
/// Backwards compatibility on Serialize / Deserialize is not required: if the client cannot read
/// the DaemonStartupConfig provided by the daemon when it tries to connect, it will reject that
/// daemon and restart (and in fact it will probably not get that far since a version check is done
/// before parsing DaemonStartupConfig).
#[derive(Allocative, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DaemonStartupConfig {
    pub buck_settings: BuckSettings,
    pub num_tokio_workers: Option<usize>,
    pub daemon_buster: Option<String>,
    pub digest_algorithms: Option<String>,
    pub source_digest_algorithm: Option<String>,
    pub paranoid: bool,
    pub materializations: Option<String>,
    /// Which materializer implementation the daemon runs (`buck2.materializer`); `None` is the
    /// deferred one.
    pub materializer: Option<String>,
    pub http: HttpConfig,
    pub resource_control: ResourceControlConfig,
    pub log_download_method: LogDownloadMethod,
    pub health_check_config: HealthCheckConfig,
    pub retained_event_logs: usize,
    pub macos_qos_class: Option<String>,
    pub daemon_idle_timeout_s: Option<u64>,
    /// Pagable DICE storage settings, or `None` when paging is disabled.
    pub hydration: Option<HydrationConfig>,
    /// What stands for this machine in the on-disk state buck2 keeps across daemons (the suspend
    /// MANIFEST and the materializer, dep-file and incremental databases): `hydration.host_identity`,
    /// legacy `buck2_hydration.host_identity`. `None` means the hostname. Two machines with the same
    /// identity are trusted to run local actions the same way, so CI derives it from what those
    /// actions depend on (the runner image's digest or a declared toolchain digest, and the
    /// architecture) rather than from the machine's name, and never from the whole NixOS system,
    /// which embeds the hostname; `crate::suspend_image` gives the rule, and the system check
    /// a resume makes beside it. Part of the startup config because the databases are opened
    /// once, at startup.
    pub host_identity: Option<String>,
}

impl DaemonStartupConfig {
    pub fn new(
        config: &LegacyBuckConfig,
        settings: &BuckSettings,
        paranoid: bool,
    ) -> buck2_error::Result<Self> {
        // Intepreted client side because we need the value here.

        let log_download_method = {
            // Determine the log download method to use. Only default to
            // manifold in fbcode contexts, or when specifically asked.
            let use_manifold = settings
                .log_download
                .log_use_manifold()
                .map(Ok::<_, buck2_error::Error>)
                .unwrap_or_else(|| {
                    Ok(config
                        .parse(BuckconfigKeyRef {
                            section: "buck2",
                            property: "log_use_manifold",
                        })?
                        .unwrap_or(cfg!(fbcode_build)))
                })?;

            if use_manifold {
                Ok(LogDownloadMethod::Manifold)
            } else {
                let log_url = settings.log_download.log_url().or_else(|| {
                    config.get(BuckconfigKeyRef {
                        section: "buck2",
                        property: "log_url",
                    })
                });
                if let Some(log_url) = log_url {
                    if log_url.is_empty() {
                        Err(buck2_error::buck2_error!(
                            buck2_error::ErrorTag::Input,
                            "log_url is empty, but log_use_manifold is false"
                        ))
                    } else {
                        Ok(LogDownloadMethod::Curl(log_url.to_owned()))
                    }
                } else {
                    Ok(LogDownloadMethod::None)
                }
            }
        }?;

        Ok(Self {
            buck_settings: settings.dupe(),
            num_tokio_workers: config
                .parse(BuckconfigKeyRef {
                    section: "build",
                    property: "num_tokio_workers",
                })
                .unwrap_or(Some(0)),
            daemon_buster: config
                .get(BuckconfigKeyRef {
                    section: "buck2",
                    property: "daemon_buster",
                })
                .map(ToOwned::to_owned),
            digest_algorithms: config
                .get(BuckconfigKeyRef {
                    section: "buck2",
                    property: "digest_algorithms",
                })
                .map(ToOwned::to_owned),
            source_digest_algorithm: config
                .get(BuckconfigKeyRef {
                    section: "buck2",
                    property: "source_digest_algorithm",
                })
                .map(ToOwned::to_owned),
            paranoid,
            materializations: config
                .get(BuckconfigKeyRef {
                    section: "buck2",
                    property: "materializations",
                })
                .map(ToOwned::to_owned),
            materializer: config
                .get(BuckconfigKeyRef {
                    section: "buck2",
                    property: "materializer",
                })
                .map(ToOwned::to_owned),
            http: HttpConfig::from_config(config)?,
            resource_control: ResourceControlConfig::from_config(config)?,
            log_download_method,
            health_check_config: HealthCheckConfig::from_config(config)?,
            retained_event_logs: config
                .get(BuckconfigKeyRef {
                    section: "buck2",
                    property: "retained_event_logs",
                })
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(DEFAULT_RETAINED_EVENT_LOGS),
            macos_qos_class: {
                let from_config = config
                    .get(BuckconfigKeyRef {
                        section: "buck2",
                        property: "macos_qos_class",
                    })
                    .map(ToOwned::to_owned);
                if buck2_env!("BUCK2_DISABLE_MACOS_QOS", bool)? {
                    buck2_core::soft_error!(
                        "disable_macos_qos_env_var",
                        buck2_error::buck2_error!(
                            buck2_error::ErrorTag::Input,
                            "BUCK2_DISABLE_MACOS_QOS is deprecated. \
                             Use `[buck2] macos_qos_class = skip_lowering` in buckconfig instead. \
                             This will be the default very soon."
                        ),
                        deprecation: true,
                        quiet: false,
                        error_on_oss: true
                    )?;
                    Some(from_config.unwrap_or_else(|| "skip_lowering".to_owned()))
                } else {
                    from_config
                }
            },
            daemon_idle_timeout_s: config.parse(BuckconfigKeyRef {
                section: "buck2",
                property: "daemon_idle_timeout_s",
            })?,
            hydration: HydrationConfig::from_config(config, settings)?,
            host_identity: match settings.hydration.host_identity() {
                Some(identity) => Some(identity.to_owned()),
                None => config
                    .get(BuckconfigKeyRef {
                        section: "buck2_hydration",
                        property: HOST_IDENTITY_PROPERTY,
                    })
                    .map(ToOwned::to_owned),
            }
            .filter(|identity| !identity.is_empty()),
        })
    }

    /// This config with the host identity cleared wherever it can be set, so that a digest of it
    /// changes with every startup setting except the identity, which is checked on its own.
    pub fn without_host_identity(&self) -> Self {
        let mut config = self.clone();
        config.host_identity = None;
        config.buck_settings.hydration = config.buck_settings.hydration.without_host_identity();
        config
    }

    /// Returns the automatic idle page-out configuration for this daemon.
    /// Pagable storage may remain enabled even when idle page-out is out of scope.
    pub fn idle_page_out_config_for_isolation_dir(
        &self,
        isolation_dir: &FileName,
    ) -> Option<&HydrationConfig> {
        self.hydration.as_ref().filter(|hydration| {
            hydration.page_out_on_idle
                && self
                    .buck_settings
                    .hydration
                    .page_out_on_idle_applies_to_isolation_dir(isolation_dir)
        })
    }

    pub fn serialize(&self) -> buck2_error::Result<String> {
        serde_json::to_string(&self).buck_error_context("Error serializing DaemonStartupConfig")
    }

    pub fn deserialize(s: &str) -> buck2_error::Result<Self> {
        serde_json::from_str::<Self>(s)
            .buck_error_context("Error deserializing DaemonStartupConfig")
    }

    pub fn testing_empty() -> Self {
        Self {
            buck_settings: BuckSettings::empty(),
            num_tokio_workers: None,
            daemon_buster: None,
            digest_algorithms: None,
            source_digest_algorithm: None,
            paranoid: false,
            materializations: None,
            materializer: None,
            http: HttpConfig::default(),
            resource_control: ResourceControlConfig::testing_default(),
            log_download_method: if cfg!(fbcode_build) {
                LogDownloadMethod::Manifold
            } else {
                LogDownloadMethod::None
            },
            health_check_config: HealthCheckConfig::default(),
            retained_event_logs: DEFAULT_RETAINED_EVENT_LOGS,
            macos_qos_class: None,
            daemon_idle_timeout_s: None,
            hydration: None,
            host_identity: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;
    use crate::legacy_configs::configs::testing::parse;
    use crate::settings::parser::resolve_setting_flags;
    use crate::settings::parser::table;

    #[test]
    fn test_daemon_idle_timeout_s_default() -> buck2_error::Result<()> {
        let config = parse(&[("config", indoc!(r#""#))], "config")?;
        let startup_config = DaemonStartupConfig::new(&config, &BuckSettings::empty(), false)?;
        assert_eq!(startup_config.daemon_idle_timeout_s, None);
        Ok(())
    }

    #[test]
    fn test_daemon_idle_timeout_s_configured() -> buck2_error::Result<()> {
        let config = parse(
            &[(
                "config",
                indoc!(
                    r#"
                    [buck2]
                    daemon_idle_timeout_s = 10800
                    "#
                ),
            )],
            "config",
        )?;
        let startup_config = DaemonStartupConfig::new(&config, &BuckSettings::empty(), false)?;
        assert_eq!(startup_config.daemon_idle_timeout_s, Some(10800));
        Ok(())
    }

    #[test]
    fn test_hydration_settings_enable_idle_page_out() -> buck2_error::Result<()> {
        let config = parse(&[("config", "")], "config")?;
        let settings = resolve_setting_flags(vec![table("[hydration]\npage_out_on_idle = true")])?;

        let startup_config = DaemonStartupConfig::new(&config, &settings, false)?;
        let hydration = startup_config
            .idle_page_out_config_for_isolation_dir(
                FileName::new("v2").expect("The default isolation dir should be valid"),
            )
            .expect("Idle page-out should enable hydration");
        assert!(hydration.page_out_on_idle);
        Ok(())
    }

    #[test]
    fn test_hydration_settings_page_out_policy() -> buck2_error::Result<()> {
        let config = parse(&[("config", "")], "config")?;
        let settings = resolve_setting_flags(vec![table(
            "[hydration]\nenable_paging = true\npage_out_policy = \"once_per_key\"\neviction_policy = \"on_idle\"",
        )])?;

        let startup_config = DaemonStartupConfig::new(&config, &settings, false)?;
        let hydration = startup_config
            .hydration
            .expect("enable_paging should enable pagable storage");
        assert_eq!(hydration.page_out_policy, PageOutPolicy::OncePerKey);
        assert_eq!(hydration.eviction_policy, EvictionPolicy::OnIdle);
        Ok(())
    }

    #[test]
    fn test_host_identity_defaults_to_none() -> buck2_error::Result<()> {
        let config = parse(&[("config", "")], "config")?;
        let startup_config = DaemonStartupConfig::new(&config, &BuckSettings::empty(), false)?;
        assert_eq!(startup_config.host_identity, None);
        Ok(())
    }

    #[test]
    fn test_host_identity_from_legacy_config_and_settings() -> buck2_error::Result<()> {
        let config = parse(
            &[("config", "[buck2_hydration]\nhost_identity = legacy\n")],
            "config",
        )?;
        let startup_config = DaemonStartupConfig::new(&config, &BuckSettings::empty(), false)?;
        assert_eq!(startup_config.host_identity.as_deref(), Some("legacy"));

        let settings = resolve_setting_flags(vec![table(
            "[hydration]\nhost_identity = \"img=sha256:abc;arch=x86_64\"",
        )])?;
        let startup_config = DaemonStartupConfig::new(&config, &settings, false)?;
        assert_eq!(
            startup_config.host_identity.as_deref(),
            Some("img=sha256:abc;arch=x86_64")
        );
        Ok(())
    }

    #[test]
    fn test_empty_host_identity_means_the_hostname() -> buck2_error::Result<()> {
        let config = parse(
            &[("config", "[buck2_hydration]\nhost_identity =\n")],
            "config",
        )?;
        let startup_config = DaemonStartupConfig::new(&config, &BuckSettings::empty(), false)?;
        assert_eq!(startup_config.host_identity, None);
        Ok(())
    }

    #[test]
    fn test_without_host_identity_clears_every_source() -> buck2_error::Result<()> {
        let config = parse(&[("config", "")], "config")?;
        let plain = DaemonStartupConfig::new(&config, &BuckSettings::empty(), false)?;
        let settings = resolve_setting_flags(vec![table("[hydration]\nhost_identity = \"a\"")])?;
        let with = DaemonStartupConfig::new(&config, &settings, false)?;
        assert_ne!(plain.serialize()?, with.serialize()?);
        assert_eq!(
            plain.without_host_identity().serialize()?,
            with.without_host_identity().serialize()?
        );
        Ok(())
    }

    #[test]
    fn test_suspend_is_off_by_default() -> buck2_error::Result<()> {
        let config = parse(
            &[("config", "[buck2_hydration]\nenable_paging = true\n")],
            "config",
        )?;
        let startup_config = DaemonStartupConfig::new(&config, &BuckSettings::empty(), false)?;
        let hydration = startup_config.hydration.expect("paging is on");
        assert_eq!(hydration.suspend, None);
        Ok(())
    }

    #[test]
    fn test_suspend_from_legacy_config_with_its_options() -> buck2_error::Result<()> {
        let config = parse(
            &[(
                "config",
                indoc!(
                    r#"
                    [buck2_hydration]
                    enable_paging = true
                    suspend = true
                    suspend_max_db_gb = 7
                    suspend_env_ignore = CI_JOB_ID, BUILD_NUMBER
                    suspend_store_check = nix-store --check-validity
                    "#
                ),
            )],
            "config",
        )?;
        let startup_config = DaemonStartupConfig::new(&config, &BuckSettings::empty(), false)?;
        let suspend = startup_config
            .hydration
            .expect("paging is on")
            .suspend
            .expect("suspend is on");
        assert_eq!(suspend.max_db_gb, 7);
        assert_eq!(suspend.env_ignore, vec!["CI_JOB_ID", "BUILD_NUMBER"]);
        assert_eq!(suspend.store_check, vec!["nix-store", "--check-validity"]);
        Ok(())
    }

    #[test]
    fn test_suspend_setting_defaults() -> buck2_error::Result<()> {
        let config = parse(&[("config", "")], "config")?;
        let settings = resolve_setting_flags(vec![table(
            "[hydration]\nenable_paging = true\nsuspend = true",
        )])?;
        let startup_config = DaemonStartupConfig::new(&config, &settings, false)?;
        let suspend = startup_config
            .hydration
            .expect("paging is on")
            .suspend
            .expect("suspend is on");
        assert_eq!(suspend.max_db_gb, 50);
        assert_eq!(suspend.env_ignore, DEFAULT_SUSPEND_ENV_IGNORE);
        assert!(suspend.store_check.is_empty());
        Ok(())
    }

    #[test]
    fn test_suspend_refuses_without_paging() -> buck2_error::Result<()> {
        let config = parse(
            &[("config", "[buck2_hydration]\nsuspend = true\n")],
            "config",
        )?;
        let error = DaemonStartupConfig::new(&config, &BuckSettings::empty(), false)
            .expect_err("suspend without paging is refused");
        assert!(
            error.to_string().contains("hydration.enable_paging"),
            "the refusal names the key: {error}"
        );
        Ok(())
    }

    #[test]
    fn test_suspend_refuses_once_per_key() -> buck2_error::Result<()> {
        let config = parse(&[("config", "")], "config")?;
        let settings = resolve_setting_flags(vec![table(
            "[hydration]\nenable_paging = true\nsuspend = true\npage_out_policy = \"once_per_key\"",
        )])?;
        let error = DaemonStartupConfig::new(&config, &settings, false)
            .expect_err("suspend under once_per_key is refused");
        assert!(
            error.to_string().contains("hydration.page_out_policy"),
            "the refusal names the key: {error}"
        );
        Ok(())
    }

    #[test]
    fn test_hydration_settings_fall_back_to_legacy_config() -> buck2_error::Result<()> {
        let config = parse(
            &[(
                "config",
                indoc!(
                    r#"
                    [buck2_hydration]
                    page_out_on_idle = true
                    "#
                ),
            )],
            "config",
        )?;

        let startup_config = DaemonStartupConfig::new(&config, &BuckSettings::empty(), false)?;
        let hydration = startup_config
            .hydration
            .expect("Legacy idle page-out config should enable hydration");
        assert!(hydration.page_out_on_idle);
        Ok(())
    }

    #[test]
    fn test_hydration_settings_override_legacy_config() -> buck2_error::Result<()> {
        let config = parse(
            &[(
                "config",
                indoc!(
                    r#"
                    [buck2_hydration]
                    enable_paging = true
                    page_out_on_idle = true
                    "#
                ),
            )],
            "config",
        )?;
        let settings = resolve_setting_flags(vec![table(
            "[hydration]\nenable_paging = true\npage_out_on_idle = false",
        )])?;

        let startup_config = DaemonStartupConfig::new(&config, &settings, false)?;
        let hydration = startup_config
            .hydration
            .expect("Explicit Buck settings should keep hydration enabled");
        assert!(!hydration.page_out_on_idle);
        Ok(())
    }

    #[test]
    fn test_hydration_settings_can_disable_legacy_config() -> buck2_error::Result<()> {
        let config = parse(
            &[(
                "config",
                indoc!(
                    r#"
                    [buck2_hydration]
                    enable_paging = true
                    page_out_on_idle = true
                    "#
                ),
            )],
            "config",
        )?;
        let settings = resolve_setting_flags(vec![table(
            "[hydration]\nenable_paging = false\npage_out_on_idle = false",
        )])?;

        let startup_config = DaemonStartupConfig::new(&config, &settings, false)?;
        assert_eq!(startup_config.hydration, None);
        Ok(())
    }

    #[test]
    fn test_non_default_idle_page_out_scope() -> buck2_error::Result<()> {
        let config = parse(
            &[(
                "config",
                indoc!(
                    r#"
                    [buck2_hydration]
                    page_out_on_idle = true
                    "#
                ),
            )],
            "config",
        )?;
        let settings = resolve_setting_flags(vec![table(
            "[hydration]\npage_out_on_idle_isolation_dir_scope = \"non_default\"",
        )])?;
        let startup_config = DaemonStartupConfig::new(&config, &settings, false)?;

        assert!(
            startup_config.hydration.is_some(),
            "The isolation scope must not disable pagable storage"
        );
        assert_eq!(
            startup_config.idle_page_out_config_for_isolation_dir(
                FileName::new("v2").expect("The default isolation dir should be valid"),
            ),
            None,
            "The non-default scope must disable idle page-out for the default daemon"
        );
        let custom_hydration = startup_config
            .idle_page_out_config_for_isolation_dir(
                FileName::new("custom").expect("The test isolation dir should be valid"),
            )
            .expect("Idle page-out should remain enabled for another daemon");
        assert!(
            custom_hydration.page_out_on_idle,
            "The non-default scope must retain idle page-out for another daemon"
        );
        Ok(())
    }
}
