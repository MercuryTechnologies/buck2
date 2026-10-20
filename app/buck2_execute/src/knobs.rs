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

use dupe::Dupe;

/// Command-level config that can tweak how the executors work.
#[derive(Clone, Dupe, Default)]
pub struct ExecutorGlobalKnobs {
    pub enable_miniperf: bool,

    /// Whether to emit action keys to execution logs (thos are pretty verbose and omitted by
    /// default).
    pub log_action_keys: bool,

    /// Maximum duration in seconds that an execution can remain in the RE queue state before it is cancelled.
    pub re_cancel_on_estimated_queue_time_exceeds: Option<Duration>,

    /// Maximum duration in seconds that an execution can remain in the RE queue state before local execution is unblocked.
    /// Note this overrides, and should possibly replace, `remote_execution_queue_time_threshold_s` configured per executor.
    pub re_fallback_on_estimated_queue_time_exceeds: Option<Duration>,

    /// Which of an action-cache hit's blobs buck2 asks the CAS about before it takes the hit,
    /// `[buck2_re_client] cache_hit_presence_check`.
    pub cache_hit_presence_check: CacheHitPresenceCheck,
}

/// Which of an action-cache hit's blobs buck2 asks the CAS about before it takes the hit. A hit
/// whose blobs the CAS has lost is treated as a miss instead of failing the action later.
/// BuildBuddy validates a hit's output files itself before serving it, but not its stored
/// stdout and stderr, so against such a server `streams` keeps the protection the client adds
/// and drops the repeat of the server's check.
///
/// `streams` is the lever for a self-hosted cache's load: on lab's BuildBuddy (c1, 2026-10-20) it
/// cut the client's FindMissingBlobs digests on a fully cached spec test from 261,791 to 10,750,
/// with the same action keys and no change in wall time. Use it only against a BuildBuddy version
/// whose GetActionResult output validation has been read at source (v2.314.0, ValidateActionResult
/// in server/remote_cache/action_cache_server/action_cache_server.go), and read it again on every
/// version bump before keeping `streams` there; anywhere else keep `all`. Lab keeps `all`.
#[derive(Clone, Copy, Dupe, Debug, Default, PartialEq, Eq)]
pub enum CacheHitPresenceCheck {
    /// The output files and the stored stdout and stderr.
    #[default]
    All,
    /// Only the stored stdout and stderr.
    Streams,
    /// Nothing: a hit is taken as the server returns it.
    Off,
}

impl FromStr for CacheHitPresenceCheck {
    type Err = buck2_error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "all" => Ok(Self::All),
            "streams" => Ok(Self::Streams),
            "off" => Ok(Self::Off),
            _ => Err(buck2_error::buck2_error!(
                buck2_error::ErrorTag::Input,
                "cache_hit_presence_check must be all, streams or off, not `{}`",
                s
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_hit_presence_check_parses_its_three_values_and_nothing_else() {
        assert_eq!("all".parse::<CacheHitPresenceCheck>().unwrap(), CacheHitPresenceCheck::All);
        assert_eq!(
            "streams".parse::<CacheHitPresenceCheck>().unwrap(),
            CacheHitPresenceCheck::Streams
        );
        assert_eq!("off".parse::<CacheHitPresenceCheck>().unwrap(), CacheHitPresenceCheck::Off);
        assert!("none".parse::<CacheHitPresenceCheck>().is_err());
        assert_eq!(CacheHitPresenceCheck::default(), CacheHitPresenceCheck::All);
    }
}
