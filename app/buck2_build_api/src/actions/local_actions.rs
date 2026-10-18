/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! The actions a daemon ran on its own machine, for `buck2 suspend --portable`.
//!
//! A local action inherits the daemon's environment and can read paths outside the project,
//! such as a nix store, so its outputs can depend on the machine and on per-run variables
//! (`GITHUB_SHA`, `HOSTNAME`) that a suspend image leaves out of its checks. A daemon resumed on
//! another machine, or from a portable image, dirties these actions before any command, so each
//! runs again there. An action served by the remote cache was produced remotely: lab uploads no
//! cache entry from a local execution.

use buck2_artifact::actions::key::ActionKey;
use buck2_hash::BuckDashSet;
use dice::DiceData;
use dice::DiceDataBuilder;
use dupe::Dupe;

use crate::actions::calculation::BuildKey;
use crate::actions::execute::action_executor::ActionExecutionKind;

/// Every action this daemon ran on its own machine, held in a DICE root's data.
#[derive(Default)]
pub struct LocalActions(BuckDashSet<ActionKey>);

impl LocalActions {
    /// Whether an action that finished with `kind`, or failed (`None`), may have produced its
    /// result on this machine. A failure counts, because its error is what the graph keeps.
    pub fn ran_here(kind: Option<buck2_data::ActionExecutionKind>) -> bool {
        use buck2_data::ActionExecutionKind as K;
        match kind {
            None => true,
            Some(K::NotSet | K::Local | K::LocalDepFile | K::LocalWorker | K::LocalActionCache) => {
                true
            }
            Some(
                K::Remote
                | K::ActionCache
                | K::Simple
                | K::Deferred
                | K::RemoteDepFileCache
                | K::RemoteWorker
                | K::RemoteServedFromCache
                | K::RemoteDeduplicated,
            ) => false,
        }
    }

    /// Whether an action that finished with `kind` produced its result on this machine. A hit of
    /// the identical-action cache counts only when the action it reuses ran here: outputs that
    /// came from remote execution or the remote action cache are no more this machine's when
    /// another configuration's action served them. A go stdlib built in several configurations
    /// is served this way, and recording those hits made a resume run them again with every
    /// action that depends on them.
    pub fn executed_here(kind: &ActionExecutionKind) -> bool {
        match kind {
            ActionExecutionKind::LocalActionCache { produced_locally } => *produced_locally,
            kind => Self::ran_here(Some(kind.as_enum())),
        }
    }

    pub fn record(&self, key: &ActionKey) {
        self.0.insert(key.dupe());
    }

    /// Records the actions a suspended daemon ran on its machine, when this daemon resumes it.
    pub fn extend(&self, list: &LocalActionList) {
        for key in list {
            self.record(key);
        }
    }

    pub fn contains(&self, key: &ActionKey) -> bool {
        self.0.contains(key)
    }

    /// The recorded actions, in no particular order.
    pub fn snapshot(&self) -> LocalActionList {
        self.0.iter().map(|k| k.dupe()).collect()
    }

    /// The recorded actions of a DICE root's data, if it has the set.
    pub fn of(data: &DiceData) -> Option<&LocalActions> {
        data.get::<LocalActions>().ok()
    }
}

/// The list a suspend image records the set as.
pub type LocalActionList = Vec<ActionKey>;

/// The DICE keys of the listed actions, which a resume dirties.
pub fn build_keys(list: LocalActionList) -> Vec<BuildKey> {
    list.into_iter().map(BuildKey).collect()
}

pub trait SetLocalActions {
    fn set_local_actions(&mut self);
}

impl SetLocalActions for DiceDataBuilder {
    fn set_local_actions(&mut self) {
        self.set(LocalActions::default());
    }
}

impl SetLocalActions for DiceData {
    fn set_local_actions(&mut self) {
        self.set(LocalActions::default());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every kind, so that moving one between the arms of `ran_here` fails here.
    #[test]
    fn only_kinds_that_ran_here_count() {
        use buck2_data::ActionExecutionKind as K;
        assert!(LocalActions::ran_here(None));
        for kind in [
            K::NotSet,
            K::Local,
            K::LocalDepFile,
            K::LocalWorker,
            K::LocalActionCache,
        ] {
            assert!(LocalActions::ran_here(Some(kind)), "{kind:?}");
        }
        for kind in [
            K::Remote,
            K::ActionCache,
            K::Simple,
            K::Deferred,
            K::RemoteDepFileCache,
            K::RemoteWorker,
            K::RemoteServedFromCache,
            K::RemoteDeduplicated,
        ] {
            assert!(!LocalActions::ran_here(Some(kind)), "{kind:?}");
        }
    }

    /// An identical-action cache hit is this machine's only when the action it reuses ran here.
    #[test]
    fn a_cache_hit_is_local_only_when_its_source_ran_here() {
        assert!(LocalActions::executed_here(
            &ActionExecutionKind::LocalActionCache {
                produced_locally: true
            }
        ));
        assert!(!LocalActions::executed_here(
            &ActionExecutionKind::LocalActionCache {
                produced_locally: false
            }
        ));
        assert!(!LocalActions::executed_here(&ActionExecutionKind::Simple));
        assert!(LocalActions::executed_here(&ActionExecutionKind::LocalDepFile));
    }

    /// A resumed daemon's set holds the suspended daemon's actions beside its own, so that its
    /// own suspend lists both.
    #[test]
    fn extend_keeps_both_daemons_actions() {
        use buck2_artifact::actions::key::ActionIndex;
        use buck2_core::deferred::key::DeferredHolderKey;

        let key = |i| {
            ActionKey::new(
                DeferredHolderKey::testing_new("cell//pkg:t"),
                ActionIndex::new(i),
            )
        };
        let local = LocalActions::default();
        local.record(&key(0));
        local.extend(&vec![key(1), key(0)]);
        let mut listed = local.snapshot();
        listed.sort_by_key(|k| k.action_index().0);
        assert_eq!(listed, vec![key(0), key(1)]);
    }
}
