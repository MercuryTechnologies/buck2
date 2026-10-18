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
//!
//! The exception is an action that declares `store_path_outputs = True`: each of its outputs is a
//! symlink to a path of an immutable store, such as `/nix/store`, whose name is a function of the
//! action's inputs. Running it again on another machine names the same path, and makes it exist
//! there if it does not. So a resume keeps such an action's value when every path it names lies
//! under a configured store prefix (`buck2_hydration.suspend_store_prefixes`, `/nix/store/` by
//! default) and the store's own check (`buck2_hydration.suspend_store_check`) calls it valid on
//! the new machine, and dirties it, as any other, when one is not. The declaration is an
//! unchecked promise of the rule, so with no check configured every such action is dirtied:
//! that a path exists says nothing of whether it is complete, or the store's.

use buck2_artifact::actions::key::ActionKey;
use buck2_execute::directory::ActionDirectoryEntry;
use buck2_execute::directory::ActionDirectoryMember;
use buck2_hash::BuckDashMap;
use dice::DiceData;
use dice::DiceDataBuilder;
use dupe::Dupe;

use crate::actions::calculation::BuildKey;
use crate::actions::execute::action_executor::ActionExecutionKind;
use crate::actions::execute::action_executor::ActionOutputs;

/// Every action this daemon ran on its own machine, with the store paths of those whose outputs
/// are store paths, held in a DICE root's data.
#[derive(Default)]
pub struct LocalActions(BuckDashMap<ActionKey, Option<StorePaths>>);

/// The absolute store paths an action's outputs link to, in the order of its outputs.
pub type StorePaths = Box<[String]>;

/// One locally executed action, as a suspend image lists it.
#[derive(Debug, Clone, PartialEq, Eq, pagable::Pagable)]
pub struct LocalAction {
    pub key: ActionKey,
    /// Set for an action that declared `store_path_outputs` and whose every output is a symlink
    /// to an absolute path: what a resume checks to keep its value.
    pub store_paths: Option<StorePaths>,
}

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

    /// The store paths an action's outputs name, if the action declared `store_path_outputs`
    /// and every output is a symlink to an absolute path. Anything else, a file, a directory or a
    /// symlink into the project, makes it an ordinary local action.
    pub fn store_paths(declared: bool, outputs: &ActionOutputs) -> Option<StorePaths> {
        if !declared {
            return None;
        }
        let mut paths = Vec::new();
        for value in outputs.values() {
            match value.entry() {
                ActionDirectoryEntry::Leaf(ActionDirectoryMember::ExternalSymlink(link)) => {
                    let path = link.to_path_buf();
                    if !path.is_absolute() {
                        return None;
                    }
                    paths.push(path.to_str()?.to_owned());
                }
                _ => return None,
            }
        }
        if paths.is_empty() {
            return None;
        }
        Some(paths.into_boxed_slice())
    }

    /// Records an action that ran here, replacing what an earlier run of it recorded.
    pub fn record(&self, key: &ActionKey, store_paths: Option<StorePaths>) {
        self.0.insert(key.dupe(), store_paths);
    }

    /// Records the actions a suspended daemon ran on its machine, when this daemon resumes it.
    /// An action this daemon ran since keeps what this daemon recorded.
    pub fn extend(&self, list: &[LocalAction]) {
        for a in list {
            self.0
                .entry(a.key.dupe())
                .or_insert_with(|| a.store_paths.clone());
        }
    }

    pub fn contains(&self, key: &ActionKey) -> bool {
        self.0.contains_key(key)
    }

    /// The recorded actions, in no particular order.
    pub fn snapshot(&self) -> LocalActionList {
        self.0
            .iter()
            .map(|e| LocalAction {
                key: e.key().dupe(),
                store_paths: e.value().clone(),
            })
            .collect()
    }

    /// The recorded actions of a DICE root's data, if it has the set.
    pub fn of(data: &DiceData) -> Option<&LocalActions> {
        data.get::<LocalActions>().ok()
    }
}

/// The list a suspend image records the set as.
pub type LocalActionList = Vec<LocalAction>;

/// The DICE keys of the listed actions, which a resume dirties.
pub fn build_keys(list: impl IntoIterator<Item = LocalAction>) -> Vec<BuildKey> {
    list.into_iter().map(|a| BuildKey(a.key)).collect()
}

/// Splits a resumed image's list into the actions to dirty and the number kept: an action whose
/// store paths `valid` accepts keeps its value; every other one is dirtied. Each distinct path is
/// asked once, so a store path that several actions name costs one check.
pub fn partition_by_store_validity(
    list: LocalActionList,
    mut valid: impl FnMut(&str) -> bool,
) -> (Vec<LocalAction>, usize) {
    let mut verdicts: std::collections::HashMap<String, bool> = std::collections::HashMap::new();
    let mut dirty = Vec::new();
    let mut kept = 0;
    for a in list {
        let keep = match &a.store_paths {
            Some(paths) => paths.iter().all(|p| {
                *verdicts
                    .entry(p.clone())
                    .or_insert_with(|| valid(p.as_str()))
            }),
            None => false,
        };
        if keep {
            kept += 1;
        } else {
            dirty.push(a);
        }
    }
    (dirty, kept)
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
        local.record(&key(0), None);
        let paths: StorePaths = vec!["/nix/store/a-x".to_owned()].into_boxed_slice();
        local.extend(&[
            LocalAction {
                key: key(1),
                store_paths: Some(paths.clone()),
            },
            LocalAction {
                key: key(0),
                store_paths: Some(paths.clone()),
            },
        ]);
        let mut listed = local.snapshot();
        listed.sort_by_key(|a| a.key.action_index().0);
        assert_eq!(
            listed,
            vec![
                // This daemon ran key(0) itself: what it recorded stands.
                LocalAction {
                    key: key(0),
                    store_paths: None
                },
                LocalAction {
                    key: key(1),
                    store_paths: Some(paths)
                },
            ]
        );
    }

    /// Only an action whose every store path is valid keeps its value; one with no store paths,
    /// or with one invalid path, is dirtied. A path two actions share is checked once.
    #[test]
    fn a_resume_keeps_only_actions_whose_store_paths_are_valid() {
        use buck2_artifact::actions::key::ActionIndex;
        use buck2_core::deferred::key::DeferredHolderKey;

        let key = |i| {
            ActionKey::new(
                DeferredHolderKey::testing_new("cell//pkg:t"),
                ActionIndex::new(i),
            )
        };
        let paths = |ps: &[&str]| -> Option<StorePaths> {
            Some(ps.iter().map(|p| (*p).to_owned()).collect())
        };
        let list = vec![
            LocalAction {
                key: key(0),
                store_paths: None,
            },
            LocalAction {
                key: key(1),
                store_paths: paths(&["/s/valid"]),
            },
            LocalAction {
                key: key(2),
                store_paths: paths(&["/s/valid", "/s/missing"]),
            },
            LocalAction {
                key: key(3),
                store_paths: paths(&["/s/valid"]),
            },
        ];
        let mut asked = Vec::new();
        let (dirty, kept) = partition_by_store_validity(list, |p| {
            asked.push(p.to_owned());
            p == "/s/valid"
        });
        assert_eq!(kept, 2);
        assert_eq!(
            dirty.iter().map(|a| a.key.dupe()).collect::<Vec<_>>(),
            vec![key(0), key(2)]
        );
        assert_eq!(asked, vec!["/s/valid", "/s/missing"]);
    }

    /// Only a declared action whose every output is an absolute external symlink names store
    /// paths.
    #[test]
    fn store_paths_need_the_declaration_and_only_absolute_links() {
        use buck2_common::cas_digest::CasDigestConfig;
        use buck2_common::external_symlink::ExternalSymlink;
        use buck2_common::file_ops::metadata::FileMetadata;
        use buck2_common::file_ops::metadata::TrackedFileDigest;
        use buck2_core::configuration::data::ConfigurationData;
        use buck2_core::deferred::base_deferred_key::BaseDeferredKey;
        use buck2_core::fs::buck_out_path::BuckOutPathKind;
        use buck2_core::fs::buck_out_path::BuildArtifactPath;
        use buck2_core::target::configured_target_label::ConfiguredTargetLabel;
        use buck2_execute::artifact_value::ArtifactValue;
        use buck2_fs::paths::forward_rel_path::ForwardRelativePathBuf;
        use buck2_hash::BuckIndexMap;

        let owner = BaseDeferredKey::TargetLabel(ConfiguredTargetLabel::testing_parse(
            "cell//pkg:target",
            ConfigurationData::testing_new(),
        ));
        let path = |name: &str| {
            BuildArtifactPath::new(
                owner.dupe(),
                ForwardRelativePathBuf::unchecked_new(name.to_owned()),
                BuckOutPathKind::Configuration,
            )
        };
        let link = |target: &str| {
            ArtifactValue::new(
                ActionDirectoryEntry::Leaf(ActionDirectoryMember::ExternalSymlink(
                    std::sync::Arc::new(
                        ExternalSymlink::new(
                            std::path::PathBuf::from(target),
                            ForwardRelativePathBuf::default(),
                        )
                        .unwrap(),
                    ),
                )),
                None,
            )
        };
        let file = ArtifactValue::file(FileMetadata {
            digest: TrackedFileDigest::from_content(b"x", CasDigestConfig::testing_default()),
            is_executable: false,
        });
        let links = ActionOutputs::new(BuckIndexMap::from_iter([
            (path("out.link"), link("/nix/store/abc-x")),
            (path("out.link-dev"), link("/nix/store/abd-x-dev")),
        ]));
        assert_eq!(
            LocalActions::store_paths(true, &links).as_deref(),
            Some(&["/nix/store/abc-x".to_owned(), "/nix/store/abd-x-dev".to_owned()][..])
        );
        assert_eq!(LocalActions::store_paths(false, &links), None);
        let mixed = ActionOutputs::new(BuckIndexMap::from_iter([
            (path("out.link"), link("/nix/store/abc-x")),
            (path("log"), file),
        ]));
        assert_eq!(LocalActions::store_paths(true, &mixed), None);
    }
}
