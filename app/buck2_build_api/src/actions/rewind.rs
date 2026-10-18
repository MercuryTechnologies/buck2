/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::any::Any;
use std::sync::Mutex;

use allocative::Allocative;
use buck2_artifact::actions::key::ActionKey;
use buck2_common::file_ops::metadata::FileDigest;
use buck2_directory::directory::directory::Directory;
use buck2_directory::directory::directory_iterator::DirectoryIterator;
use buck2_directory::directory::entry::DirectoryEntry;
use buck2_error::TypedContext;
use buck2_execute::artifact_value::ArtifactValue;
use buck2_execute::directory::ActionDirectoryMember;
use buck2_hash::BuckDashSet;
use dice::DiceTransaction;
use dice::UserComputationData;
use dupe::Dupe;

use crate::actions::calculation::BuildKey;
use crate::build::BuildTargetResult;

/// How many times one command rewinds the graph before it reports the error it keeps hitting.
pub const MAX_REPEATED_ACTION_REWINDS: usize = 20;

#[derive(Allocative, Clone, Debug, Eq, PartialEq)]
pub struct ActionRewindRequest {
    action_keys: Vec<ActionKey>,
}

impl ActionRewindRequest {
    pub fn new(action_keys: Vec<ActionKey>) -> Self {
        Self { action_keys }
    }

    pub fn action_keys(&self) -> &[ActionKey] {
        &self.action_keys
    }

    pub fn into_action_keys(self) -> Vec<ActionKey> {
        self.action_keys
    }

    pub fn merge(&mut self, other: &Self) {
        for key in &other.action_keys {
            if !self.action_keys.iter().any(|existing| existing == key) {
                self.action_keys.push(key.dupe());
            }
        }
    }
}

impl TypedContext for ActionRewindRequest {
    fn eq(&self, other: &dyn TypedContext) -> bool {
        (other as &dyn Any).downcast_ref::<Self>() == Some(self)
    }

    fn display(&self) -> Option<String> {
        Some(format!("ActionRewindRequest({:?})", self.action_keys))
    }
}

/// The actions the current transaction rewound, and a rewind requested by something that has no
/// error to carry it in: a test execution whose input left the CAS reports its test as cancelled,
/// so that it is not counted as failed, and leaves its request here for the command to apply.
pub struct ActionRewindTrackerHolder(BuckDashSet<ActionKey>, Mutex<Option<ActionRewindRequest>>);

pub trait HasActionRewindTracker {
    fn init_action_rewind_tracker(&mut self);

    fn set_rewound_actions(&self, action_keys: &[ActionKey]);

    fn is_action_rewound(&self, action_key: &ActionKey) -> bool;

    /// Asks the command to rewind `request` once the work in flight is done.
    fn request_action_rewind(&self, request: &ActionRewindRequest);

    /// The rewinds requested through [`Self::request_action_rewind`] since the last call.
    fn take_requested_action_rewind(&self) -> Option<ActionRewindRequest>;
}

impl HasActionRewindTracker for UserComputationData {
    fn init_action_rewind_tracker(&mut self) {
        self.data.set(ActionRewindTrackerHolder(
            BuckDashSet::default(),
            Mutex::new(None),
        ));
    }

    fn request_action_rewind(&self, request: &ActionRewindRequest) {
        let Ok(holder) = self.data.get::<ActionRewindTrackerHolder>() else {
            return;
        };
        let mut pending = holder.1.lock().expect("poisoned");
        match &mut *pending {
            Some(existing) => existing.merge(request),
            None => *pending = Some(request.clone()),
        }
    }

    fn take_requested_action_rewind(&self) -> Option<ActionRewindRequest> {
        self.data
            .get::<ActionRewindTrackerHolder>()
            .ok()?
            .1
            .lock()
            .expect("poisoned")
            .take()
    }

    fn set_rewound_actions(&self, action_keys: &[ActionKey]) {
        let holder = self
            .data
            .get::<ActionRewindTrackerHolder>()
            .expect("ActionRewindTracker should be set");
        holder.0.clear();
        for action_key in action_keys {
            holder.0.insert(action_key.dupe());
        }
    }

    fn is_action_rewound(&self, action_key: &ActionKey) -> bool {
        self.data
            .get::<ActionRewindTrackerHolder>()
            .is_ok_and(|holder| holder.0.contains(action_key))
    }
}

/// The rewind the errors of a build ask for, merged, if any does.
pub fn find_action_rewind_request(build_result: &BuildTargetResult) -> Option<ActionRewindRequest> {
    fn merge_rewind(request: &mut Option<ActionRewindRequest>, error: &buck2_error::Error) {
        let Some(found) = error.find_typed_context::<ActionRewindRequest>() else {
            return;
        };

        match request {
            Some(request) => request.merge(&found),
            None => *request = Some((*found).clone()),
        }
    }

    let mut request = None;
    for errors in build_result.other_errors.values() {
        for error in errors {
            merge_rewind(&mut request, error);
        }
    }

    for configured in build_result.configured.values().flatten() {
        for error in &configured.errors {
            merge_rewind(&mut request, &error.inner);
        }
        for output in &configured.outputs {
            if let Err(error) = &output.inner {
                merge_rewind(&mut request, error);
            }
        }
    }

    request
}

/// Dirties the actions `rewind` names, so that the next computation on the returned transaction
/// runs them again, bypassing the action cache.
pub async fn apply_action_rewind(
    ctx: DiceTransaction,
    rewind: ActionRewindRequest,
) -> buck2_error::Result<DiceTransaction> {
    let action_keys = rewind.into_action_keys();
    let ctx = ctx
        .update_to_latest(
            action_keys
                .iter()
                .cloned()
                .map(BuildKey)
                .collect::<Vec<_>>(),
        )
        .await?;
    ctx.per_transaction_data().set_rewound_actions(&action_keys);
    Ok(ctx)
}

/// Whether an input's value is, or holds, a file of `digest`. The lost input is named by the
/// file that could not be fetched, while the input may be the directory around it, as a
/// genrule's `out` directory is.
pub fn value_holds_digest(value: &ArtifactValue, digest: &FileDigest) -> bool {
    if value.digest() == Some(digest) {
        return true;
    }
    match value.entry() {
        DirectoryEntry::Dir(d) => d.unordered_walk_leaves().without_paths().any(|leaf| {
            matches!(leaf, ActionDirectoryMember::File(f) if f.digest.data() == digest)
        }),
        DirectoryEntry::Leaf(_) => false,
    }
}
