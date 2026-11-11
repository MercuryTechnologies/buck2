/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! `InvNodeKey`: an invalidation body as a DICE key, for the phase that installs records.
//!
//! Its value is the digest `A` this daemon computes for the body, and its equality compares that
//! digest. A record installed above `InvNodeKey { expected: E }` stays valid across a change only
//! if the recompute after the change finds `A == E` again, that is only if every leaf is back to
//! the writer's value. A unit value (v1) compared equal after every recompute, so DICE kept the
//! record across any leaf change: that is the cut-off the test below shows.
//!
//! Phases 1 to 3 install nothing, and the shadow reader evaluates bodies with
//! [`crate::body::verdict`] instead; this key is here so that its equality is tested now.

use std::sync::Arc;

use allocative::Allocative;
use async_trait::async_trait;
use buck2_common::frontier::Canon;
use buck2_common::frontier::LeafId;
use derive_more::Display;
use dice::DiceComputations;
use dice::EqualityBehavior;
use dice::Key;
use dice::NoValueSerialize;
use dice::ValueSerialize;
use dice_futures::cancellation::CancellationContext;
use pagable::Pagable;
use pagable::pagable_typetag;

use crate::body::Body;
use crate::body::BodySource;
use crate::body::Digest;

/// Computes leaves through DICE, so that a leaf is a dep of the `InvNodeKey` that reads it.
#[async_trait]
pub trait FrontierLeafSource: Send + Sync + 'static {
    async fn leaf(
        &self,
        ctx: &mut DiceComputations<'_>,
        id: &LeafId,
    ) -> buck2_error::Result<Option<Canon>>;
}

/// What `InvNodeKey` reads its bodies and leaves through: in the transaction's data.
#[derive(Clone)]
pub struct FrontierInvData {
    pub bodies: Arc<dyn BodySource + Send + Sync>,
    pub leaves: Arc<dyn FrontierLeafSource>,
}

#[derive(Allocative, Clone, Debug, Display, PartialEq, Eq, Hash, Pagable)]
#[display("InvNodeKey({expected})")]
#[pagable_typetag(dice::DiceKeyDyn)]
pub struct InvNodeKey {
    pub expected: Digest,
}

#[derive(Allocative, Clone, Debug, PartialEq, Eq)]
pub struct InvVerdict {
    /// `A`: the body's digest over this daemon's leaf values.
    pub actual: Digest,
    pub valid: bool,
}

#[async_trait]
impl Key for InvNodeKey {
    type Value = Result<InvVerdict, Arc<str>>;

    async fn compute(
        &self,
        ctx: &mut DiceComputations,
        _cancellations: &CancellationContext,
    ) -> Self::Value {
        let data = ctx
            .per_transaction_data()
            .data
            .get::<FrontierInvData>()
            .map_err(|e| Arc::from(e.to_string()))?
            .clone();
        let body = data
            .bodies
            .body(&self.expected)
            .map_err(|e| Arc::from(format!("{e:#}")))?;
        if body.digest() != self.expected {
            // A corrupt or substituted body is a verdict, never trusted.
            return Ok(InvVerdict {
                actual: "corrupt".to_owned(),
                valid: false,
            });
        }
        let mut locals: Vec<(LeafId, Canon)> = Vec::with_capacity(body.leaves.len());
        for (id, _) in &body.leaves {
            let v = data
                .leaves
                .leaf(ctx, id)
                .await
                .map_err(|e| Arc::from(format!("{e:#}")))?
                .unwrap_or_else(|| "unavailable".to_owned());
            locals.push((id.clone(), v));
        }
        let mut kids = Vec::with_capacity(body.children.len());
        for c in &body.children {
            let child = ctx
                .compute(&InvNodeKey {
                    expected: c.clone(),
                })
                .await
                .map_err(|e| Arc::from(format!("{e:#}")))?;
            let child = child.as_ref().map_err(|e| e.clone())?;
            kids.push(child.actual.clone());
        }
        let actual = Body::digest_with(
            body.version.as_deref(),
            locals.iter().map(|(id, c)| (id, c.as_str())),
            kids.iter().map(|k| k.as_str()),
        );
        Ok(InvVerdict {
            valid: actual == self.expected,
            actual,
        })
    }

    /// The digest, not unit: see the module comment.
    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| match (x, y) {
            (Ok(x), Ok(y)) => x.actual == y.actual,
            _ => false,
        })
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        NoValueSerialize::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use dice::DetectCycles;
    use dice::Dice;
    use dice::UserComputationData;
    use dupe::Dupe;

    use super::*;

    /// The leaf: a value set from outside, like a file's metadata.
    #[derive(Allocative, Clone, Debug, Display, PartialEq, Eq, Hash, Pagable)]
    #[pagable_typetag(dice::DiceKeyDyn)]
    struct Leaf;

    #[async_trait]
    impl Key for Leaf {
        type Value = String;

        async fn compute(&self, _: &mut DiceComputations, _: &CancellationContext) -> String {
            unreachable!("injected")
        }

        fn equality_behavior() -> EqualityBehavior<String> {
            EqualityBehavior::Compare(|a, b| a == b)
        }

        fn value_serialize() -> impl ValueSerialize<Value = String> {
            NoValueSerialize::new()
        }
    }

    struct Leaves;

    #[async_trait]
    impl FrontierLeafSource for Leaves {
        async fn leaf(
            &self,
            ctx: &mut DiceComputations<'_>,
            _id: &LeafId,
        ) -> buck2_error::Result<Option<Canon>> {
            Ok(Some(ctx.compute(&Leaf).await?.clone()))
        }
    }

    struct Bodies(Mutex<HashMap<Digest, Body>>);

    impl BodySource for Bodies {
        fn body(&self, e: &str) -> anyhow::Result<Body> {
            self.0
                .lock()
                .unwrap()
                .get(e)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("no body"))
        }
    }

    /// The v1 key: unit-valued, so every recompute compares equal.
    #[derive(Allocative, Clone, Debug, Display, PartialEq, Eq, Hash, Pagable)]
    #[pagable_typetag(dice::DiceKeyDyn)]
    struct UnitInvNodeKey(Digest);

    #[async_trait]
    impl Key for UnitInvNodeKey {
        type Value = ();

        async fn compute(&self, ctx: &mut DiceComputations, c: &CancellationContext) {
            // The same premises as `InvNodeKey`, a unit value.
            let _ = InvNodeKey {
                expected: self.0.clone(),
            }
            .compute(ctx, c)
            .await;
        }

        fn equality_behavior() -> EqualityBehavior<()> {
            EqualityBehavior::Compare(|_, _| true)
        }

        fn value_serialize() -> impl ValueSerialize<Value = ()> {
            NoValueSerialize::new()
        }
    }

    #[derive(Default, Allocative)]
    struct Count(AtomicUsize);

    /// K: the installed key, whose only premise is its invalidation node.
    #[derive(Allocative, Clone, Debug, Display, PartialEq, Eq, Hash, Pagable)]
    #[display("Installed({expected}, {unit})")]
    #[pagable_typetag(dice::DiceKeyDyn)]
    struct Installed {
        expected: Digest,
        unit: bool,
    }

    #[async_trait]
    impl Key for Installed {
        type Value = bool;

        async fn compute(&self, ctx: &mut DiceComputations, _: &CancellationContext) -> bool {
            ctx.per_transaction_data()
                .data
                .get::<Arc<Count>>()
                .unwrap()
                .0
                .fetch_add(1, Ordering::SeqCst);
            if self.unit {
                ctx.compute(&UnitInvNodeKey(self.expected.clone()))
                    .await
                    .unwrap();
                true
            } else {
                ctx.compute(&InvNodeKey {
                    expected: self.expected.clone(),
                })
                .await
                .unwrap()
                .as_ref()
                .unwrap()
                .valid
            }
        }

        fn equality_behavior() -> EqualityBehavior<bool> {
            EqualityBehavior::Compare(|a, b| a == b)
        }

        fn value_serialize() -> impl ValueSerialize<Value = bool> {
            NoValueSerialize::new()
        }
    }

    fn leaf_id() -> LeafId {
        LeafId::Path {
            cell: "root".to_owned(),
            path: "a".to_owned(),
        }
    }

    /// Returns K's compute count and verdict after: install, change the leaf, revert it.
    async fn run(unit: bool) -> Vec<(usize, bool)> {
        let body = Body::new(None, vec![(leaf_id(), "a".to_owned())], vec![]);
        let e = body.digest();
        let bodies = Arc::new(Bodies(Mutex::new(HashMap::from([(e.clone(), body)]))));
        let dice = Dice::builder().build(DetectCycles::Disabled);
        let root = dice.new_root().await;
        let count = Arc::new(Count::default());
        let data = || {
            let mut d = UserComputationData::new();
            d.data.set(count.dupe());
            d.data.set(FrontierInvData {
                bodies: bodies.clone(),
                leaves: Arc::new(Leaves),
            });
            d
        };
        let k = Installed {
            expected: e.clone(),
            unit,
        };
        let mut out = Vec::new();
        for v in ["a", "b", "a"] {
            let mut u = dice.updater_with_data(root, data());
            u.changed_to([(Leaf, v.to_owned())]).unwrap();
            let tx = u.commit().await;
            let valid = *tx.compute(&k).await.unwrap();
            out.push((count.0.load(Ordering::SeqCst), valid));
            drop(tx);
            dice.wait_for_idle().await;
        }
        out
    }

    #[tokio::test]
    async fn digest_valued_node_reaches_the_installed_key_and_flips_back() {
        // Installed valid; recomputed and invalid after the change; recomputed and valid again
        // after the revert.
        assert_eq!(run(false).await, vec![(1, true), (2, false), (3, true)]);
    }

    #[tokio::test]
    async fn unit_valued_node_cuts_off_early() {
        // The v1 defect: the change never reaches K, which keeps its first value.
        assert_eq!(run(true).await, vec![(1, true), (1, true), (1, true)]);
    }
}
