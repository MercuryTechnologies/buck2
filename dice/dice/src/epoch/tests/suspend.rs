/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! `Dice::suspend_image` and `Dice::restore_image` end to end: a graph computed in one `Dice`,
//! suspended over a sqlite paging store, and resumed by a second `Dice` over the same store.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use allocative::Allocative;
use async_trait::async_trait;
use derive_more::Display;
use dice_futures::cancellation::CancellationContext;
use dupe::Dupe;
use pagable::Pagable;
use pagable::pagable_typetag;
use tempfile::TempDir;
use tempfile::tempdir;

use crate::BranchId;
use crate::DiceKeyDyn;
use crate::DiceProjectionDyn;
use crate::DiceStorage;
use crate::EvictionPolicy;
use crate::PagableStorageBackend;
use crate::PageOutPolicy;
use crate::SuspendDrop;
use crate::api::computations::DiceComputations;
use crate::api::cycles::DetectCycles;
use crate::api::injected::InjectedKey;
use crate::api::key::EqualityBehavior;
use crate::api::key::Key;
use crate::api::key::NoValueSerialize;
use crate::api::key::PagableValueSerialize;
use crate::api::key::ResidentValueSerialize;
use crate::api::key::ValueSerialize;
use crate::api::projection::DiceProjectionComputations;
use crate::api::projection::ProjectionKey;
use crate::api::user_data::UserComputationData;
use crate::dice::Dice;

/// The computes run in one transaction, by key, as their `Display`s.
#[derive(Clone, Dupe, Default)]
struct Computes(Arc<Mutex<BTreeMap<String, usize>>>);

impl Computes {
    fn record(ctx: &DiceComputations, key: &dyn std::fmt::Display) {
        if let Ok(c) = ctx.per_transaction_data().data.get::<Computes>() {
            *c.0.lock().unwrap().entry(key.to_string()).or_default() += 1;
        }
    }

    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().unwrap())
            .into_keys()
            .collect()
    }

    fn data(&self) -> UserComputationData {
        let mut d = UserComputationData::new();
        d.data.set(self.dupe());
        d
    }
}

#[derive(Allocative, Clone, Dupe, Debug, Display, PartialEq, Eq, Hash, Pagable)]
#[display("Input({_0})")]
#[pagable_typetag(DiceKeyDyn)]
struct Input(u32);

impl InjectedKey for Input {
    type Value = u64;

    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| x == y)
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        PagableValueSerialize::<Self::Value>::new()
    }
}

#[derive(Allocative, Clone, Dupe, Debug, Display, PartialEq, Eq, Hash, Pagable)]
#[display("Leaf({_0})")]
#[pagable_typetag(DiceKeyDyn)]
struct Leaf(u32);

#[async_trait]
impl Key for Leaf {
    type Value = u64;

    async fn compute(
        &self,
        ctx: &mut DiceComputations,
        _cancellations: &CancellationContext,
    ) -> Self::Value {
        Computes::record(ctx, self);
        *ctx.compute(&Input(self.0)).await.unwrap() * 10
    }

    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| x == y)
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        PagableValueSerialize::<Self::Value>::new()
    }
}

/// The sum of every leaf, read only through [`Half`].
#[derive(Allocative, Clone, Dupe, Debug, Display, PartialEq, Eq, Hash, Pagable)]
#[display("Sum")]
#[pagable_typetag(DiceKeyDyn)]
struct Sum;

const LEAVES: u32 = 4;

#[async_trait]
impl Key for Sum {
    type Value = u64;

    async fn compute(
        &self,
        ctx: &mut DiceComputations,
        _cancellations: &CancellationContext,
    ) -> Self::Value {
        Computes::record(ctx, self);
        let mut sum = 0;
        for i in 0..LEAVES {
            sum += *ctx.compute(&Leaf(i)).await.unwrap();
        }
        sum
    }

    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| x == y)
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        PagableValueSerialize::<Self::Value>::new()
    }
}

#[derive(Allocative, Clone, Dupe, Debug, Display, PartialEq, Eq, Hash, Pagable)]
#[display("Half")]
#[pagable_typetag(DiceProjectionDyn)]
struct Half;

impl ProjectionKey for Half {
    type DeriveFromKey = Sum;
    type Value = u64;

    fn compute(&self, sum: &u64, _ctx: &DiceProjectionComputations) -> u64 {
        sum / 2
    }

    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| x == y)
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        ResidentValueSerialize::new(PagableValueSerialize::<Self::Value>::new())
    }
}

/// Reads `Half` and leaf 0.
#[derive(Allocative, Clone, Dupe, Debug, Display, PartialEq, Eq, Hash, Pagable)]
#[display("Top")]
#[pagable_typetag(DiceKeyDyn)]
struct Top;

#[async_trait]
impl Key for Top {
    type Value = u64;

    async fn compute(
        &self,
        ctx: &mut DiceComputations,
        _cancellations: &CancellationContext,
    ) -> Self::Value {
        Computes::record(ctx, self);
        let sum = ctx.compute_opaque(&Sum).await.unwrap();
        let half = ctx.projection(&sum, &Half).unwrap();
        half + *ctx.compute(&Leaf(0)).await.unwrap()
    }

    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| x == y)
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        PagableValueSerialize::<Self::Value>::new()
    }
}

/// A key whose values are never written: an image cannot carry it.
#[derive(Allocative, Clone, Dupe, Debug, Display, PartialEq, Eq, Hash, Pagable)]
#[display("Unwritten")]
#[pagable_typetag(DiceKeyDyn)]
struct Unwritten;

#[async_trait]
impl Key for Unwritten {
    type Value = u64;

    async fn compute(
        &self,
        ctx: &mut DiceComputations,
        _cancellations: &CancellationContext,
    ) -> Self::Value {
        Computes::record(ctx, self);
        *ctx.compute(&Leaf(1)).await.unwrap() + 1
    }

    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| x == y)
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        NoValueSerialize::<Self::Value>::new()
    }
}

/// Reads [`Unwritten`], so it goes when that does.
#[derive(Allocative, Clone, Dupe, Debug, Display, PartialEq, Eq, Hash, Pagable)]
#[display("AboveUnwritten")]
#[pagable_typetag(DiceKeyDyn)]
struct AboveUnwritten;

#[async_trait]
impl Key for AboveUnwritten {
    type Value = u64;

    async fn compute(
        &self,
        ctx: &mut DiceComputations,
        _cancellations: &CancellationContext,
    ) -> Self::Value {
        Computes::record(ctx, self);
        *ctx.compute(&Unwritten).await.unwrap() * 2
    }

    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| x == y)
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        PagableValueSerialize::<Self::Value>::new()
    }
}

async fn open(dir: &TempDir) -> anyhow::Result<(Arc<Dice>, BranchId)> {
    let mut storage = DiceStorage::open(dir.path(), PagableStorageBackend::Sqlite)?;
    storage.set_page_out_policy(PageOutPolicy::OncePerValue);
    storage.set_eviction_policy(EvictionPolicy::OnIdle);
    let mut builder = Dice::builder();
    builder.set_pagable_storage(storage);
    let dice = builder.build(DetectCycles::Disabled);
    let root = dice.new_root().await;
    Ok((dice, root))
}

async fn inject(dice: &Arc<Dice>, root: BranchId, inputs: &[(u32, u64)]) {
    let mut updater = dice.updater(root);
    updater
        .changed_to(
            inputs
                .iter()
                .map(|(k, v)| (Input(*k), *v))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    drop(updater.commit().await);
}

/// Computes `Top` and `AboveUnwritten`, returning their values and the keys that ran.
async fn compute_all(
    dice: &Arc<Dice>,
    root: BranchId,
    computes: &Computes,
) -> anyhow::Result<((u64, u64), Vec<String>)> {
    let tx = dice.updater_with_data(root, computes.data()).commit().await;
    let top = *tx.compute(&Top).await?;
    let above = *tx.compute(&AboveUnwritten).await?;
    drop(tx);
    dice.wait_for_idle().await;
    Ok(((top, above), computes.take()))
}

fn initial_inputs() -> Vec<(u32, u64)> {
    (0..LEAVES).map(|i| (i, u64::from(i) + 1)).collect()
}

/// Suspends a computed graph and resumes it in a second `Dice` over the same store.
async fn suspend_and_resume(
    dir: &TempDir,
) -> anyhow::Result<(
    Arc<Dice>,
    BranchId,
    (u64, u64),
    crate::SuspendStats,
    crate::ResumeStats,
)> {
    let (dice, root) = open(dir).await?;
    inject(&dice, root, &initial_inputs()).await;
    let (values, ran) = compute_all(&dice, root, &Computes::default()).await?;
    assert!(ran.contains(&"Top".to_owned()));

    let head = dice.head(root).await;
    let image = dice.suspend_image(head).await?;
    drop(dice);

    let (resumed, root) = open(dir).await?;
    let resume = resumed.restore_image(root, &image.bytes).await?;
    Ok((resumed, root, values, image.stats, resume))
}

#[tokio::test]
async fn resumed_graph_serves_every_carried_value_without_computing() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let (dice, root, values, suspended, resumed) = suspend_and_resume(&dir).await?;

    // The unwritable key and the key above it are the only ones left out.
    assert_eq!(
        suspended.dropped,
        BTreeMap::from([
            ((SuspendDrop::NoRow, "Unwritten"), 1),
            ((SuspendDrop::Transitive, "AboveUnwritten"), 1),
        ])
    );
    assert_eq!(resumed.dropped, 0);
    assert_eq!(resumed.asserted, LEAVES as usize);
    // Leaves, Sum, Half and Top.
    assert_eq!(resumed.certified, LEAVES as usize + 3);
    assert!(
        suspended.rows_written >= LEAVES as usize + 1,
        "the injected inputs and the projection are written by the suspend itself"
    );

    let computes = Computes::default();
    let (again, ran) = compute_all(&dice, root, &computes).await?;
    assert_eq!(again, values);
    assert_eq!(
        ran,
        vec!["AboveUnwritten".to_owned(), "Unwritten".to_owned()],
        "only what the image could not carry is computed"
    );
    Ok(())
}

#[tokio::test]
async fn resumed_graph_keeps_equal_inputs_and_recomputes_changed_ones() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let (dice, root, values, _, _) = suspend_and_resume(&dir).await?;

    // Re-injecting what was injected changes nothing, as on a live daemon.
    inject(&dice, root, &initial_inputs()).await;
    let computes = Computes::default();
    let (again, ran) = compute_all(&dice, root, &computes).await?;
    assert_eq!(again, values);
    assert!(!ran.contains(&"Top".to_owned()), "ran: {ran:?}");

    // A changed input recomputes exactly its cone.
    inject(&dice, root, &[(2, 100)]).await;
    let (changed, ran) = compute_all(&dice, root, &computes).await?;
    assert_ne!(changed.0, values.0);
    assert_eq!(
        ran,
        vec!["Leaf(2)".to_owned(), "Sum".to_owned(), "Top".to_owned()]
    );
    Ok(())
}

#[tokio::test]
async fn an_image_cannot_be_restored_over_an_emptied_store() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let (dice, root) = open(&dir).await?;
    inject(&dice, root, &initial_inputs()).await;
    compute_all(&dice, root, &Computes::default()).await?;
    let head = dice.head(root).await;
    let image = dice.suspend_image(head).await?;
    drop(dice);

    let other = tempdir()?;
    let (fresh, root) = open(&other).await?;
    assert!(
        fresh.restore_image(root, &image.bytes).await.is_err(),
        "the rows an image names live only in the store it was written over"
    );
    // Nothing was installed: the graph computes from scratch.
    inject(&fresh, root, &initial_inputs()).await;
    let (_, ran) = compute_all(&fresh, root, &Computes::default()).await?;
    assert!(ran.contains(&"Top".to_owned()));
    Ok(())
}

/// What a resumed graph computes survives the next suspend and resume, as what the first graph
/// computed did.
#[tokio::test]
async fn a_resumed_graph_suspends_and_resumes_again() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let (dice, root, _, _, _) = suspend_and_resume(&dir).await?;

    // Change one input in the resumed graph and recompute its cone there.
    inject(&dice, root, &initial_inputs()).await;
    inject(&dice, root, &[(2, 100)]).await;
    let (changed, ran) = compute_all(&dice, root, &Computes::default()).await?;
    assert!(ran.contains(&"Leaf(2)".to_owned()), "ran: {ran:?}");

    let head = dice.head(root).await;
    let image = dice.suspend_image(head).await?;
    drop(dice);

    let (again, root) = open(&dir).await?;
    let resumed = again.restore_image(root, &image.bytes).await?;
    assert_eq!(resumed.dropped, 0);
    let mut inputs = initial_inputs();
    inputs[2].1 = 100;
    inject(&again, root, &inputs).await;
    let (values, ran) = compute_all(&again, root, &Computes::default()).await?;
    assert_eq!(values, changed);
    assert_eq!(
        ran,
        vec!["AboveUnwritten".to_owned(), "Unwritten".to_owned()],
        "nothing either graph computed is computed again but what no image carries"
    );
    Ok(())
}

/// An input changed and committed but not yet recomputed when the graph is suspended: after the
/// resume, a recompute that finds the same value cuts its dependents off, as it would have on the
/// suspended graph.
#[tokio::test]
async fn an_input_dirtied_before_the_suspend_cuts_off_after_the_resume() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let (dice, root) = open(&dir).await?;
    inject(&dice, root, &initial_inputs()).await;
    compute_all(&dice, root, &Computes::default()).await?;
    // Dirty a leaf without changing anything it reads, and suspend before anything recomputes.
    let mut updater = dice.updater(root);
    updater.changed(vec![Leaf(1)])?;
    drop(updater.commit().await);
    let head = dice.head(root).await;
    let image = dice.suspend_image(head).await?;
    assert!(image.stats.candidates > 0);
    drop(dice);

    let (resumed, root) = open(&dir).await?;
    let stats = resumed.restore_image(root, &image.bytes).await?;
    assert!(stats.candidates > 0, "{stats:?}");
    inject(&resumed, root, &initial_inputs()).await;
    let (_, ran) = compute_all(&resumed, root, &Computes::default()).await?;
    assert!(ran.contains(&"Leaf(1)".to_owned()), "ran: {ran:?}");
    assert!(
        !ran.contains(&"Sum".to_owned()) && !ran.contains(&"Top".to_owned()),
        "an equal value cuts off: {ran:?}"
    );
    Ok(())
}

/// Flips the last byte of every row in every shard of the paging store under `dir`, as damage
/// to the store while no daemon ran would.
fn damage_every_row(dir: &std::path::Path) -> anyhow::Result<usize> {
    let mut damaged = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d)? {
            let path = e?.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if path.is_dir() {
                stack.push(path);
            } else if name.starts_with("pagable.") && name.ends_with(".db") {
                let conn = rusqlite::Connection::open(&path)?;
                let rows: Vec<(i64, Vec<u8>)> = conn
                    .prepare("SELECT rowid, value FROM pagable_data")?
                    .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                    .collect::<Result<_, _>>()?;
                for (rowid, mut value) in rows {
                    if let Some(last) = value.last_mut() {
                        *last ^= 0xff;
                    }
                    conn.execute(
                        "UPDATE pagable_data SET value = ?1 WHERE rowid = ?2",
                        rusqlite::params![value, rowid],
                    )?;
                    damaged += 1;
                }
            }
        }
    }
    Ok(damaged)
}

/// A row written beside the image (the materializer's artifacts, the local actions) reads back
/// in the next process, and once the store is damaged it fails its read rather than handing back
/// other data, as do the rows `restore_image` hydrates; `buck2 resume` turns either failure into
/// a cold start.
#[tokio::test]
async fn a_damaged_store_fails_the_reads_of_a_resume() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let (dice, root) = open(&dir).await?;
    inject(&dice, root, &initial_inputs()).await;
    compute_all(&dice, root, &Computes::default()).await?;
    let row = dice.write_suspend_row(&vec![1u64, 2, 3])?;
    let head = dice.head(root).await;
    let image = dice.suspend_image(head).await?;
    drop(dice);

    let (resumed, _) = open(&dir).await?;
    assert_eq!(
        resumed.read_suspend_row::<Vec<u64>>(row).await?,
        vec![1, 2, 3]
    );
    drop(resumed);

    assert!(damage_every_row(dir.path())? > 0);
    let (damaged, root) = open(&dir).await?;
    assert!(damaged.read_suspend_row::<Vec<u64>>(row).await.is_err());
    assert!(damaged.restore_image(root, &image.bytes).await.is_err());
    Ok(())
}
