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

/// A restore read in full and then dropped, as `buck2 resume` drops one when the materializer
/// lost an artifact it read at the same time, installs nothing: the graph computes from scratch.
#[tokio::test]
async fn a_prepared_restore_installs_nothing_until_installed() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let (dice, root) = open(&dir).await?;
    inject(&dice, root, &initial_inputs()).await;
    compute_all(&dice, root, &Computes::default()).await?;
    let head = dice.head(root).await;
    let image = dice.suspend_image(head).await?;
    drop(dice);

    let (resumed, root) = open(&dir).await?;
    let prepared = resumed.prepare_restore(root, &image.bytes).await?;
    drop(prepared);
    inject(&resumed, root, &initial_inputs()).await;
    let (_, ran) = compute_all(&resumed, root, &Computes::default()).await?;
    assert!(ran.contains(&"Top".to_owned()), "ran: {ran:?}");
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

/// Node `i` of a binary tree of `n` nodes over [`Input`]s: the bench's graph. The third field
/// is `i` again, behind an `Arc`, so each key's row names an arc row, as buck2's keys' rows do.
#[derive(Allocative, Clone, Dupe, Debug, Display, PartialEq, Eq, Hash, Pagable)]
#[display("Node({_0}/{_1})")]
#[pagable_typetag(DiceKeyDyn)]
struct Node(u32, u32, Arc<u32>);

impl Node {
    fn new(i: u32, n: u32) -> Self {
        Node(i, n, Arc::new(i))
    }
}

#[async_trait]
impl Key for Node {
    type Value = u64;

    async fn compute(
        &self,
        ctx: &mut DiceComputations,
        _cancellations: &CancellationContext,
    ) -> Self::Value {
        Computes::record(ctx, self);
        let (i, n) = (self.0, self.1);
        let mut sum = *ctx.compute(&Input(i % LEAVES)).await.unwrap();
        for child in [2 * i + 1, 2 * i + 2] {
            if child < n {
                sum += *ctx.compute(&Node::new(child, n)).await.unwrap();
            }
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

/// Times a suspend and a resume of a graph of `DICE_SUSPEND_BENCH_KEYS` keys (default 100,000)
/// over a sqlite store under `DICE_SUSPEND_BENCH_DIR` (default a temporary directory), printing
/// each step. Not a test: `cargo test -p dice --release -- --ignored --nocapture suspend_bench`.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn suspend_bench() -> anyhow::Result<()> {
    let n: u32 = std::env::var("DICE_SUSPEND_BENCH_KEYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);
    let dir = match std::env::var_os("DICE_SUSPEND_BENCH_DIR") {
        Some(d) => tempfile::tempdir_in(d)?,
        None => tempdir()?,
    };
    let (dice, root) = open(&dir).await?;
    inject(&dice, root, &initial_inputs()).await;
    let t = std::time::Instant::now();
    let tx = dice.updater(root).commit().await;
    let value = *tx.compute(&Node::new(0, n)).await?;
    drop(tx);
    dice.wait_for_idle().await;
    eprintln!("compute {n} nodes: {:.3}s", t.elapsed().as_secs_f64());

    let head = dice.head(root).await;
    let t = std::time::Instant::now();
    let image = dice.suspend_image(head).await?;
    eprintln!(
        "suspend: {:.3}s, {} keys, {} certificates, {} bytes, digest {}",
        t.elapsed().as_secs_f64(),
        image.stats.keys,
        image.stats.certified,
        image.bytes.len(),
        {
            use std::hash::Hash;
            use std::hash::Hasher;
            // Fixed keys: the same in every process, so two builds' images compare.
            let mut h = std::collections::hash_map::DefaultHasher::new();
            image.bytes.hash(&mut h);
            format!("{:016x}", h.finish())
        }
    );
    for (step, d) in &image.stats.timings {
        eprintln!("  suspend {step}: {:.3}s", d.as_secs_f64());
    }
    assert_eq!(image.stats.dropped_total(), 0);
    drop(dice);
    // `DICE_SUSPEND_BENCH_EVICT=<command>` runs `<command> <store>` here, e.g. `vmtouch -e`, to
    // resume from a store the page cache does not hold, as on a runner the image was carried to.
    if let Some(evict) = std::env::var_os("DICE_SUSPEND_BENCH_EVICT") {
        let evict = evict.to_string_lossy().into_owned();
        let mut words = evict.split_whitespace();
        let status = std::process::Command::new(words.next().unwrap())
            .args(words)
            .arg(dir.path())
            .status()?;
        assert!(status.success());
    }

    let (resumed, root) = open(&dir).await?;
    let t = std::time::Instant::now();
    let stats = resumed.restore_image(root, &image.bytes).await?;
    eprintln!(
        "resume: {:.3}s, {} keys",
        t.elapsed().as_secs_f64(),
        stats.keys
    );
    for (step, d) in &stats.timings {
        eprintln!("  resume {step}: {:.3}s", d.as_secs_f64());
    }
    assert_eq!(stats.dropped, 0);
    inject(&resumed, root, &initial_inputs()).await;
    let tx = resumed.updater(root).commit().await;
    assert_eq!(*tx.compute(&Node::new(0, n)).await?, value);
    Ok(())
}

/// A sqlite store that counts reads of rows it may still hold in a write buffer: rows stored
/// since the last flush. sqlite finds such a row only by flushing its shard, a commit per read.
struct CountingStore {
    inner: pagable_storage::storage::sqlite::SqliteBackedPagableStorage,
    unflushed: Mutex<std::collections::HashSet<pagable::DataKey>>,
    buffered_reads: Arc<std::sync::atomic::AtomicUsize>,
    /// Reads of the commit frontier: one per bind of the arcs a page-out wrote.
    frontier_reads: Arc<std::sync::atomic::AtomicUsize>,
}

impl CountingStore {
    fn note_read(&self, key: &pagable::DataKey) {
        if self.unflushed.lock().unwrap().contains(key) {
            self.buffered_reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

#[async_trait]
impl pagable::storage::traits::PagableStorage for CountingStore {
    fn arc_cache(&self) -> &pagable::storage::traits::DeserializedArcCache {
        self.inner.arc_cache()
    }

    fn fetch_data_blocking(
        &self,
        key: &pagable::DataKey,
    ) -> anyhow::Result<Arc<pagable::storage::data::PagableData>> {
        self.note_read(key);
        self.inner.fetch_data_blocking(key)
    }

    async fn fetch_data(
        &self,
        key: &pagable::DataKey,
    ) -> anyhow::Result<Arc<pagable::storage::data::PagableData>> {
        self.note_read(key);
        self.inner.fetch_data(key).await
    }

    fn schedule_for_paging(&self, arc: Box<dyn pagable::arc_erase::ArcEraseDyn>) {
        self.inner.schedule_for_paging(arc)
    }

    fn storage_context(&self) -> &pagable::StorageContext {
        self.inner.storage_context()
    }

    fn store_data_ticketed(
        &self,
        data: pagable::storage::data::PagableData,
    ) -> anyhow::Result<(pagable::DataKey, pagable::storage::traits::WriteTicket)> {
        let stored = self.inner.store_data_ticketed(data)?;
        self.unflushed.lock().unwrap().insert(stored.0);
        Ok(stored)
    }

    fn commit_frontier(&self) -> pagable::storage::traits::CommitFrontier {
        self.frontier_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.commit_frontier()
    }

    fn discard_unwritten(&self) {
        self.inner.discard_unwritten()
    }

    fn flush(&self) -> anyhow::Result<()> {
        self.inner.flush()?;
        self.unflushed.lock().unwrap().clear();
        Ok(())
    }

    fn release_memory(&self) {
        self.inner.release_memory()
    }
}

/// A suspend writes every key's row before it reads any back, so it never reads a row its
/// store still buffers; and it binds the arcs those rows name once, not after every row. It used
/// to read each key back as it wrote it, which over sqlite flushed a shard per key (640 s for
/// mwb's 1.3M keys), and with that gone, binding after every row scanned every arc not yet
/// committed, quadratic in the rows written between flushes.
#[tokio::test]
async fn a_suspend_reads_no_key_row_its_store_still_buffers() -> anyhow::Result<()> {
    const NODES: u32 = 2_000;
    let dir = tempdir()?;
    let buffered_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let frontier_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let store = Arc::new(CountingStore {
        inner: pagable_storage::storage::sqlite::SqliteBackedPagableStorage::try_new(
            dir.path(),
        )?,
        unflushed: Mutex::new(Default::default()),
        buffered_reads: buffered_reads.dupe(),
        frontier_reads: frontier_reads.dupe(),
    });
    let open_over = |store: Arc<CountingStore>| async move {
        let mut storage = DiceStorage::new(store);
        storage.set_page_out_policy(PageOutPolicy::OncePerValue);
        storage.set_eviction_policy(EvictionPolicy::OnIdle);
        let mut builder = Dice::builder();
        builder.set_pagable_storage(storage);
        let dice = builder.build(DetectCycles::Disabled);
        let root = dice.new_root().await;
        (dice, root)
    };

    let (dice, root) = open_over(store.dupe()).await;
    inject(&dice, root, &initial_inputs()).await;
    let tx = dice.updater(root).commit().await;
    let value = *tx.compute(&Node::new(0, NODES)).await?;
    drop(tx);
    dice.wait_for_idle().await;
    let head = dice.head(root).await;
    buffered_reads.store(0, std::sync::atomic::Ordering::Relaxed);
    // The page-out at the start of the suspend reads the frontier a few times on its own.
    dice.page_out().await?;
    frontier_reads.store(0, std::sync::atomic::Ordering::Relaxed);
    let image = dice.suspend_image(head).await?;
    assert_eq!(image.stats.dropped_total(), 0);
    assert!(image.stats.keys > NODES as usize, "{:?}", image.stats);
    assert_eq!(
        buffered_reads.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "every buffered row read is a commit of its shard"
    );
    let binds = frontier_reads.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        binds < 10,
        "{binds} binds for {} keys: each scans every arc not yet committed",
        image.stats.keys
    );
    drop(dice);

    // The image means what it meant: the resumed graph serves the value without computing.
    let (resumed, root) = open_over(store).await;
    let stats = resumed.restore_image(root, &image.bytes).await?;
    assert_eq!(stats.dropped, 0);
    assert_eq!(stats.keys, image.stats.keys);
    inject(&resumed, root, &initial_inputs()).await;
    let computes = Computes::default();
    let tx = resumed.updater_with_data(root, computes.data()).commit().await;
    assert_eq!(*tx.compute(&Node::new(0, NODES)).await?, value);
    assert_eq!(computes.take(), Vec::<String>::new());
    Ok(())
}
