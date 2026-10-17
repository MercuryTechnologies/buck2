/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Suspending DICE to disk and resuming it in a new process.
//!
//! Paging already writes most values as content-addressed rows; what it does not write is the
//! core state that names them. A suspend writes that as an *image* of one version: every key the
//! version resolves `Valid`, the row of the value behind its revision, and how it got that
//! revision (an assertion, or a certificate and its premises). Values paging keeps in memory are
//! written as rows first, and keys are written as rows too, so the image itself is small.
//!
//! A resume replays the image onto a new root through the core's own operations: one commit of
//! the assertions at their recorded revisions, then each certificate through `write`, premises
//! first. `write` installs a certificate only where every premise resolves to its recorded
//! revision, so whatever the image could not carry (a key that did not survive a round trip
//! through its serializer, a value whose serializer declined it) costs the certificates that read
//! it a recompute, and never makes a stale value valid. Each certificate's ε is reset to
//! `EpsilonToken::INITIAL`: nothing has been dirtied on a new root, and every token this process
//! mints differs from it. Revision mints are restored as they stood, so no revision is minted twice.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc as StdArc;

use dice_core::BranchId;
use dupe::Dupe;
use pagable::DataKey;
use pagable::storage::traits::ArcSerCache;
use serde::Deserialize;
use serde::Serialize;

use crate::HashMap;
use crate::api::key::InvalidationSourcePriority;
use crate::core::graph::HeadSource;
use crate::core::graph::HeadValue;
use crate::core::graph::RestoredAssert;
use crate::core::graph::RestoredCert;
use crate::core::graph::revision::Revision;
use crate::core::state::RestoreHead;
use crate::deps::graph::DepsImage;
use crate::deps::graph::SeriesParallelDeps;
use crate::dice::Dice;
use crate::key::CowDiceKeyHashed;
use crate::key::DiceKey;
use crate::key::DiceKeyErased;
use crate::value::MaybeResident;

/// Bumped on any change to [`Image`]'s encoding. The binary digest in buck2's MANIFEST already
/// refuses an image written by another build; this guards a hand-copied image.
const IMAGE_FORMAT: u32 = 2;

/// A row's `DataKey` as 16 bytes.
pub type Row = [u8; 16];

/// The 16 bytes `DataKey::from_stored_bytes` reads.
fn row(key: DataKey) -> Row {
    let v = key.get();
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&(v as u64).to_ne_bytes());
    out[8..].copy_from_slice(&((v >> 64) as u64).to_ne_bytes());
    out
}

fn data_key(row: Row) -> anyhow::Result<DataKey> {
    DataKey::from_stored_bytes(row)
}

#[derive(Serialize, Deserialize)]
struct Image {
    format: u32,
    /// Ordinal order; a projection's base comes before it.
    keys: Vec<ImageKey>,
    asserted: Vec<ImageAssert>,
    /// Premises before the certificates that read them.
    certs: Vec<ImageCert>,
    /// Keys the head had dirtied since their certificate: `(key ordinal, priority)`.
    dirtied: Vec<(u32, u8)>,
}

#[derive(Serialize, Deserialize)]
enum ImageKeyKind {
    Key,
    Projection { base: u32 },
}

#[derive(Serialize, Deserialize)]
struct ImageKey {
    kind: ImageKeyKind,
    row: Row,
    /// The revision the key's mint hands out next; 0 for a key the image holds no value of.
    mint: u32,
}

#[derive(Serialize, Deserialize)]
struct ImageAssert {
    key: u32,
    revision: u32,
    priority: u8,
    injected: bool,
    row: Row,
}

#[derive(Serialize, Deserialize)]
struct ImageCert {
    key: u32,
    revision: u32,
    deps: DepsImage,
    row: Row,
    /// The key's values never page out (a projection's), so the resume reads the value now.
    resident: bool,
}

fn priority_to_u8(p: InvalidationSourcePriority) -> u8 {
    match p {
        InvalidationSourcePriority::Ignored => 0,
        InvalidationSourcePriority::Normal => 1,
        InvalidationSourcePriority::High => 2,
    }
}

fn priority_from_u8(p: u8) -> anyhow::Result<InvalidationSourcePriority> {
    Ok(match p {
        0 => InvalidationSourcePriority::Ignored,
        1 => InvalidationSourcePriority::Normal,
        2 => InvalidationSourcePriority::High,
        other => return Err(anyhow::anyhow!("unknown invalidation priority {other}")),
    })
}

/// Why a suspend left a key out of its image. A key left out costs a recompute of it, and of
/// every key that read it, after the resume.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SuspendDrop {
    /// The key's serializer declined its value (an `Err`, or a `NoValueSerialize` key).
    NoRow,
    /// Serializing the value failed.
    ValueError,
    /// The key did not survive a round trip through its serializer: it errored, panicked, or
    /// read back unequal, or with another hash.
    KeyRoundTrip,
    /// A premise of its certificate was left out.
    Transitive,
}

impl SuspendDrop {
    pub fn as_str(self) -> &'static str {
        match self {
            SuspendDrop::NoRow => "no_row",
            SuspendDrop::ValueError => "value_error",
            SuspendDrop::KeyRoundTrip => "key_round_trip",
            SuspendDrop::Transitive => "transitive",
        }
    }
}

/// What a suspend wrote.
#[derive(Debug, Default, Clone)]
pub struct SuspendStats {
    /// Keys in the image's key table.
    pub keys: usize,
    pub asserted: usize,
    /// Certificates of keys valid at the head, and of keys unknown at it but with a candidate.
    pub certified: usize,
    /// Of which candidates.
    pub candidates: usize,
    /// Values only memory held, written as rows by the suspend.
    pub rows_written: usize,
    /// Keys left out, by cause and key type.
    pub dropped: BTreeMap<(SuspendDrop, &'static str), usize>,
    /// How long each step took: the page-out, reading the head, writing the values only memory
    /// held, writing and checking the key rows, ordering the certificates, making the store
    /// durable, and encoding the image.
    pub timings: Vec<(&'static str, std::time::Duration)>,
}

impl SuspendStats {
    pub fn dropped_total(&self) -> usize {
        self.dropped.values().sum()
    }
}

/// What a resume replayed.
#[derive(Debug, Default, Clone)]
pub struct ResumeStats {
    pub keys: usize,
    pub asserted: usize,
    /// Certificates installed and valid at the head.
    pub certified: usize,
    /// Certificates installed and unknown at the head, as candidates.
    pub candidates: usize,
    /// Certificates `write` did not install.
    pub dropped: usize,
    /// How long each step took: reading the key rows, reading the asserted values, reading the
    /// certificates (with the resident values), and installing the head.
    pub timings: Vec<(&'static str, std::time::Duration)>,
}

/// The graph's part of a suspended daemon: an [`Image`]'s bytes and what went into it.
pub struct SuspendImage {
    pub bytes: Vec<u8>,
    pub stats: SuspendStats,
}

impl Dice {
    /// Writes an image of version `at` for a later [`Dice::restore_image`] in another process.
    ///
    /// The caller must hold DICE idle (no computation running) and must not commit or compute
    /// until the process exits: the image names rows by content and keys by value, and is only
    /// meaningful together with the paging store as this call leaves it, which it flushes and
    /// fsyncs before returning.
    pub async fn suspend_image(
        self: &StdArc<Self>,
        at: dice_core::Version,
    ) -> anyhow::Result<SuspendImage> {
        let storage = self
            .pagable_storage
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("suspend needs pagable storage"))?
            .dupe();
        let mut timings = Vec::new();
        let mut step = std::time::Instant::now();
        let mut lap = |name: &'static str, timings: &mut Vec<_>| {
            timings.push((name, step.elapsed()));
            step = std::time::Instant::now();
        };
        // The values a page-out writes are written the usual way, in parallel; what is left is
        // what paging keeps in memory.
        self.page_out().await?;
        lap("page out", &mut timings);

        let head = self.state_handle.dump_head(at).await;
        lap("head", &mut timings);
        let mut stats = SuspendStats::default();
        let finished = ArcSerCache::new();

        // Every entry gets a row, or is dropped.
        struct Entry {
            key: DiceKey,
            revision: Revision,
            source: HeadSource,
            row: DataKey,
            mint: Revision,
            injected: bool,
            resident: bool,
        }
        let mut entries: HashMap<DiceKey, Entry> = HashMap::default();
        for e in head {
            let key_dyn = self.key_index.get(e.key);
            let row = match e.value {
                HeadValue::Row(row) => row,
                HeadValue::Resident(value) => {
                    let written = std::panic::catch_unwind(AssertUnwindSafe(|| {
                        storage.write_value_row(key_dyn, &value, &finished)
                    }));
                    match written {
                        Ok(Ok(Some(row))) => {
                            stats.rows_written += 1;
                            row
                        }
                        Ok(Ok(None)) => {
                            *stats
                                .dropped
                                .entry((SuspendDrop::NoRow, key_dyn.key_type_name()))
                                .or_default() += 1;
                            continue;
                        }
                        Ok(Err(_)) | Err(_) => {
                            *stats
                                .dropped
                                .entry((SuspendDrop::ValueError, key_dyn.key_type_name()))
                                .or_default() += 1;
                            continue;
                        }
                    }
                }
            };
            entries.insert(
                e.key,
                Entry {
                    key: e.key,
                    revision: e.revision,
                    source: e.source,
                    row,
                    mint: e.mint,
                    injected: e.injected,
                    resident: !key_dyn.value_pages_out(),
                },
            );
        }

        lap("value rows", &mut timings);

        // The key table. Every key's row is written, with every projection's base, and read
        // back: no key serializer ran before this feature, since paging writes values only, so
        // a key that does not come back equal is left out rather than trusted. A key is read
        // back from its row's data as written, not from the store, and only once every row is
        // written and flushed, for the arcs a row names. Reading each row back from the store as
        // it was written found it still in its shard's write buffer, and that read flushed the
        // shard: one commit per key, 640 s for mwb's 1.3M keys; and a read of the store costs
        // more than the decode, about 6 us a row whether one reader or many.
        let mut ordered: Vec<DiceKey> = entries.keys().copied().collect();
        ordered.sort_by_key(|k| k.index);
        // Every key whose row was written or failed to be.
        let mut tried: crate::HashSet<DiceKey> = crate::HashSet::default();
        let mut checks: Vec<(DiceKey, DataKey, pagable::storage::data::PagableData)> =
            Vec::new();
        for &entry_key in &ordered {
            let mut next = Some(entry_key);
            while let Some(key) = next.take() {
                if !tried.insert(key) {
                    break;
                }
                let erased = self.key_index.get(key);
                let base = match erased {
                    DiceKeyErased::Key(_) => None,
                    DiceKeyErased::Projection(p) => Some(p.base()),
                };
                let row = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    storage.write_key_row(erased, &finished)
                }));
                if let Ok(Ok((row, data))) = row {
                    checks.push((key, row, data));
                }
                next = base;
            }
        }
        lap("key rows written", &mut timings);
        drop(tried);
        storage.flush()?;
        lap("key rows flushed", &mut timings);
        let mut round_tripped: HashMap<DiceKey, DataKey> = HashMap::default();
        for (key, row, data) in checks {
            let erased = self.key_index.get(key);
            let base = match erased {
                DiceKeyErased::Key(_) => None,
                DiceKeyErased::Projection(p) => Some(p.base()),
            };
            // The row's key names its data, so what is decoded is what a resume reads.
            if data.compute_key() != row {
                continue;
            }
            let back = std::panic::catch_unwind(AssertUnwindSafe(|| {
                storage.decode_key_row(row, &data, base)
            }));
            if let Ok(Ok(back)) = back
                && back == *erased
                && back.hash() == erased.hash()
            {
                round_tripped.insert(key, row);
            }
        }
        let mut ordinals: HashMap<DiceKey, Option<u32>> = HashMap::default();
        let mut keys: Vec<ImageKey> = Vec::new();
        for key in ordered {
            self.ensure_ordinal(key, &round_tripped, &mut ordinals, &mut keys);
        }

        lap("key rows checked", &mut timings);

        // Mints of entry keys, recorded beside their key.
        for (key, entry) in &entries {
            if let Some(Some(ordinal)) = ordinals.get(key) {
                keys[*ordinal as usize].mint = entry.mint.as_u32();
            }
        }

        // Assertions, and certificates in post-order over their premises.
        let mut asserted = Vec::new();
        let mut kept: HashMap<DiceKey, bool> = HashMap::default();
        for entry in entries.values() {
            if let HeadSource::Asserted { priority } = &entry.source {
                match ordinals.get(&entry.key).copied().flatten() {
                    Some(ordinal) => {
                        asserted.push(ImageAssert {
                            key: ordinal,
                            revision: entry.revision.as_u32(),
                            priority: priority_to_u8(*priority),
                            injected: entry.injected,
                            row: row(entry.row),
                        });
                        kept.insert(entry.key, true);
                    }
                    None => {
                        *stats
                            .dropped
                            .entry((
                                SuspendDrop::KeyRoundTrip,
                                self.key_index.get(entry.key).key_type_name(),
                            ))
                            .or_default() += 1;
                        kept.insert(entry.key, false);
                    }
                }
            }
        }

        let mut certs = Vec::new();
        let mut dirtied_keys = Vec::new();
        let mut roots: Vec<DiceKey> = entries
            .values()
            .filter(|e| {
                matches!(
                    e.source,
                    HeadSource::Certified { .. } | HeadSource::Candidate { .. }
                )
            })
            .map(|e| e.key)
            .collect();
        roots.sort_by_key(|k| k.index);
        for root in roots {
            // Iterative DFS: graphs are deep.
            let mut stack: Vec<(DiceKey, bool)> = vec![(root, false)];
            while let Some((key, expanded)) = stack.pop() {
                if kept.contains_key(&key) {
                    continue;
                }
                let Some(entry) = entries.get(&key) else {
                    // A premise that is not valid at the head cannot be: `write` checks it.
                    kept.insert(key, false);
                    continue;
                };
                let (HeadSource::Certified { cert } | HeadSource::Candidate { cert, .. }) =
                    &entry.source
                else {
                    unreachable!("assertions are decided above");
                };
                if !expanded {
                    stack.push((key, true));
                    for premise in cert.deps.iter_keys() {
                        if !kept.contains_key(&premise) {
                            stack.push((premise, false));
                        }
                    }
                    continue;
                }
                let key_type = self.key_index.get(key).key_type_name();
                let Some(ordinal) = ordinals.get(&key).copied().flatten() else {
                    *stats
                        .dropped
                        .entry((SuspendDrop::KeyRoundTrip, key_type))
                        .or_default() += 1;
                    kept.insert(key, false);
                    continue;
                };
                let premises_kept = cert
                    .deps
                    .iter_keys()
                    .all(|premise| kept.get(&premise).copied().unwrap_or(false));
                let deps = if premises_kept {
                    cert.deps.to_image(|k| ordinals.get(&k).copied().flatten())
                } else {
                    None
                };
                let Some(deps) = deps else {
                    *stats
                        .dropped
                        .entry((SuspendDrop::Transitive, key_type))
                        .or_default() += 1;
                    kept.insert(key, false);
                    continue;
                };
                certs.push(ImageCert {
                    key: ordinal,
                    revision: entry.revision.as_u32(),
                    deps,
                    row: row(entry.row),
                    resident: entry.resident,
                });
                if let HeadSource::Candidate { dirtied, .. } = &entry.source {
                    stats.candidates += 1;
                    if let Some(priority) = dirtied {
                        dirtied_keys.push((ordinal, priority_to_u8(*priority)));
                    }
                }
                kept.insert(key, true);
            }
        }

        stats.keys = keys.len();
        stats.asserted = asserted.len();
        stats.certified = certs.len();
        lap("certificates", &mut timings);
        storage.flush_and_sync()?;
        lap("sync", &mut timings);
        let image = Image {
            format: IMAGE_FORMAT,
            keys,
            asserted,
            certs,
            dirtied: dirtied_keys,
        };
        let bytes = postcard::to_stdvec(&image)?;
        lap("encode", &mut timings);
        stats.timings = timings;
        Ok(SuspendImage { bytes, stats })
    }

    /// Gives `key` an ordinal in the key table, after its base if it is a projection. `None`
    /// (recorded in `ordinals`) if its row, or its base's, did not round-trip.
    fn ensure_ordinal(
        &self,
        key: DiceKey,
        round_tripped: &HashMap<DiceKey, DataKey>,
        ordinals: &mut HashMap<DiceKey, Option<u32>>,
        keys: &mut Vec<ImageKey>,
    ) -> Option<u32> {
        if let Some(ordinal) = ordinals.get(&key) {
            return *ordinal;
        }
        let kind = match self.key_index.get(key) {
            DiceKeyErased::Key(_) => ImageKeyKind::Key,
            DiceKeyErased::Projection(p) => {
                let Some(base_ordinal) =
                    self.ensure_ordinal(p.base(), round_tripped, ordinals, keys)
                else {
                    ordinals.insert(key, None);
                    return None;
                };
                ImageKeyKind::Projection { base: base_ordinal }
            }
        };
        let ordinal = match round_tripped.get(&key).copied() {
            Some(row_key) => {
                let ordinal = u32::try_from(keys.len()).expect("fewer than 2^32 keys");
                keys.push(ImageKey {
                    kind,
                    row: row(row_key),
                    mint: 0,
                });
                Some(ordinal)
            }
            None => None,
        };
        ordinals.insert(key, ordinal);
        ordinal
    }

    /// Replays an image [`Dice::suspend_image`] wrote onto `branch`, which must be a new root
    /// nothing has been committed to, over the paging store the suspended process left.
    ///
    /// Everything is read before anything is installed, so an error leaves DICE as it was, and
    /// the caller can start cold.
    pub async fn restore_image(
        self: &StdArc<Self>,
        branch: BranchId,
        bytes: &[u8],
    ) -> anyhow::Result<ResumeStats> {
        let storage = self
            .pagable_storage
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("resume needs pagable storage"))?
            .dupe();
        let image: Image = postcard::from_bytes(bytes)?;
        if image.format != IMAGE_FORMAT {
            return Err(anyhow::anyhow!(
                "image format {} is not {IMAGE_FORMAT}",
                image.format
            ));
        }
        storage.verify_reads();
        let mut timings = Vec::new();
        let mut step = std::time::Instant::now();
        let mut lap = |name: &'static str, timings: &mut Vec<_>| {
            timings.push((name, step.elapsed()));
            step = std::time::Instant::now();
        };

        // Read every key before interning any, so a bad image interns nothing.
        let mut erased_keys: Vec<DiceKeyErased> = Vec::with_capacity(image.keys.len());
        let mut dice_keys: Vec<DiceKey> = Vec::with_capacity(image.keys.len());
        for k in &image.keys {
            let base = match k.kind {
                ImageKeyKind::Key => None,
                ImageKeyKind::Projection { base } => Some(
                    *dice_keys
                        .get(base as usize)
                        .ok_or_else(|| anyhow::anyhow!("projection base {base} out of order"))?,
                ),
            };
            let erased = storage.read_key_row(data_key(k.row)?, base).await?;
            // Interned as read: a projection's base must be interned before its projection is
            // read, since the projection is keyed by the base's id.
            let dice_key = self
                .key_index
                .index(CowDiceKeyHashed::erased(erased.dupe()));
            erased_keys.push(erased);
            dice_keys.push(dice_key);
        }
        lap("key rows", &mut timings);
        let ordinal = |o: u32| -> anyhow::Result<DiceKey> {
            dice_keys
                .get(o as usize)
                .copied()
                .ok_or_else(|| anyhow::anyhow!("key ordinal {o} out of range"))
        };
        let revision = |r: u32| -> anyhow::Result<Revision> {
            Revision::from_u32(r).ok_or_else(|| anyhow::anyhow!("revision 0 in image"))
        };

        // Asserted values are read now: an asserted key has no recovery from an unreadable row,
        // and a re-injected value is compared only against resident ones.
        let mut asserts = Vec::with_capacity(image.asserted.len());
        for a in &image.asserted {
            let key = ordinal(a.key)?;
            let row = data_key(a.row)?;
            let value = storage.hydrate(&erased_keys[a.key as usize], row).await?;
            asserts.push(RestoredAssert {
                key,
                revision: revision(a.revision)?,
                priority: priority_from_u8(a.priority)?,
                injected: a.injected,
                value,
                row,
            });
        }
        lap("asserts", &mut timings);
        let mut certs = Vec::with_capacity(image.certs.len());
        for c in &image.certs {
            let key = ordinal(c.key)?;
            let row = data_key(c.row)?;
            let value = if c.resident {
                MaybeResident::Resident(storage.hydrate(&erased_keys[c.key as usize], row).await?)
            } else {
                MaybeResident::PagedOut(row)
            };
            let deps =
                SeriesParallelDeps::from_image(&c.deps, |o| dice_keys.get(o as usize).copied())
                    .ok_or_else(|| anyhow::anyhow!("bad deps for key ordinal {}", c.key))?;
            certs.push(RestoredCert {
                key,
                revision: revision(c.revision)?,
                deps,
                value,
            });
        }
        lap("certificates", &mut timings);
        let mut dirtied = Vec::with_capacity(image.dirtied.len());
        for (k, p) in &image.dirtied {
            dirtied.push((ordinal(*k)?, priority_from_u8(*p)?));
        }
        let mut mints = Vec::new();
        for (i, k) in image.keys.iter().enumerate() {
            if let Some(next) = Revision::from_u32(k.mint) {
                mints.push((dice_keys[i], next));
            }
        }
        let counts = self
            .state_handle
            .restore_head(RestoreHead {
                branch,
                asserts,
                certs,
                dirtied,
                mints,
            })
            .await;
        lap("head", &mut timings);
        Ok(ResumeStats {
            keys: image.keys.len(),
            asserted: counts.asserted,
            certified: counts.certified,
            candidates: counts.candidates,
            dropped: counts.dropped,
            timings,
        })
    }
}

impl Dice {
    /// Writes `value` as a row of the paging store, for state a suspend persists beside the
    /// graph (the materializer's artifacts). Call it before [`Dice::suspend_image`], which makes
    /// every row written so far durable.
    pub fn write_suspend_row<T: pagable::PagableSerialize>(
        &self,
        value: &T,
    ) -> anyhow::Result<Row> {
        let storage = self
            .pagable_storage
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("suspend needs pagable storage"))?;
        Ok(row(storage.write_row(value, &ArcSerCache::new())?))
    }

    /// Reads a row [`Dice::write_suspend_row`] wrote in a suspended process.
    pub async fn read_suspend_row<T: for<'de> pagable::PagableDeserialize<'de>>(
        &self,
        row: Row,
    ) -> anyhow::Result<T> {
        let storage = self
            .pagable_storage
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("resume needs pagable storage"))?;
        storage.verify_reads();
        storage.read_row(data_key(row)?).await
    }
}
