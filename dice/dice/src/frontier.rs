/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! The graph a frontier primer or shadow reader walks: every key valid at one version, named by
//! the digest of its row, with the row of its value and the keys its certificate read.
//!
//! DICE knows nothing of what a leaf is. The caller's [`FrontierInspect`] is handed the typed key
//! and value of each key type it asks for, and returns whatever it wants to remember about the
//! node; DICE stores it beside the node unread.
//!
//! Unlike [`Dice::suspend_image`], this pages nothing out and leaves DICE usable: a value only
//! memory holds is written as a row, as a suspend would, and stays resident.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::Arc as StdArc;

use dupe::Dupe;
use pagable::DataKey;
use pagable::storage::traits::ArcSerCache;

use crate::HashMap;
use crate::core::graph::HeadSource;
use crate::core::graph::HeadValue;
use crate::dice::Dice;
use crate::key::DiceKey;
use crate::key::DiceKeyErased;
use crate::value::DiceValidValue;

/// What a frontier walk wants to know about some key types.
pub trait FrontierInspect: Send + Sync {
    /// Whether [`FrontierInspect::inspect`] should see keys of this type: `Some(true)` with
    /// their value, which is read back from the paging store if it was paged out, `Some(false)`
    /// with the key alone.
    fn wants(&self, key_type: &'static str) -> Option<bool>;

    /// `key` is the key (for a projection, the projection key alone), `base` a projection's base
    /// key, `value` the key's value when it could be read. The result is stored unread.
    fn inspect(
        &self,
        key: &dyn Any,
        base: Option<&dyn Any>,
        value: Option<&dyn Any>,
    ) -> Option<Box<dyn Any + Send + Sync>>;
}

/// How a node holds its revision at the dumped version.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FrontierSource {
    /// Set from outside: an `InjectedKey`, or a key `changed_to` asserted.
    Asserted { injected: bool },
    /// Computed, with a certificate whose premises are `deps`.
    Certified,
}

/// One node of [`Dice::frontier_graph`].
pub struct FrontierNode {
    pub key_type: &'static str,
    /// blake3 of the key's row (for a projection, of its base's id and its own row). `None` if
    /// the key's serializer declined or failed it: such a node can be named by no record.
    pub key_id: Option<[u8; 32]>,
    /// The digest of the row holding the value, `None` if the value's serializer declined it.
    pub value_row: Option<[u8; 16]>,
    pub source: FrontierSource,
    /// Indices in [`FrontierGraph::nodes`] of the premises of the node's certificate.
    pub deps: Vec<u32>,
    /// Whether every premise is in the graph. A certificate's premises are valid at its version,
    /// so this is false only for a premise whose key DICE could not name.
    pub deps_complete: bool,
    /// Whether the key's equality is `AlwaysUnequal`.
    pub always_unequal: bool,
    /// What [`FrontierInspect::inspect`] returned.
    pub info: Option<Box<dyn Any + Send + Sync>>,
}

pub struct FrontierGraph {
    pub nodes: Vec<FrontierNode>,
}

fn row_bytes(key: DataKey) -> [u8; 16] {
    let v = key.get();
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&(v as u64).to_le_bytes());
    out[8..].copy_from_slice(&((v >> 64) as u64).to_le_bytes());
    out
}

impl Dice {
    /// Every key valid at `at`: asserted keys and certified ones (candidates, which are not
    /// valid, are left out). The caller must hold DICE idle, as for a suspend; afterwards DICE
    /// may compute again.
    pub async fn frontier_graph(
        self: &StdArc<Self>,
        at: dice_core::Version,
        inspect: &dyn FrontierInspect,
    ) -> anyhow::Result<FrontierGraph> {
        let storage = self
            .pagable_storage
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("the frontier needs pagable storage"))?
            .dupe();
        let head = self.state_handle.dump_head(at).await;
        let finished = ArcSerCache::new();

        struct Entry {
            key: DiceKey,
            source: HeadSource,
            value: HeadValue,
            injected: bool,
        }
        let mut entries: Vec<Entry> = Vec::new();
        for e in head {
            if matches!(e.source, HeadSource::Candidate { .. }) {
                continue;
            }
            entries.push(Entry {
                key: e.key,
                source: e.source,
                value: e.value,
                injected: e.injected,
            });
        }
        entries.sort_by_key(|e| e.key.index);
        let index: HashMap<DiceKey, u32> = entries
            .iter()
            .enumerate()
            .map(|(i, e)| (e.key, u32::try_from(i).expect("fewer than 2^32 keys")))
            .collect();

        // Key rows: a projection's id covers its base's, so that two projections of different
        // bases never share one.
        let mut key_rows: HashMap<DiceKey, Option<[u8; 16]>> = HashMap::default();
        let key_row = |key: DiceKey, key_rows: &mut HashMap<DiceKey, Option<[u8; 16]>>| {
            if let Some(r) = key_rows.get(&key) {
                return *r;
            }
            let erased = self.key_index.get(key);
            let written = std::panic::catch_unwind(AssertUnwindSafe(|| {
                storage.write_key_row(erased, &finished)
            }));
            let r = match written {
                Ok(Ok((row, _))) => Some(row_bytes(row)),
                _ => None,
            };
            key_rows.insert(key, r);
            r
        };
        let key_id = |key: DiceKey, key_rows: &mut HashMap<DiceKey, Option<[u8; 16]>>| {
            let own = key_row(key, key_rows)?;
            let mut h = blake3::Hasher::new();
            match self.key_index.get(key) {
                DiceKeyErased::Key(_) => {
                    h.update(b"key");
                }
                DiceKeyErased::Projection(p) => {
                    let base = key_row(p.base(), key_rows)?;
                    h.update(b"projection");
                    h.update(&base);
                }
            }
            h.update(&own);
            Some(*h.finalize().as_bytes())
        };

        let mut nodes = Vec::with_capacity(entries.len());
        for e in &entries {
            let erased = self.key_index.get(e.key);
            let key_type = erased.key_type_name();
            let (value_row, resident): (Option<[u8; 16]>, Option<DiceValidValue>) = match &e.value
            {
                HeadValue::Row(row) => (Some(row_bytes(*row)), None),
                HeadValue::Resident(value) => {
                    let written = std::panic::catch_unwind(AssertUnwindSafe(|| {
                        storage.write_value_row(erased, value, &finished)
                    }));
                    let row = match written {
                        Ok(Ok(Some(row))) => Some(row_bytes(row)),
                        _ => None,
                    };
                    (row, Some(value.dupe()))
                }
            };
            let info = if let Some(with_value) = inspect.wants(key_type) {
                let value = match (with_value, resident, &e.value) {
                    (false, _, _) => None,
                    (true, Some(v), _) => Some(v),
                    (true, None, HeadValue::Row(row)) => storage.hydrate(erased, *row).await.ok(),
                    (true, None, HeadValue::Resident(v)) => Some(v.dupe()),
                };
                let base_key = match erased {
                    DiceKeyErased::Key(_) => None,
                    DiceKeyErased::Projection(p) => Some(self.key_index.get(p.base())),
                };
                let key_any: &dyn Any = erased.as_any();
                let base_any: Option<&dyn Any> = base_key.map(|b| b.as_any());
                let value_any: Option<&dyn Any> =
                    value.as_ref().map(|v| v.as_dyn().value_as_any());
                inspect.inspect(key_any, base_any, value_any)
            } else {
                None
            };
            let (source, deps, deps_complete) = match &e.source {
                HeadSource::Asserted { .. } => (
                    FrontierSource::Asserted {
                        injected: e.injected,
                    },
                    Vec::new(),
                    true,
                ),
                HeadSource::Certified { cert } => {
                    let mut deps = Vec::new();
                    let mut complete = true;
                    for premise in cert.deps.iter_keys() {
                        match index.get(&premise) {
                            Some(i) => deps.push(*i),
                            None => complete = false,
                        }
                    }
                    deps.sort_unstable();
                    deps.dedup();
                    (FrontierSource::Certified, deps, complete)
                }
                HeadSource::Candidate { .. } => unreachable!("candidates are left out above"),
            };
            nodes.push(FrontierNode {
                key_type,
                key_id: key_id(e.key, &mut key_rows),
                value_row,
                source,
                deps,
                deps_complete,
                always_unequal: erased.equality_is_always_unequal(),
                info,
            });
        }
        storage.flush()?;
        storage.bind_written(&finished);
        Ok(FrontierGraph { nodes })
    }
}
