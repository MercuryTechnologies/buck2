/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Invalidation bodies and verdicts.
//!
//! A body names a node's leaf premises with their canonical values, and the digests `E` of the
//! bodies of its other premises. `E(n) = H(body(n))`, so a body is fetched by `E` and checked
//! against it, and a reader recomputes the same digest from its own leaf values: `A(n)`. The
//! verdict of a body is `valid` exactly when `A(n) == E(n)`, that is when every leaf under it has
//! the writer's value. Non-leaf premises are never named, only walked through, so a record can sit
//! above a node whose value carries a Starlark heap's identity.

use std::collections::HashMap;

use buck2_common::frontier::Canon;
use buck2_common::frontier::LeafId;
use buck2_common::frontier::hex;
use serde::Deserialize;
use serde::Serialize;

/// Bumped on any change to how bodies, records or the store are encoded.
pub const FORMAT: u32 = 1;

pub type Digest = String;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Body {
    /// The frontier version, in a record's root body only.
    pub version: Option<Digest>,
    /// Sorted, distinct.
    pub leaves: Vec<(LeafId, Canon)>,
    /// Sorted, distinct.
    pub children: Vec<Digest>,
}

fn field(h: &mut blake3::Hasher, s: &str) {
    h.update(&(s.len() as u64).to_le_bytes());
    h.update(s.as_bytes());
}

impl Body {
    pub fn new(
        version: Option<Digest>,
        mut leaves: Vec<(LeafId, Canon)>,
        mut children: Vec<Digest>,
    ) -> Self {
        leaves.sort();
        leaves.dedup();
        children.sort();
        children.dedup();
        Body {
            version,
            leaves,
            children,
        }
    }

    /// `E` when the leaf values are the body's own, `A` when they are a reader's.
    pub fn digest_with<'a>(
        version: Option<&str>,
        leaves: impl IntoIterator<Item = (&'a LeafId, &'a str)>,
        children: impl IntoIterator<Item = &'a str>,
    ) -> Digest {
        let mut h = blake3::Hasher::new();
        h.update(b"frontier-body");
        h.update(&FORMAT.to_le_bytes());
        match version {
            Some(v) => {
                h.update(&[1]);
                field(&mut h, v);
            }
            None => {
                h.update(&[0]);
            }
        }
        let leaves: Vec<_> = leaves.into_iter().collect();
        h.update(&(leaves.len() as u64).to_le_bytes());
        for (id, canon) in leaves {
            // A stable encoding of the id: its JSON, which serde writes field by field in
            // declaration order.
            field(&mut h, &serde_json::to_string(id).expect("leaf ids serialize"));
            field(&mut h, canon);
        }
        let children: Vec<_> = children.into_iter().collect();
        h.update(&(children.len() as u64).to_le_bytes());
        for c in children {
            field(&mut h, c);
        }
        hex(h.finalize().as_bytes())
    }

    pub fn digest(&self) -> Digest {
        Self::digest_with(
            self.version.as_deref(),
            self.leaves.iter().map(|(id, c)| (id, c.as_str())),
            self.children.iter().map(|c| c.as_str()),
        )
    }
}

/// Where a reader gets bodies from: the store, by `E`.
pub trait BodySource {
    fn body(&self, e: &str) -> anyhow::Result<Body>;
}

/// A reader's value of a leaf: `None` when this daemon cannot compute it.
pub trait LeafOracle {
    fn leaf(&self, id: &LeafId) -> Option<Canon>;
    fn version(&self) -> Digest;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    Valid,
    /// The first leaf (in body order, depth first) whose value differs, or that this daemon
    /// could not compute.
    Invalid { leaf: LeafId, unavailable: bool },
    /// The version differs.
    Version,
    /// A body did not hash to its name: the store is corrupt.
    Corrupt(Digest),
}

/// Computes `A` of every body under `e`, memoised in `actual`, and the first leaf whose value
/// differs.
pub fn verdict(
    root: &str,
    bodies: &dyn BodySource,
    oracle: &dyn LeafOracle,
    actual: &mut HashMap<Digest, (Digest, Option<(LeafId, bool)>)>,
) -> anyhow::Result<Verdict> {
    // Iterative post-order: invalidation trees are as deep as the graph.
    enum Frame {
        Enter(Digest),
        Exit(Digest, Body),
    }
    let mut stack = vec![Frame::Enter(root.to_owned())];
    let mut corrupt: Option<Digest> = None;
    while let Some(frame) = stack.pop() {
        match frame {
            Frame::Enter(e) => {
                if actual.contains_key(&e) {
                    continue;
                }
                let body = bodies.body(&e)?;
                if body.digest() != e {
                    corrupt = Some(e.clone());
                    actual.insert(e.clone(), (String::new(), None));
                    continue;
                }
                let children = body.children.clone();
                stack.push(Frame::Exit(e, body));
                for c in children.into_iter().rev() {
                    if !actual.contains_key(&c) {
                        stack.push(Frame::Enter(c));
                    }
                }
            }
            Frame::Exit(e, body) => {
                let mut first: Option<(LeafId, bool)> = None;
                let mut locals = Vec::with_capacity(body.leaves.len());
                for (id, expected) in &body.leaves {
                    let got = oracle.leaf(id);
                    if first.is_none() {
                        match &got {
                            None => first = Some((id.clone(), true)),
                            Some(v) if v != expected => first = Some((id.clone(), false)),
                            _ => {}
                        }
                    }
                    // An unavailable leaf takes a value no writer can have produced.
                    locals.push((id, got.unwrap_or_else(|| "unavailable".to_owned())));
                }
                let mut kids = Vec::with_capacity(body.children.len());
                for c in &body.children {
                    let (a, f) = actual.get(c).cloned().unwrap_or_default();
                    if first.is_none() {
                        first = f;
                    }
                    kids.push(a);
                }
                let version = body.version.as_ref().map(|_| oracle.version());
                let a = Body::digest_with(
                    version.as_deref(),
                    locals.iter().map(|(id, c)| (*id, c.as_str())),
                    kids.iter().map(|k| k.as_str()),
                );
                actual.insert(e, (a, first));
            }
        }
    }
    if let Some(c) = corrupt {
        return Ok(Verdict::Corrupt(c));
    }
    let (a, first) = actual.get(root).cloned().unwrap_or_default();
    if a == root {
        return Ok(Verdict::Valid);
    }
    let body = bodies.body(root)?;
    if let Some(v) = &body.version
        && *v != oracle.version()
    {
        return Ok(Verdict::Version);
    }
    Ok(match first {
        Some((leaf, unavailable)) => Verdict::Invalid { leaf, unavailable },
        // Every leaf matched but the digest did not: only a corrupt child explains it.
        None => Verdict::Corrupt(root.to_owned()),
    })
}
