/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! The primer's flattening of a daemon's graph into invalidation bodies and records, and the
//! shadow reader's check of records against a daemon's graph. Both work on [`Node`]s, which the
//! server builds from DICE's frontier graph, so that this logic is tested without a daemon.

use std::collections::BTreeMap;
use std::collections::HashMap;

use buck2_common::frontier::Canon;
use buck2_common::frontier::LeafId;
use buck2_common::frontier::hex;
use serde::Deserialize;
use serde::Serialize;

use crate::body::Body;
use crate::body::BodySource;
use crate::body::Digest;
use crate::body::FORMAT;
use crate::body::LeafOracle;
use crate::body::Verdict;
use crate::body::verdict;

/// What a node is to the walk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Role {
    /// A premise named by value.
    Leaf(LeafId),
    /// Nothing above it is shared.
    Barrier(String),
    /// A premise whose role the leaves take over.
    Skip,
    /// Walked through.
    Inner,
    /// A locally executed action: a leaf (by its value row) to every record above it, and walked
    /// through for its own record.
    LocalOut,
}

#[derive(Clone, Debug)]
pub struct Node {
    pub kind: String,
    pub key_id: Option<[u8; 32]>,
    pub value_row: Option<[u8; 16]>,
    pub deps: Vec<u32>,
    pub deps_complete: bool,
    pub role: Role,
}

/// A leaf's value on the writer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeafValue {
    Canon(Canon),
    /// A config value from outside the tracked files: named by property, never by value.
    External(String),
    Unavailable(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub format: u32,
    pub kind: String,
    pub key_id: String,
    pub value_row: String,
    /// `E` of the root body.
    pub root: Digest,
    /// For a `BuildKey`: `local` or `remote-or-cache`.
    pub exec_class: Option<String>,
    pub writer_commit: String,
}

impl Record {
    pub fn digest(&self) -> Digest {
        hex(blake3::hash(&serde_json::to_vec(self).expect("records serialize")).as_bytes())
    }
}

/// `H(format, V, kind, key id)`.
pub fn record_key(version: &str, kind: &str, key_id: &[u8; 32]) -> Digest {
    let mut h = blake3::Hasher::new();
    h.update(b"frontier-record-key");
    h.update(&FORMAT.to_le_bytes());
    h.update(&(version.len() as u64).to_le_bytes());
    h.update(version.as_bytes());
    h.update(&(kind.len() as u64).to_le_bytes());
    h.update(kind.as_bytes());
    h.update(key_id);
    hex(h.finalize().as_bytes())
}

/// What the primer produced.
#[derive(Default)]
pub struct Primed {
    /// Record key to record.
    pub records: BTreeMap<Digest, Record>,
    /// Every body a record's tree names, by `E`.
    pub bodies: HashMap<Digest, Body>,
    /// `(kind, key id, reason)` of every node of a covered kind that got no record.
    pub unshareable: Vec<(String, String, String)>,
}

/// Bodies of every non-leaf node, bottom-up, and records for the nodes whose kind `covered`
/// accepts.
pub fn prime(
    nodes: &[Node],
    leaves: &HashMap<LeafId, LeafValue>,
    version: &str,
    covered: &dyn Fn(&str) -> bool,
    exec_class: &dyn Fn(&Node) -> Option<String>,
    commit: &str,
) -> Primed {
    // Per node: Ok(E) or Err(reason).
    let mut status: Vec<Option<Result<Digest, String>>> = vec![None; nodes.len()];
    let mut bodies: HashMap<Digest, Body> = HashMap::new();
    let mut node_body: HashMap<usize, Body> = HashMap::new();

    let leaf_entry = |id: &LeafId| -> Result<(LeafId, Canon), String> {
        match leaves.get(id) {
            Some(LeafValue::Canon(c)) => Ok((id.clone(), c.clone())),
            Some(LeafValue::External(p)) => Err(format!("external config premise: {p}")),
            Some(LeafValue::Unavailable(why)) => Err(format!("leaf unavailable: {why}")),
            None => Err(format!("leaf unavailable: {}", id.kind())),
        }
    };

    for start in 0..nodes.len() {
        if status[start].is_some() || !matches!(nodes[start].role, Role::Inner | Role::LocalOut) {
            continue;
        }
        let mut stack: Vec<(usize, bool)> = vec![(start, false)];
        while let Some((i, expanded)) = stack.pop() {
            if status[i].is_some() {
                continue;
            }
            let n = &nodes[i];
            if !expanded {
                stack.push((i, true));
                for &d in &n.deps {
                    let d = d as usize;
                    if status[d].is_none() && matches!(nodes[d].role, Role::Inner) {
                        stack.push((d, false));
                    }
                }
                continue;
            }
            let result = (|| {
                if !n.deps_complete {
                    return Err("a premise is missing from the graph".to_owned());
                }
                let mut ls = Vec::new();
                let mut cs = Vec::new();
                for &d in &n.deps {
                    let dn = &nodes[d as usize];
                    match &dn.role {
                        Role::Leaf(id) => ls.push(leaf_entry(id)?),
                        Role::Barrier(r) => return Err(r.clone()),
                        Role::Skip => {}
                        Role::LocalOut => match (&dn.key_id, &dn.value_row) {
                            (Some(k), Some(v)) => ls.push((
                                LeafId::LocalOut {
                                    key_type: dn.kind.clone(),
                                    key_id: hex(k),
                                },
                                hex(v),
                            )),
                            _ => return Err("local action without rows".to_owned()),
                        },
                        Role::Inner => match status[d as usize].as_ref() {
                            Some(Ok(e)) => cs.push(e.clone()),
                            Some(Err(r)) => return Err(r.clone()),
                            None => return Err("cycle".to_owned()),
                        },
                    }
                }
                let body = Body::new(None, ls, cs);
                let e = body.digest();
                node_body.insert(i, body.clone());
                bodies.insert(e.clone(), body);
                Ok(e)
            })();
            status[i] = Some(result);
        }
    }

    let mut out = Primed::default();
    for (i, n) in nodes.iter().enumerate() {
        if !covered(&n.kind) || !matches!(n.role, Role::Inner | Role::LocalOut) {
            continue;
        }
        let key_hex = n.key_id.map(|k| hex(&k)).unwrap_or_default();
        let reason = match (&status[i], &n.key_id, &n.value_row) {
            (Some(Ok(_)), Some(key_id), Some(value_row)) => {
                let body = &node_body[&i];
                let root = Body::new(
                    Some(version.to_owned()),
                    body.leaves.clone(),
                    body.children.clone(),
                );
                let root_e = root.digest();
                out.bodies.insert(root_e.clone(), root);
                let record = Record {
                    format: FORMAT,
                    kind: n.kind.clone(),
                    key_id: key_hex,
                    value_row: hex(value_row),
                    root: root_e,
                    exec_class: exec_class(n),
                    writer_commit: commit.to_owned(),
                };
                out.records
                    .insert(record_key(version, &n.kind, key_id), record);
                continue;
            }
            (Some(Err(r)), _, _) => r.clone(),
            (_, None, _) => "key row did not round-trip".to_owned(),
            (_, _, None) => "value has no row".to_owned(),
            (None, _, _) => "not walked".to_owned(),
        };
        out.unshareable.push((n.kind.clone(), key_hex, reason));
    }
    // Only the bodies a record's tree reaches are kept.
    let mut keep: HashMap<Digest, Body> = HashMap::new();
    let mut todo: Vec<Digest> = out.records.values().map(|r| r.root.clone()).collect();
    while let Some(e) = todo.pop() {
        if keep.contains_key(&e) {
            continue;
        }
        let Some(b) = out.bodies.get(&e).or_else(|| bodies.get(&e)) else {
            continue;
        };
        todo.extend(b.children.iter().cloned());
        keep.insert(e, b.clone());
    }
    out.bodies = keep;
    out
}

/// One covered node's outcome in a shadow pass.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Valid, and the value row is the record's.
    Hit,
    /// Valid, but the value row differs: a missing premise or local nondeterminism. The soundness
    /// alarm.
    ValidButDifferent,
    /// A leaf differs (by kind).
    Invalid(String),
    /// A leaf this daemon could not compute.
    Unavailable(String),
    /// The version differs.
    Version,
    Miss,
    /// The key could not be named.
    NoKey,
    Corrupt,
}

impl Outcome {
    pub fn label(&self) -> String {
        match self {
            Outcome::Hit => "hit".to_owned(),
            Outcome::ValidButDifferent => "valid but different".to_owned(),
            Outcome::Invalid(k) => format!("invalid: {k}"),
            Outcome::Unavailable(k) => format!("unavailable: {k}"),
            Outcome::Version => "version".to_owned(),
            Outcome::Miss => "miss".to_owned(),
            Outcome::NoKey => "no key".to_owned(),
            Outcome::Corrupt => "corrupt".to_owned(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShadowEntry {
    pub kind: String,
    pub key_id: String,
    pub outcome: Outcome,
    /// The first differing leaf, for an invalid verdict.
    pub leaf: Option<LeafId>,
}

/// Where the shadow pass finds records: the store's pointer for the base commit.
pub trait RecordSource: BodySource {
    fn record(&self, record_key: &str) -> anyhow::Result<Option<Record>>;
}

/// Every leaf the bodies of the records for `nodes` name: what the server computes before
/// [`shadow`], for those its own graph did not hold.
pub fn needed_leaves(
    nodes: &[Node],
    version: &str,
    covered: &dyn Fn(&str) -> bool,
    store: &dyn RecordSource,
) -> anyhow::Result<Vec<LeafId>> {
    let mut seen: HashMap<Digest, ()> = HashMap::new();
    let mut out: Vec<LeafId> = Vec::new();
    let mut todo: Vec<Digest> = Vec::new();
    for n in nodes {
        if !covered(&n.kind) {
            continue;
        }
        let Some(k) = &n.key_id else { continue };
        if let Some(r) = store.record(&record_key(version, &n.kind, k))? {
            todo.push(r.root);
        }
    }
    while let Some(e) = todo.pop() {
        if seen.insert(e.clone(), ()).is_some() {
            continue;
        }
        let b = store.body(&e)?;
        out.extend(b.leaves.into_iter().map(|(id, _)| id));
        todo.extend(b.children);
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// Checks the record of every covered node against this daemon's leaf values, and compares the
/// value. Installs nothing.
pub fn shadow(
    nodes: &[Node],
    oracle: &dyn LeafOracle,
    covered: &dyn Fn(&str) -> bool,
    store: &dyn RecordSource,
) -> anyhow::Result<Vec<ShadowEntry>> {
    let version = oracle.version();
    let mut memo = HashMap::new();
    let mut out = Vec::new();
    for n in nodes {
        if !covered(&n.kind) || !matches!(n.role, Role::Inner | Role::LocalOut) {
            continue;
        }
        let key_hex = n.key_id.map(|k| hex(&k)).unwrap_or_default();
        let entry = |outcome, leaf| ShadowEntry {
            kind: n.kind.clone(),
            key_id: key_hex.clone(),
            outcome,
            leaf,
        };
        let Some(k) = &n.key_id else {
            out.push(entry(Outcome::NoKey, None));
            continue;
        };
        let Some(record) = store.record(&record_key(&version, &n.kind, k))? else {
            out.push(entry(Outcome::Miss, None));
            continue;
        };
        let e = match verdict(&record.root, store, oracle, &mut memo)? {
            Verdict::Valid => {
                let same = n.value_row.map(|v| hex(&v)) == Some(record.value_row.clone());
                entry(
                    if same {
                        Outcome::Hit
                    } else {
                        Outcome::ValidButDifferent
                    },
                    None,
                )
            }
            Verdict::Invalid { leaf, unavailable } => entry(
                if unavailable {
                    Outcome::Unavailable(leaf.kind().to_owned())
                } else {
                    Outcome::Invalid(leaf.kind().to_owned())
                },
                Some(leaf),
            ),
            Verdict::Version => entry(Outcome::Version, None),
            Verdict::Corrupt(_) => entry(Outcome::Corrupt, None),
        };
        out.push(e);
    }
    Ok(out)
}

/// Counts by kind and outcome label.
pub fn summarize(entries: &[ShadowEntry]) -> BTreeMap<String, BTreeMap<String, usize>> {
    let mut m: BTreeMap<String, BTreeMap<String, usize>> = BTreeMap::new();
    for e in entries {
        *m.entry(e.kind.clone())
            .or_default()
            .entry(e.outcome.label())
            .or_default() += 1;
    }
    m
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    struct Mem {
        bodies: HashMap<Digest, Body>,
        records: BTreeMap<Digest, Record>,
    }

    impl BodySource for Mem {
        fn body(&self, e: &str) -> anyhow::Result<Body> {
            self.bodies
                .get(e)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("no body {e}"))
        }
    }

    impl RecordSource for Mem {
        fn record(&self, k: &str) -> anyhow::Result<Option<Record>> {
            Ok(self.records.get(k).cloned())
        }
    }

    struct Oracle {
        leaves: RefCell<HashMap<LeafId, Canon>>,
        version: String,
    }

    impl LeafOracle for Oracle {
        fn leaf(&self, id: &LeafId) -> Option<Canon> {
            self.leaves.borrow().get(id).cloned()
        }
        fn version(&self) -> Digest {
            self.version.clone()
        }
    }

    fn path(p: &str) -> LeafId {
        LeafId::Path {
            cell: "root".to_owned(),
            path: p.to_owned(),
        }
    }

    fn listing(p: &str) -> LeafId {
        LeafId::Listing {
            cell: "root".to_owned(),
            path: p.to_owned(),
            check_ignores: true,
        }
    }

    fn config(s: &str, p: &str) -> LeafId {
        LeafId::Config {
            cell: "root".to_owned(),
            section: s.to_owned(),
            property: p.to_owned(),
        }
    }

    fn node(kind: &str, id: u8, deps: &[u32], role: Role) -> Node {
        Node {
            kind: kind.to_owned(),
            key_id: Some([id; 32]),
            value_row: Some([id; 16]),
            deps: deps.to_vec(),
            deps_complete: true,
            role,
        }
    }

    /// A graph shaped like the fixture's: a configured node reads an interpreter result (whose
    /// value carries a heap nonce and is walked through), which reads a BUILD file, a glob
    /// listing and a config property; a second node reads the first's package only.
    fn graph() -> (Vec<Node>, HashMap<LeafId, LeafValue>) {
        let nodes = vec![
            node("PathMetadataKey", 0, &[], Role::Leaf(path("pkg/BUCK"))),
            node("ReadDirKey", 1, &[], Role::Leaf(listing("pkg/srcs"))),
            node(
                "LegacyBuckConfigPropertyProjectionKey",
                2,
                &[],
                Role::Leaf(config("build", "x")),
            ),
            node("InterpreterResultsKey", 3, &[0, 1, 2], Role::Inner),
            node("ConfiguredTargetNodeKey", 4, &[3], Role::Inner),
            node("PathMetadataKey", 5, &[], Role::Leaf(path("other/BUCK"))),
            node("ConfiguredTargetNodeKey", 6, &[5], Role::Inner),
        ];
        let mut leaves = HashMap::new();
        leaves.insert(path("pkg/BUCK"), LeafValue::Canon("f1".to_owned()));
        leaves.insert(listing("pkg/srcs"), LeafValue::Canon("l1".to_owned()));
        leaves.insert(config("build", "x"), LeafValue::Canon("c1".to_owned()));
        leaves.insert(path("other/BUCK"), LeafValue::Canon("o1".to_owned()));
        (nodes, leaves)
    }

    fn covered(k: &str) -> bool {
        k == "ConfiguredTargetNodeKey"
    }

    fn primed() -> (Vec<Node>, Mem, Oracle) {
        let (nodes, leaves) = graph();
        let p = prime(&nodes, &leaves, "V", &covered, &|_| None, "c0");
        assert_eq!(p.records.len(), 2, "{:?}", p.unshareable);
        let oracle = Oracle {
            leaves: RefCell::new(
                leaves
                    .into_iter()
                    .map(|(k, v)| match v {
                        LeafValue::Canon(c) => (k, c),
                        _ => unreachable!(),
                    })
                    .collect(),
            ),
            version: "V".to_owned(),
        };
        (
            nodes,
            Mem {
                bodies: p.bodies,
                records: p.records,
            },
            oracle,
        )
    }

    fn outcomes(nodes: &[Node], o: &Oracle, m: &Mem) -> Vec<Outcome> {
        shadow(nodes, o, &covered, m)
            .unwrap()
            .into_iter()
            .map(|e| e.outcome)
            .collect()
    }

    #[test]
    fn unchanged_tree_hits() {
        let (nodes, m, o) = primed();
        assert_eq!(outcomes(&nodes, &o, &m), vec![Outcome::Hit, Outcome::Hit]);
    }

    /// Mode change, type change and content change all reach the verdict through the path leaf,
    /// whose canonical value covers them (tested in `buck2_common::frontier`); here the walk must
    /// name the leaf, leave the unrelated record valid, and return to valid on revert.
    #[test]
    fn changed_path_leaf_invalidates_only_records_above_it_and_reverts() {
        let (nodes, m, o) = primed();
        let before = o.leaves.borrow()[&path("pkg/BUCK")].clone();
        o.leaves
            .borrow_mut()
            .insert(path("pkg/BUCK"), "f1-chmod+x".to_owned());
        let entries = shadow(&nodes, &o, &covered, &m).unwrap();
        assert_eq!(entries[0].outcome, Outcome::Invalid("path".to_owned()));
        assert_eq!(entries[0].leaf, Some(path("pkg/BUCK")));
        assert_eq!(entries[1].outcome, Outcome::Hit);
        o.leaves.borrow_mut().insert(path("pkg/BUCK"), before);
        assert_eq!(outcomes(&nodes, &o, &m), vec![Outcome::Hit, Outcome::Hit]);
    }

    #[test]
    fn untracked_file_in_globbed_directory_invalidates() {
        let (nodes, m, o) = primed();
        o.leaves
            .borrow_mut()
            .insert(listing("pkg/srcs"), "l1+untracked".to_owned());
        let entries = shadow(&nodes, &o, &covered, &m).unwrap();
        assert_eq!(entries[0].outcome, Outcome::Invalid("listing".to_owned()));
        assert_eq!(entries[1].outcome, Outcome::Hit);
    }

    #[test]
    fn config_projection_change_invalidates() {
        let (nodes, m, o) = primed();
        o.leaves
            .borrow_mut()
            .insert(config("build", "x"), "c2".to_owned());
        let entries = shadow(&nodes, &o, &covered, &m).unwrap();
        assert_eq!(entries[0].outcome, Outcome::Invalid("config".to_owned()));
        assert_eq!(entries[0].leaf, Some(config("build", "x")));
    }

    #[test]
    fn unavailable_leaf_is_not_valid() {
        let (nodes, m, o) = primed();
        o.leaves.borrow_mut().remove(&listing("pkg/srcs"));
        let entries = shadow(&nodes, &o, &covered, &m).unwrap();
        assert_eq!(
            entries[0].outcome,
            Outcome::Unavailable("listing".to_owned())
        );
    }

    #[test]
    fn version_change_invalidates_every_record() {
        let (nodes, m, mut o) = primed();
        // The record key includes V, so another version finds no record at all.
        o.version = "V2".to_owned();
        assert_eq!(outcomes(&nodes, &o, &m), vec![Outcome::Miss, Outcome::Miss]);
    }

    #[test]
    fn same_leaves_different_value_is_the_alarm() {
        let (mut nodes, m, o) = primed();
        nodes[4].value_row = Some([99; 16]);
        assert_eq!(
            outcomes(&nodes, &o, &m),
            vec![Outcome::ValidButDifferent, Outcome::Hit]
        );
    }

    #[test]
    fn corrupt_body_is_detected() {
        let (nodes, mut m, o) = primed();
        let root = m.records.values().next().unwrap().root.clone();
        let b = m.bodies.get_mut(&root).unwrap();
        // A substituted body: it no longer hashes to the name it is stored under.
        b.version = Some("tampered".to_owned());
        let got = outcomes(&nodes, &o, &m);
        assert!(got.contains(&Outcome::Corrupt), "{got:?}");
        assert!(got.contains(&Outcome::Hit), "{got:?}");
    }

    #[test]
    fn barrier_and_external_config_make_records_unshareable() {
        let (mut nodes, mut leaves) = graph();
        leaves.insert(
            config("build", "x"),
            LeafValue::External("build.x".to_owned()),
        );
        nodes.push(node(
            "LegacyBuckConfigForCellKey",
            7,
            &[],
            Role::Barrier("whole-config premise via LegacyBuckConfigForCellKey".to_owned()),
        ));
        nodes[6].deps.push(7);
        let p = prime(&nodes, &leaves, "V", &covered, &|_| None, "c0");
        assert!(p.records.is_empty());
        let reasons: Vec<_> = p.unshareable.iter().map(|u| u.2.clone()).collect();
        assert_eq!(
            reasons,
            vec![
                "external config premise: build.x".to_owned(),
                "whole-config premise via LegacyBuckConfigForCellKey".to_owned()
            ]
        );
        // No value of the external property reaches any body.
        for b in p.bodies.values() {
            assert!(b.leaves.iter().all(|(id, _)| *id != config("build", "x")));
        }
    }

    #[test]
    fn local_action_is_a_leaf_above_it() {
        let nodes = vec![
            node("PathMetadataKey", 0, &[], Role::Leaf(path("src.txt"))),
            node("BuildKey", 1, &[0], Role::LocalOut),
            node("BuildKey", 2, &[1], Role::LocalOut),
        ];
        let mut leaves = HashMap::new();
        leaves.insert(path("src.txt"), LeafValue::Canon("s".to_owned()));
        let cov = |k: &str| k == "BuildKey";
        let p = prime(&nodes, &leaves, "V", &cov, &|_| Some("local".to_owned()), "c0");
        assert_eq!(p.records.len(), 2);
        let upper = p
            .records
            .values()
            .find(|r| r.key_id == hex(&[2; 32]))
            .unwrap();
        let root = &p.bodies[&upper.root];
        assert_eq!(root.leaves.len(), 1);
        assert!(matches!(root.leaves[0].0, LeafId::LocalOut { .. }));
        assert!(root.children.is_empty());
    }
}
