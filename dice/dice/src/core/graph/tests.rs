/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! The value layer: interning (`docs/incrementality.md` Appendix A), retention, and the page-out
//! lifecycle. Every mutating call re-checks the consistency of values, index and core.

use allocative::Allocative;
use async_trait::async_trait;
use derive_more::Display;
use dice_core::BranchId;
use dice_futures::cancellation::CancellationContext;
use dupe::Dupe;
use pagable::DataKey;
use pagable::Pagable;
use pagable::pagable_typetag;

use crate::DiceKeyDyn;
use crate::api::computations::DiceComputations;
use crate::api::key::InvalidationSourcePriority;
use crate::api::key::Key;
use crate::api::key::NoValueSerialize;
use crate::api::key::ValueSerialize;
use crate::api::storage_type::StorageType;
use crate::core::graph::ComputedValueUpdate;
use crate::core::graph::RestoreCounts;
use crate::core::graph::RestoredAssert;
use crate::core::graph::RestoredCert;
use crate::core::graph::VersionedGraph;
use crate::core::graph::revision::EpsilonToken;
use crate::core::graph::types::Candidate;
use crate::core::graph::types::VersionedGraphKey;
use crate::core::graph::types::VersionedGraphResult;
use crate::deps::graph::DepEdge;
use crate::deps::graph::SeriesParallelDeps;
use crate::dice::PagableNodeCounts;
use crate::key::DiceKey;
use crate::storage::EvictionPolicy;
use crate::storage::PageOutPolicy;
use crate::updater::ChangeType;
use crate::value::DiceKeyValue;
use crate::value::DiceValidValue;
use crate::value::MaybeResident;
use crate::value::MaybeResidentComputedValue;
use crate::value::TrackedInvalidationPaths;
use crate::versions::VersionNumber;

#[derive(Allocative, Clone, Dupe, Debug, Display, PartialEq, Eq, Hash, Pagable)]
#[pagable_typetag(DiceKeyDyn)]
struct K;

#[async_trait]
impl Key for K {
    type Value = usize;

    async fn compute(
        &self,
        _ctx: &mut DiceComputations,
        _cancellations: &CancellationContext,
    ) -> Self::Value {
        unimplemented!("test")
    }

    fn equality_behavior() -> crate::EqualityBehavior<Self::Value> {
        crate::EqualityBehavior::Compare(|x, y| x == y)
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        NoValueSerialize::<Self::Value>::new()
    }
}

#[derive(Allocative, Clone, Dupe, Debug, Display, PartialEq, Eq, Hash, Pagable)]
#[pagable_typetag(DiceKeyDyn)]
struct NoEquality;

#[async_trait]
impl Key for NoEquality {
    type Value = usize;

    async fn compute(
        &self,
        _ctx: &mut DiceComputations,
        _cancellations: &CancellationContext,
    ) -> Self::Value {
        unimplemented!("test")
    }

    fn equality_behavior() -> crate::EqualityBehavior<Self::Value> {
        crate::EqualityBehavior::Compare(|_, _| panic!("equality must not be consulted"))
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        NoValueSerialize::<Self::Value>::new()
    }
}

fn value(v: usize) -> DiceValidValue {
    DiceValidValue::testing_new(DiceKeyValue::<K>::new(v))
}

fn key(i: u32) -> DiceKey {
    DiceKey { index: i }
}

/// A graph with one root, holding the injected key `key(0)` at value 100 (version 2), paging
/// each key out once, the semantics the tests here are written against.
fn with_leaf() -> (VersionedGraph, BranchId) {
    with_leaf_under(PageOutPolicy::OncePerKey)
}

/// [`with_leaf`] paging under `policy`.
fn with_leaf_under(policy: PageOutPolicy) -> (VersionedGraph, BranchId) {
    let mut graph = VersionedGraph::new();
    graph.set_page_out_policy(policy);
    let root = graph.new_root();
    inject(&mut graph, root, key(0), 100);
    (graph, root)
}

fn inject(graph: &mut VersionedGraph, root: BranchId, k: DiceKey, v: usize) -> VersionNumber {
    let version = graph.commit(
        root,
        [(
            k,
            ChangeType::UpdateValue(value(v), StorageType::Injected),
            InvalidationSourcePriority::Normal,
        )],
    );
    graph.assert_consistent();
    version
}

fn dirty(graph: &mut VersionedGraph, root: BranchId, k: DiceKey) -> VersionNumber {
    let version = graph.commit(
        root,
        [(
            k,
            ChangeType::Invalidate,
            InvalidationSourcePriority::Normal,
        )],
    );
    graph.assert_consistent();
    version
}

/// The deps a compute of a key over `key(0)` records at `v`.
fn deps_on_leaf(graph: &VersionedGraph, v: VersionNumber) -> SeriesParallelDeps {
    let revision = graph
        .get(VersionedGraphKey::new(v, key(0)))
        .unpack_match()
        .expect("the leaf is injected")
        .revision()
        .expect("an injected value is valid");
    SeriesParallelDeps::serial_from_edges(vec![DepEdge::new(key(0), revision)])
}

/// A computed write of `value` for `k`, stamped as a transaction at `v` would stamp it.
fn compute(
    graph: &mut VersionedGraph,
    k: DiceKey,
    v: VersionNumber,
    value: DiceValidValue,
) -> MaybeResidentComputedValue {
    let deps = deps_on_leaf(graph, v);
    let epsilon = graph.core.epsilon(k, v);
    let out = graph.update_computed(
        VersionedGraphKey::new(v, k),
        ComputedValueUpdate {
            value,
            deps,
            epsilon,
        },
        TrackedInvalidationPaths::clean(),
    );
    graph.assert_consistent();
    out.into_computed()
}

fn revalidate(
    graph: &mut VersionedGraph,
    k: DiceKey,
    v: VersionNumber,
    candidate: Candidate,
) -> MaybeResidentComputedValue {
    let out = graph.revalidate(
        VersionedGraphKey::new(v, k),
        candidate,
        TrackedInvalidationPaths::clean(),
    );
    graph.assert_consistent();
    out
}

fn candidate_at(graph: &VersionedGraph, k: DiceKey, v: VersionNumber) -> Candidate {
    match graph.get(VersionedGraphKey::new(v, k)) {
        VersionedGraphResult::Unknown {
            candidate: Some(candidate),
            ..
        } => candidate,
        other => panic!("expected an Unknown with a candidate, got {other:?}"),
    }
}

fn page_out(graph: &mut VersionedGraph, k: DiceKey, data_key: u128) {
    page_out_under(graph, k, data_key, EvictionPolicy::OnIdle);
}

/// [`page_out`] committing under `policy`.
fn page_out_under(graph: &mut VersionedGraph, k: DiceKey, data_key: u128, policy: EvictionPolicy) {
    let to_evict: Vec<_> = graph
        .keys_to_page_out()
        .into_iter()
        .filter(|(candidate, _)| *candidate == k)
        .map(|(k, v)| {
            (
                k,
                crate::value::PageOutResult {
                    serialized_value: v,
                    data_key: pagable::DataKey::testing_new(data_key),
                },
            )
        })
        .collect();
    assert!(!to_evict.is_empty(), "{k:?} has no page-out candidate");
    graph.commit_page_out(to_evict, policy);
    graph.assert_consistent();
}

/// A write that is a trace of the same circumstances as the stored certificate reuses the stored
/// value and revision without consulting `Key::equality`.
#[test]
fn identical_certificate_reuses_stored_value_without_equality() {
    let (mut graph, root) = with_leaf();
    let v2 = graph.head(root);
    let first_value = DiceValidValue::testing_new(DiceKeyValue::<NoEquality>::new(1));
    let first = compute(&mut graph, key(1), v2, first_value.dupe());
    let second_value = DiceValidValue::testing_new(DiceKeyValue::<NoEquality>::new(2));
    let second = compute(&mut graph, key(1), v2, second_value);
    assert_eq!(second.revision(), first.revision());
    assert!(second.testing_resident_value().instance_equal(&first_value));
}

/// The same over a stored value that is paged out: the incoming value takes its place.
#[test]
fn identical_certificate_over_paged_out_value_makes_it_resident() {
    let (mut graph, root) = with_leaf();
    let v2 = graph.head(root);
    let first = compute(&mut graph, key(1), v2, value(7));
    page_out(&mut graph, key(1), 0x1234);
    assert!(
        graph
            .get(VersionedGraphKey::new(v2, key(1)))
            .unpack_match()
            .unwrap()
            .paged_out_data_key()
            .is_some()
    );
    let recomputed = value(7);
    let second = compute(&mut graph, key(1), v2, recomputed.dupe());
    assert_eq!(second.revision(), first.revision());
    assert!(second.testing_resident_value().instance_equal(&recomputed));
    assert!(
        graph
            .get(VersionedGraphKey::new(v2, key(1)))
            .unpack_match()
            .unwrap()
            .testing_resident_value()
            .instance_equal(&recomputed)
    );
    assert_eq!(graph.pagable_node_counts().paged_out, 0);
}

/// Recomputing to an equal value under different deps re-finds the revision (Appendix A step 2).
#[test]
fn recompute_to_equal_value_reuses_revision() {
    let (mut graph, root) = with_leaf();
    let v2 = graph.head(root);
    let first = compute(&mut graph, key(1), v2, value(42));
    let v3 = inject(&mut graph, root, key(0), 200);
    let again = value(42);
    let second = compute(&mut graph, key(1), v3, again.dupe());
    assert_eq!(second.revision(), first.revision());
    assert!(
        !second.testing_resident_value().instance_equal(&again),
        "the stored instance is canonical"
    );
}

#[test]
fn recompute_to_distinct_value_mints_fresh_revision() {
    let (mut graph, root) = with_leaf();
    let v2 = graph.head(root);
    let first = compute(&mut graph, key(1), v2, value(1));
    let v3 = inject(&mut graph, root, key(0), 200);
    let second = compute(&mut graph, key(1), v3, value(2));
    assert_ne!(first.revision(), second.revision());
    // The old value is no longer named by any claim and is dropped.
    assert_eq!(graph.pagable_node_counts().resident, 1);
}

/// A paged-out value cannot be compared, so a recompute over it mints; over-distinguishing is
/// sound and only costs reuse.
#[test]
fn recompute_over_paged_out_value_mints_fresh_revision() {
    let (mut graph, root) = with_leaf();
    let v2 = graph.head(root);
    let first = compute(&mut graph, key(1), v2, value(7));
    page_out(&mut graph, key(1), 0x1234);
    let v3 = inject(&mut graph, root, key(0), 200);
    let second = compute(&mut graph, key(1), v3, value(7));
    assert_ne!(first.revision(), second.revision());
}

/// Injected values are all retained, so a key returned to an earlier value gets that value's
/// revision back, and an injection of the current value is not a change.
#[test]
fn injected_values_refind_their_revisions() {
    let mut graph = VersionedGraph::new();
    let root = graph.new_root();
    let revision_at = |graph: &VersionedGraph, v| {
        graph
            .get(VersionedGraphKey::new(v, key(0)))
            .unpack_match()
            .unwrap()
            .revision()
    };
    let v2 = inject(&mut graph, root, key(0), 42);
    let v3 = inject(&mut graph, root, key(0), 43);
    let v4 = inject(&mut graph, root, key(0), 42);
    assert_eq!(v2, VersionNumber::testing_new(2));
    assert_eq!(v4, VersionNumber::testing_new(4));
    assert_ne!(revision_at(&graph, v2), revision_at(&graph, v3));
    assert_eq!(revision_at(&graph, v2), revision_at(&graph, v4));
    let (r2, r3) = (revision_at(&graph, v2), revision_at(&graph, v3));
    assert_eq!(
        inject(&mut graph, root, key(0), 42),
        v4,
        "an equal injection mints no version"
    );
    assert_eq!(revision_at(&graph, v2), r2);
    assert_eq!(revision_at(&graph, v3), r3);
    assert_eq!(revision_at(&graph, v4), r2);
}

/// A computed key with no tracked deps is a function of its untracked input alone, so two
/// computes under the same ε are the same trace: the second adopts the first's revision and value
/// without consulting equality (Appendix A step 1). A dirty ends that trace.
#[test]
fn a_dep_less_recompute_under_the_same_epsilon_adopts_the_stored_revision() {
    let mut graph = VersionedGraph::new();
    let root = graph.new_root();
    let v1 = graph.head(root);
    let compute_without_deps = |graph: &mut VersionedGraph, v: VersionNumber, n: usize| {
        let epsilon = graph.core.epsilon(key(1), v);
        let out = graph.update_computed(
            VersionedGraphKey::new(v, key(1)),
            ComputedValueUpdate {
                value: DiceValidValue::testing_new(DiceKeyValue::<NoEquality>::new(n)),
                deps: SeriesParallelDeps::None,
                epsilon,
            },
            TrackedInvalidationPaths::clean(),
        );
        graph.assert_consistent();
        out.into_computed()
    };
    let first = compute_without_deps(&mut graph, v1, 1);
    let second = compute_without_deps(&mut graph, v1, 2);
    assert_eq!(first.revision(), second.revision());
    #[allow(ambiguous_wide_pointer_comparisons)]
    let same_allocation = std::sync::Arc::ptr_eq(
        first.testing_resident_value().testing_value(),
        second.testing_resident_value().testing_value(),
    );
    assert!(same_allocation);
    let v2 = dirty(&mut graph, root, key(1));
    match graph.get(VersionedGraphKey::new(v2, key(1))) {
        VersionedGraphResult::Unknown { candidate, .. } => {
            assert!(candidate.is_none_or(|c| !c.revalidatable));
        }
        other => panic!("a dirty ends the trace, got {other:?}"),
    }
}

/// A re-issued certificate whose value a later write replaced keeps its own revision, so the
/// transaction's dependents can still match their recorded edges against it.
#[test]
fn dependency_validated_keeps_its_revision_when_superseded() {
    let (mut graph, root) = with_leaf();
    let v2 = graph.head(root);
    let res1 = value(1);
    let first = compute(&mut graph, key(1), v2, res1.dupe());
    let v3 = inject(&mut graph, root, key(0), 200);
    let candidate = candidate_at(&graph, key(1), v3);
    let second = compute(&mut graph, key(1), v3, value(2));
    assert_ne!(first.revision(), second.revision());
    // A validation of the first certificate at version 2 that raced the second compute.
    let returned = revalidate(&mut graph, key(1), v2, candidate);
    assert_eq!(returned.revision(), first.revision());
    assert!(returned.testing_resident_value().instance_equal(&res1));
    assert_eq!(
        graph
            .get(VersionedGraphKey::new(v3, key(1)))
            .unpack_match()
            .unwrap()
            .revision(),
        second.revision()
    );
    // The key is attached to the newer value, so the racing revalidation installs nothing and
    // version 2 is left to re-establish the older certificate on demand.
    assert!(matches!(
        graph.get(VersionedGraphKey::new(v2, key(1))),
        VersionedGraphResult::Unknown { .. }
    ));
}

/// A revalidation whose value is still paged out keeps it paged out.
#[test]
fn dependency_validated_paged_out_value_stays_paged_out() {
    let (mut graph, root) = with_leaf();
    let v2 = graph.head(root);
    compute(&mut graph, key(1), v2, value(1));
    page_out(&mut graph, key(1), 0x77);
    inject(&mut graph, root, key(0), 200);
    let v4 = inject(&mut graph, root, key(0), 100);
    let candidate = candidate_at(&graph, key(1), v4);
    assert!(matches!(candidate.entry, MaybeResident::PagedOut(_)));
    let returned = revalidate(&mut graph, key(1), v4, candidate);
    assert!(returned.paged_out_data_key().is_some());
    assert_eq!(
        graph.pagable_node_counts(),
        PagableNodeCounts {
            resident: 0,
            paged_out: 1,
            candidates: 0,
            backed: 0,
        }
    );
}

/// After a dirty, the stored certificate is offered but cannot be revalidated: its ε is not the
/// version's.
#[test]
fn a_dirtied_key_offers_a_stale_candidate() {
    let (mut graph, root) = with_leaf();
    let v2 = graph.head(root);
    compute(&mut graph, key(1), v2, value(1));
    let v3 = dirty(&mut graph, root, key(1));
    match graph.get(VersionedGraphKey::new(v3, key(1))) {
        VersionedGraphResult::Unknown {
            candidate: Some(candidate),
            epsilon,
        } => assert_ne!(candidate.cert.epsilon, epsilon),
        other => panic!("expected a stale candidate, got {other:?}"),
    }
    // Recomputing to the same value under the new ε re-finds the revision and re-attaches.
    let before = candidate_at(&graph, key(1), v3).cert.revision;
    let after = compute(&mut graph, key(1), v3, value(1));
    assert_eq!(after.revision(), Some(before));
    assert!(
        graph
            .get(VersionedGraphKey::new(v3, key(1)))
            .unpack_match()
            .is_some()
    );
}

fn counts(resident: usize, paged_out: usize, candidates: usize) -> PagableNodeCounts {
    PagableNodeCounts {
        resident,
        paged_out,
        candidates,
        backed: 0,
    }
}

fn backed_counts(resident: usize, paged_out: usize, backed: usize) -> PagableNodeCounts {
    PagableNodeCounts {
        resident,
        paged_out,
        candidates: 0,
        backed,
    }
}

fn evict_backed(graph: &mut VersionedGraph) -> u64 {
    let evicted = graph.evict_backed(usize::MAX);
    graph.assert_consistent();
    evicted
}

fn stored_data_key(graph: &VersionedGraph, k: DiceKey, v: VersionNumber) -> Option<DataKey> {
    graph
        .get(VersionedGraphKey::new(v, k))
        .unpack_match()
        .unwrap()
        .paged_out_data_key()
}

/// Under `OncePerKey` a key's value is paged out at most once: a value recomputed or read back
/// after a page-out stays resident with no row, and is not a candidate.
#[test]
fn once_per_key_keeps_recomputed_and_read_back_values_resident() {
    let (mut graph, root) = with_leaf();
    let v2 = graph.head(root);
    compute(&mut graph, key(1), v2, value(7));
    page_out(&mut graph, key(1), 0x1);
    let v3 = inject(&mut graph, root, key(0), 200);
    compute(&mut graph, key(1), v3, value(8));
    assert_eq!(graph.pagable_node_counts(), counts(1, 0, 0));
    assert!(graph.keys_to_page_out().is_empty());

    compute(&mut graph, key(2), v3, value(9));
    page_out(&mut graph, key(2), 0x2);
    graph.rehydrate(key(2), DataKey::testing_new(0x2), value(9));
    graph.assert_consistent();
    assert_eq!(graph.pagable_node_counts(), counts(2, 0, 0));
    assert!(stored_data_key(&graph, key(2), v3).is_none());
}

/// Under `OnMemoryPressure` a page-out evicts nothing: a value never paged out is written and
/// stays resident, backed by its row, until evicted on demand.
#[test]
fn on_memory_pressure_page_out_keeps_the_values_it_writes_resident() {
    let (mut graph, root) = with_leaf();
    let v2 = graph.head(root);
    compute(&mut graph, key(1), v2, value(7));
    page_out_under(&mut graph, key(1), 0x1, EvictionPolicy::OnMemoryPressure);
    assert_eq!(graph.pagable_node_counts(), backed_counts(1, 0, 1));
    assert!(graph.keys_to_page_out().is_empty());
    assert_eq!(evict_backed(&mut graph), 1);
    assert_eq!(graph.pagable_node_counts(), counts(0, 1, 0));
    assert_eq!(
        stored_data_key(&graph, key(1), v2),
        Some(DataKey::testing_new(0x1))
    );
}

/// A recomputed value is written by the next page-out but stays resident, backed by its row,
/// until evicted on demand.
#[test]
fn recomputed_value_is_written_and_kept_resident() {
    let (mut graph, root) = with_leaf_under(PageOutPolicy::OncePerValue);
    let v2 = graph.head(root);
    compute(&mut graph, key(1), v2, value(7));
    page_out(&mut graph, key(1), 0x1);
    let v3 = inject(&mut graph, root, key(0), 200);
    compute(&mut graph, key(1), v3, value(8));
    assert_eq!(graph.pagable_node_counts(), counts(1, 0, 1));

    page_out(&mut graph, key(1), 0x2);
    assert_eq!(graph.pagable_node_counts(), backed_counts(1, 0, 1));
    assert!(stored_data_key(&graph, key(1), v3).is_none());
    assert!(graph.keys_to_page_out().is_empty());

    assert_eq!(evict_backed(&mut graph), 1);
    assert_eq!(graph.pagable_node_counts(), counts(0, 1, 0));
    assert_eq!(
        stored_data_key(&graph, key(1), v3),
        Some(DataKey::testing_new(0x2))
    );
}

/// A value read back from its row is backed by it, and evicts without another write.
#[test]
fn rehydrated_value_is_backed_by_its_row() {
    let (mut graph, root) = with_leaf_under(PageOutPolicy::OncePerValue);
    let v2 = graph.head(root);
    compute(&mut graph, key(1), v2, value(7));
    page_out(&mut graph, key(1), 0x1234);
    graph.rehydrate(key(1), DataKey::testing_new(0x1234), value(7));
    graph.assert_consistent();
    assert_eq!(graph.pagable_node_counts(), backed_counts(1, 0, 1));
    assert!(graph.keys_to_page_out().is_empty());
    assert!(stored_data_key(&graph, key(1), v2).is_none());

    assert_eq!(evict_backed(&mut graph), 1);
    assert_eq!(graph.pagable_node_counts(), counts(0, 1, 0));
    assert_eq!(
        stored_data_key(&graph, key(1), v2),
        Some(DataKey::testing_new(0x1234))
    );
    assert_eq!(evict_backed(&mut graph), 0);
}

/// A computed key whose values are asserted (`changed_to` on a `Key`, which buck2 does for its
/// pageable starlark roots) pages like one that was computed.
#[test]
fn asserted_values_of_computed_keys_page_out() {
    let mut graph = VersionedGraph::new();
    graph.set_page_out_policy(PageOutPolicy::OncePerValue);
    let root = graph.new_root();
    let v2 = graph.commit(
        root,
        [(
            key(1),
            ChangeType::UpdateValue(value(1), StorageType::Normal),
            InvalidationSourcePriority::Normal,
        )],
    );
    graph.assert_consistent();
    assert_eq!(graph.pagable_node_counts(), counts(1, 0, 1));
    page_out(&mut graph, key(1), 7);
    assert_eq!(graph.pagable_node_counts(), counts(0, 1, 0));
    assert_eq!(
        graph
            .get(VersionedGraphKey::new(v2, key(1)))
            .unpack_match()
            .unwrap()
            .paged_out_data_key(),
        Some(pagable::DataKey::testing_new(7))
    );
    graph.rehydrate(key(1), pagable::DataKey::testing_new(7), value(1));
    graph.assert_consistent();
    assert_eq!(graph.pagable_node_counts(), backed_counts(1, 0, 1));
}

#[test]
fn page_out_index_tracks_the_values() {
    let (mut graph, root) = with_leaf_under(PageOutPolicy::OncePerValue);
    let v2 = graph.head(root);
    // An `InjectedKey` takes no part in paging.
    assert_eq!(graph.pagable_node_counts(), counts(0, 0, 0));
    compute(&mut graph, key(1), v2, value(1));
    compute(&mut graph, key(2), v2, value(2));
    assert_eq!(graph.pagable_node_counts(), counts(2, 0, 2));
    page_out(&mut graph, key(1), 1);
    assert_eq!(graph.pagable_node_counts(), counts(1, 1, 1));
    assert_eq!(graph.paged_out_keys().len(), 1);
    graph.rehydrate(key(1), pagable::DataKey::testing_new(1), value(1));
    graph.assert_consistent();
    assert_eq!(
        graph.pagable_node_counts(),
        PagableNodeCounts {
            resident: 2,
            paged_out: 0,
            candidates: 1,
            backed: 1,
        }
    );
    // A stale page-in for a data key no value holds is ignored.
    graph.rehydrate(key(1), pagable::DataKey::testing_new(99), value(1));
    graph.assert_consistent();
    let k2_result = graph.get(VersionedGraphKey::new(v2, key(2)));
    let k2_value = k2_result.unpack_match().unwrap();
    let k2_value = k2_value.testing_resident_value().testing_value().dupe();
    graph.mark_non_pageable(vec![(key(2), DiceValidValue::from_arc(k2_value))]);
    graph.assert_consistent();
    assert_eq!(graph.pagable_node_counts(), backed_counts(2, 0, 1));
    assert!(graph.keys_to_page_out().is_empty());
    // Deleting the branch releases the values, and the index with them.
    graph.delete_branch(root);
    graph.assert_consistent();
    assert_eq!(graph.pagable_node_counts(), counts(0, 0, 0));
    assert!(graph.paged_out_keys().is_empty());
}

/// A fork resolves the values of the branch it was forked from without writes of its own, and
/// keeps them retained after that branch has moved on to others.
#[test]
fn a_fork_shares_and_then_keeps_the_values_it_resolves() {
    let (mut graph, root) = with_leaf();
    let v2 = graph.head(root);
    let first = compute(&mut graph, key(1), v2, value(1));
    let child = graph.fork(v2);
    graph.assert_consistent();
    let child_v1 = graph.head(child);
    let at_child = |graph: &VersionedGraph| {
        graph
            .get(VersionedGraphKey::new(child_v1, key(1)))
            .unpack_match()
            .expect("the child resolves what its fork point resolved")
            .revision()
    };
    assert_eq!(at_child(&graph), first.revision());
    assert_eq!(graph.pagable_node_counts().resident, 1);

    let v3 = inject(&mut graph, root, key(0), 200);
    let second = compute(&mut graph, key(1), v3, value(2));
    assert_ne!(first.revision(), second.revision());
    assert_eq!(at_child(&graph), first.revision());
    assert_eq!(graph.pagable_node_counts().resident, 2);
}

/// Deleting a branch releases the values only it referenced, injected ones included, and leaves
/// the ones its fork point resolves alone. Its versions then resolve nothing, and a write from
/// them is handed back without being retained.
#[test]
fn deleting_a_branch_releases_its_values_and_retains_nothing_written_from_it() {
    let (mut graph, root) = with_leaf();
    let v2 = graph.head(root);
    let shared = compute(&mut graph, key(1), v2, value(1));
    let child = graph.fork(v2);
    let child_v2 = graph.commit(
        child,
        [(
            key(0),
            ChangeType::UpdateValue(value(200), StorageType::Injected),
            InvalidationSourcePriority::Normal,
        )],
    );
    graph.assert_consistent();
    let own = compute(&mut graph, key(1), child_v2, value(2));
    assert_ne!(shared.revision(), own.revision());
    assert_eq!(graph.pagable_node_counts().resident, 2);
    assert_eq!(graph.values.get(&key(0)).unwrap().entries.len(), 2);

    graph.delete_branch(child);
    graph.assert_consistent();
    assert_eq!(graph.pagable_node_counts().resident, 1);
    assert_eq!(graph.values.get(&key(0)).unwrap().entries.len(), 1);
    assert_eq!(
        graph
            .get(VersionedGraphKey::new(v2, key(1)))
            .unpack_match()
            .unwrap()
            .revision(),
        shared.revision()
    );

    match graph.get(VersionedGraphKey::new(child_v2, key(1))) {
        VersionedGraphResult::Unknown {
            candidate: None, ..
        } => {}
        other => panic!("a deleted branch resolves nothing, got {other:?}"),
    }
    let late = value(3);
    let returned = graph
        .update_computed(
            VersionedGraphKey::new(child_v2, key(1)),
            ComputedValueUpdate {
                value: late.dupe(),
                deps: SeriesParallelDeps::None,
                epsilon: EpsilonToken::INITIAL,
            },
            TrackedInvalidationPaths::clean(),
        )
        .into_computed();
    graph.assert_consistent();
    assert!(returned.testing_resident_value().instance_equal(&late));
    assert!(returned.revision().unwrap().as_u32() > own.revision().unwrap().as_u32());
    assert_eq!(graph.pagable_node_counts().resident, 1);
}

/// Deleting a branch keeps the per-key revision counters, so a value computed afterwards can
/// never collide with a revision handed out before.
#[test]
fn deleting_a_branch_keeps_revision_counters() {
    let (mut graph, root) = with_leaf();
    let v2 = graph.head(root);
    let before = compute(&mut graph, key(1), v2, value(1));
    graph.delete_branch(root);
    graph.assert_consistent();
    assert_eq!(graph.pagable_node_counts().resident, 0);

    let other = graph.new_root();
    let head = graph.commit(
        other,
        [(
            key(0),
            ChangeType::UpdateValue(value(100), StorageType::Injected),
            InvalidationSourcePriority::Normal,
        )],
    );
    let after = compute(&mut graph, key(1), head, value(2));
    assert!(after.revision().unwrap().as_u32() > before.revision().unwrap().as_u32());
}

/// A dump covers every live branch, so it does not need the branch the state began with: that
/// one is deleted like any other once nothing runs on it.
#[test]
fn introspection_covers_every_live_branch() {
    let (mut graph, root) = with_leaf();
    let v2 = graph.head(root);
    compute(&mut graph, key(1), v2, value(1));
    let other = graph.new_root();
    graph.commit(
        other,
        [(
            key(0),
            ChangeType::UpdateValue(value(100), StorageType::Injected),
            InvalidationSourcePriority::Normal,
        )],
    );

    let both = graph.introspect();
    assert_eq!(both.nodes.len(), 3);
    assert!(both.nodes.contains_key(&(root, key(0))));
    assert!(both.nodes.contains_key(&(root, key(1))));
    assert!(both.nodes.contains_key(&(other, key(0))));
    assert_eq!(both.edges[&key(1)].as_slice(), &[key(0)]);

    graph.delete_branch(root);
    let after = graph.introspect();
    assert_eq!(
        after.nodes.keys().collect::<Vec<_>>(),
        vec![&(other, key(0))]
    );
    assert!(after.edges.values().all(|deps| deps.is_empty()));
}

/// The head of `root` at `v` as `buck2 suspend` dumps it, replayed onto a new graph as a resume
/// does, mapping every key to itself.
fn replay(graph: &VersionedGraph, v: VersionNumber) -> (VersionedGraph, BranchId, RestoreCounts) {
    use crate::core::graph::HeadSource;
    use crate::core::graph::HeadValue;

    let mut asserts = Vec::new();
    let mut certs = Vec::new();
    let mut dirtied = Vec::new();
    let mut mints = Vec::new();
    for entry in graph.dump_head(v) {
        mints.push((entry.key, entry.mint));
        match entry.source {
            HeadSource::Asserted { priority } => {
                let HeadValue::Resident(value) = entry.value else {
                    panic!("an injected value stays resident");
                };
                asserts.push(RestoredAssert {
                    key: entry.key,
                    revision: entry.revision,
                    priority,
                    injected: entry.injected,
                    value,
                    row: DataKey::testing_new(1),
                });
            }
            source @ (HeadSource::Certified { .. } | HeadSource::Candidate { .. }) => {
                let cert = match source {
                    HeadSource::Certified { cert } => cert,
                    HeadSource::Candidate {
                        cert,
                        dirtied: priority,
                    } => {
                        if let Some(priority) = priority {
                            dirtied.push((entry.key, priority));
                        }
                        cert
                    }
                    HeadSource::Asserted { .. } => unreachable!(),
                };
                let value = match entry.value {
                    HeadValue::Row(row) => MaybeResident::PagedOut(row),
                    HeadValue::Resident(value) => MaybeResident::Resident(value),
                };
                let deps = cert
                    .deps
                    .to_image(|k| Some(k.index))
                    .expect("every key has an ordinal");
                certs.push(RestoredCert {
                    key: entry.key,
                    revision: entry.revision,
                    deps: SeriesParallelDeps::from_image(&deps, |o| Some(key(o)))
                        .expect("ordinals map back"),
                    value,
                });
            }
        }
    }
    let mut restored = VersionedGraph::new();
    restored.set_page_out_policy(PageOutPolicy::OncePerKey);
    let root = restored.new_root();
    let counts = restored.restore_head(root, asserts, certs, dirtied, mints);
    restored.assert_consistent();
    (restored, root, counts)
}

/// A replayed head resolves every key the dumped one did, to the same revision, and a
/// re-injection of an equal value keeps it.
#[test]
fn replayed_head_resolves_like_the_dumped_one() {
    let (mut graph, root) = with_leaf();
    let v = graph.head(root);
    let computed = compute(&mut graph, key(1), v, value(7));
    page_out(&mut graph, key(1), 5);

    let (mut restored, root2, counts) = replay(&graph, v);
    assert_eq!(
        counts,
        RestoreCounts {
            asserted: 1,
            certified: 1,
            candidates: 0,
            dropped: 0
        }
    );
    let head = restored.head(root2);
    let got = restored
        .get(VersionedGraphKey::new(head, key(1)))
        .unpack_match()
        .expect("the certificate resolves on the new root")
        .revision();
    assert_eq!(got, computed.revision());

    let again = inject(&mut restored, root2, key(0), 100);
    assert!(
        restored
            .get(VersionedGraphKey::new(again, key(1)))
            .unpack_match()
            .is_some(),
        "re-injecting an equal value keeps every dependent valid"
    );
    let changed = inject(&mut restored, root2, key(0), 101);
    assert!(
        restored
            .get(VersionedGraphKey::new(changed, key(1)))
            .unpack_match()
            .is_none(),
        "a changed input invalidates its dependents after a resume as before"
    );
}

/// A certificate whose premise the image does not carry covers nothing, and its value is let go.
#[test]
fn replay_drops_a_certificate_whose_premise_is_missing() {
    let (mut graph, root) = with_leaf();
    let v = graph.head(root);
    compute(&mut graph, key(1), v, value(7));
    page_out(&mut graph, key(1), 5);

    use crate::core::graph::HeadSource;
    use crate::core::graph::HeadValue;
    let certs: Vec<RestoredCert> = graph
        .dump_head(v)
        .into_iter()
        .filter_map(|e| match (e.source, e.value) {
            (HeadSource::Certified { cert }, HeadValue::Row(row)) => Some(RestoredCert {
                key: e.key,
                revision: e.revision,
                deps: SeriesParallelDeps::from_image(
                    &cert.deps.to_image(|k| Some(k.index)).unwrap(),
                    |o| Some(key(o)),
                )
                .unwrap(),
                value: MaybeResident::PagedOut(row),
            }),
            _ => None,
        })
        .collect();
    assert_eq!(certs.len(), 1);
    let mut restored = VersionedGraph::new();
    let root2 = restored.new_root();
    let counts = restored.restore_head(root2, Vec::new(), certs, Vec::new(), Vec::new());
    restored.assert_consistent();
    assert_eq!(counts.dropped, 1);
    assert_eq!(counts.certified, 0);
    let head = restored.head(root2);
    assert!(
        restored
            .get(VersionedGraphKey::new(head, key(1)))
            .unpack_match()
            .is_none()
    );
}

/// A key recomputed after a resume never gets a revision its image already named: the paged-out
/// value behind that revision cannot be compared, so reusing the number would name two values.
#[test]
fn replay_keeps_the_mints() {
    let (mut graph, root) = with_leaf();
    let v = graph.head(root);
    let first = compute(&mut graph, key(1), v, value(7));
    page_out(&mut graph, key(1), 5);

    let (mut restored, root2, _) = replay(&graph, v);
    let changed = inject(&mut restored, root2, key(0), 101);
    let recomputed = compute(&mut restored, key(1), changed, value(8));
    assert_ne!(recomputed.revision(), first.revision());
}

/// A key the head had dirtied, and a key that read it, come back unknown with their
/// certificates as candidates: the dirtied key's to compare a recompute against, its
/// dependent's to re-establish, as on a live daemon.
#[test]
fn replay_keeps_candidates_of_a_dirtied_key() {
    let (mut graph, root) = with_leaf();
    let v = graph.head(root);
    let leaf_value = compute(&mut graph, key(1), v, value(7));
    let deps = SeriesParallelDeps::serial_from_edges(vec![DepEdge::new(
        key(1),
        leaf_value.revision().expect("valid"),
    )]);
    let epsilon = graph.core.epsilon(key(2), v);
    graph.update_computed(
        VersionedGraphKey::new(v, key(2)),
        ComputedValueUpdate {
            value: value(8),
            deps,
            epsilon,
        },
        TrackedInvalidationPaths::clean(),
    );
    graph.assert_consistent();
    page_out(&mut graph, key(1), 5);
    page_out(&mut graph, key(2), 6);
    let dirtied = dirty(&mut graph, root, key(1));

    let (restored, root2, counts) = replay(&graph, dirtied);
    assert_eq!(
        counts,
        RestoreCounts {
            asserted: 1,
            certified: 0,
            candidates: 2,
            dropped: 0
        }
    );
    let head = restored.head(root2);
    let candidate = |k| match restored.get(VersionedGraphKey::new(head, k)) {
        VersionedGraphResult::Unknown {
            candidate: Some(candidate),
            ..
        } => candidate.revalidatable,
        other => panic!("{k:?}: expected an Unknown with a candidate, got {other:?}"),
    };
    assert!(!candidate(key(1)), "the dirtied key's input changed");
    assert!(candidate(key(2)), "its dependent may be re-established");
}
