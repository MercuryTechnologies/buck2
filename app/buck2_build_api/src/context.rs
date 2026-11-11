/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Injects data used for build onto dice

use allocative::Allocative;
use async_trait::async_trait;
use buck2_core::fs::buck_out_path::BuckOutPathResolver;
use buck2_core::fs::project_rel_path::ProjectRelativePathBuf;
use derive_more::Display;
use dice::DiceComputations;
use dice::DiceTransactionUpdater;
use dice::EqualityBehavior;
use dice::InjectedKey;
use dice::PagableValueSerialize;
use dice::ValueSerialize;
use dupe::Dupe;
use pagable::Pagable;
use pagable::pagable_typetag;

#[async_trait]
pub trait HasBuildContextData<'d> {
    async fn get_buck_out_path(&mut self) -> buck2_error::Result<&'d BuckOutPathResolver>;
}

pub trait SetBuildContextData {
    fn set_buck_out_path(
        &mut self,
        path: Option<ProjectRelativePathBuf>,
    ) -> buck2_error::Result<()>;
}

#[derive(PartialEq, Eq, Allocative, Pagable)]
pub struct BuildData {
    buck_out_path_resolver: BuckOutPathResolver,
}

#[derive(Clone, Dupe, Display, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
#[display("{:?}", self)]
#[pagable_typetag(dice::DiceKeyDyn)]
struct BuildDataKey;

impl InjectedKey for BuildDataKey {
    type Value = BuildData;

    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| x == y)
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        PagableValueSerialize::<Self::Value>::new()
    }
}

#[async_trait]
impl<'d> HasBuildContextData<'d> for DiceComputations<'d> {
    async fn get_buck_out_path(&mut self) -> buck2_error::Result<&'d BuckOutPathResolver> {
        let data = self.compute(&BuildDataKey).await?;
        Ok(&data.buck_out_path_resolver)
    }
}

impl SetBuildContextData for DiceTransactionUpdater {
    fn set_buck_out_path(
        &mut self,
        path: Option<ProjectRelativePathBuf>,
    ) -> buck2_error::Result<()> {
        Ok(self.changed_to(vec![(
            BuildDataKey,
            BuildData {
                buck_out_path_resolver: BuckOutPathResolver::new(path.unwrap_or_else(|| {
                    ProjectRelativePathBuf::unchecked_new("buck-out/v2".to_owned())
                })),
            },
        )])?)
    }
}

impl buck2_common::frontier::FrontierLeaf for BuildData {
    fn frontier_digest(&self, h: &mut buck2_common::frontier::FrontierHasher) {
        let BuildData {
            buck_out_path_resolver,
        } = self;
        buck2_common::frontier::digest_str(h, buck_out_path_resolver.root().as_str());
    }
}

pub fn frontier_build_data_key_type() -> &'static str {
    <BuildDataKey as dice::Key>::key_type_name()
}

pub fn frontier_inspect_build_data(
    key: &dyn std::any::Any,
    value: Option<&dyn std::any::Any>,
) -> Option<buck2_common::frontier::Inspected> {
    use buck2_common::frontier::Inspected;
    key.downcast_ref::<BuildDataKey>()?;
    let key_type = frontier_build_data_key_type();
    Some(match value.and_then(|v| v.downcast_ref::<BuildData>()) {
        Some(d) => Inspected::Leaf {
            id: buck2_common::frontier::LeafId::Injected {
                key_type: key_type.to_owned(),
            },
            canon: Some(buck2_common::frontier::canonical_injected(key_type, d)),
        },
        None => Inspected::Barrier("BuildDataKey value unreadable".to_owned()),
    })
}
