/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use allocative::Allocative;
use async_trait::async_trait;
use dice::DiceComputations;
use dice::DiceTransactionUpdater;
use dice::EqualityBehavior;
use dice::InjectedKey;
use dice::PagableValueSerialize;
use dice::ValueSerialize;
use dupe::Dupe;
use pagable::Pagable;
use pagable::pagable_typetag;

#[derive(Debug, Clone, Dupe, Eq, PartialEq, Allocative, Pagable)]
struct StarlarkTypesValue {
    disable_starlark_types: bool,
    unstable_typecheck: bool,
}

#[derive(
    Debug,
    derive_more::Display,
    Copy,
    Clone,
    Dupe,
    Eq,
    PartialEq,
    Hash,
    Allocative,
    Pagable
)]
#[display("{:?}", self)]
#[pagable_typetag(dice::DiceKeyDyn)]
struct StarlarkTypesKey;

impl InjectedKey for StarlarkTypesKey {
    type Value = StarlarkTypesValue;

    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| x == y)
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        PagableValueSerialize::<Self::Value>::new()
    }
}

pub trait SetStarlarkTypes {
    fn set_starlark_types(
        &mut self,
        disable_starlark_types: bool,
        unstable_typecheck: bool,
    ) -> buck2_error::Result<()>;
}

impl SetStarlarkTypes for DiceTransactionUpdater {
    fn set_starlark_types(
        &mut self,
        disable_starlark_types: bool,
        unstable_typecheck: bool,
    ) -> buck2_error::Result<()> {
        Ok(self.changed_to([(
            StarlarkTypesKey,
            StarlarkTypesValue {
                disable_starlark_types,
                unstable_typecheck,
            },
        )])?)
    }
}

#[async_trait]
pub trait GetStarlarkTypes {
    async fn get_disable_starlark_types(&mut self) -> buck2_error::Result<bool>;
    async fn get_unstable_typecheck(&mut self) -> buck2_error::Result<bool>;
}

#[async_trait]
impl GetStarlarkTypes for DiceComputations<'_> {
    async fn get_disable_starlark_types(&mut self) -> buck2_error::Result<bool> {
        Ok(self
            .compute(&StarlarkTypesKey)
            .await?
            .disable_starlark_types)
    }

    async fn get_unstable_typecheck(&mut self) -> buck2_error::Result<bool> {
        Ok(self.compute(&StarlarkTypesKey).await?.unstable_typecheck)
    }
}

impl buck2_common::frontier::FrontierLeaf for StarlarkTypesValue {
    fn frontier_digest(&self, h: &mut buck2_common::frontier::FrontierHasher) {
        let StarlarkTypesValue {
            disable_starlark_types,
            unstable_typecheck,
        } = self;
        h.update(&[*disable_starlark_types as u8, *unstable_typecheck as u8]);
    }
}

pub fn frontier_starlark_types_key_type() -> &'static str {
    <StarlarkTypesKey as dice::Key>::key_type_name()
}

pub fn frontier_inspect_starlark_types(
    key: &dyn std::any::Any,
    value: Option<&dyn std::any::Any>,
) -> Option<buck2_common::frontier::Inspected> {
    use buck2_common::frontier::Inspected;
    key.downcast_ref::<StarlarkTypesKey>()?;
    let key_type = frontier_starlark_types_key_type();
    Some(match value.and_then(|v| v.downcast_ref::<StarlarkTypesValue>()) {
        Some(v) => Inspected::Leaf {
            id: buck2_common::frontier::LeafId::Injected {
                key_type: key_type.to_owned(),
            },
            canon: Some(buck2_common::frontier::canonical_injected(key_type, v)),
        },
        None => Inspected::Barrier("StarlarkTypesKey value unreadable".to_owned()),
    })
}
