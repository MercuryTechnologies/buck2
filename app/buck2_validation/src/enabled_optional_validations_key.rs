/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::collections::BTreeSet;

use allocative::Allocative;
use derive_more::Display;
use dice::DiceTransactionUpdater;
use dice::EqualityBehavior;
use dice::InjectedKey;
use dice::PagableValueSerialize;
use dice::ValueSerialize;
use dupe::Dupe;
use pagable::Pagable;
use pagable::pagable_typetag;

#[derive(Display, Debug, Hash, Eq, Clone, Dupe, PartialEq, Allocative, Pagable)]
#[pagable_typetag(dice::DiceKeyDyn)]
// DICE key that corresponds to optional validations that are enabled via command line flag.
pub(crate) struct EnabledOptionalValidationsKey;

pub trait SetEnabledOptionalValidations {
    fn set_enabled_optional_validations(
        &mut self,
        validations: Vec<String>,
    ) -> buck2_error::Result<()>;
}

impl InjectedKey for EnabledOptionalValidationsKey {
    type Value = BTreeSet<String>;

    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| x == y)
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        PagableValueSerialize::<Self::Value>::new()
    }
}

impl SetEnabledOptionalValidations for DiceTransactionUpdater {
    fn set_enabled_optional_validations(
        &mut self,
        validations: Vec<String>,
    ) -> buck2_error::Result<()> {
        Ok(self.changed_to(vec![(
            EnabledOptionalValidationsKey,
            BTreeSet::from_iter(validations),
        )])?)
    }
}

pub fn frontier_validations_key_type() -> &'static str {
    <EnabledOptionalValidationsKey as dice::Key>::key_type_name()
}

pub fn frontier_inspect_validations(
    key: &dyn std::any::Any,
    value: Option<&dyn std::any::Any>,
) -> Option<buck2_common::frontier::Inspected> {
    use buck2_common::frontier::Inspected;
    key.downcast_ref::<EnabledOptionalValidationsKey>()?;
    let key_type = frontier_validations_key_type();
    Some(match value.and_then(|v| v.downcast_ref::<BTreeSet<String>>()) {
        Some(v) => Inspected::Leaf {
            id: buck2_common::frontier::LeafId::Injected {
                key_type: key_type.to_owned(),
            },
            canon: Some(buck2_common::frontier::canonical_injected(key_type, v)),
        },
        None => Inspected::Barrier("EnabledOptionalValidationsKey unreadable".to_owned()),
    })
}
