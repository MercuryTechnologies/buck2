/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! A serializer that computes the [`DataKey`] a value would be stored under,
//! without storing it or anything it reaches.

use postcard::ser_flavors::Flavor;

use crate::PagableSerializer;
use crate::arc_erase::ArcEraseDyn;
use crate::arc_erase::ArcSerializeOutcome;
use crate::flavors::PagableVecFlavor;
use crate::storage::data::DataKey;
use crate::traits::PagableCursor;
use crate::traits::StorageContext;

/// Serializes into memory and hashes the result the way storage keys it: the
/// bytes plus the keys of the nested arcs, each computed the same way, bottom
/// up. Nothing is written to storage and no arc is given a key.
///
/// `skip_arc` names arcs whose identity the caller has already written into the
/// stream itself, so their content is left out of the digest rather than
/// serialized again.
pub struct DigestSerializer<'a> {
    serde: postcard::Serializer<PagableVecFlavor>,
    arcs: Vec<DataKey>,
    arc_types: Vec<&'static str>,
    storage_context: &'a StorageContext,
    skip_arc: fn(&dyn ArcEraseDyn) -> bool,
}

impl<'a> DigestSerializer<'a> {
    pub fn new(
        storage_context: &'a StorageContext,
        skip_arc: fn(&dyn ArcEraseDyn) -> bool,
    ) -> Self {
        Self {
            serde: postcard::Serializer {
                output: PagableVecFlavor::new(),
            },
            arcs: Vec::new(),
            arc_types: Vec::new(),
            storage_context,
            skip_arc,
        }
    }

    /// The bytes written so far, and each nested arc's type and key.
    pub fn finish_parts(self) -> crate::Result<(Vec<u8>, Vec<(&'static str, DataKey)>)> {
        let data = self.serde.output.finalize()?;
        Ok((data, self.arc_types.into_iter().zip(self.arcs).collect()))
    }

    /// The key of everything written so far.
    pub fn finish(self) -> crate::Result<DataKey> {
        let data = self.serde.output.finalize()?;
        Ok(DataKey::compute(
            self.arcs.len(),
            &data,
            bytemuck::cast_slice(&self.arcs),
        ))
    }
}

impl PagableSerializer for DigestSerializer<'_> {
    fn serde(&mut self) -> &mut postcard::Serializer<PagableVecFlavor> {
        &mut self.serde
    }

    fn serialize_arc(&mut self, arc: &dyn ArcEraseDyn) -> crate::Result<()> {
        if (self.skip_arc)(arc) {
            return Ok(());
        }
        let mut nested = DigestSerializer::new(self.storage_context, self.skip_arc);
        let key = match arc.serialize(&mut nested)? {
            ArcSerializeOutcome::Serialized => nested.finish()?,
            ArcSerializeOutcome::ReuseDataKey(key) => key,
        };
        self.arcs.push(key);
        self.arc_types.push(arc.type_name());
        Ok(())
    }

    fn position(&mut self) -> PagableCursor {
        PagableCursor {
            byte_pos: self.serde.output.position(),
            arc_index: self.arcs.len(),
        }
    }

    fn storage_context(&self) -> &StorageContext {
        self.storage_context
    }
}
