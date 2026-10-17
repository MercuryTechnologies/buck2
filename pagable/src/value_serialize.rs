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

use crate::Pagable;
use crate::PagableDeserializer;
use crate::PagableSerializer;

/// Serializes a value to/from pagable storage that may or may not be serializable.
/// Should possibly be called PagableMaybeSerialize.
///
/// `pagable_serialize_value` returns `None` to indicate the value should not be
/// paged out (e.g. error or otherwise non-pagable values), in which case it is
/// kept in memory.
pub trait ValueSerialize {
    type Value: Allocative + Send + Sync + 'static;

    fn pagable_serialize_value(
        &self,
        v: &Self::Value,
        ser: &mut dyn PagableSerializer,
    ) -> Option<crate::Result<()>>;

    fn pagable_deserialize_value<'de, D: PagableDeserializer<'de> + ?Sized>(
        &self,
        deser: &mut D,
    ) -> crate::Result<Self::Value>;

    /// Whether a page-out may evict a value it serialized. A value that must stay resident (DICE
    /// hands a projection's value out by reference, without a page-in) can still be serialized,
    /// so that a suspended daemon's image carries it.
    fn pages_out(&self) -> bool {
        true
    }
}

/// [`ValueSerialize`] that panics if serialization is attempted, for values whose
/// pagable support is not yet implemented.
pub struct TodoValueSerialize<V>(std::marker::PhantomData<V>);
impl<V> TodoValueSerialize<V> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<V: Allocative + Send + Sync + 'static> ValueSerialize for TodoValueSerialize<V> {
    type Value = V;

    fn pagable_serialize_value(
        &self,
        _v: &Self::Value,
        _ser: &mut dyn PagableSerializer,
    ) -> Option<crate::Result<()>> {
        unimplemented!()
    }

    fn pagable_deserialize_value<'de, D: PagableDeserializer<'de> + ?Sized>(
        &self,
        _deser: &mut D,
    ) -> crate::Result<Self::Value> {
        unimplemented!()
    }
}

/// [`ValueSerialize`] that never pages out, keeping every value in memory.
pub struct NoValueSerialize<V>(std::marker::PhantomData<V>);
impl<V> NoValueSerialize<V> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<V: Allocative + Send + Sync + 'static> ValueSerialize for NoValueSerialize<V> {
    type Value = V;

    fn pagable_serialize_value(
        &self,
        _v: &Self::Value,
        _ser: &mut dyn PagableSerializer,
    ) -> Option<crate::Result<()>> {
        None
    }

    fn pagable_deserialize_value<'de, D: PagableDeserializer<'de> + ?Sized>(
        &self,
        _deser: &mut D,
    ) -> crate::Result<Self::Value> {
        unimplemented!()
    }
}

/// [`ValueSerialize`] for a `Result` value that pages out the `Ok` payload and
/// keeps `Err` values in memory.
pub struct OkPagableValueSerialize<V>(std::marker::PhantomData<V>);

impl<V> OkPagableValueSerialize<V> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<V: Pagable + Allocative + Send + Sync + 'static, E: Allocative + Send + Sync + 'static>
    ValueSerialize for OkPagableValueSerialize<Result<V, E>>
{
    type Value = Result<V, E>;

    fn pagable_serialize_value(
        &self,
        v: &Self::Value,
        ser: &mut dyn PagableSerializer,
    ) -> Option<crate::Result<()>> {
        match v {
            Ok(v) => Some(v.pagable_serialize(ser)),
            Err(_) => None,
        }
    }

    fn pagable_deserialize_value<'de, D: PagableDeserializer<'de> + ?Sized>(
        &self,
        deser: &mut D,
    ) -> crate::Result<Self::Value> {
        Ok(Ok(V::pagable_deserialize(deser)?))
    }
}

/// [`ValueSerialize`] for a [`Pagable`] value that always pages out.
pub struct PagableValueSerialize<V>(std::marker::PhantomData<V>);

impl<V> PagableValueSerialize<V> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<V: Pagable + Allocative + Send + Sync + 'static> ValueSerialize for PagableValueSerialize<V> {
    type Value = V;

    fn pagable_serialize_value(
        &self,
        v: &Self::Value,
        ser: &mut dyn PagableSerializer,
    ) -> Option<crate::Result<()>> {
        Some(v.pagable_serialize(ser))
    }

    fn pagable_deserialize_value<'de, D: PagableDeserializer<'de> + ?Sized>(
        &self,
        deser: &mut D,
    ) -> crate::Result<Self::Value> {
        V::pagable_deserialize(deser)
    }
}

/// A [`ValueSerialize`] that serializes as `S` does but never lets a page-out evict the value.
///
/// For values that must stay resident while the daemon runs, such as DICE projections, which are
/// read by reference without a page-in, but that `buck2 suspend` still has to write, because a
/// suspended daemon's image holds every value its graph names.
pub struct ResidentValueSerialize<S>(S);

impl<S> ResidentValueSerialize<S> {
    pub fn new(inner: S) -> Self {
        Self(inner)
    }
}

impl<S: ValueSerialize> ValueSerialize for ResidentValueSerialize<S> {
    type Value = S::Value;

    fn pagable_serialize_value(
        &self,
        v: &Self::Value,
        ser: &mut dyn PagableSerializer,
    ) -> Option<crate::Result<()>> {
        self.0.pagable_serialize_value(v, ser)
    }

    fn pagable_deserialize_value<'de, D: PagableDeserializer<'de> + ?Sized>(
        &self,
        deser: &mut D,
    ) -> crate::Result<Self::Value> {
        self.0.pagable_deserialize_value(deser)
    }

    fn pages_out(&self) -> bool {
        false
    }
}

/// [`ValueSerialize`] for an `Option` whose `Some` payload is not serializable: `None` is
/// written as nothing and read back as `None`; a `Some` is declined, so it stays in memory.
pub struct NoneValueSerialize<V>(std::marker::PhantomData<V>);

impl<V> NoneValueSerialize<V> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T: Allocative + Send + Sync + 'static> ValueSerialize for NoneValueSerialize<Option<T>> {
    type Value = Option<T>;

    fn pagable_serialize_value(
        &self,
        v: &Self::Value,
        _ser: &mut dyn PagableSerializer,
    ) -> Option<crate::Result<()>> {
        match v {
            None => Some(Ok(())),
            Some(_) => None,
        }
    }

    fn pagable_deserialize_value<'de, D: PagableDeserializer<'de> + ?Sized>(
        &self,
        _deser: &mut D,
    ) -> crate::Result<Self::Value> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use crate::PagableSerializer;
    use crate::testing::TestingDeserializer;
    use crate::testing::TestingSerializer;
    use crate::value_serialize::NoneValueSerialize;
    use crate::value_serialize::PagableValueSerialize;
    use crate::value_serialize::ResidentValueSerialize;
    use crate::value_serialize::ValueSerialize;

    #[test]
    fn resident_serializes_like_its_inner_serializer_but_never_pages_out() -> crate::Result<()> {
        let inner = PagableValueSerialize::<u32>::new();
        assert!(inner.pages_out());
        let resident = ResidentValueSerialize::new(PagableValueSerialize::<u32>::new());
        assert!(!resident.pages_out());

        let mut ser = TestingSerializer::new();
        resident
            .pagable_serialize_value(&42u32, &mut ser as &mut dyn PagableSerializer)
            .expect("the inner serializer accepts the value")?;
        let data = ser.finish();
        let mut deser = TestingDeserializer::new(&data);
        assert_eq!(resident.pagable_deserialize_value(&mut deser)?, 42);
        Ok(())
    }

    #[test]
    fn none_is_written_and_some_is_declined() -> crate::Result<()> {
        let s = NoneValueSerialize::<Option<String>>::new();
        let mut ser = TestingSerializer::new();
        assert!(
            s.pagable_serialize_value(&Some("boom".to_owned()), &mut ser)
                .is_none()
        );
        let mut ser = TestingSerializer::new();
        s.pagable_serialize_value(&None, &mut ser)
            .expect("None is written")?;
        let data = ser.finish();
        let mut deser = TestingDeserializer::new(&data);
        assert_eq!(s.pagable_deserialize_value(&mut deser)?, None);
        Ok(())
    }
}
