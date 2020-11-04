//! Serialize DDlog types to/from FlatBuffers.

use crate::*;
use ::ordered_float::OrderedFloat;
use differential_datalog::program::Response;
use flatbuffers as fbrt;

/// Trait for types that can be de-serialized from FlatBuffer-embedded objects.
pub trait FromFlatBuffer<T>: Sized {
    fn from_flatbuf(v: T) -> Response<Self>;
}

/// Trait for types that can be serialized to a FlatBuffer.
pub trait ToFlatBuffer<'b>: Sized {
    type Target: 'b;
    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target;
}

/// Trait for serializing types into FlatBuffer tables.  For types whose `ToFlatBuffer::Target` is
/// a table, this just invokes `ToFlatBuffer::to_flatbuf()`.  Other types are wrapped in a table.
pub trait ToFlatBufferTable<'b>: Sized {
    type Target: 'b;
    fn to_flatbuf_table(
        &self,
        fbb: &mut fbrt::FlatBufferBuilder<'b>,
    ) -> fbrt::WIPOffset<Self::Target>;
}

/// Trait for serializing types so they can be stored in FlatBuffer vectors.  For primitive
/// types and types that serialize in a table, this just invokes `ToFlatBuffer::to_flatbuf()`.
/// Other types are wrapped in tables.
pub trait ToFlatBufferVectorElement<'b>: Sized {
    type Target: 'b + fbrt::Push + Copy;
    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target;
}

impl FromFlatBuffer<bool> for bool {
    fn from_flatbuf(v: bool) -> Response<bool> {
        Ok(v)
    }
}

impl FromFlatBuffer<f32> for OrderedFloat<f32> {
    fn from_flatbuf(v: f32) -> Response<OrderedFloat<f32>> {
        Ok(OrderedFloat(v))
    }
}

impl<'b> ToFlatBuffer<'b> for OrderedFloat<f32> {
    type Target = f32;

    fn to_flatbuf(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        **self
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for OrderedFloat<f32> {
    type Target = f32;

    fn to_flatbuf_vector_element(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        **self
    }
}

impl FromFlatBuffer<f64> for OrderedFloat<f64> {
    fn from_flatbuf(v: f64) -> Response<OrderedFloat<f64>> {
        Ok(OrderedFloat(v))
    }
}

impl<'b> ToFlatBuffer<'b> for OrderedFloat<f64> {
    type Target = f64;

    fn to_flatbuf(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        **self
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for OrderedFloat<f64> {
    type Target = f64;

    fn to_flatbuf_vector_element(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        **self
    }
}

impl<'b> ToFlatBuffer<'b> for bool {
    type Target = bool;

    fn to_flatbuf(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for bool {
    type Target = bool;

    fn to_flatbuf_vector_element(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl FromFlatBuffer<u8> for u8 {
    fn from_flatbuf(v: u8) -> Response<u8> {
        Ok(v)
    }
}

impl<'b> ToFlatBuffer<'b> for u8 {
    type Target = u8;

    fn to_flatbuf(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for u8 {
    type Target = u8;

    fn to_flatbuf_vector_element(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl FromFlatBuffer<u16> for u16 {
    fn from_flatbuf(v: u16) -> Response<u16> {
        Ok(v)
    }
}

impl<'b> ToFlatBuffer<'b> for u16 {
    type Target = u16;

    fn to_flatbuf(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for u16 {
    type Target = u16;

    fn to_flatbuf_vector_element(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl FromFlatBuffer<u32> for u32 {
    fn from_flatbuf(v: u32) -> Response<u32> {
        Ok(v)
    }
}

impl<'b> ToFlatBuffer<'b> for u32 {
    type Target = u32;

    fn to_flatbuf(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for u32 {
    type Target = u32;

    fn to_flatbuf_vector_element(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl FromFlatBuffer<u64> for u64 {
    fn from_flatbuf(v: u64) -> Response<u64> {
        Ok(v)
    }
}

impl<'b> ToFlatBuffer<'b> for u64 {
    type Target = u64;

    fn to_flatbuf(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for u64 {
    type Target = u64;

    fn to_flatbuf_vector_element(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl<'a> FromFlatBuffer<fb::__BigUint<'a>> for u128 {
    fn from_flatbuf(v: fb::__BigUint<'a>) -> Response<u128> {
        let bytes = v.bytes().ok_or_else(|| {
            format!("u128::from_flatbuf: invalid buffer: failed to extract bytes")
        })?;
        if bytes.len() > 16 {
            Err(format!(
                "u128::from_flatbuf: invalid buffer length {}",
                bytes.len()
            ))
        } else {
            let mut arr: [u8; 16] = [0; 16];
            let off = 16 - bytes.len();
            for (i, x) in bytes.iter().enumerate() {
                arr[i + off] = *x;
            }
            Ok(u128::from_be_bytes(arr))
        }
    }
}

impl<'b> ToFlatBuffer<'b> for u128 {
    type Target = fbrt::WIPOffset<fb::__BigUint<'b>>;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        let vec = fbb.create_vector(&self.to_be_bytes());
        fb::__BigUint::create(fbb, &fb::__BigUintArgs { bytes: Some(vec) })
    }
}

impl<'b> ToFlatBufferTable<'b> for u128 {
    type Target = fb::__BigUint<'b>;
    fn to_flatbuf_table(
        &self,
        fbb: &mut fbrt::FlatBufferBuilder<'b>,
    ) -> fbrt::WIPOffset<Self::Target> {
        self.to_flatbuf(fbb)
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for u128 {
    type Target = <u128 as ToFlatBuffer<'b>>::Target;

    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        self.to_flatbuf(fbb)
    }
}

impl FromFlatBuffer<i8> for i8 {
    fn from_flatbuf(v: i8) -> Response<i8> {
        Ok(v)
    }
}

impl<'b> ToFlatBuffer<'b> for i8 {
    type Target = i8;

    fn to_flatbuf(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for i8 {
    type Target = i8;

    fn to_flatbuf_vector_element(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl FromFlatBuffer<i16> for i16 {
    fn from_flatbuf(v: i16) -> Response<i16> {
        Ok(v)
    }
}

impl<'b> ToFlatBuffer<'b> for i16 {
    type Target = i16;

    fn to_flatbuf(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for i16 {
    type Target = i16;

    fn to_flatbuf_vector_element(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl FromFlatBuffer<i32> for i32 {
    fn from_flatbuf(v: i32) -> Response<i32> {
        Ok(v)
    }
}

impl<'b> ToFlatBuffer<'b> for i32 {
    type Target = i32;

    fn to_flatbuf(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for i32 {
    type Target = i32;

    fn to_flatbuf_vector_element(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl FromFlatBuffer<i64> for i64 {
    fn from_flatbuf(v: i64) -> Response<i64> {
        Ok(v)
    }
}

impl<'b> ToFlatBuffer<'b> for i64 {
    type Target = i64;

    fn to_flatbuf(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for i64 {
    type Target = i64;

    fn to_flatbuf_vector_element(&self, _fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        *self
    }
}

impl<'a> FromFlatBuffer<fb::__BigInt<'a>> for i128 {
    fn from_flatbuf(v: fb::__BigInt<'a>) -> Response<i128> {
        let bytes = v.bytes().ok_or_else(|| {
            format!("i128::from_flatbuf: invalid buffer: failed to extract bytes")
        })?;
        if bytes.len() > 16 {
            Err(format!(
                "i128::from_flatbuf: invalid buffer length {}",
                bytes.len()
            ))
        } else {
            let mut arr: [u8; 16] = [0; 16];
            let off = 16 - bytes.len();
            for (i, x) in bytes.iter().enumerate() {
                arr[i + off] = *x;
            }
            let u = u128::from_be_bytes(arr);
            let i: i128 = if v.sign() {
                u as i128
            } else {
                (u as i128).wrapping_neg()
            };
            Ok(i)
        }
    }
}

impl<'b> ToFlatBuffer<'b> for i128 {
    type Target = fbrt::WIPOffset<fb::__BigInt<'b>>;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        let vec = fbb.create_vector(&self.wrapping_abs().to_be_bytes());
        fb::__BigInt::create(
            fbb,
            &fb::__BigIntArgs {
                sign: *self >= 0,
                bytes: Some(vec),
            },
        )
    }
}

impl<'b> ToFlatBufferTable<'b> for i128 {
    type Target = fb::__BigInt<'b>;
    fn to_flatbuf_table(
        &self,
        fbb: &mut fbrt::FlatBufferBuilder<'b>,
    ) -> fbrt::WIPOffset<Self::Target> {
        self.to_flatbuf(fbb)
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for i128 {
    type Target = <i128 as ToFlatBuffer<'b>>::Target;

    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        self.to_flatbuf(fbb)
    }
}

impl<'a> FromFlatBuffer<&'a str> for String {
    fn from_flatbuf(s: &'a str) -> Response<String> {
        Ok(s.to_string())
    }
}

impl<'a> FromFlatBuffer<fb::__String<'a>> for String {
    fn from_flatbuf(v: fb::__String<'a>) -> Response<Self> {
        let v = v
            .v()
            .ok_or_else(|| format!("String::from_flatbuf: invalid buffer: failed to extract v"))?;
        <String>::from_flatbuf(v)
    }
}

impl<'b> ToFlatBuffer<'b> for String {
    type Target = fbrt::WIPOffset<&'b str>;
    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        fbb.create_string(self)
    }
}

impl<'b> ToFlatBufferTable<'b> for String {
    type Target = fb::__String<'b>;
    fn to_flatbuf_table(
        &self,
        fbb: &mut fbrt::FlatBufferBuilder<'b>,
    ) -> fbrt::WIPOffset<Self::Target> {
        let v = self.to_flatbuf(fbb);
        fb::__String::create(fbb, &fb::__StringArgs { v: Some(v) })
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for String {
    type Target = <String as ToFlatBuffer<'b>>::Target;

    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        self.to_flatbuf(fbb)
    }
}

/// Iterator based on `fbrt::Vector`
pub struct FBIter<'a, F> {
    vec: fbrt::Vector<'a, F>,
    off: usize,
}

impl<'a, F: 'a> FBIter<'a, F> {
    pub fn from_vector(vec: fbrt::Vector<'a, F>) -> FBIter<'a, F> {
        FBIter { vec: vec, off: 0 }
    }
}

impl<'a, F> Iterator for FBIter<'a, F>
where
    F: fbrt::Follow<'a> + 'a,
{
    type Item = F::Inner;

    fn next(&mut self) -> Option<Self::Item> {
        if self.off >= self.vec.len() {
            None
        } else {
            let off = self.off;
            self.off += 1;
            Some(self.vec.get(off))
        }
    }
}

/* AUTOMATICALLY GENERATED CODE AFTER THIS POINT */
