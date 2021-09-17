//! Serialize DDlog commands to/from FlatBuffers

use differential_datalog::{
    ddval::{DDValConvert, DDValue},
    flatbuf::FlatbufConverter,
    program::{self, IdxId, Response, Update},
    DeltaMap,
};
use enum_primitive_derive::Primitive;
use flatbuffers::{self as fbrt, FlatBufferBuilder, Follow, Push, Vector, WIPOffset};
use num_traits::FromPrimitive;
use ordered_float::OrderedFloat;
use std::{collections::BTreeSet, fmt::Debug, hash::Hash};

/// Trait for types that can be de-serialized from FlatBuffer-embedded objects.
pub trait FromFlatBuffer<T>: Sized {
    fn from_flatbuf(v: T) -> Response<Self>;
}

/// Trait for types that can be serialized to a FlatBuffer.
pub trait ToFlatBuffer<'a>: Sized {
    type Target: 'a;

    fn to_flatbuf(&self, fbb: &mut FlatBufferBuilder<'a>) -> Self::Target;
}

/// Trait for serializing types into FlatBuffer tables.  For types whose `ToFlatBuffer::Target` is
/// a table, this just invokes `ToFlatBuffer::to_flatbuf()`.  Other types are wrapped in a table.
pub trait ToFlatBufferTable<'a>: Sized {
    type Target: 'a;

    fn to_flatbuf_table(&self, fbb: &mut FlatBufferBuilder<'a>) -> WIPOffset<Self::Target>;
}

/// Trait for serializing types so they can be stored in FlatBuffer vectors.  For primitive
/// types and types that serialize in a table, this just invokes `ToFlatBuffer::to_flatbuf()`.
/// Other types are wrapped in tables.
pub trait ToFlatBufferVectorElement<'a>: Sized {
    type Target: 'a + Push + Copy;

    fn to_flatbuf_vector_element(&self, fbb: &mut FlatBufferBuilder<'a>) -> Self::Target;
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

impl<'a> ToFlatBuffer<'_> for OrderedFloat<f32> {
    type Target = f32;

    fn to_flatbuf(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        **self
    }
}

impl<'a> ToFlatBufferVectorElement<'_> for OrderedFloat<f32> {
    type Target = f32;

    fn to_flatbuf_vector_element(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        **self
    }
}

impl FromFlatBuffer<f64> for OrderedFloat<f64> {
    fn from_flatbuf(v: f64) -> Response<OrderedFloat<f64>> {
        Ok(OrderedFloat(v))
    }
}

impl ToFlatBuffer<'_> for OrderedFloat<f64> {
    type Target = f64;

    fn to_flatbuf(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        **self
    }
}

impl ToFlatBufferVectorElement<'_> for OrderedFloat<f64> {
    type Target = f64;

    fn to_flatbuf_vector_element(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        **self
    }
}

impl ToFlatBuffer<'_> for bool {
    type Target = bool;

    fn to_flatbuf(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl ToFlatBufferVectorElement<'_> for bool {
    type Target = bool;

    fn to_flatbuf_vector_element(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl FromFlatBuffer<u8> for u8 {
    fn from_flatbuf(v: u8) -> Response<u8> {
        Ok(v)
    }
}

impl ToFlatBuffer<'_> for u8 {
    type Target = u8;

    fn to_flatbuf(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl ToFlatBufferVectorElement<'_> for u8 {
    type Target = u8;

    fn to_flatbuf_vector_element(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl FromFlatBuffer<u16> for u16 {
    fn from_flatbuf(v: u16) -> Response<u16> {
        Ok(v)
    }
}

impl ToFlatBuffer<'_> for u16 {
    type Target = u16;

    fn to_flatbuf(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl ToFlatBufferVectorElement<'_> for u16 {
    type Target = u16;

    fn to_flatbuf_vector_element(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl FromFlatBuffer<u32> for u32 {
    fn from_flatbuf(v: u32) -> Response<u32> {
        Ok(v)
    }
}

impl ToFlatBuffer<'_> for u32 {
    type Target = u32;

    fn to_flatbuf(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl ToFlatBufferVectorElement<'_> for u32 {
    type Target = u32;

    fn to_flatbuf_vector_element(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl FromFlatBuffer<u64> for u64 {
    fn from_flatbuf(v: u64) -> Response<u64> {
        Ok(v)
    }
}

impl ToFlatBuffer<'_> for u64 {
    type Target = u64;

    fn to_flatbuf(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl ToFlatBufferVectorElement<'_> for u64 {
    type Target = u64;

    fn to_flatbuf_vector_element(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl FromFlatBuffer<i8> for i8 {
    fn from_flatbuf(v: i8) -> Response<i8> {
        Ok(v)
    }
}

impl ToFlatBuffer<'_> for i8 {
    type Target = i8;

    fn to_flatbuf(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl ToFlatBufferVectorElement<'_> for i8 {
    type Target = i8;

    fn to_flatbuf_vector_element(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl FromFlatBuffer<i16> for i16 {
    fn from_flatbuf(v: i16) -> Response<i16> {
        Ok(v)
    }
}

impl ToFlatBuffer<'_> for i16 {
    type Target = i16;

    fn to_flatbuf(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl ToFlatBufferVectorElement<'_> for i16 {
    type Target = i16;

    fn to_flatbuf_vector_element(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl FromFlatBuffer<i32> for i32 {
    fn from_flatbuf(v: i32) -> Response<i32> {
        Ok(v)
    }
}

impl ToFlatBuffer<'_> for i32 {
    type Target = i32;

    fn to_flatbuf(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl ToFlatBufferVectorElement<'_> for i32 {
    type Target = i32;

    fn to_flatbuf_vector_element(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl FromFlatBuffer<i64> for i64 {
    fn from_flatbuf(v: i64) -> Response<i64> {
        Ok(v)
    }
}

impl ToFlatBuffer<'_> for i64 {
    type Target = i64;

    fn to_flatbuf(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl ToFlatBufferVectorElement<'_> for i64 {
    type Target = i64;

    fn to_flatbuf_vector_element(&self, _builder: &mut FlatBufferBuilder<'_>) -> Self::Target {
        *self
    }
}

impl<'a> FromFlatBuffer<&'a str> for String {
    fn from_flatbuf(s: &'a str) -> Response<String> {
        Ok(s.to_string())
    }
}

impl<'a> ToFlatBuffer<'a> for String {
    type Target = WIPOffset<&'a str>;

    fn to_flatbuf(&self, fbb: &mut FlatBufferBuilder<'a>) -> Self::Target {
        fbb.create_string(self)
    }
}

impl<'a> ToFlatBufferVectorElement<'a> for String {
    type Target = <String as ToFlatBuffer<'a>>::Target;

    fn to_flatbuf_vector_element(&self, fbb: &mut FlatBufferBuilder<'a>) -> Self::Target {
        self.to_flatbuf(fbb)
    }
}

/// Iterator based on `fbrt::Vector`
#[derive(Debug)]
pub struct FBIter<'a, F: flatbuffers::Follow<'a> + 'a>
where
    <F as flatbuffers::Follow<'a>>::Inner: Debug,
{
    vec: Vector<'a, F>,
    off: usize,
}

impl<'a, F: flatbuffers::Follow<'a> + 'a> FBIter<'a, F>
where
    <F as flatbuffers::Follow<'a>>::Inner: Debug,
{
    pub fn from_vector(vec: Vector<'a, F>) -> FBIter<'a, F> {
        FBIter { vec, off: 0 }
    }
}

impl<'a, F> Iterator for FBIter<'a, F>
where
    F: flatbuffers::Follow<'a> + 'a,
    <F as flatbuffers::Follow<'a>>::Inner: Debug,
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

impl<'a> FromFlatBuffer<fb::__String<'a>> for String {
    fn from_flatbuf(v: fb::__String<'a>) -> Response<Self> {
        let v = v
            .v()
            .ok_or_else(|| format!("String::from_flatbuf: invalid buffer: failed to extract v"))?;
        <String>::from_flatbuf(v)
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

impl<'b> ToFlatBuffer<'b> for u128 {
    type Target = fbrt::WIPOffset<fb::__BigUint<'b>>;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        let vec = fbb.create_vector(&self.to_be_bytes());
        fb::__BigUint::create(fbb, &fb::__BigUintArgs { bytes: Some(vec) })
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

impl<'b> ToFlatBuffer<'b> for i128 {
    type Target = fbrt::WIPOffset<fb::__BigInt<'b>>;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        let vec = fbb.create_vector(&self.to_be_bytes());
        fb::__BigInt::create(
            fbb,
            &fb::__BigIntArgs {
                bytes: Some(vec),
                sign: self.is_positive(),
            },
        )
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

            Ok(i128::from_be_bytes(arr))
        }
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

// Wrapper type, so we can implement traits for it.
pub struct DDValueUpdate(pub Update<DDValue>);

impl<'a> FromFlatBuffer<fb::__Command<'a>> for DDValueUpdate {
    fn from_flatbuf(cmd: fb::__Command<'a>) -> Response<Self> {
        let relid = cmd.relid() as program::RelId;
        let val_table = cmd.val().ok_or_else(|| {
            format!("Update::from_flatbuf: invalid buffer: failed to extract value")
        })?;

        match FBOperation::from_i8(cmd.operation()) {
            Some(FBOperation::InsertOrDelete) => {
                let val = relval_from_flatbuf(relid, val_table)?;
                match cmd.weight() {
                    1 => Ok(DDValueUpdate(Update::Insert { relid, v: val })),
                    (-1) => Ok(DDValueUpdate(Update::DeleteValue { relid, v: val })),
                    w => Err(format!("Update::from_flatbuf: non-unit weight {}", w)),
                }
            }

            Some(FBOperation::InsertOrUpdate) => {
                let val = relval_from_flatbuf(relid, val_table)?;
                Ok(DDValueUpdate(Update::InsertOrUpdate { relid, v: val }))
            }

            Some(FBOperation::DeleteByKey) => {
                let val = relkey_from_flatbuf(relid, val_table)?;
                Ok(DDValueUpdate(Update::DeleteKey { relid, k: val }))
            }

            None => Err(format!(
                "Update::from_flatbuf: could not convert operation code {}",
                cmd.operation()
            )),
        }
    }
}

// These should be ideally imported from flatbuf, but
// I don't know how to do this.  In the meantime keep in sync
// with Flatbuffer.hs
#[derive(Debug, Eq, PartialEq, Primitive)]
enum FBOperation {
    InsertOrDelete = 0,
    InsertOrUpdate = 1,
    DeleteByKey = 2,
}

// Wrapper type, so we can implement traits for it.
struct FBDDValue<'a>(program::RelId, &'a DDValue, isize);

impl<'b> ToFlatBuffer<'b> for FBDDValue<'_> {
    type Target = fbrt::WIPOffset<fb::__Command<'b>>;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        let (val_type, v) = relval_to_flatbuf(self.0, self.1, fbb);
        fb::__Command::create(
            fbb,
            &fb::__CommandArgs {
                operation: FBOperation::InsertOrDelete as i8,
                weight: self.2 as i64,
                relid: self.0 as u64,
                val_type,
                val: Some(v),
            },
        )
    }
}

#[derive(Debug, Clone)]
pub struct DDlogFlatbufConverter;

impl FlatbufConverter for DDlogFlatbufConverter {
    fn updates_from_buffer(&self, buffer: &[u8]) -> Result<Vec<Update<DDValue>>, String> {
        updates_from_flatbuf(buffer)?
            .map(|cmd| DDValueUpdate::from_flatbuf(cmd).map(|x| x.0))
            .collect()
    }

    fn query_index_from_buffer(&self, buffer: &[u8]) -> Result<(IdxId, DDValue), String> {
        let q = unsafe { flatbuffers::root_unchecked::<fb::__Query<'_>>(buffer) };

        if let Some(key) = q.key() {
            Ok((
                q.idxid() as usize,
                idxkey_from_flatbuf(q.idxid() as usize, key)?,
            ))
        } else {
            Err("Invalid buffer: failed to extract key".to_string())
        }
    }

    fn updates_to_buffer(&self, delta: &DeltaMap<DDValue>) -> Result<(Vec<u8>, usize), String> {
        // Pre-compute size of delta.
        let mut delta_size: usize = 0;
        for (_, rel) in delta.iter() {
            delta_size += rel.len();
        }

        // Each record takes at least 8 bytes of FlatBuffer space.
        let mut fbb = fbrt::FlatBufferBuilder::with_capacity(8 * delta_size);
        let mut cmds = Vec::with_capacity(delta_size);

        for (relid, rel) in delta.iter() {
            for (v, w) in rel.iter() {
                //assert!(*w == 1 || *w == -1);
                cmds.push(FBDDValue(*relid, v, *w).to_flatbuf(&mut fbb));
            }
        }

        let cmd_vec = fbb.create_vector(cmds.as_slice());
        let cmd_table = fb::__Commands::create(
            &mut fbb,
            &fb::__CommandsArgs {
                commands: Some(cmd_vec),
            },
        );

        fb::finish___commands_buffer(&mut fbb, cmd_table);
        Ok(fbb.collapse())
    }

    fn index_values_to_buffer(
        &self,
        index_id: IdxId,
        contents: &BTreeSet<DDValue>,
    ) -> Result<(Vec<u8>, usize), String> {
        Ok(idx_values_to_flatbuf(index_id, contents.iter()))
    }
}

pub fn updates_from_flatbuf<'a>(
    buf: &'a [u8],
) -> Response<FBIter<'a, fbrt::ForwardsUOffset<fb::__Command<'a>>>> {
    if let Some(cmds) = unsafe { fb::root_as___commands_unchecked(buf) }.commands() {
        Ok(FBIter::from_vector(cmds))
    } else {
        Err("Invalid buffer: failed to extract commands".to_string())
    }
}

pub fn idx_values_to_flatbuf<'a, I>(idxid: program::IdxId, vals: I) -> (Vec<u8>, usize)
where
    I: Iterator<Item = &'a DDValue>,
{
    let size = vals.size_hint().0;

    /* Each value takes at least 4 bytes of FlatBuffer space. */
    let mut fbb = fbrt::FlatBufferBuilder::with_capacity(4 * size);
    let mut val_tables = Vec::with_capacity(size);

    for val in vals {
        val_tables.push(idxval_to_flatbuf_vector_element(idxid, val, &mut fbb));
    }

    let val_vec = fbb.create_vector(val_tables.as_slice());
    let vals_table = fb::__Values::create(
        &mut fbb,
        &fb::__ValuesArgs {
            values: Some(val_vec),
        },
    );

    fbb.finish(vals_table, None);
    fbb.collapse()
}

/* AUTOMATICALLY GENERATED CODE AFTER THIS POINT */
