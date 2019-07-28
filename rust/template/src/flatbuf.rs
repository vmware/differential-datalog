//! Serialize DDlog commands to/from FlatBuffers

use super::*;
use differential_datalog::program::{Response, Update, RelId};
use flatbuffers as fbrt;
use flatbuf_generated::ddlog::__datalog_example as fb;

use std::iter;

/// Trait for types that can be de-serialized from FlatBuffer-embedded objects.
pub trait FromFlatBuffer<T>: Sized {
    fn from_flatbuf(v: T) -> Response<Self>;
}

impl FromFlatBuffer<bool> for bool {
    fn from_flatbuf(v: bool) -> Response<bool> { Ok(v) }
}

impl FromFlatBuffer<u8> for u8 {
    fn from_flatbuf(v: u8) -> Response<u8> { Ok(v) }
}

impl FromFlatBuffer<u16> for u16 {
    fn from_flatbuf(v: u16) -> Response<u16> { Ok(v) }
}

impl FromFlatBuffer<u32> for u32 {
    fn from_flatbuf(v: u32) -> Response<u32> { Ok(v) }
}

impl FromFlatBuffer<u64> for u64 {
    fn from_flatbuf(v: u64) -> Response<u64> { Ok(v) }
}

impl <'a> FromFlatBuffer<fb::__BigUint<'a>> for u128 {
    fn from_flatbuf(v: fb::__BigUint<'a>) -> Response<u128> {
        let bytes = v.bytes().ok_or_else(||format!("u128::from_flatbuf: invalid buffer: failed to extract bytes"))?;
        if bytes.len() > 16 {
            Err(format!("u128::from_flatbuf: invalid buffer length {}", bytes.len()))
        } else {
            let mut arr: [u8; 16] = [0; 16];
            let off = 16-bytes.len();
            for (i,x) in bytes.iter().enumerate() {
                arr[i+off] = *x;
            };
            Ok(u128::from_be_bytes(arr))
        }
    }
}

impl FromFlatBuffer<i8> for i8 {
    fn from_flatbuf(v: i8) -> Response<i8> { Ok(v) }
}

impl FromFlatBuffer<i16> for i16 {
    fn from_flatbuf(v: i16) -> Response<i16> { Ok(v) }
}

impl FromFlatBuffer<i32> for i32 {
    fn from_flatbuf(v: i32) -> Response<i32> { Ok(v) }
}

impl FromFlatBuffer<i64> for i64 {
    fn from_flatbuf(v: i64) -> Response<i64> { Ok(v) }
}

impl <'a> FromFlatBuffer<fb::__BigInt<'a>> for i128 {
    fn from_flatbuf(v: fb::__BigInt<'a>) -> Response<i128> {
        let bytes = v.bytes().ok_or_else(||format!("i128::from_flatbuf: invalid buffer: failed to extract bytes"))?;
        if bytes.len() > 16 {
            Err(format!("i128::from_flatbuf: invalid buffer length {}", bytes.len()))
        } else {
            let mut arr: [u8; 16] = [0; 16];
            let off = 16-bytes.len();
            for (i,x) in bytes.iter().enumerate() {
                arr[i+off] = *x;
            };
            let mut u = u128::from_be_bytes(arr);
            let i: i128 = if v.sign() {
                u as i128
            } else {
                -(u as i128)
            };
            Ok(i)
        }
    }
}

impl <'a> FromFlatBuffer<&'a str> for String {
    fn from_flatbuf(s: &'a str) -> Response<String> { Ok(s.to_string()) }
}

impl <'a> FromFlatBuffer<fb::__BigInt<'a>> for Int {
    fn from_flatbuf(fb: fb::__BigInt<'a>) -> Response<Int> {
        let bytes = fb.bytes().ok_or_else(||format!("Int::from_flatbuf: invalid buffer: failed to extract bytes"))?;
        Ok(Int::from_bytes_be(fb.sign(), bytes))
    }
}

impl <'a> FromFlatBuffer<fb::__BigUint<'a>> for Uint {
    fn from_flatbuf(fb: fb::__BigUint<'a>) -> Response<Uint> {
        let bytes = fb.bytes().ok_or_else(||format!("Uint::from_flatbuf: invalid buffer: failed to extract bytes"))?;
        Ok(Uint::from_bytes_be(bytes))
    }
}


impl <'a> FromFlatBuffer<fb::__Command<'a>> for Update<Value> {
    fn from_flatbuf(cmd: fb::__Command<'a>) -> Response<Self> {
        let val_table = cmd.val().ok_or_else(||format!("Update::from_flatbuf: invalid buffer: failed to extract value"))?;
        let val = Value::from_flatbuf((cmd.val_type(), val_table))?;
        match cmd.kind() {
            fb::__CommandKind::Insert => {
                Ok(Update::Insert{relid: cmd.relid() as RelId, v: val})
            },
            fb::__CommandKind::Delete => {
                Ok(Update::DeleteValue{relid: cmd.relid() as RelId, v: val})
            },
            k => {
                Err(format!("Update::from_flatbuf: invalid command kind {:?}", k))
            }
        }
    }
}

/// Iterator based on `fbrt::Vector`
pub struct FBIter<'a, F> {
    vec: fbrt::Vector<'a, F>,
    off: usize
}

impl <'a, F: 'a> FBIter<'a, F> {
    pub fn from_vector(vec: fbrt::Vector<'a, F>) -> FBIter<'a, F> {
        FBIter{vec: vec, off: 0}
    }
}

impl <'a, F> Iterator for FBIter<'a, F>
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

pub fn updates_from_flatbuf<'a>(buf: &'a [u8]) -> Response<FBIter<'a, fbrt::ForwardsUOffset<fb::__Command<'a>>>>
{
    if let Some(cmds) = fb::get_root_as___commands(buf).commands() {
        Ok(FBIter::from_vector(cmds))
    } else {
        Err("Invalid buffer: failed to extract commands".to_string())
    }
}

/* AUTOMATICALLY GENERATED CODE AFTER THIS POINT */
