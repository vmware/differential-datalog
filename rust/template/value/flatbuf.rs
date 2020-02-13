//! Serialize DDlog commands to/from FlatBuffers

use super::*;
use differential_datalog::program::{RelId, Response, Update};
use differential_datalog::DeltaMap;
use flatbuffers as fbrt;

use types::flatbuf::*;

// Wrapper type, so we can implement traits for it.
pub struct DDValueUpdate(pub Update<DDValue>);

impl<'a> FromFlatBuffer<fb::__Command<'a>> for DDValueUpdate {
    fn from_flatbuf(cmd: fb::__Command<'a>) -> Response<Self> {
        let relid = cmd.relid() as RelId;
        let val_table = cmd.val().ok_or_else(|| {
            format!("Update::from_flatbuf: invalid buffer: failed to extract value")
        })?;
        let val = relval_from_flatbuf(relid, val_table)?;
        match cmd.kind() {
            fb::__CommandKind::Insert => Ok(DDValueUpdate(Update::Insert { relid, v: val })),
            fb::__CommandKind::Delete => Ok(DDValueUpdate(Update::DeleteValue { relid, v: val })),
        }
    }
}

// Wrapper type, so we can implement traits for it.
struct FBDDValue<'a>(RelId, &'a DDValue, bool);

impl<'b> ToFlatBuffer<'b> for FBDDValue<'_> {
    type Target = fbrt::WIPOffset<fb::__Command<'b>>;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        let (val_type, v) = relval_to_flatbuf(self.0, self.1, fbb);
        fb::__Command::create(
            fbb,
            &fb::__CommandArgs {
                kind: if self.2 {
                    fb::__CommandKind::Insert
                } else {
                    fb::__CommandKind::Delete
                },
                relid: self.0 as u64,
                val_type,
                val: Some(v),
            },
        )
    }
}

pub fn updates_from_flatbuf<'a>(
    buf: &'a [u8],
) -> Response<FBIter<'a, fbrt::ForwardsUOffset<fb::__Command<'a>>>> {
    if let Some(cmds) = fb::get_root_as___commands(buf).commands() {
        Ok(FBIter::from_vector(cmds))
    } else {
        Err("Invalid buffer: failed to extract commands".to_string())
    }
}

pub fn updates_to_flatbuf(delta: &DeltaMap<DDValue>) -> (Vec<u8>, usize) {
    /* Pre-compute size of delta. */
    let mut delta_size: usize = 0;

    for (_, rel) in delta.as_ref().iter() {
        delta_size += rel.len();
    }

    /* Each record takes at least 8 bytes of FlatBuffer space. */
    let mut fbb = fbrt::FlatBufferBuilder::new_with_capacity(8 * delta_size);
    let mut cmds = Vec::with_capacity(delta_size);

    for (relid, rel) in delta.as_ref().iter() {
        for (v, w) in rel.iter() {
            assert!(*w == 1 || *w == -1);
            cmds.push(FBDDValue(*relid, v, *w == 1).to_flatbuf(&mut fbb));
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
    fbb.collapse()
}

pub fn query_from_flatbuf<'a>(buf: &'a [u8]) -> Response<(IdxId, DDValue)> {
    let q = flatbuffers::get_root::<fb::__Query<'a>>(buf);
    if let Some(key) = q.key() {
        Ok((
            q.idxid() as usize,
            idxkey_from_flatbuf(q.idxid() as usize, key)?,
        ))
    } else {
        Err("Invalid buffer: failed to extract key".to_string())
    }
}

pub fn idx_values_to_flatbuf<'a, I>(idxid: IdxId, vals: I) -> (Vec<u8>, usize)
where
    I: Iterator<Item = &'a DDValue>,
{
    let size = vals.size_hint().0;

    /* Each value takes at least 4 bytes of FlatBuffer space. */
    let mut fbb = fbrt::FlatBufferBuilder::new_with_capacity(4 * size);
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
