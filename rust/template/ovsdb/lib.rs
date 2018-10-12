//! Parse OVSDB database update messages and convert them into DDlog table update commands

extern crate differential_datalog;
extern crate serde_json;
extern crate num;

use differential_datalog::record::*;
use serde_json::Value;
use serde_json::map::Map;
use num::BigInt;

#[cfg(test)]
mod test;

fn parse_uuid(s: &str) -> Result<BigInt, String> {
    let digits: Vec<u8> = s.as_bytes().iter().map(|x|*x).filter(|x|*x != b'-').collect();
    BigInt::parse_bytes(digits.as_slice(), 16)
        .ok_or(format!("invalid uuid string \"{}\"", s))
}

fn lookup<'a>(m: &'a mut Map<String, Value>, field: &str) -> Result<Value, String> {
    m.remove(field).ok_or(format!("JSON map does not contain \"{}\" field: {:?}", field, m))
}

/* <table-updates> is an object that maps from a table name to a <table-update>.
 *
 * `prefix` is the DB name prepended to all table names
 */
pub fn cmds_from_table_updates(prefix: &str, upds: Map<String,Value>) -> Result<Vec<UpdCmd>, String> {
    let mut commands = Vec::new();
    let oks: Result<Vec<()>, String> = upds.into_iter().map(|(table, updates)|cmds_from_table_update(prefix.to_string() + table.as_str(), updates, &mut commands)).collect();
    oks?;
    Ok(commands)
}

/* A <table-update> is an object that maps from the row's UUID to a <row-update> object.
 */
fn cmds_from_table_update(table: String, updates: Value, cmds: &mut Vec<UpdCmd>) -> Result<(), String> {
    match updates {
        Value::Object(upds) => {
            upds.into_iter().map(|(uuid, u)| cmd_from_row_update(table.as_str(), uuid, u, cmds)).collect()
        },
        _ => Err(format!("table update is not an object: {}", updates))
    }
}

/* A <row-update> is an object
 *  with the following members:
 *
 * "old": <row>   present for "delete" and "modify" updates
 * "new": <row>   present for "initial", "insert", and "modify" updates
 */
fn cmd_from_row_update(table: &str, uuid: String, update: Value, cmds: &mut Vec<UpdCmd>) -> Result<(), String> {
    let uuid = Record::Int(parse_uuid(uuid.as_ref())?);
    match update {
        Value::Object(mut m) => {
            let old = lookup(&mut m, "old").ok().map(|v|row_from_obj(v));
            let new = lookup(&mut m, "new").ok().map(|v|row_from_obj(v));
            if old.is_some() {
                // delete_key
                cmds.push(UpdCmd::DeleteKey(table.to_string(), uuid.clone()))
            };
            if new.is_some() {
                let mut fields = new.unwrap()?;
                fields.push(("_uuid".to_owned(), uuid));
                // insert
                cmds.push(UpdCmd::Insert(table.to_string(), Record::NamedStruct(table.to_string(), fields)))
            };
            Ok(())
        },
        _ => Err(format!("row update is not an object: {}", update))
    }
}

/* ["uuid", "cf9dca71-2495-4e61-9fe2-8ea4fb2859cd"]
 */
fn record_from_array(mut src: Vec<Value>) -> Result<Record,String> {
    if src.len() != 2 {
        return Err(format!("record array is not of length 2: {:?}", src));
    };
    let val = src.remove(1);
    let field = src.remove(0);
    match field {
        Value::String(field) => {
            match (field.as_str(), val) {
                ("uuid", Value::String(uuid)) => {
                    Ok(Record::Int(parse_uuid(uuid.as_ref())?))
                },
                ("named-uuid", Value::String(uuid_name)) => {
                    Ok(Record::String(uuid_name))
                },
                ("set", Value::Array(atoms)) => {
                    let elems: Result<Vec<Record>, String> = atoms.into_iter().map(|a|record_from_val(a)).collect();
                    Ok(Record::Array(elems?))
                },
                ("map", Value::Array(pairs)) => {
                    let elems: Result<Vec<(Record, Record)>, String> = pairs.into_iter().map(|p|pair_from_val(p)).collect();
                    Ok(Record::Array(elems?.into_iter().map(|(k,v)|Record::Tuple(vec![k,v])).collect()))
                },
                _ => Err(format!("unexpected array value type \"{}\"", field))
            }
            
        },
        _ => Err(format!("invalid array value: {:?}", src))
    }
}

fn pair_from_val(pair: Value) -> Result<(Record, Record), String> {
    match pair {
        Value::Array(mut v) => {
            if v.len() != 2 {
                return Err(format!("key-value pair must be of length 2: {:?}", v))
            };
            let val = v.remove(1);
            let key = v.remove(0);
            Ok((record_from_val(key)?, record_from_val(val)?))
        },
        _ => Err(format!("invalid key-value pair {}", pair))
    }
}

/*
  {
    "interfaces": [ "named-uuid", "rowe82c26a4_bbf4_4e84_87fd_2107d5998a12"],
    "name": "br0"
  }
*/
fn row_from_obj(val: Value) -> Result<Vec<(String, Record)>, String> {
    match val{
        Value::Object(m) => {
            m.into_iter().map(|(field, val)|Ok((field, record_from_val(val)?))).collect()
        },
        _ => Err(format!("\"row\" is not an object in {}", val))
    }
}

fn record_from_val(v: Value) -> Result<Record, String> {
    match v {
        // <string>
        Value::String(s) => Ok(Record::String(s)),
        // <number>
        Value::Number(ref n) if n.is_u64() => Ok(Record::Int(BigInt::from(n.as_u64().unwrap()))),
        // <boolean>
        Value::Bool(b) => Ok(Record::Bool(b)),
        // <uuid>, <named-uuid>, <set>, <map>
        Value::Array(v) => record_from_array(v),
        _ => Err(format!("unexpected value {}", v))
    } 
}
